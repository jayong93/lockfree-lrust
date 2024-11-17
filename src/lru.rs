use std::{
    num::NonZeroUsize,
    ops::ControlFlow,
    process::abort,
    ptr::{self, NonNull},
    sync::atomic::{self, AtomicUsize, Ordering},
    usize,
};

use crossbeam::{
    epoch::{pin, Atomic, Guard, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;
use smallvec::{smallvec, SmallVec};

use crate::atomic_ref_count::{AtomicRefCounted, CompareExchangeOk, RefCounted};

pub struct Lru<K: Ord + Send + Sync + 'static, V: Send + Sync + 'static> {
    skiplist: SkipMap<RefCounted<K>, Atomic<Node<K, V>>>,
    // `head` and `tail` are sentry nodes which have no valid data
    head: Owned<Node<K, V>>,
    tail: Owned<Node<K, V>>,
    cap: NonZeroUsize,
    size: AtomicUsize,
}

impl<K: Ord + Send + Sync + 'static, V: Send + Sync + 'static> Drop for Lru<K, V> {
    fn drop(&mut self) {
        self.skiplist.clear();
    }
}

impl<K: Ord + Send + Sync + 'static, V: Send + Sync + 'static> Lru<K, V> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let mut head = Node::uninit(Atomic::null(), Atomic::null());
        let head_ptr = head.as_ref() as *const Node<K, V>;
        let mut tail = Node::uninit(Atomic::null(), Atomic::null());
        let tail_ptr = tail.as_ref() as *const Node<K, V>;
        head.next = tail_ptr.into();
        tail.prev = head_ptr.into();
        Self {
            skiplist: SkipMap::new(),
            head,
            tail,
            cap: capacity,
            size: AtomicUsize::new(0),
        }
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn remove(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let mut node = entry.value().load(Ordering::Relaxed, &guard);
        let desc = Owned::new(Descriptor::new(node.into(), smallvec![NodeOp::Remove]));
        let backoff = Backoff::new();
        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            let old_last_op = old_desc_ref.get_ops().last().unwrap();
            if let NodeOp::Remove = &old_last_op.op {
                return None;
            }

            self.run_op(old_desc_ref, &guard);

            if let NodeOp::Detach = &old_last_op.op {
                let key = node_ref
                    .key
                    .try_clone_inner(Ordering::Relaxed, &guard)
                    .unwrap()
                    .unwrap();
                let value = node_ref
                    .value
                    .try_clone_inner(Ordering::Relaxed, &guard)
                    .unwrap()
                    .unwrap();
                let PutResult { node_put, .. } = self.put_internal(key, value, &guard);
                node = node_put;
                desc.node.store(node_put, Ordering::Relaxed);
                continue;
            }
            if node_ref
                .desc
                .compare_exchange_weak(
                    old_desc,
                    Shared::from(ptr::from_ref(desc.as_ref())),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_ok()
            {
                entry.remove();
                self.size.fetch_sub(1, Ordering::Relaxed);

                let desc = desc.into_shared(&guard);
                self.run_op(unsafe { desc.deref() }, &guard);
                let value = loop {
                    if let Ok(value) = node_ref.value.try_clone_inner(Ordering::Relaxed, &guard) {
                        break value;
                    }
                }
                .unwrap();
                unsafe {
                    guard.defer_destroy(old_desc);
                    guard.defer_destroy(node);
                }
                break Some(value);
            }
            backoff.spin();
        }
    }

    pub fn pop_back(&self) -> Option<RefCounted<V>> {
        let guard = pin();
        let backoff = Backoff::new();
        let mut node = self.tail.prev.load(Ordering::Relaxed, &guard);
        loop {
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            // `Detach` means it will be inserted again at MRU position and `Remove` means it is
            // already removed. So retry with a previous node.
            if let NodeOp::Detach | NodeOp::Remove = old_desc_ref.get_ops().last().unwrap().op {
                node = node_ref.prev.load(Ordering::Relaxed, &guard);
                continue;
            }

            self.run_op(old_desc_ref, &guard);
            let desc = Owned::new(Descriptor::new(node.into(), smallvec![NodeOp::Remove]))
                .into_shared(&guard);
            if node_ref
                .desc
                .compare_exchange_weak(old_desc, desc, Ordering::SeqCst, Ordering::Relaxed, &guard)
                .is_ok()
            {
                self.size.fetch_sub(1, Ordering::Release);
                self.run_op(unsafe { desc.deref() }, &guard);
                let value = node_ref
                    .value
                    .try_clone_inner(Ordering::Relaxed, &guard)
                    .unwrap()
                    .unwrap();
                unsafe {
                    guard.defer_destroy(old_desc);
                    guard.defer_destroy(node);
                }
                break Some(value);
            }
            backoff.spin();
        }
    }

    #[inline]
    pub fn put(
        &self,
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
    ) -> Option<RefCounted<V>> {
        let guard = pin();
        let PutResult { old_value, .. } = self.put_internal(key, value, &guard);
        old_value
    }

    fn put_internal<'a>(
        &self,
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
        guard: &'a Guard,
    ) -> PutResult<'a, K, V> {
        let head = self.head.as_ref();
        let new_node = Node::new(
            key,
            value,
            head.next.clone(),
            Atomic::from(ptr::from_ref(head)),
            &guard,
        );
        let new_node = unsafe { new_node.deref() };
        let desc = new_node.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node
                .key
                .try_clone_inner(Ordering::Relaxed, &guard)
                .unwrap()
                .unwrap();
            let entry =
                self.skiplist
                    .compare_insert(key, Atomic::from(new_node as *const _), |node_ptr| {
                        let node_ref = unsafe { node_ptr.load(Ordering::Relaxed, &guard).deref() };
                        let desc = node_ref.desc.load(Ordering::Relaxed, &guard);

                        // Remove an existing skiplist entry only when the node's last op is `Detach`
                        if let NodeOp::Detach | NodeOp::Remove = unsafe { desc.deref() }
                            .get_ops()
                            .last()
                            .map(|op| &op.op)
                            .unwrap()
                        {
                            return true;
                        }
                        false
                    });

            let node = entry.value().load(Ordering::Relaxed, &guard);
            let node_ref = unsafe { node.deref() };
            let old_value = if ptr::eq(node_ref, new_node) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    self.pop_back();
                }
                self.run_op(desc_ref, &guard);
                None
            } else {
                // Another node alreay exists
                let mut old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
                let mut new_value =
                    if let NodeOp::Update { new } = &desc_ref.get_ops().last().unwrap().op {
                        new.try_clone_inner(Ordering::Relaxed, &guard)
                            .unwrap()
                            .unwrap()
                    } else {
                        continue 'outer;
                    };

                let desc: Shared<Descriptor<K, V>> = 'desc_select: loop {
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let last_old_op = old_desc_ref.get_ops().last().unwrap();
                    let new_desc = match &last_old_op.op {
                        // The node was removed, try to allocate a new node
                        NodeOp::Detach | NodeOp::Remove => {
                            continue 'outer;
                        }
                        NodeOp::Update { new } => loop {
                            if new.is_constant(&guard) {
                                break Owned::new(Descriptor::new(
                                    Atomic::from(node),
                                    smallvec![NodeOp::Detach],
                                ))
                                .into_shared(&guard);
                            }

                            if let Err(value) = last_old_op
                                .op
                                .try_change_new_value(new_value.clone(), &guard)
                            {
                                new_value = value;
                                backoff.spin();
                                continue;
                            }

                            break 'desc_select old_desc;
                        },
                        NodeOp::Attach => Owned::new(Descriptor::new(
                            Atomic::from(node),
                            smallvec![NodeOp::Attach, NodeOp::new_update(new_value.clone())],
                        ))
                        .into_shared(&guard),
                    };

                    self.run_op(old_desc_ref, &guard);

                    if let Err(err) = node_ref.desc.compare_exchange_weak(
                        old_desc,
                        new_desc,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                        &guard,
                    ) {
                        old_desc = err.current;
                    } else {
                        unsafe {
                            guard.defer_destroy(old_desc);
                        }
                        break new_desc;
                    }
                    backoff.spin();
                };

                unsafe { guard.defer_destroy(Shared::from(ptr::from_ref(new_node))) };

                let desc_ref = unsafe { desc.deref() };
                self.run_op(desc_ref, &guard);
                let value = desc_ref.get_ops().last().unwrap().extract_result(&guard);
                value
            };
            break PutResult {
                node_put: node,
                old_value,
            };
        }
    }

    pub fn get(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let node = unsafe { entry.value().load(Ordering::Relaxed, &guard).deref() };
        let mut old_desc = node.desc.load(Ordering::Relaxed, &guard);
        let old_desc_ref = unsafe { old_desc.deref() };
        let last_op = old_desc_ref.get_ops().last().unwrap();
        if let NodeOp::Remove = last_op.op {
            return None;
        }
        atomic::fence(Ordering::Acquire);

        let backoff = Backoff::new();
        let desc = loop {
            let old_desc_ref = unsafe { old_desc.deref() };
            let last_op = old_desc_ref.get_ops().last().unwrap();
            let new_desc = match &last_op.op {
                NodeOp::Remove => return None,
                NodeOp::Detach => {
                    let key = node
                        .key
                        .try_clone_inner(Ordering::Relaxed, &guard)
                        .unwrap()
                        .unwrap();
                    let value = node
                        .value
                        .try_clone_inner(Ordering::Relaxed, &guard)
                        .unwrap()
                        .unwrap();
                    self.put_internal(key, value.clone(), &guard);
                    return Some(value);
                }
                NodeOp::Update { new } => {
                    let is_last_op_finished = last_op.is_finished(&guard);
                    if !is_last_op_finished {
                        self.run_op(old_desc_ref, &guard);
                    }
                    if !is_last_op_finished
                        || ptr::eq(
                            self.head.next.load(Ordering::Relaxed, &guard).as_raw(),
                            node,
                        )
                    {
                        return Some(
                            new.try_clone_inner(Ordering::Relaxed, &guard)
                                .unwrap()
                                .unwrap(),
                        );
                    }

                    Owned::new(Descriptor::new(
                        Atomic::from(ptr::from_ref(node)),
                        smallvec![NodeOp::Detach],
                    ))
                }
                _ => unreachable!(),
            };
            if let Err(err) = node.desc.compare_exchange_weak(
                old_desc,
                Shared::from(ptr::from_ref(new_desc.as_ref())),
                Ordering::SeqCst,
                Ordering::Acquire,
                &guard,
            ) {
                old_desc = err.current;
            } else {
                let new_desc = new_desc.into_shared(&guard);
                unsafe {
                    guard.defer_destroy(old_desc);
                }
                break new_desc;
            }
            backoff.spin();
        };
        self.run_op(unsafe { desc.deref() }, &guard);
        let key = node
            .key
            .try_clone_inner(Ordering::Relaxed, &guard)
            .unwrap()
            .unwrap();
        let value = node
            .value
            .try_clone_inner(Ordering::Relaxed, &guard)
            .unwrap()
            .unwrap();
        self.put(key, value.clone());
        Some(value)
    }

    fn put_node_after_head<'a>(
        &self,
        node: &Node<K, V>,
        op_info: &NodeOpInfo<V>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> ControlFlow<Option<RefCounted<V>>> {
        let node_ptr = Shared::from(ptr::from_ref(node));
        let mut node_next = node.next.load(Ordering::Acquire, guard);

        let head = self.head.as_ref();
        let mut mru_node = head.next.load(Ordering::Acquire, guard);

        let next = loop {
            // Another thread finished our job instead.
            if op_info.is_finished(guard) {
                return ControlFlow::Break(None);
            }

            // If we load `head.next` and our operation is not done yet, that means the MRU node is
            // valid or is our node. That's because all threads must help an op of a MRU node
            // before updating `head.next`. In other words, at this point, `head.next` is already
            // replaced with `node` or isn't changed from `mru_node`.

            let new_node_next = node.next.load(Ordering::Relaxed, guard);
            if new_node_next != node_next {
                atomic::fence(Ordering::Acquire);
                let new_mru_node = head.next.load(Ordering::Relaxed, guard);
                // `head.next` is still `mru_node` and only `node.next` was updated to `mru_node`
                if new_mru_node == mru_node {
                    break new_node_next;
                }
                // `head.next` was changed to our node so `node.next` should be updated already
                if ptr::eq(new_mru_node.as_raw(), node) {
                    break new_node_next;
                }
                // `head.next` was changed to another node or our job was already done by another
                // thread. Retry.
                atomic::fence(Ordering::Acquire);
                node_next = new_node_next;
                mru_node = new_mru_node;
                continue;
            }

            // Another thread already set this node as a MRU node
            if ptr::eq(node, mru_node.as_raw()) {
                break node_next;
            }

            let mru_node_ref = unsafe { mru_node.deref() };
            if let Some(mru_desc) =
                unsafe { mru_node_ref.desc.load(Ordering::Acquire, guard).as_ref() }
            {
                self.run_op(mru_desc, guard);
            }

            if head
                .next
                .compare_exchange_weak(
                    mru_node.with_tag(0),
                    node_ptr.with_tag(0),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    guard,
                )
                .is_err()
            {
                return ControlFlow::Continue(());
            }
            break mru_node;
        };

        self.help_link(node_ptr, unsafe { next.deref() }, backoff, guard);
        ControlFlow::Break(None)
    }

    fn help_link<'a>(
        &self,
        mut prev: Shared<'a, Node<K, V>>,
        node: &'a Node<K, V>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> Shared<'a, Node<K, V>> {
        let mut last = Shared::<Node<K, V>>::null();
        loop {
            if prev.is_null() {
                abort();
            }
            let prev_ref = unsafe { prev.as_ref().unwrap() };
            let prev_next = prev_ref.next.load(Ordering::Relaxed, guard);
            if prev_next.tag() != 0 {
                // A thread who was tried to remove `prev` don't update `last`'s next pointer yet.
                // We will update it instead.
                if !last.is_null() {
                    prev_ref.mark_prev(backoff, guard);
                    unsafe { last.deref() }
                        .next
                        .compare_exchange(
                            Shared::from(ptr::from_ref(prev_ref)),
                            prev_next.with_tag(0),
                            Ordering::Release,
                            Ordering::Acquire,
                            guard,
                        )
                        .ok();
                    prev = last;
                    last = Shared::null();
                } else {
                    // Find a previous node which is not deleted.
                    prev = prev_ref.prev.load(Ordering::Relaxed, guard);
                }
                continue;
            }
            let node_prev = node.prev.load(Ordering::Relaxed, guard);
            // The node was removed by another thread. Pass responsibility of clean-up to that thread.
            if node_prev.tag() != 0 {
                return prev;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(prev_next.as_raw(), node) {
                last = prev;
                prev = prev_next;
                continue;
            }
            // Another thread already helped our job.
            if ptr::eq(node_prev.as_raw(), prev.as_raw()) {
                return prev;
            }
            let prev_next = prev_ref.next.load(Ordering::Relaxed, guard);
            if ptr::eq(prev_next.as_raw(), node)
                && node
                    .prev
                    .compare_exchange_weak(
                        node_prev,
                        prev.with_tag(0),
                        Ordering::Release,
                        Ordering::Acquire,
                        guard,
                    )
                    .is_ok()
            {
                if prev_ref.prev.load(Ordering::Relaxed, guard).tag() == 0 {
                    return prev;
                }
            }
            backoff.spin();
        }
    }

    fn help_detach<'a>(&self, node: &Node<K, V>, backoff: &Backoff, guard: &'a Guard) {
        node.mark_prev(backoff, guard);
        let mut last = Shared::<Node<K, V>>::null();
        let mut prev = node.prev.load(Ordering::Relaxed, guard);
        if prev.is_null() {
            abort()
        }
        let mut next = node.next.load(Ordering::Relaxed, guard);
        loop {
            // Not need to null check for `prev`/`next` because if they are set as head/tail
            // once, they will not go backward/forward any more. That is because head/tail
            // node will not marked as deleted.

            // `prev`'s next pointer was already changed from `node` to `next`.
            if ptr::eq(prev.as_raw(), next.as_raw()) {
                return;
            }
            let next_ref = unsafe { next.deref() };
            let next_next = next_ref.next.load(Ordering::Relaxed, guard);
            if next_next.tag() != 0 {
                next_ref.mark_prev(backoff, guard);
                next = next_next.as_raw().into();
                continue;
            }
            let prev_ref = unsafe { prev.deref() };
            let prev_next = prev_ref.next.load(Ordering::Relaxed, guard);
            // If `prev` was deleted
            if prev_next.tag() != 0 {
                // A thread who was tried to remove `prev` don't update `last`'s next pointer yet.
                // We will update it instead.
                if !last.is_null() {
                    prev_ref.mark_prev(backoff, guard);
                    unsafe { last.deref() }
                        .next
                        .compare_exchange(
                            prev.with_tag(0),
                            prev_next.with_tag(0),
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        )
                        .ok();
                    prev = last;
                    last = Shared::null();
                } else {
                    // Find a previous node which is not deleted.
                    prev = prev_ref.prev.load(Ordering::Relaxed, guard);
                    if prev.is_null() {
                        abort()
                    }
                }
                continue;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(prev_next.as_raw(), node) {
                last = prev;
                prev = prev_next;
                if prev.is_null() {
                    abort()
                }
                continue;
            }

            if prev_ref
                .next
                .compare_exchange_weak(
                    Shared::from(node as *const _),
                    next.with_tag(0),
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                return;
            }
            backoff.spin();
        }
    }

    fn detach_node<'a>(
        &self,
        node: &Node<K, V>,
        op_info: &NodeOpInfo<V>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> Option<RefCounted<V>> {
        let mut next;
        loop {
            next = node.next.load(Ordering::Acquire, guard);
            if op_info.is_finished(guard) {
                return None;
            }
            // Another thread already marked this node
            if next.tag() != 0 {
                break;
            }
            if let Err(err) = node.next.compare_exchange_weak(
                next,
                next.with_tag(1),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                // Another thread removed the next node and update `next` pointer of this node
                if err.current.tag() == 0 {
                    atomic::fence(Ordering::Acquire);
                    self.help_link(
                        Shared::from(ptr::from_ref(node)),
                        unsafe { err.current.deref() },
                        backoff,
                        guard,
                    );
                    continue;
                }
            } else {
                break;
            };
            backoff.spin();
        }

        self.help_detach(node, backoff, guard);
        let prev = node.prev.load(Ordering::Acquire, guard);
        self.help_link(prev, unsafe { next.deref() }, backoff, guard);
        node.value.clone_inner(Ordering::Relaxed, guard)
    }

    fn loop_until_succeess<'a>(
        &self,
        op: &NodeOpInfo<V>,
        guard: &'a Guard,
        backoff: &Backoff,
        func: impl Fn() -> ControlFlow<Option<RefCounted<V>>>,
    ) -> bool {
        loop {
            if op.is_finished(guard) {
                return false;
            }

            match func() {
                ControlFlow::Continue(_) => {
                    backoff.spin();
                    continue;
                }
                ControlFlow::Break(v) => {
                    return op.store_result(v, guard);
                }
            }
        }
    }

    fn run_op(&self, desc: &Descriptor<K, V>, guard: &Guard) {
        let backoff = Backoff::new();

        let node = unsafe { desc.node.load(Ordering::Relaxed, guard).deref() };
        if node.desc.load(Ordering::Relaxed, guard) != Shared::from(ptr::from_ref(desc)) {
            atomic::fence(Ordering::Acquire);
            return;
        }
        desc.run_ops(guard, |op| match &op.op {
            NodeOp::Attach => {
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.put_node_after_head(node, op, &backoff, guard)
                });
            }
            NodeOp::Update { new } => {
                loop {
                    let Ok(new_value) = new
                        .try_clone_inner(Ordering::Acquire, guard)
                        .map(|v| v.unwrap())
                    else {
                        // Someone changed value. Retry.
                        backoff.spin();
                        continue;
                    };
                    if !new.make_constant(
                        Some(&new_value),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        // Someone changed value. Retry.
                        backoff.spin();
                        continue;
                    }

                    let old_value = node.value.load(Ordering::Acquire, guard);
                    if op.is_finished(guard) {
                        return;
                    }
                    atomic::fence(Ordering::Acquire);
                    let cur_value = node.value.load(Ordering::Relaxed, guard);
                    if cur_value.map(NonNull::from) != old_value.map(NonNull::from) {
                        return;
                    }
                    if let Ok(CompareExchangeOk { old }) = node.value.compare_exchange_weak(
                        old_value,
                        Some(new_value),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        op.store_result(old, guard);
                        return;
                    }

                    // The failure of CAS above means that another thread has succeeded in changing.
                    // So we don't need to store any result.
                }
            }
            NodeOp::Detach | NodeOp::Remove => {
                if let Some(result) = self.detach_node(node, op, &backoff, guard) {
                    op.store_result(Some(result), guard);
                }
            }
        })
    }
}

struct PutResult<'a, K: Send + Sync, V: Send + Sync> {
    node_put: Shared<'a, Node<K, V>>,
    old_value: Option<RefCounted<V>>,
}

struct Node<K: Send + Sync, V: Send + Sync> {
    key: AtomicRefCounted<K>,
    value: AtomicRefCounted<V>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    desc: Atomic<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let desc = std::mem::replace(&mut self.desc, Atomic::null());
        drop(unsafe { desc.into_owned() });
    }
}

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
        next: Atomic<Node<K, V>>,
        prev: Atomic<Node<K, V>>,
        _: &'a Guard,
    ) -> Shared<'a, Self> {
        let ptr = Box::leak(
            Owned::new(Self {
                key: AtomicRefCounted::new(key.into()),
                value: AtomicRefCounted::null(),
                next,
                prev,
                desc: Atomic::null(),
            })
            .into_box(),
        );
        ptr.desc = Atomic::new(Descriptor::new(
            Atomic::from(ptr::from_ref(ptr)),
            smallvec![NodeOp::Attach, NodeOp::new_update(value.into())],
        ));
        Shared::from(ptr::from_ref(ptr))
    }

    #[inline]
    fn uninit<'a>(next: Atomic<Node<K, V>>, prev: Atomic<Node<K, V>>) -> Owned<Self> {
        Owned::new(Self {
            key: AtomicRefCounted::null(),
            value: AtomicRefCounted::null(),
            next,
            prev,
            desc: Atomic::null(),
        })
    }

    fn mark_prev(&self, backoff: &Backoff, guard: &Guard) {
        loop {
            let prev = self.prev.load(Ordering::Acquire, guard);
            if prev.tag() != 0 {
                return;
            }
            if self
                .prev
                .compare_exchange_weak(
                    prev,
                    prev.with_tag(1),
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                return;
            }
            backoff.spin();
        }
    }
}

struct Descriptor<K: Send + Sync, V: Send + Sync> {
    node: Atomic<Node<K, V>>,
    ops: SmallVec<[NodeOpInfo<V>; 2]>,
}

impl<K: Send + Sync, V: Send + Sync> Descriptor<K, V> {
    fn new(node: Atomic<Node<K, V>>, ops: SmallVec<[NodeOp<V>; 2]>) -> Self {
        Self {
            node,
            ops: ops
                .into_iter()
                .map(|op| NodeOpInfo {
                    result: Atomic::null(),
                    op,
                })
                .collect(),
        }
    }

    #[inline]
    fn get_ops(&self) -> &[NodeOpInfo<V>] {
        self.ops.as_slice()
    }

    #[inline]
    fn run_ops(&self, guard: &Guard, f: impl Fn(&NodeOpInfo<V>)) {
        let ops = self.get_ops();
        let num_ops = ops
            .iter()
            .rev()
            .take_while(|op| !op.is_finished(guard))
            .count();
        for op in &ops[(ops.len() - num_ops)..] {
            f(&op);
        }
    }
}

// TODO: Make it linked list
#[derive(Debug)]
struct NodeOpInfo<V: Send + Sync> {
    result: Atomic<V>,
    op: NodeOp<V>,
}

impl<V: Send + Sync> Drop for NodeOpInfo<V> {
    fn drop(&mut self) {
        let fake_guard = unsafe { crossbeam::epoch::unprotected() };
        if let Some(inner) = unsafe {
            self.result
                .swap(Shared::null(), Ordering::Acquire, fake_guard)
                .as_ref()
        } {
            drop(unsafe { RefCounted::from_raw(inner.into()) })
        }
    }
}

impl<V: Send + Sync> NodeOpInfo<V> {
    #[inline]
    fn is_finished(&self, guard: &Guard) -> bool {
        let is_finished = self.result.load(Ordering::Relaxed, guard).tag() != 0;
        if is_finished {
            atomic::fence(Ordering::Acquire);
        }
        is_finished
    }

    #[inline]
    fn extract_result(&self, guard: &Guard) -> Option<RefCounted<V>> {
        let old_value = self.result.load(Ordering::Relaxed, guard);
        if old_value.tag() == 0 {
            None
        } else {
            let result = self
                .result
                .swap(Shared::null().with_tag(1), Ordering::AcqRel, guard);
            unsafe {
                result
                    .as_ref()
                    .map(|value| RefCounted::from_raw(value.into()))
            }
        }
    }

    #[inline]
    fn store_result<'a>(&self, value: Option<RefCounted<V>>, guard: &'a Guard) -> bool {
        let old_value = self.result.load(Ordering::Relaxed, guard);
        if old_value.tag() != 0 {
            atomic::fence(Ordering::Acquire);
            return false;
        }
        let value = value
            .map(|v| Shared::from(v.into_raw().as_ptr().cast_const()))
            .unwrap_or(Shared::null());
        let value = value.with_tag(1);
        self.result
            .compare_exchange(
                old_value,
                value,
                Ordering::Release,
                Ordering::Acquire,
                guard,
            )
            .is_ok()
    }
}

#[derive(Debug)]
enum NodeOp<V: Send + Sync> {
    /// The node is removed.
    Remove,
    /// The node is removed but another node with the same key-value will be inserted.
    Detach,
    Attach,
    Update {
        new: AtomicRefCounted<V>,
    },
}

impl<V: Send + Sync> NodeOp<V> {
    fn new_update(new_value: RefCounted<V>) -> Self {
        Self::Update {
            new: AtomicRefCounted::new(new_value),
        }
    }

    fn try_change_new_value(
        &self,
        new_value: RefCounted<V>,
        guard: &Guard,
    ) -> Result<(), RefCounted<V>> {
        if let NodeOp::Update { new } = self {
            let old_value = new.load(Ordering::Acquire, guard);
            if new.is_constant(guard) {
                return Err(new_value);
            }

            new.compare_exchange(
                old_value,
                Some(new_value),
                Ordering::SeqCst,
                Ordering::Relaxed,
                guard,
            )
            .map(|_| ())
            .map_err(|err| err.new.unwrap())
        } else {
            Err(new_value)
        }
    }
}
