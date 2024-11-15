use std::{
    borrow::Borrow,
    mem::ManuallyDrop,
    num::NonZeroUsize,
    ops::{ControlFlow, Deref},
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
        let node = entry.value().load(Ordering::Relaxed, &guard);
        let node_ref = unsafe { node.deref() };
        let mut old_desc = node_ref
            .desc
            .clone_inner(Ordering::Relaxed, &guard)
            .unwrap();
        let desc = RefCounted::new(Descriptor::new(node.into(), vec![NodeOp::Detach]));
        let mut new_desc = desc.clone();
        let backoff = Backoff::new();
        loop {
            if let NodeOp::Detach = old_desc.get_ops().last().map(|op| &op.op).unwrap() {
                return None;
            }

            self.run_op(&old_desc, &guard);
            if let Err(err) = node_ref.desc.compare_exchange_weak(
                Some(&old_desc),
                Some(new_desc),
                Ordering::SeqCst,
                Ordering::SeqCst,
                &guard,
            ) {
                old_desc = if let CompareExchangeErrCuurentValue::Cloned(v) = err.current.unwrap() {
                    v
                } else {
                    node_ref
                        .desc
                        .clone_inner(Ordering::Relaxed, &guard)
                        .unwrap()
                };
                new_desc = err.new.unwrap();
            } else {
                entry.remove();
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.run_op(&desc, &guard);
                let value = loop {
                    if let Ok(value) = node_ref.value.try_clone_inner(Ordering::Relaxed, &guard) {
                        break value;
                    }
                }
                .unwrap();
                unsafe {
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
        loop {
            let node = self.tail.prev.load(Ordering::Relaxed, &guard);
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref
                .desc
                .clone_inner(Ordering::Relaxed, &guard)
                .unwrap();
            self.run_op(&old_desc, &guard);
            if let NodeOp::Detach = old_desc.get_ops().last().unwrap().op {
                continue;
            }

            let desc = RefCounted::new(Descriptor::new(node.into(), vec![NodeOp::Detach]));
            let new_desc = desc.clone();
            if node_ref
                .desc
                .compare_exchange_weak(
                    Some(&old_desc),
                    Some(new_desc),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_ok()
            {
                self.skiplist
                    .remove(node_ref.key.load(Ordering::Relaxed, &guard).unwrap());
                self.size.fetch_sub(1, Ordering::Release);
                self.run_op(&desc, &guard);
                let value = node_ref
                    .value
                    .clone_inner(Ordering::Relaxed, &guard)
                    .unwrap();
                unsafe {
                    guard.defer_destroy(node);
                }
                break Some(value);
            }
            backoff.spin();
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<RefCounted<V>> {
        let guard = pin();
        let head = self.head.as_ref();
        let new_node = Node::new(
            key,
            value,
            head.next.clone(),
            Atomic::from(ptr::from_ref(head)),
            &guard,
        );
        let new_node = unsafe { new_node.deref() };
        let desc = new_node
            .desc
            .try_clone_inner(Ordering::Relaxed, &guard)
            .unwrap()
            .unwrap();
        let new_value = if let NodeOp::Update { new } = &desc.get_ops().last().unwrap().op {
            new.clone()
        } else {
            unreachable!()
        };
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
                        let desc = node_ref.desc.load(Ordering::Acquire, &guard).unwrap();

                        // Remove an existing skiplist entry only when the node's last op is `Detach`
                        if let NodeOp::Detach = desc.get_ops().last().map(|op| &op.op).unwrap() {
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
                self.run_op(&desc, &guard);
                None
            } else {
                // Another node alreay exists
                let mut del_desc = RefCounted::new(Descriptor::new(
                    Atomic::from(ptr::from_ref(node_ref)),
                    vec![NodeOp::Detach],
                ));
                let old_value = node_ref.value.load(Ordering::Acquire, &guard);
                let mut old_desc = node_ref
                    .desc
                    .clone_inner(Ordering::Relaxed, &guard)
                    .unwrap();
                atomic::fence(Ordering::Acquire);

                let desc: RefCounted<Descriptor<K, V>> = loop {
                    let last_old_op = old_desc.get_ops().last().unwrap();
                    match &last_old_op.op {
                        // The node was removed, try to allocate a new node
                        NodeOp::Detach => {
                            backoff.spin();
                            continue 'outer;
                        }
                        NodeOp::Update { new } => {
                            // TODO: Chain our update op to the last update op
                            if !last_old_op.is_finished(&guard) {
                                let cur_value =
                                    node_ref.value.load(Ordering::Relaxed, &guard).unwrap();
                                if ptr::eq(old_value, cur_value) {}
                            }
                            if old_value == new_value.deref() {}
                        }
                        _ => unreachable!()
                    }

                    self.run_op(&old_desc, &guard);

                    if let Err(err) = node_ref.desc.compare_exchange_weak(
                        Some(&old_desc),
                        // SAFETY: SyncUnsafeCell has the same memory layout with inner type.
                        Some(del_desc),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    ) {
                        del_desc = err.new.unwrap();
                        old_desc = if let CompareExchangeErrCuurentValue::Cloned(v) =
                            err.current.unwrap()
                        {
                            v
                        } else {
                            node_ref
                                .desc
                                .clone_inner(Ordering::Acquire, &guard)
                                .unwrap()
                        };
                    } else {
                        break desc;
                    }
                    backoff.spin();
                };
                self.run_op(&desc, &guard);
                let value = desc.get_ops().last().unwrap().extract_result(&guard);
                value
            };
            break old_value;
        }
    }

    pub fn get(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let node = unsafe { entry.value().load(Ordering::Relaxed, &guard).deref() };
        let desc = node.desc.clone_inner(Ordering::Acquire, &guard).unwrap();
        if let Some(NodeOpInfo {
            op: NodeOp::Detach, ..
        }) = desc.get_ops().last()
        {
            return None;
        }

        self.run_op(&desc, &guard);
        let mru = self.head.next.load(Ordering::Relaxed, &guard);
        let get_value_fn = || node.value.clone_inner(Ordering::Acquire, &guard);
        if ptr::eq(mru.as_raw(), node) {
            get_value_fn()
        } else {
            let mut old_desc = desc;
            let new_desc = RefCounted::new(Descriptor::new(
                Atomic::from(ptr::from_ref(node)),
                vec![NodeOp::Detach, NodeOp::Attach],
            ));
            let backoff = Backoff::new();
            loop {
                if let Err(err) = node.desc.compare_exchange_weak(
                    Some(&old_desc),
                    Some(new_desc.clone()),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                    &guard,
                ) {
                    old_desc =
                        if let CompareExchangeErrCuurentValue::Cloned(v) = err.current.unwrap() {
                            v
                        } else {
                            node.desc.clone_inner(Ordering::Acquire, &guard).unwrap()
                        };
                    if let NodeOp::Detach = old_desc.get_ops().last().map(|op| &op.op).unwrap() {
                        return None;
                    }
                    backoff.spin();
                    self.run_op(&old_desc, &guard);
                } else {
                    let value = get_value_fn();
                    self.run_op(&new_desc, &guard);
                    break value;
                }
            }
        }
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
            if let Some(mru_desc) = mru_node_ref.desc.clone_inner(Ordering::Acquire, guard) {
                self.run_op(&mru_desc, guard);
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

    fn run_op(&self, desc: &RefCounted<Descriptor<K, V>>, guard: &Guard) {
        let backoff = Backoff::new();

        let node = unsafe { desc.node.load(Ordering::Relaxed, guard).deref() };
        desc.run_ops(guard, |op| match &op.op {
            NodeOp::Attach => {
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.put_node_after_head(node, op, &backoff, guard)
                });
            }
            NodeOp::Update { new } => {
                loop {
                    let old_value = node.value.load(Ordering::Relaxed, guard);
                    if op.is_finished(guard) {
                        return;
                    }
                    let cur_value = node.value.load(Ordering::Relaxed, guard);
                    if cur_value.map(NonNull::from) != old_value.map(NonNull::from) {
                        return;
                    }
                    let new = new.clone();
                    if let Ok(CompareExchangeOk { old }) = node.value.compare_exchange_weak(
                        old_value,
                        Some(new),
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
            NodeOp::Detach => {
                if let Some(result) = self.detach_node(node, op, &backoff, guard) {
                    op.store_result(Some(result), guard);
                }
            }
        });
    }
}

struct Node<K: Send + Sync, V: Send + Sync> {
    key: AtomicRefCounted<K>,
    value: AtomicRefCounted<V>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    desc: AtomicRefCounted<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let key = std::mem::replace(&mut self.key, AtomicRefCounted::null());
        let value = std::mem::replace(&mut self.value, AtomicRefCounted::null());
        let desc = std::mem::replace(&mut self.desc, AtomicRefCounted::null());
        let fake_guard = unsafe { crossbeam::epoch::unprotected() };
        key.finalize(fake_guard);
        value.finalize(fake_guard);
        desc.finalize(fake_guard);
    }
}

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(
        key: K,
        value: V,
        next: Atomic<Node<K, V>>,
        prev: Atomic<Node<K, V>>,
        _: &'a Guard,
    ) -> Shared<'a, Self> {
        let ptr = Box::leak(
            Owned::new(Self {
                key: AtomicRefCounted::new(RefCounted::new(key)),
                value: AtomicRefCounted::null(),
                next,
                prev,
                desc: AtomicRefCounted::null(),
            })
            .into_box(),
        );
        ptr.desc = AtomicRefCounted::new(RefCounted::new(Descriptor::new(
            Atomic::from(ptr::from_ref(ptr)),
            vec![
                NodeOp::Attach,
                NodeOp::Update {
                    new: RefCounted::new(value),
                },
            ],
        )));
        Shared::from(ptr::from_ref(ptr))
    }

    #[inline]
    fn uninit<'a>(next: Atomic<Node<K, V>>, prev: Atomic<Node<K, V>>) -> Owned<Self> {
        Owned::new(Self {
            key: AtomicRefCounted::null(),
            value: AtomicRefCounted::null(),
            next,
            prev,
            desc: AtomicRefCounted::null(),
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
    ops: Vec<NodeOpInfo<V>>,
}

impl<K: Send + Sync, V: Send + Sync> Descriptor<K, V> {
    fn new(node: Atomic<Node<K, V>>, ops: Vec<NodeOp<V>>) -> Self {
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
            // Need acquire ordering to see results of previous ops
            .take_while(|op| !op.is_finished(guard))
            .count();
        for op in &ops[(ops.len() - num_ops)..] {
            f(&op);
        }
    }
}

// TODO: Make it linked list
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
enum NodeOp<V: Send + Sync> {
    Detach,
    Attach,
    Update { new: RefCounted<V> },
}

const MAX_REF_COUNT: usize = usize::MAX >> 10;

#[repr(transparent)]
#[derive(Debug)]
pub struct RefCounted<T: Send + Sync> {
    inner: NonNull<RefCountedInner<T>>,
}

impl<T: Send + Sync> Clone for RefCounted<T> {
    fn clone(&self) -> Self {
        let inner = unsafe { self.inner.as_ref() };
        inner.try_increment();
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr::from_ref(inner).cast_mut()) },
        }
    }
}

impl<T: Send + Sync> Drop for RefCounted<T> {
    fn drop(&mut self) {
        unsafe {
            self.inner.as_ref().decrement();
        }
    }
}

unsafe impl<T: Send + Sync> Send for RefCounted<T> {}
unsafe impl<T: Send + Sync> Sync for RefCounted<T> {}

impl<T: Send + Sync> RefCounted<T> {
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            inner: unsafe {
                NonNull::new_unchecked(Box::into_raw(
                    Owned::new(RefCountedInner::new(data)).into_box(),
                ))
            },
        }
    }

    pub fn into_inner(this: Self) -> Option<T> {
        let this = ManuallyDrop::new(this);
        let inner_ptr = this.inner.as_ptr();
        if unsafe { &*inner_ptr }
            .ref_count
            .fetch_sub(1, Ordering::Release)
            != 1
        {
            return None;
        }
        atomic::fence(Ordering::Acquire);
        let guard = pin();

        let inner = unsafe { ptr::read(inner_ptr) };
        unsafe {
            guard.defer_destroy(Shared::from(
                inner_ptr
                    .cast_const()
                    .cast::<ManuallyDrop<RefCountedInner<T>>>(),
            ));
        }
        Some(inner.data)
    }

    #[inline]
    pub fn into_raw(self) -> NonNull<T> {
        self.into_inner_raw().cast()
    }

    #[inline]
    pub unsafe fn from_raw(this: NonNull<T>) -> Self {
        Self { inner: this.cast() }
    }

    #[inline]
    pub fn as_ptr(&self) -> NonNull<T> {
        self.inner.cast()
    }

    #[inline]
    fn into_inner_raw(self) -> NonNull<RefCountedInner<T>> {
        let ptr = self.inner;
        std::mem::forget(self);
        ptr
    }

    #[inline]
    unsafe fn from_inner_raw(this: NonNull<RefCountedInner<T>>) -> Self {
        Self { inner: this }
    }
}

impl<T: Send + Sync> Deref for RefCounted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &self.inner.as_ref().data }
    }
}

impl<T: Send + Sync> Borrow<T> for RefCounted<T> {
    fn borrow(&self) -> &T {
        self.deref()
    }
}

impl<T: Send + Sync + Eq> Eq for RefCounted<T> {}

impl<T: Send + Sync + PartialEq> PartialEq for RefCounted<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<T: Send + Sync + PartialOrd> PartialOrd for RefCounted<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl<T: Send + Sync + Ord> Ord for RefCounted<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other.deref())
    }
}

#[repr(transparent)]
struct AtomicRefCounted<T: Send + Sync> {
    inner: Atomic<RefCountedInner<T>>,
}

unsafe impl<T: Send + Sync> Send for AtomicRefCounted<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicRefCounted<T> {}

impl<T: Send + Sync> AtomicRefCounted<T> {
    #[inline]
    fn new(data: RefCounted<T>) -> Self {
        Self {
            inner: Atomic::from(data.into_inner_raw().as_ptr().cast_const().cast()),
        }
    }

    #[inline]
    fn null() -> Self {
        Self {
            inner: Atomic::null(),
        }
    }

    /// This keeps trying to clone `RefCounted` until success. Because
    #[inline]
    fn load<'a>(&self, order: Ordering, guard: &'a Guard) -> Option<&'a T> {
        let inner = self.inner.load(order, guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            Some(&inner.data)
        } else {
            None
        }
    }

    #[inline]
    fn clone_inner(&self, order: Ordering, guard: &Guard) -> Option<RefCounted<T>> {
        loop {
            if let Ok(cloned) = self.try_clone_inner(order, guard) {
                break cloned;
            }
        }
    }

    #[inline]
    fn try_clone_inner(&self, order: Ordering, guard: &Guard) -> Result<Option<RefCounted<T>>, ()> {
        let inner = self.inner.load(order, guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            if inner.try_increment() {
                Ok(Some(unsafe { RefCounted::from_inner_raw(inner.into()) }))
            } else {
                Err(())
            }
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn compare_exchange_common<'a, F>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        func: F,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>>
    where
        T: 'a,
        F: FnOnce(
            &Atomic<RefCountedInner<T>>,
            Shared<'a, RefCountedInner<T>>,
            Shared<'a, RefCountedInner<T>>,
            Ordering,
            Ordering,
            &'a Guard,
        ) -> Result<
            Shared<'a, RefCountedInner<T>>,
            crossbeam::epoch::CompareExchangeError<
                'a,
                RefCountedInner<T>,
                Shared<'a, RefCountedInner<T>>,
            >,
        >,
    {
        let current = if let Some(current) = current {
            Shared::from(ptr::from_ref(current).cast::<RefCountedInner<T>>())
        } else {
            Shared::null()
        };
        let new_ptr = if let Some(new) = &new {
            Shared::from(new.inner.as_ptr().cast_const())
        } else {
            Shared::null()
        };
        match func(&self.inner, current, new_ptr, success, failure, guard) {
            Ok(_) => Ok({
                std::mem::forget(new);
                CompareExchangeOk {
                    old: unsafe { current.as_ref() }.map(|v| {
                        // SAFETY: Only we can get the old `RefCountedInner` pointer created by
                        // `RefCounted::into_inner_raw`.
                        unsafe { RefCounted::from_inner_raw(v.into()) }
                    }),
                }
            }),
            // The `old` pointer might be queued for deallocation if another thread already took it
            // from `self` by another compare-exchange call. But we have `Guard` here we can assume
            // the pointer is valid until the `Guard` is dropped.
            Err(err) => Err(CompareExchangeErr {
                current: unsafe {
                    err.current.as_ref().map(|inner| {
                        if inner.try_increment() {
                            CompareExchangeErrCuurentValue::Cloned(RefCounted::from_inner_raw(
                                inner.into(),
                            ))
                        } else {
                            CompareExchangeErrCuurentValue::Removed(Shared::from(
                                ptr::from_ref(inner).cast::<T>(),
                            ))
                        }
                    })
                },
                new,
            }),
        }
    }

    #[inline]
    fn compare_exchange_weak<'a>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange_weak::<Shared<_>>,
            guard,
        )
    }

    #[inline]
    fn compare_exchange<'a>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange::<Shared<_>>,
            guard,
        )
    }

    fn finalize(self, guard: &Guard) {
        if let Some(inner) = unsafe { self.inner.load(Ordering::Acquire, guard).as_ref() } {
            inner.decrement();
        }
    }
}

struct CompareExchangeOk<T: Send + Sync> {
    old: Option<RefCounted<T>>,
}
enum CompareExchangeErrCuurentValue<'a, T: Send + Sync> {
    Removed(Shared<'a, T>),
    Cloned(RefCounted<T>),
}

struct CompareExchangeErr<'a, T: Send + Sync> {
    current: Option<CompareExchangeErrCuurentValue<'a, T>>,
    new: Option<RefCounted<T>>,
}

#[repr(C)]
struct RefCountedInner<T: Send + Sync> {
    data: T,
    ref_count: AtomicUsize,
}

impl<T: Send + Sync> RefCountedInner<T> {
    fn new(data: T) -> Self {
        Self {
            ref_count: AtomicUsize::new(1),
            data,
        }
    }

    #[inline]
    fn try_increment(&self) -> bool {
        let mut old_count = self.ref_count.load(Ordering::Relaxed);

        loop {
            if old_count == 0 {
                return false;
            }
            if old_count >= MAX_REF_COUNT {
                abort()
            }

            if let Err(current) = self.ref_count.compare_exchange_weak(
                old_count,
                old_count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                old_count = current;
            } else {
                return true;
            };
        }
    }

    #[inline]
    fn decrement(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            let guard = pin();
            // This line should be called once because otherwise `Self::finalize` might be called
            // with `self` pointer which was already deallocated by another thread
            let this = ptr::from_ref(self);
            unsafe { guard.defer_unchecked(move || Self::finalize(this)) };
        }
    }

    #[cold]
    unsafe fn finalize(this: *const Self) {
        drop(Owned::from_raw(this.cast_mut()))
    }
}
