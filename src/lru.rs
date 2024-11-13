use std::{
    borrow::Borrow,
    cell::UnsafeCell,
    mem::{ManuallyDrop, MaybeUninit},
    num::NonZeroUsize,
    ops::{ControlFlow, Deref},
    process::abort,
    ptr::{self, NonNull},
    sync::atomic::{self, AtomicUsize, Ordering},
};

use crossbeam::{
    epoch::{pin, Atomic, Guard, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;

// fn collector() -> &'static Collector {
//     static COLLECTOR: OnceLock<Collector> = OnceLock::new();
//     COLLECTOR.get_or_init(|| Collector::new())
// }
//
// fn pin() -> Guard {
//     HANDLE.with(|handle| handle.pin())
// }
//
// thread_local! {
//     static HANDLE: LocalHandle = collector().register();
// }

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
        let mut old_desc = node_ref.desc.load(Ordering::Acquire, &guard).unwrap();
        let desc = RefCounted::new(Descriptor::new(node.into(), vec![NodeOp::Detach]));
        let mut new_desc = desc.clone();
        let backoff = Backoff::new();
        loop {
            if let NodeOp::Detach = old_desc.get_ops(&guard).last().map(|op| &op.op).unwrap() {
                return None;
            }

            self.run_op(old_desc.deref(), &guard);
            if let Err(err) = node_ref.desc.compare_exchange_weak(
                Some(&old_desc),
                Some(new_desc),
                Ordering::SeqCst,
                Ordering::SeqCst,
                &guard,
            ) {
                old_desc = err.current.unwrap();
                new_desc = err.new.unwrap();
            } else {
                entry.remove();
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.run_op(desc.deref(), &guard);
                let value = node_ref.value.load(Ordering::Relaxed, &guard).unwrap();
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
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard).unwrap();
            self.run_op(old_desc.deref(), &guard);
            if let NodeOp::Detach = old_desc.get_ops(&guard).last().unwrap().op {
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
                self.skiplist.remove(
                    node_ref
                        .key
                        .load(Ordering::Relaxed, &guard)
                        .unwrap()
                        .deref(),
                );
                self.size.fetch_sub(1, Ordering::Release);
                self.run_op(desc.deref(), &guard);
                let value = node_ref.value.load(Ordering::Relaxed, &guard).unwrap();
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
            Atomic::from(head as *const _),
            vec![
                // NodeOp::ChangeHeadDesc,
                NodeOp::Attach,
            ],
            &guard,
        );
        let new_node = unsafe { new_node.deref() };
        let desc = new_node.desc.load(Ordering::Relaxed, &guard).unwrap();
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node.key.load(Ordering::Relaxed, &guard).unwrap();
            let entry =
                self.skiplist
                    .compare_insert(key, Atomic::from(new_node as *const _), |node_ptr| {
                        let node_ref = unsafe { node_ptr.load(Ordering::Relaxed, &guard).deref() };
                        let desc = node_ref.desc.load(Ordering::Relaxed, &guard).unwrap();

                        // Remove an existing skiplist entry only when the node's last op is `Detach`
                        if let NodeOp::Detach =
                            desc.get_ops(&guard).last().map(|op| &op.op).unwrap()
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
                self.run_op(&*desc, &guard);
                None
            } else {
                // Another node alreay exists

                let mut old_value = node_ref.value.load(Ordering::Relaxed, &guard).unwrap();
                let new_value = new_node.value.load(Ordering::Relaxed, &guard).unwrap();
                let mut desc = RefCounted::new(SyncUnsafeCell::new(Descriptor::new(
                    Atomic::from(node_ref as *const _),
                    vec![
                        NodeOp::Detach,
                        // NodeOp::ChangeHeadDesc,
                        NodeOp::Attach,
                        NodeOp::Update {
                            old: old_value.clone(),
                            new: new_value,
                        },
                    ],
                )));
                // Need `acquire` ordering to read value of OP properly
                let mut old_desc = node_ref.desc.load(Ordering::Acquire, &guard).unwrap();
                let desc: RefCounted<Descriptor<K, V>> = loop {
                    self.run_op(&*old_desc, &guard);

                    let last_old_ops = old_desc.get_ops(&guard).last().map(|op| &op.op).unwrap();
                    match last_old_ops {
                        // The node was removed, try to allocate a new node
                        NodeOp::Detach => {
                            backoff.spin();
                            continue 'outer;
                        }
                        NodeOp::Update { new, .. } => {
                            old_value = new.clone();
                        }
                        _ => {}
                    }

                    // Update the expected value
                    // SAFETY: `desc` can be seen only in this thread for now.
                    match unsafe { &mut *desc.get() }
                        .get_ops_mut(&guard)
                        .last_mut()
                        .map(|op| &mut op.op)
                        .unwrap()
                    {
                        NodeOp::Update { old, .. } => {
                            *old = old_value.clone();
                        }
                        _ => unreachable!(),
                    }

                    if let Err(err) = node_ref.desc.compare_exchange_weak(
                        Some(&old_desc),
                        // SAFETY: SyncUnsafeCell has the same memory layout with inner type.
                        Some(unsafe {
                            RefCounted::from_inner_raw(desc.clone().into_inner_raw().cast())
                        }),
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        &guard,
                    ) {
                        old_desc = err.current.unwrap();
                        desc = unsafe {
                            RefCounted::from_inner_raw(err.new.unwrap().into_inner_raw().cast())
                        };
                    } else {
                        break unsafe { RefCounted::from_inner_raw(desc.into_inner_raw().cast()) };
                    }
                    backoff.spin();
                };
                self.run_op(&*desc, &guard);
                let result = desc
                    .get_ops(&guard)
                    .last()
                    .unwrap()
                    .result
                    .load(Ordering::Relaxed, &guard);
                if result.is_null() {
                    None
                } else {
                    Some(unsafe { RefCounted::from_raw(result.as_raw().cast()) })
                }
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
        let desc = node.desc.load(Ordering::Acquire, &guard).unwrap();
        if let Some(NodeOpInfo {
            op: NodeOp::Detach, ..
        }) = desc.get_ops(&guard).last()
        {
            return None;
        }

        self.run_op(&*desc, &guard);
        let mru = self.head.next.load(Ordering::Relaxed, &guard);
        let get_value_fn = || node.value.load(Ordering::Relaxed, &guard).clone();
        if ptr::eq(mru.as_raw(), node) {
            get_value_fn()
        } else {
            let mut old_desc = desc;
            let new_desc = RefCounted::new(Descriptor::new(
                Atomic::from(node as *const _),
                vec![
                    NodeOp::Detach,
                    // NodeOp::ChangeHeadDesc,
                    NodeOp::Attach,
                ],
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
                    old_desc = err.current.unwrap();
                    if let NodeOp::Detach =
                        old_desc.get_ops(&guard).last().map(|op| &op.op).unwrap()
                    {
                        return None;
                    }
                    backoff.spin();
                    self.run_op(&*old_desc, &guard);
                } else {
                    let value = get_value_fn();
                    self.run_op(&*new_desc, &guard);
                    break value;
                }
            }
        }
    }

    // Set head's OP same as mine
    // fn change_head_op<'a>(
    //     &self,
    //     mut desc: RefCounted<Descriptor<K, V>>,
    //     op_info: &NodeOpInfo<V>,
    //     guard: &'a Guard,
    // ) -> Option<Shared<'a, Node<K, V>>> {
    //     let backoff = Backoff::new();
    //     let head = self.head.as_ref();
    //     let mut old_head_desc = head.desc.load(Ordering::Acquire, guard);
    //     loop {
    //         if op_info.result.load(Ordering::Relaxed, guard).tag() != 0 {
    //             return None;
    //         }
    //
    //         if let Some(head_desc) = old_head_desc.as_ref() {
    //             if ptr::eq(head_desc.as_ptr(), desc.as_ptr()) {
    //                 // If `next` ptr has a newer value than we expect, the tag for the result of this
    //                 // op should be 1 because the thread wrote the newer `next` must have seen the
    //                 // result.
    //                 let next = head.next.load(Ordering::Acquire, guard);
    //                 if op_info.result.load(Ordering::Relaxed, guard).tag() == 0 {
    //                     return Some(next);
    //                 }
    //                 return None;
    //             }
    //
    //             self.run_op(head_desc, guard);
    //         }
    //
    //         let next = head.next.load(Ordering::Relaxed, guard);
    //         if let Err(err) = head.desc.compare_exchange_weak(
    //             &old_head_desc,
    //             desc,
    //             Ordering::SeqCst,
    //             Ordering::Acquire,
    //             &guard,
    //         ) {
    //             desc = err.new;
    //             old_head_desc = err
    //                 .current
    //                 .try_promote()
    //                 .unwrap_or_else(|| head.clone_desc());
    //         } else {
    //             return Some(next);
    //         }
    //         backoff.spin();
    //     }
    // }

    fn put_node_after_head<'a>(
        &self,
        node: &Node<K, V>,
        op_info: &NodeOpInfo<V>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> ControlFlow<Option<RefCounted<V>>> {
        let node_ptr = Shared::from(ptr::from_ref(node));

        let head = self.head.as_ref();
        let mut mru_node = head.next.load(Ordering::Acquire, guard);
        let mut node_next = node.next.load(Ordering::Relaxed, guard);
        let mut node_prev = node.prev.load(Ordering::Relaxed, guard);

        let next = loop {
            // Another thread finished our job instead.
            if op_info.result.load(Ordering::Relaxed, guard).tag() != 0 {
                return ControlFlow::Break(None);
            }

            // If we load `head.next` and our operation is not done yet, that means the MRU node is
            // valid or is our node. That's because all threads must help an op of a MRU node
            // before updating `head.next`. In other words, at this point, `head.next` is already
            // replaced with `node` or isn't changed from `mru_node`.

            if node_prev.tag() != 0 {
                if let Err(err) = node.prev.compare_exchange_weak(
                    node_prev,
                    Shared::from(ptr::from_ref(head)),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    guard,
                ) {
                    node_prev = err.current;
                } else {
                    node_prev = Shared::from(ptr::from_ref(head));
                }
            }

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
            if let Some(mru_desc) = mru_node_ref.desc.load(Ordering::Acquire, guard) {
                self.run_op(&*mru_desc, guard);
            }

            if !ptr::eq(node_next.as_raw(), mru_node_ref) {
                if node
                    .next
                    .compare_exchange_weak(
                        node_next,
                        mru_node,
                        Ordering::Release,
                        Ordering::Acquire,
                        guard,
                    )
                    .is_err()
                {
                    return ControlFlow::Continue(());
                }
            }

            if head
                .next
                .compare_exchange_weak(
                    mru_node,
                    node_ptr,
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
                            Shared::from(prev_ref as *const _),
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
                        prev,
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
        assert!(!prev.is_null());
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
                    assert!(!prev.is_null());
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
                    next,
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
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> ControlFlow<Option<RefCounted<V>>> {
        let mut next = node.next.load(Ordering::Relaxed, guard);
        loop {
            if next.tag() == 0 {
                let new_next = next.with_tag(1);
                if let Err(err) = node.next.compare_exchange_weak(
                    next,
                    new_next,
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    // Conflict with other removal thread
                    if err.current.tag() != 0 {
                        return ControlFlow::Continue(());
                    }
                    // Another thread removed the next node and update `next` pointer of this node
                    else {
                        atomic::fence(Ordering::Acquire);
                        self.help_link(
                            Shared::from(node as *const _),
                            unsafe { err.current.deref() },
                            backoff,
                            guard,
                        );
                        next = err.current;
                        continue;
                    }
                };
            }
            break;
        }

        self.help_detach(node, backoff, guard);
        let prev = node.prev.load(Ordering::Acquire, guard);
        self.help_link(prev, unsafe { next.deref() }, backoff, guard);
        ControlFlow::Break(node.value.load(Ordering::Relaxed, guard))
    }

    fn loop_until_succeess<'a>(
        &self,
        op: &NodeOpInfo<V>,
        guard: &'a Guard,
        backoff: &Backoff,
        func: impl Fn() -> ControlFlow<Option<RefCounted<V>>>,
    ) -> bool {
        loop {
            let result = op.result.load(Ordering::Acquire, guard);
            if result.tag() != 0 {
                return false;
            }

            match func() {
                ControlFlow::Continue(_) => {
                    backoff.spin();
                    continue;
                }
                ControlFlow::Break(Some(v)) => {
                    return op.store_result(Shared::from(RefCounted::into_raw(v)), guard);
                }
                _ => {
                    return op.store_result(Shared::null(), guard);
                }
            }
        }
    }

    fn run_op(&self, desc: &Descriptor<K, V>, guard: &Guard) {
        let backoff = Backoff::new();

        let node = unsafe { desc.node.load(Ordering::Relaxed, guard).deref() };
        desc.run_ops(guard, |op| match &op.op {
            NodeOp::Attach => {
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.put_node_after_head(node, op, &backoff, guard)
                });
            }
            // NodeOp::ChangeHeadDesc => {
            //     if let Some(next_node) = self.change_head_op(desc, op, guard) {
            //         op.store_result(next_node, guard);
            //     }
            // }
            NodeOp::Update { old, new } => {
                if let Ok(CompareExchangeOk { old }) = node.value.compare_exchange(
                    Some(old),
                    Some(new.clone()),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    guard,
                ) {
                    op.store_result(Shared::from(RefCounted::into_raw(old.unwrap())), guard);
                }
                // The failure of CAS above means that another thread has succeeded in changing.
                // So we don't need to store any result.
            }
            NodeOp::Detach => {
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.detach_node(node, &backoff, guard)
                });
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
        key.finalize();
        value.finalize();
        desc.finalize();
    }
}

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(
        key: K,
        value: V,
        next: Atomic<Node<K, V>>,
        prev: Atomic<Node<K, V>>,
        ops: Vec<NodeOp<V>>,
        _: &'a Guard,
    ) -> Shared<'a, Self> {
        let ptr = Box::leak(
            Owned::new(Self {
                key: AtomicRefCounted::new(RefCounted::new(key)),
                value: AtomicRefCounted::new(RefCounted::new(value)),
                next,
                prev,
                desc: AtomicRefCounted::null(),
            })
            .into_box(),
        );
        ptr.desc = AtomicRefCounted::new(RefCounted::new(Descriptor::new(
            Atomic::from(ptr::from_ref(ptr)),
            ops,
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
    ops: Owned<[MaybeUninit<NodeOpInfo<V>>]>,
}

impl<K: Send + Sync, V: Send + Sync> Drop for Descriptor<K, V> {
    fn drop(&mut self) {
        // SAFETY: `self.ops` always are initialized unless `self` was not constructed via `new`
        // function.
        for op in self.ops.as_mut() {
            unsafe {
                op.assume_init_drop();
            }
        }
    }
}

impl<K: Send + Sync, V: Send + Sync> Descriptor<K, V> {
    fn new(node: Atomic<Node<K, V>>, ops: Vec<NodeOp<V>>) -> Self {
        let mut arr: Owned<[MaybeUninit<NodeOpInfo<V>>]> = Owned::init(ops.len());
        for (target, op) in arr.as_mut().iter_mut().zip(ops) {
            target.write(NodeOpInfo {
                result: Atomic::null(),
                op,
            });
        }
        Self { node, ops: arr }
    }

    #[inline]
    fn get_ops<'a>(&self, _: &'a Guard) -> &'a [NodeOpInfo<V>] {
        // SAFETY: `self.ops` always are initialized unless `self` was not constructed via `new`
        // function.
        unsafe { std::mem::transmute(self.ops.as_ref()) }
    }

    #[inline]
    fn get_ops_mut<'a>(&mut self, _: &'a Guard) -> &'a mut [NodeOpInfo<V>] {
        // SAFETY: `self.ops` always are initialized unless `self` was not constructed via `new`
        // function.
        unsafe { std::mem::transmute(self.ops.as_mut()) }
    }

    fn run_ops<'a>(&self, guard: &'a Guard, f: impl Fn(&'a NodeOpInfo<V>))
    where
        V: 'a,
    {
        let ops = self.get_ops(guard);
        let num_ops = ops
            .iter()
            .rev()
            // Need acquire ordering to see results of previous ops
            .take_while(|op| op.result.load(Ordering::Acquire, guard).tag() == 0)
            .count();
        for op in &ops[(ops.len() - num_ops)..] {
            f(&op);
        }
    }
}

#[derive(Debug, Clone)]
struct NodeOpInfo<V: Send + Sync> {
    result: Atomic<V>,
    op: NodeOp<V>,
}

impl<V: Send + Sync> NodeOpInfo<V> {
    #[inline]
    fn store_result<'a>(&self, value: Shared<'a, V>, guard: &'a Guard) -> bool {
        let old_value = self.result.load(Ordering::Relaxed, guard);
        if old_value.tag() != 0 {
            return false;
        }
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
    // ChangeHeadDesc,
    Attach,
    Update {
        old: RefCounted<V>,
        new: RefCounted<V>,
    },
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
        inner.increment();
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
    pub fn into_raw(self) -> *const T {
        self.inner.as_ptr().cast()
    }

    #[inline]
    pub unsafe fn from_raw(this: *const T) -> Self {
        Self {
            inner: NonNull::new_unchecked(this.cast_mut().cast()),
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.inner.as_ptr().cast_const().cast()
    }

    #[inline]
    fn into_inner_raw(self) -> *const RefCountedInner<T> {
        let ptr = self.inner.as_ptr();
        std::mem::forget(self);
        ptr
    }

    #[inline]
    unsafe fn from_inner_raw(this: *const RefCountedInner<T>) -> Self {
        Self {
            inner: NonNull::new_unchecked(this.cast_mut()),
        }
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
            inner: Atomic::from(data.into_inner_raw()),
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
    fn load(&self, order: Ordering, guard: &Guard) -> Option<RefCounted<T>> {
        let inner = self.inner.load(order, guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            inner.increment();
            unsafe { Some(RefCounted::from_inner_raw(ptr::from_ref(inner))) }
        } else {
            None
        }
    }

    #[inline]
    fn compare_exchange_common<'a, F>(
        &'a self,
        current: Option<&RefCounted<T>>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        func: F,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<T>>
    where
        T: 'a,
        F: FnOnce(
            &'a Atomic<RefCountedInner<T>>,
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
            Shared::from(current.inner.as_ptr().cast_const())
        } else {
            Shared::null()
        };
        let new_ptr = if let Some(new) = &new {
            Shared::from(new.inner.as_ptr().cast_const())
        } else {
            Shared::null()
        };
        match func(&self.inner, current, new_ptr, success, failure, guard) {
            Ok(_) => Ok(unsafe {
                CompareExchangeOk {
                    old: current
                        .as_ref()
                        .map(|current| RefCounted::from_inner_raw(current)),
                }
            }),
            // The `old` pointer might be queued for deallocation if another thread already took it
            // from `self` by another compare-exchange call. But we have `Guard` here we can assume
            // the pointer is valid until the `Guard` is dropped.
            Err(old) => Err(CompareExchangeErr {
                current: unsafe {
                    old.current.as_ref().map(|inner| {
                        inner.increment();
                        RefCounted::from_inner_raw(inner)
                    })
                },
                new,
            }),
        }
    }

    #[inline]
    fn compare_exchange_weak<'a>(
        &self,
        current: Option<&RefCounted<T>>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<T>> {
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
        current: Option<&RefCounted<T>>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange::<Shared<_>>,
            guard,
        )
    }

    fn finalize(self) {
        if let Some(inner) = unsafe { self.inner.try_into_owned() } {
            Box::leak(inner.into_box()).decrement();
        }
    }
}

struct CompareExchangeOk<T: Send + Sync> {
    old: Option<RefCounted<T>>,
}
struct CompareExchangeErr<T: Send + Sync> {
    current: Option<RefCounted<T>>,
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

    fn increment(&self) {
        let mut old_count = self.ref_count.load(Ordering::Relaxed);

        loop {
            if old_count >= MAX_REF_COUNT {
                abort()
            }

            if let Err(current) = self.ref_count.compare_exchange_weak(
                old_count,
                old_count + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                old_count = current;
            } else {
                break;
            };
        }
    }

    fn decrement(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            let guard = pin();
            unsafe { guard.defer_unchecked(move || Self::finalize(self)) };
        }
    }

    unsafe fn finalize(this: *const Self) {
        let this_ref = unsafe { &*this };
        let old_count = this_ref.ref_count.load(Ordering::Relaxed);

        if old_count > 0 {
            return;
        }
        if this_ref
            .ref_count
            .compare_exchange_weak(0, usize::MAX, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            drop(Owned::from_raw(this.cast_mut()))
        }
    }
}

#[derive(Debug)]
#[repr(transparent)]
struct SyncUnsafeCell<T> {
    inner: UnsafeCell<T>,
}

impl<T> SyncUnsafeCell<T> {
    fn new(data: T) -> Self {
        Self {
            inner: UnsafeCell::new(data),
        }
    }

    fn get(&self) -> *mut T {
        self.inner.get()
    }
}

unsafe impl<T> Sync for SyncUnsafeCell<T> {}
unsafe impl<T> Send for SyncUnsafeCell<T> {}
