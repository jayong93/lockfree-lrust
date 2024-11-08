use std::{
    mem::MaybeUninit,
    num::NonZeroUsize,
    ops::ControlFlow,
    ptr::{self, NonNull},
    sync::{
        atomic::{self, AtomicUsize},
        OnceLock,
    },
};

use crossbeam::{
    epoch::{Atomic, Collector, Guard, LocalHandle, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;

fn box_into_inner<T>(boxed: Box<T>) -> T {
    *boxed
}

fn collector() -> &'static Collector {
    static COLLECTOR: OnceLock<Collector> = OnceLock::new();
    COLLECTOR.get_or_init(|| Collector::new())
}

fn pin() -> Guard {
    HANDLE.with(|handle| handle.pin())
}

thread_local! {
    static HANDLE: LocalHandle = collector().register();
}

pub struct Lru<K: Ord + Send + Sync + 'static, V: Send + Sync + 'static> {
    skiplist: SkipMap<KeyRef<K>, Atomic<Node<K, V>>>,
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

    fn pop_back_with_size(&self) -> (usize, Option<V>) {
        self.size.fetch_sub(1, atomic::Ordering::Relaxed);
        todo!()
    }

    #[inline]
    pub fn pop_back(&self) -> Option<V> {
        let (_, value) = self.pop_back_with_size();
        value
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let guard = pin();
        // SAFETY: Head is never deallocated.
        let head = self.head.as_ref();
        let new_node = Node::new(
            key,
            value,
            head.next.clone(),
            Atomic::from(head as *const _),
            vec![NodeOp::Attach],
            &guard,
        );
        let new_node = unsafe { new_node.deref() };
        let desc = unsafe {
            new_node
                .desc
                .load(atomic::Ordering::Relaxed, &guard)
                .deref()
        };
        // SAFETY: new_node and its key are always initialized at this point.
        let key_ref = unsafe { KeyRef::new(new_node.key.assume_init_ref()) };
        let backoff = Backoff::new();

        'outer: loop {
            let entry = self.skiplist.compare_insert(
                key_ref.clone(),
                Atomic::from(new_node as *const _),
                |node_ptr| {
                    let node = unsafe { node_ptr.load(atomic::Ordering::Relaxed, &guard).deref() };

                    // Remove an existing skiplist entry only when the node's last op is `Detach`
                    if let Some(NodeOpInfo {
                        op: NodeOp::Detach, ..
                    }) = unsafe { node.desc.load(atomic::Ordering::Relaxed, &guard).as_ref() }
                        .and_then(|desc| desc.get_ops(&guard).last())
                    {
                        return true;
                    }
                    false
                },
            );

            let node = entry.value().load(atomic::Ordering::Relaxed, &guard);
            let node = unsafe { node.deref() };
            let old_value = if ptr::eq(node, new_node) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, atomic::Ordering::Acquire);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    let mut is_second_try = false;
                    loop {
                        let (new_len, removed) = self.pop_back_with_size();
                        // All entries in the skiplist may be pending insertions.
                        // We should help the most old insertion to make a room for our node.
                        if new_len >= self.cap.get() && removed.is_none() {
                            if is_second_try {
                                // Find any other node which has unfinished operation and help it.
                                if let Some(desc) = self
                                    .skiplist
                                    .iter()
                                    .filter_map(|entry| unsafe {
                                        let desc = entry
                                            .value()
                                            .load(atomic::Ordering::Acquire, &guard)
                                            .deref()
                                            .desc
                                            .load(atomic::Ordering::Relaxed, &guard)
                                            .deref();
                                        desc.get_ops(&guard)
                                            .last()
                                            .filter(|last_op| {
                                                last_op
                                                    .result
                                                    .load(atomic::Ordering::Relaxed, &guard)
                                                    .tag()
                                                    == 0
                                            })
                                            .map(|_| desc)
                                    })
                                    .next()
                                {
                                    self.run_op(desc, &guard);
                                }
                                // There is no other node with unfinished operation.
                                // Retry refreshingly.
                                else {
                                    is_second_try = false;
                                }
                            } else if let Some(head_desc) = unsafe {
                                head.desc.load(atomic::Ordering::Acquire, &guard).as_ref()
                            } {
                                self.run_op(head_desc, &guard);
                                is_second_try = true;
                            }
                        } else {
                            break;
                        }
                    }
                }
                self.run_op(desc, &guard);
                None
            } else {
                // Another node alreay exists

                // SAFETY: `op` was not seen by other thread yet, so it is safe to be replaced.
                let mut desc = Owned::new(Descriptor::new(
                    Atomic::from(node as *const _),
                    vec![
                        NodeOp::Detach,
                        NodeOp::Attach,
                        NodeOp::Update {
                            old: Atomic::null(),
                            new: new_node.value.clone(),
                        },
                    ],
                ));
                // Need `acquire` ordering to read value of OP properly
                let mut old_desc =
                    unsafe { node.desc.load(atomic::Ordering::Acquire, &guard).deref() };
                let desc = loop {
                    self.run_op(old_desc, &guard);
                    if matches!(
                        old_desc.get_ops(&guard).last().map(|op| &op.op),
                        Some(NodeOp::Detach)
                    ) {
                        // The node was removed, try to allocate a new node
                        backoff.spin();
                        continue 'outer;
                    }
                    match desc
                        .get_ops_mut(&guard)
                        .last_mut()
                        .map(|op| &mut op.op)
                        .unwrap()
                    {
                        NodeOp::Update { old, .. } => {
                            *old = node.value.load(atomic::Ordering::Relaxed, &guard).into();
                        }
                        _ => unreachable!(),
                    }

                    if let Err(err) = node.desc.compare_exchange_weak(
                        (old_desc as *const Descriptor<K, V>).into(),
                        Shared::from(desc.as_ref() as *const Descriptor<K, V>),
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::Acquire,
                        &guard,
                    ) {
                        old_desc = unsafe { err.current.deref() }
                    } else {
                        break unsafe { desc.into_shared(&guard).deref() };
                    }
                    backoff.spin();
                };
                self.run_op(desc, &guard);
                let result = desc
                    .get_ops(&guard)
                    .last()
                    .unwrap()
                    .result
                    .load(atomic::Ordering::Acquire, &guard);
                if result.is_null() {
                    None
                } else {
                    Some(unsafe { box_into_inner(result.into_owned().into_box()) })
                }
            };
            break old_value;
        }
    }

    fn change_head_op(&self, desc: &Descriptor<K, V>, guard: &Guard) {
        let backoff = Backoff::new();
        let head = self.head.as_ref();
        let mut old_head_desc = head.desc.load(atomic::Ordering::Acquire, &guard);
        loop {
            if ptr::eq(old_head_desc.as_raw(), desc) {
                return;
            }
            if let Some(op_info) = desc.get_ops(guard).iter().rev().next() {
                if op_info.result.load(atomic::Ordering::Relaxed, guard).tag() != 0 {
                    // Another thread already finished the operation.
                    return;
                }
            }

            self.run_op(unsafe { old_head_desc.deref() }, &guard);
            if let Err(err) = head.desc.compare_exchange_weak(
                old_head_desc,
                Shared::from(desc as *const Descriptor<K, V>),
                atomic::Ordering::Release,
                atomic::Ordering::Acquire,
                &guard,
            ) {
                old_head_desc = err.current;
            } else {
                return;
            }
            backoff.spin();
        }
    }

    fn put_node_after_head<'a>(
        &self,
        node: &Node<K, V>,
        guard: &'a Guard,
    ) -> ControlFlow<Shared<'a, V>> {
        // SAFETY: Head is always valid
        let head = self.head.as_ref();
        // SAFETY: All data nodes are valid until GC-ed and tail is always valid.
        let mru_node = unsafe { head.next.load(atomic::Ordering::Acquire, guard).deref() };
        if ptr::eq(mru_node, node) {
            return ControlFlow::Break(Shared::null());
        }
        let node = Shared::from(node as *const _);
        if let Err(err) = mru_node.prev.compare_exchange_weak(
            Shared::from(head as *const _),
            node,
            atomic::Ordering::Release,
            atomic::Ordering::Relaxed,
            guard,
        ) {
            if !ptr::eq(err.current.as_raw(), node.as_raw()) {
                return ControlFlow::Continue(());
            }
        }
        // Ok even it is failed because someone already changed `head.next`.
        head.next
            .compare_exchange(
                Shared::from(mru_node as *const _),
                node,
                atomic::Ordering::Release,
                atomic::Ordering::Relaxed,
                guard,
            )
            .ok();

        ControlFlow::Break(Shared::null())
    }

    fn help_link<'a>(
        &self,
        mut prev: Shared<'a, Node<K, V>>,
        node: &'a Node<K, V>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) {
        let mut last = Shared::<Node<K, V>>::null();
        loop {
            let prev_ref = unsafe { prev.deref() };
            let prev_next = prev_ref.next.load(atomic::Ordering::Relaxed, guard);
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
                            atomic::Ordering::SeqCst,
                            atomic::Ordering::Relaxed,
                            guard,
                        )
                        .ok();
                    prev = last;
                    last = Shared::null();
                } else {
                    // Find a previous node which is not deleted.
                    prev = prev_ref.prev.load(atomic::Ordering::Relaxed, guard);
                }
                continue;
            }
            let node_prev = node.prev.load(atomic::Ordering::Relaxed, guard);
            // The node was removed by another thread. Pass responsibility of clean-up to that thread.
            if node_prev.tag() != 0 {
                return;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(prev_next.as_raw(), node) {
                last = prev;
                prev = prev_next;
                continue;
            }
            // Another thread already helped our job.
            if ptr::eq(node_prev.as_raw(), prev.as_raw()) {
                return;
            }
            let prev_next = prev_ref.next.load(atomic::Ordering::Relaxed, guard);
            if ptr::eq(prev_next.as_raw(), node)
                && node
                    .prev
                    .compare_exchange_weak(
                        node_prev,
                        prev,
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::Relaxed,
                        guard,
                    )
                    .is_ok()
            {
                if prev_ref.prev.load(atomic::Ordering::Relaxed, guard).tag() == 0 {
                    return;
                }
            }
            backoff.spin();
        }
    }

    fn help_detach<'a>(&self, node: &Node<K, V>, backoff: &Backoff, guard: &'a Guard) {
        node.mark_prev(backoff, guard);
        let mut last = Shared::<Node<K, V>>::null();
        let mut prev = node.prev.load(atomic::Ordering::Relaxed, guard);
        let mut next = node.next.load(atomic::Ordering::Relaxed, guard);
        loop {
            // Not need to null check for `prev`/`next` because if they are set as head/tail
            // once, they will not go backward/forward any more. That is because head/tail
            // node will not marked as deleted.

            // `prev`'s next pointer was already changed from `node` to `next`.
            if ptr::eq(prev.as_raw(), next.as_raw()) {
                return;
            }
            let next_ref = unsafe { next.deref() };
            let next_next = next_ref.next.load(atomic::Ordering::Relaxed, guard);
            if next_next.tag() != 0 {
                next_ref.mark_prev(backoff, guard);
                next = next_next.as_raw().into();
                continue;
            }
            let prev_ref = unsafe { prev.deref() };
            let prev_next = prev_ref.next.load(atomic::Ordering::Relaxed, guard);
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
                            atomic::Ordering::SeqCst,
                            atomic::Ordering::Relaxed,
                            guard,
                        )
                        .ok();
                    prev = last;
                    last = Shared::null();
                } else {
                    // Find a previous node which is not deleted.
                    prev = prev_ref.prev.load(atomic::Ordering::Relaxed, guard);
                }
                continue;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(prev_next.as_raw(), node) {
                last = prev;
                prev = prev_next;
                continue;
            }

            if prev_ref
                .next
                .compare_exchange_weak(
                    Shared::from(node as *const _),
                    next,
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::Relaxed,
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
    ) -> ControlFlow<Shared<'a, V>> {
        let mut next = node.next.load(atomic::Ordering::Relaxed, guard);
        loop {
            if next.tag() == 0 {
                let new_next = next.with_tag(1);
                if let Err(err) = node.next.compare_exchange_weak(
                    next,
                    new_next,
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::Relaxed,
                    guard,
                ) {
                    // Conflict with other removal thread
                    if err.current.tag() != 0 {
                        return ControlFlow::Continue(());
                    }
                    // Another thread removed the next node and update `next` pointer of this node
                    else {
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
        let prev = node.prev.load(atomic::Ordering::Acquire, guard);
        self.help_link(prev, unsafe { next.deref() }, backoff, guard);
        ControlFlow::Break(node.value.load(atomic::Ordering::Acquire, guard))
    }

    fn loop_until_succeess<'a>(
        &self,
        op: &NodeOpInfo<V>,
        guard: &'a Guard,
        backoff: &Backoff,
        func: impl Fn() -> ControlFlow<Shared<'a, V>>,
    ) -> bool {
        loop {
            let result = op.result.load(atomic::Ordering::Relaxed, guard);
            if result.tag() != 0 {
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

        let node = unsafe { desc.node.load(atomic::Ordering::Relaxed, guard).deref() };
        desc.run_ops(guard, |op| match &op.op {
            NodeOp::Attach => {
                // Set head's OP same as mine
                self.change_head_op(desc, guard);
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.put_node_after_head(node, guard)
                });
            }
            NodeOp::Update { old, new } => {
                let old = old.load(atomic::Ordering::Relaxed, guard);
                let new = new.load(atomic::Ordering::Relaxed, guard);
                if node
                    .value
                    .compare_exchange(
                        old,
                        new,
                        atomic::Ordering::SeqCst,
                        atomic::Ordering::Relaxed,
                        guard,
                    )
                    .is_ok()
                {
                    op.store_result(old, guard);
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
    key: MaybeUninit<K>,
    value: Atomic<V>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    desc: Atomic<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
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
                key: MaybeUninit::new(key),
                value: Atomic::new(value),
                next,
                prev,
                desc: Atomic::null(),
            })
            .into_box(),
        );
        ptr.desc = Atomic::new(Descriptor::new(Atomic::from(ptr as *const _), ops));
        Shared::from(ptr as *const _)
    }

    fn uninit<'a>(next: Atomic<Node<K, V>>, prev: Atomic<Node<K, V>>) -> Owned<Self> {
        Owned::new(Self {
            key: MaybeUninit::uninit(),
            value: Atomic::null(),
            next,
            prev,
            desc: Atomic::null(),
        })
    }

    fn mark_prev(&self, backoff: &Backoff, guard: &Guard) {
        loop {
            let prev = self.prev.load(atomic::Ordering::Relaxed, guard);
            if prev.tag() != 0 {
                return;
            }
            if self
                .prev
                .compare_exchange_weak(
                    prev,
                    prev.with_tag(1),
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::Relaxed,
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

struct KeyRef<K: Send + Sync>(NonNull<K>);

impl<K: Send + Sync> Clone for KeyRef<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

unsafe impl<K: Send + Sync> Send for KeyRef<K> {}

impl<K: Send + Sync + PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.0.as_ref().eq(other.0.as_ref()) }
    }
}
impl<K: Send + Sync + PartialOrd> PartialOrd for KeyRef<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        unsafe { self.0.as_ref().partial_cmp(other.0.as_ref()) }
    }
}
impl<K: Send + Sync + Ord> Ord for KeyRef<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        unsafe { self.0.as_ref().cmp(other.0.as_ref()) }
    }
}
impl<K: Send + Sync + Eq> Eq for KeyRef<K> {}

impl<K: Send + Sync> KeyRef<K> {
    fn new(key: &K) -> Self {
        unsafe { Self(NonNull::new_unchecked(key as *const K as *mut K)) }
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
            unsafe { ptr::drop_in_place(op as *mut _ as *mut NodeOpInfo<V>) };
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
        unsafe {
            &*(self.ops.as_ref() as *const [MaybeUninit<NodeOpInfo<V>>] as *const [NodeOpInfo<V>])
        }
    }

    #[inline]
    fn get_ops_mut<'a>(&mut self, _: &'a Guard) -> &'a mut [NodeOpInfo<V>] {
        // SAFETY: `self.ops` always are initialized unless `self` was not constructed via `new`
        // function.
        unsafe {
            &mut *(self.ops.as_mut() as *mut [MaybeUninit<NodeOpInfo<V>>] as *mut [NodeOpInfo<V>])
        }
    }

    fn run_ops<'a>(&self, guard: &'a Guard, f: impl Fn(&'a NodeOpInfo<V>))
    where
        V: 'a,
    {
        let ops = self.get_ops(guard);
        let num_ops = ops
            .iter()
            .rev()
            .take_while(|op| op.result.load(atomic::Ordering::Relaxed, guard).tag() == 0)
            .count();
        for op in &ops[(ops.len() - num_ops)..] {
            f(&op);
        }
    }
}

struct NodeOpInfo<V: Send + Sync> {
    result: Atomic<V>,
    op: NodeOp<V>,
}

impl<V: Send + Sync> NodeOpInfo<V> {
    fn store_result<'a>(&self, value: Shared<'a, V>, guard: &'a Guard) -> bool {
        let old_value = self.result.load(atomic::Ordering::Relaxed, guard);
        if old_value.tag() != 0 {
            return false;
        }
        let value = value.with_tag(1);
        self.result
            .compare_exchange(
                old_value,
                value,
                atomic::Ordering::Release,
                atomic::Ordering::Acquire,
                guard,
            )
            .is_ok()
    }
}

enum NodeOp<V> {
    Detach,
    Attach,
    Update { old: Atomic<V>, new: Atomic<V> },
}
