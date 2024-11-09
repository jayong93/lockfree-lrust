use std::{
    borrow::Borrow,
    mem::{ManuallyDrop, MaybeUninit},
    num::NonZeroUsize,
    ops::{ControlFlow, Deref},
    process::abort,
    ptr::{self},
    sync::{
        atomic::{self, AtomicPtr, AtomicUsize},
        OnceLock,
    },
};

use crossbeam::{
    epoch::{Atomic, Collector, Guard, LocalHandle, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;

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
        self.size.load(atomic::Ordering::Relaxed)
    }

    pub fn remove(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };
        let guard = pin();
        let node = entry.value().load(atomic::Ordering::Relaxed, &guard);
        let node_ref = unsafe { node.deref() };
        let mut old_desc = unsafe {
            node_ref
                .desc
                .load(atomic::Ordering::Acquire, &guard)
                .deref()
        };
        if let NodeOp::Detach = old_desc.get_ops(&guard).last().map(|op| &op.op).unwrap() {
            return None;
        }
        let new_desc = Owned::new(Descriptor::new(node.into(), vec![NodeOp::Detach]));
        let backoff = Backoff::new();
        loop {
            self.run_op(old_desc, &guard);
            if let Err(err) = node_ref.desc.compare_exchange_weak(
                Shared::from(ptr::from_ref(old_desc)),
                Shared::from(ptr::from_ref(new_desc.as_ref())),
                atomic::Ordering::SeqCst,
                atomic::Ordering::Acquire,
                &guard,
            ) {
                old_desc = unsafe { err.current.deref() };
            } else {
                self.size.fetch_sub(1, atomic::Ordering::Release);
                self.run_op(unsafe { new_desc.into_shared(&guard).deref() }, &guard);
                let value = unsafe { node_ref.value.assume_init_ref() }.clone();
                unsafe {
                    guard.defer_unchecked(move || {
                        Node::finalize(node.as_raw());
                    });
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
            let node = self.tail.prev.load(atomic::Ordering::Relaxed, &guard);
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = unsafe {
                node_ref
                    .desc
                    .load(atomic::Ordering::Acquire, &guard)
                    .deref()
            };
            self.run_op(old_desc, &guard);
            if let NodeOp::Detach = old_desc.get_ops(&guard).last().unwrap().op {
                continue;
            }

            let new_desc = Owned::new(Descriptor::new(node.into(), vec![NodeOp::Detach]));
            if node_ref
                .desc
                .compare_exchange_weak(
                    Shared::from(ptr::from_ref(old_desc)),
                    Shared::from(ptr::from_ref(new_desc.as_ref())),
                    atomic::Ordering::SeqCst,
                    atomic::Ordering::Relaxed,
                    &guard,
                )
                .is_ok()
            {
                self.size.fetch_sub(1, atomic::Ordering::Release);
                self.run_op(unsafe { new_desc.into_shared(&guard).deref() }, &guard);
                let value = unsafe { node_ref.value.assume_init_ref() }.clone();
                unsafe {
                    guard.defer_unchecked(move || {
                        Node::finalize(node.as_raw());
                    });
                }
                break Some(value);
            }
            backoff.spin();
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<RefCounted<V>> {
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
        let backoff = Backoff::new();

        'outer: loop {
            let key = unsafe { new_node.key.assume_init_ref().clone() };
            let entry =
                self.skiplist
                    .compare_insert(key, Atomic::from(new_node as *const _), |node_ptr| {
                        let node_ref =
                            unsafe { node_ptr.load(atomic::Ordering::Relaxed, &guard).deref() };
                        let desc = unsafe {
                            node_ref
                                .desc
                                .load(atomic::Ordering::Acquire, &guard)
                                .deref()
                        };

                        // Remove an existing skiplist entry only when the node's last op is `Detach`
                        if let NodeOp::Detach =
                            desc.get_ops(&guard).last().map(|op| &op.op).unwrap()
                        {
                            return true;
                        }
                        false
                    });

            let node = entry.value().load(atomic::Ordering::Relaxed, &guard);
            let node_ref = unsafe { node.deref() };
            let old_value = if ptr::eq(node_ref, new_node) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, atomic::Ordering::Acquire);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    let mut is_second_try = false;
                    loop {
                        let removed = self.pop_back();
                        // All entries in the skiplist may be pending insertions.
                        // We should help the most old insertion to make a room for our node.
                        if removed.is_none()
                            && self.size.load(atomic::Ordering::Acquire) >= self.cap.get()
                        {
                            if is_second_try {
                                // Find any other node which has unfinished operation and help it.
                                if let Some(desc) = self
                                    .skiplist
                                    .iter()
                                    .filter_map(|entry| unsafe {
                                        let desc = entry
                                            .value()
                                            .load(atomic::Ordering::Relaxed, &guard)
                                            .deref()
                                            .desc
                                            .load(atomic::Ordering::Acquire, &guard)
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

                let mut old_value = unsafe { node_ref.value.assume_init_ref() };
                let new_value = unsafe { new_node.value.assume_init_ref().clone() };
                // SAFETY: `op` was not seen by other thread yet, so it is safe to be replaced.
                let mut desc = Owned::new(Descriptor::new(
                    Atomic::from(node_ref as *const _),
                    vec![
                        NodeOp::Detach,
                        NodeOp::Attach,
                        NodeOp::Update {
                            old: old_value.clone(),
                            new: new_value,
                        },
                    ],
                ));
                // Need `acquire` ordering to read value of OP properly
                let mut old_desc = unsafe {
                    node_ref
                        .desc
                        .load(atomic::Ordering::Acquire, &guard)
                        .deref()
                };
                let desc = loop {
                    self.run_op(old_desc, &guard);

                    let last_old_ops = old_desc.get_ops(&guard).last().map(|op| &op.op).unwrap();
                    // The node was removed, try to allocate a new node
                    match last_old_ops {
                        NodeOp::Detach => {
                            backoff.spin();
                            continue 'outer;
                        }
                        NodeOp::Update { new, .. } => {
                            old_value = new;
                        }
                        _ => {}
                    }

                    // Update the expected value
                    match desc
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
                        (old_desc as *const Descriptor<K, V>).into(),
                        Shared::from(desc.as_ref() as *const Descriptor<K, V>),
                        atomic::Ordering::Release,
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
                    Some(unsafe { RefCounted::from_raw(result.as_raw() as *mut V) })
                }
            };
            break old_value;
        }
    }

    // TODO: Apply ref counting to V
    pub fn get(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let node = unsafe {
            entry
                .value()
                .load(atomic::Ordering::Relaxed, &guard)
                .deref()
        };
        let desc = unsafe { node.desc.load(atomic::Ordering::Relaxed, &guard).deref() };
        if let Some(NodeOpInfo {
            op: NodeOp::Detach, ..
        }) = desc.get_ops(&guard).last()
        {
            return None;
        }

        let mru = self.head.next.load(atomic::Ordering::Relaxed, &guard);
        let get_value_fn = || unsafe { Some(node.value.assume_init_ref().clone()) };
        if ptr::eq(mru.as_raw(), node) {
            get_value_fn()
        } else {
            let mut old_desc = Shared::from(desc as *const _);
            let new_desc = Owned::new(Descriptor::new(
                Atomic::from(node as *const _),
                vec![NodeOp::Detach, NodeOp::Attach],
            ))
            .into_shared(&guard);
            let backoff = Backoff::new();
            loop {
                self.run_op(desc, &guard);
                if let Err(err) = node.desc.compare_exchange_weak(
                    old_desc,
                    new_desc,
                    atomic::Ordering::Release,
                    atomic::Ordering::Acquire,
                    &guard,
                ) {
                    old_desc = err.current;
                    if let Some(NodeOp::Detach) = unsafe { old_desc.deref() }
                        .get_ops(&guard)
                        .last()
                        .map(|op| &op.op)
                    {
                        return None;
                    }
                    backoff.spin();
                } else {
                    let value = get_value_fn();
                    self.run_op(unsafe { new_desc.deref() }, &guard);
                    break value;
                }
            }
        }
    }

    // Set head's OP same as mine
    fn change_head_op<'a>(
        &self,
        desc: &Descriptor<K, V>,
        op_info: &NodeOpInfo<V>,
        guard: &'a Guard,
    ) -> Option<Shared<'a, Node<K, V>>> {
        let backoff = Backoff::new();
        let head = self.head.as_ref();
        let mut old_head_desc = head.desc.load(atomic::Ordering::Acquire, &guard);
        loop {
            if ptr::eq(old_head_desc.as_raw(), desc) {
                let next = head.next.load(atomic::Ordering::SeqCst, guard);
                // If the above load runs before storing result from another thread, the `next`
                // pointer is still right next pointer.
                if op_info.result.load(atomic::Ordering::Relaxed, guard).tag() == 0 {
                    return Some(next);
                }
                return None;
            }

            if let Some(head_desc) = unsafe { old_head_desc.as_ref() } {
                self.run_op(head_desc, guard);
            }

            let next = head.next.load(atomic::Ordering::Relaxed, guard);
            if let Err(err) = head.desc.compare_exchange_weak(
                old_head_desc,
                Shared::from(desc as *const Descriptor<K, V>),
                atomic::Ordering::Release,
                atomic::Ordering::Acquire,
                &guard,
            ) {
                old_head_desc = err.current;
            } else {
                op_info.store_result(next, guard);
                return Some(next);
            }
            backoff.spin();
        }
    }

    fn put_node_after_head<'a>(
        &self,
        node: &Node<K, V>,
        guard: &'a Guard,
    ) -> ControlFlow<Option<RefCounted<V>>> {
        // SAFETY: Head is always valid
        let head = self.head.as_ref();
        // SAFETY: All data nodes are valid until GC-ed and tail is always valid.
        let mru_node = unsafe { head.next.load(atomic::Ordering::Acquire, guard).deref() };
        if ptr::eq(mru_node, node) {
            return ControlFlow::Break(None);
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
                            atomic::Ordering::Release,
                            atomic::Ordering::Acquire,
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
            let prev_next = prev_ref.next.load(atomic::Ordering::Relaxed, guard);
            if ptr::eq(prev_next.as_raw(), node)
                && node
                    .prev
                    .compare_exchange_weak(
                        node_prev,
                        prev,
                        atomic::Ordering::Release,
                        atomic::Ordering::Relaxed,
                        guard,
                    )
                    .is_ok()
            {
                if prev_ref.prev.load(atomic::Ordering::Relaxed, guard).tag() == 0 {
                    return prev;
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
                            atomic::Ordering::Release,
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
                    atomic::Ordering::Release,
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
    ) -> ControlFlow<Option<RefCounted<V>>> {
        let mut next = node.next.load(atomic::Ordering::Relaxed, guard);
        loop {
            if next.tag() == 0 {
                let new_next = next.with_tag(1);
                if let Err(err) = node.next.compare_exchange_weak(
                    next,
                    new_next,
                    atomic::Ordering::Release,
                    atomic::Ordering::Relaxed,
                    guard,
                ) {
                    // Conflict with other removal thread
                    if err.current.tag() != 0 {
                        return ControlFlow::Continue(());
                    }
                    // Another thread removed the next node and update `next` pointer of this node
                    else {
                        atomic::fence(atomic::Ordering::Acquire);
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
        ControlFlow::Break(Some(unsafe { node.value.assume_init_ref() }.clone()))
    }

    fn loop_until_succeess<'a>(
        &self,
        op: &NodeOpInfo<V>,
        guard: &'a Guard,
        backoff: &Backoff,
        func: impl Fn() -> ControlFlow<Option<RefCounted<V>>>,
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
                ControlFlow::Break(Some(v)) => {
                    return op
                        .store_result(Shared::from(RefCounted::into_raw(v).cast_const()), guard);
                }
                _ => {
                    return op.store_result(Shared::null(), guard);
                }
            }
        }
    }

    fn run_op(&self, desc: &Descriptor<K, V>, guard: &Guard) {
        let backoff = Backoff::new();

        let node = unsafe { desc.node.load(atomic::Ordering::Relaxed, guard).deref() };
        desc.run_ops(guard, |op| match &op.op {
            NodeOp::Attach => {
                self.loop_until_succeess(op, guard, &backoff, || {
                    self.put_node_after_head(node, guard)
                });
            }
            NodeOp::ChangeHeadDesc => {
                self.change_head_op(desc, op, guard);
            }
            NodeOp::Update { old, new } => {
                if let Ok(old) = unsafe { node.value.assume_init_ref() }.compare_exchange(
                    old,
                    new.clone(),
                    atomic::Ordering::AcqRel,
                    atomic::Ordering::Relaxed,
                    guard,
                ) {
                    op.store_result(Shared::from(RefCounted::into_raw(old).cast_const()), guard);
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
    key: MaybeUninit<RefCounted<K>>,
    value: MaybeUninit<RefCounted<V>>,
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
                key: MaybeUninit::new(RefCounted::new(key)),
                value: MaybeUninit::new(RefCounted::new(value)),
                next,
                prev,
                desc: Atomic::null(),
            })
            .into_box(),
        );
        ptr.desc = Atomic::new(Descriptor::new(Atomic::from(ptr::from_ref(ptr)), ops));
        Shared::from(ptr::from_ref(ptr))
    }

    fn uninit<'a>(next: Atomic<Node<K, V>>, prev: Atomic<Node<K, V>>) -> Owned<Self> {
        Owned::new(Self {
            key: MaybeUninit::uninit(),
            value: MaybeUninit::uninit(),
            next,
            prev,
            desc: Atomic::null(),
        })
    }

    fn mark_prev(&self, backoff: &Backoff, guard: &Guard) {
        loop {
            let prev = self.prev.load(atomic::Ordering::Acquire, guard);
            if prev.tag() != 0 {
                return;
            }
            if self
                .prev
                .compare_exchange_weak(
                    prev,
                    prev.with_tag(1),
                    atomic::Ordering::Release,
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

    /// Should not used on head/tail node
    unsafe fn finalize(this: *const Self) {
        let this = this.cast_mut();
        unsafe {
            (&mut *this).key.assume_init_drop();
            (&mut *this).value.assume_init_drop();
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
            .take_while(|op| op.result.load(atomic::Ordering::Relaxed, guard).tag() == 0)
            .count();
        for op in &ops[(ops.len() - num_ops)..] {
            f(&op);
        }
    }
}

struct NodeOpInfo<V: Send + Sync> {
    result: Atomic<()>,
    op: NodeOp<V>,
}

impl<V: Send + Sync> NodeOpInfo<V> {
    fn store_result<'a, T: Sized>(&self, value: Shared<'a, T>, guard: &'a Guard) -> bool {
        let old_value = self.result.load(atomic::Ordering::Relaxed, guard);
        if old_value.tag() != 0 {
            return false;
        }
        let value = Shared::from(value.as_raw().cast::<()>()).with_tag(1);
        self.result
            .compare_exchange(
                old_value,
                value,
                atomic::Ordering::SeqCst,
                atomic::Ordering::Acquire,
                guard,
            )
            .is_ok()
    }
}

enum NodeOp<V: Send + Sync> {
    Detach,
    ChangeHeadDesc,
    Attach,
    Update {
        old: RefCounted<V>,
        new: RefCounted<V>,
    },
}

const MAX_REF_COUNT: usize = usize::MAX >> 10;

#[repr(transparent)]
pub struct RefCounted<T: Send + Sync> {
    inner: AtomicPtr<RefCountedInner<T>>,
}

impl<T: Send + Sync> Borrow<T> for RefCounted<T> {
    fn borrow(&self) -> &T {
        self.deref()
    }
}

impl<T: Send + Sync + Eq> Eq for RefCounted<T> {}

impl<T: Send + Sync + PartialEq> PartialEq for RefCounted<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other)
    }
}

impl<T: Send + Sync + PartialOrd> PartialOrd for RefCounted<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other)
    }
}

impl<T: Send + Sync + Ord> Ord for RefCounted<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deref().cmp(other)
    }
}

impl<T: Send + Sync> Deref for RefCounted<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.inner.load(atomic::Ordering::Acquire)).data }
    }
}

impl<T: Send + Sync> Clone for RefCounted<T> {
    fn clone(&self) -> Self {
        let inner_ptr = self.inner.load(atomic::Ordering::Relaxed);
        let inner = unsafe { &*inner_ptr };
        let old_count = inner.ref_count.fetch_add(1, atomic::Ordering::Relaxed);
        if old_count > MAX_REF_COUNT {
            abort();
        }
        Self {
            inner: AtomicPtr::new(inner_ptr),
        }
    }
}

impl<T: Send + Sync> Drop for RefCounted<T> {
    fn drop(&mut self) {
        let inner_ptr = self.inner.load(atomic::Ordering::Relaxed);
        let inner = unsafe { &*inner_ptr };
        if inner.ref_count.fetch_sub(1, atomic::Ordering::Release) == 1 {
            atomic::fence(atomic::Ordering::Acquire);
            let guard = pin();
            unsafe { guard.defer_destroy(Shared::from(inner_ptr.cast_const())) };
        }
    }
}

impl<T: Send + Sync> RefCounted<T> {
    #[inline]
    fn new(data: T) -> Self {
        let ptr = Box::into_raw(Box::new(RefCountedInner::new(data)));
        Self {
            inner: AtomicPtr::new(ptr),
        }
    }

    pub fn into_inner(this: Self) -> Option<T> {
        let this = ManuallyDrop::new(this);
        let inner_ptr = this.inner.load(atomic::Ordering::Relaxed);
        if unsafe { &*inner_ptr }
            .ref_count
            .fetch_sub(1, atomic::Ordering::Release)
            != 1
        {
            return None;
        }
        atomic::fence(atomic::Ordering::Acquire);
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

    fn into_raw(self) -> *mut T {
        self.inner.load(atomic::Ordering::Relaxed) as *mut T
    }

    unsafe fn from_raw(ptr: *mut T) -> Self {
        Self {
            inner: AtomicPtr::new(ptr as *mut RefCountedInner<T>),
        }
    }

    #[inline]
    fn compare_exchange_common<'a>(
        &self,
        current: &RefCounted<T>,
        new: RefCounted<T>,
        success: atomic::Ordering,
        failure: atomic::Ordering,
        func: impl FnOnce(
            &AtomicPtr<RefCountedInner<T>>,
            *mut RefCountedInner<T>,
            *mut RefCountedInner<T>,
            atomic::Ordering,
            atomic::Ordering,
        ) -> Result<*mut RefCountedInner<T>, *mut RefCountedInner<T>>,
        _: &'a Guard,
    ) -> Result<RefCounted<T>, CompareExchangeErr<'a, T>> {
        let current = current.inner.load(atomic::Ordering::Relaxed);
        let new_ptr = new.inner.load(atomic::Ordering::Relaxed);
        match func(&self.inner, current, new_ptr, success, failure) {
            Ok(old) => Ok(Self {
                inner: AtomicPtr::new(old),
            }),
            // The `old` pointer might be queued for deallocation if another thread already took it
            // from `self` by another compare-exchange call. But we have `Guard` here we can assume
            // the pointer is valid until the `Guard` is dropped.
            Err(old) => Err(CompareExchangeErr {
                current: unsafe { &*old },
                new,
            }),
        }
    }

    #[inline]
    fn compare_exchange_weak<'a>(
        &self,
        current: &RefCounted<T>,
        new: RefCounted<T>,
        success: atomic::Ordering,
        failure: atomic::Ordering,
        guard: &'a Guard,
    ) -> Result<RefCounted<T>, CompareExchangeErr<'a, T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            AtomicPtr::compare_exchange_weak,
            guard,
        )
    }

    #[inline]
    fn compare_exchange<'a>(
        &self,
        current: &RefCounted<T>,
        new: RefCounted<T>,
        success: atomic::Ordering,
        failure: atomic::Ordering,
        guard: &'a Guard,
    ) -> Result<RefCounted<T>, CompareExchangeErr<'a, T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            AtomicPtr::compare_exchange,
            guard,
        )
    }
}

struct CompareExchangeErr<'a, T: Send + Sync> {
    current: &'a RefCountedInner<T>,
    new: RefCounted<T>,
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

    fn try_promote(&self) -> Option<RefCounted<T>> {
        let old_count = self.ref_count.load(atomic::Ordering::Relaxed);
        if old_count == 0 {
            return None;
        }
        if let Err(curr) = self.ref_count.compare_exchange_weak(
            old_count,
            old_count + 1,
            atomic::Ordering::Acquire,
            atomic::Ordering::Relaxed,
        ) {
            if curr == 0 {
                return None;
            }
        }
        Some(RefCounted {
            inner: AtomicPtr::new(ptr::from_ref(self).cast_mut()),
        })
    }
}
