use std::{
    hint::unreachable_unchecked,
    mem::{ManuallyDrop, MaybeUninit},
    num::NonZeroUsize,
    ops::Deref,
    ptr::{self, NonNull},
    sync::{
        atomic::{self, AtomicBool, AtomicUsize},
        Arc, OnceLock,
    },
};

use crossbeam::{
    epoch::{Atomic, Collector, Guard, LocalHandle, Owned, Pointer, Shared},
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
    skiplist: SkipMap<KeyRef<K>, Atomic<Node<K, V>>>,
    // `head` and `tail` are sentry nodes which have no valid data
    head: Atomic<Node<K, V>>,
    tail: Atomic<Node<K, V>>,
    cap: NonZeroUsize,
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
            head: head.into(),
            tail: tail.into(),
            cap: capacity,
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<V> {
        let guard = pin();
        let head = self.head.load(atomic::Ordering::Relaxed, &guard);
        // SAFETY: Head is never deallocated.
        let head = unsafe { head.deref() };
        let new_node = Node::new(
            key,
            value,
            head.next.clone(),
            Atomic::from(head as *const _),
            NodeOp::Hit,
            &guard,
        );
        let new_node = unsafe { new_node.deref() };
        let desc = unsafe { new_node.op.load(atomic::Ordering::Relaxed, &guard).deref() };
        // SAFETY: new_node and its key are always initialized at this point.
        let key_ref = unsafe { KeyRef::new(new_node.key.assume_init_ref()) };
        let backoff = Backoff::new();

        'outer: loop {
            let entry = self
                .skiplist
                .get_or_insert(key_ref.clone(), Atomic::from(new_node as *const _));

            let node = entry.value().load(atomic::Ordering::Relaxed, &guard);
            let node = unsafe { node.deref() };
            let desc = if !ptr::eq(node, new_node) {
                // Another node alreay exists

                // SAFETY: `op` was not seen by other thread yet, so it is safe to be replaced.
                let mut desc = Owned::new(Descriptor::new(
                    NodeOp::Update {
                        old: Atomic::null(),
                        new: new_node.value.clone(),
                    },
                    Atomic::from(node as *const _),
                ));
                let mut old_desc =
                    unsafe { node.op.load(atomic::Ordering::Acquire, &guard).deref() };
                loop {
                    // Need `acquire` ordering to read value of OP properly
                    self.run_op(old_desc, &guard, OpRunMode::Help);
                    if matches!(old_desc.op, NodeOp::Remove) {
                        // The node was removed, try to allocate a new node
                        backoff.spin();
                        continue 'outer;
                    }
                    match &mut desc.op {
                        NodeOp::Update { old, .. } => {
                            *old = node.value.load(atomic::Ordering::Relaxed, &guard).into();
                        }
                        _ => unsafe { unreachable_unchecked() },
                    }

                    if let Err(err) = node.op.compare_exchange_weak(
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
                }
            } else {
                desc
            };

            return self.run_op(desc, &guard, OpRunMode::NoHelp);
        }
    }

    fn change_head_op(&self, desc: &Descriptor<K, V>, guard: &Guard) {
        let backoff = Backoff::new();
        let head = unsafe { self.head.load(atomic::Ordering::Relaxed, guard).deref() };
        let mut old_head_desc = head.op.load(atomic::Ordering::Acquire, &guard);
        loop {
            if ptr::eq(old_head_desc.as_raw(), desc) {
                return;
            }
            let op_result = desc.result.load(atomic::Ordering::Acquire, &guard);
            if op_result.tag() != 0 {
                // Another thread already finished the operation.
                return;
            }

            self.run_op(unsafe { old_head_desc.deref() }, &guard, OpRunMode::Help);
            if let Err(err) = head.op.compare_exchange_weak(
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
    ) -> Option<ControlFlow<'a, V>> {
        // SAFETY: Head is always valid
        let head = unsafe { self.head.load(atomic::Ordering::Relaxed, guard).deref() };
        // SAFETY: All data nodes are valid until GC-ed and tail is always valid.
        let mru_node = unsafe { head.next.load(atomic::Ordering::Acquire, guard).deref() };
        if ptr::eq(mru_node, node) {
            return Some(ControlFlow::Return(None));
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
                return None;
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

        Some(ControlFlow::Store(Shared::null()))
    }

    fn loop_until_succeess<'a>(
        &self,
        desc: &Descriptor<K, V>,
        guard: &'a Guard,
        mode: OpRunMode,
        backoff: &Backoff,
        func: impl Fn() -> Option<ControlFlow<'a, V>>,
    ) -> Option<V> {
        loop {
            let result = desc.result.load(atomic::Ordering::Relaxed, guard);
            if result.tag() != 0 {
                return if matches!(mode, OpRunMode::Help) {
                    None
                } else if result.is_null() {
                    None
                } else {
                    // SAFETY: Only the owner of the descriptor can run OP in non-help mode
                    // so it is safe to extract value from result
                    unsafe {
                        // Extract a value from result and prevent dropping of the value
                        let result: Shared<ManuallyDrop<V>> = std::mem::transmute(result);
                        let mut result = result.into_owned();
                        Some(ManuallyDrop::take(result.as_mut()))
                    }
                };
            }

            match func() {
                None => {
                    backoff.spin();
                    continue;
                }
                Some(ControlFlow::Store(v)) if matches!(mode, OpRunMode::NoHelp) => unsafe {
                    let result = desc.store_result(v);
                    let result: Shared<ManuallyDrop<V>> = std::mem::transmute(result);
                    let mut result = result.into_owned();
                    break Some(ManuallyDrop::take(result.as_mut()));
                },
                Some(ControlFlow::Store(v)) => {
                    desc.store_result(v);
                    break None;
                }
                Some(ControlFlow::Return(v)) => break v,
            }
        }
    }

    fn loop_until_succeess_without_result<'a>(
        &self,
        _: &'a Guard,
        backoff: &Backoff,
        func: impl Fn() -> Option<ControlFlow<'a, V>>,
    ) {
        loop {
            match func() {
                None => {
                    backoff.spin();
                    continue;
                }
                _ => break,
            }
        }
    }

    fn run_op(&self, desc: &Descriptor<K, V>, guard: &Guard, mode: OpRunMode) -> Option<V> {
        let backoff = Backoff::new();

        let node = unsafe { desc.node.load(atomic::Ordering::Relaxed, guard).deref() };
        match &desc.op {
            NodeOp::Hit => {
                self.change_head_op(desc, guard);
                self.loop_until_succeess(desc, guard, mode, &backoff, || {
                    self.put_node_after_head(node, guard)
                })
            }
            NodeOp::Update { old, new } => {
                self.change_head_op(desc, guard);
                // Ignore control flow after moving a node next to head
                self.loop_until_succeess_without_result(guard, &backoff, || {
                    self.put_node_after_head(node, guard)
                });
                todo!("implement update op")
            }
            NodeOp::Remove => {
                self.loop_until_succeess(desc, guard, mode, &backoff, || {
                    let node = unsafe { desc.node.load(atomic::Ordering::Relaxed, guard).deref() };
                    let old_next = node.next.load(atomic::Ordering::Relaxed, guard);
                    let new_next = old_next.with_tag(1);
                    if node
                        .next
                        .compare_exchange_weak(
                            old_next,
                            new_next,
                            atomic::Ordering::SeqCst,
                            atomic::Ordering::Acquire,
                            guard,
                        )
                        .is_err()
                    {
                        return None;
                    };
                    let prev_node = node.find_prev_alive(guard);
                    let prev_node_next = prev_node.next.load(atomic::Ordering::Relaxed, guard);
                    if ptr::eq(prev_node_next.as_raw(), node) {
                        //prev_node.next.compare_exchange(Shared::from(node as *const _), , success, failure, _)
                        todo!()
                    }

                    self.skiplist
                        .remove(&KeyRef::new(unsafe { node.key.assume_init_ref() }));

                    let value = node.value.load(atomic::Ordering::Relaxed, guard);
                    Some(ControlFlow::Store(value))
                })
            }
        }
    }
}

enum ControlFlow<'a, T> {
    Store(Shared<'a, T>),
    Return(Option<T>),
}

struct Node<K: Send + Sync, V: Send + Sync> {
    key: MaybeUninit<K>,
    value: Atomic<V>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    op: Atomic<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
    fn new<'a>(
        key: K,
        value: V,
        next: Atomic<Node<K, V>>,
        prev: Atomic<Node<K, V>>,
        op: NodeOp<V>,
        _: &'a Guard,
    ) -> Shared<'a, Self> {
        let ptr = Box::leak(
            Owned::new(Self {
                key: MaybeUninit::new(key),
                value: Atomic::new(value),
                next,
                prev,
                op: Atomic::null(),
            })
            .into_box(),
        );
        ptr.op = Atomic::new(Descriptor::new(op, Atomic::from(ptr as *const _)));
        Shared::from(ptr as *const _)
    }

    fn uninit<'a>(next: Atomic<Node<K, V>>, prev: Atomic<Node<K, V>>) -> Owned<Self> {
        Owned::new(Self {
            key: MaybeUninit::uninit(),
            value: Atomic::null(),
            next,
            prev,
            op: Atomic::null(),
        })
    }

    fn find_prev_alive<'a>(&self, guard: &'a Guard) -> &'a Node<K, V> {
        loop {
            let prev = unsafe { self.prev.load(atomic::Ordering::Relaxed, guard).deref() };
            if !prev.is_removed(guard) {
                return prev;
            }
        }
    }

    fn find_next_alive<'a>(&self, guard: &'a Guard) -> &'a Node<K, V> {
        loop {
            let next = unsafe { self.next.load(atomic::Ordering::Relaxed, guard).deref() };
            if !next.is_removed(guard) {
                return next;
            }
        }
    }

    fn is_removed(&self, guard: &Guard) -> bool {
        let prev_next = self.next.load(atomic::Ordering::SeqCst, guard);
        if prev_next.tag() == 0 {
            return false;
        }
        return true;
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
    result: Atomic<V>,
    node: Atomic<Node<K, V>>,
    op: NodeOp<V>,
}

impl<K: Send + Sync, V: Send + Sync> Descriptor<K, V> {
    fn new(op: NodeOp<V>, node: Atomic<Node<K, V>>) -> Self {
        Self {
            result: Atomic::null(),
            node,
            op,
        }
    }

    fn store_result<'a>(&self, value: Shared<'a, V>) -> Shared<'a, V> {
        let value = value.with_tag(1);
        self.result.store(value, atomic::Ordering::SeqCst);
        value
    }
}

enum NodeOp<V> {
    Remove,
    Hit,
    Update { old: Atomic<V>, new: Atomic<V> },
}

enum OpRunMode {
    Help,
    NoHelp,
}
