use std::{
    num::NonZeroUsize,
    ops::Deref,
    process::abort,
    ptr::{self},
    sync::{
        atomic::{self, AtomicUsize, Ordering},
        LazyLock,
    },
    usize,
};

use crossbeam::{
    epoch::{pin, Atomic, Guard, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;
use op::LruOperation;

use crate::atomic_ref_count::{AtomicRefCounted, RefCounted};

static REMOVED_NODES: LazyLock<SkipMap<usize, Atomic<(usize, u64)>>> =
    LazyLock::new(|| SkipMap::new());

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
        let mut head = Node::uninit();
        let head_ptr = ptr::from_ref(head.as_ref());
        let mut tail = Node::uninit();
        let tail_ptr = ptr::from_ref(tail.as_ref());
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

    pub fn len(&self) -> usize {
        println!("{}", self.skiplist.len());
        self.size.load(Ordering::Relaxed)
    }

    pub fn remove(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let mut node = entry.value().load(Ordering::Relaxed, &guard);
        let backoff = Backoff::new();
        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match &old_desc_ref {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ref, &backoff, &guard);
                    return None;
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ref, &backoff, &guard);
                    assert!(op.is_finished(&guard));
                    node = op.get_new_node(&guard);
                }
                Descriptor::Insert(op) => {
                    op.run_op(self, node_ref, &backoff, &guard);
                    let value = op.clone_value(&guard);
                    let desc = Owned::new(Descriptor::Remove(op::Remove::new(value.clone())));
                    if let Ok(new) = node_ref.desc.compare_exchange_weak(
                        old_desc,
                        desc,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { new.deref() }.run_op(self, node_ref, &backoff, &guard);
                        atomic::fence(Ordering::Release);
                        entry.remove();
                        assert!(matches!(
                            unsafe { node_ref.desc.load(Ordering::Relaxed, &guard).deref() },
                            Descriptor::Remove(_)
                        ));
                        decrease_removed_node(node.as_raw(), line!(), &guard);
                        unsafe { guard.defer_destroy(node) };
                        unsafe {
                            guard.defer_destroy(old_desc);
                        }
                        break Some(value);
                    }
                    backoff.spin();
                }
            }
        }
    }

    pub fn pop_back(&self) -> Option<RefCounted<V>> {
        let guard = pin();
        let mut node_history = Vec::with_capacity(10);
        loop {
            let node = self.tail.prev.load(Ordering::Acquire, &guard);
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            node_history.push(node);
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match &old_desc_ref {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ref, &Backoff::new(), &guard);
                }
                Descriptor::Detach(op) => {
                    assert!(self.skiplist.get(node_ref.key.as_ref().unwrap().deref()).is_some());
                    op.run_op(self, node_ref, &Backoff::new(), &guard);
                    assert!(op.is_finished(&guard));
                }
                Descriptor::Insert(op) => {
                    op.run_op(self, node_ref, &Backoff::new(), &guard);
                    let value = op.clone_value(&guard);
                    let desc = Owned::new(Descriptor::Remove(op::Remove::new(value.clone())));
                    if let Ok(new) = node_ref.desc.compare_exchange_weak(
                        old_desc,
                        desc,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { new.deref() }.run_op(self, node_ref, &Backoff::new(), &guard);
                        if let Some(entry) =
                            self.skiplist.get(node_ref.key.as_ref().unwrap().deref())
                        {
                            if ptr::eq(
                                entry.value().load(Ordering::Relaxed, &guard).as_raw(),
                                node_ref,
                            ) {
                                atomic::fence(Ordering::Release);
                                entry.remove();
                                assert!(matches!(
                                    unsafe {
                                        node_ref.desc.load(Ordering::Relaxed, &guard).deref()
                                    },
                                    Descriptor::Remove(_)
                                ));
                            }
                        }

                        decrease_removed_node(node.as_raw(), line!(), &guard);
                        unsafe { guard.defer_destroy(node) };
                        unsafe {
                            guard.defer_destroy(old_desc);
                        }
                        break Some(value);
                    }
                }
            }
        }
    }

    pub fn put(
        &self,
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
    ) -> Option<RefCounted<V>> {
        let guard = pin();
        let new_node = Node::new(key, value, self, &guard);
        increase_removed_node(new_node.as_raw(), line!(), &guard);
        let new_node_ref = unsafe { new_node.deref() };
        let desc = new_node_ref.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node_ref.key.as_ref().unwrap().clone();
            let entry = self.skiplist.get_or_insert(key, Atomic::from(new_node));
            let mut node = entry.value().load(Ordering::Acquire, &guard);

            let old_value = if ptr::eq(node.as_raw(), new_node_ref) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    self.pop_back();
                }
                desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
                None
            } else if !ptr::eq(
                new_node_ref.desc.load(Ordering::Relaxed, &guard).as_raw(),
                desc_ref,
            ) {
                // Another thread already inserted our node.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    assert!(self.pop_back().is_some());
                }
                None
            } else {
                // Another node alreay exists
                let value = desc_ref.clone_value(&guard);
                let (cur_desc, old_value) = 'desc_select: loop {
                    let node_ref = unsafe { node.deref() };
                    let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let new_desc = match old_desc_ref {
                        // The node was removed, try to allocate a new node
                        Descriptor::Remove(op) => {
                            op.link_prev.run_op(self, node_ref, &backoff, &guard);
                            atomic::fence(Ordering::Release);
                            entry.remove();
                            assert!(matches!(
                                unsafe { node_ref.desc.load(Ordering::Relaxed, &guard).deref() },
                                Descriptor::Remove(_)
                            ));
                            continue 'outer;
                        }
                        Descriptor::Detach(op) => {
                            op.run_op(self, node_ref, &backoff, &guard);
                            assert!(op.is_finished(&guard));
                            node = op.get_new_node(&guard);
                            assert!(!node.is_null());
                            continue 'desc_select;
                        }
                        Descriptor::Insert(op) => match op.change_value(value.clone(), &guard) {
                            Err(value) => Owned::new(Descriptor::Detach(op::Detach::new(value)))
                                .into_shared(&guard),
                            Ok(old_value) => {
                                break 'desc_select (old_desc, old_value);
                            }
                        },
                    };

                    old_desc_ref.run_op(self, node_ref, &backoff, &guard);
                    assert!(
                        matches!(old_desc_ref, Descriptor::Insert(op) if LruOperation::<K,V>::is_finished(op, &guard))
                    );

                    if node_ref
                        .desc
                        .compare_exchange_weak(
                            old_desc,
                            new_desc,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                            &guard,
                        )
                        .is_ok()
                    {
                        let old_value = old_desc_ref.clone_value(&guard);
                        unsafe {
                            guard.defer_destroy(old_desc);
                        }
                        assert!(matches!(unsafe { new_desc.deref() }, Descriptor::Detach(_)));
                        break (new_desc, old_value);
                    }
                    drop(unsafe { new_desc.into_owned() });
                    backoff.spin();
                };

                decrease_removed_node(new_node.as_raw(), line!(), &guard);
                unsafe { guard.defer_destroy(new_node) };

                let cur_desc_ref = unsafe { cur_desc.deref() };
                cur_desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
                Some(old_value)
            };

            break old_value;
        }
    }

    pub fn get(&self, key: &K) -> Option<RefCounted<V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = pin();
        let mut node = entry.value().load(Ordering::Relaxed, &guard);
        let backoff = Backoff::new();

        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref {
                Descriptor::Remove(_) => return None,
                Descriptor::Detach(op) => {
                    atomic::fence(Ordering::Acquire);
                    op.run_op(self, node_ref, &backoff, &guard);
                    assert!(op.is_finished(&guard));
                    node = op.get_new_node(&guard);
                    continue;
                }
                Descriptor::Insert(op) => {
                    atomic::fence(Ordering::Acquire);
                    if !LruOperation::<K, V>::is_finished(op, &guard)
                        || ptr::eq(
                            self.head.next.load(Ordering::Relaxed, &guard).as_raw(),
                            node_ref,
                        )
                    {
                        return Some(op.clone_value(&guard));
                    }
                    let value = op.clone_value(&guard);
                    let new_desc = Owned::new(Descriptor::Detach(op::Detach::new(value.clone())));
                    if node_ref
                        .desc
                        .compare_exchange_weak(
                            old_desc,
                            Shared::from(ptr::from_ref(new_desc.as_ref())),
                            Ordering::SeqCst,
                            Ordering::Acquire,
                            &guard,
                        )
                        .is_ok()
                    {
                        let new_desc = new_desc.into_shared(&guard);
                        unsafe {
                            guard.defer_destroy(old_desc);
                        }
                        unsafe { new_desc.deref() }.run_op(self, node_ref, &backoff, &guard);
                        break Some(value);
                    }
                }
            }
            backoff.spin();
        }
    }

    fn put_node_after_head<'a>(
        &self,
        node: &Node<K, V>,
        op: &op::Attach,
        backoff: &Backoff,
        guard: &'a Guard,
    ) {
        let node_ptr = Shared::from(ptr::from_ref(node));

        let head = self.head.as_ref();
        let mut mru_node = head.next.load(Ordering::Acquire, guard);

        loop {
            // Another thread finished our job instead.
            if <op::Attach as LruOperation<K, V>>::is_finished(op, guard) {
                return;
            }

            let mru_node_ref = unsafe { mru_node.deref() };
            let mru_prev = mru_node_ref.prev.load(Ordering::Relaxed, guard);
            if mru_prev != Shared::from(ptr::from_ref(head)) {
                mru_node = self.help_link(head, mru_node.with_tag(0), &Backoff::new(), guard);
                assert!(!mru_node.is_null());
                assert!(mru_node.tag() == 0);
                assert!(!ptr::eq(head, mru_node.as_raw()));
                continue;
            }

            if ptr::eq(mru_node.as_raw(), node)
                || !ptr::eq(node.prev.load(Ordering::Relaxed, guard).as_raw(), head)
            {
                break;
            }

            assert!(!ptr::eq(
                mru_node_ref.next.load(Ordering::Relaxed, guard).as_raw(),
                node
            ));
            if let Err(err) = mru_node_ref.prev.compare_exchange_weak(
                Shared::from(ptr::from_ref(head)),
                node_ptr.with_tag(0),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                if ptr::eq(err.current.as_raw(), node) {
                    break;
                }

                atomic::fence(Ordering::Acquire);
                // Our node was inserted once but now it is removed
                if node.next.load(Ordering::Relaxed, guard).tag() != 0 {
                    assert!(LruOperation::<K, V>::is_finished(op, guard));
                    return;
                }

                if err.current.tag() != 0 {
                    mru_node = self.help_link(
                        unsafe { err.current.deref() },
                        mru_node.with_tag(0),
                        &Backoff::new(),
                        guard,
                    );
                    assert!(mru_node.tag() == 0);
                    assert!(!mru_node.is_null());
                } else {
                    mru_node = err.current;
                }
                backoff.spin();
                continue;
            }
            self.help_link(node, mru_node.with_tag(0), &Backoff::new(), guard);
            break;
        }

        loop {
            let head_next = head.next.load(Ordering::Acquire, guard);
            if Shared::from(ptr::from_ref(head)) != node.prev.load(Ordering::Relaxed, guard) {
                break;
            }
            if ptr::eq(head_next.as_raw(), node) {
                break;
            }
            assert!(!ptr::eq(
                head.prev.load(Ordering::Relaxed, guard).as_raw(),
                node
            ));
            if head
                .next
                .compare_exchange_weak(
                    head_next.with_tag(0),
                    node_ptr.with_tag(0),
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                if node.next.load(Ordering::Relaxed, guard).tag() != 0 {
                    self.help_link(head, node_ptr, &Backoff::new(), guard);
                }
                break;
            }
            backoff.spin();
        }
    }

    fn help_link<'a>(
        &self,
        node: &'a Node<K, V>,
        mut next: Shared<'a, Node<K, V>>,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> Shared<'a, Node<K, V>> {
        let mut last: Shared<'a, Node<K, V>> = Shared::null();
        loop {
            let next_ref = unsafe { next.deref() };
            let next_prev = next_ref.prev.load(Ordering::Acquire, guard);
            if next_prev.tag() != 0 {
                if !last.is_null() {
                    next_ref.mark_next(backoff, guard);
                    unsafe { last.deref() }
                        .prev
                        .compare_exchange(
                            next.with_tag(0),
                            next_prev.with_tag(0),
                            Ordering::Release,
                            Ordering::Acquire,
                            guard,
                        )
                        .ok();
                    next = last;
                    last = Shared::null();
                } else {
                    next = next_ref.next.load(Ordering::Acquire, guard).with_tag(0);
                    assert!(!ptr::eq(self.head.as_ref(), next.as_raw()));
                    assert!(!next.is_null());
                }
                continue;
            }
            let node_next = node.next.load(Ordering::Relaxed, guard);
            // The node was removed by another thread. Pass responsibility of clean-up to that thread.
            if node_next.tag() != 0 {
                atomic::fence(Ordering::Acquire);
                assert!(!next.is_null());
                return next;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_raw(), node) {
                last = next;
                assert!(!ptr::eq(next_prev.as_raw(), self.head.as_ref()));
                assert!(!next_prev.is_null());
                assert!(!ptr::eq(self.head.as_ref(), next.as_raw()));
                next = next_prev;
                continue;
            }
            assert!(!ptr::eq(
                node.prev.load(Ordering::Relaxed, guard).as_raw(),
                next.as_raw()
            ));
            if node
                .next
                .compare_exchange_weak(
                    node_next,
                    next.with_tag(0),
                    Ordering::Release,
                    Ordering::Acquire,
                    guard,
                )
                .is_ok()
            {
                if next_ref.next.load(Ordering::Acquire, guard).tag() == 0 {
                    if next.is_null() {
                        abort();
                    }
                    return next;
                }
                continue;
            }
            backoff.spin();
        }
    }

    fn help_detach<'a>(&self, node: &Node<K, V>, backoff: &Backoff, guard: &'a Guard) {
        node.mark_next(backoff, guard);
        let mut last: Shared<'a, Node<K, V>> = Shared::null();
        let mut prev = node.prev.load(Ordering::Acquire, guard);
        let mut next = node.next.load(Ordering::Acquire, guard);
        if next.is_null() {
            abort()
        }
        loop {
            // `next`'s prev pointer was already changed from `node` to `prev`.
            if ptr::eq(prev.as_raw(), next.as_raw()) {
                return;
            }
            let prev_ref = unsafe { prev.deref() };
            {
                let prev_prev = prev_ref.prev.load(Ordering::Acquire, guard);
                if prev_prev.tag() != 0 {
                    // If we don't mark next here, concurrent `help_link` might not recognize that
                    // this node was deleted.
                    prev_ref.mark_next(backoff, guard);
                    prev = prev_prev.with_tag(0);
                    continue;
                }
            }
            let next_ref = unsafe { next.deref() };
            let next_prev = next_ref.prev.load(Ordering::Acquire, guard);
            // If `prev` was deleted
            if next_prev.tag() != 0 {
                if !last.is_null() {
                    next_ref.mark_next(backoff, guard);
                    unsafe { last.deref() }
                        .prev
                        .compare_exchange(
                            next.with_tag(0),
                            next_prev.with_tag(0),
                            Ordering::Release,
                            Ordering::Acquire,
                            guard,
                        )
                        .ok();
                    next = last;
                    last = Shared::null();
                } else {
                    next = next_ref.next.load(Ordering::Acquire, guard).with_tag(0);
                    assert!(!next.is_null());
                }
                // Find a next node which is not deleted.
                continue;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_raw(), node) {
                last = next;
                assert!(!next_prev.is_null());
                next = next_prev;
                continue;
            }

            assert!(!ptr::eq(next_ref, prev.as_raw()));
            assert!(!ptr::eq(
                next_ref.next.load(Ordering::Relaxed, guard).as_raw(),
                prev.as_raw()
            ));
            if next_ref
                .prev
                .compare_exchange_weak(
                    Shared::from(ptr::from_ref(node)),
                    prev.with_tag(0),
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

#[derive(Debug)]
pub(crate) struct Node<K: Send + Sync, V: Send + Sync> {
    key: Option<RefCounted<K>>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    desc: Atomic<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let desc = std::mem::replace(&mut self.desc, Atomic::null());
        drop(unsafe { desc.try_into_owned() });
    }
}

impl<K: Send + Sync + Ord, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
        lru_cache: &Lru<K, V>,
        guard: &'a Guard,
    ) -> Shared<'a, Self> {
        Owned::new(Self {
            key: Some(key.into()),
            next: Atomic::from(ptr::from_ref(lru_cache.tail.as_ref())),
            prev: Atomic::from(ptr::from_ref(lru_cache.head.as_ref())),
            desc: Atomic::new(Descriptor::Insert(op::Insert::new(value.into()))),
        })
        .into_shared(guard)
    }

    #[inline]
    fn uninit<'a>() -> Owned<Self> {
        Owned::new(Self {
            key: None,
            next: Atomic::null(),
            prev: Atomic::null(),
            desc: Atomic::null(),
        })
    }

    fn mark_next(&self, backoff: &Backoff, guard: &Guard) {
        loop {
            let next = self.next.load(Ordering::Acquire, guard);
            if next.tag() != 0 {
                return;
            }
            if self
                .next
                .compare_exchange_weak(
                    next,
                    next.with_tag(1),
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

enum Descriptor<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// The node is removed.
    Remove(op::Remove<V>),
    /// The node is removed but another node with the same key-value will be inserted.
    Detach(op::Detach<K, V>),
    Insert(op::Insert<V>),
}

impl<K, V> Descriptor<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn clone_value(&self, guard: &Guard) -> RefCounted<V> {
        match self {
            Self::Insert(op) => op.clone_value(guard),
            Self::Remove(op) => op.value.clone(),
            Self::Detach(op) => op.remove.value.clone(),
        }
    }
    fn run_op(
        &self,
        lru_cache: &Lru<K, V>,
        node: &Node<K, V>,
        backoff: &Backoff,
        guard: &Guard,
    ) -> bool
    where
        K: Send + Sync + Ord,
    {
        match self {
            Self::Remove(op) => op.run_op(lru_cache, node, backoff, guard),
            Self::Detach(op) => op.run_op(lru_cache, node, backoff, guard),
            Self::Insert(op) => op.run_op(lru_cache, node, backoff, guard),
        }
    }
}

fn decrease_removed_node<K, V>(addr: *const Node<K, V>, linenu: u32, guard: &Guard)
where
    K: Send + Sync,
    V: Send + Sync,
{
    let entry = REMOVED_NODES.get(&(addr as usize)).unwrap();
    let v = entry.value();
    let old = v.load(Ordering::SeqCst, guard);
    let (count, line) = unsafe { old.as_ref().unwrap() };
    assert_eq!(*count, 1);
    let new = Owned::new((count - 1, line << 32 | linenu as u64)).into_shared(guard);
    if let Err(err) = v.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard) {
        let cur = unsafe { err.current.deref() };
        panic!("{cur:?}");
    }
}
fn increase_removed_node<K, V>(addr: *const Node<K, V>, linenu: u32, guard: &Guard)
where
    K: Send + Sync,
    V: Send + Sync,
{
    let entry = REMOVED_NODES.get_or_insert(addr as usize, Atomic::null());
    let v = entry.value();
    let old = v.load(Ordering::SeqCst, guard);
    let new = if let Some((count, line)) = unsafe { old.as_ref() } {
        assert_eq!(*count, 0);
        Owned::new((1, line << 32 | linenu as u64)).into_shared(guard)
    } else {
        Owned::new((1, linenu as u64)).into_shared(guard)
    };
    v.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst, guard)
        .ok();
}

mod op {
    use atomic::AtomicBool;

    use super::*;

    fn store_bool_result(result: &AtomicBool) -> bool {
        if result.load(Ordering::Relaxed) {
            atomic::fence(Ordering::Acquire);
            return false;
        }
        !result.fetch_or(true, Ordering::AcqRel)
    }

    fn check_bool_result(result: &AtomicBool) -> bool {
        if result.load(Ordering::Relaxed) {
            atomic::fence(Ordering::Acquire);
            return true;
        }
        false
    }

    pub(crate) trait LruOperation<'a, K, V>
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result;

        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool;
        fn store_result(&self, result: Self::Result, guard: &'a Guard) -> bool;
        fn is_finished(&self, guard: &Guard) -> bool;
    }

    pub struct LinkPrev {
        result: AtomicBool,
    }

    impl LinkPrev {
        fn new() -> Self {
            Self {
                result: AtomicBool::new(false),
            }
        }
    }

    impl<'a, K, V> LruOperation<'a, K, V> for LinkPrev
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = ();

        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool {
            let mut prev;
            loop {
                if LruOperation::<K, V>::is_finished(self, guard) {
                    return false;
                }

                prev = node.prev.load(Ordering::Relaxed, guard);
                // Another thread already marked this node
                if prev.tag() != 0 {
                    break;
                }
                assert!(!ptr::eq(node, prev.as_raw()));
                if let Err(err) = node.prev.compare_exchange_weak(
                    prev,
                    prev.with_tag(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    guard,
                ) {
                    // Another thread removed the prev node and update `prev` pointer of this node
                    if err.current.tag() == 0 {
                        continue;
                    }
                } else {
                    break;
                };
                backoff.spin();
            }

            lru_cache.help_detach(node, backoff, guard);
            LruOperation::<K, V>::store_result(self, (), guard)
        }

        fn store_result(&self, _: Self::Result, _: &'a Guard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &Guard) -> bool {
            check_bool_result(&self.result)
        }
    }

    pub struct Remove<T: Send + Sync> {
        pub value: RefCounted<T>,
        pub(crate) link_prev: LinkPrev,
        result: AtomicBool,
    }

    impl<T: Send + Sync> Remove<T> {
        #[inline]
        pub fn new(value: RefCounted<T>) -> Self {
            Self {
                value,
                link_prev: LinkPrev::new(),
                result: AtomicBool::new(false),
            }
        }
    }

    impl<'a, K, V> LruOperation<'a, K, V> for Remove<V>
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = ();
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool {
            if !LruOperation::<K, V>::is_finished(self, guard) {
                LruOperation::<K, V>::run_op(&self.link_prev, lru_cache, node, backoff, guard);
                let prev = node.prev.load(Ordering::Relaxed, guard);
                let next = node.next.load(Ordering::Acquire, guard);
                lru_cache.help_link(unsafe { prev.deref() }, next.with_tag(0), backoff, guard);
                LruOperation::<K, V>::store_result(self, (), guard)
            } else {
                false
            }
        }

        fn store_result(&self, _: Self::Result, _: &'a Guard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &Guard) -> bool {
            check_bool_result(&self.result)
        }
    }

    pub struct Detach<K, V>
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        pub(crate) remove: Remove<V>,
        new_node: Atomic<Node<K, V>>,
    }

    impl<K, V> Detach<K, V>
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        #[inline]
        pub fn new(value: RefCounted<V>) -> Self {
            Self {
                remove: Remove::new(value),
                new_node: Atomic::null(),
            }
        }

        pub fn get_new_node<'a>(&self, guard: &'a Guard) -> Shared<'a, Node<K, V>> {
            self.new_node.load(Ordering::Acquire, guard).with_tag(0)
        }
    }

    impl<'a, K, V> LruOperation<'a, K, V> for Detach<K, V>
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = Shared<'a, Node<K, V>>;

        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool {
            if !LruOperation::<K, V>::is_finished(self, guard) {
                let key = node.key.as_ref().unwrap().clone();
                let Some(entry) = lru_cache.skiplist.get(key.deref()) else {
                    // This op was finished by another thread and the entry was deleted later.
                    assert!(LruOperation::<K, V>::is_finished(self, guard));
                    return false;
                };
                let node_entry = entry.value();
                let cur_node = node_entry.load(Ordering::Relaxed, guard);
                let result = if !ptr::eq(cur_node.as_raw(), node) {
                    cur_node
                } else {
                    let value = self.remove.value.clone();
                    let new_node = Node::new(key.clone(), value, lru_cache, guard);
                    increase_removed_node(new_node.as_raw(), line!(), guard);

                    if let Err(err) = node_entry.compare_exchange(
                        cur_node,
                        new_node,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                        guard,
                    ) {
                        unsafe {
                            decrease_removed_node(new_node.as_raw(), line!(), &guard);
                            drop(new_node.into_owned());
                        }
                        err.current
                    } else {
                        decrease_removed_node(ptr::from_ref(node), line!(), &guard);
                        unsafe { guard.defer_destroy(Shared::from(ptr::from_ref(node))) };
                        new_node
                    }
                };

                let new_desc =
                    unsafe { result.deref().desc.load(Ordering::Relaxed, guard).deref() };
                if let Descriptor::Insert(new_op) = new_desc {
                    new_op
                        .attach
                        .run_op(lru_cache, unsafe { result.deref() }, backoff, guard);
                };
                LruOperation::<K, V>::store_result(self, result, guard);
            }
            return LruOperation::<K, V>::run_op(&self.remove, lru_cache, node, backoff, guard);
        }

        fn store_result(&self, result: Self::Result, guard: &'a Guard) -> bool {
            assert!(result.is_null() == false);
            if !self.new_node.load(Ordering::Relaxed, guard).is_null() {
                atomic::fence(Ordering::Acquire);
                return false;
            }
            if self
                .new_node
                .compare_exchange(
                    Shared::null(),
                    result.with_tag(0),
                    Ordering::Release,
                    Ordering::Acquire,
                    guard,
                )
                .is_ok()
            {
                return true;
            }
            false
        }

        fn is_finished(&self, guard: &Guard) -> bool {
            self.new_node.load(Ordering::Acquire, guard).is_null() == false
        }
    }

    pub struct Attach {
        result: AtomicBool,
    }

    impl<'a, K, V> LruOperation<'a, K, V> for Attach
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = ();
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return false;
            }

            lru_cache.put_node_after_head(node, self, &backoff, guard);
            <op::Attach as LruOperation<K, V>>::store_result(self, (), guard)
        }

        fn store_result(&self, _: Self::Result, _: &'a Guard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &Guard) -> bool {
            check_bool_result(&self.result)
        }
    }

    pub struct Insert<T>
    where
        T: Send + Sync,
    {
        pub(crate) attach: Attach,
        value: AtomicRefCounted<T>,
    }

    impl<T> Insert<T>
    where
        T: Send + Sync,
    {
        #[inline]
        pub fn new(value: RefCounted<T>) -> Self {
            Self {
                value: AtomicRefCounted::new(value),
                attach: Attach {
                    result: AtomicBool::new(false),
                },
            }
        }

        pub fn change_value(
            &self,
            mut new_value: RefCounted<T>,
            guard: &Guard,
        ) -> Result<RefCounted<T>, RefCounted<T>> {
            loop {
                if self.value.is_constant(guard) {
                    return Err(new_value);
                }
                let Ok(old_value) = self
                    .value
                    .try_clone_inner(Ordering::Relaxed, guard)
                    .map(Option::unwrap)
                else {
                    continue;
                };
                if let Err(err) = self.value.compare_exchange_weak(
                    Some(&*old_value),
                    Some(new_value),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    guard,
                ) {
                    new_value = err.new.unwrap();
                    continue;
                }
                break Ok(old_value);
            }
        }

        pub fn clone_value(&self, guard: &Guard) -> RefCounted<T> {
            self.value.clone_inner(Ordering::Relaxed, guard).unwrap()
        }
    }

    impl<'a, K, V> LruOperation<'a, K, V> for Insert<V>
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = &'a RefCounted<V>;
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) -> bool {
            LruOperation::run_op(&self.attach, lru_cache, node, backoff, guard);
            loop {
                let Ok(new_value) = self
                    .value
                    .try_clone_inner(Ordering::Acquire, guard)
                    .map(|v| v.unwrap())
                else {
                    // Someone changed value. Retry.
                    continue;
                };
                if LruOperation::<K, V>::is_finished(self, guard) {
                    return false;
                }
                if LruOperation::<K, V>::store_result(self, &new_value, guard) {
                    return true;
                }
                backoff.spin();
            }
        }

        fn store_result(&self, new_value: Self::Result, guard: &'a Guard) -> bool {
            self.value.make_constant(
                Some(&new_value),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            )
        }

        fn is_finished(&self, guard: &Guard) -> bool {
            self.value.is_constant(guard)
        }
    }
}
