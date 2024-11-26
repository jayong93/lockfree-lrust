use std::{
    num::NonZeroUsize,
    ops::Deref,
    ptr::{self},
    sync::atomic::{self, AtomicUsize, Ordering},
    usize,
};

use crossbeam::utils::Backoff;
use crossbeam_skiplist::SkipMap;
use op::LruOperation;
use seize::{unprotected, Collector, Linked, LocalGuard};

use crate::{
    atomic_ref_count::internal::{AtomicRefCounted, RefCounted},
    reclaim::RetireShared,
};
use crate::{
    atomic_ref_count::Entry,
    reclaim::{Atomic, Shared},
};

pub struct Lru<K, V>
where
    K: Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    collector: Collector,
    skiplist: SkipMap<RefCounted<K>, Atomic<Node<K, V>>>,
    // `head` and `tail` are sentry nodes which have no valid data
    pub head: Box<Linked<Node<K, V>>>,
    pub tail: Box<Linked<Node<K, V>>>,
    cap: NonZeroUsize,
    size: AtomicUsize,
}

unsafe impl<K: Ord + Send + Sync, V: Send + Sync> Send for Lru<K, V> {}
unsafe impl<K: Ord + Send + Sync, V: Send + Sync> Sync for Lru<K, V> {}

impl<K, V> Drop for Lru<K, V>
where
    K: Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.skiplist.clear();
    }
}

impl<K, V> Lru<K, V>
where
    K: Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        let collector = Collector::new();
        let head = Node::uninit(&collector);
        let tail = Node::uninit(&collector);
        let head_ref = unsafe { &mut *head.as_ptr() };
        let tail_ref = unsafe { &mut *tail.as_ptr() };
        head_ref.next = tail.into();
        tail_ref.prev = head.into();
        Self {
            collector,
            skiplist: SkipMap::new(),
            head: unsafe { head.into_box() },
            tail: unsafe { tail.into_box() },
            cap: capacity,
            size: AtomicUsize::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn remove(&self, key: &K) -> Option<Entry<K, V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = self.collector.enter();
        let mut node = entry.value().load(Ordering::Acquire, &guard);
        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ref, &Backoff::new(), &guard);
                    return None;
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ref, &Backoff::new(), &guard);
                    node = op.get_new_node(&guard);
                }
                Descriptor::Insert(op) => {
                    let backoff = Backoff::new();
                    op.run_op(self, node_ref, &backoff, &guard);
                    let value = op.clone_value(&guard);
                    let desc = Shared::boxed(
                        Descriptor::Remove(op::Remove::new(value.clone())),
                        &self.collector,
                    );
                    if let Ok(old) = node_ref.desc.compare_exchange_weak(
                        old_desc,
                        desc,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { desc.deref() }.run_op(self, node_ref, &Backoff::new(), &guard);
                        entry.remove();
                        unsafe {
                            guard.retire_shared(node);
                            guard.retire_shared(old);
                        }
                        break Some(Entry::new(
                            node_ref.key.as_ref().unwrap().clone(),
                            value,
                            &self.collector,
                        ));
                    }
                    backoff.spin();
                }
            }
        }
    }

    pub fn pop_back(&self) -> Option<Entry<K, V>> {
        let guard = self.collector.enter();
        loop {
            let node = self.tail.prev.load(Ordering::Acquire, &guard);
            if ptr::eq(node.as_ptr(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ref, &Backoff::new(), &guard);
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ref, &Backoff::new(), &guard);
                }
                Descriptor::Insert(op) => {
                    op.run_op(self, node_ref, &Backoff::new(), &guard);
                    let value = op.clone_value(&guard);
                    let desc = Shared::boxed(
                        Descriptor::Remove(op::Remove::new(value.clone())),
                        &self.collector,
                    );
                    if let Ok(old) = node_ref.desc.compare_exchange_weak(
                        old_desc,
                        desc,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                        &guard,
                    ) {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { desc.deref() }.run_op(self, node_ref, &Backoff::new(), &guard);
                        if let Some(entry) =
                            self.skiplist.get(node_ref.key.as_ref().unwrap().deref())
                        {
                            if ptr::eq(
                                entry.value().load(Ordering::Relaxed, &guard).as_ptr(),
                                node_ref,
                            ) {
                                entry.remove();
                            }
                        }

                        unsafe {
                            guard.retire_shared(node);
                            guard.retire_shared(old);
                        }
                        break Some(Entry::new(
                            node_ref.key.as_ref().unwrap().clone(),
                            value,
                            &self.collector,
                        ));
                    }
                }
            }
        }
    }

    pub fn put(&self, key: K, value: V) -> Option<Entry<K, V>> {
        let guard = self.collector.enter();
        let new_node = Node::new(key, value, self);
        let new_node_ref = unsafe { new_node.deref() };
        let desc = new_node_ref.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node_ref.key.as_ref().unwrap().clone();
            let entry = self.skiplist.get_or_insert(key, Atomic::from(new_node));
            let mut node = entry.value().load(Ordering::Acquire, &guard);

            let old_value = if ptr::eq(node.as_ptr(), new_node_ref) {
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
                new_node_ref.desc.load(Ordering::Relaxed, &guard).as_ptr(),
                desc_ref,
            ) {
                // Another thread already inserted our node.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    self.pop_back();
                }
                None
            } else {
                // Another node alreay exists
                let value = desc_ref.clone_value(&guard);
                let (cur_desc, old_value) = 'desc_select: loop {
                    let node_ref = unsafe { node.deref() };
                    let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let new_desc = match old_desc_ref.deref() {
                        // The node was removed, try to allocate a new node
                        Descriptor::Remove(op) => {
                            op.link_prev.run_op(self, node_ref, &backoff, &guard);
                            atomic::fence(Ordering::Release);
                            entry.remove();
                            continue 'outer;
                        }
                        Descriptor::Detach(op) => {
                            op.run_op(self, node_ref, &backoff, &guard);
                            node = op.get_new_node(&guard);
                            continue 'desc_select;
                        }
                        Descriptor::Insert(op) => match op.change_value(value.clone(), &guard) {
                            Err(value) => Shared::boxed(
                                Descriptor::Detach(op::Detach::new(value)),
                                &self.collector,
                            ),
                            Ok(old_value) => {
                                break 'desc_select (old_desc, old_value);
                            }
                        },
                    };

                    old_desc_ref.run_op(self, node_ref, &backoff, &guard);

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
                            guard.retire_shared(old_desc);
                        }
                        break (new_desc, old_value);
                    }
                    drop(unsafe { new_desc.into_box() });
                    backoff.spin();
                };

                unsafe { guard.retire_shared(new_node) };

                let cur_desc_ref = unsafe { cur_desc.deref() };
                cur_desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
                Some(old_value)
            };

            break old_value.map(|v| {
                Entry::new(
                    unsafe { node.deref() }.key.as_ref().unwrap().clone(),
                    v,
                    &self.collector,
                )
            });
        }
    }

    pub fn get(&self, key: &K) -> Option<Entry<K, V>> {
        let Some(entry) = self.skiplist.get(key) else {
            return None;
        };

        let guard = self.collector.enter();
        let mut node = entry.value().load(Ordering::Relaxed, &guard);
        let backoff = Backoff::new();

        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(_) => return None,
                Descriptor::Detach(op) => {
                    atomic::fence(Ordering::Acquire);
                    op.run_op(self, node_ref, &backoff, &guard);
                    node = op.get_new_node(&guard);
                    continue;
                }
                Descriptor::Insert(op) => {
                    atomic::fence(Ordering::Acquire);
                    if !LruOperation::<K, V>::is_finished(op, &guard)
                        || ptr::eq(
                            self.head.next.load(Ordering::Relaxed, &guard).as_ptr(),
                            node_ref,
                        )
                    {
                        return Some(Entry::new(
                            node_ref.key.as_ref().unwrap().clone(),
                            op.clone_value(&guard),
                            &self.collector,
                        ));
                    }
                    let value = op.clone_value(&guard);
                    let new_desc = Shared::boxed(
                        Descriptor::Detach(op::Detach::new(value.clone())),
                        &self.collector,
                    );
                    if node_ref
                        .desc
                        .compare_exchange_weak(
                            old_desc,
                            new_desc,
                            Ordering::SeqCst,
                            Ordering::Acquire,
                            &guard,
                        )
                        .is_ok()
                    {
                        unsafe {
                            guard.retire_shared(old_desc);
                        }
                        unsafe { new_desc.deref() }.run_op(self, node_ref, &backoff, &guard);
                        break Some(Entry::new(
                            node_ref.key.as_ref().unwrap().clone(),
                            value,
                            &self.collector,
                        ));
                    }
                }
            }
            backoff.spin();
        }
    }

    fn put_node_after_head<'a>(
        &self,
        node: &Linked<Node<K, V>>,
        op: &op::Attach,
        backoff: &Backoff,
        guard: &'a LocalGuard,
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
                continue;
            }

            if ptr::eq(mru_node.as_ptr(), node)
                || !ptr::eq(node.prev.load(Ordering::Relaxed, guard).as_ptr(), head)
            {
                break;
            }

            if let Err(err) = mru_node_ref.prev.compare_exchange_weak(
                Shared::from(ptr::from_ref(head)),
                node_ptr.with_tag(0),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                if ptr::eq(err.current.as_ptr(), node) {
                    break;
                }

                atomic::fence(Ordering::Acquire);
                // Our node was inserted once but now it is removed
                if node.next.load(Ordering::Relaxed, guard).tag() != 0 {
                    return;
                }

                if err.current.tag() != 0 {
                    mru_node = self.help_link(
                        unsafe { err.current.deref() },
                        mru_node.with_tag(0),
                        &Backoff::new(),
                        guard,
                    );
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
            if ptr::eq(head_next.as_ptr(), node) {
                break;
            }
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
        node: &'a Linked<Node<K, V>>,
        mut next: Shared<'a, Node<K, V>>,
        backoff: &Backoff,
        guard: &'a LocalGuard,
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
                }
                continue;
            }
            let node_next = node.next.load(Ordering::Relaxed, guard);
            // The node was removed by another thread. Pass responsibility of clean-up to that thread.
            if node_next.tag() != 0 {
                atomic::fence(Ordering::Acquire);
                return next;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_ptr(), node) {
                last = next;
                next = next_prev;
                continue;
            }
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
                    return next;
                }
                continue;
            }
            backoff.spin();
        }
    }

    fn help_detach<'a>(&self, node: &Linked<Node<K, V>>, backoff: &Backoff, guard: &'a LocalGuard) {
        node.mark_next(backoff, guard);
        let mut last: Shared<'a, Node<K, V>> = Shared::null();
        let mut prev = node.prev.load(Ordering::Acquire, guard);
        let mut next = node.next.load(Ordering::Acquire, guard);
        loop {
            // `next`'s prev pointer was already changed from `node` to `prev`.
            if ptr::eq(prev.as_ptr(), next.as_ptr()) {
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
                }
                // Find a next node which is not deleted.
                continue;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_ptr(), node) {
                last = next;
                next = next_prev;
                continue;
            }

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
pub struct Node<K: Send + Sync, V: Send + Sync> {
    key: Option<RefCounted<K>>,
    next: Atomic<Node<K, V>>,
    prev: Atomic<Node<K, V>>,
    desc: Atomic<Descriptor<K, V>>,
}

impl<K: Send + Sync, V: Send + Sync> Drop for Node<K, V> {
    fn drop(&mut self) {
        let fake_guard = unsafe { unprotected() };
        let desc = self.desc.load(Ordering::Acquire, &fake_guard);
        drop(unsafe { desc.try_into_box() });
    }
}

impl<K: Send + Sync + Ord, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(key: K, value: V, lru_cache: &Lru<K, V>) -> Shared<'a, Self> {
        Self::from_ref_counted(
            RefCounted::new(key, &lru_cache.collector),
            RefCounted::new(value, &lru_cache.collector),
            lru_cache,
        )
    }

    fn from_ref_counted<'a>(
        key: RefCounted<K>,
        value: RefCounted<V>,
        lru_cache: &Lru<K, V>,
    ) -> Shared<'a, Self> {
        Shared::boxed(
            Self {
                key: Some(key),
                next: Shared::from(ptr::from_ref(lru_cache.tail.as_ref())).into(),
                prev: Shared::from(ptr::from_ref(lru_cache.head.as_ref())).into(),
                desc: Shared::boxed(
                    Descriptor::Insert(op::Insert::new(value)),
                    &lru_cache.collector,
                )
                .into(),
            },
            &lru_cache.collector,
        )
    }

    #[inline]
    fn uninit<'a>(collector: &Collector) -> Shared<'a, Self> {
        Shared::boxed(
            Self {
                key: None,
                next: Atomic::null(),
                prev: Atomic::null(),
                desc: Atomic::null(),
            },
            collector,
        )
    }

    fn mark_next(&self, backoff: &Backoff, guard: &LocalGuard) {
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
    fn clone_value(&self, guard: &LocalGuard) -> RefCounted<V> {
        match self {
            Self::Insert(op) => op.clone_value(guard),
            Self::Remove(op) => op.value.clone(),
            Self::Detach(op) => op.remove.value.clone(),
        }
    }
    fn run_op(
        &self,
        lru_cache: &Lru<K, V>,
        node: &Linked<Node<K, V>>,
        backoff: &Backoff,
        guard: &LocalGuard,
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
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
        ) -> bool;
        fn store_result(&self, result: Self::Result, guard: &'a LocalGuard) -> bool;
        fn is_finished(&self, guard: &LocalGuard) -> bool;
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
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
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

        fn store_result(&self, _: Self::Result, _: &'a LocalGuard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &LocalGuard) -> bool {
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
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
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

        fn store_result(&self, _: Self::Result, _: &'a LocalGuard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &LocalGuard) -> bool {
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
        K: Send + Sync + Ord,
        V: Send + Sync,
    {
        #[inline]
        pub fn new(value: RefCounted<V>) -> Self {
            Self {
                remove: Remove::new(value),
                new_node: Atomic::null(),
            }
        }

        pub fn get_new_node<'a>(&self, guard: &'a LocalGuard) -> Shared<'a, Node<K, V>> {
            self.new_node.load(Ordering::Acquire, guard).with_tag(0)
        }

        fn create_new_node<'a>(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Linked<Node<K, V>>,
            guard: &'a LocalGuard,
        ) -> Shared<'a, Node<K, V>> {
            let new_node = self.new_node.load(Ordering::Relaxed, guard);
            if !new_node.is_null() {
                atomic::fence(Ordering::Acquire);
                return new_node;
            }
            let key = node.key.as_ref().unwrap().clone();
            let value = self.remove.value.clone();
            let new_node = Node::from_ref_counted(key, value, lru_cache);
            if LruOperation::<K, V>::store_result(self, new_node, guard) {
                new_node
            } else {
                unsafe {
                    drop(new_node.into_box());
                }
                self.new_node.load(Ordering::Relaxed, guard)
            }
        }
    }

    impl<'a, K, V> LruOperation<'a, K, V> for Detach<K, V>
    where
        K: Send + Sync + Ord + 'a,
        V: Send + Sync + 'a,
    {
        type Result = Shared<'a, Node<K, V>>;

        #[inline]
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
        ) -> bool {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return false;
            }

            let new_node = self.create_new_node(lru_cache, node, guard);
            let key = node.key.as_ref().unwrap().deref();
            if let Some(entry) = lru_cache.skiplist.get(key) {
                let node_entry = entry.value();
                let cur_node = node_entry.load(Ordering::Relaxed, guard);
                if !ptr::eq(cur_node.as_ptr(), node) {
                    atomic::fence(Ordering::Acquire);
                } else if node_entry
                    .compare_exchange(
                        cur_node,
                        new_node,
                        Ordering::SeqCst,
                        Ordering::Acquire,
                        guard,
                    )
                    .is_ok()
                {
                    unsafe { guard.retire_shared(Shared::from(ptr::from_ref(node))) };
                }

                let new_desc =
                    unsafe { new_node.deref().desc.load(Ordering::Relaxed, guard).deref() };
                if let Descriptor::Insert(new_op) = new_desc.deref() {
                    new_op
                        .attach
                        .run_op(lru_cache, unsafe { new_node.deref() }, backoff, guard);
                };
            };

            return LruOperation::<K, V>::run_op(&self.remove, lru_cache, node, backoff, guard);
        }

        fn store_result(&self, result: Self::Result, guard: &'a LocalGuard) -> bool {
            self.new_node
                .compare_exchange(
                    Shared::null(),
                    result.with_tag(0),
                    Ordering::Release,
                    Ordering::Acquire,
                    guard,
                )
                .is_ok()
        }

        fn is_finished(&self, guard: &LocalGuard) -> bool {
            LruOperation::<K, V>::is_finished(&self.remove, guard)
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
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
        ) -> bool {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return false;
            }

            lru_cache.put_node_after_head(node, self, &backoff, guard);
            <op::Attach as LruOperation<K, V>>::store_result(self, (), guard)
        }

        fn store_result(&self, _: Self::Result, _: &'a LocalGuard) -> bool {
            store_bool_result(&self.result)
        }

        fn is_finished(&self, _: &LocalGuard) -> bool {
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
            guard: &LocalGuard,
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

        pub fn clone_value(&self, guard: &LocalGuard) -> RefCounted<T> {
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
            node: &Linked<Node<K, V>>,
            backoff: &Backoff,
            guard: &LocalGuard,
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

        fn store_result(&self, _: Self::Result, guard: &'a LocalGuard) -> bool {
            self.value
                .make_constant(Ordering::Release, Ordering::Relaxed, guard)
        }

        fn is_finished(&self, guard: &LocalGuard) -> bool {
            self.value.is_constant(guard)
        }
    }
}
