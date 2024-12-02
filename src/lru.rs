use std::{
    mem,
    num::NonZeroUsize,
    ops::Deref,
    ptr::{self},
    sync::atomic::{self, AtomicUsize, Ordering},
    usize,
};

use crossbeam::utils::{Backoff, CachePadded};
use crossbeam_skiplist::SkipMap;
use op::LruOperation;
use seize::{unprotected, Collector, Guard, Linked, LocalGuard};

use crate::{
    atomic_ref_count::internal::{AtomicRefCounted, RefCounted},
    reclaim::{Pointer, RetireShared},
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
    collector: Box<Collector>,
    skiplist: SkipMap<RefCounted<K>, Atomic<Node<K, V>>>,
    // `head` and `tail` are sentry nodes which have no valid data
    head: Box<Linked<Node<K, V>>>,
    tail: Box<Linked<Node<K, V>>>,
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
        // TODO: Delete this line because it is only for testing
        unsafe {
            Box::leak(mem::replace(
                &mut self.collector,
                Box::new(Collector::new()),
            ))
            .reclaim_all()
        };
    }
}

type NodeEntry<'a, K, V> = crossbeam_skiplist::map::Entry<'a, RefCounted<K>, Atomic<Node<K, V>>>;

impl<K, V> Lru<K, V>
where
    K: Ord + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new(capacity: NonZeroUsize) -> Self {
        let collector = Box::new(Collector::new());
        let head = Node::uninit(&collector);
        let tail = Node::uninit(&collector);
        let head_ref = unsafe { &mut *head.as_ptr() };
        let tail_ref = unsafe { &mut *tail.as_ptr() };
        head_ref.next = CachePadded::new(tail.into());
        tail_ref.prev = CachePadded::new(head.into());
        Self {
            collector,
            skiplist: SkipMap::new(),
            head: unsafe { head.into_box() },
            tail: unsafe { tail.into_box() },
            cap: capacity,
            size: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn head<'a>(&self, guard: &'a LocalGuard<'a>) -> NodeRefGuard<'a, K, V> {
        self.head.try_increase_ref();
        unsafe { NodeRefGuard::new(ptr::from_ref(self.head.as_ref()).into(), guard) }
    }

    #[inline]
    fn tail<'a>(&self, guard: &'a LocalGuard<'a>) -> NodeRefGuard<'a, K, V> {
        self.tail.try_increase_ref();
        unsafe { NodeRefGuard::new(ptr::from_ref(self.tail.as_ref()).into(), guard) }
    }

    #[inline]
    fn copy_entry_node<'a, F>(
        &'a self,
        entry_fn: F,
        guard: &'a LocalGuard<'a>,
    ) -> Option<(NodeRefGuard<'a, K, V>, NodeEntry<'a, K, V>)>
    where
        F: Fn() -> Option<NodeEntry<'a, K, V>>,
    {
        loop {
            let Some(entry) = entry_fn() else { return None };
            loop {
                let node_ptr = entry.value().load(Ordering::Relaxed, guard);
                if let Some(node) = Node::try_copy(node_ptr, guard) {
                    return Some((node, entry));
                }
                if ptr::eq(
                    entry.value().load(Ordering::Relaxed, guard).as_ptr(),
                    node_ptr.as_ptr(),
                ) {
                    break;
                }
            }
        }
    }

    #[inline]
    fn remove_entry<'a>(entry: NodeEntry<'a, K, V>, guard: &'a LocalGuard) {
        if entry.remove() {
            let node = entry.value().load(Ordering::Relaxed, guard);
            Node::release(node, guard);
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn remove(&self, key: &K) -> Option<Entry<K, V>> {
        let guard = self.collector.enter();
        let Some((mut node, mut entry)) = self.copy_entry_node(|| self.skiplist.get(key), &guard)
        else {
            return None;
        };

        loop {
            let node_ptr = node.as_shared();
            let node_ref = unsafe { node_ptr.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ptr, &Backoff::new(), &guard);
                    return None;
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ptr, &Backoff::new(), &guard);
                    if let Some(new_node) = op.get_new_node(&guard) {
                        node = new_node;
                    } else {
                        if let Some((new_node, new_entry)) =
                            self.copy_entry_node(|| self.skiplist.get(key), &guard)
                        {
                            node = new_node;
                            entry = new_entry;
                        } else {
                            return None;
                        };
                    }
                }
                Descriptor::Insert(op) => {
                    let backoff = Backoff::new();
                    op.run_op(self, node_ptr, &backoff, &guard);
                    let value = op.clone_value(&guard);
                    let desc = Shared::boxed(
                        Descriptor::Remove(op::Remove::new(value.clone())),
                        &self.collector,
                    );
                    if node_ref
                        .desc
                        .compare_exchange_weak(
                            old_desc,
                            desc,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                            &guard,
                        )
                        .is_ok()
                    {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { desc.deref() }.run_op(self, node_ptr, &Backoff::new(), &guard);
                        Self::remove_entry(entry, &guard);
                        unsafe {
                            guard.retire_shared(old_desc);
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
            let node = Node::deref_node(&self.tail.prev, Ordering::Relaxed, &guard);
            if ptr::eq(node.as_shared().as_ptr(), self.head.as_ref()) {
                return None;
            }
            let node_ptr = node.as_shared();
            let node_ref = unsafe { node_ptr.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ptr, &Backoff::new(), &guard);
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ptr, &Backoff::new(), &guard);
                }
                Descriptor::Insert(op) => {
                    op.run_op(self, node_ptr, &Backoff::new(), &guard);
                    let value = op.clone_value(&guard);
                    let desc = Shared::boxed(
                        Descriptor::Remove(op::Remove::new(value.clone())),
                        &self.collector,
                    );
                    if node_ref
                        .desc
                        .compare_exchange_weak(
                            old_desc,
                            desc,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                            &guard,
                        )
                        .is_ok()
                    {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { desc.deref() }.run_op(self, node_ptr, &Backoff::new(), &guard);
                        if let Some(entry) =
                            self.skiplist.get(node_ref.key.as_ref().unwrap().deref())
                        {
                            if ptr::eq(
                                entry.value().load(Ordering::Relaxed, &guard).as_ptr(),
                                node_ref,
                            ) {
                                Self::remove_entry(entry, &guard);
                            }
                        }

                        unsafe {
                            guard.retire_shared(old_desc);
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
        let new_node = Node::new(key, value, self, &guard);
        let new_node_ref = unsafe { new_node.as_shared().deref() };
        let desc = new_node_ref.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();
        let key = new_node_ref.key.as_ref().unwrap().clone();

        'outer: loop {
            let entry = self
                .skiplist
                .get_or_insert(key.clone(), Atomic::from(new_node.as_shared()));
            let cur_node = entry.value().load(Ordering::Relaxed, &guard);

            let old_value = if ptr::eq(cur_node.as_ptr(), new_node_ref) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    self.pop_back();
                }
                desc_ref.run_op(self, new_node.as_shared(), &backoff, &guard);
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
                let Some(mut node) = Node::try_copy(cur_node, &guard) else {
                    continue;
                };
                let value = desc_ref.clone_value(&guard);
                let (cur_desc, old_value) = 'desc_select: loop {
                    let node_ptr = node.as_shared();
                    let node_ref = unsafe { node_ptr.deref() };
                    let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let new_desc = match old_desc_ref.deref() {
                        // The node was removed, try to allocate a new node
                        Descriptor::Remove(op) => {
                            op.link_prev.run_op(self, node_ptr, &backoff, &guard);
                            Self::remove_entry(entry, &guard);
                            continue 'outer;
                        }
                        Descriptor::Detach(op) => {
                            op.run_op(self, node_ptr, &backoff, &guard);
                            if let Some(new_node) = op.get_new_node(&guard) {
                                node = new_node;
                            } else {
                                continue 'outer;
                            }
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

                    old_desc_ref.run_op(self, node_ptr, &backoff, &guard);

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

                // Our node will not be used so we can remove it forcely.
                drop(unsafe { new_node.into_node().into_box() });

                let cur_desc_ref = unsafe { cur_desc.deref() };
                cur_desc_ref.run_op(self, node.as_shared(), &backoff, &guard);
                Some(old_value)
            };

            break old_value.map(|v| Entry::new(key, v, &self.collector));
        }
    }

    pub fn get(&self, key: &K) -> Option<Entry<K, V>> {
        let guard = self.collector.enter();
        let backoff = Backoff::new();
        let Some((mut node, _)) = self.copy_entry_node(|| self.skiplist.get(key), &guard) else {
            return None;
        };

        loop {
            let node_ptr = node.as_shared();
            let node_ref = unsafe { node_ptr.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match old_desc_ref.deref() {
                Descriptor::Remove(_) => return None,
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ptr, &backoff, &guard);
                    if let Some(new_node) = op.get_new_node(&guard) {
                        node = new_node;
                    } else {
                        let Some((new_node, _)) =
                            self.copy_entry_node(|| self.skiplist.get(key), &guard)
                        else {
                            return None;
                        };
                        node = new_node;
                    }
                    continue;
                }
                Descriptor::Insert(op) => {
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
                        unsafe { new_desc.deref() }.run_op(self, node_ptr, &backoff, &guard);
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
        node: NodeRefGuard<'a, K, V>,
        op: &op::Attach,
        backoff: &Backoff,
        guard: &'a LocalGuard,
    ) {
        let node_ref = unsafe { node.as_shared().deref() };
        let head = self.head(guard);
        let head_ref = unsafe { head.as_shared().deref() };
        let mut mru_node = Node::deref_node(&head_ref.next, Ordering::Acquire, guard);

        loop {
            // Another thread finished our job instead.
            if <op::Attach as LruOperation<K, V>>::is_finished(op, guard) {
                return;
            }

            let node_next = node_ref.next.load(Ordering::Acquire, guard);

            let mru_node_ref = unsafe { mru_node.node.deref() };
            let mru_prev = mru_node_ref.prev.load(Ordering::Relaxed, guard);
            if mru_prev != Shared::from(ptr::from_ref(head_ref)) {
                mru_node = self.help_link(head.as_shared(), mru_node, &Backoff::new(), guard);
                continue;
            }

            if ptr::eq(mru_node.node.as_ptr(), node_ref)
                || !ptr::eq(
                    node_ref.prev.load(Ordering::Relaxed, guard).as_ptr(),
                    head_ref,
                )
            {
                break;
            }

            atomic::fence(Ordering::Acquire);
            let new_node_next = node_ref.next.load(Ordering::Relaxed, guard);
            if !ptr::eq(new_node_next.as_ptr(), mru_node_ref) {
                // `node.next` is not changed as `mru_node` yet.
                if ptr::eq(node_next.as_ptr(), new_node_next.as_ptr()) {
                    if let Err(err) = node_ref.next.compare_exchange(
                        node_next.without_tag(),
                        mru_node.clone(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        if err.current.tag() != 0 {
                            return;
                        }
                        if !ptr::eq(err.current.as_ptr(), mru_node_ref) {
                            backoff.spin();
                            continue;
                        }
                    }
                }
                // `node` was already inserted to list or `mru_node` is not a MRU node anymore.
                else {
                    continue;
                }
            }

            if let Err(err) = mru_node_ref.prev.compare_exchange_weak(
                head.as_shared(),
                node.clone(),
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                if ptr::eq(err.current.as_ptr(), node_ref) {
                    break;
                }

                atomic::fence(Ordering::Acquire);
                // Our node was inserted once but now it is removed
                if node_ref.next.load(Ordering::Relaxed, guard).tag() != 0 {
                    return;
                }

                backoff.spin();
                continue;
            }
            break;
        }

        loop {
            let head_next = head_ref.next.load(Ordering::Acquire, guard);
            if Shared::from(ptr::from_ref(head_ref)) != node_ref.prev.load(Ordering::Relaxed, guard)
            {
                break;
            }
            if ptr::eq(head_next.as_ptr(), node_ref) {
                break;
            }
            if head_ref
                .next
                .compare_exchange_weak(
                    head_next.without_tag(),
                    node.clone(),
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                if node_ref.next.load(Ordering::Relaxed, guard).tag() != 0 {
                    self.help_link(head.as_shared(), node, &Backoff::new(), guard);
                }
                break;
            }
            backoff.spin();
        }
    }

    fn help_link<'a>(
        &self,
        node: Shared<'a, Node<K, V>>,
        mut next: NodeRefGuard<'a, K, V>,
        backoff: &Backoff,
        guard: &'a LocalGuard,
    ) -> NodeRefGuard<'a, K, V> {
        let node_ref = unsafe { node.deref() };
        let mut last: Option<NodeRefGuard<'a, K, V>> = None;
        loop {
            let next_ref = unsafe { next.node.deref() };
            let next_prev = Node::try_deref_node(&next_ref.prev, Ordering::Acquire, guard);
            match next_prev {
                None => {
                    if let Some(last_guard) = last {
                        next_ref.mark_next(backoff, guard);
                        let next_prev = Node::deref_node(&next_ref.prev, Ordering::Acquire, guard);
                        unsafe { last_guard.as_shared().deref() }
                            .prev
                            .compare_exchange(
                                next.as_shared(),
                                next_prev,
                                Ordering::Release,
                                Ordering::Acquire,
                                guard,
                            )
                            .ok();
                        next = last_guard;
                        last = None;
                    } else {
                        next = Node::deref_node(&next_ref.next, Ordering::Acquire, guard);
                    }
                    continue;
                }
                Some(next_prev) => {
                    let node_next = node_ref.next.load(Ordering::Relaxed, guard);
                    // The node was removed by another thread. Pass responsibility of clean-up to that thread.
                    if node_next.tag() != 0 {
                        break;
                    }
                    // Found a non-deleted previous node and set it as `last`.
                    if !ptr::eq(next_prev.node.as_ptr(), node_ref) {
                        last = Some(next);
                        next = next_prev;
                        continue;
                    }
                    if node_ref
                        .next
                        .compare_exchange_weak(
                            node_next,
                            next.clone(),
                            Ordering::Release,
                            Ordering::Acquire,
                            guard,
                        )
                        .is_ok()
                    {
                        if next_ref.next.load(Ordering::Acquire, guard).tag() == 0 {
                            return next;
                        }
                    }
                    backoff.spin();
                }
            }
        }
        next
    }

    fn help_detach<'a>(
        &self,
        node: Shared<'a, Node<K, V>>,
        backoff: &Backoff,
        guard: &'a LocalGuard,
    ) {
        let node_ref = unsafe { node.deref() };
        node_ref.mark_next(backoff, guard);
        let mut last: Option<NodeRefGuard<K, V>> = None;
        let mut prev = Node::deref_node(&node_ref.prev, Ordering::Acquire, guard);
        let mut next = Node::deref_node(&node_ref.next, Ordering::Acquire, guard);
        loop {
            // `next`'s prev pointer was already changed from `node` to `prev`.
            if ptr::eq(prev.as_shared().as_ptr(), next.as_shared().as_ptr()) {
                return;
            }
            let prev_ref = unsafe { prev.as_shared().deref() };
            {
                let prev_prev = prev_ref.prev.load(Ordering::Relaxed, guard);
                if prev_prev.tag() != 0 {
                    // If we don't mark next here, concurrent `help_link` might not recognize that
                    // this node was deleted.
                    prev_ref.mark_next(backoff, guard);
                    prev = Node::deref_node(&prev_ref.prev, Ordering::Acquire, guard);
                    continue;
                }
            }
            let next_ref = unsafe { next.as_shared().deref() };
            let next_prev = Node::try_deref_node(&next_ref.prev, Ordering::Acquire, guard);
            match next_prev {
                None => {
                    if let Some(last_node) = last {
                        next_ref.mark_next(backoff, guard);
                        let next_prev = Node::deref_node(&next_ref.prev, Ordering::Acquire, guard);
                        unsafe { last_node.as_shared().deref() }
                            .prev
                            .compare_exchange(
                                next.as_shared(),
                                next_prev,
                                Ordering::Release,
                                Ordering::Acquire,
                                guard,
                            )
                            .ok();
                        next = last_node;
                        last = None;
                    } else {
                        // Find a next node which is not deleted.
                        next = Node::deref_node(&next_ref.next, Ordering::Acquire, guard);
                    }
                    continue;
                }
                Some(next_prev) => {
                    // Found a non-deleted previous node and set it as `last`.
                    if !ptr::eq(next_prev.as_shared().as_ptr(), node.as_ptr()) {
                        last = Some(next);
                        next = next_prev;
                        continue;
                    }

                    if let Err(err) = next_ref.prev.compare_exchange_weak(
                        node.without_tag(),
                        prev,
                        Ordering::Release,
                        Ordering::Relaxed,
                        guard,
                    ) {
                        prev = err.new;
                    } else {
                        return;
                    }
                    backoff.spin();
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Node<K: Send + Sync, V: Send + Sync> {
    key: Option<RefCounted<K>>,
    ref_count: CachePadded<AtomicUsize>,
    next: CachePadded<Atomic<Node<K, V>>>,
    prev: CachePadded<Atomic<Node<K, V>>>,
    desc: CachePadded<Atomic<Descriptor<K, V>>>,
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
    fn new<'a>(
        key: K,
        value: V,
        lru_cache: &Lru<K, V>,
        guard: &'a LocalGuard<'a>,
    ) -> NodeRefGuard<'a, K, V> {
        Self::from_ref_counted(
            RefCounted::new(key, &lru_cache.collector),
            RefCounted::new(value, &lru_cache.collector),
            lru_cache,
            guard,
        )
    }

    fn from_ref_counted<'a>(
        key: RefCounted<K>,
        value: RefCounted<V>,
        lru_cache: &Lru<K, V>,
        guard: &'a LocalGuard<'a>,
    ) -> NodeRefGuard<'a, K, V> {
        unsafe {
            NodeRefGuard::new(
                Shared::boxed(
                    Self {
                        key: Some(key),
                        // `ref_count` should be 2 because when it is inserted to LRU, there would
                        // be a skiplist entry for it as well.
                        ref_count: CachePadded::new(AtomicUsize::new(2)),
                        next: CachePadded::new(Shared::from(lru_cache.tail(guard)).into()),
                        prev: CachePadded::new(Shared::from(lru_cache.head(guard)).into()),
                        desc: CachePadded::new(
                            Shared::boxed(
                                Descriptor::Insert(op::Insert::new(value)),
                                &lru_cache.collector,
                            )
                            .into(),
                        ),
                    },
                    &lru_cache.collector,
                ),
                guard,
            )
        }
    }

    #[inline]
    fn uninit<'a>(collector: &Collector) -> Shared<'a, Self> {
        Shared::boxed(
            Self {
                key: None,
                ref_count: CachePadded::new(AtomicUsize::new(1)),
                next: CachePadded::new(Atomic::null()),
                prev: CachePadded::new(Atomic::null()),
                desc: CachePadded::new(Atomic::null()),
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

    fn try_increase_ref(&self) -> bool {
        let mut old_count = self.ref_count.load(Ordering::Relaxed);
        loop {
            if old_count == 0 {
                atomic::fence(Ordering::Acquire);
                return false;
            }
            if let Err(count) = self.ref_count.compare_exchange(
                old_count,
                old_count + 1,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                old_count = count;
            } else {
                break true;
            }
        }
    }

    fn release<'a>(this: Shared<'a, Self>, guard: &'a impl Guard) {
        let mut stack: smallvec::SmallVec<[Shared<'a, Self>; 5]> = smallvec::smallvec![this];
        while let Some(node) = stack.pop() {
            let node_ref = unsafe { node.deref() };
            if node_ref.ref_count.fetch_sub(1, Ordering::Release) == 1 {
                atomic::fence(Ordering::Acquire);
                let next = node_ref.next.load(Ordering::Relaxed, guard);
                if !next.is_null() {
                    stack.push(next);
                }
                let prev = node_ref.prev.load(Ordering::Relaxed, guard);
                if !prev.is_null() {
                    stack.push(prev);
                }
                unsafe { guard.retire_shared(node) };
            };
        }
    }

    fn try_deref_node<'a>(
        ptr: &Atomic<Node<K, V>>,
        order: Ordering,
        guard: &'a LocalGuard,
    ) -> Option<NodeRefGuard<'a, K, V>> {
        loop {
            let node = ptr.load(order, guard);
            debug_assert!(!node.is_null());
            if node.tag() != 0 {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            if !node_ref.try_increase_ref() {
                assert_ne!(ptr.load(order, guard), node);
                continue;
            }
            break Some(NodeRefGuard {
                node: node.without_tag(),
                guard,
            });
        }
    }

    fn deref_node<'a>(
        ptr: &Atomic<Node<K, V>>,
        order: Ordering,
        guard: &'a LocalGuard,
    ) -> NodeRefGuard<'a, K, V> {
        loop {
            let node = ptr.load(order, guard);
            debug_assert!(!node.is_null());
            let node_ref = unsafe { node.deref() };
            if !node_ref.try_increase_ref() {
                let new = ptr.load(order, guard);
                assert_ne!(new.as_ptr(), node.as_ptr());
                continue;
            }
            break NodeRefGuard {
                node: node.without_tag(),
                guard,
            };
        }
    }

    fn try_copy<'a>(
        this: Shared<'a, Node<K, V>>,
        guard: &'a LocalGuard,
    ) -> Option<NodeRefGuard<'a, K, V>> {
        debug_assert!(!this.is_null());
        if unsafe { this.deref() }.try_increase_ref() {
            Some(unsafe { NodeRefGuard::new(this, guard) })
        } else {
            None
        }
    }

    fn remove_cross_ref(&self, guard: &LocalGuard) {
        loop {
            let prev = self.prev.load(Ordering::Relaxed, guard);
            let prev_ref = unsafe { prev.deref() };
            if prev_ref.prev.load(Ordering::Relaxed, guard).tag() != 0 {
                let prev_prev = Node::deref_node(&prev_ref.prev, Ordering::Relaxed, guard);
                self.prev
                    .store::<_, LocalGuard>(prev_prev.into_node().with_tag(1), Ordering::Relaxed);
                Node::release(prev, guard);
                continue;
            }
            let next = self.next.load(Ordering::Relaxed, guard);
            let next_ref = unsafe { next.deref() };
            if next_ref.next.load(Ordering::Relaxed, guard).tag() != 0 {
                let next_next = Node::deref_node(&next_ref.next, Ordering::Relaxed, guard);
                self.next
                    .store::<_, LocalGuard>(next_next.into_node().with_tag(1), Ordering::Relaxed);
                Node::release(next, guard);
                continue;
            }
            break;
        }
    }
}

struct NodeRefGuard<'a, K, V>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    node: Shared<'a, Node<K, V>>,
    guard: &'a LocalGuard<'a>,
}

impl<'a, K, V> Clone for NodeRefGuard<'a, K, V>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    fn clone(&self) -> Self {
        if !self.node.is_null() {
            Node::try_increase_ref(unsafe { self.node.deref() });
        }
        Self { ..*self }
    }
}

impl<'a, K, V> Pointer<'a, Node<K, V>, LocalGuard<'a>> for NodeRefGuard<'a, K, V>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    #[inline]
    unsafe fn new_with_guard(ptr: Shared<'a, Node<K, V>>, guard: &'a LocalGuard<'a>) -> Self {
        Self::new(ptr, guard)
    }

    #[inline]
    fn as_shared(&self) -> Shared<'a, Node<K, V>> {
        self.node
    }
}

impl<'a, K, V> NodeRefGuard<'a, K, V>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    #[inline]
    unsafe fn new(node: Shared<'a, Node<K, V>>, guard: &'a LocalGuard<'a>) -> Self {
        Self {
            node: node.without_tag(),
            guard,
        }
    }

    #[inline]
    fn into_node(self) -> Shared<'a, Node<K, V>> {
        let node = self.node;
        mem::forget(self);
        node
    }
}

impl<'a, K, V> From<NodeRefGuard<'a, K, V>> for Shared<'a, Node<K, V>>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    #[inline]
    fn from(value: NodeRefGuard<'a, K, V>) -> Self {
        value.into_node()
    }
}

impl<'a, K, V> Drop for NodeRefGuard<'a, K, V>
where
    K: Send + Sync + Ord,
    V: Send + Sync,
{
    #[inline]
    fn drop(&mut self) {
        if !self.node.is_null() {
            Node::release(self.node, self.guard);
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
    fn run_op<'a>(
        &self,
        lru_cache: &Lru<K, V>,
        node: Shared<'a, Node<K, V>>,
        backoff: &Backoff,
        guard: &'a LocalGuard,
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

        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
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

        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
        ) -> bool {
            let mut prev;
            loop {
                if LruOperation::<K, V>::is_finished(self, guard) {
                    return false;
                }

                let node_ref = unsafe { node.deref() };

                prev = node_ref.prev.load(Ordering::Relaxed, guard);
                // Another thread already marked this node
                if prev.tag() != 0 {
                    break;
                }
                if let Err(err) = node_ref.prev.compare_exchange_weak(
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
        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
        ) -> bool {
            if !LruOperation::<K, V>::is_finished(self, guard) {
                LruOperation::<K, V>::run_op(&self.link_prev, lru_cache, node, backoff, guard);
                let node_ref = unsafe { node.deref() };
                let prev = node_ref.prev.load(Ordering::Relaxed, guard);
                let next = Node::deref_node(&node_ref.next, Ordering::Relaxed, guard);
                lru_cache.help_link(prev, next, backoff, guard);
                if LruOperation::<K, V>::store_result(self, (), guard) {
                    unsafe { node.deref() }.remove_cross_ref(guard);
                    true
                } else {
                    false
                }
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

        pub fn get_new_node<'a>(&self, guard: &'a LocalGuard) -> Option<NodeRefGuard<'a, K, V>> {
            Node::try_deref_node(&self.new_node, Ordering::Acquire, guard)
        }

        fn create_new_node<'a>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'a, Node<K, V>>,
            guard: &'a LocalGuard,
        ) -> Option<NodeRefGuard<'a, K, V>> {
            // ORDERING: `Relaxed` is fine because reading non-null `new_node` doesn't mean
            // any visible side effect useful.
            let new_node = self.new_node.load(Ordering::Relaxed, guard);
            if !new_node.is_null() {
                return Node::try_deref_node(&self.new_node, Ordering::Acquire, guard);
            }
            let key = unsafe { node.deref() }.key.as_ref().unwrap().clone();
            let value = self.remove.value.clone();
            let new_node = Node::from_ref_counted(key, value, lru_cache, guard);
            if LruOperation::<K, V>::store_result(self, new_node.clone().into_node(), guard) {
                Some(new_node)
            } else {
                drop(unsafe { new_node.into_node().into_box() });
                Node::try_deref_node(&self.new_node, Ordering::Acquire, guard)
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
        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
        ) -> bool {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return false;
            }

            let node_ref = unsafe { node.deref() };
            let new_node = self.create_new_node(lru_cache, node, guard);
            if let Some(new_node) = new_node {
                let new_node_ptr = new_node.as_shared();
                let key = node_ref.key.as_ref().unwrap().deref();
                let mut need_clean_up = false;
                if let Some(entry) = lru_cache.skiplist.get(key) {
                    let node_entry = entry.value();
                    let cur_node = node_entry.load(Ordering::Relaxed, guard);
                    if !ptr::eq(cur_node.as_ptr(), node_ref) {
                        atomic::fence(Ordering::Acquire);
                    }
                    // Here, we use `Shared` type instead of `NodeRefGuard` because we don't track
                    // node references in skiplist entries.
                    else if node_entry
                        .compare_exchange(
                            cur_node,
                            new_node,
                            Ordering::SeqCst,
                            Ordering::Acquire,
                            guard,
                        )
                        .is_ok()
                    {
                        need_clean_up = true;
                    }
                };
                let new_desc = unsafe {
                    new_node_ptr
                        .deref()
                        .desc
                        .load(Ordering::Relaxed, guard)
                        .deref()
                };
                // We have to insert the new node because otherwise threads poping a node from back
                // might not see the node after helping this op.
                if let Descriptor::Insert(new_op) = new_desc.deref() {
                    new_op
                        .attach
                        .run_op(lru_cache, new_node_ptr, backoff, guard);
                };
                if need_clean_up {
                    // Marking `new_node` so it can be reclaimed safely. We have to do marking only
                    // after insertion because otherwise threads poping a node from back might not
                    // see the newly inserted node after helping this op.
                    self.new_node
                        .swap(new_node_ptr.with_tag(1), Ordering::Release, guard);
                    Node::release(new_node_ptr, guard);
                }
            }

            return LruOperation::<K, V>::run_op(&self.remove, lru_cache, node, backoff, guard);
        }

        fn store_result(&self, result: Self::Result, guard: &'a LocalGuard) -> bool {
            self.new_node
                .compare_exchange(
                    Shared::null(),
                    result.without_tag(),
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
        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
        ) -> bool {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return false;
            }

            lru_cache.put_node_after_head(
                Node::try_copy(node, guard).unwrap(),
                self,
                &backoff,
                guard,
            );
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

        fn run_op<'g>(
            &self,
            lru_cache: &Lru<K, V>,
            node: Shared<'g, Node<K, V>>,
            backoff: &Backoff,
            guard: &'g LocalGuard<'g>,
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
