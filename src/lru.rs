use std::{
    num::NonZeroUsize,
    ops::Deref,
    process::abort,
    ptr::{self},
    sync::atomic::{self, AtomicUsize, Ordering},
    usize,
};

use crossbeam::{
    epoch::{pin, Atomic, Guard, Owned, Shared},
    utils::Backoff,
};
use crossbeam_skiplist::SkipMap;
use op::LruOperation;

use crate::atomic_ref_count::{AtomicRefCounted, RefCounted};

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

    pub fn size(&self) -> usize {
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
                        entry.remove();
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { new.deref() }.run_op(self, node_ref, &backoff, &guard);
                        unsafe {
                            guard.defer_destroy(old_desc);
                            guard.defer_destroy(node);
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
        let backoff = Backoff::new();
        loop {
            let node = self.tail.prev.load(Ordering::Acquire, &guard);
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match &old_desc_ref {
                Descriptor::Remove(op) => {
                    op.link_prev.run_op(self, node_ref, &backoff, &guard);
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ref, &backoff, &guard);
                    let new_node = op.get_new_node(&guard);
                    unsafe {
                        let new_desc = new_node.deref().desc.load(Ordering::Relaxed, &guard);
                        match new_desc.deref() {
                            Descriptor::Insert(op) => {
                                op.attach
                                    .run_op(self, new_node.deref(), &Backoff::new(), &guard);
                            }
                            _ => {}
                        }
                    }
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
                        if let Some(entry) =
                            self.skiplist.get(node_ref.key.as_ref().unwrap().deref())
                        {
                            if ptr::eq(
                                entry.value().load(Ordering::Relaxed, &guard).as_raw(),
                                node.as_raw(),
                            ) {
                                entry.remove();
                            }
                        }
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        unsafe { new.deref() }.run_op(self, node_ref, &backoff, &guard);
                        unsafe {
                            guard.defer_destroy(old_desc);
                            guard.defer_destroy(node);
                        }
                        break Some(value);
                    }
                    backoff.spin();
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
        let new_node_ref = unsafe { new_node.deref() };
        let desc = new_node_ref.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node_ref.key.as_ref().unwrap().clone();
            let mut node = self
                .skiplist
                .compare_insert(key, Atomic::from(new_node), |node_ptr| {
                    let node = node_ptr.load(Ordering::Relaxed, &guard);
                    let node_ref = unsafe { node.deref() };
                    let desc = node_ref.desc.load(Ordering::Relaxed, &guard);

                    // Remove an existing skiplist entry only when the node's op is `Remove`
                    if let Descriptor::Remove(_) = unsafe { desc.deref() } {
                        return true;
                    }
                    false
                })
                .value()
                .load(Ordering::Relaxed, &guard);

            let old_value = if ptr::eq(node.as_raw(), new_node_ref) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    assert!(self.pop_back().is_some());
                }
                desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
                None
            } else {
                // Another node alreay exists
                let value = desc_ref.clone_value(&guard);
                let (desc, old_value) = 'desc_select: loop {
                    let node_ref = unsafe { node.deref() };
                    let old_desc = node_ref.desc.load(Ordering::Acquire, &guard);
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let new_desc = match old_desc_ref {
                        // The node was removed, try to allocate a new node
                        Descriptor::Remove(op) => {
                            op.link_prev.run_op(self, node_ref, &backoff, &guard);
                            continue 'outer;
                        }
                        Descriptor::Detach(op) => {
                            op.run_op(self, node_ref, &backoff, &guard);
                            node = op.get_new_node(&guard);
                            assert!(!node.is_null());
                            continue 'desc_select;
                        }
                        Descriptor::Insert(op) => {
                            match op.change_value(value.clone(), &guard) {
                                Err(value) => {
                                    Owned::new(Descriptor::Detach(op::Detach::new(value)))
                                        .into_shared(&guard)
                                }
                                Ok(old_value) => {
                                    break 'desc_select (old_desc, old_value);
                                }
                            }
                        }
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
                            guard.defer_destroy(old_desc);
                        }
                        break (new_desc, old_value);
                    }
                    backoff.spin();
                };

                unsafe { guard.defer_destroy(new_node) };

                let desc_ref = unsafe { desc.deref() };
                desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
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
            let last_op = &old_desc_ref;
            match last_op {
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
            let mru_prev = mru_node_ref
                .prev
                .load(Ordering::Relaxed, guard);
            if mru_prev != Shared::from(ptr::from_ref(head)) {
                mru_node = self.help_link(head, mru_node, &Backoff::new(), guard);
                assert!(!mru_node.is_null());
                assert!(mru_node.tag() == 0);
                assert!(!ptr::eq(head, mru_node.as_raw()));
                continue;
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
                        mru_node,
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
            self.help_link(node, mru_node, &Backoff::new(), guard);
            break;
        }

        loop {
            let head_next = head.next.load(Ordering::Acquire, guard);
            if node.prev.load(Ordering::Relaxed, guard) != Shared::from(ptr::from_ref(head)) {
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
        let mut last_link_deleted = true;
        loop {
            let next_ref = unsafe { next.deref() };
            let next_prev = next_ref.prev.load(Ordering::Acquire, guard);
            if next_prev.tag() != 0 {
                if !last_link_deleted {
                    self.help_detach(next_ref, backoff, guard);
                    last_link_deleted = true;
                }
                next = next_ref.next.load(Ordering::Acquire, guard).with_tag(0);
                assert!(!ptr::eq(self.head.as_ref(), next.as_raw()));
                assert!(!next.is_null());
                continue;
            }
            let node_next = node.next.load(Ordering::Acquire, guard);
            // The node was removed by another thread. Pass responsibility of clean-up to that thread.
            if node_next.tag() != 0 {
                assert!(!next.is_null());
                return next;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_raw(), node) {
                last_link_deleted = false;
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
        let mut last_link_deleted = true;
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
            let prev_prev = prev_ref.prev.load(Ordering::Acquire, guard);
            if prev_prev.tag() != 0 {
                prev = prev_prev.with_tag(0);
                continue;
            }
            let next_ref = unsafe { next.deref() };
            let next_prev = next_ref.prev.load(Ordering::Acquire, guard);
            // If `prev` was deleted
            if next_prev.tag() != 0 {
                // A thread who was tried to remove `next` don't update `last`'s prev pointer yet.
                // We will update it instead.
                if !last_link_deleted {
                    self.help_detach(next_ref, backoff, guard);
                    last_link_deleted = true;
                }
                // Find a next node which is not deleted.
                next = next_ref.next.load(Ordering::Acquire, guard).with_tag(0);
                assert!(!next.is_null());
                continue;
            }
            // Found a non-deleted previous node and set it as `last`.
            if !ptr::eq(next_prev.as_raw(), node) {
                last_link_deleted = false;
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
                    Ordering::Acquire,
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
    fn run_op(&self, lru_cache: &Lru<K, V>, node: &Node<K, V>, backoff: &Backoff, guard: &Guard)
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
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        );
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
        ) {
            let mut prev;
            loop {
                prev = node.prev.load(Ordering::Acquire, guard);
                if LruOperation::<K, V>::is_finished(self, guard) {
                    return;
                }
                // Another thread already marked this node
                if prev.tag() != 0 {
                    break;
                }
                assert!(!ptr::eq(node, prev.as_raw()));
                if let Err(err) = node.prev.compare_exchange_weak(
                    prev,
                    prev.with_tag(1),
                    Ordering::Release,
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
            LruOperation::<K, V>::store_result(self, (), guard);
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
        ) {
            if !LruOperation::<K, V>::is_finished(self, guard) {
                LruOperation::<K, V>::run_op(&self.link_prev, lru_cache, node, backoff, guard);
                let prev = node.prev.load(Ordering::Relaxed, guard);
                let next = node.next.load(Ordering::Acquire, guard);
                lru_cache.help_link(unsafe { prev.deref() }, next, backoff, guard);
                LruOperation::<K, V>::store_result(self, (), guard);
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
            self.new_node.load(Ordering::Relaxed, guard).with_tag(0)
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
        ) {
            LruOperation::<K, V>::run_op(&self.remove, lru_cache, node, backoff, guard);
            if !LruOperation::<K, V>::is_finished(self, guard) {
                let key = node.key.as_ref().unwrap().clone();
                let value = self.remove.value.clone();
                let new_node = Node::new(key.clone(), value, lru_cache, guard);
                let new_entry =
                    lru_cache
                        .skiplist
                        .compare_insert(key, Atomic::from(new_node), |old_node| {
                            let old_node = old_node.load(Ordering::Relaxed, guard);
                            ptr::eq(old_node.as_raw(), node)
                        });
                let inserted = new_entry.value().load(Ordering::Relaxed, guard);
                if !ptr::eq(inserted.as_raw(), new_node.as_raw()) {
                    unsafe {
                        guard.defer_destroy(new_node);
                    }
                }
                if LruOperation::<K, V>::store_result(self, inserted, guard) {
                    unsafe { guard.defer_destroy(Shared::from(ptr::from_ref(node))) };
                }
            }
        }

        fn store_result(&self, result: Self::Result, guard: &'a Guard) -> bool {
            let old_node = self.new_node.load(Ordering::Relaxed, guard);
            if old_node.tag() == 0 {
                if self
                    .new_node
                    .compare_exchange(
                        old_node,
                        result.with_tag(1),
                        Ordering::Release,
                        Ordering::Acquire,
                        guard,
                    )
                    .is_ok()
                {
                    return true;
                }
            }
            false
        }

        fn is_finished(&self, guard: &Guard) -> bool {
            self.new_node.load(Ordering::Acquire, guard).tag() != 0
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
        ) {
            if LruOperation::<K, V>::is_finished(self, guard) {
                return;
            }

            lru_cache.put_node_after_head(node, self, &backoff, guard);
            <op::Attach as LruOperation<K, V>>::store_result(self, (), guard);
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
        ) {
            LruOperation::run_op(&self.attach, lru_cache, node, backoff, guard);
            loop {
                let Ok(new_value) = self
                    .value
                    .try_clone_inner(Ordering::Acquire, guard)
                    .map(|v| v.unwrap())
                else {
                    // Someone changed value. Retry.
                    backoff.spin();
                    continue;
                };
                if !LruOperation::<K, V>::store_result(self, &new_value, guard) {
                    // Someone changed value. Retry.
                    backoff.spin();
                    continue;
                }
                break;
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
