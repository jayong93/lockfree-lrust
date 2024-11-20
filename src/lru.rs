use std::{
    num::NonZeroUsize,
    ops::{ControlFlow, Deref},
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
        let head_ptr = head.as_ref() as *const Node<K, V>;
        let mut tail = Node::uninit();
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
        let backoff = Backoff::new();
        loop {
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match &old_desc_ref {
                Descriptor::Remove(_) => return None,
                Descriptor::Detach(op) => {
                    atomic::fence(Ordering::Acquire);
                    op.run_op(self, node_ref, &backoff, &guard);
                    node = op.get_new_node(&guard);
                    continue;
                }
                Descriptor::Insert(op) => {
                    atomic::fence(Ordering::Acquire);
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
        let mut node = self.tail.prev.load(Ordering::Relaxed, &guard);
        loop {
            if ptr::eq(node.as_raw(), self.head.as_ref()) {
                return None;
            }
            let node_ref = unsafe { node.deref() };
            let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
            let old_desc_ref = unsafe { old_desc.deref() };
            match &old_desc_ref {
                Descriptor::Remove(_) => {
                    node = node_ref.prev.load(Ordering::Acquire, &guard);
                    continue;
                }
                Descriptor::Detach(op) => {
                    op.run_op(self, node_ref, &backoff, &guard);
                    node = node_ref.prev.load(Ordering::Acquire, &guard);
                    continue;
                }
                Descriptor::Insert(op) => {
                    atomic::fence(Ordering::Acquire);
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

    #[inline]
    pub fn put(
        &self,
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
    ) -> Option<RefCounted<V>> {
        let guard = pin();
        let new_node = Node::new(key, value, self.head.as_ref(), self.tail.as_ref(), &guard);
        let new_node_ref = unsafe { new_node.deref() };
        let desc = new_node_ref.desc.load(Ordering::Relaxed, &guard);
        let desc_ref = unsafe { desc.deref() };
        let backoff = Backoff::new();

        'outer: loop {
            let key = new_node_ref.key.as_ref().unwrap().clone();
            let entry = self
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
                });

            let mut node = entry.value().load(Ordering::Relaxed, &guard);
            let old_value = if ptr::eq(node.as_raw(), new_node_ref) {
                // Our node has been inserted.
                let len = self.size.fetch_add(1, Ordering::Relaxed);

                // If the capacity is reached, remove LRU node.
                if len >= self.cap.get() {
                    atomic::fence(Ordering::Acquire);
                    if self.pop_back().is_none() {
                        abort();
                    }
                }
                desc_ref.run_op(self, unsafe { node.deref() }, &backoff, &guard);
                None
            } else {
                // Another node alreay exists
                let value = desc_ref.clone_value(&guard);
                let (desc, old_value) = 'desc_select: loop {
                    let node_ref = unsafe { node.deref() };
                    let old_desc = node_ref.desc.load(Ordering::Relaxed, &guard);
                    let old_desc_ref = unsafe { old_desc.deref() };
                    let last_old_op = &old_desc_ref;
                    let new_desc = match last_old_op {
                        // The node was removed, try to allocate a new node
                        Descriptor::Remove(_) => {
                            continue 'outer;
                        }
                        Descriptor::Detach(op) => {
                            atomic::fence(Ordering::Acquire);
                            op.run_op(self, node_ref, &backoff, &guard);
                            node = op.get_new_node(&guard);
                            continue 'desc_select;
                        }
                        Descriptor::Insert(op) => {
                            atomic::fence(Ordering::Acquire);
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

    fn put_node_after_head<'a, Op>(
        &self,
        node: &Node<K, V>,
        op: &Op,
        backoff: &Backoff,
        guard: &'a Guard,
    ) -> ControlFlow<()>
    where
        Op: op::LruOperation<K, V>,
    {
        let node_ptr = Shared::from(ptr::from_ref(node));
        let mut node_next = node.next.load(Ordering::Acquire, guard);

        let head = self.head.as_ref();
        let mut mru_node = head.next.load(Ordering::Acquire, guard);

        let next = 'next_node: loop {
            // Another thread finished our job instead.
            if op.is_finished(guard) {
                return ControlFlow::Break(());
            }

            // If we load `head.next` and our operation is not done yet, that means the MRU node is
            // valid or is our node. That's because all threads must help an op of a MRU node
            // before updating `head.next`. In other words, at this point, `head.next` is already
            // replaced with `node` or isn't changed from `mru_node`.

            if ptr::eq(node, mru_node.as_raw()) {
                break 'next_node node_next;
            }

            'change_node_next: loop {
                let new_node_next = node.next.load(Ordering::Relaxed, guard);
                if new_node_next.tag() != 0 {
                    return ControlFlow::Break(());
                }
                if !ptr::eq(new_node_next.as_raw(), node_next.as_raw()) {
                    atomic::fence(Ordering::Acquire);
                    let new_mru_node = head.next.load(Ordering::Relaxed, guard);
                    // `head.next` is not `mru_node` any more
                    if !ptr::eq(new_mru_node.as_raw(), mru_node.as_raw()) {
                        atomic::fence(Ordering::Acquire);
                        // Some thread helped us, try linking
                        if ptr::eq(new_mru_node.as_raw(), node) {
                            break 'next_node mru_node;
                        }
                        // `head.next` was changed to another node or our job was already done by another
                        // thread. Retry.
                        node_next = new_node_next;
                        mru_node = new_mru_node;
                        continue 'next_node;
                    }
                    // MRU node wasn't changed but only `node.next`. Some thread might helped us, so we
                    // don't need to change `node.next`
                    break 'change_node_next;
                }

                let mru_node_ref = unsafe { mru_node.deref() };
                if !ptr::eq(mru_node_ref, node_next.as_raw()) {
                    // Help a MRU node first
                    if let Some(mru_desc) =
                        unsafe { mru_node_ref.desc.load(Ordering::Relaxed, guard).as_ref() }
                    {
                        atomic::fence(Ordering::Acquire);
                        mru_desc.run_op(self, mru_node_ref, backoff, guard);
                    }

                    let next_prev = mru_node_ref.prev.load(Ordering::Relaxed, guard);
                    if next_prev.tag() != 0 {
                        return ControlFlow::Continue(());
                    }

                    if ptr::eq(head, next_prev.as_raw()) {
                        if mru_node_ref
                            .prev
                            .compare_exchange(
                                Shared::from(ptr::from_ref(head)),
                                node_ptr.with_tag(0),
                                Ordering::Release,
                                Ordering::Acquire,
                                guard,
                            )
                            .is_err()
                        {
                            return ControlFlow::Continue(());
                        };
                    } else if !ptr::eq(node, next_prev.as_raw()) {
                        atomic::fence(Ordering::Acquire);
                        return ControlFlow::Continue(());
                    }
                    node.next
                        .compare_exchange(
                            Shared::from(ptr::from_ref(self.tail.as_ref())),
                            mru_node.with_tag(0),
                            Ordering::Release,
                            Ordering::Relaxed,
                            guard,
                        )
                        .ok();
                }
                break;
            }

            let new_mru_node = head.next.load(Ordering::Relaxed, guard);
            if ptr::eq(new_mru_node.as_raw(), mru_node.as_raw()) {
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
            } else if ptr::eq(new_mru_node.as_raw(), node) {
                atomic::fence(Ordering::Acquire);
                break mru_node;
            } else {
                atomic::fence(Ordering::Acquire);
                assert!(op.is_finished(guard))
            }
        };

        self.help_link(node_ptr, unsafe { next.deref() }, backoff, guard);
        ControlFlow::Break(())
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

    fn detach_node<'a, Op>(&self, node: &Node<K, V>, op: &Op, backoff: &Backoff, guard: &'a Guard)
    where
        Op: op::LruOperation<K, V>,
    {
        let mut next;
        loop {
            next = node.next.load(Ordering::Acquire, guard);
            if op.is_finished(guard) {
                return;
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
    }
}

#[derive(Debug)]
struct Node<K: Send + Sync, V: Send + Sync> {
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

impl<K: Send + Sync, V: Send + Sync> Node<K, V> {
    #[inline]
    fn new<'a>(
        key: impl Into<RefCounted<K>>,
        value: impl Into<RefCounted<V>>,
        head: &Node<K, V>,
        tail: &Node<K, V>,
        guard: &'a Guard,
    ) -> Shared<'a, Self> {
        let ptr = Owned::new(Self {
            key: Some(key.into()),
            next: Atomic::from(ptr::from_ref(tail)),
            prev: Atomic::from(ptr::from_ref(head)),
            desc: Atomic::null(),
        })
        .into_shared(guard);
        unsafe { ptr.deref() }.desc.store(
            Owned::new(Descriptor::Insert(op::Insert::new(value.into()))),
            Ordering::Relaxed,
        );
        ptr
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
            Self::Detach(op) => op.value.clone(),
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

    pub trait LruOperation<K: Send + Sync + Ord, V: Send + Sync> {
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        );
        fn is_finished(&self, guard: &Guard) -> bool;
    }

    pub struct Remove<T: Send + Sync> {
        pub value: RefCounted<T>,
        result: AtomicBool,
    }

    impl<T: Send + Sync> Remove<T> {
        #[inline]
        pub fn new(value: RefCounted<T>) -> Self {
            Self {
                value,
                result: AtomicBool::new(false),
            }
        }
    }

    impl<K, V> LruOperation<K, V> for Remove<V>
    where
        K: Send + Sync + Ord,
        V: Send + Sync,
    {
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) {
            lru_cache.detach_node(node, self, &backoff, guard);
            self.result.fetch_or(true, Ordering::Release);
        }

        fn is_finished(&self, _: &Guard) -> bool {
            self.result.load(Ordering::Acquire)
        }
    }

    pub struct Detach<K, V>
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        pub value: RefCounted<V>,
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
                value,
                new_node: Atomic::null(),
            }
        }

        pub fn get_new_node<'a>(&self, guard: &'a Guard) -> Shared<'a, Node<K, V>> {
            self.new_node.load(Ordering::Relaxed, guard)
        }
    }

    impl<K, V> LruOperation<K, V> for Detach<K, V>
    where
        K: Send + Sync + Ord,
        V: Send + Sync,
    {
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) {
            lru_cache.detach_node(node, self, &backoff, guard);
            let key = node.key.as_ref().unwrap().clone();
            let value = self.value.clone();
            let new_node = Node::new(
                key.clone(),
                value,
                lru_cache.head.as_ref(),
                lru_cache.tail.as_ref(),
                guard,
            );
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

            let old_node = self.new_node.load(Ordering::Relaxed, guard);
            if old_node.tag() == 0 {
                if self
                    .new_node
                    .compare_exchange(
                        old_node,
                        inserted.with_tag(1),
                        Ordering::Release,
                        Ordering::Acquire,
                        guard,
                    )
                    .is_ok()
                {
                    unsafe { guard.defer_destroy(Shared::from(ptr::from_ref(node))) };
                }
            }
        }

        fn is_finished(&self, guard: &Guard) -> bool {
            self.new_node.load(Ordering::Acquire, guard).tag() != 0
        }
    }

    pub struct Attach {
        result: AtomicBool,
    }

    impl<K, V> LruOperation<K, V> for Attach
    where
        K: Send + Sync + Ord,
        V: Send + Sync,
    {
        fn run_op(
            &self,
            lru_cache: &Lru<K, V>,
            node: &Node<K, V>,
            backoff: &Backoff,
            guard: &Guard,
        ) {
            loop {
                if LruOperation::<K, V>::is_finished(self, guard) {
                    return;
                }

                if self.result.load(Ordering::Acquire) {
                    break;
                }

                match lru_cache.put_node_after_head(node, self, &backoff, guard) {
                    ControlFlow::Continue(_) => {
                        backoff.spin();
                        continue;
                    }
                    ControlFlow::Break(_) => {
                        self.result.fetch_or(true, Ordering::Release);
                        break;
                    }
                }
            }
        }

        fn is_finished(&self, _: &Guard) -> bool {
            if self.result.load(Ordering::Relaxed) {
                atomic::fence(Ordering::Acquire);
                return true;
            }
            false
        }
    }

    pub struct Insert<T>
    where
        T: Send + Sync,
    {
        attach: Attach,
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

    impl<K, V> LruOperation<K, V> for Insert<V>
    where
        K: Send + Sync + Ord,
        V: Send + Sync,
    {
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
                if !self.value.make_constant(
                    Some(&new_value),
                    Ordering::Release,
                    Ordering::Relaxed,
                    guard,
                ) {
                    // Someone changed value. Retry.
                    backoff.spin();
                    continue;
                }
                break;
            }
        }

        fn is_finished(&self, guard: &Guard) -> bool {
            self.value.is_constant(guard)
        }
    }
}
