use std::{
    borrow::Borrow,
    mem::{self, ManuallyDrop},
    ops::Deref,
    process::abort,
    ptr::{self, NonNull},
    sync::atomic::{self, AtomicUsize, Ordering},
};

use seize::{Collector, Guard, Linked, LocalGuard};

use crate::reclaim::{cast_from_linked, cast_to_linked, Atomic, Shared};

const MAX_REF_COUNT: usize = usize::MAX >> 1;

#[repr(transparent)]
#[derive(Debug)]
pub(crate) struct RefCounted<T> {
    inner: NonNull<Linked<RefCountedInner<T>>>,
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

impl<T> Drop for RefCounted<T> {
    fn drop(&mut self) {
        RefCountedInner::decrement(self.inner.as_ptr());
    }
}

unsafe impl<T: Send + Sync> Send for RefCounted<T> {}
unsafe impl<T: Send + Sync> Sync for RefCounted<T> {}

impl<T: Send + Sync> RefCounted<T> {
    #[inline]
    pub fn new(data: T, collector: &Collector) -> Self {
        Self {
            inner: unsafe { NonNull::new_unchecked(RefCountedInner::new(data, collector)) },
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

        let inner = unsafe { ptr::read(inner_ptr) };
        Some(inner.value.data)
    }

    #[inline]
    pub fn into_raw(self) -> NonNull<T> {
        unsafe {
            NonNull::new_unchecked(cast_from_linked(self.into_inner_raw().as_ptr()).cast::<T>())
        }
    }

    #[inline]
    pub unsafe fn from_raw(this: NonNull<T>) -> Self {
        Self::from_inner_raw(unsafe {
            NonNull::new_unchecked(cast_to_linked(this.cast::<RefCountedInner<T>>().as_ptr()))
        })
    }

    #[inline]
    pub fn as_ptr(&self) -> NonNull<T> {
        self.inner.cast()
    }

    #[inline]
    fn into_inner_raw(self) -> NonNull<Linked<RefCountedInner<T>>> {
        let ptr = self.inner;
        std::mem::forget(self);
        ptr
    }

    #[inline]
    unsafe fn from_inner_raw(this: NonNull<Linked<RefCountedInner<T>>>) -> Self {
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
#[derive(Debug)]
pub(crate) struct AtomicRefCounted<T> {
    inner: Atomic<RefCountedInner<T>>,
}

impl<T> Drop for AtomicRefCounted<T> {
    fn drop(&mut self) {
        let fake_guard = unsafe { seize::unprotected() };
        let inner = self.inner.load(Ordering::SeqCst, &fake_guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            RefCountedInner::decrement(ptr::from_ref(inner).cast_mut())
        }
    }
}

unsafe impl<T: Send + Sync> Send for AtomicRefCounted<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicRefCounted<T> {}

impl<T: Send + Sync> AtomicRefCounted<T> {
    #[inline]
    pub fn new(data: RefCounted<T>) -> Self {
        Self {
            inner: Shared::from(data.into_inner_raw().as_ptr()).into(),
        }
    }

    #[inline]
    pub fn null() -> Self {
        Self {
            inner: Atomic::null(),
        }
    }

    #[inline]
    pub fn load<'a>(&self, order: Ordering, guard: &'a LocalGuard) -> Shared<'a, T> {
        self.inner
            .load(order, guard)
            .as_ptr()
            .cast::<Linked<T>>()
            .into()
    }

    #[inline]
    pub fn clone_inner(&self, order: Ordering, guard: &LocalGuard) -> Option<RefCounted<T>> {
        loop {
            if let Ok(cloned) = self.try_clone_inner(order, guard) {
                break cloned;
            }
        }
    }

    #[inline]
    pub fn try_clone_inner(
        &self,
        order: Ordering,
        guard: &LocalGuard,
    ) -> Result<Option<RefCounted<T>>, ()> {
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
    fn compare_exchange_common<'a, F, G>(
        &self,
        expected: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        func: F,
        guard: &'a G,
    ) -> Result<Option<RefCounted<T>>, CompareExchangeErr<T>>
    where
        G: Guard,
        F: FnOnce(
            &Atomic<RefCountedInner<T>>,
            Shared<'a, RefCountedInner<T>>,
            Shared<'a, RefCountedInner<T>>,
            Ordering,
            Ordering,
            &'a G,
        ) -> Result<
            Shared<'a, RefCountedInner<T>>,
            crate::reclaim::CompareExchangeError<'a, RefCountedInner<T>, Shared<'a, RefCountedInner<T>>, G>,
        >,
    {
        let expected = if let Some(current) = expected {
            Shared::from(cast_to_linked(
                ptr::from_ref(current)
                    .cast::<RefCountedInner<T>>()
                    .cast_mut(),
            ))
        } else {
            Shared::null()
        };
        let new_ptr = if let Some(new) = &new {
            Shared::from(new.inner.as_ptr())
        } else {
            Shared::null()
        };
        match func(&self.inner, expected, new_ptr, success, failure, guard) {
            Ok(old) => {
                Ok({
                    std::mem::forget(new);
                    // SAFETY: In `AtomicRefCounted`, `expected` is always aligned so `old`
                    // pointer must be also aligned.
                    unsafe { old.as_ref() }.map(|v| {
                        // SAFETY: Only we can get the old `RefCountedInner` pointer created by
                        // `RefCounted::into_inner_raw`.
                        unsafe { RefCounted::from_inner_raw(v.into()) }
                    })
                })
            }
            // The `err.current` might be queued for deallocation if another thread already took it
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
                            CompareExchangeErrCuurentValue::Removed(
                                cast_from_linked(ptr::from_ref(inner).cast_mut()).cast(),
                            )
                        }
                    })
                },
                new,
            }),
        }
    }

    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &LocalGuard,
    ) -> Result<Option<RefCounted<T>>, CompareExchangeErr<T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange_weak,
            guard,
        )
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &LocalGuard,
    ) -> Result<Option<RefCounted<T>>, CompareExchangeErr<T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange,
            guard,
        )
    }

    #[inline]
    pub fn make_constant(&self, success: Ordering, failure: Ordering, guard: &LocalGuard) -> bool {
        loop {
            let old_ptr = self.inner.load(failure, guard);
            if old_ptr.tag() != 0 {
                return false;
            }
            if self
                .inner
                .compare_exchange_weak(old_ptr, old_ptr.with_tag(1), success, failure, guard)
                .is_ok()
            {
                return true;
            }
        }
    }

    #[inline]
    pub fn is_constant(&self, guard: &LocalGuard) -> bool {
        self.inner.load(Ordering::Relaxed, guard).tag() != 0
    }
}

pub enum CompareExchangeErrCuurentValue<T: Send + Sync> {
    Removed(*mut T),
    Cloned(RefCounted<T>),
}

pub struct CompareExchangeErr<T: Send + Sync> {
    pub current: Option<CompareExchangeErrCuurentValue<T>>,
    pub new: Option<RefCounted<T>>,
}

#[repr(C)]
struct RefCountedInner<T> {
    data: T,
    ref_count: AtomicUsize,
    collector: NonNull<Collector>,
}

impl<T> RefCountedInner<T> {
    fn new(data: T, collector: &Collector) -> *mut Linked<Self> {
        collector.link_boxed(Self {
            ref_count: AtomicUsize::new(1),
            data,
            collector: collector.into(),
        })
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
    fn decrement(this: *mut Linked<Self>) {
        if unsafe { &*this }.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            // This line should be called once because otherwise `Self::finalize` might be called
            // with `self` pointer which was already deallocated by another thread
            let collector = unsafe { (&*this).collector.as_ref() };
            unsafe { collector.retire(this, seize::reclaim::boxed::<Linked<RefCountedInner<T>>>) };
        }
    }
}
