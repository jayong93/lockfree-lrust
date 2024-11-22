use std::{
    borrow::Borrow,
    mem::ManuallyDrop,
    ops::Deref,
    process::abort,
    ptr::{self, NonNull},
    sync::atomic::{self, AtomicUsize, Ordering},
};

use crossbeam::epoch::{pin, Atomic, Guard, Owned, Shared};

const MAX_REF_COUNT: usize = usize::MAX >> 10;

#[repr(transparent)]
#[derive(Debug)]
pub struct RefCounted<T: Send + Sync> {
    inner: NonNull<RefCountedInner<T>>,
}

impl<T: Send + Sync> From<T> for RefCounted<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: Send + Sync> Clone for RefCounted<T> {
    fn clone(&self) -> Self {
        let inner = unsafe { self.inner.as_ref() };
        inner.try_increment_strong();
        Self {
            inner: unsafe { NonNull::new_unchecked(ptr::from_ref(inner).cast_mut()) },
        }
    }
}

impl<T: Send + Sync> Drop for RefCounted<T> {
    fn drop(&mut self) {
        unsafe {
            self.inner.as_ref().decrement_strong();
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
    pub fn into_raw(self) -> NonNull<T> {
        self.into_inner_raw().cast()
    }

    #[inline]
    pub unsafe fn from_raw(this: NonNull<T>) -> Self {
        Self { inner: this.cast() }
    }

    #[inline]
    pub fn as_ptr(&self) -> NonNull<T> {
        self.inner.cast()
    }

    #[inline]
    fn into_inner_raw(self) -> NonNull<RefCountedInner<T>> {
        let ptr = self.inner;
        std::mem::forget(self);
        ptr
    }

    #[inline]
    unsafe fn from_inner_raw(this: NonNull<RefCountedInner<T>>) -> Self {
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
pub struct AtomicRefCounted<T: Send + Sync> {
    inner: Atomic<RefCountedInner<T>>,
}

impl<T: Send + Sync> Drop for AtomicRefCounted<T> {
    fn drop(&mut self) {
        let fake_guard = unsafe { crossbeam::epoch::unprotected() };
        if let Some(inner) = unsafe {
            self.inner
                .swap(Shared::null().with_tag(1), Ordering::Relaxed, fake_guard)
                .as_ref()
        } {
            atomic::fence(Ordering::Acquire);
            inner.decrement_strong();
        }
    }
}

unsafe impl<T: Send + Sync> Send for AtomicRefCounted<T> {}
unsafe impl<T: Send + Sync> Sync for AtomicRefCounted<T> {}

impl<T: Send + Sync> AtomicRefCounted<T> {
    #[inline]
    pub fn new(data: RefCounted<T>) -> Self {
        Self {
            inner: Atomic::from(data.into_inner_raw().as_ptr().cast_const().cast()),
        }
    }

    #[inline]
    pub fn null() -> Self {
        Self {
            inner: Atomic::null(),
        }
    }

    /// This keeps trying to clone `RefCounted` until success. Because
    #[inline]
    pub fn load<'a>(&self, order: Ordering, guard: &'a Guard) -> Option<&'a T> {
        let inner = self.inner.load(order, guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            Some(&inner.data)
        } else {
            None
        }
    }

    #[inline]
    pub fn clone_inner(&self, order: Ordering, guard: &Guard) -> Option<RefCounted<T>> {
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
        guard: &Guard,
    ) -> Result<Option<RefCounted<T>>, ()> {
        let inner = self.inner.load(order, guard);
        if let Some(inner) = unsafe { inner.as_ref() } {
            if inner.try_increment_strong() {
                Ok(Some(unsafe { RefCounted::from_inner_raw(inner.into()) }))
            } else {
                Err(())
            }
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn compare_exchange_common<'a, F>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        func: F,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>>
    where
        T: 'a,
        F: FnOnce(
            &Atomic<RefCountedInner<T>>,
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
            Shared::from(ptr::from_ref(current).cast::<RefCountedInner<T>>())
        } else {
            Shared::null()
        };
        let new_ptr = if let Some(new) = &new {
            Shared::from(new.inner.as_ptr().cast_const())
        } else {
            Shared::null()
        };
        match func(&self.inner, current, new_ptr, success, failure, guard) {
            Ok(_) => Ok({
                std::mem::forget(new);
                CompareExchangeOk {
                    old: unsafe { current.as_ref() }.map(|v| {
                        // SAFETY: Only we can get the old `RefCountedInner` pointer created by
                        // `RefCounted::into_inner_raw`.
                        unsafe { RefCounted::from_inner_raw(v.into()) }
                    }),
                }
            }),
            // The `old` pointer might be queued for deallocation if another thread already took it
            // from `self` by another compare-exchange call. But we have `Guard` here we can assume
            // the pointer is valid until the `Guard` is dropped.
            Err(err) => Err(CompareExchangeErr {
                current: unsafe {
                    err.current.as_ref().map(|inner| {
                        if inner.try_increment_strong() {
                            CompareExchangeErrCuurentValue::Cloned(RefCounted::from_inner_raw(
                                inner.into(),
                            ))
                        } else {
                            CompareExchangeErrCuurentValue::Removed(Shared::from(
                                ptr::from_ref(inner).cast::<T>(),
                            ))
                        }
                    })
                },
                new,
            }),
        }
    }

    #[inline]
    pub fn compare_exchange_weak<'a>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>> {
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
    pub fn compare_exchange<'a>(
        &self,
        current: Option<&T>,
        new: Option<RefCounted<T>>,
        success: Ordering,
        failure: Ordering,
        guard: &'a Guard,
    ) -> Result<CompareExchangeOk<T>, CompareExchangeErr<'a, T>> {
        self.compare_exchange_common(
            current,
            new,
            success,
            failure,
            Atomic::compare_exchange::<Shared<_>>,
            guard,
        )
    }

    #[inline]
    pub fn make_constant(
        &self,
        expected: Option<&T>,
        success: Ordering,
        failure: Ordering,
        guard: &Guard,
    ) -> bool {
        let value = if let Some(value) = expected {
            Shared::from(ptr::from_ref(value).cast::<RefCountedInner<T>>())
        } else {
            Shared::null()
        };

        let cur_value = self.inner.load(Ordering::Relaxed, guard);
        if !ptr::eq(cur_value.as_raw(), value.as_raw()) {
            return false;
        }
        if cur_value.tag() != 0 {
            return false;
        }

        if self
            .inner
            .compare_exchange(value, value.with_tag(1), success, failure, guard)
            .is_ok()
        {
            return true;
        }
        false
    }

    #[inline]
    pub fn is_constant(&self, guard: &Guard) -> bool {
        self.inner.load(Ordering::Relaxed, guard).tag() != 0
    }
}

pub struct CompareExchangeOk<T: Send + Sync> {
    pub old: Option<RefCounted<T>>,
}
pub enum CompareExchangeErrCuurentValue<'a, T: Send + Sync> {
    Removed(Shared<'a, T>),
    Cloned(RefCounted<T>),
}

pub struct CompareExchangeErr<'a, T: Send + Sync> {
    pub current: Option<CompareExchangeErrCuurentValue<'a, T>>,
    pub new: Option<RefCounted<T>>,
}

#[repr(transparent)]
pub struct WeakRefCounted<T>
where
    T: Send + Sync,
{
    inner: NonNull<RefCountedInner<T>>,
}

#[repr(C)]
struct RefCountedInner<T: Send + Sync> {
    data: T,
    ref_count: AtomicUsize,
    weak_count: AtomicUsize,
}

impl<T: Send + Sync> RefCountedInner<T> {
    fn new(data: T) -> Self {
        Self {
            ref_count: AtomicUsize::new(1),
            weak_count: AtomicUsize::new(1),
            data,
        }
    }

    #[inline]
    fn try_increment_strong(&self) -> bool {
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
    fn decrement_strong(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            // This line should be called once because otherwise `Self::finalize` might be called
            // with `self` pointer which was already deallocated by another thread
            let this = ptr::from_ref(self);
            unsafe { guard.defer_unchecked(move || Self::finalize(this)) };
        }
    }

    #[inline]
    fn decrement_strong(&self) {
        if self.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            atomic::fence(Ordering::Acquire);
            let guard = pin();
            // This line should be called once because otherwise `Self::finalize` might be called
            // with `self` pointer which was already deallocated by another thread
            let this = ptr::from_ref(self);
            unsafe { guard.defer_unchecked(move || Self::finalize(this)) };
        }
    }

    #[cold]
    unsafe fn finalize(this: *const Self) {
        drop(Owned::from_raw(this.cast_mut()))
    }
}
