pub(crate) use seize::{Collector, Guard, Linked, LocalGuard};

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::{fmt, ptr};

#[inline]
pub const fn cast_to_linked<T>(ptr: *mut T) -> *mut Linked<T> {
    let offset = std::mem::offset_of!(Linked<T>, value);
    // SAFETY: `Link` is the first field in `Linked<T>` type.
    unsafe { ptr.byte_sub(offset) }.cast::<Linked<T>>()
}
#[inline]
pub fn cast_from_linked<T>(ptr: *mut Linked<T>) -> *mut T {
    let offset = std::mem::offset_of!(Linked<T>, value);
    // SAFETY: `Link` is the first field in `Linked<T>` type.
    unsafe { ptr.byte_add(offset) }.cast::<T>()
}

#[inline]
const fn low_bits<T>() -> usize {
    (1 << std::mem::align_of::<T>().trailing_zeros()) - 1
}

#[repr(transparent)]
pub(crate) struct Atomic<T>(AtomicPtr<Linked<T>>);

impl<T> Atomic<T> {
    pub(crate) fn null() -> Self {
        Self(AtomicPtr::default())
    }

    pub(crate) fn load<'g>(&self, ordering: Ordering, guard: &'g impl Guard) -> Shared<'g, T> {
        guard.protect(&self.0, ordering).into()
    }

    pub(crate) fn store(&self, new: Shared<'_, T>, ordering: Ordering) {
        self.0.store(new.ptr, ordering);
    }

    pub(crate) fn swap<'g>(
        &self,
        new: Shared<'_, T>,
        ord: Ordering,
        _: &'g impl Guard,
    ) -> Shared<'g, T> {
        self.0.swap(new.ptr, ord).into()
    }

    pub(crate) fn compare_exchange<'g>(
        &self,
        current: Shared<'_, T>,
        new: Shared<'g, T>,
        success: Ordering,
        failure: Ordering,
        _: &'g impl Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T>> {
        match self
            .0
            .compare_exchange(current.ptr, new.ptr, success, failure)
        {
            Ok(ptr) => Ok(ptr.into()),
            Err(current) => Err(CompareExchangeError {
                current: current.into(),
                new,
            }),
        }
    }

    pub(crate) fn compare_exchange_weak<'g>(
        &self,
        current: Shared<'_, T>,
        new: Shared<'g, T>,
        success: Ordering,
        failure: Ordering,
        _: &'g impl Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T>> {
        match self
            .0
            .compare_exchange_weak(current.ptr, new.ptr, success, failure)
        {
            Ok(ptr) => Ok(ptr.into()),
            Err(current) => Err(CompareExchangeError {
                current: current.into(),
                new,
            }),
        }
    }
}

impl<T> From<Shared<'_, T>> for Atomic<T> {
    fn from(shared: Shared<'_, T>) -> Self {
        Atomic(shared.ptr.into())
    }
}

impl<T> Clone for Atomic<T> {
    fn clone(&self) -> Self {
        Atomic(self.0.load(Ordering::Relaxed).into())
    }
}

impl<T> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.ptr)
    }
}

impl<T> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.0.load(Ordering::SeqCst))
    }
}

pub(crate) struct CompareExchangeError<'g, T> {
    pub(crate) current: Shared<'g, T>,
    pub(crate) new: Shared<'g, T>,
}

#[repr(transparent)]
pub(crate) struct Shared<'g, T> {
    ptr: *mut Linked<T>,
    _g: PhantomData<&'g ()>,
}

impl<'g, T> Shared<'g, T> {
    #[inline]
    pub(crate) fn null() -> Self {
        Shared::from(ptr::null_mut())
    }

    #[inline]
    pub(crate) fn boxed(value: T, collector: &Collector) -> Self {
        Shared::from(collector.link_boxed(value))
    }

    #[inline]
    pub(crate) unsafe fn into_box(self) -> Box<Linked<T>> {
        Box::from_raw(self.as_ptr())
    }

    #[inline]
    pub(crate) unsafe fn try_into_box(self) -> Option<Box<Linked<T>>> {
        if self.is_null() {
            return None;
        }
        Some(Box::from_raw(self.as_ptr()))
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *mut Linked<T> {
        self.without_tag().ptr
    }

    #[inline]
    pub(crate) unsafe fn as_ref(&self) -> Option<&'g Linked<T>> {
        self.as_ptr().as_ref()
    }

    #[inline]
    pub(crate) unsafe fn deref(&self) -> &'g Linked<T> {
        &*self.as_ptr()
    }

    #[inline]
    pub(crate) fn with_tag(self, tag: usize) -> Self {
        Self {
            ptr: ((self.ptr as usize & !low_bits::<Linked<T>>()) | (tag & low_bits::<Linked<T>>()))
                as *mut Linked<T>,
            ..self
        }
    }

    #[inline]
    pub(crate) fn without_tag(self) -> Self {
        Self {
            ptr: (self.ptr as usize & !low_bits::<Linked<T>>()) as *mut Linked<T>,
            ..self
        }
    }

    #[inline]
    pub(crate) fn tag(self) -> usize {
        self.ptr as usize & low_bits::<Linked<T>>()
    }

    pub(crate) fn is_null(&self) -> bool {
        self.as_ptr().is_null()
    }
}

impl<'g, T> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T> Eq for Shared<'_, T> {}

impl<T> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Shared<'_, T> {}

impl<T> From<*mut Linked<T>> for Shared<'_, T> {
    fn from(ptr: *mut Linked<T>) -> Self {
        Shared {
            ptr,
            _g: PhantomData,
        }
    }
}

impl<T> From<*const Linked<T>> for Shared<'_, T> {
    fn from(ptr: *const Linked<T>) -> Self {
        Shared {
            ptr: ptr.cast_mut(),
            _g: PhantomData,
        }
    }
}

pub(crate) trait RetireShared {
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>);
}

impl<G> RetireShared for G
where
    G: Guard,
{
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>) {
        self.defer_retire(shared.ptr, seize::reclaim::boxed::<Linked<T>>);
    }
}

pub(crate) enum GuardRef<'g> {
    Owned(LocalGuard<'g>),
    Ref(&'g LocalGuard<'g>),
}

impl<'g> Deref for GuardRef<'g> {
    type Target = LocalGuard<'g>;

    #[inline]
    fn deref(&self) -> &LocalGuard<'g> {
        match *self {
            GuardRef::Owned(ref guard) | GuardRef::Ref(&ref guard) => guard,
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn test_tag() {
        let collector = Collector::new();
        let original_ptr = collector.link_boxed(1);
        let shared: Shared<_> = original_ptr.into();
        assert_eq!(shared.as_ptr(), original_ptr);
        assert_eq!(shared.with_tag(1).as_ptr(), original_ptr);
        assert_eq!(shared.with_tag(1).ptr, unsafe { original_ptr.byte_add(1) });
        assert_eq!(shared.with_tag(1).tag(), 1);
        assert_eq!(shared.with_tag(3).ptr, unsafe { original_ptr.byte_add(3) });
        assert_eq!(shared.with_tag(3).tag(), 3);
        assert_eq!(shared.with_tag(1).without_tag().ptr, original_ptr);
    }

    #[test]
    fn test_link_cast() {
        let collector = Collector::new();
        let ptr = collector.link_boxed(1234u16);
        let value_ptr = cast_from_linked(ptr);
        assert_eq!(unsafe{*value_ptr}, 1234);
        let casted = cast_to_linked(value_ptr);
        assert_eq!(casted, ptr);

        let ptr = collector.link_boxed([1, 2, 3]);
        let value_ptr = cast_from_linked(ptr);
        eprintln!("{ptr:p}, {value_ptr:p}");
        assert_eq!(unsafe{*value_ptr}, [1, 2, 3]);
        let casted = cast_to_linked(value_ptr);
        assert_eq!(casted, ptr);
    }
}
