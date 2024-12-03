pub(crate) use seize::{Collector, Guard, Linked};
use thread_local::ThreadLocal;

use std::marker::PhantomData;
use std::ptr::NonNull;
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

    pub(crate) fn store<'a, P, G>(&self, new: P, ordering: Ordering)
    where
        P: Pointer<'a, T, G>,
        G: Guard,
    {
        self.0.store(new.into().ptr, ordering);
    }

    pub(crate) fn swap<'g, P, G>(&self, new: P, ord: Ordering, guard: &'g G) -> P
    where
        P: Pointer<'g, T, G>,
        G: Guard,
    {
        unsafe { P::new_with_guard(self.0.swap(new.into().ptr, ord).into(), guard) }
    }

    pub(crate) fn compare_exchange<'g, P, G>(
        &self,
        current: Shared<'g, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        guard: &'g G,
    ) -> Result<P, CompareExchangeError<'g, T, P, G>>
    where
        P: Pointer<'g, T, G>,
        G: Guard,
    {
        let new: Shared<_> = new.into();
        match self
            .0
            .compare_exchange(current.ptr, new.ptr, success, failure)
        {
            Ok(ptr) => Ok(unsafe { P::new_with_guard(ptr.into(), guard) }),
            Err(current) => Err(CompareExchangeError {
                current: current.into(),
                new: unsafe { P::new_with_guard(new, guard) },
                _marker: Default::default(),
            }),
        }
    }

    pub(crate) fn compare_exchange_weak<'g, P, G>(
        &self,
        current: Shared<'_, T>,
        new: P,
        success: Ordering,
        failure: Ordering,
        guard: &'g G,
    ) -> Result<P, CompareExchangeError<'g, T, P, G>>
    where
        P: Pointer<'g, T, G>,
        G: Guard,
    {
        let new: Shared<_> = new.into();
        match self
            .0
            .compare_exchange_weak(current.ptr, new.ptr, success, failure)
        {
            Ok(ptr) => Ok(unsafe { P::new_with_guard(ptr.into(), guard) }),
            Err(current) => Err(CompareExchangeError {
                current: current.into(),
                new: unsafe { P::new_with_guard(new, guard) },
                _marker: Default::default(),
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

pub(crate) struct CompareExchangeError<'g, T, P, G>
where
    P: Pointer<'g, T, G>,
    G: Guard,
{
    pub(crate) current: Shared<'g, T>,
    pub(crate) new: P,
    _marker: PhantomData<G>,
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

impl<'g, T, G> Pointer<'g, T, G> for Shared<'g, T>
where
    G: Guard,
{
    #[inline]
    unsafe fn new_with_guard(ptr: Shared<'g, T>, _: &'g G) -> Self {
        ptr
    }

    #[inline]
    fn as_shared(&self) -> Shared<'g, T> {
        *self
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
        self.defer_retire(shared.as_ptr(), seize::reclaim::boxed::<Linked<T>>);
    }
}

pub(crate) trait Pointer<'a, T, G>: Into<Shared<'a, T>>
where
    G: Guard,
{
    unsafe fn new_with_guard(ptr: Shared<'a, T>, guard: &'a G) -> Self;
    fn as_shared(&self) -> Shared<'a, T>;
}

pub(crate) trait Freeable {
    fn connect(&mut self, other: *mut Self);
    fn next(&self) -> *mut Self;
    fn dealloc(this: *mut Self);
}

pub(crate) struct FreeList<T>
where
    T: Freeable,
{
    storage: ThreadLocal<AtomicPtr<T>>,
}

impl<T> Drop for FreeList<T>
where
    T: Freeable,
{
    fn drop(&mut self) {
        for head in self.storage.iter_mut() {
            let entry = unsafe { head.load(Ordering::Relaxed).as_mut() };
            let Some(mut entry) = entry else { continue };
            loop {
                let next = unsafe { entry.next().as_mut() };
                Freeable::dealloc(ptr::from_mut(entry));
                let Some(next) = next else { break };
                entry = next;
            }
        }
    }
}

impl<T> FreeList<T>
where
    T: Freeable,
{
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            storage: ThreadLocal::new(),
        }
    }

    #[inline]
    pub(crate) unsafe fn free(&self, mut value: NonNull<T>) {
        let head = self.storage.get_or(|| AtomicPtr::new(ptr::null_mut()));
        let next_ptr = head.load(Ordering::Relaxed);
        unsafe { value.as_mut() }.connect(next_ptr);
        head.store(value.as_ptr(), Ordering::Relaxed);
    }

    #[inline]
    pub(crate) unsafe fn reuse(&self) -> Option<NonNull<T>> {
        let head = self.storage.get_or(|| AtomicPtr::new(ptr::null_mut()));
        let ptr = head.load(Ordering::Relaxed);
        unsafe { ptr.as_ref() }.map(|entry| {
            head.store(entry.next(), Ordering::Relaxed);
            entry.into()
        })
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
        assert_eq!(shared.with_tag(1).with_tag(7).tag(), 7);
        assert_eq!(shared.with_tag(3).ptr, unsafe { original_ptr.byte_add(3) });
        assert_eq!(shared.with_tag(3).tag(), 3);
        assert_eq!(shared.with_tag(1).without_tag().ptr, original_ptr);
    }

    #[test]
    fn test_link_cast() {
        let collector = Collector::new();
        let ptr = collector.link_boxed(1234u16);
        let value_ptr = cast_from_linked(ptr);
        assert_eq!(unsafe { *value_ptr }, 1234);
        let casted = cast_to_linked(value_ptr);
        assert_eq!(casted, ptr);

        let ptr = collector.link_boxed([1, 2, 3]);
        let value_ptr = cast_from_linked(ptr);
        assert_eq!(unsafe { *value_ptr }, [1, 2, 3]);
        let casted = cast_to_linked(value_ptr);
        assert_eq!(casted, ptr);
    }
}
