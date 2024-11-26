use std::{marker::PhantomData, ops::Deref};

use seize::Collector;

pub(crate) mod internal;

pub struct Entry<'a, K, V> {
    key: internal::RefCounted<K>,
    value: internal::RefCounted<V>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub(crate) fn new(
        key: internal::RefCounted<K>,
        value: internal::RefCounted<V>,
        _: &'a Collector,
    ) -> Self {
        Self {
            key,
            value,
            _marker: PhantomData,
        }
    }

    pub fn key<'b>(&'b self) -> &'b K {
        self.key.deref()
    }

    pub fn value<'b>(&'b self) -> &'b V {
        self.value.deref()
    }
}
