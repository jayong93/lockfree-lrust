use std::{num::NonZeroUsize, ptr};

use lockfree_car::lru::Lru;

const TEST_COUNT: usize = 1_000_000;

#[test]
fn test_no_eviction() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for i in 0..TEST_COUNT {
                    cache.put(i % 10 + 1, i);
                }
            });
        }
    });
    assert_eq!(cache.len(), 10);
    for i in 1..=10 {
        cache.get(&i).unwrap();
    }
}

#[test]
fn test_eviction() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for i in 0..TEST_COUNT {
                    cache.put(i % 200 + 1, i);
                }
            });
        }
    });
    assert_eq!(cache.len(), 100);
}

#[test]
fn test_put_and_remove() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    std::thread::scope(|s| {
        for i in 0..4 {
            if i % 2 == 0 {
                s.spawn(|| {
                    for i in 0..TEST_COUNT {
                        cache.put(i % 200 + 1, i);
                    }
                });
            } else {
                s.spawn(|| {
                    for i in 0..TEST_COUNT {
                        cache.remove(&(i % 200 + 1));
                    }
                });
            }
        }
    });
    assert!(cache.len() <= 100);
}

#[test]
fn test_lru_property() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(10).unwrap());
    for i in 0..10 {
        cache.put(i + 1, i);
    }
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for i in 0..TEST_COUNT {
                    cache.put(i % 5 + 1, i);
                }
            });
        }
    });
    assert_eq!(cache.len(), 10);
    let mut it = cache.iter();
    assert!(matches!(it.next(), Some(entry) if *entry.value() == 9 && *entry.key() == 10));
    for _ in 0..4 {
        it.next();
    }
    let next_lru = it.next().unwrap();
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for i in 0..TEST_COUNT {
                    cache.put(i % 5 + 5, i);
                }
            });
        }
    });
    let mut new_it = cache.iter();
    let new_lru = new_it.next().unwrap();
    assert_eq!(ptr::from_ref(next_lru.key()), ptr::from_ref(new_lru.key()));
    assert_eq!(ptr::from_ref(next_lru.value()), ptr::from_ref(new_lru.value()));
}
