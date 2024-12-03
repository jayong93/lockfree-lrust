use std::num::NonZeroUsize;

use criterion::{criterion_group, criterion_main, Criterion};
use lockfree_car::lru::Lru;

pub fn bench_eviction(c: &mut Criterion) {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    c.bench_function("bench_eviction", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for _ in 0..8 {
                    s.spawn(|| {
                        for i in 0..1000000 {
                            cache.put(i % 200 + 1, i);
                        }
                    });
                }
            });
        });
    });
}

pub fn bench_no_eviction(c: &mut Criterion) {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    c.bench_function("bench_no_eviction", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for _ in 0..8 {
                    s.spawn(|| {
                        for i in 0..1000000 {
                            cache.put(i % 10 + 1, i);
                        }
                    });
                }
            });
        });
    });
}

pub fn bench_put_and_remove(c: &mut Criterion) {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    c.bench_function("bench_put_and_remove", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for i in 0..8 {
                    if i % 2 == 0 {
                        s.spawn(|| {
                            for i in 0..1000000 {
                                cache.put(i % 200 + 1, i);
                            }
                        });
                    } else {
                        s.spawn(|| {
                            for i in 0..1000000 {
                                cache.remove(&(i % 200 + 1));
                            }
                        });
                    }
                }
            });
        });
    });
    // let cache: moka::sync::Cache<usize, usize> = moka::sync::Cache::new(100);
    // c.bench_function("bench_put_and_remove", |b| {
    //     b.iter(|| {
    //         std::thread::scope(|s| {
    //             for i in 0..8 {
    //                 if i % 2 == 0 {
    //                     s.spawn(|| {
    //                         for i in 0..1000000 {
    //                             cache.insert(i % 200 + 1, i);
    //                         }
    //                     });
    //                 } else {
    //                     s.spawn(|| {
    //                         for i in 0..1000000 {
    //                             cache.remove(&(i % 200 + 1));
    //                         }
    //                     });
    //                 }
    //             }
    //         });
    //     });
    // });
}

criterion_group!(name=benches; config=Criterion::default().sample_size(20); targets=bench_put_and_remove);
criterion_main!(benches);
