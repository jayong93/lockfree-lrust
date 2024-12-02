use std::num::NonZeroUsize;

use lockfree_car::lru::Lru;

// #[test]
// fn test_no_eviction() {
//     let cache: Lru<u8, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
//     std::thread::scope(|s| {
//         for _ in 0..1 {
//             s.spawn(|| {
//                 for i in 0..10000000usize {
//                     cache.put((i % 10 + 1) as u8, i);
//                 }
//             });
//         }
//     });
//     assert_eq!(cache.len(), 10);
//     for i in 1..=10 {
//         cache.get(&i).unwrap();
//     }
// }

// #[test]
// fn test_eviction() {
//     let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
//     std::thread::scope(|s| {
//         for _ in 0..8 {
//             s.spawn(|| {
//                 for i in 0..1000000 {
//                     cache.put(i % 200 + 1, i);
//                 }
//             });
//         }
//     });
//     assert_eq!(cache.len(), 100);
// }

#[test]
fn test_put_and_remove() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    std::thread::scope(|s| {
        for i in 0..4 {
            if i % 2 == 0 {
                s.spawn(|| {
                    for i in 0..10000000 {
                        cache.put(i % 200 + 1, i);
                    }
                });
            } else {
                s.spawn(|| {
                    for i in 0..10000000 {
                        cache.remove(&(i % 200 + 1));
                    }
                });
            }
        }
    });
    // let cache = moka::sync::Cache::<usize, usize>::new(100);
    // std::thread::scope(|s| {
    //     for i in 0..16 {
    //         if i % 2 == 0 {
    //             s.spawn(|| {
    //                 for i in 0..10000000 {
    //                     cache.insert(i % 200 + 1, i);
    //                 }
    //             });
    //         } else {
    //             s.spawn(|| {
    //                 for i in 0..10000000 {
    //                     cache.remove(&(i % 200 + 1));
    //                 }
    //             });
    //         }
    //     }
    // });
    assert!(cache.len() <= 100);
}
