use std::num::NonZeroUsize;

use lockfree_car::{self, lru::Lru};

fn main() {
    println!("do");
    let cache: Lru<bool, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    std::thread::scope(|s| {
        for _ in 0..16 {
            s.spawn(|| {
                for i in 0..1000000 {
                    cache.put(false, i);
                }
            });
        }
    });
    assert_eq!(cache.size(), 1);
    println!("done");
}
