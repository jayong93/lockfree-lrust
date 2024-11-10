use std::num::NonZeroUsize;

use lockfree_car::{self, lru::Lru};

fn main() {
    println!("do");
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
    for _ in 0..1 {
        for i in 0..100000 {
            cache.put(i % 98 + 1, i);
        }
    }
    assert_eq!(cache.size(), 98);
    println!("done");
}
