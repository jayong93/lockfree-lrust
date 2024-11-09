use std::num::NonZeroUsize;

use lockfree_car::lru::Lru;

#[test]
fn test_no_eviction() {
    let cache: Lru<usize, usize> = Lru::new(NonZeroUsize::new(100).unwrap());
        for _ in 0..1 {
                for i in 0..198 {
                    cache.put(i % 99 + 1, i);
                }
        }
    assert_eq!(cache.size(), 99);
}
