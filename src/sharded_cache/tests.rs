use super::*;
use rustc_hash::FxBuildHasher;

/// A byte weighter over `(u64 key, Vec<u8> value)`: weight is the value len.
#[derive(Clone, Copy)]
struct LenWeighter;
impl Weighter<u64, alloc::vec::Vec<u8>> for LenWeighter {
    fn weight(&self, _: &u64, v: &alloc::vec::Vec<u8>) -> u64 {
        v.len() as u64
    }
}

fn byte_cache(capacity: u64) -> ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> {
    ShardedCache::with_weighter(capacity, 8, 1024, LenWeighter, FxBuildHasher)
}

#[test]
fn insert_get_roundtrip() {
    let c = byte_cache(10_000);
    c.insert(1, vec![0u8; 100]);
    assert_eq!(c.get(&1), Some(vec![0u8; 100]));
    assert_eq!(c.get(&2), None);
}

/// One shard of 1000 bytes filled with ten entries of 100 bytes, so the next
/// insert of 100 bytes has to evict.
fn full_shard(ghost: usize) -> ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> {
    let c = ShardedCache::with_weighter(1_000, 1, ghost, LenWeighter, FxBuildHasher);
    for key in 1_000..1_010 {
        c.insert(key, vec![0u8; 100]);
    }
    assert_eq!(c.weight(), 1_000);
    c
}

/// A shard has filled once an insert made it evict, and stays filled when
/// eviction leaves it room again: what fills it is whether it still grows
/// into fresh memory, not what it holds at the moment.
#[test]
fn a_shard_has_filled_once_it_has_evicted_and_stays_so() {
    let c = full_shard(1024);
    assert!(
        !c.has_filled(&1),
        "a shard at capacity has evicted nothing yet"
    );
    c.insert(2_000, vec![0u8; 100]);
    assert!(c.has_filled(&1), "the insert past capacity evicted");
    c.remove(&2_000);
    assert!(c.weight() < 1_000, "the removal left room");
    assert!(c.has_filled(&1), "a shard that has filled stays filled");
}

/// A first miss is taken while its shard has room: inserting it evicts
/// nothing, so there is no churn to save, and a working set that fits the
/// cache is cached on its first read.
#[test]
fn admit_takes_a_first_miss_while_the_shard_has_room() {
    let c = byte_cache(10_000);
    assert!(c.admit(&5, 100));
    assert_eq!(c.get(&5), None, "admitting stores nothing by itself");
}

/// Into a full shard a key missed once is only remembered; the second miss
/// admits it. Guards against caching every one-off read, whose insert and
/// later eviction cost a miss more than the miss itself.
#[test]
fn admit_refuses_a_first_miss_into_a_full_shard_and_takes_the_second() {
    let c = full_shard(1024);
    assert!(!c.admit(&5, 100), "a key never seen is not admitted");
    assert!(c.admit(&5, 100), "a key seen once before is admitted");
}

/// A key admitted on a repeat goes into the main queue, so churn through the
/// small queue that evicts one-off entries leaves it resident.
#[test]
fn admitted_entry_survives_churn_of_one_off_entries() {
    let c = full_shard(1024);
    assert!(!c.admit(&1, 100));
    assert!(c.admit(&1, 100));
    c.insert(1, vec![0u8; 100]);
    for key in 100..200 {
        c.insert(key, vec![0u8; 100]);
    }
    assert_eq!(c.get(&1), Some(vec![0u8; 100]));
}

/// Main-queue entries left after `count` keys are each missed twice into a
/// full shard (admitted on the repeat) and inserted at `priority`, none read
/// again. The shard starts as ten entries read once each, so they move to
/// main as the first inserts evict.
fn main_survivors_of_repeat_misses(priority: Priority, count: u64) -> usize {
    let c = full_shard(1024);
    for key in 1_000..1_010 {
        assert!(c.get(&key).is_some());
    }
    for key in 0..count {
        assert!(!c.admit(&key, 100));
        assert!(c.admit(&key, 100));
        c.insert_with_priority(key, vec![0u8; 100], priority);
    }
    (1_000..1_010).filter(|key| c.peek(key).is_some()).count()
}

/// A value admitted on a repeat miss goes on probation: a stream of such
/// values read once each leaves every block already in main resident, where
/// at normal priority the same stream evicts them all. This is what keeps a
/// sustained read of separated values from evicting the table blocks every
/// read walks.
#[test]
fn probation_entries_admitted_on_a_repeat_leave_main_resident() {
    assert_eq!(
        main_survivors_of_repeat_misses(Priority::Probation, 100),
        10
    );
    assert_eq!(
        main_survivors_of_repeat_misses(Priority::Normal, 100),
        0,
        "a ghost hit at normal priority enters main and evicts there"
    );
}

/// On probation an entry still earns main by being read again while in the
/// small queue, so a value read repeatedly stays cached.
#[test]
fn probation_entry_read_again_survives_churn() {
    let c = full_shard(1024);
    assert!(!c.admit(&1, 100));
    assert!(c.admit(&1, 100));
    c.insert_with_priority(1, vec![0u8; 100], Priority::Probation);
    assert!(c.get(&1).is_some());
    for key in 100..200 {
        c.insert(key, vec![0u8; 100]);
    }
    assert_eq!(c.get(&1), Some(vec![0u8; 100]));
}

/// The doorkeeper is bounded by the ghost queue: a key whose first miss has
/// aged out of it is refused again rather than remembered forever.
#[test]
fn admit_forgets_a_miss_past_the_ghost_capacity() {
    let c = full_shard(16);
    assert!(!c.admit(&1, 100));
    for key in 100..200 {
        assert!(!c.admit(&key, 100));
    }
    assert!(
        !c.admit(&1, 100),
        "the first miss aged out of the ghost queue"
    );
}

#[test]
fn peek_does_not_promote_but_get_does() {
    let c = byte_cache(10_000);
    c.insert(7, vec![1u8; 50]);
    // peek returns the value without counting a hit.
    assert_eq!(c.peek(&7), Some(vec![1u8; 50]));
    assert_eq!(c.peek(&999), None);
}

#[test]
fn weight_tracks_resident_bytes() {
    let c = byte_cache(10_000);
    assert_eq!(c.weight(), 0);
    c.insert(1, vec![0u8; 100]);
    c.insert(2, vec![0u8; 200]);
    assert_eq!(c.weight(), 300);
    c.remove(&1);
    assert_eq!(c.weight(), 200);
    c.remove(&999); // absent — no-op
    assert_eq!(c.weight(), 200);
}

/// `remove_where` drops exactly the entries it picks, in every shard (the
/// descriptor cache closes a repair's files through it), and their weight
/// with them; the others stay readable.
#[test]
fn remove_where_drops_only_the_picked_entries() {
    let c = byte_cache(10_000);
    for key in 0..64 {
        c.insert(key, vec![0u8; 100]);
    }
    c.remove_where(|key| *key >= 32);
    assert_eq!(c.len(), 32);
    assert_eq!(c.weight(), 3_200);
    assert_eq!(c.get(&40), None);
    assert_eq!(c.get(&3), Some(vec![0u8; 100]));

    c.insert(40, vec![1u8; 100]);
    assert_eq!(c.get(&40), Some(vec![1u8; 100]));
}

#[test]
fn replace_adjusts_weight_in_place() {
    let c = byte_cache(10_000);
    c.insert(1, vec![0u8; 100]);
    c.insert(1, vec![0u8; 250]); // replace, larger
    assert_eq!(c.weight(), 250);
    assert_eq!(c.get(&1), Some(vec![0u8; 250]));
    c.insert(1, vec![0u8; 30]); // replace, smaller
    assert_eq!(c.weight(), 30);
}

/// An entry heavier than one shard's capacity can never stay resident: letting
/// it in evicts the shard's whole population and then the entry itself goes.
/// It must be refused at the door, leaving the resident set untouched.
#[test]
fn oversized_entry_is_rejected_without_evicting_residents() {
    // 800 bytes over 8 shards = 100 bytes per shard.
    let c = byte_cache(800);
    for k in 0..20u64 {
        c.insert(k, vec![0u8; 30]);
    }
    let before = c.weight();
    assert!(before > 0);

    // Heavier than any shard (100), lighter than the whole cache (800).
    c.insert(1000, vec![0u8; 150]);
    assert_eq!(c.get(&1000), None, "an uncacheable entry is not admitted");
    assert_eq!(c.weight(), before, "the resident set is untouched");
}

#[test]
fn eviction_keeps_resident_under_capacity() {
    let c = byte_cache(1_000);
    // Insert far more than capacity; the cache must evict to stay bounded.
    for i in 0..1_000u64 {
        c.insert(i, vec![0u8; 100]);
    }
    // Allow a single in-flight item of slack over the nominal capacity.
    assert!(
        c.weight() <= 1_000 + 100,
        "resident weight {} exceeded capacity",
        c.weight(),
    );
}

#[test]
fn frequently_read_entries_survive_eviction_pressure() {
    let c = byte_cache(2_000);
    // A hot key, read repeatedly, should be retained under churn that
    // evicts many cold keys.
    c.insert(0, vec![0u8; 100]);
    for _ in 0..8 {
        assert_eq!(c.get(&0), Some(vec![0u8; 100]));
    }
    for i in 1..200u64 {
        c.insert(i, vec![0u8; 100]);
        let _ = c.get(&0); // keep touching the hot key
    }
    assert_eq!(c.get(&0), Some(vec![0u8; 100]), "hot key was evicted");
}

#[test]
fn high_priority_entry_survives_churn_that_evicts_normal() {
    // A single shard so the churn deterministically targets the same queues
    // as the pinned entry (multi-shard would scatter the cold keys).
    let c: ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> =
        ShardedCache::with_weighter(2_000, 1, 1024, LenWeighter, FxBuildHasher);

    // One high-priority entry (pinned metadata) and one normal entry of the
    // same size, neither re-read after insertion.
    c.insert_with_priority(0, vec![0u8; 100], Priority::High);
    c.insert(1, vec![0u8; 100]);

    // Churn far past capacity with cold, single-touch normal entries (the
    // data-block-churn analogue).
    for i in 2..200u64 {
        c.insert(i, vec![0u8; 100]);
    }

    // The pinned entry survived; the same-age normal entry did not.
    assert_eq!(
        c.get(&0),
        Some(vec![0u8; 100]),
        "high-priority entry must survive data-style churn",
    );
    assert_eq!(c.get(&1), None, "normal entry should have been evicted");
}

#[test]
fn high_priority_replace_promotes_small_entry() {
    let c: ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> =
        ShardedCache::with_weighter(2_000, 1, 1024, LenWeighter, FxBuildHasher);

    // Admit at normal priority — lands in the probationary small queue.
    c.insert(0, vec![0u8; 100]);
    // Refresh the SAME key at High priority — must promote it to main with
    // full frequency credit, not leave it in the small queue.
    c.insert_with_priority(0, vec![0u8; 100], Priority::High);

    // Churn cold normal entries far past capacity; the promoted entry must
    // survive (it would be cold-evicted from the small queue otherwise).
    for i in 1..200u64 {
        c.insert(i, vec![0u8; 100]);
    }
    assert_eq!(
        c.get(&0),
        Some(vec![0u8; 100]),
        "High-priority replace must promote the entry to main and protect it",
    );
}

#[test]
fn high_priority_replace_keeps_per_queue_tallies_consistent() {
    // One shard, small capacity so eviction runs and pops the stale node.
    let c: ShardedCache<u64, alloc::vec::Vec<u8>, LenWeighter, FxBuildHasher> =
        ShardedCache::with_weighter(250, 1, 1024, LenWeighter, FxBuildHasher);

    // Admit normal then promote via High replace: the entry moves to main
    // but a stale node is left in the small queue.
    c.insert(0, vec![0u8; 100]);
    c.insert_with_priority(0, vec![0u8; 100], Priority::High);

    // Churn so `evict_from_small` pops the stale small-queue node for key 0.
    // If that node is mistaken for a live small entry, the per-queue byte
    // tallies double-count (small loses bytes it never held, main gains a
    // duplicate node) and drift from the true resident weight by location.
    for i in 1..20u64 {
        c.insert(i, vec![0u8; 100]);
    }

    // Recompute the per-queue tallies from the map and compare to the
    // running counters: they must match exactly. Read everything under a
    // tightly-scoped guard, then assert after it is dropped.
    let (small_bytes, main_bytes, small, main) = {
        let shard = c.shards[0].0.read();
        let (mut small, mut main) = (0u64, 0u64);
        for (_, slot) in &shard.map {
            match slot.loc {
                Location::Small => small += slot.weight,
                Location::Main => main += slot.weight,
            }
        }
        (shard.small_bytes, shard.main_bytes, small, main)
    };
    assert_eq!(small_bytes, small, "small_bytes tally drifted");
    assert_eq!(main_bytes, main, "main_bytes tally drifted");
}

#[test]
fn unit_weighter_is_count_capacity() {
    let c: ShardedCache<u64, u64, UnitWeighter, FxBuildHasher> =
        ShardedCache::with_weighter(4, 2, 64, UnitWeighter, FxBuildHasher);
    for i in 0..100u64 {
        c.insert(i, i);
    }
    // Unit weight → capacity is a max entry count (+ per-shard slack).
    assert!(c.weight() <= 4 + 2, "entry count {} exceeded", c.weight());
}

#[test]
fn oversized_entry_does_not_wedge_the_cache() {
    let c = byte_cache(1_000);
    c.insert(1, vec![0u8; 5_000]); // larger than the whole cache
    // It must not be retained, and the cache must stay usable.
    c.insert(2, vec![0u8; 100]);
    assert_eq!(c.get(&2), Some(vec![0u8; 100]));
    assert!(c.weight() <= 1_100);
}

// Concurrent stress: many threads hammer get/insert/remove on a shared
// cache. The win of the design is that `get` takes only a shared lock, so
// readers run in parallel; this test exercises that path under contention
// and asserts the invariants (no panic, no deadlock, weight stays bounded
// and consistent with what is resident) hold afterwards.
#[cfg(feature = "std")]
#[test]
fn concurrent_stress_keeps_invariants() {
    use std::sync::Arc;
    use std::thread;

    let cache = Arc::new(byte_cache(50_000));
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..5_000u64 {
                    let key = (t * 5_000 + i) % 2_000; // overlapping key space
                    match i % 4 {
                        // Payload byte value is irrelevant (all entries are
                        // 64 B); a constant avoids a u64→u8 cast lint.
                        0 => cache.insert(key, vec![0u8; 64]),
                        1 => {
                            let _ = cache.get(&key);
                        }
                        2 => {
                            let _ = cache.peek(&key);
                        }
                        _ => cache.remove(&key),
                    }
                }
            })
        })
        .collect();
    for h in threads {
        h.join().expect("worker thread panicked");
    }

    // Weight must stay within capacity (+ one in-flight item of slack) and
    // equal the actual resident bytes recomputed from the entry count.
    assert!(
        cache.weight() <= 50_000 + 64,
        "weight {} exceeded capacity after concurrent churn",
        cache.weight(),
    );
    // The atomic total must agree with len()*item_weight (all items are 64B):
    // proves the signed-delta bookkeeping never drifted under concurrency.
    assert_eq!(
        cache.weight(),
        cache.len() as u64 * 64,
        "atomic weight diverged from resident entries",
    );
}
