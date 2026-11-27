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

/// A miss is admitted into a shard that has filled as any miss is, and into
/// one that has not, never: with room for it, and with the key remembered as
/// a recent miss alike.
#[test]
fn a_shard_that_has_not_filled_admits_nothing_once_filled() {
    let c = ShardedCache::with_weighter(1_000, 1, 1024, LenWeighter, FxBuildHasher);
    c.insert(1_000, vec![0u8; 100]);
    assert!(c.admit(&1, 100), "room for it");
    assert!(
        !c.admit_once_filled(&1, 100),
        "room, and nothing evicted yet"
    );

    let c = full_shard(1024);
    assert!(
        !c.admit_once_filled(&1, 100),
        "full, and nothing evicted yet"
    );
    assert!(!c.admit(&1, 100), "a first miss into a full shard");
    assert!(c.admit(&1, 100), "the miss repeated");
    assert!(
        !c.admit_once_filled(&1, 100),
        "a repeated miss, and still nothing evicted"
    );

    c.insert(2_000, vec![0u8; 100]);
    assert!(c.admit_once_filled(&1, 100), "filled, and a repeated miss");
    assert!(!c.admit_once_filled(&2, 100), "filled, and a first miss");
    c.remove(&2_000);
    assert!(c.admit_once_filled(&3, 100), "filled, with room");
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

    // Heavier than half the cache (400), lighter than all of it (800).
    c.insert(1000, vec![0u8; 500]);
    assert_eq!(c.get(&1000), None, "an uncacheable entry is not admitted");
    assert_eq!(c.weight(), before, "the resident set is untouched");
}

/// An entry heavier than a shard and within half the cache is kept: found
/// like any other, with every shard giving up a share of its bytes so the
/// cache stays within its capacity.
#[test]
fn an_entry_heavier_than_a_shard_is_kept_within_the_capacity() {
    // 800 bytes over 8 shards = 100 bytes per shard.
    let c = byte_cache(800);
    for k in 0..200u64 {
        c.insert(k, vec![0u8; 10]);
    }
    assert!(c.weight() > 700, "the shards are near full");

    c.insert(1_000, vec![7u8; 240]);
    assert_eq!(c.get(&1_000), Some(vec![7u8; 240]));
    assert!(c.weight() <= 800, "{} bytes resident", c.weight());
    for shard in 0..8 {
        let (_, resident) = c.shard_state(shard).unwrap();
        assert!(resident <= 100 - 240 / 8, "shard {shard} holds {resident}");
    }

    c.remove(&1_000);
    assert_eq!(c.get(&1_000), None);
    for k in 1_000..1_200u64 {
        c.insert(k, vec![0u8; 10]);
    }
    assert!(c.weight() > 700, "the shards took their bytes back");
}

/// Large entries are evicted among themselves, the unread ones first, so
/// they never take more than half the cache.
#[test]
fn large_entries_evict_the_unread_among_them() {
    let c = byte_cache(800);
    c.insert(1, vec![1u8; 300]);
    c.insert(2, vec![2u8; 300]);
    assert!(c.get(&1).is_some(), "read once since it went in");

    // Seven eighths of the cache hold two of them: the third takes the
    // unread one's place, once that one has been in for a few inserts that
    // needed room.
    c.insert(3, vec![3u8; 300]);
    assert_eq!(c.get(&3), None, "the unread one has only just gone in");
    for _ in 0..4 {
        c.insert(3, vec![3u8; 300]);
    }
    assert_eq!(c.get(&1), Some(vec![1u8; 300]), "the read one stays");
    assert_eq!(c.get(&2), None, "the unread one went");
    assert_eq!(c.get(&3), Some(vec![3u8; 300]));
    assert_eq!(c.weight(), 600);
}

/// Keys read in a round a little larger than the cache holds are found on
/// most reads: the entries held stay for many rounds. Evicting the oldest for
/// each miss would evict, every time, the key the round reads next, and no
/// read would find its entry, while paying an insert and an eviction each.
#[test]
fn a_round_of_large_keys_larger_than_the_cache_is_mostly_found() {
    // 8 MiB over 8 shards; entries of 600 KiB are large and eleven fit the
    // 7 MiB they may weigh. The round has thirteen keys.
    let c = byte_cache(8 << 20);
    let mut found = 0;
    for round in 0..40 {
        for key in 0..13u64 {
            if c.get(&key).is_some() {
                found += usize::from(round >= 20);
            } else {
                c.insert(key, vec![0u8; 600 << 10]);
            }
        }
    }
    // Eleven of thirteen held: at least nine of each round's reads found,
    // over the last twenty rounds.
    assert!(found >= 20 * 9, "{found} of 260 reads found their entry");
}

/// Entries heavier than half a shard do not depend on which shard their keys
/// fall in: a shard holds one of them at most, so kept in the shards two
/// that share one would evict each other in a cache with room for both.
#[test]
fn entries_heavier_than_half_a_shard_all_stay_while_the_cache_has_room() {
    // 8 MiB over 8 shards = 1 MiB a shard; ten entries of 600 KiB.
    let c = byte_cache(8 << 20);
    for key in 0..10u64 {
        c.insert(key, vec![0u8; 600 << 10]);
    }
    assert!((0..10u64).all(|key| c.contains(&key)), "6 of 7 MiB");
    assert_eq!(c.weight(), 10 * (600 << 10));
}

/// A block of half a shard in a cache too small for more stays in its
/// shard's own queues, which keep index and filter blocks over data blocks:
/// only entries of a blob frame's size and up go the large entries' way.
#[test]
fn an_ordinary_block_of_half_a_shard_is_not_a_large_entry() {
    // 64 KiB over 8 shards = 8 KiB a shard; blocks of 5 KiB.
    let c = byte_cache(64 << 10);
    for key in 0..64u64 {
        c.insert(key, vec![0u8; 5 << 10]);
    }
    // One to a shard: eight at most, where the large entries' 56 KiB would
    // hold eleven.
    assert!(c.len() <= 8, "{} blocks kept", c.len());
    assert!(c.weight() <= 64 << 10);
}

/// A key's entry replaced across the line between the two kinds keeps the
/// cache's weight what its entries weigh.
#[test]
fn replacing_between_large_and_small_keeps_the_weight() {
    let c = byte_cache(800);
    c.insert(1, vec![0u8; 150]);
    assert_eq!(c.weight(), 150);
    c.insert(1, vec![0u8; 20]);
    assert_eq!((c.weight(), c.len()), (20, 1));
    c.insert(1, vec![0u8; 300]);
    assert_eq!((c.weight(), c.len()), (300, 1));
    assert_eq!(c.get(&1), Some(vec![0u8; 300]));
    c.set_capacity(200);
    assert_eq!(c.get(&1), None, "heavier than the shrunk cache");
    assert_eq!(c.weight(), 0);
}

/// A first miss of a large entry is taken at once only while the whole
/// cache has its bytes free; past that, on its second miss.
#[test]
fn a_large_miss_is_admitted_with_room_or_on_its_repeat() {
    let c = byte_cache(800);
    assert!(c.admit(&1, 150), "the cache is empty");
    for k in 0..200u64 {
        c.insert(k, vec![0u8; 10]);
    }
    assert!(!c.admit(&1_000, 150), "a first miss with no room");
    assert!(c.admit(&1_000, 150), "the miss repeated");
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

    // Recompute the per-queue tallies from the entries and compare to the
    // running counters: they must match exactly.
    let [small_bytes, main_bytes, small, main] = c.queue_tallies(0).expect("one shard");
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

/// Shrinking a cache to a quarter of what it holds leaves its weight within
/// the new capacity and keeps the entries read since they went in over the
/// ones never read, as its S3-FIFO order ranks them. Its capacity reports the
/// target at once, and the heaviest entry it takes follows it.
#[test]
fn shrinking_to_a_quarter_evicts_down_to_it_and_keeps_hot_entries() {
    // Room to spare, so no shard evicts before the shrink however the keys
    // spread over the four.
    let c = ShardedCache::with_weighter(128_000, 4, 4_096, LenWeighter, FxBuildHasher);
    for key in 0..640u64 {
        c.insert(key, vec![0u8; 100]);
    }
    assert_eq!(c.weight(), 64_000);
    // Every eighth entry read: an eighth of the weight, inside the quarter.
    let hot = |key: u64| key.is_multiple_of(8);
    for key in (0..640u64).filter(|&key| hot(key)) {
        assert!(c.get(&key).is_some());
    }

    c.set_capacity(16_000);
    assert_eq!(c.capacity(), 16_000);
    assert_eq!(c.shard_capacity(), 4_000);
    assert_eq!(c.max_entry_weight(), 8_000);
    assert!(c.weight() <= 16_000, "{} bytes left", c.weight());

    let (hot_kept, cold_kept) =
        (0..640u64)
            .filter(|key| c.contains(key))
            .fold(
                (0, 0),
                |(h, k), key| if hot(key) { (h + 1, k) } else { (h, k + 1) },
            );
    assert_eq!(hot_kept, 80, "every hot entry fits and stays");
    assert_eq!(hot_kept + cold_kept, c.len());
    // The hot eighth all stays; of the cold rest, no more than the room the
    // hot ones left (16,000 - 8,000 bytes).
    assert!(cold_kept <= 80, "{cold_kept} of 560 cold entries kept");
}

/// Growing evicts nothing; the cache then fills to the new capacity as entries
/// go in, and an entry heavier than the old shard but within the new one is
/// taken.
#[test]
fn growing_evicts_nothing_and_the_cache_fills_to_the_new_capacity() {
    let c = byte_cache(8_000);
    for key in 0..200u64 {
        c.insert(key, vec![0u8; 100]);
    }
    let before = (c.weight(), c.len());
    assert!(before.0 <= 8_000);

    c.set_capacity(32_000);
    assert_eq!((c.weight(), c.len()), before, "growing evicted");
    assert_eq!(c.shard_capacity(), 4_000);
    for key in 1_000..2_000u64 {
        c.insert(key, vec![0u8; 100]);
    }
    // Each of the 8 shards fills to within one entry of its 4,000 bytes.
    assert!(
        c.weight() > 32_000 - 8 * 100 && c.weight() <= 32_000,
        "{} bytes after filling",
        c.weight()
    );
    c.insert(5_000, vec![0u8; 3_000]);
    assert!(c.contains(&5_000), "an entry the grown shard can keep");
}

/// After a shrink, an entry heavier than the new shard is refused, and its
/// key's former value stays where it was, as for any oversized insert.
#[test]
fn after_a_shrink_an_entry_heavier_than_the_new_shard_is_refused() {
    let c = byte_cache(80_000);
    c.insert(1, vec![1u8; 100]);
    c.set_capacity(8_000);
    c.insert(1, vec![2u8; 5_000]);
    c.insert(2, vec![2u8; 5_000]);
    assert_eq!(c.get(&1), Some(vec![1u8; 100]), "the old value stays");
    assert_eq!(c.get(&2), None, "heavier than half the 8,000 bytes");
}

/// The values a shrink evicts are dropped once each, and those a reader
/// holds outlive the eviction.
#[test]
fn a_shrink_drops_each_evicted_value_once_and_spares_what_readers_hold() {
    use core::sync::atomic::{AtomicUsize, Ordering};

    let drops = alloc::sync::Arc::new(AtomicUsize::new(0));
    let c = ShardedCache::with_weighter(1_000, 1, 1024, CountedWeight, FxBuildHasher);
    for key in 0..10 {
        c.insert(
            key,
            alloc::sync::Arc::new(Counted {
                weight: 100,
                drops: alloc::sync::Arc::clone(&drops),
            }),
        );
    }
    let held = c.get(&0).expect("resident");

    c.set_capacity(0);
    assert_eq!((c.len(), c.weight()), (0, 0));
    assert_eq!(drops.load(Ordering::Relaxed), 9, "all but the held one");
    assert_eq!(held.weight, 100, "the reader's value is intact");
    drop(held);
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

/// Resizes racing each other and readers and writers leave every shard at
/// the capacity stored last, within it, and with its tallies agreeing with
/// what it holds.
#[cfg(feature = "std")]
#[test]
fn resizes_racing_reads_and_inserts_settle_on_the_last_capacity() {
    use std::sync::Arc;
    use std::thread;

    let cache = Arc::new(byte_cache(64_000));
    let workers: Vec<_> = (0..6u64)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..20_000u64 {
                    let key = (t * 7_919 + i) % 3_000;
                    if i % 3 == 0 {
                        cache.insert(key, vec![0u8; 64]);
                    } else if let Some(value) = cache.get(&key) {
                        assert_eq!(value, vec![0u8; 64], "a torn entry");
                    }
                }
            })
        })
        .collect();
    let resizers: Vec<_> = (0..2u64)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..500u64 {
                    cache.set_capacity(4_000 + ((i * 2 + t) % 16) * 4_000);
                }
            })
        })
        .collect();
    for handle in resizers.into_iter().chain(workers) {
        handle.join().expect("thread panicked");
    }

    assert_eq!(cache.shard_capacity(), cache.capacity().div_ceil(8));
    for shard in 0..cache.shards.len() {
        let (capacity, resident) = cache.shard_state(shard).expect("a shard of the cache");
        assert_eq!(capacity, cache.shard_capacity());
        assert!(resident <= capacity);
    }
    assert_eq!(cache.weight(), cache.len() as u64 * 64);
}

/// A key replaced over and over is found by every read made meanwhile, with
/// one of the values written: the new slot takes the old one's place in the
/// chain, so no read walks past both.
#[cfg(feature = "std")]
#[test]
fn a_read_racing_replaces_of_its_key_always_finds_a_value() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let cache = Arc::new(ShardedCache::with_weighter(
        1 << 20,
        1,
        1024,
        LenWeighter,
        FxBuildHasher,
    ));
    // Neighbours in the one shard, so the key's chain is not it alone.
    for key in 0..64u64 {
        cache.insert(key, vec![0u8; 8]);
    }
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (cache, done) = (Arc::clone(&cache), Arc::clone(&done));
            thread::spawn(move || {
                let mut reads = 0u64;
                while !done.load(Ordering::Relaxed) {
                    let value = cache.get(&7).expect("the key is always resident");
                    assert!(value.iter().all(|&byte| byte == value[0]), "a torn value");
                    reads += 1;
                }
                reads
            })
        })
        .collect();
    for round in 0..50_000u32 {
        cache.insert(7, vec![round.to_le_bytes()[0]; 8]);
    }
    done.store(true, Ordering::Relaxed);
    for reader in readers {
        assert!(reader.join().expect("reader panicked") > 0);
    }
    assert_eq!((cache.len(), cache.weight()), (64, 64 * 8));
}

/// Entries resident before a shard's table grows are found by every read
/// made while it grows: a read that walked links the rebuild moved looks
/// again instead of answering that the key is gone.
#[cfg(feature = "std")]
#[test]
fn a_read_racing_the_tables_growth_finds_what_was_resident() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let cache = Arc::new(ShardedCache::with_weighter(
        1 << 30,
        1,
        1024,
        LenWeighter,
        FxBuildHasher,
    ));
    for key in 0..32u64 {
        cache.insert(key, vec![1u8; 8]);
    }
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4u64)
        .map(|t| {
            let (cache, done) = (Arc::clone(&cache), Arc::clone(&done));
            thread::spawn(move || {
                let mut key = t;
                while !done.load(Ordering::Relaxed) {
                    assert!(cache.contains(&(key % 32)), "key {} lost", key % 32);
                    key += 1;
                }
            })
        })
        .collect();
    // Doubles the buckets a dozen times over.
    for key in 1_000..60_000u64 {
        cache.insert(key, vec![2u8; 8]);
    }
    done.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().expect("reader panicked");
    }
    assert_eq!(cache.len(), 32 + 59_000);
}

/// Values read, replaced, removed and evicted from several threads at once
/// are each dropped once: by the writer that took the entry out, or by the
/// last reader that held its slot.
#[cfg(feature = "std")]
#[test]
fn values_under_concurrent_reads_and_writes_are_each_dropped_once() {
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    let drops = alloc::sync::Arc::new(AtomicUsize::new(0));
    let made = alloc::sync::Arc::new(AtomicUsize::new(0));
    // Two shards of 2,000: forty entries of 100 fill the cache, so inserts
    // evict throughout.
    let cache = alloc::sync::Arc::new(ShardedCache::with_weighter(
        4_000,
        2,
        1024,
        CountedWeight,
        FxBuildHasher,
    ));
    let workers: Vec<_> = (0..8u64)
        .map(|t| {
            let (cache, drops, made) = (
                alloc::sync::Arc::clone(&cache),
                alloc::sync::Arc::clone(&drops),
                alloc::sync::Arc::clone(&made),
            );
            thread::spawn(move || {
                for i in 0..20_000u64 {
                    let key = (t * 31 + i) % 97;
                    match (t + i) % 5 {
                        0 | 1 => {
                            made.fetch_add(1, Ordering::Relaxed);
                            cache.insert(
                                key,
                                alloc::sync::Arc::new(Counted {
                                    weight: 100,
                                    drops: alloc::sync::Arc::clone(&drops),
                                }),
                            );
                        }
                        2 | 3 => {
                            if let Some(value) = cache.get(&key) {
                                assert_eq!(value.weight, 100);
                            }
                        }
                        _ => cache.remove(&key),
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker panicked");
    }

    assert_eq!(cache.weight(), cache.len() as u64 * 100);
    assert!(cache.weight() <= 4_000);
    assert_eq!(
        drops.load(Ordering::Relaxed) + cache.len(),
        made.load(Ordering::Relaxed),
        "every value not resident was dropped once"
    );
    drop(cache);
    assert_eq!(
        drops.load(Ordering::Relaxed),
        made.load(Ordering::Relaxed),
        "each value once"
    );
}

/// A shard that never fills keeps no more stale queue nodes than a small
/// multiple of what it holds, however often its entries are replaced: no
/// eviction pops them there.
#[test]
fn replacing_in_a_shard_that_never_fills_does_not_grow_its_queues() {
    let c = ShardedCache::with_weighter(1 << 20, 1, 1024, LenWeighter, FxBuildHasher);
    for round in 0..10_000u64 {
        c.insert(round % 4, vec![0u8; 8]);
    }
    let queued = c.queued_nodes(0).expect("one shard");
    assert!(
        queued <= 2 * 4 + 16 + 1,
        "{queued} nodes queued for 4 entries"
    );
}

/// A value of some weight that counts its drops.
struct Counted {
    weight: u64,
    drops: alloc::sync::Arc<core::sync::atomic::AtomicUsize>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct CountedWeight;
impl Weighter<u64, alloc::sync::Arc<Counted>> for CountedWeight {
    fn weight(&self, _: &u64, v: &alloc::sync::Arc<Counted>) -> u64 {
        v.weight
    }
}

/// An insert that outweighs more residents than it holds in place evicts
/// every one of them and drops each once: the ones held in place and the
/// ones past them alike, and none of what stays.
#[test]
fn an_insert_evicting_more_than_it_holds_in_place_drops_each_once() {
    use core::sync::atomic::{AtomicUsize, Ordering};

    let drops = alloc::sync::Arc::new(AtomicUsize::new(0));
    let value = |weight| {
        alloc::sync::Arc::new(Counted {
            weight,
            drops: alloc::sync::Arc::clone(&drops),
        })
    };
    let c = ShardedCache::with_weighter(1_000, 1, 1024, CountedWeight, FxBuildHasher);
    for key in 0..20 {
        c.insert(key, value(50));
    }
    assert_eq!((c.len(), drops.load(Ordering::Relaxed)), (20, 0));

    // Twelve residents make room for it, more than are held in place.
    c.insert(100, value(600));
    let evicted = 20 + 1 - c.len();
    assert!(evicted > HELD, "{evicted} evicted");
    assert_eq!(drops.load(Ordering::Relaxed), evicted);
    assert!(c.weight() <= 1_000);

    drop(c);
    assert_eq!(drops.load(Ordering::Relaxed), 21, "each value once");
}
