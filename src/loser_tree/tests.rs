// Test names below are short forms (e.g. `empty_tree`,
// `drain_in_order`) — the project convention requires
// `<what>_<condition>_<expected>` for top-level tests, OR
// short names inside a descriptively-named submodule. The
// descriptively-named submodule path is what's exercised
// here: `loser_tree::tests::<short_name>` reads as "loser
// tree's <short_name> test", which gives the short forms the
// missing `<what>` context.
use super::*;
use test_log::test;

fn cmp_u32(a: &u32, b: &u32) -> Ordering {
    a.cmp(b)
}

fn collect<F: Fn(&u32, &u32) -> Ordering>(mut t: LoserTree<u32, F>) -> Vec<u32> {
    let mut out = Vec::new();
    while let Some(v) = t.pop_min() {
        out.push(v);
    }
    out
}

#[test]
fn empty_tree() {
    let t: LoserTree<u32, fn(&u32, &u32) -> Ordering> =
        LoserTree::build(alloc::vec![None, None, None], cmp_u32);
    assert!(t.is_empty());
    assert_eq!(t.active_count(), 0);
    assert_eq!(t.peek_min(), None);
    assert_eq!(t.winner_slot(), None);
}

#[test]
fn single_slot() {
    let mut t = LoserTree::build(alloc::vec![Some(42_u32)], cmp_u32);
    assert!(!t.is_empty());
    assert_eq!(t.peek_min(), Some(&42));
    assert_eq!(t.pop_min(), Some(42));
    assert!(t.is_empty());
    assert_eq!(t.pop_min(), None);
}

#[test]
fn drain_in_order() {
    // 4 sources, each with a distinct value.
    let t = LoserTree::build(alloc::vec![Some(3_u32), Some(1), Some(4), Some(2)], cmp_u32);
    assert_eq!(collect(t), [1, 2, 3, 4]);
}

#[test]
fn non_pow2_padding() {
    // 5 slots → cap = 8. Sentinels must not affect winner.
    let t = LoserTree::build(
        alloc::vec![Some(50_u32), Some(10), Some(40), Some(20), Some(30)],
        cmp_u32,
    );
    assert_eq!(collect(t), [10, 20, 30, 40, 50]);
}

#[test]
fn replace_min_stays_winner_when_still_smallest() {
    // Slot 0 keeps yielding monotonically increasing values that
    // are still below everyone else.
    let mut t = LoserTree::build(
        alloc::vec![Some(1_u32), Some(100), Some(200), Some(300)],
        cmp_u32,
    );
    assert_eq!(t.replace_min(2), 1);
    assert_eq!(t.peek_min(), Some(&2));
    assert_eq!(t.winner_slot(), Some(0));
    assert_eq!(t.replace_min(3), 2);
    assert_eq!(t.peek_min(), Some(&3));
    assert_eq!(t.winner_slot(), Some(0));
}

#[test]
fn replace_min_changes_winner() {
    let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(5), Some(3), Some(7)], cmp_u32);
    assert_eq!(t.winner_slot(), Some(0));
    // Replace slot 0's value with something larger than slot 2 (3)
    // but smaller than slot 1 (5).
    assert_eq!(t.replace_min(4), 1);
    assert_eq!(t.peek_min(), Some(&3)); // slot 2 wins now
    assert_eq!(t.winner_slot(), Some(2));
    assert_eq!(t.replace_min(6), 3);
    assert_eq!(t.peek_min(), Some(&4)); // slot 0 wins again with 4
    assert_eq!(t.winner_slot(), Some(0));
}

#[test]
fn pop_min_then_drain() {
    let mut t = LoserTree::build(
        alloc::vec![Some(10_u32), Some(20), Some(5), Some(15)],
        cmp_u32,
    );
    assert_eq!(t.pop_min(), Some(5));
    assert_eq!(t.active_count(), 3);
    assert_eq!(t.peek_min(), Some(&10));
    assert_eq!(collect(t), [10, 15, 20]);
}

#[test]
fn mixed_replace_and_pop() {
    // Drain interleaved: simulates a real merge where some sources
    // get exhausted partway through.
    let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(2), Some(3), Some(4)], cmp_u32);
    assert_eq!(t.replace_min(5), 1); // slot 0 now 5
    assert_eq!(t.replace_min(6), 2); // slot 1 now 6
    // Order should now be 3, 4, 5, 6.
    assert_eq!(t.pop_min(), Some(3));
    assert_eq!(t.pop_min(), Some(4));
    assert_eq!(t.pop_min(), Some(5));
    assert_eq!(t.pop_min(), Some(6));
    assert!(t.is_empty());
}

#[test]
fn reverse_comparator_gives_max_tree() {
    // Same data, max-tree semantics via reversed cmp.
    let cmp = |a: &u32, b: &u32| b.cmp(a);
    let mut t = LoserTree::build(alloc::vec![Some(1_u32), Some(4), Some(2), Some(3)], cmp);
    assert_eq!(t.peek_min(), Some(&4)); // "min" under reversed cmp = max
    assert_eq!(t.pop_min(), Some(4));
    assert_eq!(t.pop_min(), Some(3));
    assert_eq!(t.pop_min(), Some(2));
    assert_eq!(t.pop_min(), Some(1));
}

#[test]
fn deterministic_tiebreak_by_cmp() {
    // Ties go to whichever the comparator picks; we encode source
    // index via tuple so equal values resolve deterministically.
    let cmp = |a: &(u32, usize), b: &(u32, usize)| (a.0, a.1).cmp(&(b.0, b.1));
    let mut t = LoserTree::build(
        alloc::vec![Some((5_u32, 0)), Some((5, 1)), Some((5, 2)), Some((5, 3)),],
        cmp,
    );
    // All same key → slot 0 wins on tiebreak.
    assert_eq!(t.winner_slot(), Some(0));
    let mut order = Vec::new();
    while let Some((_, idx)) = t.pop_min() {
        order.push(idx);
    }
    assert_eq!(order, [0, 1, 2, 3]);
}

#[test]
fn random_inputs_match_sorted_reference() {
    // Property-style: random multi-source data must drain in the
    // same order a sorted concatenation produces.
    use rand::SeedableRng;
    use rand::seq::SliceRandom;
    let mut rng = rand::rngs::StdRng::seed_from_u64(0xC0DE_F00D);
    for n in [1_usize, 2, 3, 7, 8, 9, 31, 32, 33] {
        for trial in 0..32 {
            let mut all: Vec<u32> = (0..(n as u32 * 4)).collect();
            all.shuffle(&mut rng);
            // Round-robin distribute into n buckets.
            let mut buckets: Vec<Vec<u32>> = (0..n).map(|_| Vec::new()).collect();
            for (i, v) in all.iter().enumerate() {
                #[expect(clippy::indexing_slicing, reason = "i % n always < n")]
                buckets[i % n].push(*v);
            }
            // Each bucket must be individually sorted (loser tree
            // assumes per-source sortedness, just like a merger).
            for b in &mut buckets {
                b.sort_unstable();
            }
            // Snapshot the sorted reference before consuming buckets.
            let mut reference = all.clone();
            reference.sort_unstable();
            // Build tree from the first item of each bucket;
            // simulate refilling via replace_min until all drained.
            let mut iters: Vec<std::vec::IntoIter<u32>> =
                buckets.into_iter().map(IntoIterator::into_iter).collect();
            let initial: Vec<Option<u32>> = iters.iter_mut().map(Iterator::next).collect();
            let mut t = LoserTree::build(initial, cmp_u32);
            let mut out = Vec::with_capacity(reference.len());
            while let Some(slot) = t.winner_slot() {
                #[expect(clippy::indexing_slicing, reason = "slot < n by construction")]
                if let Some(next_val) = iters[slot].next() {
                    out.push(t.replace_min(next_val));
                } else {
                    out.push(t.pop_min().unwrap());
                }
            }
            assert_eq!(out, reference, "n={n} trial={trial}");
        }
    }
}
