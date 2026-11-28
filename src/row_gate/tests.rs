use super::*;

/// The cache the reads go through: 16 MiB, so each shadow holds 64 KiB.
const CACHE: u64 = 16 << 20;

fn gate() -> RowGate {
    RowGate::new()
}

/// A well-mixed 64-bit value of `n`, standing in for a key's hash.
fn hash(n: u64) -> u64 {
    let mut z = n.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Plays `reads` sampled reads of keys drawn evenly from `keys` distinct
/// ones: rows of 300 bytes, thirteen to a block of 4 KiB.
fn read_evenly(gate: &RowGate, keys: u64, reads: u64, seed: u64) {
    for n in 0..reads {
        let key = hash(seed.wrapping_add(n)) % keys;
        let block = BlockSeen {
            offset: key / 13 * 4_096,
            weight: 4_096,
        };
        gate.observe(CACHE, 1, hash(key), 300, block, 0, true);
    }
}

/// A shadow counts hits on rows of a hot set that fits it. Where the cache
/// gave the same reads no row, as when a stream of blocks evicts each row
/// before its key is read again, the tree gives its rows up in spite of the
/// shadows, and does not take them back while the reads stay as they are.
#[test]
fn rows_the_cache_does_not_deliver_are_given_up() {
    let gate = gate();
    let play = |windows: u64, held: bool| {
        for n in 0..windows * u64::from(WINDOW) {
            let key = hash(n) % 40;
            let block = BlockSeen {
                offset: key / 13 * 4_096,
                weight: 4_096,
            };
            gate.observe(CACHE, 1, hash(key), 300, block, 0, held);
        }
    };
    play(8, false);
    assert_eq!(gate.eighths(), 0, "no row was delivered");
    play(16, false);
    assert_eq!(gate.eighths(), 0, "closed, with nothing new to measure");
    // Tried again once the trust is back, and kept when the cache delivers.
    play(200, true);
    assert_eq!(gate.eighths(), 8);
}

/// Every key read in turn, round after round, where the cache holds most but
/// not all of them: a row comes back only after every other key was read, by
/// when the cache has dropped it, so rows cost their probes, refusals and
/// fills and answer almost nothing. The gate closes.
#[test]
fn rounds_over_more_keys_than_the_cache_holds_turn_rows_off() {
    let gate = gate();
    let keys = 70_000;
    let mut closed_after = None;
    for n in 0..60 * u64::from(WINDOW) * SAMPLE {
        let key = n % keys;
        let key_hash = hash(key);
        if !gate.samples(key_hash) {
            continue;
        }
        // Keys are stored in another order than they are read in.
        let block = BlockSeen {
            offset: hash(key ^ 0x5555) % keys / 14 * 4_096,
            weight: 4_096,
        };
        gate.observe(CACHE, 1, key_hash, 330, block, 0, true);
        if gate.eighths() == 0 && closed_after.is_none() {
            closed_after = Some(n);
        }
    }
    assert_eq!(gate.eighths(), 0);
    // At its third window, which ends inside the sixth round of the keys.
    assert!(
        closed_after.is_some_and(|reads| reads <= 6 * keys),
        "closed after {closed_after:?} reads",
    );
}

/// A cache whose loads come from the page cache times them at a few hundred
/// nanoseconds, a fraction of what a miss costs the read that takes it: the
/// allocation, the insert and the eviction it causes are not in that time.
/// Priced as a hit plus such a load, a miss cost a fifth more than a hit, and
/// a tree reading 70,000 keys evenly through a 16 MiB cache kept rows that
/// made it a quarter slower. A miss costs the constant at the least, and
/// more where the load is slower than the constant allows for.
#[test]
fn a_load_timed_fast_does_not_make_a_miss_cheap() {
    assert_eq!(block_miss_cost(0), cost::BLOCK_MISS);
    assert_eq!(block_miss_cost(300), cost::BLOCK_MISS);
    assert_eq!(block_miss_cost(1_000_000), cost::BLOCK_HIT + 1_000_000);
}

/// The shadows stay within a two-hundredth of the cache however small the
/// rows: a tree of rows a tenth of the floor samples more sparsely and
/// weighs its entries at no less than the floor allows.
#[test]
fn the_shadows_stay_within_a_two_hundredth_of_the_cache() {
    for row in [24, 100, 300, 4_000] {
        let gate = gate();
        for n in 0..200_000 {
            let key = hash(n) % 5_000_000;
            let block = BlockSeen {
                offset: key / 64 * 4_096,
                weight: 4_096,
            };
            gate.observe(CACHE, 1, hash(key), row, block, 0, true);
        }
        assert!(
            gate.memory() <= CACHE / 200,
            "rows of {row}: {} bytes",
            gate.memory(),
        );
        assert!(gate.memory() > 0);
    }
}

/// Where a cold load is slow, a block kept out of the cache by rows costs
/// more than the rows save. A hot set read beside many other keys keeps its
/// rows while a block miss costs the constant, and gives them all up when the
/// cache has timed its loads at a millisecond: the choice follows the load
/// time the cache measured, not the constant.
#[test]
fn a_slow_load_weighs_against_rows_that_crowd_out_blocks() {
    let play = |load_nanos: u64| {
        let gate = gate();
        for n in 0..40 * u64::from(WINDOW) {
            // Nine reads in ten of 150 hot keys, the tenth of 20,000 others.
            let key = if n % 10 == 0 {
                150 + hash(n) % 20_000
            } else {
                hash(n) % 150
            };
            let block = BlockSeen {
                offset: key / 13 * 4_096,
                weight: 4_096,
            };
            gate.observe(CACHE, 1, hash(key), 300, block, load_nanos, true);
        }
        gate.eighths()
    };
    assert_eq!((play(0), play(1_000_000)), (8, 0));
}

/// Keys read evenly from far more than the cache holds leave their rows
/// unread: every one is a probe, a fill and bytes taken from the blocks. The
/// gate closes, so such a tree reads as one with no row cache.
#[test]
fn reads_spread_over_more_than_the_cache_holds_turn_rows_off() {
    let gate = gate();
    assert_eq!(gate.eighths(), 8, "rows are on until reads say otherwise");
    read_evenly(&gate, 1_000_000, 20 * u64::from(WINDOW), 1);
    assert_eq!(gate.eighths(), 0);
}

/// A set of keys whose rows fit is answered from them at a third of what a
/// block costs: a gate that was closed opens fully.
#[test]
fn a_hot_set_that_fits_turns_rows_on() {
    let gate = gate();
    gate.set_eighths(0);
    read_evenly(&gate, 40, 20 * u64::from(WINDOW), 2);
    assert_eq!(gate.eighths(), 8);
}

/// Every candidate is played whatever fraction is in use, so a tree whose
/// rows went off gets them back when its reads change, and loses them again
/// when they change back.
#[test]
fn the_gate_follows_a_change_of_reads_both_ways() {
    let gate = gate();
    read_evenly(&gate, 1_000_000, 20 * u64::from(WINDOW), 3);
    assert_eq!(gate.eighths(), 0, "spread reads");
    read_evenly(&gate, 40, 20 * u64::from(WINDOW), 4);
    assert_eq!(gate.eighths(), 8, "then a hot set");
    read_evenly(&gate, 1_000_000, 20 * u64::from(WINDOW), 5);
    assert_eq!(gate.eighths(), 0, "then spread again");
}

/// Two trees reading through one cache choose apart: the one whose keys come
/// back keeps its rows while the one whose keys do not gives them up, though
/// their reads are interleaved.
#[test]
fn trees_with_opposite_reads_choose_opposite_fractions() {
    let (hot, spread) = (gate(), gate());
    for round in 0..20 {
        read_evenly(&hot, 40, u64::from(WINDOW), 6 + round);
        read_evenly(&spread, 1_000_000, u64::from(WINDOW), 60 + round);
    }
    assert_eq!(hot.eighths(), 8);
    assert_eq!(spread.eighths(), 0);
}

/// A key is inside the gate by its hash alone, so the keys inside stay the
/// same ones while the fraction stands, and a wider fraction keeps every key
/// a narrower one held.
#[test]
fn a_wider_fraction_keeps_the_keys_of_a_narrower_one() {
    let gate = gate();
    let inside = |eighths: u8| -> Vec<u64> {
        gate.set_eighths(eighths);
        (0..4_096).map(hash).filter(|&h| gate.admits(h)).collect()
    };
    let (none, eighth, half, all) = (inside(0), inside(1), inside(4), inside(8));
    assert!(none.is_empty(), "a closed gate admits no key");
    assert_eq!(all.len(), 4_096);
    assert!(eighth.iter().all(|h| half.contains(h)));
    // An eighth and a half of evenly hashed keys, within a fifth.
    assert!((410..=615).contains(&eighth.len()), "{}", eighth.len());
    assert!((1_640..=2_460).contains(&half.len()), "{}", half.len());
}
