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
        gate.observe(CACHE, 1, hash(key), 300, block);
    }
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
