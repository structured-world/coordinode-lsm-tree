use super::*;
use crate::table::filter::ribbon::hashing::SplitMix64;

/// Row-major reference: XOR the rows the coefficient mask selects, exactly
/// as the unpacked probe did. Every packed walk is checked against this.
fn reference_reduce(rows: &[u64], start: usize, coeff_lo: u64, r: u8) -> u64 {
    let mask = if r == 64 { u64::MAX } else { (1u64 << r) - 1 };
    let mut acc = 0u64;
    let mut lo = coeff_lo;
    while lo != 0 {
        let i = lo.trailing_zeros() as usize;
        lo &= lo - 1;
        acc ^= rows[start + i] & mask;
    }
    acc
}

fn random_rows(count: usize, r: u8, seed: u64) -> Vec<u64> {
    let mask = if r == 64 { u64::MAX } else { (1u64 << r) - 1 };
    let mut rng = SplitMix64::new(seed);
    (0..count).map(|_| rng.next_u64() & mask).collect()
}

fn to_bytes(words: &[u64]) -> Vec<u8> {
    words.iter().flat_map(|w| w.to_le_bytes()).collect()
}

#[test]
fn segments_for_rounds_up_to_whole_words() {
    assert_eq!(segments_for(0), 0);
    assert_eq!(segments_for(1), 1);
    assert_eq!(segments_for(64), 1);
    assert_eq!(segments_for(65), 2);
    // `b` need not be a multiple of 64: m = 100 is a legal layer size and
    // leaves the second segment partial.
    assert_eq!(segments_for(100), 2);
}

#[test]
fn z_byte_len_is_r_bits_per_row_rounded_to_segments() {
    // 64 rows at r = 10 is ten words, not sixty-four.
    assert_eq!(z_byte_len(64, 10), Some(80));
    assert_eq!(z_byte_len(128, 10), Some(160));
    // A partial segment still costs a whole one.
    assert_eq!(z_byte_len(65, 10), Some(160));
    assert_eq!(z_byte_len(1, 1), Some(8));
    assert_eq!(z_byte_len(64, 64), Some(512));
}

#[test]
fn z_byte_len_overflow_reports_none_instead_of_wrapping() {
    assert_eq!(z_byte_len(usize::MAX, 64), None);
}

#[test]
fn transpose_round_trips_for_every_width() {
    for r in 1..=64u8 {
        // 200 rows leaves the last segment partial for every r.
        let rows = random_rows(200, r, u64::from(r) * 7 + 1);
        let words = transpose(&rows, r);
        assert_eq!(words.len(), segments_for(rows.len()) * usize::from(r));
        assert_eq!(untranspose(&words, rows.len(), r), rows, "r = {r}");
    }
}

#[test]
fn transpose_ignores_bits_above_r() {
    // A caller that has not masked its rows produces the same payload as one
    // that has: the layout stores r bits and nothing above them.
    let masked = alloc::vec![0b0101u64, 0b0011u64];
    let dirty = alloc::vec![0b0101u64 | (1 << 40), 0b0011u64 | (1 << 63)];
    assert_eq!(transpose(&masked, 4), transpose(&dirty, 4));
}

#[test]
fn transpose_of_no_rows_is_empty() {
    assert!(transpose(&[], 10).is_empty());
    assert_eq!(z_byte_len(0, 10), Some(0));
}

#[test]
fn walk_band_matches_the_row_major_reduce_at_every_offset() {
    for r in [1u8, 7, 8, 10, 33, 64] {
        let rows = random_rows(512, r, u64::from(r) * 31 + 5);
        let z = to_bytes(&transpose(&rows, r));
        let mut rng = SplitMix64::new(u64::from(r) * 99 + 3);
        // Every start offset within a segment, so both the aligned
        // single-word read and the two-word recombination are covered.
        for start in 0..=(rows.len() - 64) {
            let coeff_lo = rng.next_u64() | 1;
            let expected = reference_reduce(&rows, start, coeff_lo, r);
            let walk = walk_band::<false>(&z, r, start, coeff_lo, 0);
            assert_eq!(
                walk,
                BandWalk::Reduced(expected),
                "r = {r}, start = {start}"
            );
        }
    }
}

#[test]
fn walk_band_handles_a_partial_final_segment() {
    // m = 100 is not a multiple of 64, so the last segment holds 36 live
    // rows. The furthest legal band starts at m - 64 = 36 and reaches row 99.
    let r = 10u8;
    let rows = random_rows(100, r, 4242);
    let z = to_bytes(&transpose(&rows, r));
    let coeff_lo = u64::MAX;
    let expected = reference_reduce(&rows, 36, coeff_lo, r);
    assert_eq!(
        walk_band::<false>(&z, r, 36, coeff_lo, 0),
        BandWalk::Reduced(expected)
    );
}

#[test]
fn early_out_agrees_with_the_full_walk_on_a_match() {
    let r = 12u8;
    let rows = random_rows(256, r, 77);
    let z = to_bytes(&transpose(&rows, r));
    let mut rng = SplitMix64::new(1234);
    for start in [0usize, 1, 31, 63, 64, 100, 192] {
        let coeff_lo = rng.next_u64() | 1;
        let expected = reference_reduce(&rows, start, coeff_lo, r);
        // Fed its own answer as the fingerprint, the early-out walk must run
        // to the end and report the same accumulator.
        assert_eq!(
            walk_band::<true>(&z, r, start, coeff_lo, expected),
            BandWalk::Reduced(expected),
            "start = {start}"
        );
    }
}

#[test]
fn early_out_reports_mismatch_when_any_bit_differs() {
    let r = 12u8;
    let rows = random_rows(256, r, 88);
    let z = to_bytes(&transpose(&rows, r));
    let coeff_lo = 0xDEAD_BEEF_CAFE_F00Du64 | 1;
    let expected = reference_reduce(&rows, 17, coeff_lo, r);
    // Flip each bit of the correct answer in turn: every one of them must be
    // caught, including the highest, which is the last the walk reads.
    for j in 0..r {
        let wrong = expected ^ (1u64 << j);
        assert_eq!(
            walk_band::<true>(&z, r, 17, coeff_lo, wrong),
            BandWalk::Mismatch,
            "flipped bit {j}"
        );
    }
}

#[test]
fn early_out_ignores_fingerprint_bits_above_r() {
    // The fingerprint is masked to r bits by the equation generator; a
    // stray high bit must not be compared, or every probe would mismatch.
    let r = 8u8;
    let rows = random_rows(128, r, 909);
    let z = to_bytes(&transpose(&rows, r));
    let coeff_lo = 0x1234_5678_9ABC_DEF1u64;
    let expected = reference_reduce(&rows, 5, coeff_lo, r);
    assert_eq!(
        walk_band::<true>(&z, r, 5, coeff_lo, expected | (1 << 40)),
        BandWalk::Reduced(expected)
    );
}

#[test]
fn a_truncated_payload_reports_truncated_not_a_wrong_answer() {
    let r = 10u8;
    let rows = random_rows(128, r, 5150);
    let full = to_bytes(&transpose(&rows, r));
    // Cut one byte off the end: the last column word of the second segment
    // is now short, and a band that reaches it must say so.
    let short = &full[..full.len() - 1];
    assert_eq!(
        walk_band::<false>(short, r, 64, u64::MAX, 0),
        BandWalk::Truncated
    );
    // An empty payload truncates on the very first word.
    assert_eq!(walk_band::<false>(&[], r, 0, 1, 0), BandWalk::Truncated);
}

#[test]
fn a_present_key_never_reads_as_absent_from_a_truncated_payload() {
    // The invariant that matters, stated as a present key would exercise it:
    // its stored value IS the fingerprint, so every bit the walk can read
    // agrees and the walk reaches the missing word, where it must truncate.
    // Answering absent there would make a point read skip live data.
    let r = 10u8;
    let rows = random_rows(128, r, 611);
    let full = to_bytes(&transpose(&rows, r));
    let coeff_lo = u64::MAX;
    // Band 64 lives entirely in the second segment, whose top column words
    // are the ones removed below.
    let present = reference_reduce(&rows, 64, coeff_lo, r);
    for missing_words in 1..usize::from(r) {
        let short = &full[..full.len() - missing_words * 8];
        assert_eq!(
            walk_band::<true>(short, r, 64, coeff_lo, present),
            BandWalk::Truncated,
            "{missing_words} column words removed"
        );
    }
}

#[test]
fn an_absent_key_still_answers_absent_when_a_later_column_is_missing() {
    // The converse of the invariant above, and the reason the early-out may
    // report Mismatch on a truncated payload: the bits it read are the real
    // stored bits, and a disagreement among them proves the key is absent
    // whatever the missing words would have held. Only a walk that reaches a
    // missing word has to fail closed.
    let r = 10u8;
    let rows = random_rows(128, r, 612);
    let full = to_bytes(&transpose(&rows, r));
    let coeff_lo = u64::MAX;
    let present = reference_reduce(&rows, 64, coeff_lo, r);
    // Flip bit 0 — the first bit the walk reads — so it disagrees before
    // reaching the truncation.
    let absent = present ^ 1;
    let short = &full[..full.len() - 8];
    assert_eq!(
        walk_band::<true>(short, r, 64, coeff_lo, absent),
        BandWalk::Mismatch
    );
}

#[test]
fn a_single_bit_width_reduces_to_one_parity() {
    let mut padded = alloc::vec![1u64, 0, 1, 1, 0, 1, 0, 0];
    padded.resize(64, 0);
    let z = to_bytes(&transpose(&padded, 1));
    // Selecting rows 0, 2 and 3: three ones, parity 1.
    let coeff = 0b1101u64;
    assert_eq!(walk_band::<false>(&z, 1, 0, coeff, 0), BandWalk::Reduced(1));
    // Selecting rows 0 and 2: two ones, parity 0.
    assert_eq!(
        walk_band::<false>(&z, 1, 0, 0b0101, 0),
        BandWalk::Reduced(0)
    );
}
