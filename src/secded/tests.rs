use super::*;

#[test]
fn data_cols_are_distinct_odd_weight_at_least_three() {
    for (i, &col) in DATA_COLS.iter().enumerate() {
        assert!(col.count_ones() % 2 == 1, "col {i} must be odd weight");
        assert!(
            col.count_ones() >= 3,
            "col {i} must avoid the weight-1 check columns"
        );
        for (j, &other) in DATA_COLS.iter().enumerate() {
            if i != j {
                assert_ne!(col, other, "cols {i} and {j} collide");
            }
        }
    }
}

#[test]
fn clean_word_round_trips() {
    let data = 0x0123_4567_89ab_cdefu64.to_le_bytes();
    let mut parity = [0u8; 1];
    Hsiao7264::encode_word(&data, &mut parity);
    let mut recv = data;
    assert_eq!(
        Hsiao7264::decode_word(&mut recv, &parity),
        SecdedOutcome::Clean,
    );
    assert_eq!(recv, data, "clean decode must not alter the word");
}

#[test]
fn corrects_every_single_data_bit_flip() {
    // Exhaustive over all 64 data-bit positions for a few seed words.
    for seed in [0u64, u64::MAX, 0x0123_4567_89ab_cdef, 0xdead_beef_cafe_babe] {
        let data = seed.to_le_bytes();
        let mut parity = [0u8; 1];
        Hsiao7264::encode_word(&data, &mut parity);
        for bit in 0..64 {
            let mut recv = (seed ^ (1u64 << bit)).to_le_bytes();
            let outcome = Hsiao7264::decode_word(&mut recv, &parity);
            assert_eq!(
                outcome,
                SecdedOutcome::Corrected,
                "single flip at bit {bit} (seed {seed:#x}) must be corrected",
            );
            assert_eq!(
                recv, data,
                "corrected word must equal the original (bit {bit}, seed {seed:#x})",
            );
        }
    }
}

#[test]
fn corrects_single_check_bit_flip() {
    let data = 0x0123_4567_89ab_cdefu64.to_le_bytes();
    let mut parity = [0u8; 1];
    Hsiao7264::encode_word(&data, &mut parity);
    for bit in 0..8 {
        let mut bad_parity = parity;
        bad_parity[0] ^= 1 << bit;
        let mut recv = data;
        let outcome = Hsiao7264::decode_word(&mut recv, &bad_parity);
        assert_eq!(
            outcome,
            SecdedOutcome::Corrected,
            "check-bit flip {bit} must be reported corrected",
        );
        assert_eq!(recv, data, "data must be intact on a check-bit flip");
    }
}

#[test]
fn detects_every_double_data_bit_flip() {
    // Exhaustive over all C(64,2) data-bit pairs: every double flip must be
    // detected (Uncorrectable), never silently miscorrected.
    let seed = 0x0123_4567_89ab_cdefu64;
    let data = seed.to_le_bytes();
    let mut parity = [0u8; 1];
    Hsiao7264::encode_word(&data, &mut parity);
    for a in 0..64 {
        for b in (a + 1)..64 {
            let corrupted = seed ^ (1u64 << a) ^ (1u64 << b);
            let mut recv = corrupted.to_le_bytes();
            let outcome = Hsiao7264::decode_word(&mut recv, &parity);
            assert_eq!(
                outcome,
                SecdedOutcome::Uncorrectable,
                "double flip at bits {a},{b} must be detected, not miscorrected",
            );
            assert_eq!(
                recv.as_slice(),
                corrupted.to_le_bytes().as_slice(),
                "uncorrectable decode must leave the word unchanged",
            );
        }
    }
}

#[test]
fn overhead_is_one_eighth() {
    assert_eq!(Hsiao7264::PARITY_BYTES * 8, Hsiao7264::DATA_WORD_BYTES);
}

#[test]
fn block_parity_len_is_one_byte_per_eight() {
    assert_eq!(block_parity_len(0), 0);
    assert_eq!(block_parity_len(1), 1);
    assert_eq!(block_parity_len(8), 1);
    assert_eq!(block_parity_len(9), 2);
    assert_eq!(block_parity_len(64), 8);
    assert_eq!(block_parity_len(65), 9);
}

#[test]
fn block_round_trip_is_clean() {
    // Non-multiple-of-8 length exercises the zero-padded final word.
    let payload: alloc::vec::Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7)).collect();
    let parity = encode_block_parity(&payload);
    assert_eq!(parity.len(), block_parity_len(payload.len()));
    let mut recv = payload.clone();
    assert_eq!(try_correct_block(&mut recv, &parity), SecdedOutcome::Clean);
    assert_eq!(recv, payload, "clean block must be unchanged");
}

#[test]
fn block_corrects_single_bit_flip_at_every_byte() {
    let payload: alloc::vec::Vec<u8> = (0..100u8).map(|i| i.wrapping_mul(7)).collect();
    let parity = encode_block_parity(&payload);
    for byte_idx in 0..payload.len() {
        for bit in 0..8 {
            let mut recv = payload.clone();
            recv[byte_idx] ^= 1 << bit;
            let outcome = try_correct_block(&mut recv, &parity);
            assert_eq!(
                outcome,
                SecdedOutcome::Corrected,
                "single flip at byte {byte_idx} bit {bit} must be corrected",
            );
            assert_eq!(
                recv, payload,
                "block must be repaired to the original (byte {byte_idx}, bit {bit})",
            );
        }
    }
}

#[test]
fn block_detects_double_bit_flip_within_one_word() {
    let payload: alloc::vec::Vec<u8> = (0..32u8).collect();
    let parity = encode_block_parity(&payload);
    // Two flips inside the first 8-byte word.
    let mut recv = payload;
    recv[0] ^= 0b0000_0001;
    recv[1] ^= 0b0000_0010;
    assert_eq!(
        try_correct_block(&mut recv, &parity),
        SecdedOutcome::Uncorrectable,
        "two bit errors in one word must be detected",
    );
}

#[test]
fn block_truncated_parity_is_uncorrectable() {
    let payload: alloc::vec::Vec<u8> = (0..64u8).collect();
    let parity = encode_block_parity(&payload);
    let short = &parity[..parity.len() - 1];
    let mut recv = payload;
    assert_eq!(
        try_correct_block(&mut recv, short),
        SecdedOutcome::Uncorrectable,
        "a truncated parity trailer cannot verify the block",
    );
}
