use super::*;
// `test_log::test` shadows the std `test` attribute so every
// `#[test]` below routes through test-log for RUST_LOG capture.
use test_log::test;

fn xxh3_oracle(expected: u128) -> impl FnMut(&[u8]) -> bool {
    move |candidate: &[u8]| crate::hash::hash128(candidate) == expected
}

#[test]
fn shard_bytes_rounds_up_to_even_quarter() {
    // RS(4,2)-style splits: payload 4 → shard 1 → rounded to 2.
    assert_eq!(shard_bytes(4, 4), 2);
    // payload = 33 → ceil(33/4) = 9 → rounded to 10
    assert_eq!(shard_bytes(33, 4), 10);
    // payload = 4096 → exact, no rounding
    assert_eq!(shard_bytes(4096, 4), 1024);
    // payload = 4097 → ceil(4097/4) = 1025 → rounded to 1026
    assert_eq!(shard_bytes(4097, 4), 1026);
    // 10-shard split (10% XOR overhead): 4096 → ceil(409.6)=410
    assert_eq!(shard_bytes(4096, 10), 410);
    assert_eq!(shard_bytes(0, 4), 0);
}

#[test]
fn rs_4_2_layout_is_byte_identical_to_legacy() {
    // The legacy fixed scheme is RS(4,2); parity_len must match the
    // old `shard_bytes(N) * 2` formula so existing SSTs round-trip.
    for n in [1usize, 4, 33, 4096, 4097] {
        assert_eq!(
            parity_len(n, RS_DATA_SHARDS, RS_PARITY_SHARDS),
            shard_bytes(n, 4) * 2,
        );
    }
}

#[test]
fn rs_encode_decode_roundtrip_no_corruption() {
    let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 4, 2).expect("encode");
    assert_eq!(parity.len(), parity_len(payload.len(), 4, 2));
    let expected = crate::hash::hash128(&payload);
    let recovered = try_recover(
        &payload,
        &parity,
        payload.len(),
        4,
        2,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("recoverable");
    assert_eq!(recovered, payload);
}

#[test]
fn rs_recovers_from_double_data_shard_corruption() {
    let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 4, 2).expect("encode");
    let expected = crate::hash::hash128(&payload);
    let mut corrupted = payload.clone();
    let sb = shard_bytes(payload.len(), 4);
    for b in &mut corrupted[0..sb] {
        *b ^= 0xAA;
    }
    for b in &mut corrupted[2 * sb..3 * sb] {
        *b ^= 0xBB;
    }
    let recovered = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        4,
        2,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("double-shard corruption recoverable under RS(4,2)");
    assert_eq!(recovered, payload);
}

#[test]
fn rs_unrecoverable_when_three_shards_corrupt() {
    let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 4, 2).expect("encode");
    let expected = crate::hash::hash128(&payload);
    let mut corrupted = payload.clone();
    let sb = shard_bytes(payload.len(), 4);
    for b in &mut corrupted[0..3 * sb] {
        *b ^= 0xCC;
    }
    let outcome = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        4,
        2,
        xxh3_oracle(expected),
    )
    .expect("try_recover");
    assert!(
        outcome.is_none(),
        "three-shard corruption must be unrecoverable"
    );
}

#[test]
fn xor_single_parity_overhead_is_one_over_data_shards() {
    // 10 data shards → parity is exactly one shard ≈ 10% of payload.
    let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 10, 1).expect("encode");
    assert_eq!(parity.len(), parity_len(payload.len(), 10, 1));
    assert_eq!(parity.len(), shard_bytes(4096, 10)); // one shard
}

#[test]
fn xor_recovers_single_data_shard_loss() {
    let payload: Vec<u8> = (0..4096_u32)
        .map(|i| (i.wrapping_mul(7) & 0xff) as u8)
        .collect();
    let parity = encode_parity(&payload, 8, 1).expect("encode");
    let expected = crate::hash::hash128(&payload);
    let sb = shard_bytes(payload.len(), 8);
    let mut corrupted = payload.clone();
    // Wipe shard 3.
    for b in &mut corrupted[3 * sb..4 * sb] {
        *b ^= 0xFF;
    }
    let recovered = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        8,
        1,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("single-shard loss recoverable under XOR");
    assert_eq!(recovered, payload);
}

#[test]
fn xor_recovers_when_parity_itself_is_corrupt() {
    // Data intact, parity shard corrupt → candidate-0 path returns
    // the untouched data.
    let payload: Vec<u8> = (0..2048_u32).map(|i| (i & 0xff) as u8).collect();
    let mut parity = encode_parity(&payload, 8, 1).expect("encode");
    let expected = crate::hash::hash128(&payload);
    parity[0] ^= 0xFF;
    let recovered = try_recover(
        &payload,
        &parity,
        payload.len(),
        8,
        1,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("data intact, parity corrupt is recoverable");
    assert_eq!(recovered, payload);
}

#[test]
fn xor_unrecoverable_when_two_data_shards_lost() {
    // XOR single-parity tolerates exactly one shard loss.
    let payload: Vec<u8> = (0..4096_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 8, 1).expect("encode");
    let expected = crate::hash::hash128(&payload);
    let sb = shard_bytes(payload.len(), 8);
    let mut corrupted = payload.clone();
    for b in &mut corrupted[0..sb] {
        *b ^= 0xAA;
    }
    for b in &mut corrupted[2 * sb..3 * sb] {
        *b ^= 0xBB;
    }
    let outcome = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        8,
        1,
        xxh3_oracle(expected),
    )
    .expect("try_recover");
    assert!(
        outcome.is_none(),
        "two-shard loss must be unrecoverable under XOR"
    );
}

#[test]
fn rs_8_2_recovers_double_loss_low_overhead() {
    // RS(8,2): 25% overhead, two-shard tolerance — the higher tier.
    let payload: Vec<u8> = (0..8192_u32).map(|i| (i & 0xff) as u8).collect();
    let parity = encode_parity(&payload, 8, 2).expect("encode");
    assert_eq!(parity.len(), shard_bytes(8192, 8) * 2);
    let expected = crate::hash::hash128(&payload);
    let sb = shard_bytes(payload.len(), 8);
    let mut corrupted = payload.clone();
    for b in &mut corrupted[5 * sb..6 * sb] {
        *b ^= 0xAA;
    }
    for b in &mut corrupted[7 * sb..8 * sb] {
        *b ^= 0xBB;
    }
    let recovered = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        8,
        2,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("double-shard loss recoverable under RS(8,2)");
    assert_eq!(recovered, payload);
}

#[test]
fn handles_unaligned_block_size() {
    let payload: Vec<u8> = (0..33_u8).collect();
    let parity = encode_parity(&payload, 4, 2).expect("encode");
    let expected = crate::hash::hash128(&payload);
    let mut corrupted = payload.clone();
    corrupted[0] ^= 0xFF;
    let recovered = try_recover(
        &corrupted,
        &parity,
        payload.len(),
        4,
        2,
        xxh3_oracle(expected),
    )
    .expect("try_recover")
    .expect("unaligned single-shard corruption recoverable");
    assert_eq!(recovered, payload);
}

#[test]
fn next_combination_enumerates_all_pairs() {
    // C(4,2) = 6 pairs in lexicographic order.
    let mut combo = vec![0usize, 1];
    let mut seen = vec![combo.clone()];
    while next_combination(&mut combo, 4) {
        seen.push(combo.clone());
    }
    assert_eq!(
        seen,
        vec![
            vec![0, 1],
            vec![0, 2],
            vec![0, 3],
            vec![1, 2],
            vec![1, 3],
            vec![2, 3],
        ],
    );
}
