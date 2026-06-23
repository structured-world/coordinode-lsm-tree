#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    reason = "test assertions over known-good fixtures; failure surfaces via panic"
)]

use super::*;

fn key_hash(i: u64) -> u64 {
    crate::hash::hash64(&i.to_le_bytes())
}

#[test]
fn build_returns_none_for_empty_input() {
    let spec = LocatorSpec {
        precision: LocatorPrecision::Restart,
        block_id_bits: None,
        slot_bits: None,
    };
    assert!(build_locator_section(&[], spec).is_none());
}

#[test]
fn auto_width_section_round_trips_block_id_and_slot() {
    // 300 keys across 12 blocks, up to 25 restarts per block.
    let spec = LocatorSpec {
        precision: LocatorPrecision::Restart,
        block_id_bits: None,
        slot_bits: None,
    };
    let entries: Vec<(u64, u64, u64)> =
        (0..300u64).map(|i| (key_hash(i), i % 12, i % 25)).collect();
    let bytes = build_locator_section(&entries, spec).expect("section built");
    // Auto widths: 12 blocks → 4 bits, 25 slots → 5 bits.
    assert_eq!(bytes[2], 4);
    assert_eq!(bytes[3], 5);
    for i in 0..300u64 {
        assert_eq!(
            locate(&bytes, key_hash(i)).unwrap(),
            Some((i % 12, i % 25)),
            "key {i} locator mismatch",
        );
    }
}

#[test]
fn explicit_widths_too_small_skips_gracefully() {
    // max block_id = 100 needs 7 bits; configure only 4 → skip.
    let spec = LocatorSpec {
        precision: LocatorPrecision::Restart,
        block_id_bits: Some(4),
        slot_bits: Some(4),
    };
    let entries: Vec<(u64, u64, u64)> =
        (0..200u64).map(|i| (key_hash(i), i % 101, i % 8)).collect();
    assert!(build_locator_section(&entries, spec).is_none());
}

#[test]
fn explicit_widths_that_fit_round_trip() {
    let spec = LocatorSpec {
        precision: LocatorPrecision::Entry,
        block_id_bits: Some(10),
        slot_bits: Some(12),
    };
    let entries: Vec<(u64, u64, u64)> = (0..500u64)
        .map(|i| (key_hash(i), i % 1000, i % 4000))
        .collect();
    let bytes = build_locator_section(&entries, spec).expect("section built");
    assert_eq!(bytes[2], 10);
    assert_eq!(bytes[3], 12);
    for i in 0..500u64 {
        assert_eq!(
            locate(&bytes, key_hash(i)).unwrap(),
            Some((i % 1000, i % 4000))
        );
    }
}

#[test]
fn block_precision_section_round_trips_block_id_only() {
    // Block precision is the default: `slot` is dropped (locator = block_id),
    // so every key resolves to (block_id, 0). Covers the `block_only` build
    // path and a zero-width slot decode.
    let spec = LocatorSpec {
        precision: LocatorPrecision::Block,
        block_id_bits: None,
        slot_bits: None,
    };
    let entries: Vec<(u64, u64, u64)> =
        (0..300u64).map(|i| (key_hash(i), i % 12, i % 25)).collect();
    let bytes = build_locator_section(&entries, spec).expect("section built");
    assert_eq!(bytes[3], 0, "block precision must record slot_bits = 0");
    for i in 0..300u64 {
        assert_eq!(
            locate(&bytes, key_hash(i)).unwrap(),
            Some((i % 12, 0)),
            "key {i} must resolve to its block with slot 0",
        );
    }
}

#[test]
fn locate_rejects_truncated_section() {
    // A section shorter than the fixed header cannot be parsed.
    let err = locate(&[0u8; SECTION_HEADER_LEN - 1], 123).unwrap_err();
    assert!(matches!(err, crate::Error::InvalidHeader("LocatorSection")));
}

#[test]
fn build_skips_when_ribbon_cannot_satisfy_conflicting_values() {
    // Two entries share a key hash but map to different locators, so the
    // retrieval ribbon has no consistent solution. The build must fail
    // gracefully and skip the section (the point read falls back to the
    // index) rather than abort the SST.
    let spec = LocatorSpec {
        precision: LocatorPrecision::Restart,
        block_id_bits: None,
        slot_bits: None,
    };
    let mut entries: Vec<(u64, u64, u64)> =
        (0..200u64).map(|i| (key_hash(i), i % 8, i % 4)).collect();
    // Re-use entry 0's hash with a different (block_id, slot) → conflict.
    entries.push((key_hash(0), 7, 3));
    assert!(
        build_locator_section(&entries, spec).is_none(),
        "a conflicting hash collision must skip the section, not panic or abort",
    );
}

#[test]
fn locate_does_not_panic_on_forged_slot_bits_64() {
    // The writer never emits slot_bits == 64 (it enforces block_id_bits >= 1
    // and r <= 64), but a checksum-surviving corruption could. The block-id
    // extraction `packed >> slot_bits` must be guarded the same way the slot
    // mask is: `>> 64` panics in debug and wraps to `>> 0` in release. locate
    // must return without panicking for every key.
    let spec = LocatorSpec {
        precision: LocatorPrecision::Restart,
        block_id_bits: None,
        slot_bits: None,
    };
    let entries: Vec<(u64, u64, u64)> = (0..100u64).map(|i| (key_hash(i), i % 8, i % 4)).collect();
    let mut bytes = build_locator_section(&entries, spec).expect("section built");
    bytes[3] = 64; // forge slot_bits = 64
    for i in 0..100u64 {
        // Decoded values may be garbage, but the shift must not panic.
        let _ = locate(&bytes, key_hash(i)).expect("locate must not error");
    }
}

#[test]
fn locate_rejects_unknown_version() {
    // A header whose version byte is not the one this build writes is a
    // forward-incompatibility / corruption signal, surfaced as an error.
    let mut section = [0u8; SECTION_HEADER_LEN];
    section[0] = SECTION_VERSION.wrapping_add(1);
    let err = locate(&section, 123).unwrap_err();
    assert!(matches!(
        err,
        crate::Error::InvalidHeader("LocatorSection version")
    ));
}
