use super::*;
use crate::ValueType;

fn val(user_key: &[u8], value: &[u8], seqno: u64, vt: ValueType) -> InternalValue {
    InternalValue::from_components(user_key.to_vec(), value.to_vec(), seqno, vt)
}

#[test]
fn kv_digest_is_invariant_to_callsite_but_sensitive_to_content() {
    // The digest must depend only on logical content, so two
    // independent constructions of the same entry agree (carry ==
    // recompute), while any field change flips it.
    let a = val(b"key", b"value", 7, ValueType::Value);
    let same = val(b"key", b"value", 7, ValueType::Value);
    let d = ChecksumAlgorithm::Xxh3_64;
    assert_eq!(kv_digest(&a, d), kv_digest(&same, d));

    // Each logical field participates.
    assert_ne!(
        kv_digest(&a, d),
        kv_digest(&val(b"KEY", b"value", 7, ValueType::Value), d),
        "user_key must matter"
    );
    assert_ne!(
        kv_digest(&a, d),
        kv_digest(&val(b"key", b"VALUE", 7, ValueType::Value), d),
        "value must matter"
    );
    assert_ne!(
        kv_digest(&a, d),
        kv_digest(&val(b"key", b"value", 8, ValueType::Value), d),
        "seqno must matter"
    );
    assert_ne!(
        kv_digest(&a, d),
        kv_digest(&val(b"key", b"value", 7, ValueType::Tombstone), d),
        "value_type must matter"
    );
}

#[test]
fn kv_digest_is_injective_across_key_value_boundary() {
    // Without length framing, `user_key ‖ value` is ambiguous: the pair
    // (key="a", value="bc") and (key="ab", value="c") concatenate to the
    // same bytes, so a structured corruption that shifts the key/value
    // boundary would evade per-KV verification. The digest domain must be
    // injective for (value_type, seqno, user_key, value).
    let d = ChecksumAlgorithm::Xxh3_64;
    let a = val(b"a", b"bc", 7, ValueType::Value);
    let b = val(b"ab", b"c", 7, ValueType::Value);
    assert_ne!(
        kv_digest(&a, d),
        kv_digest(&b, d),
        "key/value boundary must be unambiguous in the digest domain",
    );
}

#[test]
fn footer_roundtrips_inner_payload() {
    // append_footer then split_inner must reproduce the inner bytes
    // exactly, for both digest widths (the footer occupies the tail).
    for algo in [ChecksumAlgorithm::Xxh3_64, ChecksumAlgorithm::Xxh3Low32] {
        let inner = b"standard data block payload bytes".to_vec();
        let digests: Vec<u64> = (0..5).map(|i| 0x0102_0304_0506_0708 ^ i).collect();

        let mut payload = inner.clone();
        append_footer(&mut payload, &digests, algo);
        assert!(
            payload.len() > inner.len(),
            "footer must add bytes for {algo:?}"
        );

        let recovered = split_inner(&payload).expect("well-formed footer must split");
        assert_eq!(recovered, &inner[..], "inner payload must round-trip");
    }
}

#[test]
fn split_inner_rejects_too_short() {
    // Fewer bytes than the fixed tail cannot carry a footer.
    assert!(split_inner(&[0u8; FOOTER_TAIL_LEN - 1]).is_err());
}

#[test]
fn split_inner_rejects_unknown_algorithm_tag() {
    // A tag outside the registry must be rejected, not silently
    // coerced to a known algorithm.
    let mut payload = b"inner".to_vec();
    payload.push(0xFE); // unknown algo tag
    payload.extend_from_slice(&0u32.to_le_bytes());
    assert!(split_inner(&payload).is_err());
}

#[test]
fn split_inner_rejects_count_exceeding_available_bytes() {
    // A forged count that would reach past the inner payload must be
    // rejected rather than slicing out of bounds.
    let mut payload = b"x".to_vec();
    payload.push(ChecksumAlgorithm::Xxh3_64.wire_tag());
    payload.extend_from_slice(&1000u32.to_le_bytes()); // 1000×8 ≫ payload
    assert!(split_inner(&payload).is_err());
}

#[test]
fn split_full_recovers_digests() {
    // The scrub path relies on split_full to recover the inner slice,
    // the per-entry digests in order, and the algorithm. Xxh3_64 keeps
    // the full u64; Xxh3Low32 stores only the low 32 bits, so compare
    // against the masked expectation.
    for algo in [ChecksumAlgorithm::Xxh3_64, ChecksumAlgorithm::Xxh3Low32] {
        let inner = b"standard data block payload bytes".to_vec();
        let digests: Vec<u64> = (0..5u64).map(|i| 0x0102_0304_0506_0708 ^ i).collect();

        let mut payload = inner.clone();
        append_footer(&mut payload, &digests, algo);

        let split = split_full(&payload).expect("well-formed footer must split");
        assert_eq!(split.inner, &inner[..], "inner payload must round-trip");
        assert_eq!(split.algo, algo, "algorithm tag must round-trip");

        let mask = if algo.digest_size() == 8 {
            u64::MAX
        } else {
            0xFFFF_FFFF
        };
        let expected: Vec<u64> = digests.iter().map(|d| d & mask).collect();
        assert_eq!(
            split.count(),
            expected.len(),
            "digest count must round-trip"
        );
        let recovered: Vec<u64> = (0..split.count())
            .map(|i| split.digest(i).expect("index < count is in range"))
            .collect();
        assert_eq!(
            recovered, expected,
            "digests must round-trip (masked to width) for {algo:?}"
        );
    }
}

#[test]
fn descriptor_byte_roundtrips_none_and_every_algorithm() {
    // The per-SST descriptor byte must round-trip "no footer" and each
    // known algorithm, with `0` reserved for absent so it never aliases
    // Xxh3_64 (wire tag 0).
    assert_eq!(descriptor_byte(None), 0);
    assert_eq!(
        descriptor_from_byte(0).expect("zero decodes"),
        None,
        "0 must mean no footer"
    );

    for algo in [
        ChecksumAlgorithm::Xxh3_64,
        ChecksumAlgorithm::Xxh3Low32,
        ChecksumAlgorithm::Crc32c,
    ] {
        let byte = descriptor_byte(Some(algo));
        assert_ne!(byte, 0, "present footer must not encode as 0 for {algo:?}");
        assert_eq!(
            descriptor_from_byte(byte).expect("present byte decodes"),
            Some(algo),
            "algorithm must round-trip for {algo:?}"
        );
    }
}

#[test]
fn descriptor_from_byte_rejects_unknown_nonzero_tag() {
    // A non-zero byte that maps to no known algorithm is corrupt /
    // forward-incompatible meta and must error, not silently decode.
    assert!(descriptor_from_byte(0xFE).is_err());
}
