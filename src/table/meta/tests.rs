use super::*;

#[test]
fn validated_kv_seqno_within_bounds() {
    assert_eq!(validated_kv_seqno(5, 10).unwrap(), 5);
}

#[test]
fn validated_kv_seqno_equal_to_max() {
    assert_eq!(validated_kv_seqno(10, 10).unwrap(), 10);
}

#[test]
fn validated_kv_seqno_zero() {
    assert_eq!(validated_kv_seqno(0, 10).unwrap(), 0);
}

#[test]
fn validated_kv_seqno_exceeds_max_returns_error() {
    let err = validated_kv_seqno(11, 10).unwrap_err();
    assert!(matches!(err, crate::Error::Io(e) if e.kind() == crate::io::ErrorKind::InvalidData));
}

#[test]
fn validated_restart_interval_index_non_zero() {
    assert_eq!(validated_restart_interval_index(1).unwrap(), 1);
    assert_eq!(validated_restart_interval_index(u8::MAX).unwrap(), u8::MAX);
}

#[test]
fn validated_restart_interval_index_zero_returns_error() {
    let err = validated_restart_interval_index(0).unwrap_err();
    assert!(matches!(err, crate::Error::Io(e) if e.kind() == crate::io::ErrorKind::InvalidData));
}

// ---------------------------------------------------------------
// Regression tests for #201: ParsedMeta panics on corrupted meta
// ---------------------------------------------------------------

use crate::{InternalValue, coding::Encode};

fn meta(key: &str, value: &[u8]) -> InternalValue {
    InternalValue::from_components(key, value, 0, crate::ValueType::Value)
}

/// Build a complete set of valid meta items (same keys as table writer).
fn valid_meta_items() -> Vec<InternalValue> {
    vec![
        meta("block_count#data", &1u64.to_le_bytes()),
        meta("block_count#filter", &0u64.to_le_bytes()),
        meta("block_count#index", &1u64.to_le_bytes()),
        meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
        meta("compression#data", &CompressionType::None.encode_into_vec()),
        meta(
            "compression#index",
            &CompressionType::None.encode_into_vec(),
        ),
        meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
        meta("created_at", &1_000_000u128.to_le_bytes()),
        meta("data_block_hash_ratio", &0.0f64.to_le_bytes()),
        meta("descriptor#kv_checksum", &[0u8]),
        meta("descriptor#page_ecc", &[0u8, 0, 0, 0]),
        meta("file_size", &4096u64.to_le_bytes()),
        meta("filter_hash_type", &[u8::from(ChecksumType::Xxh3)]),
        meta("index_keys_have_seqno", &[0x1]),
        meta("initial_level", &[0]),
        meta("item_count", &10u64.to_le_bytes()),
        meta("key#max", b"z"),
        meta("key#min", b"a"),
        meta("key_count", &10u64.to_le_bytes()),
        meta("prefix_truncation#data", &[1]),
        meta("prefix_truncation#index", &[1]),
        meta("range_tombstone_count", &0u64.to_le_bytes()),
        meta("restart_interval#data", &[16]),
        meta("restart_interval#index", &[4]),
        meta("seqno#kv_max", &5u64.to_le_bytes()),
        meta("seqno#max", &10u64.to_le_bytes()),
        meta("seqno#min", &1u64.to_le_bytes()),
        meta("table_id", &42u64.to_le_bytes()),
        meta("table_version", &[3u8]),
        meta("tombstone_count", &0u64.to_le_bytes()),
        meta("user_data_size", &1024u64.to_le_bytes()),
        meta("weak_tombstone_count", &0u64.to_le_bytes()),
        meta("weak_tombstone_reclaimable", &0u64.to_le_bytes()),
    ]
}

/// Write a meta block from given items to a temp file and call
/// `ParsedMeta::load_with_handle`, returning the result.
fn load_meta_from_items(items: &[InternalValue]) -> crate::Result<ParsedMeta> {
    load_meta_from_items_expecting(items, None)
}

/// Like [`load_meta_from_items`] but threads an explicit
/// `expected_table_id` into `load_with_handle` so the payload-id
/// cross-check can be exercised.
fn load_meta_from_items_expecting(
    items: &[InternalValue],
    expected_table_id: Option<TableId>,
) -> crate::Result<ParsedMeta> {
    use std::io::Write;

    let encoded = DataBlock::encode_into_vec(items, 1, 0.0).unwrap();

    let mut buf = Vec::new();
    let _header = Block::write_into(
        &mut buf,
        &encoded,
        crate::table::block::BlockIdentity::for_test(0, BlockType::Meta),
        &crate::table::block::BlockTransform::PLAIN,
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.block");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }

    let file = std::fs::File::open(&path).unwrap();
    #[expect(clippy::cast_possible_truncation, reason = "test meta blocks are tiny")]
    let handle = BlockHandle::new(crate::table::BlockOffset(0), buf.len() as u32);
    ParsedMeta::load_with_handle(&file, &handle, expected_table_id, None)
}

/// Regression: `valid_meta_items` encodes `table_id = 42`. When the caller
/// supplies a DIFFERENT `expected_table_id`, the parse must be rejected
/// (out-of-band id is the durable identity; a payload whose stored id
/// disagrees is a swapped / wrong file). `Some(42)` must succeed; `None`
/// (diagnostic readers) skips the cross-check.
#[test]
fn load_with_handle_rejects_table_id_mismatch() {
    let items = valid_meta_items();

    let mismatch = load_meta_from_items_expecting(&items, Some(99));
    assert!(
        matches!(mismatch, Err(crate::Error::InvalidHeader("TableMeta"))),
        "payload table_id 42 read under expected 99 must be rejected, got {mismatch:?}"
    );

    assert!(
        load_meta_from_items_expecting(&items, Some(42)).is_ok(),
        "matching expected table_id must parse",
    );
    assert!(
        load_meta_from_items_expecting(&items, None).is_ok(),
        "None expected id (diagnostic read) must skip the cross-check",
    );
}

/// Sanity check: valid meta items produce a successful parse.
#[test]
fn load_with_handle_valid_meta_succeeds() {
    let items = valid_meta_items();
    let result = load_meta_from_items(&items);
    assert!(result.is_ok(), "valid meta must parse: {result:?}");
}

/// Backward compatibility: an SST written before the per-table key/value
/// byte sums existed has no `key_bytes#sum` / `value_bytes#sum` meta keys
/// (`valid_meta_items` omits them, mirroring an older writer). The parse
/// must succeed with both fields `None`, never error.
#[test]
fn load_with_handle_missing_key_value_byte_sums_parses_as_none() {
    let items = valid_meta_items();
    let parsed = load_meta_from_items(&items).unwrap();
    assert_eq!(parsed.sum_user_key_bytes, None);
    assert_eq!(parsed.sum_value_bytes, None);
}

/// When present, the byte sums round-trip through the meta block as
/// `Some(_)`.
#[test]
fn load_with_handle_key_value_byte_sums_present_round_trip() {
    let mut items = valid_meta_items();
    items.push(meta("key_bytes#sum", &320u64.to_le_bytes()));
    items.push(meta("value_bytes#sum", &640u64.to_le_bytes()));
    // The meta block is a sorted KV block; re-sort after appending.
    items.sort_by(|a, b| a.key.user_key.cmp(&b.key.user_key));

    let parsed = load_meta_from_items(&items).unwrap();
    assert_eq!(parsed.sum_user_key_bytes, Some(320));
    assert_eq!(parsed.sum_value_bytes, Some(640));
}

/// Missing `table_version` must return `Err(InvalidHeader)`, not panic.
#[test]
fn load_with_handle_missing_table_version_returns_err() {
    let items: Vec<_> = valid_meta_items()
        .into_iter()
        .filter(|iv| &*iv.key.user_key != b"table_version")
        .collect();
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}

/// Wrong `table_version` value must return `Err(InvalidHeader)`, not panic.
#[test]
fn load_with_handle_wrong_table_version_returns_err() {
    let mut items = valid_meta_items();
    if let Some(item) = items
        .iter_mut()
        .find(|iv| &*iv.key.user_key == b"table_version")
    {
        *item = meta("table_version", &[99u8]);
    }
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}

/// Missing `key#min` must return `Err(InvalidHeader)`, not panic.
#[test]
fn load_with_handle_missing_key_min_returns_err() {
    let items: Vec<_> = valid_meta_items()
        .into_iter()
        .filter(|iv| &*iv.key.user_key != b"key#min")
        .collect();
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}

/// Missing `compression#data` must return `Err(InvalidHeader)`, not panic.
#[test]
fn load_with_handle_missing_compression_data_returns_err() {
    let items: Vec<_> = valid_meta_items()
        .into_iter()
        .filter(|iv| &*iv.key.user_key != b"compression#data")
        .collect();
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}

/// A `descriptor#kv_checksum` byte of 0 parses as "no per-KV footer"
/// (`kv_checksum_algo == None`) — the common, footer-free table.
#[test]
fn load_with_handle_kv_descriptor_zero_parses_as_none() {
    let parsed = load_meta_from_items(&valid_meta_items()).unwrap();
    assert_eq!(parsed.kv_checksum_algo, None);
}

/// A non-zero `descriptor#kv_checksum` byte round-trips to the encoded
/// algorithm, so the read / scrub path learns the whole table's footer
/// state from this single per-SST byte.
#[test]
fn load_with_handle_kv_descriptor_nonzero_parses_algorithm() {
    let mut items = valid_meta_items();
    let byte =
        crate::table::block::kv_checksum::descriptor_byte(Some(ChecksumAlgorithm::Xxh3Low32));
    if let Some(item) = items
        .iter_mut()
        .find(|iv| &*iv.key.user_key == b"descriptor#kv_checksum")
    {
        *item = meta("descriptor#kv_checksum", &[byte]);
    }
    let parsed = load_meta_from_items(&items).unwrap();
    assert_eq!(parsed.kv_checksum_algo, Some(ChecksumAlgorithm::Xxh3Low32));
}

/// Missing `descriptor#kv_checksum` must return `Err(InvalidHeader)`,
/// not panic — the descriptor is a required per-SST field.
#[test]
fn load_with_handle_missing_kv_descriptor_returns_err() {
    let items: Vec<_> = valid_meta_items()
        .into_iter()
        .filter(|iv| &*iv.key.user_key != b"descriptor#kv_checksum")
        .collect();
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}

/// A wrong-length descriptor payload (e.g. `[0, 0xFF]`) must be rejected,
/// not silently truncated to the first byte — otherwise trailing bytes
/// would be ignored and weaken corruption detection on these
/// format-critical per-SST descriptors. `descriptor#kv_checksum` is a
/// single byte (rejected as `InvalidHeader`); `descriptor#page_ecc` is a
/// fixed 4-byte value whose codec rejects a wrong length as
/// `InvalidTrailer`.
#[test]
fn load_with_handle_overlong_descriptor_payload_is_rejected() {
    for key in ["descriptor#kv_checksum", "descriptor#page_ecc"] {
        let mut items = valid_meta_items();
        if let Some(item) = items
            .iter_mut()
            .find(|iv| &*iv.key.user_key == key.as_bytes())
        {
            *item = meta(key, &[0u8, 0xFF]);
        }
        let result = load_meta_from_items(&items);
        assert!(
            matches!(
                result,
                Err(crate::Error::InvalidHeader("TableMeta") | crate::Error::InvalidTrailer)
            ),
            "wrong-length {key} payload must be rejected, got {result:?}",
        );
    }
}

#[test]
fn load_with_handle_overlong_fixed_width_field_is_rejected() {
    // created_at (16 B) and the seqno fields (8 B each) are fixed-width;
    // an overlong payload (junk byte appended) must be rejected as
    // InvalidHeader, not silently truncated to the leading bytes.
    for (key, mut value) in [
        ("created_at", 1_000_000u128.to_le_bytes().to_vec()),
        ("seqno#min", 1u64.to_le_bytes().to_vec()),
        ("seqno#max", 10u64.to_le_bytes().to_vec()),
        ("seqno#kv_max", 5u64.to_le_bytes().to_vec()),
    ] {
        value.push(0xFF); // overlong
        let mut items = valid_meta_items();
        if let Some(item) = items
            .iter_mut()
            .find(|iv| &*iv.key.user_key == key.as_bytes())
        {
            *item = meta(key, &value);
        }
        let result = load_meta_from_items(&items);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
            "overlong {key} payload must be rejected, got {result:?}",
        );
    }
}

/// `descriptor#page_ecc` round-trips: `[0,0,0,0]` → off, and a shard
/// scheme descriptor → on with the decoded `EccParams`.
#[test]
fn load_with_handle_page_ecc_descriptor_parses() {
    let parsed = load_meta_from_items(&valid_meta_items()).unwrap();
    assert!(!parsed.page_ecc, "kind 0 means no Page ECC");
    assert_eq!(parsed.ecc_params, None);

    let mut items = valid_meta_items();
    if let Some(item) = items
        .iter_mut()
        .find(|iv| &*iv.key.user_key == b"descriptor#page_ecc")
    {
        // kind 3 = ReedSolomon, data_shards 8, parity_shards 2, Block.
        *item = meta("descriptor#page_ecc", &[3u8, 8, 2, 0]);
    }
    let parsed = load_meta_from_items(&items).unwrap();
    assert!(parsed.page_ecc, "a present scheme means Page ECC is on");
    assert_eq!(
        parsed.ecc_params,
        Some(crate::table::block::EccParams::try_new(8, 2).unwrap()),
    );
}

/// An unsupported-but-decodable ECC descriptor (page granularity, an unknown
/// kind, a non-canonical "off") does NOT fail meta load: it resolves to "no
/// recovery scheme" (`ecc_params == None`). The per-block read then frames
/// the payload by `data_length` and reports `EccStatus::Unrecognized` (a
/// WARN) rather than failing the read. This is the three-state contract:
/// unrecognized ECC is a typing warning, not corruption.
#[test]
fn load_with_handle_unsupported_ecc_descriptor_parses_without_recovery_scheme() {
    for descriptor in [
        [3u8, 8, 2, 1], // ReedSolomon(8,2) with page granularity
        [9u8, 0, 0, 0], // unknown kind
        [0u8, 8, 2, 1], // non-canonical "off" with junk reserved bytes
    ] {
        let mut items = valid_meta_items();
        if let Some(item) = items
            .iter_mut()
            .find(|iv| &*iv.key.user_key == b"descriptor#page_ecc")
        {
            *item = meta("descriptor#page_ecc", &descriptor);
        }
        let parsed = load_meta_from_items(&items)
            .unwrap_or_else(|e| panic!("descriptor {descriptor:?} must parse, got {e:?}"));
        assert_eq!(
            parsed.ecc_params, None,
            "unsupported descriptor {descriptor:?} must yield no recovery scheme",
        );
        assert!(
            parsed.ecc_unrecognized,
            "unsupported descriptor {descriptor:?} must flag ecc_unrecognized \
             (drives the scrub warn + skip)",
        );
        assert!(
            !parsed.page_ecc,
            "unrecognized ECC is not 'recognized active'"
        );
    }

    // Sanity: a recognized + applicable scheme is NOT flagged unrecognized,
    // and `Off` is neither.
    let parsed = load_meta_from_items(&valid_meta_items()).unwrap();
    assert!(!parsed.ecc_unrecognized && !parsed.page_ecc, "off");

    let mut items = valid_meta_items();
    if let Some(item) = items
        .iter_mut()
        .find(|iv| &*iv.key.user_key == b"descriptor#page_ecc")
    {
        *item = meta("descriptor#page_ecc", &[3u8, 8, 2, 0]); // RS(8,2) block
    }
    let parsed = load_meta_from_items(&items).unwrap();
    assert!(
        parsed.page_ecc && !parsed.ecc_unrecognized && parsed.ecc_params.is_some(),
        "recognized RS(8,2) is applicable, not unrecognized",
    );

    // SEC-DED (block granularity, no shard layout) is a recognized,
    // applicable scheme: it resolves to its dedicated `EccParams::SECDED`,
    // is NOT flagged unrecognized, and turns Page ECC on.
    let mut items = valid_meta_items();
    if let Some(item) = items
        .iter_mut()
        .find(|iv| &*iv.key.user_key == b"descriptor#page_ecc")
    {
        *item = meta("descriptor#page_ecc", &[1u8, 0, 0, 0]); // Secded, block
    }
    let parsed = load_meta_from_items(&items).unwrap();
    assert!(parsed.page_ecc, "SEC-DED turns Page ECC on");
    assert!(
        !parsed.ecc_unrecognized,
        "SEC-DED is recognized, not a warning"
    );
    assert_eq!(
        parsed.ecc_params,
        Some(crate::table::block::EccParams::SECDED),
        "SEC-DED resolves to its dedicated parity params",
    );
}

/// Missing `descriptor#page_ecc` must return `Err(InvalidHeader)`, not
/// panic — it is a required per-SST field.
#[test]
fn load_with_handle_missing_page_ecc_descriptor_returns_err() {
    let items: Vec<_> = valid_meta_items()
        .into_iter()
        .filter(|iv| &*iv.key.user_key != b"descriptor#page_ecc")
        .collect();
    let result = load_meta_from_items(&items);
    assert!(
        matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
        "expected InvalidHeader(\"TableMeta\"), got {result:?}",
    );
}
