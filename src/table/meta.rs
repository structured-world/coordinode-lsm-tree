// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{Block, BlockHandle, DataBlock};
use crate::fs::FsFile;
use crate::{
    CompressionType, KeyRange, SeqNo, TableId, checksum::ChecksumType, coding::Decode,
    comparator::default_comparator, runtime_config::ChecksumAlgorithm, table::block::BlockType,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::ops::Deref;

/// Nanosecond timestamp.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Timestamp(u128);

impl Deref for Timestamp {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Timestamp> for u128 {
    fn from(val: Timestamp) -> Self {
        val.0
    }
}

impl From<u128> for Timestamp {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
pub struct ParsedMeta {
    pub id: TableId,
    pub created_at: Timestamp,
    pub data_block_count: u64,
    pub index_block_count: u64,
    pub key_range: KeyRange,
    pub(super) seqnos: (SeqNo, SeqNo),

    /// Highest seqno from KV entries only (excludes range tombstones).
    ///
    /// Falls back to `seqnos.1` (overall max) for tables written before
    /// this field was introduced, which is conservative but correct.
    pub(super) highest_kv_seqno: SeqNo,
    pub file_size: u64,
    pub item_count: u64,
    pub tombstone_count: u64,
    pub weak_tombstone_count: u64,
    pub weak_tombstone_reclaimable: u64,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    /// Per-SST per-KV-footer descriptor: `Some(algo)` when every data block
    /// in this table carries a per-KV checksum footer under `algo`, `None`
    /// when the table was written with no per-KV footers. Sourced from the
    /// `descriptor#kv_checksum` meta byte. Because an SST is homogeneous,
    /// this single value lets the read / scrub path know the footer state
    /// of the whole table without inspecting any data block header.
    pub kv_checksum_algo: Option<ChecksumAlgorithm>,

    /// Per-SST Page-ECC indicator: `true` when this table was written with
    /// Page ECC enabled. Sourced from the `descriptor#page_ecc` meta byte.
    ///
    /// Note this is "ECC enabled for the table", not "every block carries a
    /// parity trailer": a block with an empty payload emits a zero-length
    /// trailer (`expected_parity_len(0) == 0`) even under `page_ecc`.
    ///
    /// For SST block types (`Data` / `Index` / `Filter` / `RangeTombstone`)
    /// this descriptor IS the parity-presence signal today: those blocks omit
    /// the `block_flags` byte on disk, so there is no per-block `ECC_PARITY`
    /// flag to read and the read path derives the trailer from this table-wide
    /// flag + `expected_parity_len(data_length)`. The self-describing block
    /// types (`Meta` / `Manifest` / `ManifestFooter`) keep the byte and still
    /// carry a per-block `ECC_PARITY` flag.
    pub page_ecc: bool,

    /// Index block format for this SST (#224). `0` = legacy: index
    /// entries carry no per-block seqno bounds (byte-identical to the
    /// pre-#224 format). `1` = each index entry includes
    /// `seqno_min` / `seqno_max` for its data block, enabling
    /// `scan_since_seqno` block-skip.
    ///
    /// Optional on disk: SSTs written before this field existed lack the
    /// `index_format` property and parse as `0`, so legacy tables read
    /// correctly and fall back to the per-entry filter scan path.
    pub index_format: u8,
}

macro_rules! read_u8 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let item = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("TableMeta"))?;
        // Single-byte meta value: reject an overlong / corrupt payload instead
        // of silently truncating to the first byte (read_u8 ignores trailing
        // bytes), which would weaken corruption detection on these
        // format-critical descriptor fields.
        match &item.value[..] {
            [b] => *b,
            _ => return Err(crate::Error::InvalidHeader("TableMeta")),
        }
    }};
}

macro_rules! read_u64 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let item = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("TableMeta"))?;
        // Exactly eight little-endian bytes: an overlong / short payload is
        // corrupt meta, not silently truncatable.
        let bytes = <[u8; 8]>::try_from(&item.value[..])
            .map_err(|_| crate::Error::InvalidHeader("TableMeta"))?;
        u64::from_le_bytes(bytes)
    }};
}

/// Validates that `kv_seqno` does not exceed `max_seqno`.
///
/// KV-only seqno must be ≤ overall max (which includes both KV and RT seqnos).
/// A value above `max_seqno` indicates on-disk corruption.
fn validated_kv_seqno(kv_seqno: SeqNo, max_seqno: SeqNo) -> crate::Result<SeqNo> {
    if kv_seqno > max_seqno {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "seqno#kv_max exceeds seqno#max",
        )
        .into());
    }
    Ok(kv_seqno)
}

fn validated_restart_interval_index(restart_interval: u8) -> crate::Result<u8> {
    if restart_interval == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "restart_interval#index must be greater than zero",
        )
        .into());
    }
    Ok(restart_interval)
}

impl ParsedMeta {
    #[expect(clippy::too_many_lines)]
    pub fn load_with_handle(
        file: &dyn FsFile,
        handle: &BlockHandle,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    ) -> crate::Result<Self> {
        let block = Block::from_file(
            file,
            *handle,
            crate::table::block::BlockIdentity {
                // Meta is the FIRST block read during table open;
                // table_id is what meta itself carries (chicken-and-
                // egg). Binding the meta block's AAD to its own
                // file offset still gives block-swap resistance
                // within a single file; cross-file substitution is
                // prevented by the meta block's own table_id field
                // being part of the verified payload.
                tree_id: 0,
                table_id: 0,
                block_offset: *handle.offset(),
                block_type: BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            },
            // Meta blocks are always written uncompressed; the
            // transform is therefore Plain (no encryption configured)
            // or Encrypted (provider supplied by the caller).
            &match encryption {
                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                None => crate::table::block::BlockTransform::PLAIN,
            },
        )?;

        if block.header.block_type != BlockType::Meta {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        let block = DataBlock::new(block);

        // Metadata keys are always lexicographic, so use the default comparator.
        let cmp = default_comparator();

        {
            let table_version = block
                .point_read(b"table_version", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                .value;

            if *table_version != [3u8] {
                return Err(crate::Error::InvalidHeader("TableMeta"));
            }
        }

        {
            let hash_type = block
                .point_read(b"filter_hash_type", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                .value;

            if *hash_type != [u8::from(ChecksumType::Xxh3)] {
                return Err(crate::Error::InvalidHeader("TableMeta"));
            }
        }

        {
            let hash_type = block
                .point_read(b"checksum_type", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                .value;

            if *hash_type != [u8::from(ChecksumType::Xxh3)] {
                return Err(crate::Error::InvalidHeader("TableMeta"));
            }
        }

        let _index_block_restart_interval =
            validated_restart_interval_index(read_u8!(block, b"restart_interval#index", &cmp))?;

        let id = read_u64!(block, b"table_id", &cmp);
        let item_count = read_u64!(block, b"item_count", &cmp);
        let tombstone_count = read_u64!(block, b"tombstone_count", &cmp);
        let data_block_count = read_u64!(block, b"block_count#data", &cmp);
        let index_block_count = read_u64!(block, b"block_count#index", &cmp);
        let _filter_block_count = read_u64!(block, b"block_count#filter", &cmp);
        let file_size = read_u64!(block, b"file_size", &cmp);
        let weak_tombstone_count = read_u64!(block, b"weak_tombstone_count", &cmp);
        let weak_tombstone_reclaimable = read_u64!(block, b"weak_tombstone_reclaimable", &cmp);

        let created_at = {
            let bytes = block
                .point_read(b"created_at", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?;

            let mut bytes = &bytes.value[..];
            bytes.read_u128::<LittleEndian>()?.into()
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"key#min", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                .value,
            block
                .point_read(b"key#max", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                .value,
        ));

        let seqnos = {
            let min = {
                let bytes = block
                    .point_read(b"seqno#min", SeqNo::MAX, &cmp)?
                    .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            let max = {
                let bytes = block
                    .point_read(b"seqno#max", SeqNo::MAX, &cmp)?
                    .ok_or(crate::Error::InvalidHeader("TableMeta"))?
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            (min, max)
        };

        // Optional field introduced for table-skip optimization.
        // Old tables lack this key; fall back to overall max seqno
        // (conservative: table-skip compares rt.seqno > highest_kv_seqno,
        // so falling back to the higher overall max just disables the
        // optimization for legacy tables — correct but not optimal).
        // If the key exists but is truncated, propagate the I/O error to
        // surface metadata corruption rather than silently falling back.
        let highest_kv_seqno =
            if let Some(item) = block.point_read(b"seqno#kv_max", SeqNo::MAX, &cmp)? {
                let mut bytes = &item.value[..];
                validated_kv_seqno(bytes.read_u64::<LittleEndian>()?, seqnos.1)?
            } else {
                seqnos.1
            };

        let data_block_compression = {
            let bytes = block
                .point_read(b"compression#data", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?;

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let index_block_compression = {
            let bytes = block
                .point_read(b"compression#index", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?;

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        // The per-SST transform descriptor keys are REQUIRED, not optional.
        // They are new in the V5 on-disk format, which bumped the block magic
        // to `[L,S,M,4]` (pre-V5 used `[L,S,M,3]`). A pre-V5 table's blocks —
        // including this meta block — are already rejected at the magic check
        // in `Header::decode_from` before this parse runs, so requiring the
        // descriptor adds no incremental incompatibility: there is no
        // readable older table that reaches this point lacking the key.
        // (Treating it as optional-with-default would only mask a corrupt or
        // truncated V5 meta block.)
        let kv_checksum_algo = crate::table::block::kv_checksum::descriptor_from_byte(read_u8!(
            block,
            b"descriptor#kv_checksum",
            &cmp
        ))?;

        let page_ecc = read_u8!(block, b"descriptor#page_ecc", &cmp) != 0;

        // Optional field introduced for scan_since_seqno block-skip (#224).
        // SSTs written before this key existed parse as 0 (legacy index
        // format, no per-block seqno bounds).
        let index_format = match block.point_read(b"index_format", SeqNo::MAX, &cmp)? {
            None => 0u8,
            // The value must be EXACTLY one byte. `read_u8` alone would ignore
            // trailing bytes, so a corrupt payload like `[1, 0xFF]` would parse
            // as `1`. Since this byte selects the index-entry decoder, require
            // an exact one-byte payload and reject anything else.
            Some(item) if item.value.len() == 1 => {
                let mut bytes = &item.value[..];
                bytes.read_u8()?
            }
            Some(_) => return Err(crate::Error::InvalidHeader("TableMeta")),
        };
        // Only `0` (legacy) and `1` (per-block seqno bounds) are defined in
        // this format slice. Reject any other byte as corrupt / forward-
        // incompatible metadata rather than letting a bogus on-disk format
        // flow downstream to the index decoder.
        if index_format > 1 {
            return Err(crate::Error::InvalidHeader("TableMeta"));
        }

        Ok(Self {
            id,
            created_at,
            data_block_count,
            index_block_count,
            key_range,
            seqnos,
            highest_kv_seqno,
            file_size,
            item_count,
            tombstone_count,
            weak_tombstone_count,
            weak_tombstone_reclaimable,
            data_block_compression,
            index_block_compression,
            kv_checksum_algo,
            page_ecc,
            index_format,
        })
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
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
        assert!(matches!(err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::InvalidData));
    }

    #[test]
    fn validated_restart_interval_index_non_zero() {
        assert_eq!(validated_restart_interval_index(1).unwrap(), 1);
        assert_eq!(validated_restart_interval_index(u8::MAX).unwrap(), u8::MAX);
    }

    #[test]
    fn validated_restart_interval_index_zero_returns_error() {
        let err = validated_restart_interval_index(0).unwrap_err();
        assert!(matches!(err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::InvalidData));
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
            meta("descriptor#page_ecc", &[0u8]),
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
        use std::io::Write;

        let encoded = DataBlock::encode_into_vec(items, 1, 0.0).unwrap();

        let mut buf = Vec::new();
        let _header = Block::write_into(
            &mut buf,
            &encoded,
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Meta),
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
        ParsedMeta::load_with_handle(&file, &handle, None)
    }

    /// Sanity check: valid meta items produce a successful parse.
    #[test]
    fn load_with_handle_valid_meta_succeeds() {
        let items = valid_meta_items();
        let result = load_meta_from_items(&items);
        assert!(result.is_ok(), "valid meta must parse: {result:?}");
    }

    #[test]
    fn index_format_absent_parses_as_legacy_zero() {
        // valid_meta_items() carries no `index_format` key (legacy SST).
        // It must parse as index_format = 0 so pre-#224 tables fall back
        // to the per-entry scan path.
        let meta = load_meta_from_items(&valid_meta_items()).unwrap();
        assert_eq!(meta.index_format, 0);
    }

    #[test]
    fn index_format_present_parses_its_value() {
        // An SST carrying `index_format = 1` must parse as 1 so the read
        // path knows the index entries hold per-block seqno bounds.
        let mut items = valid_meta_items();
        // Insert in sorted position (between filter_hash_type and
        // index_keys_have_seqno) to keep the block ordered.
        items.push(meta("index_format", &[1u8]));
        items.sort_by(|a, b| a.key.user_key.cmp(&b.key.user_key));
        let meta = load_meta_from_items(&items).unwrap();
        assert_eq!(meta.index_format, 1);
    }

    #[test]
    fn index_format_unknown_byte_is_rejected() {
        // Only 0 and 1 are defined in this format slice; a present-but-unknown
        // byte (e.g. corrupt metadata, or a forward-incompatible writer) must
        // fail fast as InvalidHeader rather than flowing downstream as a bogus
        // on-disk index format.
        for bad in [2u8, 255u8] {
            let mut items = valid_meta_items();
            items.push(meta("index_format", &[bad]));
            items.sort_by(|a, b| a.key.user_key.cmp(&b.key.user_key));
            let result = load_meta_from_items(&items);
            assert!(
                matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
                "index_format = {bad} must be rejected, got {result:?}",
            );
        }
    }

    #[test]
    fn index_format_overlong_payload_is_rejected() {
        // The index_format value must be exactly one byte. A payload with
        // trailing bytes (e.g. [1, 0xFF]) would otherwise parse as 1 because
        // read_u8 ignores the remainder — since this byte selects the index
        // decoder, an overlong payload is corrupt metadata and must be rejected.
        let mut items = valid_meta_items();
        items.push(meta("index_format", &[1u8, 0xFF]));
        items.sort_by(|a, b| a.key.user_key.cmp(&b.key.user_key));
        let result = load_meta_from_items(&items);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
            "overlong index_format payload must be rejected, got {result:?}",
        );
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

    /// An overlong single-byte descriptor payload (e.g. `[0, 0xFF]`) must be
    /// rejected as `InvalidHeader`, not silently truncated to the first byte —
    /// `read_u8!` would otherwise ignore the trailing bytes and weaken
    /// corruption detection on these format-critical per-SST descriptors.
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
                matches!(result, Err(crate::Error::InvalidHeader("TableMeta"))),
                "overlong {key} payload must be rejected, got {result:?}",
            );
        }
    }

    /// `descriptor#page_ecc` round-trips: `0` → `false` (no parity trailer),
    /// any non-zero → `true`.
    #[test]
    fn load_with_handle_page_ecc_descriptor_parses() {
        let parsed = load_meta_from_items(&valid_meta_items()).unwrap();
        assert!(!parsed.page_ecc, "0 byte means no Page ECC");

        let mut items = valid_meta_items();
        if let Some(item) = items
            .iter_mut()
            .find(|iv| &*iv.key.user_key == b"descriptor#page_ecc")
        {
            *item = meta("descriptor#page_ecc", &[1u8]);
        }
        let parsed = load_meta_from_items(&items).unwrap();
        assert!(parsed.page_ecc, "non-zero byte means Page ECC is on");
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
}
