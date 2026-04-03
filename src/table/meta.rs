// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, DataBlock};
use crate::fs::FsFile;
use crate::{
    CompressionType, KeyRange, SeqNo, TableId, checksum::ChecksumType, coding::Decode,
    comparator::default_comparator, table::block::BlockType,
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
}

macro_rules! read_u8 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("TableMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u8()?
    }};
}

macro_rules! read_u64 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("TableMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
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
            CompressionType::None,
            encryption,
            #[cfg(zstd_any)]
            None,
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
            BlockType::Meta,
            CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
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
}
