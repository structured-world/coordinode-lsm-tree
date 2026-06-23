// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{Block, BlockHandle, DataBlock};
use crate::fs::FsFile;
use crate::{
    CompressionType, KeyRange, SeqNo, TableId, checksum::ChecksumType, coding::Decode,
    comparator::default_comparator, runtime_config::ChecksumAlgorithm, table::block::BlockType,
};
use core::ops::Deref;

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

    /// Sum of user-key byte lengths across all entries in this table (every
    /// version), or `None` for tables written before this field existed.
    /// Used by storage introspection to report the average key size; callers
    /// fall back to a file-size estimate when absent.
    pub sum_user_key_bytes: Option<u64>,

    /// Sum of value byte lengths across all entries in this table (every
    /// version), or `None` for tables written before this field existed.
    pub sum_value_bytes: Option<u64>,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    /// Per-SST per-KV-footer descriptor: `Some(algo)` when every data block
    /// in this table carries a per-KV checksum footer under `algo`, `None`
    /// when the table was written with no per-KV footers. Sourced from the
    /// `descriptor#kv_checksum` meta byte. Because an SST is homogeneous,
    /// this single value lets the read / scrub path know the footer state
    /// of the whole table without inspecting any data block header.
    pub kv_checksum_algo: Option<ChecksumAlgorithm>,

    /// `true` only when this table was written with a RECOGNIZED + applicable
    /// Page ECC scheme (`page_ecc == ecc_params.is_some()`). Under the
    /// three-state contract this is NOT a blanket "ECC present" flag: a table
    /// whose descriptor decodes to an unsupported scheme has `page_ecc = false`
    /// but [`Self::ecc_unrecognized`] `= true`.
    ///
    /// Callers MUST size / verify parity trailers from [`Self::ecc_params`]
    /// (the per-SST scheme), NOT from this boolean — for SST block types
    /// (`Data` / `Index` / `Filter` / `RangeTombstone`) decoded headers zero
    /// `block_flags`, so trailer sizing comes from `ecc_params` via
    /// `Header::on_disk_size_with(ecc)`. Self-describing block types (`Meta` /
    /// `Manifest` / `ManifestFooter`) keep the `block_flags` byte and still
    /// carry a per-block `ECC_PARITY` flag (fixed RS(4,2) layout).
    pub page_ecc: bool,

    /// Per-SST Page-ECC shard scheme decoded from the
    /// `descriptor#page_ecc` value: `Some(params)` when this table's
    /// blocks carry a parity trailer under a recognized + applicable scheme
    /// (the read path sizes + recovers with it), `None` otherwise. Agrees
    /// with [`Self::page_ecc`] (`page_ecc == ecc_params.is_some()`).
    pub ecc_params: Option<crate::table::block::EccParams>,

    /// `true` when the `descriptor#page_ecc` value decoded to an ECC scheme
    /// this build cannot apply: page granularity, an unknown kind, or a
    /// non-canonical descriptor (recognized block-granularity `Secded` /
    /// `Xor` / `ReedSolomon` are applicable, not flagged here). The
    /// per-block read still returns the payload (framed by `data_length`,
    /// checksum-verified) with `EccStatus::Unrecognized`, but the trailer
    /// length is not derivable from a scheme — so the sequential scrub walk
    /// cannot size it and must skip ECC verification for this table, warning
    /// that recompaction is needed. Mutually exclusive with a `Some`
    /// [`Self::ecc_params`].
    pub ecc_unrecognized: bool,

    /// Restart interval the data blocks were encoded with. Needed to rebuild a
    /// positional restart index when partial-decoding a block on the lazy read
    /// path (the restart heads sit every `data_block_restart_interval` entries).
    pub data_block_restart_interval: u8,

    /// Whether this SST's data blocks are column-organized (PAX) rather than
    /// row-major. Read from the optional `descriptor#columnar` property;
    /// defaults to `false` for SSTs written without it (row-major). The reader
    /// reconstructs row entries from a columnar block on load.
    pub columnar: bool,
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

macro_rules! read_u128 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let item = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("TableMeta"))?;
        // Exactly sixteen little-endian bytes — same exact-width contract as
        // read_u64!, so an overlong / short payload is rejected as corrupt meta.
        let bytes = <[u8; 16]>::try_from(&item.value[..])
            .map_err(|_| crate::Error::InvalidHeader("TableMeta"))?;
        u128::from_le_bytes(bytes)
    }};
}

/// Validates that `kv_seqno` does not exceed `max_seqno`.
///
/// KV-only seqno must be ≤ overall max (which includes both KV and RT seqnos).
/// A value above `max_seqno` indicates on-disk corruption.
fn validated_kv_seqno(kv_seqno: SeqNo, max_seqno: SeqNo) -> crate::Result<SeqNo> {
    if kv_seqno > max_seqno {
        return Err(crate::io::Error::new(
            crate::io::ErrorKind::InvalidData,
            "seqno#kv_max exceeds seqno#max",
        )
        .into());
    }
    Ok(kv_seqno)
}

fn validated_restart_interval_index(restart_interval: u8) -> crate::Result<u8> {
    if restart_interval == 0 {
        return Err(crate::io::Error::new(
            crate::io::ErrorKind::InvalidData,
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
        expected_table_id: Option<crate::TableId>,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    ) -> crate::Result<Self> {
        let block = Block::from_file(
            file,
            *handle,
            crate::table::block::BlockIdentity {
                // The caller supplies the durable `table_id` from an
                // out-of-band source (the SST file path / manifest entry),
                // NOT from this meta payload — so the Meta block's AAD binds
                // it to its owning table and a Meta block transplanted from
                // another SST fails AEAD verification. `table_id` is durable
                // across reopen (persisted as `metadata.id` + the file name),
                // unlike the ephemeral tree id, so binding it is safe.
                // `None` (diagnostic readers / unencrypted opens) maps to 0:
                // the AAD identity is unused without a provider, and no
                // payload-id cross-check is enforced.
                table_id: expected_table_id.unwrap_or(0),
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
        // Data-block restart interval: needed to rebuild a positional restart
        // index when partial-decoding a block (lazy read path).
        let data_block_restart_interval =
            validated_restart_interval_index(read_u8!(block, b"restart_interval#data", &cmp))?;

        // Optional layout descriptor: absent (older / row-major SSTs) means
        // row-major; otherwise exactly one byte, non-zero meaning columnar. An
        // empty or overlong payload is on-disk corruption, rejected here.
        let columnar = match block.point_read(b"descriptor#columnar", SeqNo::MAX, &cmp)? {
            None => false,
            Some(v) => match v.value.as_ref() {
                [b] => *b != 0,
                _ => return Err(crate::Error::InvalidHeader("TableMeta")),
            },
        };

        let id = read_u64!(block, b"table_id", &cmp);
        // Cross-check the payload's stored id against the caller's durable
        // expected id (manifest entry / SST path). A mismatch means a swapped
        // or wrong-id file: reject it here rather than recovering the table
        // under the wrong logical id (on encryption-OFF opens nothing else
        // catches it; on encryption-ON opens the AEAD already bound the
        // expected id, but the explicit check fails loudly + uniformly).
        // `None` skips the check for diagnostic readers (inspect / scrub).
        if let Some(expected) = expected_table_id
            && id != expected
        {
            return Err(crate::Error::InvalidHeader("TableMeta"));
        }
        let item_count = read_u64!(block, b"item_count", &cmp);
        let tombstone_count = read_u64!(block, b"tombstone_count", &cmp);
        let data_block_count = read_u64!(block, b"block_count#data", &cmp);
        let index_block_count = read_u64!(block, b"block_count#index", &cmp);
        let _filter_block_count = read_u64!(block, b"block_count#filter", &cmp);
        let file_size = read_u64!(block, b"file_size", &cmp);
        let weak_tombstone_count = read_u64!(block, b"weak_tombstone_count", &cmp);
        let weak_tombstone_reclaimable = read_u64!(block, b"weak_tombstone_reclaimable", &cmp);

        let created_at = read_u128!(block, b"created_at", &cmp).into();

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
            let min = read_u64!(block, b"seqno#min", &cmp);
            let max = read_u64!(block, b"seqno#max", &cmp);
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
                // Present-but-wrong-width is corrupt meta — require exactly 8
                // bytes rather than truncating an overlong payload.
                let bytes = <[u8; 8]>::try_from(&item.value[..])
                    .map_err(|_| crate::Error::InvalidHeader("TableMeta"))?;
                validated_kv_seqno(u64::from_le_bytes(bytes), seqnos.1)?
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

        // Per-SST ECC descriptor: 4 bytes [kind, data_shards,
        // parity_shards, granularity]. `kind == 0` = no parity. A present
        // descriptor records the exact shard scheme so the read path
        // re-derives the parity layout (no implicit RS(4,2) fallback).
        let (page_ecc, ecc_params, ecc_unrecognized) = {
            let v = block
                .point_read(b"descriptor#page_ecc", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("TableMeta"))?;
            use crate::runtime_config::{EccDescriptor, EccGranularity};
            // Three-state ECC contract: a recognized + applicable scheme
            // (`Secded`, `Xor`/`ReedSolomon`, block granularity, valid shards)
            // yields the recovery params (`ecc_unrecognized = false`). Anything
            // else that still decodes (page granularity, an unknown kind, or a
            // non-canonical descriptor) is NOT a hard error: it resolves to "no
            // recovery scheme" (`None`) with `ecc_unrecognized = true`. The
            // per-block read then frames the payload by `data_length`, verifies
            // it by checksum, and reports `EccStatus::Unrecognized` (a WARN +
            // recompaction hint) instead of failing; the scrub skips ECC-walk of
            // such tables.
            use crate::runtime_config::EccScheme;
            match crate::runtime_config::ecc_descriptor_from_bytes(&v.value)? {
                EccDescriptor::Off => (false, None, false),
                // SEC-DED has no shard layout (`shard_params() == None`); it
                // sizes its parity from `block_parity_len` instead, so it is
                // mapped to its dedicated `EccParams::SECDED` rather than going
                // through the shard path. Mirrors the writer's `resolve_ecc`.
                EccDescriptor::Recognized(EccScheme::Secded, EccGranularity::Block) => {
                    (true, Some(crate::table::block::EccParams::SECDED), false)
                }
                EccDescriptor::Recognized(scheme, EccGranularity::Block) => {
                    let params = scheme
                        .shard_params()
                        .map(|(d, p)| {
                            #[expect(
                                clippy::cast_possible_truncation,
                                reason = "shard counts originate as u8 in the descriptor"
                            )]
                            crate::table::block::EccParams::try_new(d as u8, p as u8)
                        })
                        .transpose()?;
                    // A non-SECDED scheme with no shard layout is unimplemented.
                    let unrecognized = params.is_none();
                    (params.is_some(), params, unrecognized)
                }
                EccDescriptor::Recognized(_, EccGranularity::Page)
                | EccDescriptor::Unrecognized => (false, None, true),
            }
        };

        // Optional shape fields (absent on tables written before storage
        // introspection support; callers fall back to a file-size estimate).
        // Present-but-wrong-width is corrupt meta, so require exactly 8 bytes.
        let read_opt_u64 = |key: &[u8]| -> crate::Result<Option<u64>> {
            match block.point_read(key, SeqNo::MAX, &cmp)? {
                Some(item) => {
                    let bytes = <[u8; 8]>::try_from(&item.value[..])
                        .map_err(|_| crate::Error::InvalidHeader("TableMeta"))?;
                    Ok(Some(u64::from_le_bytes(bytes)))
                }
                None => Ok(None),
            }
        };
        let sum_user_key_bytes = read_opt_u64(b"key_bytes#sum")?;
        let sum_value_bytes = read_opt_u64(b"value_bytes#sum")?;

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
            sum_user_key_bytes,
            sum_value_bytes,
            data_block_compression,
            index_block_compression,
            kv_checksum_algo,
            page_ecc,
            ecc_params,
            ecc_unrecognized,
            data_block_restart_interval,
            columnar,
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
mod tests;
