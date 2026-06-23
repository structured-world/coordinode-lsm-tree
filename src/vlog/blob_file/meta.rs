// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::io::{LittleEndian, ReadBytesExt};
#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
use crate::{
    CompressionType, InternalValue, KeyRange, SeqNo, Slice,
    checksum::ChecksumType,
    coding::{Decode, Encode},
    comparator::default_comparator,
    table::{Block, DataBlock},
    vlog::BlobFileId,
};
#[cfg(feature = "std")]
use std::io::{Read, Write};

macro_rules! read_u64 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
    }};
}

macro_rules! read_u128 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)?
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u128::<LittleEndian>()?
    }};
}

pub const METADATA_HEADER_MAGIC: &[u8] = b"META";

// Note: `pub` for crate-internal use; parent `vlog` module is NOT
// exported from `lib.rs`, so this struct is not public API.
#[derive(Debug, PartialEq, Eq)]
pub struct Metadata {
    pub id: BlobFileId,

    /// Blob file format version (3 = V3, 4 = V4 with header CRC).
    pub version: u8,

    pub created_at: u128,

    /// Number of KV-pairs in the blob file
    pub item_count: u64,

    /// compressed size in bytes (on disk) (without metadata or trailer)
    pub total_compressed_bytes: u64,

    /// true size in bytes (if no compression were used)
    pub total_uncompressed_bytes: u64,

    /// Key range
    pub key_range: KeyRange,

    /// Compression type used for all blobs in this file
    pub compression: CompressionType,
}

impl Metadata {
    pub fn encode_into<W: Write>(&self, writer: &mut W) -> crate::Result<()> {
        fn meta(key: &str, value: &[u8]) -> InternalValue {
            InternalValue::from_components(key, value, 0, crate::ValueType::Value)
        }

        // Write header
        writer.write_all(METADATA_HEADER_MAGIC)?;

        #[rustfmt::skip]
        let meta_items = [
            meta("blob_file_version", &[self.version]),
            meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
            meta("compression", &self.compression.encode_into_vec()),
            meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
            meta("created_at", &self.created_at.to_le_bytes()),
            meta("file_size", &self.total_compressed_bytes.to_le_bytes()),
            meta("id", &self.id.to_le_bytes()),
            meta("item_count", &self.item_count.to_le_bytes()),
            meta("key#max", self.key_range.max()),
            meta("key#min", self.key_range.min()),
            meta("uncompressed_size", &self.total_uncompressed_bytes.to_le_bytes()),
        ];

        // NOTE: Just to make sure the items are definitely sorted
        #[cfg(debug_assertions)]
        {
            let is_sorted = meta_items.iter().is_sorted_by_key(|kv| &kv.key);
            assert!(is_sorted, "meta items not sorted correctly");
        }

        // TODO: no binary index
        let buf = DataBlock::encode_into_vec(&meta_items, 1, 0.0)?;

        // Blob files are currently not encrypted at all: neither this metadata
        // block nor the blob value frames/contents are covered by block-level
        // encryption. The metadata contains structural fields (version, counts,
        // compression) plus key_range (min/max keys), which may leak key
        // prefixes, and KV separation can leave large values on disk in
        // plaintext. Full blob-level encryption (metadata + contents) is
        // planned as a follow-up to block-level encryption.
        // TODO: encrypt blob metadata and blob contents when an encryption
        // provider is threaded through the blob file writer/reader paths.
        Block::write_into(
            writer,
            &buf,
            crate::table::block::BlockIdentity {
                // Mirror the table-meta bootstrapping exception:
                // blob meta is read via from_slice BEFORE the
                // reader knows the BlobFileId (the id is what
                // from_slice produces). For write/read AAD to
                // match once #251 wires AAD, the writer must
                // use the same table_id=0 the reader uses.
                // Pre-#251 the value is accepted-but-not-consumed,
                // but choosing the asymmetric `self.id` here would
                // bake a permanent decrypt-mismatch into any
                // encrypted blob meta we ever write.
                table_id: 0,
                block_type: crate::table::block::BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            },
            // Blob-meta blocks are always uncompressed and currently
            // never encrypted (see TODO above on blob-level
            // encryption). Plain transform here.
            &crate::table::block::BlockTransform::PLAIN,
        )?;

        Ok(())
    }

    pub fn from_slice(slice: &Slice) -> crate::Result<Self> {
        let reader = &mut &slice[..];

        // Check header
        let mut magic = [0u8; METADATA_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != METADATA_HEADER_MAGIC {
            return Err(crate::Error::InvalidHeader("BlobFileMeta"));
        }

        // TODO: Block::from_slice
        let block = Block::from_reader(
            reader,
            crate::table::block::BlockIdentity {
                // from_slice constructs Self by parsing the blob
                // meta — self.id is what THIS read produces, not
                // available beforehand. table_id=0 here mirrors
                // the table-meta parse path: cross-blob swap
                // detection still relies on the meta payload's
                // own id field being part of the verified body.
                table_id: 0,
                block_type: crate::table::block::BlockType::Meta,
                dict_id: 0,
                window_log: 0,
            },
            // Blob-meta blocks are always uncompressed and currently
            // never encrypted (see TODO above on blob-level
            // encryption). Plain transform here.
            &crate::table::block::BlockTransform::PLAIN,
        )?;
        let block = DataBlock::new(block);

        // Metadata keys are always lexicographic, so use the default comparator.
        let cmp = default_comparator();

        let version = {
            let bytes = block
                .point_read(b"blob_file_version", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;
            *bytes
                .value
                .first()
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
        };

        // Reject unknown versions early to catch corrupted or
        // future-incompatible metadata before downstream code
        // misinterprets header fields.
        match version {
            3 | 4 => {}
            _ => return Err(crate::Error::InvalidHeader("BlobFileMeta")),
        }

        let id = read_u64!(block, b"id", &cmp);
        let created_at = read_u128!(block, b"created_at", &cmp);
        let item_count = read_u64!(block, b"item_count", &cmp);
        let file_size = read_u64!(block, b"file_size", &cmp);
        let total_uncompressed_bytes = read_u64!(block, b"uncompressed_size", &cmp);

        let compression = {
            let bytes = block
                .point_read(b"compression", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"key#min", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
                .value,
            block
                .point_read(b"key#max", SeqNo::MAX, &cmp)?
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
                .value,
        ));

        Ok(Self {
            id,
            version,
            created_at,
            compression,
            item_count,
            total_compressed_bytes: file_size,
            total_uncompressed_bytes,
            key_range,
        })
    }
}

#[cfg(test)]
mod tests;
