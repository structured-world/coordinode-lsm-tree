// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::{
    blob_tree::handle::BlobIndirection, coding::Decode, compaction::stream::DroppedKvCallback,
    version::BlobFileList, vlog::BlobFileId,
};

/// Tracks fragmentation information in a blob file
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct FragmentationEntry {
    /// Number of unreferenced (garbage) blobs
    pub(crate) len: usize,

    /// Unreferenced (garbage) blob bytes that could be freed (compressed)
    pub(crate) bytes: u64,

    /// Unreferenced (garbage) bytes of frames of their own, freed from disk
    /// exactly
    pub(crate) on_disk_bytes: u64,

    /// Unreferenced (garbage) entries of values that share a block,
    /// uncompressed: their part of the file's blocks on disk is what they
    /// free
    pub(crate) block_bytes: u64,
}

impl FragmentationEntry {
    /// Garbage of `len` values of `bytes` in all, whose frames of their own
    /// take `on_disk_bytes`.
    #[must_use]
    pub fn new(len: usize, bytes: u64, on_disk_bytes: u64) -> Self {
        Self {
            len,
            bytes,
            on_disk_bytes,
            block_bytes: 0,
        }
    }

    /// This garbage with `block_bytes` of uncompressed entries of values that
    /// share a block.
    #[must_use]
    pub fn with_block_bytes(mut self, block_bytes: u64) -> Self {
        self.block_bytes = block_bytes;
        self
    }

    /// The bytes this garbage takes on disk in `meta`'s file.
    pub(crate) fn on_disk(&self, meta: &crate::vlog::blob_file::meta::Metadata) -> u64 {
        self.on_disk_bytes + meta.block_share(self.block_bytes)
    }
}

/// Tracks fragmentation information in a value log (list of blob files)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FragmentationMap(crate::HashMap<BlobFileId, FragmentationEntry>);

impl core::ops::Deref for FragmentationMap {
    type Target = crate::HashMap<BlobFileId, FragmentationEntry>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::ops::DerefMut for FragmentationMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FragmentationMap {
    /// Returns the number of bytes that could be freed from disk in the blob
    /// files of `value_log`.
    #[must_use]
    pub fn stale_bytes(&self, value_log: &BlobFileList) -> u64 {
        self.0
            .iter()
            .filter_map(|(id, x)| value_log.get(*id).map(|file| x.on_disk(file.meta())))
            .sum()
    }

    /// Removes blob file entries that are not part of the value log (anymore)
    /// to reduce linear memory growth.
    pub fn prune(&mut self, value_log: &BlobFileList) {
        self.0.retain(|&k, _| value_log.contains_key(k));
    }

    /// Merges a fragmentation map into another.
    ///
    /// This is used after a compaction stream is summed up (using the expiration callback), to apply
    /// the diff to the tree's fragmentation stats.
    pub fn merge_into(self, other: &mut Self) {
        for (blob_file_id, diff) in self.0 {
            other.add(blob_file_id, diff);
        }
    }

    /// Adds `diff` to the counters of `blob_file_id`.
    pub(crate) fn add(&mut self, blob_file_id: BlobFileId, diff: FragmentationEntry) {
        self.0
            .entry(blob_file_id)
            .and_modify(|counter| {
                counter.bytes += diff.bytes;
                counter.len += diff.len;
                counter.on_disk_bytes += diff.on_disk_bytes;
                counter.block_bytes += diff.block_bytes;
            })
            .or_insert(diff);
    }
}

impl crate::coding::Encode for FragmentationMap {
    fn encode_into<W: crate::io::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        use crate::io::{LE, WriteBytesExt};

        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 4 billion blob files"
        )]
        writer.write_u32::<LE>(self.len() as u32)?;

        for (blob_file_id, item) in self.iter() {
            writer.write_u64::<LE>(*blob_file_id)?;

            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are always less than 4 billion blobs in a blob file"
            )]
            writer.write_u32::<LE>(item.len as u32)?;

            writer.write_u64::<LE>(item.bytes)?;

            writer.write_u64::<LE>(item.on_disk_bytes)?;

            writer.write_u64::<LE>(item.block_bytes)?;
        }

        Ok(())
    }
}

impl crate::coding::Decode for FragmentationMap {
    fn decode_from<R: crate::io::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        use crate::io::{LE, ReadBytesExt};

        let len = reader.read_u32::<LE>()?;
        let mut map =
            crate::HashMap::with_capacity_and_hasher(len as usize, rustc_hash::FxBuildHasher);

        for _ in 0..len {
            let id = reader.read_u64::<LE>()?;
            let len = reader.read_u32::<LE>()? as usize;
            let bytes = reader.read_u64::<LE>()?;
            let on_disk_bytes = reader.read_u64::<LE>()?;
            let block_bytes = reader.read_u64::<LE>()?;
            map.insert(
                id,
                FragmentationEntry::new(len, bytes, on_disk_bytes).with_block_bytes(block_bytes),
            );
        }

        Ok(Self(map))
    }
}

impl FragmentationMap {
    /// Decodes a manifest's statistics, `bytes` being all of them: a snapshot
    /// section or an edit's field, each checksummed whole.
    ///
    /// Statistics of the earlier layout, whose entries lack the block bytes,
    /// decode with none, as that layout kept no blocks. They outlive the blob
    /// files they count, so a store whose blob files are all gone still opens;
    /// a live blob file of that layout is refused by its own metadata.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for bytes that are neither layout's.
    pub(crate) fn decode_whole(bytes: &[u8]) -> crate::Result<Self> {
        use crate::io::{LE, ReadBytesExt};

        // Id, count, bytes, frame bytes, then this layout's block bytes.
        const ENTRY_LEN: u64 = 8 + 4 + 8 + 8 + 8;
        const EARLIER_ENTRY_LEN: u64 = ENTRY_LEN - 8;

        let (count, entries) = bytes
            .split_first_chunk::<4>()
            .ok_or(crate::Error::InvalidHeader("BlobGcStats"))?;
        let count = u64::from(u32::from_le_bytes(*count));
        let len = entries.len() as u64;
        // The two layouts' lengths agree only on no entries at all.
        if len == count * ENTRY_LEN {
            return <Self as crate::coding::Decode>::decode_from(&mut &bytes[..]);
        }
        if len != count * EARLIER_ENTRY_LEN {
            return Err(crate::Error::InvalidHeader("BlobGcStats"));
        }
        let mut reader = entries;
        let mut map = crate::HashMap::with_capacity_and_hasher(
            usize::try_from(count).unwrap_or(0),
            rustc_hash::FxBuildHasher,
        );
        for _ in 0..count {
            let id = reader.read_u64::<LE>()?;
            let len = reader.read_u32::<LE>()? as usize;
            let bytes = reader.read_u64::<LE>()?;
            let on_disk_bytes = reader.read_u64::<LE>()?;
            map.insert(id, FragmentationEntry::new(len, bytes, on_disk_bytes));
        }
        Ok(Self(map))
    }

    /// Charges the object `vptr` names as garbage in its blob file.
    pub(crate) fn charge(&mut self, vptr: &BlobIndirection) {
        let size = u64::from(vptr.size);
        let (frame_bytes, block_bytes) = vptr.stored.split();

        self.0
            .entry(vptr.vhandle.blob_file_id)
            .and_modify(|counter| {
                counter.len += 1;
                counter.bytes += size;
                counter.on_disk_bytes += frame_bytes;
                counter.block_bytes += block_bytes;
            })
            .or_insert_with(|| {
                FragmentationEntry::new(1, size, frame_bytes).with_block_bytes(block_bytes)
            });
    }
}

impl DroppedKvCallback for FragmentationMap {
    fn on_dropped(&mut self, kv: &crate::InternalValue) {
        if kv.key.value_type.is_indirection() {
            let mut reader = &kv.value[..];

            #[expect(
                clippy::expect_used,
                reason = "data is read and checked for corruption, so we expect to be able to deserialize BlobIndirection fine"
            )]
            let vptr =
                BlobIndirection::decode_from(&mut reader).expect("should parse BlobIndirection");

            self.charge(&vptr);
        } else if kv.key.value_type.is_cell_row() {
            // Only the owning references are charged: a borrowed one leaving
            // frees nothing, its object is charged once, through its owner.
            #[expect(
                clippy::expect_used,
                reason = "data is read and checked for corruption, so we expect the cell row to decode"
            )]
            let refs =
                crate::blob_tree::field_row::row_refs(&kv.value).expect("should parse a cell row");
            for (vptr, owned) in refs {
                if owned {
                    self.charge(&vptr);
                }
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing)]
mod tests;
