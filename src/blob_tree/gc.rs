// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

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

    /// Unreferenced (garbage) blob bytes that could be freed from disk
    pub(crate) on_disk_bytes: u64,
}

impl FragmentationEntry {
    #[must_use]
    pub fn new(len: usize, bytes: u64, on_disk_bytes: u64) -> Self {
        Self {
            len,
            bytes,
            on_disk_bytes,
        }
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
    /// Returns the number of bytes that could be freed from disk.
    #[must_use]
    pub fn stale_bytes(&self) -> u64 {
        self.0.values().map(|x| x.on_disk_bytes).sum()
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
            other
                .0
                .entry(blob_file_id)
                .and_modify(|counter| {
                    counter.bytes += diff.bytes;
                    counter.len += diff.len;
                    counter.on_disk_bytes += diff.on_disk_bytes;
                })
                .or_insert(diff);
        }
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
            map.insert(id, FragmentationEntry::new(len, bytes, on_disk_bytes));
        }

        Ok(Self(map))
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

            let size = u64::from(vptr.size);
            let on_disk_size = u64::from(vptr.vhandle.on_disk_size);

            self.0
                .entry(vptr.vhandle.blob_file_id)
                .and_modify(|counter| {
                    counter.len += 1;
                    counter.bytes += size;
                    counter.on_disk_bytes += on_disk_size;
                })
                .or_insert_with(|| FragmentationEntry {
                    bytes: size,
                    on_disk_bytes: on_disk_size,
                    len: 1,
                });
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing)]
mod tests;
