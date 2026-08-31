// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    Cache, GlobalTableId, TreeId, UserValue,
    version::BlobFileList,
    vlog::{ValueHandle, blob_file::reader::Reader},
};
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

pub struct Accessor<'a> {
    blob_files: &'a BlobFileList,
    #[cfg(zstd_any)]
    zstd_dictionary: Option<&'a crate::compression::ZstdDictionary>,
}

impl<'a> Accessor<'a> {
    pub fn new(blob_files: &'a BlobFileList) -> Self {
        Self {
            blob_files,
            #[cfg(zstd_any)]
            zstd_dictionary: None,
        }
    }

    /// Supplies the zstd dictionary for [`CompressionType::ZstdDict`](crate::CompressionType::ZstdDict) blob reads.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_dict(mut self, dict: Option<&'a crate::compression::ZstdDictionary>) -> Self {
        self.zstd_dictionary = dict;
        self
    }

    /// Reads one separated value.
    ///
    /// The blob file is reopened from the path it was RECOVERED under, so no
    /// caller supplies a base directory: a file can legitimately sit under a
    /// noncanonical spelling of its own id (`blobs/00` for id 0), and a path
    /// rebuilt from the id would miss it on every cache miss.
    ///
    /// # Errors
    ///
    /// Propagates the blob file's open / read failures.
    pub fn get(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(tree_id, vhandle, key) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.blob_files.get(vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)?;

        let reader = {
            let r = Reader::new(blob_file, file.as_ref());
            #[cfg(zstd_any)]
            let r = r.with_dict(self.zstd_dictionary);
            r
        };

        let value = reader.get(key, vhandle)?;
        cache.insert_blob(tree_id, vhandle, key, value.clone());

        Ok(Some(value))
    }

    /// Warms the cache with a run of upcoming separated values, coalescing
    /// adjacent records into as few reads as possible.
    ///
    /// A scan resolves one value per [`get`](Self::get), and each of those is
    /// its own read of a few hundred bytes. Values land in the blob file in the
    /// order the flush wrote them, which is key order, so a scan's next handles
    /// are usually its immediate on-disk neighbours: reading the whole run at
    /// once turns that stream of small reads into a handful of large ones.
    ///
    /// Purely an I/O optimization, and best-effort in both directions: it never
    /// changes which bytes [`get`](Self::get) returns (the same
    /// [`parse_record`](crate::vlog::blob_file::reader::Reader::parse_record)
    /// validates either path), and any failure here is dropped so the read walk
    /// handles that value authoritatively, including reporting its corruption.
    ///
    /// `items` is `(key, handle, _)` in scan order, and is CONSUMED as the
    /// working buffer: it is filtered and sorted in place, and its third field
    /// filled with each record's length, so one window costs the caller's
    /// single allocation and nothing per record.
    ///
    /// `max_gap` is how many wasted bytes between two records are worth
    /// swallowing to merge their reads; `max_read` caps a single coalesced
    /// read.
    pub fn prefetch(
        &self,
        tree_id: TreeId,
        items: &mut Vec<(&[u8], ValueHandle, usize)>,
        cache: &Cache,
        max_gap: u64,
        max_read: usize,
    ) {
        // Warm at most half the (shared) cache, so a prefetch cannot evict more
        // than it contributes. Mirrors the block prewarm's bound.
        let capacity = cache.capacity();
        if capacity == 0 {
            return;
        }

        // Keep the cold records: anything already cached needs no read, and
        // letting it anchor a span would widen the read for no gain. The record
        // length is computed once here and carried, so the span walk below and
        // the parse both use the one definition without recomputing it.
        //
        // This caps the READ, in on-disk bytes. It is not the admission budget:
        // a compressed blob file stores less than the cache will charge for the
        // decoded value, so a highly compressible window would pass a check made
        // here and still admit many times the cache's capacity. The admission
        // budget is enforced in `warm_span`, against the weight the cache
        // actually sees.
        let half = capacity / 2;
        let mut read_bytes: u64 = 0;
        items.retain_mut(|(key, vhandle, len)| {
            if read_bytes >= half || cache.contains_blob(tree_id, vhandle) {
                return false;
            }
            let Ok(record) = crate::vlog::blob_file::reader::record_len(key.len(), vhandle) else {
                return false;
            };
            read_bytes = read_bytes.saturating_add(record as u64);
            *len = record;
            true
        });

        // A single cold record is exactly what `get` already does well; the
        // prefetch only earns its keep by merging two or more.
        if items.len() < 2 {
            return;
        }

        // Group by blob file, then by offset: records reach us in key order,
        // which is on-disk order within ONE file, but a run can straddle files
        // (a compaction rewrote part of the range) and those interleave.
        //
        // Checked before sorting because the ordered case is the common one (a
        // window that stays inside one blob file arrives already grouped), and
        // proving it costs one linear pass against the sort's n log n.
        let key =
            |(_, vhandle, _): &(&[u8], ValueHandle, usize)| (vhandle.blob_file_id, vhandle.offset);
        if !items.is_sorted_by_key(key) {
            items.sort_unstable_by_key(key);
        }

        // What this prefetch may still admit, in the weight the cache charges.
        // Spans stop being warmed once it runs out.
        let mut admit_budget = half;

        let mut start = 0;
        while start < items.len() && admit_budget > 0 {
            #[expect(clippy::indexing_slicing, reason = "start < items.len() by the loop")]
            let (_, first, first_len) = items[start];
            let file_id = first.blob_file_id;

            // Extend the span while the next record is close enough to be worth
            // reading through the gap, and the whole span still fits one read.
            //
            // Offsets come from a `BlobIndirection` decoded out of an SST value,
            // so they are on-disk data and not to be trusted to be sane:
            // `record_len` bounds a record's length but nothing bounds where it
            // claims to start. Saturating adds keep a rotted offset near
            // `u64::MAX` from wrapping `span_end` below `span_start`, which
            // would turn the span length into an enormous read request.
            let mut end = start + 1;
            let mut span_end = first.offset.saturating_add(first_len as u64);
            while end < items.len() {
                #[expect(clippy::indexing_slicing, reason = "end < items.len() by the loop")]
                let (_, next, next_len) = items[end];
                if next.blob_file_id != file_id || next.offset > span_end.saturating_add(max_gap) {
                    break;
                }
                let next_end = next.offset.saturating_add(next_len as u64);
                if next_end.saturating_sub(first.offset) > max_read as u64 {
                    break;
                }
                span_end = span_end.max(next_end);
                end += 1;
            }

            if end - start >= 2
                && let Some(span) = items.get(start..end)
            {
                self.warm_span(
                    tree_id,
                    span,
                    first.offset,
                    span_end,
                    cache,
                    &mut admit_budget,
                );
            }
            start = end;
        }
    }

    /// Reads one coalesced span and parses every record it covers into the
    /// cache. Any failure returns early: those values stay cold and the read
    /// walk fetches them normally.
    ///
    /// `admit_budget` is how many bytes of cache weight this prefetch may still
    /// hand over, decremented by what each value actually weighs, and stopping
    /// the walk when it runs out.
    fn warm_span(
        &self,
        tree_id: TreeId,
        records: &[(&[u8], ValueHandle, usize)],
        span_start: u64,
        span_end: u64,
        cache: &Cache,
        admit_budget: &mut u64,
    ) {
        let Some((_, first, _)) = records.first() else {
            return;
        };
        let Some(blob_file) = self.blob_files.get(first.blob_file_id) else {
            return;
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));
        let Ok((file, _)) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)
        else {
            return;
        };

        let Ok(span_len) = usize::try_from(span_end.saturating_sub(span_start)) else {
            return;
        };
        let Ok(span) = crate::file::read_exact(file.as_ref(), span_start, span_len) else {
            return;
        };

        let reader = {
            let r = Reader::new(blob_file, file.as_ref());
            #[cfg(zstd_any)]
            let r = r.with_dict(self.zstd_dictionary);
            r
        };

        // An uncompressed value is returned as a VIEW into the buffer it was
        // parsed from. Sub-slicing the span would therefore make every cached
        // value pin the whole coalesced read: the cache would account for one
        // record and retain the entire window until the last of them is
        // evicted. Copy each record out instead, so a cached value owns exactly
        // its own bytes, exactly as it does on the one-record read path. The
        // compressed paths decompress into a fresh buffer already, so there the
        // view is dropped with this function and copying would be pure waste.
        let aliases_input = matches!(blob_file.0.meta.compression, crate::CompressionType::None);

        for &(key, vhandle, len) in records {
            if *admit_budget == 0 {
                return;
            }
            // Offsets came from the span walk above, so these are in range;
            // guard anyway rather than index, since a handle is on-disk data.
            let Ok(rel) = usize::try_from(vhandle.offset.saturating_sub(span_start)) else {
                continue;
            };
            let Some(record_end) = rel.checked_add(len) else {
                continue;
            };
            let Some(bytes) = span.get(rel..record_end) else {
                continue;
            };

            let record = if aliases_input {
                crate::Slice::from(bytes)
            } else {
                span.slice(rel..record_end)
            };
            if let Ok(value) = reader.parse_record(key, &vhandle, &record) {
                // Charged against the DECODED length, which is what the cache
                // weighs. Budgeting on the on-disk length instead would let a
                // compressed blob file admit several times the cache's
                // capacity from one window, evicting everything else to hold
                // values the scan has not reached yet.
                *admit_budget = admit_budget.saturating_sub(value.len() as u64);
                cache.insert_blob(tree_id, &vhandle, key, value);
            }
        }
    }
}
