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
        // This is a pre-filter on the records, not the read budget. What is
        // actually read is a span's EXTENT, which also covers the gaps merged
        // into it, so the budget that bounds the I/O is applied per span below.
        // Nor is it the admission budget: a compressed blob file stores less
        // than the cache will charge for the decoded value, so that is enforced
        // separately in `warm_span` against the weight the cache sees.
        let half = capacity / 2;
        let mut record_bytes: u64 = 0;
        let mut full = false;
        items.retain_mut(|(key, vhandle, len)| {
            if full || cache.contains_blob(tree_id, vhandle) {
                return false;
            }
            let Ok(record) = crate::vlog::blob_file::reader::record_len(key.len(), vhandle) else {
                return false;
            };
            // Checked BEFORE keeping, so the record that fills the budget is
            // dropped rather than admitted whole on top of it. Plain add: a
            // window holds at most `u16::MAX` records and `record_len` caps
            // each at the 256 MiB value limit plus its header, so the running
            // total cannot approach `u64::MAX`.
            if record_bytes + record as u64 > half {
                full = true;
                return false;
            }
            record_bytes += record as u64;
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

        // Two budgets, because the two costs are different. `read_budget` is
        // the I/O: what a span costs is its EXTENT, gaps included, not the sum
        // of its records, and merging through a gap is exactly what buys the
        // speedup. `admit_budget` is the cache weight, which for a compressed
        // file is a different number again. Each bounds the prefetch at half
        // the cache.
        let mut read_budget = half;
        let mut admit_budget = half;

        let mut start = 0;
        while start < items.len() && admit_budget > 0 && read_budget > 0 {
            #[expect(clippy::indexing_slicing, reason = "start < items.len() by the loop")]
            let (_, first, _) = items[start];

            // The span may not reach past what is left of the read budget, so a
            // run of small records with wide gaps cannot turn into a read the
            // budget never sanctioned.
            let Some((end, span_end)) =
                span_extent(items, start, max_gap, max_read as u64, read_budget)
            else {
                start += 1;
                continue;
            };

            if end - start >= 2
                && let Some(span) = items.get(start..end)
            {
                // `span_extent` bounds the extent by `reach`, so it fits.
                debug_assert!(span_end - first.offset <= read_budget);
                read_budget -= span_end - first.offset;
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

        // `span_end` was built as the maximum record end in the span, and every
        // one of those is at or past `span_start` (the span's own first
        // offset, in a slice sorted by offset), so the span is never negative.
        debug_assert!(span_end >= span_start);
        let Ok(span_len) = usize::try_from(span_end - span_start) else {
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
            // Every record in the span is at or past its start: the slice is
            // sorted by offset and `span_start` is its first one.
            debug_assert!(vhandle.offset >= span_start);
            let Ok(rel) = usize::try_from(vhandle.offset - span_start) else {
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
                // Checked BEFORE inserting, against the full weight the cache
                // charges (key as well as value), and against the DECODED
                // length rather than the on-disk one: a compressed blob file
                // stores less than the cache accounts for, so budgeting on
                // what was read would admit several times the capacity from
                // one window and evict everything else to hold values the scan
                // has not reached yet. Deducting after the fact would let the
                // last value of a window exceed the bound by its own size.
                let weight = (key.len() + value.len()) as u64;
                if weight > *admit_budget {
                    *admit_budget = 0;
                    return;
                }
                *admit_budget -= weight;
                cache.insert_blob(tree_id, &vhandle, key, value);
            }
        }
    }
}

/// How far one coalesced span reaches, starting at `items[start]`.
///
/// Returns the exclusive end index and the span's end offset, or `None` when
/// the first record's own end does not fit a `u64`.
///
/// A span grows while the next record is in the same blob file, starts within
/// `max_gap` of what the span already covers, and leaves the whole extent
/// within BOTH bounds: `max_read`, the cap on any single read, and
/// `read_budget`, what is left of this prefetch's I/O allowance.
///
/// The extent is what a caller actually reads, GAPS INCLUDED. Merging through a
/// gap is the point, but it means the read is wider than the records in it, so
/// both bounds have to be applied to the extent. Bounding the record bytes
/// instead would let a run of small records with wide gaps read many times what
/// the budget allowed.
///
/// Offsets come from a `BlobIndirection` decoded out of an SST value, so they
/// are on-disk data: `record_len` bounds a record's length, but nothing bounds
/// where it claims to start. An end that does not fit a `u64` is a handle no
/// writer produced, and is rejected rather than clamped. Clamping would be
/// worse than wrong here: a saturated `span_end + max_gap` compares as
/// "nothing is too far away", which merges the span across a gap of any size.
///
/// `items` must be sorted by `(blob_file_id, offset)`.
fn span_extent(
    items: &[(&[u8], ValueHandle, usize)],
    start: usize,
    max_gap: u64,
    max_read: u64,
    read_budget: u64,
) -> Option<(usize, u64)> {
    let reach = max_read.min(read_budget);
    let (_, first, first_len) = *items.get(start)?;
    let file_id = first.blob_file_id;

    let mut end = start + 1;
    let mut span_end = first.offset.checked_add(first_len as u64)?;

    // The anchor's own width counts against the bound, not just the records
    // merged after it. Its length comes from a handle, so a corrupt one can
    // declare a record wider than any read is allowed to be; checking only what
    // follows would leave that width unexamined, and a later handle pointing
    // inside the declared span would turn it into a span that gets read whole.
    // Returning it alone leaves it to the direct read, which validates it.
    if span_end - first.offset > reach {
        return Some((end, span_end));
    }

    while let Some(&(_, next, next_len)) = items.get(end) {
        let within_gap = span_end
            .checked_add(max_gap)
            .is_some_and(|reach| next.offset <= reach);
        if next.blob_file_id != file_id || !within_gap {
            break;
        }
        let Some(next_end) = next.offset.checked_add(next_len as u64) else {
            break;
        };
        // Sorted by offset, so this end is at or past the span's start.
        debug_assert!(next_end >= first.offset);
        if next_end - first.offset > reach {
            break;
        }
        span_end = span_end.max(next_end);
        end += 1;
    }

    Some((end, span_end))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
