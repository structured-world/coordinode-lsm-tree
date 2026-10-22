// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{
    Cache, GlobalTableId, TreeId, UserValue,
    config::BlobCacheFill,
    version::BlobFileList,
    vlog::{ValueHandle, blob_file::reader::Reader},
};
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

/// Values a read fetched ahead for itself, held by that read instead of the
/// shared cache: what a scan reads ahead when blob values are not cached.
///
/// Kept by place in the blob files, with the key and the declared on-disk size
/// each was read for, so a handle taking one out is proven to be the handle it
/// was read for, as a cache lookup proves it.
#[derive(Default)]
pub struct Prefetched {
    values: crate::HashMap<(crate::vlog::BlobFileId, u64), (crate::UserKey, u32, UserValue)>,
}

impl Prefetched {
    /// Takes out the value read ahead for `vhandle` under `key`, if any.
    pub(crate) fn take(&mut self, key: &[u8], vhandle: &ValueHandle) -> Option<UserValue> {
        let place = (vhandle.blob_file_id, vhandle.offset);
        match self.values.get(&place) {
            Some((read_key, size, _)) if &**read_key == key && *size == vhandle.on_disk_size => {
                self.values.remove(&place).map(|(_, _, value)| value)
            }
            _ => None,
        }
    }

    /// Whether it holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Drops what it holds.
    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }

    /// Holds `value` as read ahead for `vhandle` under `key`.
    #[cfg(test)]
    pub(crate) fn hold(&mut self, key: &[u8], vhandle: &ValueHandle, value: UserValue) {
        self.values.insert(
            (vhandle.blob_file_id, vhandle.offset),
            (crate::UserKey::from(key), vhandle.on_disk_size, value),
        );
    }
}

/// Where a read-ahead puts what it reads.
pub enum PrefetchInto<'p> {
    /// The shared cache, for every read to find.
    Cache,
    /// The read's own store, bounded by the read-ahead's own budget.
    Local(&'p mut Prefetched),
}

/// Reads separated values out of the blob files a version names.
///
/// Takes no dictionary: a `ZstdDict` blob file carries its own, pinned on the
/// handle this walks, so decoding needs nothing the version does not already
/// hold.
pub struct Accessor<'a> {
    blob_files: &'a BlobFileList,
    /// Whether the values this accessor reads from disk go into the cache.
    fill: BlobCacheFill,
    /// Where this accessor's reads are counted. A blob read is a read of the
    /// filesystem like any other, so it lands in the same `bytes_read` /
    /// `bytes_decoded` pair the block path reports through. `None` for a
    /// maintenance read (a compaction filter resolving a value), which the
    /// read counters leave out as they leave out compaction's own input.
    #[cfg(feature = "metrics")]
    metrics: Option<&'a Metrics>,
}

impl<'a> Accessor<'a> {
    pub fn new(
        blob_files: &'a BlobFileList,
        fill: BlobCacheFill,
        #[cfg(feature = "metrics")] metrics: Option<&'a Metrics>,
    ) -> Self {
        Self {
            blob_files,
            fill,
            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    /// Records one read request issued to the filesystem for blob records,
    /// covering `on_disk` bytes.
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            unused_variables,
            clippy::unused_self,
            reason = "the argument and the accessor's counters are both the feature's payload"
        )
    )]
    #[inline]
    pub(crate) fn count_request(&self, on_disk: usize) {
        #[cfg(feature = "metrics")]
        if let Some(metrics) = self.metrics {
            use core::sync::atomic::Ordering::Relaxed;
            metrics.blob_read_io.fetch_add(1, Relaxed);
            metrics
                .blob_bytes_io_requested
                .fetch_add(on_disk as u64, Relaxed);
        }
    }

    /// Records what came out of one record's validation and decompression.
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            unused_variables,
            clippy::unused_self,
            reason = "the argument and the accessor's counters are both the feature's payload"
        )
    )]
    #[inline]
    pub(crate) fn count_decoded(&self, decoded: usize) {
        #[cfg(feature = "metrics")]
        if let Some(metrics) = self.metrics {
            use core::sync::atomic::Ordering::Relaxed;
            metrics
                .blob_bytes_decoded
                .fetch_add(decoded as u64, Relaxed);
        }
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

        let lead = match cache.claim_blob(tree_id, vhandle, key) {
            crate::cache::BlobClaim::Shared(value) => {
                #[cfg(feature = "metrics")]
                if let Some(metrics) = self.metrics {
                    metrics
                        .blob_load_shared
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                // The read that led may not cache what it reads (its policy
                // is its own, taken when it began): one whose policy does
                // caches the shared value itself. The position alone is
                // checked: every value cached there passed the record check
                // of its key and size against the record's header, so an
                // entry at this position is for this same key and size.
                if self.fill.fills() && !cache.contains_blob(tree_id, vhandle) {
                    let detached = cache.insert_blob(tree_id, vhandle, key, value.clone());
                    self.count_gather(detached);
                }
                return Ok(Some(value));
            }
            crate::cache::BlobClaim::Lead(lead) => {
                // A read that finished between the miss above and this claim
                // left its value in the cache, when the fill policy let it.
                if let Some(value) = cache.get_blob(tree_id, vhandle, key) {
                    lead.publish(cache, tree_id, vhandle, key, value.clone(), false);
                    return Ok(Some(value));
                }
                Some(lead)
            }
            crate::cache::BlobClaim::Alone => None,
        };

        let timer = crate::cache::Cache::time_load();
        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)?;

        let reader = Reader::new(blob_file, file.as_ref());

        // Charged before the read is issued, as a block read is: a request the
        // filesystem then fails, or whose bytes then fail their checksum or
        // decompression, was still asked of it.
        let len = crate::vlog::blob_file::reader::record_len(key.len(), vhandle)?;
        self.count_request(len);
        let record = reader.read_record(vhandle, len)?;
        // Charged before the result is judged, like the read above: a record
        // refused after decompression was still decompressed.
        let mut decoded = 0;
        let parsed = reader.parse_record(key, vhandle, &record, &mut decoded);
        self.count_decoded(decoded);
        let value = parsed?;
        cache.record_blob_load(tree_id, vhandle, timer);
        match lead {
            Some(lead) => {
                let detached = lead.publish(
                    cache,
                    tree_id,
                    vhandle,
                    key,
                    value.clone(),
                    self.fill.fills(),
                );
                self.count_gather(detached);
            }
            None if self.fill.fills() => {
                let detached = cache.insert_blob(tree_id, vhandle, key, value.clone());
                self.count_gather(detached);
            }
            None => {}
        }

        Ok(Some(value))
    }

    /// Records a gather this accessor performed on a read's behalf.
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            unused_variables,
            clippy::unused_self,
            reason = "the argument and the accessor's counters are both the feature's payload"
        )
    )]
    #[inline]
    pub(crate) fn count_gather(&self, bytes: usize) {
        #[cfg(feature = "metrics")]
        if let Some(metrics) = self.metrics {
            metrics.record_gather(bytes);
        }
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
    ///
    /// `into` says where the values go: the shared cache, or the read's own
    /// store when blob values are not cached, which then holds at most one
    /// coalesced read's worth.
    pub fn prefetch(
        &self,
        tree_id: TreeId,
        items: &mut Vec<(&[u8], ValueHandle, usize)>,
        cache: &Cache,
        max_gap: u64,
        max_read: usize,
        mut into: PrefetchInto<'_>,
    ) {
        // Into the cache, warm at most half of it, so a prefetch cannot evict
        // more than it contributes; mirrors the block prewarm's bound. Into
        // the read's own store, what one coalesced read may hold.
        let half = match into {
            PrefetchInto::Cache => cache.capacity() / 2,
            PrefetchInto::Local(_) => max_read as u64,
        };
        if half == 0 {
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

        // Every span of the window is found first and then read in one batch,
        // so they overlap in flight instead of going out one after another.
        let mut spans: Vec<Span> = Vec::new();
        let mut start = 0;
        while start < items.len() && read_budget > 0 {
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

            if end - start >= 2 {
                // `span_extent` bounds the extent by `reach`, so it fits.
                debug_assert!(span_end - first.offset <= read_budget);
                read_budget -= span_end - first.offset;
                spans.push(Span {
                    records: start..end,
                    start: first.offset,
                    end: span_end,
                });
            }
            start = end;
        }
        // The spans go out in batches of at most one coalesced read's worth of
        // extent, so the buffers held at once are what one read held before
        // the spans were batched; a window within that (the usual one) is one
        // batch. A batch is read only while the admission budget lasts.
        let mut admit_budget = half;
        let mut start = 0;
        while start < spans.len() && admit_budget > 0 {
            let mut end = start;
            let mut extent = 0u64;
            while let Some(span) = spans.get(end) {
                let width = span.end - span.start;
                if end > start && extent + width > max_read as u64 {
                    break;
                }
                extent += width;
                end += 1;
            }
            if let Some(batch) = spans.get(start..end) {
                self.warm_spans(tree_id, items, batch, cache, &mut into, &mut admit_budget);
            }
            start = end;
        }
    }

    /// Reads every one of `spans` of `items` in one batched call per
    /// filesystem (a window's blob files are normally all one backend's), then
    /// parses the records each covers into the cache, or into the read's own
    /// store, in span order. A span whose file will not open or whose read
    /// fails stays cold, and the read walk fetches its values normally.
    ///
    /// `admit_budget` is how many bytes this prefetch may still hand over,
    /// spent by what each value actually weighs; the walk stops when it runs
    /// out.
    fn warm_spans(
        &self,
        tree_id: TreeId,
        items: &[(&[u8], ValueHandle, usize)],
        spans: &[Span],
        cache: &Cache,
        into: &mut PrefetchInto<'_>,
        admit_budget: &mut u64,
    ) {
        /// One span's read: its span, its blob file and the open file, and
        /// the buffer its extent is read into, emptied when the read fails.
        struct SpanRead<'b> {
            span: usize,
            blob_file: &'b crate::vlog::BlobFile,
            file: alloc::sync::Arc<dyn crate::fs::FsFile>,
            buf: Vec<u8>,
        }

        let mut reads: Vec<SpanRead<'_>> = Vec::with_capacity(spans.len());
        for (at, span) in spans.iter().enumerate() {
            let Some((_, first, _)) = items.get(span.records.start) else {
                continue;
            };
            let Some(blob_file) = self.blob_files.get(first.blob_file_id) else {
                continue;
            };
            let bf_id = GlobalTableId::from((tree_id, blob_file.id()));
            let Ok((file, _)) = blob_file
                .file_accessor()
                .get_or_open_blob_file(&bf_id, &blob_file.0.path)
            else {
                continue;
            };
            // `end` was built as the maximum record end in the span, and every
            // one of those is at or past `start` (the span's own first offset,
            // in a slice sorted by offset), so the extent is never negative.
            debug_assert!(span.end >= span.start);
            let Ok(len) = usize::try_from(span.end - span.start) else {
                continue;
            };
            // The whole extent, gaps included: that is what is asked of the
            // filesystem, and swallowing a gap to merge two reads is the point
            // of the coalescing, so charging only the records would hide its
            // cost. Charged before the read is issued, so a read the
            // filesystem then fails is counted, and whether or not every
            // record it covers is then parsed.
            self.count_request(len);
            #[cfg(feature = "metrics")]
            if let Some(metrics) = self.metrics {
                metrics
                    .blob_bytes_prefetched
                    .fetch_add(len as u64, core::sync::atomic::Ordering::Relaxed);
            }
            reads.push(SpanRead {
                span: at,
                blob_file,
                file,
                buf: alloc::vec![0u8; len],
            });
        }

        // One batched call per filesystem, taking its spans in window order.
        let mut issued = alloc::vec![false; reads.len()];
        while let Some(lead) = issued.iter().position(|issued| !issued) {
            let Some(fs) = reads
                .get(lead)
                .map(|read| alloc::sync::Arc::clone(&read.blob_file.0.fs))
            else {
                break;
            };
            let on_fs = |read: &SpanRead<'_>| {
                core::ptr::addr_eq(
                    alloc::sync::Arc::as_ptr(&read.blob_file.0.fs),
                    alloc::sync::Arc::as_ptr(&fs),
                )
            };
            // In ascending order, so membership is a binary search.
            let group: Vec<usize> = reads
                .iter()
                .enumerate()
                .skip(lead)
                .filter(|(at, read)| issued.get(*at) == Some(&false) && on_fs(read))
                .map(|(at, _)| at)
                .collect();
            for &at in &group {
                if let Some(slot) = issued.get_mut(at) {
                    *slot = true;
                }
            }
            let mut got = alloc::vec![false; group.len()];
            {
                let mut reqs: Vec<crate::fs::BlockRead<'_>> = reads
                    .iter_mut()
                    .enumerate()
                    .filter(|(at, _)| group.binary_search(at).is_ok())
                    .map(|(_, read)| crate::fs::BlockRead {
                        file: read.file.as_ref(),
                        offset: spans.get(read.span).map_or(0, |span| span.start),
                        buf: crate::fs::BlockBuf::new(&mut read.buf),
                    })
                    .collect();
                // Best effort: a failed call still hands over the spans it
                // filled, and the others stay cold for the read walk.
                let _ = fs.read_blocks_batched_each(&mut reqs, &mut |index, _| {
                    if let Some(slot) = got.get_mut(index) {
                        *slot = true;
                    }
                });
            }
            for (&at, got) in group.iter().zip(got) {
                if !got && let Some(read) = reads.get_mut(at) {
                    read.buf.clear();
                }
            }
        }

        for read in reads {
            if *admit_budget == 0 {
                return;
            }
            let (Some(span), false) = (spans.get(read.span), read.buf.is_empty()) else {
                continue;
            };
            let Some(records) = items.get(span.records.clone()) else {
                continue;
            };
            self.admit_span(
                tree_id,
                records,
                span.start,
                &crate::Slice::from(read.buf),
                read.blob_file,
                read.file.as_ref(),
                cache,
                into,
                admit_budget,
            );
        }
    }

    /// Parses every record of `records`, a span read from `span_start` into
    /// `span`, into the cache or the read's own store, spending
    /// `admit_budget` by each value's weight and stopping when it runs out.
    #[expect(
        clippy::too_many_arguments,
        reason = "one span: its records and bytes, its file, and where its values go under which budget"
    )]
    fn admit_span(
        &self,
        tree_id: TreeId,
        records: &[(&[u8], ValueHandle, usize)],
        span_start: u64,
        span: &crate::Slice,
        blob_file: &crate::vlog::BlobFile,
        file: &dyn crate::fs::FsFile,
        cache: &Cache,
        into: &mut PrefetchInto<'_>,
        admit_budget: &mut u64,
    ) {
        let reader = Reader::new(blob_file, file);

        // An uncompressed value is returned as a VIEW into the buffer it was
        // parsed from, here the whole coalesced read. Handed on as it is, every
        // value would pin that read for as long as anyone holds it: the cache
        // until the last of the window is evicted, a scan's caller for as long
        // as it keeps the value. So such a value is copied out, once it is
        // known to be handed on, and then owns exactly its own bytes, as on the
        // one-record read path. The compressed paths decompress into a fresh
        // buffer already, so there the span is dropped with this function and
        // copying would be pure waste.
        let aliases_span = matches!(blob_file.0.meta.compression, crate::CompressionType::None);

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
            if record_end > span.len() {
                continue;
            }
            let record = span.slice(rel..record_end);
            // Decoded only: the span's bytes were charged once above. Charged
            // whether or not the record is then accepted.
            let mut decoded = 0;
            let parsed = reader.parse_record(key, &vhandle, &record, &mut decoded);
            self.count_decoded(decoded);
            if let Ok(value) = parsed {
                // Checked BEFORE inserting, against the full weight the cache
                // charges (key as well as value), and against the DECODED
                // length rather than the on-disk one: a compressed blob file
                // stores less than the cache accounts for, so budgeting on
                // what was read would admit several times the capacity from
                // one window and evict everything else to hold values the scan
                // has not reached yet. Deducting after the fact would let the
                // last value of a window exceed the bound by its own size.
                let weight = (key.len() + value.len()) as u64;
                // A value heavier than one cache shard can never stay
                // resident (the cache refuses it), so inserting it would
                // spend admission budget on nothing. Skip it and keep
                // warming the span's other records.
                if matches!(into, PrefetchInto::Cache) && weight > cache.max_entry_weight() {
                    continue;
                }
                if weight > *admit_budget {
                    *admit_budget = 0;
                    return;
                }
                *admit_budget -= weight;
                let value = if aliases_span {
                    // A copy out of the span: charged as the gather it is.
                    self.count_gather(value.len());
                    UserValue::new(&value)
                } else {
                    value
                };
                match into {
                    PrefetchInto::Cache => {
                        let detached = cache.insert_blob(tree_id, &vhandle, key, value);
                        self.count_gather(detached);
                    }
                    PrefetchInto::Local(store) => {
                        // The key goes with the value as the cache keeps it:
                        // a copy, charged as the gather it is.
                        self.count_gather(key.len());
                        store.values.insert(
                            (vhandle.blob_file_id, vhandle.offset),
                            (crate::UserKey::from(key), vhandle.on_disk_size, value),
                        );
                    }
                }
            }
        }
    }
}

/// One coalesced read of a window: the records it covers, by their places in
/// the window, and the extent of the blob file it reads, gaps included.
struct Span {
    records: core::ops::Range<usize>,
    start: u64,
    end: u64,
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
