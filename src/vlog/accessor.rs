// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::{
    BlobFile, Cache, GlobalTableId, TreeId, UserValue,
    cache::BlobFrameAt,
    config::BlobCacheFill,
    version::BlobFileList,
    vlog::{
        ValueHandle,
        blob_file::{
            frame::{DecodedFrame, FrameKind},
            reader::{Reader, assemble, check_key, chunk_frames, covering_chunks, value_in},
        },
    },
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::ops::Range;

/// Frames a read fetched ahead for itself, held by that read instead of the
/// shared cache: what a scan reads ahead when blob values are not cached.
///
/// Kept by place, file and frame number, as the cache keeps them; a value is
/// taken out of one only for the key its frame lists at the handle's index.
#[derive(Default)]
pub(crate) struct Prefetched {
    frames: crate::HashMap<BlobFrameAt, HeldFrame>,
}

/// A frame read ahead, with the charge that counts its payload against the
/// read-ahead bound from the first value taken out of it.
struct HeldFrame {
    frame: Arc<DecodedFrame>,
    charge: core::cell::OnceCell<Arc<FrameCharge>>,
}

impl Prefetched {
    /// The value read ahead for `vhandle` under `key`, if its frame was: a
    /// view into the frame, which keeps the frame's whole payload.
    #[cfg_attr(
        not(feature = "columnar"),
        expect(
            dead_code,
            reason = "a batched object read is its only uncharged reader"
        )
    )]
    pub(crate) fn get(&self, key: &[u8], vhandle: &ValueHandle) -> Option<UserValue> {
        self.frame_for(vhandle)
            .and_then(|held| value_in(&held.frame, vhandle.index, key).ok())
    }

    /// [`Self::get`], with the charge for the payload the value keeps: made
    /// by `charge` from the payload's bytes the first time a value is taken
    /// out of the frame, and shared by every later one.
    pub(crate) fn get_charged(
        &self,
        key: &[u8],
        vhandle: &ValueHandle,
        charge: impl FnOnce(usize) -> FrameCharge,
    ) -> Option<(UserValue, Arc<FrameCharge>)> {
        let held = self.frame_for(vhandle)?;
        let value = value_in(&held.frame, vhandle.index, key).ok()?;
        let charge = held
            .charge
            .get_or_init(|| Arc::new(charge(held.frame.payload.len())));
        Some((value, Arc::clone(charge)))
    }

    /// The frame `vhandle` names a value of, unless it is a chunked value's
    /// head, which holds none.
    fn frame_for(&self, vhandle: &ValueHandle) -> Option<&HeldFrame> {
        self.frames
            .get(&vhandle.frame_at())
            .filter(|held| held.frame.header.kind != FrameKind::Head)
    }

    /// Whether it holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Keeps only the frames the values `items` name lie in: a read that
    /// holds its store across batches keeps a block the next batch shares and
    /// lets the rest go, so the store stays one read's worth.
    pub(crate) fn retain_for(&mut self, items: &[(&[u8], ValueHandle, usize)]) {
        if self.frames.is_empty() {
            return;
        }
        self.frames.retain(|at, _| {
            items
                .iter()
                .any(|(_, vhandle, _)| vhandle.frame_at() == *at)
        });
    }

    /// Holds `frame` as read ahead at `at`.
    #[cfg(test)]
    pub(crate) fn hold(&mut self, at: BlobFrameAt, frame: Arc<DecodedFrame>) {
        self.frames.insert(at, HeldFrame::new(frame));
    }
}

impl HeldFrame {
    fn new(frame: Arc<DecodedFrame>) -> Self {
        Self {
            frame,
            charge: core::cell::OnceCell::new(),
        }
    }
}

/// Bytes of a frame read ahead counted against a scan's read-ahead bound,
/// held by the scan's store and by each guard holding a value out of the
/// frame, and released with the last of them: a value is a view into its
/// frame, so what a kept guard pins is the frame, not its value.
pub(crate) struct FrameCharge {
    held: Arc<core::sync::atomic::AtomicUsize>,
    bytes: usize,
}

impl FrameCharge {
    /// Adds `bytes` to `held` until the charge is dropped.
    pub(crate) fn new(held: &Arc<core::sync::atomic::AtomicUsize>, bytes: usize) -> Self {
        held.fetch_add(bytes, core::sync::atomic::Ordering::Relaxed);
        Self {
            held: Arc::clone(held),
            bytes,
        }
    }
}

impl Drop for FrameCharge {
    fn drop(&mut self) {
        self.held
            .fetch_sub(self.bytes, core::sync::atomic::Ordering::Relaxed);
    }
}

/// Where a read-ahead puts what it reads.
pub(crate) enum PrefetchInto<'p> {
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
pub(crate) struct Accessor<'a> {
    blob_files: &'a BlobFileList,
    /// Whether the frames this accessor reads from disk go into the cache.
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
    pub(crate) fn new(
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

    /// Records one read request issued to the filesystem for blob frames,
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

    /// Records what came out of one frame's check and decompression.
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

    /// Frame `frame` of `blob_file`, from the cache or read and decoded now:
    /// concurrent misses of one frame share one read.
    ///
    /// The blob file is reopened from the path it was RECOVERED under, so no
    /// caller supplies a base directory: a file can legitimately sit under a
    /// noncanonical spelling of its own id (`blobs/00` for id 0), and a path
    /// rebuilt from the id would miss it on every cache miss.
    ///
    /// # Errors
    ///
    /// Propagates the blob file's open / read failures and a frame that does
    /// not check.
    pub(crate) fn frame(
        &self,
        tree_id: TreeId,
        blob_file: &BlobFile,
        frame: u32,
        cache: &Cache,
    ) -> crate::Result<Arc<DecodedFrame>> {
        let at = BlobFrameAt {
            blob_file_id: blob_file.id(),
            frame,
        };
        if let Some(cached) = cache.get_blob_frame(tree_id, at) {
            return Ok(cached);
        }
        let lead = match cache.claim_blob_frame(tree_id, at) {
            crate::cache::BlobClaim::Shared(shared) => {
                #[cfg(feature = "metrics")]
                if let Some(metrics) = self.metrics {
                    metrics
                        .blob_load_shared
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                }
                // The read that led may not cache what it reads (its policy
                // is its own, taken when it began): one whose policy does
                // caches the shared frame itself.
                if self.fill.fills() && !cache.contains_blob_frame(tree_id, at) {
                    cache.insert_blob_frame(tree_id, at, Arc::clone(&shared));
                }
                return Ok(shared);
            }
            crate::cache::BlobClaim::Lead(lead) => {
                // A read that finished between the miss above and this claim
                // left its frame in the cache, when the fill policy let it.
                if let Some(cached) = cache.get_blob_frame(tree_id, at) {
                    lead.publish(cache, tree_id, at, &cached, false);
                    return Ok(cached);
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
        let (_, len) = Reader::frame_span(blob_file, frame)?;
        self.count_request(len);
        let bytes = reader.read_frame_bytes(frame)?;
        // Charged before the result is judged, like the read above: a frame
        // refused after decompression was still decompressed.
        let mut decoded = 0;
        let parsed = Reader::decode(blob_file, &bytes, &mut decoded);
        self.count_decoded(decoded);
        let frame = Arc::new(parsed?);
        cache.record_blob_load(tree_id, at, timer);
        match lead {
            Some(lead) => lead.publish(cache, tree_id, at, &frame, self.fill.fills()),
            None if self.fill.fills() => cache.insert_blob_frame(tree_id, at, Arc::clone(&frame)),
            None => {}
        }
        Ok(frame)
    }

    /// Reads one separated value whole.
    ///
    /// # Errors
    ///
    /// Propagates the blob file's open / read failures, a frame that does not
    /// check, and a handle that names another key's value.
    pub(crate) fn get(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        self.read(tree_id, key, vhandle, None, cache)
    }

    /// Reads bytes `range` of one separated value: of a chunked value, only
    /// the chunks the range covers are read.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ValueRange`] for a range outside the value, and as
    /// [`Self::get`].
    pub(crate) fn get_range(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        range: Range<u64>,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        self.read(tree_id, key, vhandle, Some(range), cache)
    }

    fn read(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        range: Option<Range<u64>>,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        let Some(blob_file) = self.blob_files.get(vhandle.blob_file_id) else {
            return Ok(None);
        };
        let first = self.frame(tree_id, blob_file, vhandle.frame, cache)?;
        if first.header.kind != FrameKind::Head {
            let value = value_in(&first, vhandle.index, key)?;
            return Ok(Some(match range {
                None => value,
                Some(range) => slice_range(&value, &range)?,
            }));
        }
        check_key(&first, vhandle.index, key)?;
        let total = first.list.first().map_or(0, |e| u64::from(e.value_len));
        let range = range.unwrap_or(0..total);
        check_range(&range, total)?;
        let all = chunk_frames(&first, vhandle.frame)?;
        let (covered, _) = covering_chunks(&first, &range)?;
        let mut chunks = Vec::with_capacity((covered.end - covered.start) as usize);
        for at in covered {
            let frame = all
                .start
                .checked_add(at)
                .ok_or(crate::Error::InvalidHeader("Blob"))?;
            chunks.push(self.frame(tree_id, blob_file, frame, cache)?);
        }
        let value = assemble(&first, &chunks, Some(&range))?;
        self.count_gather(value.len());
        Ok(Some(value))
    }

    /// Warms the cache, or the read's own store, with the frames a run of
    /// upcoming separated values lie in, coalescing frames close together
    /// into as few reads as possible.
    ///
    /// A scan resolves one value per [`get`](Self::get). Values land in the
    /// blob file in the order the flush wrote them, which is key order, so a
    /// scan's next handles are usually in the same block or the next frames:
    /// reading them at once turns a stream of small reads into a few large
    /// ones.
    ///
    /// Purely an I/O optimization, and best-effort in both directions: it never
    /// changes which bytes [`get`](Self::get) returns (the same check
    /// validates either path), and any failure here is dropped so the read
    /// walk handles that value authoritatively, including reporting its
    /// corruption.
    ///
    /// `items` is `(key, handle, _)` in scan order, and is CONSUMED as the
    /// working buffer: it is filtered and sorted in place.
    ///
    /// `max_gap` is how many wasted bytes between two frames are worth
    /// swallowing to merge their reads; `max_read` caps a single coalesced
    /// read.
    pub(crate) fn prefetch(
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

        // One entry per frame to read: frames already held need no read, and
        // the values of one block share their frame.
        items.retain(|(_, vhandle, _)| {
            let held = match &into {
                PrefetchInto::Cache => cache.contains_blob_frame(tree_id, vhandle.frame_at()),
                PrefetchInto::Local(store) => store.frames.contains_key(&vhandle.frame_at()),
            };
            !held && self.blob_files.get(vhandle.blob_file_id).is_some()
        });
        let key =
            |(_, vhandle, _): &(&[u8], ValueHandle, usize)| (vhandle.blob_file_id, vhandle.frame);
        if !items.is_sorted_by_key(key) {
            items.sort_unstable_by_key(key);
        }
        // Whether each frame serves more than one value: read once ahead, a
        // block several values share spares each of them a read of it.
        let mut shared: Vec<bool> = Vec::with_capacity(items.len());
        let mut prev = None;
        for item in items.iter() {
            let at = key(item);
            // A frame seen before was pushed when it was first seen.
            match shared.last_mut() {
                Some(last) if prev == Some(at) => *last = true,
                _ => shared.push(false),
            }
            prev = Some(at);
        }
        items.dedup_by_key(|item| key(item));
        // A single cold frame of one value is exactly what `get` already does
        // well; the prefetch earns its keep by merging two or more frames, or
        // by reading once a frame several values share.
        if items.len() < 2 && !shared.contains(&true) {
            return;
        }

        // Spans of frames close together in one file, bounded by the read
        // budget, the extent counted with its gaps.
        let mut read_budget = half;
        let mut spans: Vec<Span> = Vec::new();
        let mut at = 0;
        while at < items.len() && read_budget > 0 {
            let Some(span) = self.span_from(items, at, max_gap, max_read as u64, read_budget)
            else {
                at += 1;
                continue;
            };
            let serves_many = span.frames.end - span.frames.start >= 2
                || shared.get(span.frames.start).copied().unwrap_or(false);
            at = span.frames.end;
            if serves_many {
                read_budget -= span.end - span.start;
                spans.push(span);
            }
        }
        // The spans go out in batches of at most one coalesced read's worth of
        // extent, so the buffers held at once are what one read holds; a
        // window within that (the usual one) is one batch. A batch is read
        // only while the admission budget lasts.
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

    /// [`span_extent`] over the frames this accessor's blob files place.
    fn span_from(
        &self,
        items: &[(&[u8], ValueHandle, usize)],
        start: usize,
        max_gap: u64,
        max_read: u64,
        read_budget: u64,
    ) -> Option<Span> {
        span_extent(items, start, max_gap, max_read, read_budget, |vhandle| {
            self.blob_files
                .get(vhandle.blob_file_id)?
                .frame_span(vhandle.frame)
        })
    }

    /// Reads every one of `spans` of `items` in one batched call per
    /// filesystem (a window's blob files are normally all one backend's),
    /// then decodes the frames each covers into the cache, or into the read's
    /// own store, in span order. A span whose file will not open or whose
    /// read fails stays cold, and the read walk fetches its values itself.
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
        /// the buffer its extent is read into, `None` when the read fails.
        struct SpanRead<'b> {
            span: usize,
            blob_file: &'b BlobFile,
            file: Arc<dyn crate::fs::FsFile>,
            buf: Option<crate::fs::QueuedBuf>,
        }

        let mut reads: Vec<SpanRead<'_>> = Vec::with_capacity(spans.len());
        for (at, span) in spans.iter().enumerate() {
            let Some((_, first, _)) = items.get(span.frames.start) else {
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
            let Ok(len) = usize::try_from(span.end - span.start) else {
                continue;
            };
            // The whole extent, gaps included: that is what is asked of the
            // filesystem. Charged before the read is issued, so a read the
            // filesystem then fails is counted.
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
                buf: Some(crate::fs::QueuedBuf::new(len)),
            });
        }

        // One batched call per filesystem, taking its spans in window order.
        let mut issued = alloc::vec![false; reads.len()];
        while let Some(lead) = issued.iter().position(|issued| !issued) {
            let Some(fs) = reads.get(lead).map(|read| Arc::clone(&read.blob_file.0.fs)) else {
                break;
            };
            let on_fs = |read: &SpanRead<'_>| {
                core::ptr::addr_eq(Arc::as_ptr(&read.blob_file.0.fs), Arc::as_ptr(&fs))
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
            let progress: Vec<_> = {
                let mut reqs: Vec<crate::fs::BlockRead<'_>> = reads
                    .iter_mut()
                    .enumerate()
                    .filter(|(at, _)| group.binary_search(at).is_ok())
                    .filter_map(|(_, read)| {
                        let SpanRead {
                            span,
                            ref file,
                            ref mut buf,
                            ..
                        } = *read;
                        Some(crate::fs::BlockRead {
                            file: file.as_ref(),
                            offset: spans.get(span).map_or(0, |span| span.start),
                            buf: buf.as_mut()?.block_buf(),
                        })
                    })
                    .collect();
                // Best effort: a failed call still hands over the spans it
                // filled, and the others stay cold for the read walk.
                let _ = fs.read_blocks_batched_each(&mut reqs, &mut |index, _| {
                    if let Some(slot) = got.get_mut(index) {
                        *slot = true;
                    }
                });
                reqs.iter().map(|req| req.buf.progress()).collect()
            };
            // Every span of the group had a buffer, so the requests and the
            // group line up.
            for ((&at, got), progress) in group.iter().zip(got).zip(&progress) {
                let Some(read) = reads.get_mut(at) else {
                    continue;
                };
                if let Some(buf) = read.buf.as_mut() {
                    buf.settle(progress);
                }
                if !got || !read.buf.as_ref().is_some_and(crate::fs::QueuedBuf::is_full) {
                    read.buf = None;
                }
            }
        }

        for read in reads {
            if *admit_budget == 0 {
                return;
            }
            let (Some(span), Some(bytes)) = (
                spans.get(read.span),
                read.buf.and_then(crate::fs::QueuedBuf::into_slice),
            ) else {
                continue;
            };
            self.admit_span(
                tree_id,
                items,
                span,
                &bytes,
                read.blob_file,
                cache,
                into,
                admit_budget,
            );
        }
    }

    /// Decodes every frame of `span`, read into `bytes`, into the cache or the
    /// read's own store, spending `admit_budget` by what each frame weighs and
    /// stopping when it runs out.
    #[expect(
        clippy::too_many_arguments,
        reason = "one span: its frames and bytes, its file, and where its frames go under which budget"
    )]
    fn admit_span(
        &self,
        tree_id: TreeId,
        items: &[(&[u8], ValueHandle, usize)],
        span: &Span,
        bytes: &crate::Slice,
        blob_file: &BlobFile,
        cache: &Cache,
        into: &mut PrefetchInto<'_>,
        admit_budget: &mut u64,
    ) {
        for (_, vhandle, _) in items.get(span.frames.clone()).unwrap_or_default() {
            if *admit_budget == 0 {
                return;
            }
            let Some((start, end)) = blob_file.frame_span(vhandle.frame) else {
                continue;
            };
            let (Ok(from), Ok(to)) = (
                usize::try_from(start - span.start),
                usize::try_from(end - span.start),
            ) else {
                continue;
            };
            if to > bytes.len() {
                continue;
            }
            let mut decoded = 0;
            let parsed = Reader::decode(blob_file, &bytes.slice(from..to), &mut decoded);
            self.count_decoded(decoded);
            let Ok(frame) = parsed else {
                continue;
            };
            // Checked BEFORE inserting, against the weight the cache charges:
            // the decoded frame, not what was read.
            let weight = frame.weight();
            if matches!(into, PrefetchInto::Cache) && weight > cache.max_entry_weight() {
                continue;
            }
            if weight > *admit_budget {
                *admit_budget = 0;
                return;
            }
            *admit_budget -= weight;
            // A frame decoded as stored is a view into the coalesced read;
            // copied out so it does not keep the whole read alive, unless the
            // read is that frame alone.
            let frame = if from == 0 && to == bytes.len() {
                frame
            } else {
                let (frame, copied) = own_frame(frame, blob_file.compression());
                self.count_gather(copied);
                frame
            };
            let frame = Arc::new(frame);
            let at = vhandle.frame_at();
            match into {
                PrefetchInto::Cache => cache.insert_blob_frame(tree_id, at, frame),
                PrefetchInto::Local(store) => {
                    store.frames.insert(at, HeldFrame::new(frame));
                }
            }
        }
    }
}

/// The span of frames that starts with `items[start]`'s: the following frames
/// of the same file within `max_gap` of it, the whole extent within
/// `max_read` and `read_budget`. `span` gives where a handle's frame lies, as
/// its file's index places it. `None` when the first frame is not there, or
/// is wider than that bound: such a frame is left to the direct read, so a
/// span the caller charges to its budget never exceeds what is left of it,
/// however many values share the frame.
fn span_extent(
    items: &[(&[u8], ValueHandle, usize)],
    start: usize,
    max_gap: u64,
    max_read: u64,
    read_budget: u64,
    span: impl Fn(&ValueHandle) -> Option<(u64, u64)>,
) -> Option<Span> {
    let reach = max_read.min(read_budget);
    let (_, first, _) = items.get(start)?;
    let (span_start, mut span_end) = span(first)?;
    // The first frame alone past the bound is left to the direct read.
    if span_end - span_start > reach {
        return None;
    }
    let mut end = start + 1;
    while let Some((_, next, _)) = items.get(end) {
        if next.blob_file_id != first.blob_file_id {
            break;
        }
        let Some((next_start, next_end)) = span(next) else {
            break;
        };
        // An end past `u64::MAX` is a frame index no writer produced: the
        // span stops rather than merge across a gap of any size.
        let within_gap = span_end
            .checked_add(max_gap)
            .is_some_and(|reach| next_start <= reach);
        let within_reach = next_end
            .checked_sub(span_start)
            .is_some_and(|extent| extent <= reach);
        if !within_gap || !within_reach {
            break;
        }
        span_end = next_end;
        end += 1;
    }
    Some(Span {
        frames: start..end,
        start: span_start,
        end: span_end,
    })
}

/// `frame`, decoded from a file under `compression`, holding its own bytes,
/// and the bytes copied for that: a frame stored as it is and decoded out of
/// a larger read is a view into it; a compressed one already holds its own.
pub(crate) fn own_frame(
    frame: DecodedFrame,
    compression: crate::CompressionType,
) -> (DecodedFrame, usize) {
    let stored_as_is = frame.header.is_raw() || compression == crate::CompressionType::None;
    if !stored_as_is {
        // Decoding a compressed frame already gave it entries and a payload
        // of its own.
        return (frame, 0);
    }
    let copied = frame.entries.len() + frame.payload.len();
    let frame = DecodedFrame {
        entries: crate::Slice::new(&frame.entries),
        payload: crate::Slice::new(&frame.payload),
        ..frame
    };
    (frame, copied)
}

/// Checks that `range` lies within a value of `len` bytes.
///
/// # Errors
///
/// [`crate::Error::ValueRange`] when it does not.
pub(crate) fn check_range(range: &Range<u64>, len: u64) -> crate::Result<()> {
    if range.start > range.end || range.end > len {
        return Err(crate::Error::ValueRange {
            start: range.start,
            end: range.end,
            len,
        });
    }
    Ok(())
}

/// Bytes `range` of `value`.
///
/// # Errors
///
/// [`crate::Error::ValueRange`] for a range outside the value.
pub(crate) fn slice_range(value: &UserValue, range: &Range<u64>) -> crate::Result<UserValue> {
    check_range(range, value.len() as u64)?;
    // Within the value's length, which is a `usize`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "bounded by the value's length just above"
    )]
    Ok(value.slice(range.start as usize..range.end as usize))
}

/// One coalesced read of a window: the frames it covers, by their places in
/// the window, and the extent of the blob file it reads, gaps included.
struct Span {
    frames: Range<usize>,
    start: u64,
    end: u64,
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
