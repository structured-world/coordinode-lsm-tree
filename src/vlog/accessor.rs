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

/// A frame a read holds: shared with the cache and the reads waiting on it,
/// or the read's own when neither takes it.
pub(crate) enum HeldFrame {
    Shared(Arc<DecodedFrame>),
    Own(DecodedFrame),
}

impl core::ops::Deref for HeldFrame {
    type Target = DecodedFrame;

    fn deref(&self) -> &DecodedFrame {
        match self {
            Self::Shared(frame) => frame,
            Self::Own(frame) => frame,
        }
    }
}

/// Frames a read fetched ahead for itself, held by that read instead of the
/// shared cache: what a scan reads ahead when blob values are not cached.
///
/// Kept by place, file and frame number, as the cache keeps them; a value is
/// taken out of one only for the key its frame lists at the handle's index.
#[derive(Default)]
pub(crate) struct Prefetched {
    frames: crate::HashMap<BlobFrameAt, DecodedFrame>,
}

impl Prefetched {
    /// The value read ahead for `vhandle` under `key`, if its frame was: a
    /// view into the frame, which stays for the other values it holds.
    pub(crate) fn get(&self, key: &[u8], vhandle: &ValueHandle) -> Option<UserValue> {
        let frame = self.frames.get(&vhandle.frame_at())?;
        if frame.header.kind == FrameKind::Head {
            return None;
        }
        value_in(frame, vhandle.index, key).ok()
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
        // Moved over by lookup, one per item: matching each held frame against
        // every item cost a scan's refill the product of the two.
        let mut held = core::mem::take(&mut self.frames);
        for (_, vhandle, _) in items {
            let at = vhandle.frame_at();
            if let Some(frame) = held.remove(&at) {
                self.frames.insert(at, frame);
            }
            if held.is_empty() {
                break;
            }
        }
    }

    /// Holds `frame` as read ahead at `at`.
    #[cfg(test)]
    pub(crate) fn hold(&mut self, at: BlobFrameAt, frame: DecodedFrame) {
        self.frames.insert(at, frame);
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

/// Whether a frame a read decoded goes into the cache under `fill`. A block
/// of small values always does, as a data block does: the values beside the
/// one read are read next as often as not, and a block decoded once per value
/// costs each of them the whole block. A frame of one value, or a chunk,
/// follows the fill.
pub(crate) fn caches(fill: BlobCacheFill, frame: &DecodedFrame) -> bool {
    fill.fills() || frame.header.kind == crate::vlog::blob_file::frame::FrameKind::Block
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
    ) -> crate::Result<HeldFrame> {
        self.load(tree_id, blob_file, frame, cache, |bytes, decoded| {
            Reader::decode(blob_file, bytes, decoded)
        })
    }

    /// Chunk `at` of the value `head`, frame `head_frame` of `blob_file`, as
    /// [`Self::frame`] reads a frame: decoded by the head's table, the chunk
    /// carrying no header of its own.
    ///
    /// # Errors
    ///
    /// As [`Self::frame`], and a chunk the head does not list.
    fn chunk(
        &self,
        tree_id: TreeId,
        blob_file: &BlobFile,
        head: &DecodedFrame,
        head_frame: u32,
        at: u32,
        cache: &Cache,
    ) -> crate::Result<HeldFrame> {
        let frame = head_frame
            .checked_add(1)
            .and_then(|first| first.checked_add(at))
            .ok_or(crate::Error::InvalidHeader("Blob"))?;
        self.load(tree_id, blob_file, frame, cache, |bytes, decoded| {
            Reader::decode_chunk(blob_file, head, at, bytes, decoded)
        })
    }

    /// Frame `frame` of `blob_file`, from the cache or read and decoded now
    /// by `decode`.
    fn load(
        &self,
        tree_id: TreeId,
        blob_file: &BlobFile,
        frame: u32,
        cache: &Cache,
        decode: impl FnOnce(&crate::Slice, &mut usize) -> crate::Result<DecodedFrame>,
    ) -> crate::Result<HeldFrame> {
        let at = BlobFrameAt {
            blob_file_id: blob_file.id(),
            frame,
        };
        if let Some(cached) = cache.get_blob_frame(tree_id, at) {
            return Ok(HeldFrame::Shared(cached));
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
                if caches(self.fill, &shared) && !cache.contains_blob_frame(tree_id, at) {
                    cache.insert_blob_frame(tree_id, at, Arc::clone(&shared));
                }
                return Ok(HeldFrame::Shared(shared));
            }
            crate::cache::BlobClaim::Lead(lead) => {
                // A read that finished between the miss above and this claim
                // left its frame in the cache, when the fill policy let it.
                if let Some(cached) = cache.get_blob_frame(tree_id, at) {
                    lead.publish(cache, tree_id, at, &cached, false);
                    return Ok(HeldFrame::Shared(cached));
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
        let parsed = decode(&bytes, &mut decoded);
        self.count_decoded(decoded);
        let frame = parsed?;
        cache.record_blob_load(tree_id, at, timer);
        let fills = caches(self.fill, &frame);
        Ok(match lead {
            Some(lead) => lead.publish_owned(cache, tree_id, at, frame, fills),
            None if fills => {
                let frame = Arc::new(frame);
                cache.insert_blob_frame(tree_id, at, Arc::clone(&frame));
                HeldFrame::Shared(frame)
            }
            None => HeldFrame::Own(frame),
        })
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

    /// Reads one separated value whole, `stored` what its table records of
    /// it: a value with a record of its own in a packed block is read as that
    /// record alone, any other as [`Self::get`] reads it.
    ///
    /// # Errors
    ///
    /// As [`Self::get`], and a record that does not check or is not `key`'s.
    pub(crate) fn get_stored(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        stored: crate::blob_tree::handle::Stored,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        let crate::blob_tree::handle::Stored::Packed(len) = stored else {
            return self.get(tree_id, key, vhandle, cache);
        };
        let Some(blob_file) = self.blob_files.get(vhandle.blob_file_id) else {
            return Ok(None);
        };
        // A packed block read whole before, for a scan or a fill, answers.
        if let Some(frame) = cache.get_blob_frame(tree_id, vhandle.frame_at()) {
            return value_in(&frame, vhandle.index, key).map(Some);
        }
        self.read_record(tree_id, blob_file, key, vhandle, len)
            .map(Some)
    }

    /// The record of `len` bytes `vhandle` names in its packed block of
    /// `blob_file`, read alone, checked and decoded.
    fn read_record(
        &self,
        tree_id: TreeId,
        blob_file: &BlobFile,
        key: &[u8],
        vhandle: &ValueHandle,
        len: u32,
    ) -> crate::Result<UserValue> {
        use crate::vlog::blob_file::{frame::FRAME_HEADER_LEN, packed};

        let malformed = || crate::Error::InvalidHeader("BlobRecord");
        let (start, end) = blob_file.frame_span(vhandle.frame).ok_or_else(malformed)?;
        let at = start + FRAME_HEADER_LEN as u64 + u64::from(vhandle.index);
        if at + u64::from(len) > end {
            return Err(malformed());
        }
        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));
        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)?;
        self.count_request(len as usize);
        let bytes = crate::file::read_exact(file.as_ref(), at, len as usize)?;
        let record = packed::parse_record(&bytes)?;
        if record.len != bytes.len() || bytes.get(record.key.clone()) != Some(key) {
            return Err(malformed());
        }
        let mut decoded = 0;
        let value = packed::record_value(
            &record,
            &bytes,
            blob_file.0.meta.compression,
            #[cfg(zstd_any)]
            blob_file.0.zstd_dictionary.as_deref(),
            &mut decoded,
        );
        self.count_decoded(decoded);
        value
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
        // A whole value its read does not cache is built in place, its chunks
        // read into it, and none decoded on its own.
        if range.is_none() && !self.fill.fills() {
            return self
                .read_chunks_whole(tree_id, blob_file, &first, vhandle.frame)
                .map(Some);
        }
        let range = range.unwrap_or(0..total);
        check_range(&range, total)?;
        let (covered, _) = covering_chunks(&first, &range)?;
        let mut chunks = Vec::with_capacity((covered.end - covered.start) as usize);
        for at in covered {
            chunks.push(self.chunk(tree_id, blob_file, &first, vhandle.frame, at, cache)?);
        }
        let value = assemble(&first, &chunks, Some(&range))?;
        self.count_gather(value.len());
        Ok(Some(value))
    }

    /// The chunked value `head`, frame `head_frame` of `blob_file`, read whole
    /// into one allocation: a run of chunks stored as they are is read in one
    /// request straight into the place it takes in the value, and a
    /// compressed chunk is read and decompressed into its place. No chunk is
    /// held decoded on its own, so the value costs what a value of a frame of
    /// its own does: one copy of its bytes out of the file.
    ///
    /// # Errors
    ///
    /// The blob file's open or read failures, and a chunk that does not check
    /// or does not fit the head's table.
    fn read_chunks_whole(
        &self,
        tree_id: TreeId,
        blob_file: &BlobFile,
        head: &DecodedFrame,
        head_frame: u32,
    ) -> crate::Result<UserValue> {
        use crate::vlog::blob_file::frame::{check_chunk, decode_chunk_into};

        let malformed = || crate::Error::InvalidHeader("Blob");
        let (_, count) = head.chunk_table()?;
        let total = head.list.first().map_or(0, |e| u64::from(e.value_len));
        let len = usize::try_from(total).map_err(|_| malformed())?;
        let chunks = chunk_frames(head, head_frame)?;
        if chunks.end - chunks.start != count {
            return Err(malformed());
        }
        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));
        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)?;
        let compression = blob_file.0.meta.compression;

        // Each chunk where the frame index places it, of the length its row
        // in the head's table gives.
        let mut placed = Vec::with_capacity(count as usize);
        for (at, frame) in (0_u32..).zip(chunks) {
            let spec = head.chunk(at)?;
            let (start, end) = blob_file.frame_span(frame).ok_or_else(malformed)?;
            let as_is_fits = !spec.is_stored_as_is(compression) || spec.stored_len == spec.raw_len;
            if end - start != u64::from(spec.stored_len) || !as_is_fits {
                return Err(malformed());
            }
            placed.push((spec, start));
        }

        let mut value = crate::slice::UnfilledSlice::new(len);
        let mut target = value.target();
        let mut at = 0;
        while let Some(&(spec, start)) = placed.get(at) {
            if !spec.is_stored_as_is(compression) {
                let stored_len = spec.stored_len as usize;
                self.count_request(stored_len);
                let bytes = crate::file::read_exact(file.as_ref(), start, stored_len)?;
                let mut part = target.portion(target.filled(), spec.raw_len as usize);
                let mut decoded = 0;
                let checked = decode_chunk_into(
                    &spec,
                    &bytes,
                    compression,
                    #[cfg(zstd_any)]
                    blob_file.0.zstd_dictionary.as_deref(),
                    part.init_mut(),
                    &mut decoded,
                );
                self.count_decoded(decoded);
                checked?;
                part.set_filled(spec.raw_len as usize)?;
                target.extend_from_portion(spec.raw_len as usize);
                at += 1;
                continue;
            }
            // The run of chunks stored as they are from here: back to back in
            // the file as in the value, so one read fills them all.
            let run_end = placed
                .iter()
                .skip(at)
                .position(|(spec, _)| !spec.is_stored_as_is(compression))
                .map_or(placed.len(), |n| at + n);
            let run = placed.get(at..run_end).unwrap_or_default();
            let run_len: usize = run.iter().map(|(spec, _)| spec.raw_len as usize).sum();
            self.count_request(run_len);
            let mut part = target.portion(target.filled(), run_len);
            file.read_at_buf(&mut part, start)?;
            if part.filled() != run_len {
                return Err(malformed());
            }
            let mut from = 0;
            for (spec, _) in run {
                let to = from + spec.raw_len as usize;
                check_chunk(spec, part.init_mut().get(from..to).ok_or_else(malformed)?)?;
                from = to;
            }
            self.count_decoded(run_len);
            target.extend_from_portion(run_len);
            at = run_end;
        }
        if target.filled() != len {
            return Err(malformed());
        }
        // SAFETY: every byte of the value was filled above, chunk by chunk,
        // as `target` counted them.
        #[expect(unsafe_code, reason = "see safety")]
        Ok(unsafe { value.assume_filled() })
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
            if prev == Some(at) {
                if let Some(last) = shared.last_mut() {
                    *last = true;
                }
            } else {
                shared.push(false);
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
            // read is that frame alone. In the read's own store only what it
            // hands on is: a value stored as it is would pin the read for as
            // long as its caller holds it, while the entries stay with the
            // store, which lets the read go with the frame.
            let whole = from == 0 && to == bytes.len();
            let at = vhandle.frame_at();
            match into {
                PrefetchInto::Cache => {
                    let frame = if whole {
                        frame
                    } else {
                        let (frame, copied) = own_frame(frame, blob_file.compression(), true);
                        self.count_gather(copied);
                        frame
                    };
                    cache.insert_blob_frame(tree_id, at, Arc::new(frame));
                }
                PrefetchInto::Local(store) => {
                    let frame = if whole {
                        frame
                    } else {
                        let (frame, copied) = own_frame(frame, blob_file.compression(), false);
                        self.count_gather(copied);
                        frame
                    };
                    store.frames.insert(at, frame);
                }
            }
        }
    }
}

/// The span of frames that starts with `items[start]`'s: the following frames
/// of the same file within `max_gap` of it, the whole extent within
/// `max_read` and `read_budget`. `span` gives where a handle's frame lies, as
/// its file's index places it. `None` when the first frame is not there.
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
    let mut end = start + 1;
    // The first frame alone past the bound is left to the direct read.
    if span_end - span_start > reach {
        return Some(Span {
            frames: start..end,
            start: span_start,
            end: span_end,
        });
    }
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

/// `frame`, decoded from a file under `compression`, holding its own payload,
/// and its own entries too when `entries` says so, and the bytes copied for
/// that: a frame decoded out of a larger read is a view into it, its payload
/// too when it is stored as it is.
fn own_frame(
    frame: DecodedFrame,
    compression: crate::CompressionType,
    entries: bool,
) -> (DecodedFrame, usize) {
    let stored_as_is = frame.header.is_raw() || compression == crate::CompressionType::None;
    let copied = if entries { frame.entries.len() } else { 0 }
        + if stored_as_is { frame.payload.len() } else { 0 };
    let frame = DecodedFrame {
        entries: if entries {
            crate::Slice::new(&frame.entries)
        } else {
            frame.entries
        },
        payload: if stored_as_is {
            crate::Slice::new(&frame.payload)
        } else {
            frame.payload
        },
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
