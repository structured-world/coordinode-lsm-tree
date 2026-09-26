// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Lazily-decoded data block: decode only the inner zstd blocks a read
//! actually touches, growing the decompressed prefix on demand.
//!
//! A large cold-tier data block compresses into many inner zstd blocks (see
//! the per-table `block_layout` section). A range query that reads only the
//! start of such a block should pay to decompress only the inner blocks
//! covering its key range, not the whole block.
//!
//! [`LazyBlock`] decodes the inner blocks covering a read's key range and skips
//! the trailing blocks (the perf win). The first touch is a cold `[0, N)` pass;
//! growing the covered extent RESUMES from a stored entropy/repcode snapshot +
//! decompressed window ([`PartialResume`]), decoding only the new tail blocks
//! instead of re-decompressing the prefix. The resume payload is cached
//! alongside the partial block, so even a later range query (a fresh, dropped
//! decoder) continues incrementally rather than from block 0.
//!
//! It is a TRANSIENT, single-thread engine (`FrameDecoder` is not `Send`): a
//! live `LazyBlock` lives on the stack for one block access; the resumable
//! state needed to continue later is handed back via [`LazyBlock::resume_payload`]
//! and kept in the cache.

use crate::comparator::SharedComparator;
use crate::table::DataBlock;
use crate::table::block::{Block, Decoder, Header, ParsedItem};
use crate::table::data_block::DataBlockParsedItem;
use crate::{Checksum, InternalValue, Slice, UserKey};
use std::sync::Arc;
use structured_zstd::decoding::{FrameDecoder, ResumeInput, ResumeState};

/// Cross-call resume payload for a partially-decoded cold block, stored in the
/// cache alongside the partial block so a later range query can extend the
/// decoded prefix WITHOUT re-decompressing it from inner block 0.
///
/// Holds the decompressed prefix (`window_prime`), the entropy/repcode snapshot
/// to resume at the next inner block, and the compressed-frame cursor of that
/// block. The `ResumeState` is reference-counted because it carries the FSE /
/// Huffman scratch and is not itself `Clone`.
#[derive(Clone)]
pub struct PartialResume {
    /// Decompressed bytes of inner blocks `[0, decoded_blocks)`, contiguous.
    /// Fed back as the resume match window and reused as the synthesized block's
    /// entry region.
    pub window_prime: Slice,
    /// Number of inner blocks already decoded into `window_prime`.
    pub decoded_blocks: u32,
    /// Entropy/repcode snapshot to resume at inner block `decoded_blocks`, or
    /// `None` when the frame has been fully decoded (nothing left to resume).
    pub state: Option<Arc<ResumeState>>,
    /// Absolute frame offset of inner block `decoded_blocks` (where the
    /// resuming `decode_blocks_partial` repositions `source`).
    pub compressed_cursor: u64,
}

/// A data block decoded lazily from its compressed zstd frame, inner block by
/// inner block, on demand, with true incremental resume.
pub struct LazyBlock {
    /// Compressed zstd frame (the block payload after decrypt + ECC verify),
    /// shared with the read that produced it rather than copied, so the
    /// resumable decoder can read further inner blocks on top-up.
    source: std::io::Cursor<Slice>,
    /// Decoder, reset (header re-parse) before each decode; resume restores the
    /// entropy/repcode state so only the new tail blocks are decompressed.
    decoder: FrameDecoder,
    /// Cumulative decompressed END offset of each inner block (the persisted
    /// `block_layout`). `ends.last()` == total decompressed size.
    ends: Vec<u32>,
    /// Count of inner blocks already decoded into the prefix.
    decoded_blocks: u32,
    /// Decompressed bytes of inner blocks `[0, decoded_blocks)`, contiguous,
    /// once this decoder has had to extend them.
    decompressed: Vec<u8>,
    /// A resumed prefix not copied into `decompressed` yet: the decoder needs
    /// it as an owned, growable window only when it decodes further, so a read
    /// the cached prefix already covers never copies it.
    seed: Option<Slice>,
    /// Resume snapshot to continue at `decoded_blocks`; `None` before the first
    /// decode or once the frame is fully decoded.
    resume_state: Option<Arc<ResumeState>>,
    /// Absolute frame offset of inner block `decoded_blocks`.
    compressed_cursor: u64,
    /// Bytes this decoder copied into its prefix: a resumed prefix moved into
    /// `decompressed`, and each tail appended to an existing prefix.
    copied: usize,
    /// Bytes the decompressor produced for this decoder, including the inner
    /// blocks it decoded before one that failed.
    produced: usize,
}

impl LazyBlock {
    /// Wraps a compressed `frame` with its inner-block `ends` layout for a cold
    /// (first-touch) decode. Reads the frame header up front; decodes no block
    /// bodies until [`Self::ensure_decoded_to`].
    ///
    /// # Errors
    ///
    /// Returns an error if the frame header is malformed.
    pub fn new(frame: Slice, ends: Vec<u32>) -> crate::Result<Self> {
        let mut source = std::io::Cursor::new(frame);
        let mut decoder = FrameDecoder::new();
        decoder
            .reset(&mut source)
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
        Ok(Self {
            source,
            decoder,
            ends,
            decoded_blocks: 0,
            decompressed: Vec::new(),
            seed: None,
            resume_state: None,
            compressed_cursor: 0,
            copied: 0,
            produced: 0,
        })
    }

    /// Resumes a partially-decoded block from a cached [`PartialResume`]: the
    /// decompressed prefix and resume snapshot are seeded so
    /// [`Self::ensure_decoded_to`] decodes only the new tail blocks. The frame
    /// header is re-parsed lazily inside `ensure_decoded_to`, so this is
    /// infallible.
    pub fn from_resume(frame: Slice, ends: Vec<u32>, resume: PartialResume) -> Self {
        Self {
            source: std::io::Cursor::new(frame),
            decoder: FrameDecoder::new(),
            ends,
            decoded_blocks: resume.decoded_blocks,
            decompressed: Vec::new(),
            seed: Some(resume.window_prime),
            resume_state: resume.state,
            compressed_cursor: resume.compressed_cursor,
            copied: 0,
            produced: 0,
        }
    }

    /// Total decompressed size of the block (last cumulative end).
    pub fn total_len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0) as usize
    }

    /// Decompressed prefix decoded so far.
    pub fn decoded(&self) -> &[u8] {
        match &self.seed {
            Some(seed) => seed,
            None => &self.decompressed,
        }
    }

    /// The resumed prefix, while nothing has been decoded on top of it: the
    /// caller can reuse it instead of copying [`Self::decoded`].
    const fn untouched_seed(&self) -> Option<&Slice> {
        self.seed.as_ref()
    }

    /// Number of inner blocks decoded so far (for laziness assertions / tests).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "laziness assertion helper, used in tests")
    )]
    pub fn decoded_blocks(&self) -> u32 {
        self.decoded_blocks
    }

    /// Snapshot the current decode as a [`PartialResume`] for caching, so a
    /// later read continues from here instead of re-decoding from block 0.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the read path builds its payload from the prefix it already holds"
        )
    )]
    pub fn resume_payload(&self) -> PartialResume {
        self.resume_payload_over(Slice::from(self.decoded()))
    }

    /// [`Self::resume_payload`] over `window_prime`, a `Slice` of exactly
    /// [`Self::decoded`] the caller already holds, so nothing is copied.
    fn resume_payload_over(&self, window_prime: Slice) -> PartialResume {
        debug_assert_eq!(window_prime.as_ref(), self.decoded());
        PartialResume {
            window_prime,
            decoded_blocks: self.decoded_blocks,
            state: self.resume_state.clone(),
            compressed_cursor: self.compressed_cursor,
        }
    }

    /// Grow the decompressed prefix until it covers byte offset `upto`
    /// (exclusive), decoding inner blocks up to the one covering `upto` and
    /// skipping the trailing blocks. A no-op when the prefix already reaches
    /// `upto`.
    ///
    /// The first decode is a cold `[0, end_block)` pass; every later growth
    /// RESUMES from the stored entropy/repcode snapshot + decompressed window,
    /// decoding only the new tail blocks `[decoded_blocks, end_block)` (no
    /// re-decode of the prefix). `reset` re-parses the frame header from the
    /// start, then `source` is repositioned to the resume block's compressed
    /// offset.
    ///
    /// # Errors
    ///
    /// Returns an error if an inner block fails to decode (corruption), or a
    /// resume snapshot is rejected (frame / window mismatch).
    pub fn ensure_decoded_to(&mut self, upto: usize) -> crate::Result<()> {
        if upto <= self.decoded().len() {
            return Ok(());
        }
        // Inner block whose decompressed range covers `upto - 1` (the first
        // block whose cumulative end exceeds it), clamped to the last block.
        let probe = upto.saturating_sub(1);
        let target = self
            .ends
            .partition_point(|&e| (e as usize) <= probe)
            .min(self.ends.len().saturating_sub(1));
        #[expect(
            clippy::cast_possible_truncation,
            reason = "inner-block index is bounded by ends.len(), well within u32"
        )]
        let end_block = (target + 1) as u32;
        if end_block <= self.decoded_blocks {
            return Ok(());
        }

        // The decoder extends the prefix in place, so a resumed prefix becomes
        // an owned window only now that something is decoded on top of it.
        if let Some(seed) = self.seed.take() {
            self.decompressed = seed.to_vec();
            self.copied += seed.len();
        }

        // `reset` re-parses the frame header (it reads from the start); for a
        // resume we then reposition `source` to the resume block's offset.
        self.source.set_position(0);
        self.decoder
            .reset(&mut self.source)
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;

        let resuming = self.resume_state.is_some();
        let pd = if let Some(state) = self.resume_state.as_deref() {
            self.source.set_position(self.compressed_cursor);
            self.decoder
                .decode_blocks_partial(
                    &mut self.source,
                    state.block_index(),
                    end_block,
                    Some(ResumeInput {
                        window_prime: &self.decompressed,
                        state,
                    }),
                    true,
                )
                .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?
        } else {
            self.decoder
                .decode_blocks_partial(&mut self.source, 0, end_block, None, true)
                .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?
        };
        // Whatever the pass decoded counts, the blocks before a failing one
        // included: the decompressor did that work.
        self.produced += pd.data.len();
        if let Some((idx, err)) = pd.stopped_at {
            return Err(crate::Error::Io(crate::io::Error::other(format!(
                "lazy partial decode stopped at inner block {idx}: {err:?}"
            ))));
        }

        // Fresh decode emits `[0, end_block)`; a resume emits only the new tail
        // `[block_index, end_block)`, contiguous with the existing prefix.
        let consumed = self.decoder.bytes_read_from_source();
        if resuming {
            self.copied += pd.data.len();
            self.decompressed.extend_from_slice(&pd.data);
            self.compressed_cursor += consumed;
        } else {
            // A fresh pass decodes the whole prefix: take the decoder's
            // buffer as it is.
            self.decompressed = pd.data;
            self.compressed_cursor = consumed;
        }
        self.decoded_blocks = pd.start_block + pd.blocks_decoded;
        self.resume_state = pd.resume_state.map(Arc::new);
        Ok(())
    }
}

/// Synthesize a valid standalone data-block (`entries ‖ trailer`) from a
/// decoded entry-region `prefix`, so the normal trailer-driven decoder / seek
/// path can read it. The binary index is rebuilt from the prefix's positional
/// restart heads (no trailer needed in the source); no hash index is emitted
/// (seek falls back to binary search). A truncated tail entry in `prefix` is
/// dropped (only complete entries are indexed and kept).
///
/// The entries are copied once, straight into the block's buffer next to the
/// trailer; `prefix` itself is scanned in place.
///
/// # Errors
///
/// Returns an error if the binary index fails to serialize.
fn synthesize_block_bytes(prefix: &Slice, restart_interval: u8) -> crate::Result<Slice> {
    use crate::table::block::TRAILER_START_MARKER;
    use crate::table::block::binary_index::Builder as BinaryIndexBuilder;

    // Scan the prefix's complete entries for restart-head offsets, the item
    // count, and the end of the last complete entry (truncated tail excluded).
    let probe = probe_block(prefix);
    let (restart_offsets, item_count, entries_end) =
        Decoder::<InternalValue, DataBlockParsedItem>::new_forward_headerless(
            &probe,
            restart_interval,
            prefix.len(),
        )
        .scan_restart_offsets();

    // The trailer is built on its own, positioned as if it followed the
    // entries, so the entries are copied only by the final fuse.
    let mut trailer = Vec::with_capacity(1 + restart_offsets.len() * 4 + TRAILER_FOOTER_SIZE);
    trailer.push(TRAILER_START_MARKER);

    #[expect(
        clippy::cast_possible_truncation,
        reason = "block offsets are far below u32::MAX"
    )]
    let binary_index_offset = (entries_end + trailer.len()) as u32;
    let mut bib = BinaryIndexBuilder::new(restart_offsets.len());
    for off in restart_offsets {
        bib.insert(off);
    }
    let (step_size, binary_index_len) = bib.write(&mut trailer)?;

    // Footer — byte-for-byte the layout `Trailer::write` emits.
    trailer.push(restart_interval);
    trailer.push(step_size);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "index pointers <= item count, far below u32::MAX"
    )]
    trailer.extend_from_slice(&(binary_index_len as u32).to_le_bytes());
    trailer.extend_from_slice(&binary_index_offset.to_le_bytes());
    trailer.extend_from_slice(&0u32.to_le_bytes()); // hash_index_len
    trailer.extend_from_slice(&0u32.to_le_bytes()); // hash_index_offset
    trailer.push(1); // prefix truncation on
    trailer.push(0); // fixed key size (u8, unused)
    trailer.extend_from_slice(&0u16.to_le_bytes()); // fixed key size (u16, unused)
    trailer.push(0); // fixed value size (u8, unused)
    trailer.extend_from_slice(&0u32.to_le_bytes()); // fixed value size (u32, unused)
    #[expect(
        clippy::cast_possible_truncation,
        reason = "item count far below u32::MAX for a single block"
    )]
    trailer.extend_from_slice(&(item_count as u32).to_le_bytes());

    let entries = prefix.get(..entries_end).unwrap_or(prefix);
    Ok(Slice::fused(entries, &trailer))
}

/// A decoder-only block over `prefix`, sharing its bytes.
fn probe_block(prefix: &Slice) -> Block {
    Block {
        header: synthetic_header(prefix.len()),
        data: prefix.clone(),
    }
}

/// Trailer footer size in bytes (mirrors `Trailer::TRAILER_SIZE`): the fixed
/// fields after the (optional) hash index.
const TRAILER_FOOTER_SIZE: usize = 31;

/// Synthetic block header for a decoder-only block built from decoded bytes
/// (checksum / lengths are not consulted by the entry decoder).
fn synthetic_header(len: usize) -> Header {
    use crate::table::block::BlockType;
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block length is far below u32::MAX"
    )]
    let len = len as u32;
    Header {
        block_type: BlockType::Data,
        block_flags: 0,
        stored_checksum: Checksum::from_raw(0),
        data_length: len,
        uncompressed_length: len,
    }
}

/// Synthesize a standalone `DataBlock` (entries + trailer) from an
/// already-decoded prefix, so the normal seek / iterate path can read it. Used
/// to serve a cached partial block whose decompressed prefix is held in the
/// cache (no re-decode).
///
/// The block's bytes are a fresh copy of the complete entries plus the
/// trailer; `prefix` itself is only scanned.
///
/// # Errors
///
/// Returns an error if the trailer fails to synthesize.
pub fn synthesize_data_block(prefix: &Slice, restart_interval: u8) -> crate::Result<DataBlock> {
    let bytes = synthesize_block_bytes(prefix, restart_interval)?;
    Ok(DataBlock::new(Block {
        header: synthetic_header(bytes.len()),
        data: bytes,
    }))
}

/// What [`partial_data_block`] built.
pub struct PartialBlock {
    /// The synthesized block covering the decoded prefix.
    pub block: DataBlock,
    /// The highest user key the block covers (its last complete entry's key),
    /// or `None` for an empty prefix.
    pub covered_upper: Option<UserKey>,
    /// The resume payload to cache so the next read continues from here.
    pub payload: PartialResume,
}

/// The work a [`partial_data_block`] call did, whether it succeeded or not.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartialWork {
    /// Bytes the decompressor produced for this call: only the tail it
    /// decoded, never a resumed prefix, and including the inner blocks it
    /// decoded before one that failed.
    pub decoded: usize,
    /// Bytes copied: each prefix made shareable for a scan, a resumed prefix
    /// moved into the decoder's window, each tail appended to it, and the
    /// synthesized block.
    pub copied: usize,
}

/// Build a standalone `DataBlock` covering `[block_start, upper]` from a
/// compressed `frame`, decoding only the inner zstd blocks needed to reach
/// `upper` (skipping the trailing blocks) and synthesizing a trailer so the
/// normal seek / iterate path works. The caller's iterator then trims to the
/// exact query bounds.
///
/// `resume` continues a previously-cached partial decode (see [`PartialResume`])
/// so growing the extent decodes only the new tail blocks instead of from block
/// 0. Pass `None` for a cold first-touch decode.
///
/// The prefix is made shareable once per growth step and that one `Slice`
/// serves every later scan, the cached resume window and the covered key, so
/// no step copies it again. A resumed prefix that already covers `upper` is
/// not copied at all.
///
/// `work` receives what the call decoded and copied, on failure too: a read
/// that fails at a later inner block still did the work before it.
///
/// # Errors
///
/// Returns an error if the frame or an inner block fails to decode, or the
/// trailer fails to synthesize.
pub fn partial_data_block(
    frame: Slice,
    ends: Vec<u32>,
    restart_interval: u8,
    comparator: &SharedComparator,
    upper: &[u8],
    resume: Option<PartialResume>,
    work: &mut PartialWork,
) -> crate::Result<PartialBlock> {
    let mut lazy = match resume {
        Some(payload) => LazyBlock::from_resume(frame, ends, payload),
        None => LazyBlock::new(frame, ends)?,
    };
    let built = build_partial_block(
        &mut lazy,
        restart_interval,
        comparator,
        upper,
        &mut work.copied,
    );
    work.copied += lazy.copied;
    work.decoded += lazy.produced;
    built
}

/// The body of [`partial_data_block`] over an opened decoder, adding the
/// copies it makes itself to `copied` as it makes them.
fn build_partial_block(
    lazy: &mut LazyBlock,
    restart_interval: u8,
    comparator: &SharedComparator,
    upper: &[u8],
    copied: &mut usize,
) -> crate::Result<PartialBlock> {
    let total = lazy.total_len();
    let mut prefix = lazy
        .untouched_seed()
        .cloned()
        .unwrap_or_else(|| Slice::from(lazy.decoded()));

    // Grow the decoded extent until a key strictly greater than `upper` appears
    // (so `upper`'s entry is fully decoded) or the block is exhausted. A resumed
    // block may already cover `upper`, in which case no decode happens.
    loop {
        if prefix.len() >= total
            || (!prefix.is_empty()
                && prefix_reaches_past(&prefix, restart_interval, comparator, upper))
        {
            break;
        }
        let extent = if prefix.is_empty() {
            (64 * 1024).min(total)
        } else {
            // Grow the read-ahead window geometrically, capped at `total`. The
            // `.min(total)` is the real bound; the saturating guards the doubling
            // before that cap.
            prefix.len().saturating_mul(2).min(total)
        };
        lazy.ensure_decoded_to(extent)?;
        if lazy.decoded().len() == prefix.len() {
            // Nothing left to decode: the frame ends before `upper`.
            break;
        }
        prefix = Slice::from(lazy.decoded());
        *copied += prefix.len();
    }

    let covered_upper = last_complete_key(&prefix, restart_interval);
    let block = synthesize_data_block(&prefix, restart_interval)?;
    *copied += block.inner.data.len();
    let payload = lazy.resume_payload_over(prefix);
    Ok(PartialBlock {
        block,
        covered_upper,
        payload,
    })
}

/// The user key of the last COMPLETE entry in a decoded `prefix` (a truncated
/// tail entry is excluded, matching [`synthesize_block_bytes`]). `None` when the
/// prefix holds no complete entry. This is the highest key the synthesized
/// partial block covers, used to tag the cache entry's extent.
fn last_complete_key(prefix: &Slice, restart_interval: u8) -> Option<UserKey> {
    let probe = probe_block(prefix);
    // Bound the scan to the complete-entry region so a truncated tail entry is
    // not materialized (its decoded bytes would be garbage).
    let (_offsets, _count, entries_end) =
        Decoder::<InternalValue, DataBlockParsedItem>::new_forward_headerless(
            &probe,
            restart_interval,
            prefix.len(),
        )
        .scan_restart_offsets();
    // Forward-consume to the last complete entry (the decoder is forward-only
    // here; `fold` keeps the final key without a reverse pass).
    Decoder::<InternalValue, DataBlockParsedItem>::new_forward_headerless(
        &probe,
        restart_interval,
        entries_end,
    )
    .fold(None, |_, item| {
        Some(item.materialize(&probe.data).key.user_key)
    })
}

/// Whether the decoded `prefix` contains an entry whose key is strictly greater
/// than `upper` (i.e. the prefix already covers everything `<= upper`).
fn prefix_reaches_past(
    prefix: &Slice,
    restart_interval: u8,
    comparator: &SharedComparator,
    upper: &[u8],
) -> bool {
    let probe = probe_block(prefix);
    Decoder::<InternalValue, DataBlockParsedItem>::new_forward_headerless(
        &probe,
        restart_interval,
        prefix.len(),
    )
    .any(|item| {
        comparator.compare(item.materialize(&probe.data).key.user_key.as_ref(), upper)
            == std::cmp::Ordering::Greater
    })
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "test code"
)]
mod tests;
