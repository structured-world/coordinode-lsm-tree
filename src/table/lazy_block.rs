// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Lazily-decoded data block: decode only the inner zstd blocks a read
//! actually touches, growing the decompressed prefix on demand.
//!
//! A large cold-tier data block compresses into many inner zstd blocks (see
//! the per-table `block_layout` section). A range query that reads only the
//! start of such a block should pay to decompress only the inner blocks
//! covering its key range, not the whole block.
//!
//! [`LazyBlock`] decodes the inner blocks `[0, end_block)` that cover a read's
//! key range and skips the trailing blocks (the perf win). A range query knows
//! its upper bound up front, so it decodes ONCE to the needed extent.
//!
//! `decode_blocks_partial` drains the in-range output from the match window on
//! return, so consecutive calls cannot resume one another (a later block would
//! lack the earlier blocks as match history). Each extent growth therefore
//! re-decodes the covering prefix in a single call from block 0. True
//! incremental resume (decode block N continuing, without re-decoding the
//! prefix) needs a window-priming decoder API tracked in structured-zstd#368.
//!
//! It is a TRANSIENT, single-thread engine (`FrameDecoder` is not `Send`): a
//! live `LazyBlock` lives on the stack for one block access, while the cache
//! stores only the grown decompressed bytes (a follow-up wires that path).

// The lazy-decode engine is exercised by its tests; its production consumer
// (the range-query iterator + cache path) lands in a follow-up slice.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "consumed by the range-query partial-decode wiring (next slice)"
    )
)]

use structured_zstd::decoding::FrameDecoder;

/// A data block decoded lazily from its compressed zstd frame, inner block by
/// inner block, on demand.
pub struct LazyBlock {
    /// Compressed zstd frame (the block payload after decrypt + ECC verify),
    /// owned so the resumable decoder can read further inner blocks on top-up.
    source: std::io::Cursor<Vec<u8>>,
    /// Decoder, reset before each (re-)decode of the covering prefix.
    decoder: FrameDecoder,
    /// Cumulative decompressed END offset of each inner block (the persisted
    /// `block_layout`). `ends.last()` == total decompressed size.
    ends: Vec<u32>,
    /// Count of inner blocks already decoded into `decompressed`.
    decoded_blocks: u32,
    /// Decompressed bytes of inner blocks `[0, decoded_blocks)`, contiguous.
    decompressed: Vec<u8>,
}

impl LazyBlock {
    /// Wraps a compressed `frame` with its inner-block `ends` layout (the
    /// persisted cumulative decompressed offsets). Reads the frame header
    /// up front; decodes no block bodies until [`Self::ensure_decoded_to`].
    ///
    /// # Errors
    ///
    /// Returns an error if the frame header is malformed.
    pub fn new(frame: Vec<u8>, ends: Vec<u32>) -> crate::Result<Self> {
        let mut source = std::io::Cursor::new(frame);
        let mut decoder = FrameDecoder::new();
        decoder
            .reset(&mut source)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        Ok(Self {
            source,
            decoder,
            ends,
            decoded_blocks: 0,
            decompressed: Vec::new(),
        })
    }

    /// Total decompressed size of the block (last cumulative end).
    pub fn total_len(&self) -> usize {
        self.ends.last().copied().unwrap_or(0) as usize
    }

    /// Decompressed prefix decoded so far.
    pub fn decoded(&self) -> &[u8] {
        &self.decompressed
    }

    /// Number of inner blocks decoded so far (for laziness assertions / tests).
    pub fn decoded_blocks(&self) -> u32 {
        self.decoded_blocks
    }

    /// Grow the decompressed prefix until it covers byte offset `upto`
    /// (exclusive), decoding inner blocks `[0, end_block)` where `end_block`
    /// covers `upto` and skipping the trailing blocks. A no-op when the prefix
    /// already reaches `upto`.
    ///
    /// `decode_blocks_partial` returns (and drains from the match window) the
    /// in-range output, so consecutive calls cannot resume one another — a
    /// later block would lack the earlier blocks as match history. Each growth
    /// therefore re-decodes the covering prefix in a SINGLE call from block 0
    /// (within one call the window is maintained until the final drain). A
    /// range query knows its upper bound up front, so it calls this ONCE for
    /// the extent it needs — one decode, trailing blocks skipped. (True
    /// incremental resume without re-decoding the prefix needs the
    /// window-priming decoder API tracked in structured-zstd#368.)
    ///
    /// # Errors
    ///
    /// Returns an error if an inner block fails to decode (corruption).
    pub fn ensure_decoded_to(&mut self, upto: usize) -> crate::Result<()> {
        if upto <= self.decompressed.len() {
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

        // Re-read the frame header and decode `[0, end_block)` in one call: the
        // drain-on-return behaviour means a single call is the only way to keep
        // each block's match history available to the next.
        self.source.set_position(0);
        self.decoder
            .reset(&mut self.source)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        let pd = self
            .decoder
            .decode_blocks_partial(&mut self.source, 0, end_block)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        if let Some((idx, err)) = pd.stopped_at {
            return Err(crate::Error::Io(std::io::Error::other(format!(
                "lazy partial decode stopped at inner block {idx}: {err:?}"
            ))));
        }
        self.decompressed.clear();
        self.decompressed.extend_from_slice(&pd.data);
        self.decoded_blocks = pd.blocks_decoded;
        Ok(())
    }

    /// Decode the whole block (all inner blocks).
    ///
    /// # Errors
    ///
    /// Returns an error if any inner block fails to decode.
    pub fn ensure_fully_decoded(&mut self) -> crate::Result<()> {
        self.ensure_decoded_to(self.total_len())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests {
    use super::*;
    use crate::compression::{CompressionProvider as _, ZstdBackend};
    use crate::table::DataBlock;
    use crate::{InternalValue, ValueType::Value};
    use test_log::test;

    /// Build a large sorted-KV data block, compress it, and return the frame +
    /// inner-block `ends` layout and the full decompressed reference bytes.
    fn large_block_frame() -> (Vec<u8>, Vec<u32>, Vec<u8>) {
        let items: Vec<InternalValue> = (0u64..20_000)
            .map(|i| {
                InternalValue::from_components(
                    format!("key-{i:012}").into_bytes(),
                    format!("value-{i:08}-payload").into_bytes(),
                    0,
                    Value,
                )
            })
            .collect();
        // One big data block (no spill): encode all entries into a single block.
        let block_bytes = DataBlock::encode_into_vec(&items, 16, 0.0).expect("encode block");
        // High level so the frame pre-splits into many inner zstd blocks.
        let (frame, ends) =
            ZstdBackend::compress_with_layout(&block_bytes, 19).expect("compress with layout");
        assert!(
            ends.len() >= 4,
            "fixture must split into several inner blocks, got {}",
            ends.len(),
        );
        let reference =
            ZstdBackend::decompress(&frame, block_bytes.len() + 1).expect("full decompress");
        assert_eq!(reference, block_bytes);
        (frame, ends, reference)
    }

    #[test]
    fn lazy_block_decodes_only_touched_inner_blocks() {
        let (frame, ends, reference) = large_block_frame();
        let nblocks = ends.len();

        let mut lazy = LazyBlock::new(frame, ends.clone()).expect("new lazy block");
        assert_eq!(lazy.decoded_blocks(), 0, "no block bodies decoded up front");
        assert!(lazy.decoded().is_empty());

        // Touch a byte inside the first inner block → only block 0 decoded.
        lazy.ensure_decoded_to(1).expect("decode to byte 1");
        assert_eq!(
            lazy.decoded_blocks(),
            1,
            "only the first inner block decoded"
        );
        assert_eq!(lazy.decoded().len(), ends[0] as usize);
        assert_eq!(lazy.decoded(), &reference[..ends[0] as usize]);

        // Idempotent: already covered → no further decode.
        lazy.ensure_decoded_to(ends[0] as usize)
            .expect("idempotent");
        assert_eq!(lazy.decoded_blocks(), 1);

        // Top-up to span into the third inner block → blocks 0..=2 decoded.
        let upto = ends[1] as usize + 1;
        lazy.ensure_decoded_to(upto).expect("top up");
        assert_eq!(
            lazy.decoded_blocks(),
            3,
            "topped up to cover the third block"
        );
        assert_eq!(lazy.decoded(), &reference[..ends[2] as usize]);
        assert!(
            lazy.decoded_blocks() < nblocks as u32,
            "must NOT have decoded the whole block for a partial read",
        );
    }

    #[test]
    fn lazy_block_full_decode_matches_reference() {
        let (frame, ends, reference) = large_block_frame();
        let mut lazy = LazyBlock::new(frame, ends.clone()).expect("new lazy block");
        lazy.ensure_fully_decoded().expect("full decode");
        assert_eq!(lazy.decoded_blocks(), ends.len() as u32);
        assert_eq!(lazy.decoded(), reference.as_slice());
    }

    #[test]
    fn lazy_block_growing_extents_equal_one_shot_full() {
        // Growing the decoded extent in steps (each a re-decode of the covering
        // prefix) must always equal the matching prefix of a full decode and
        // converge to the whole block.
        let (frame, ends, reference) = large_block_frame();
        let mut lazy = LazyBlock::new(frame, ends.clone()).expect("new lazy block");
        let total = reference.len();
        let mut cursor = 0usize;
        while cursor < total {
            cursor = (cursor + 64 * 1024).min(total);
            lazy.ensure_decoded_to(cursor).expect("grow extent");
            assert_eq!(lazy.decoded(), &reference[..lazy.decoded().len()]);
        }
        assert_eq!(lazy.decoded(), reference.as_slice());
    }
}
