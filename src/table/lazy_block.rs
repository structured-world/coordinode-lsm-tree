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

use crate::comparator::SharedComparator;
use crate::table::DataBlock;
use crate::table::block::{Block, Decoder, Header, ParsedItem};
use crate::table::data_block::DataBlockParsedItem;
use crate::{Checksum, InternalValue, Slice};
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
}

/// Synthesize a valid standalone data-block (`entries ‖ trailer`) from a
/// decoded entry-region `prefix`, so the normal trailer-driven decoder / seek
/// path can read it. The binary index is rebuilt from the prefix's positional
/// restart heads (no trailer needed in the source); no hash index is emitted
/// (seek falls back to binary search). A truncated tail entry in `prefix` is
/// dropped (only complete entries are indexed and kept).
///
/// # Errors
///
/// Returns an error if the binary index fails to serialize.
fn synthesize_block_bytes(prefix: &[u8], restart_interval: u8) -> crate::Result<Vec<u8>> {
    use crate::table::block::TRAILER_START_MARKER;
    use crate::table::block::binary_index::Builder as BinaryIndexBuilder;

    // Scan the prefix's complete entries for restart-head offsets, the item
    // count, and the end of the last complete entry (truncated tail excluded).
    let probe = Block {
        header: synthetic_header(prefix.len()),
        data: Slice::from(prefix),
    };
    let (restart_offsets, item_count, entries_end) =
        Decoder::<InternalValue, DataBlockParsedItem>::new_forward_headerless(
            &probe,
            restart_interval,
            prefix.len(),
        )
        .scan_restart_offsets();

    let mut out =
        Vec::with_capacity(entries_end + 1 + restart_offsets.len() * 4 + TRAILER_FOOTER_SIZE);
    out.extend_from_slice(prefix.get(..entries_end).unwrap_or(prefix));
    out.push(TRAILER_START_MARKER);

    #[expect(
        clippy::cast_possible_truncation,
        reason = "block offsets are far below u32::MAX"
    )]
    let binary_index_offset = out.len() as u32;
    let mut bib = BinaryIndexBuilder::new(restart_offsets.len());
    for off in restart_offsets {
        bib.insert(off);
    }
    let (step_size, binary_index_len) = bib.write(&mut out)?;

    // Footer — byte-for-byte the layout `Trailer::write` emits.
    out.push(restart_interval);
    out.push(step_size);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "index pointers <= item count, far below u32::MAX"
    )]
    out.extend_from_slice(&(binary_index_len as u32).to_le_bytes());
    out.extend_from_slice(&binary_index_offset.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // hash_index_len
    out.extend_from_slice(&0u32.to_le_bytes()); // hash_index_offset
    out.push(1); // prefix truncation on
    out.push(0); // fixed key size (u8, unused)
    out.extend_from_slice(&0u16.to_le_bytes()); // fixed key size (u16, unused)
    out.push(0); // fixed value size (u8, unused)
    out.extend_from_slice(&0u32.to_le_bytes()); // fixed value size (u32, unused)
    #[expect(
        clippy::cast_possible_truncation,
        reason = "item count far below u32::MAX for a single block"
    )]
    out.extend_from_slice(&(item_count as u32).to_le_bytes());

    Ok(out)
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
        checksum: Checksum::from_raw(0),
        data_length: len,
        uncompressed_length: len,
    }
}

/// Build a standalone `DataBlock` covering `[block_start, upper]` from a
/// compressed `frame`, decoding only the inner zstd blocks needed to reach
/// `upper` (skipping the trailing blocks) and synthesizing a trailer so the
/// normal seek / iterate path works. The caller's iterator then trims to the
/// exact query bounds. Returns the block and the number of inner blocks
/// decoded.
///
/// # Errors
///
/// Returns an error if the frame or an inner block fails to decode, or the
/// trailer fails to synthesize.
pub fn partial_data_block(
    frame: Vec<u8>,
    ends: Vec<u32>,
    restart_interval: u8,
    comparator: &SharedComparator,
    upper: &[u8],
) -> crate::Result<(DataBlock, u32)> {
    let mut lazy = LazyBlock::new(frame, ends)?;
    let total = lazy.total_len();
    let mut extent = 0usize;

    // Grow the decoded extent until a key strictly greater than `upper` appears
    // (so `upper`'s entry is fully decoded) or the block is exhausted.
    loop {
        extent = if extent == 0 {
            (64 * 1024).min(total)
        } else {
            extent.saturating_mul(2).min(total)
        };
        lazy.ensure_decoded_to(extent)?;
        if extent >= total
            || prefix_reaches_past(lazy.decoded(), restart_interval, comparator, upper)
        {
            break;
        }
    }

    let bytes = synthesize_block_bytes(lazy.decoded(), restart_interval)?;
    let blocks = lazy.decoded_blocks();
    let block = DataBlock::new(Block {
        header: synthetic_header(bytes.len()),
        data: Slice::from(bytes),
    });
    Ok((block, blocks))
}

/// Whether the decoded `prefix` contains an entry whose key is strictly greater
/// than `upper` (i.e. the prefix already covers everything `<= upper`).
fn prefix_reaches_past(
    prefix: &[u8],
    restart_interval: u8,
    comparator: &SharedComparator,
    upper: &[u8],
) -> bool {
    let probe = Block {
        header: synthetic_header(prefix.len()),
        data: Slice::from(prefix),
    };
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
        let total = lazy.total_len();
        lazy.ensure_decoded_to(total).expect("full decode");
        assert_eq!(lazy.decoded_blocks(), ends.len() as u32);
        assert_eq!(lazy.decoded(), reference.as_slice());
    }

    #[test]
    fn lazy_block_growing_extents_equal_one_shot_full() {
        // Growing the decoded extent in steps (each a re-decode of the covering
        // prefix) must always equal the matching prefix of a full decode and
        // converge to the whole block.
        let (frame, ends, reference) = large_block_frame();
        let mut lazy = LazyBlock::new(frame, ends).expect("new lazy block");
        let total = reference.len();
        let mut cursor = 0usize;
        while cursor < total {
            cursor = (cursor + 64 * 1024).min(total);
            lazy.ensure_decoded_to(cursor).expect("grow extent");
            assert_eq!(lazy.decoded(), &reference[..lazy.decoded().len()]);
        }
        assert_eq!(lazy.decoded(), reference.as_slice());
    }

    /// A block synthesized (trailer rebuilt) from a full block's entry region
    /// must be indistinguishable from the original under forward iteration,
    /// backward iteration, and seek to arbitrary keys — proving the rebuilt
    /// binary-index trailer is wire-correct.
    #[test]
    fn synthesized_block_matches_original_seek_iter_both_ways() {
        use crate::SeqNo;
        use crate::comparator::default_comparator;
        use crate::table::block::Decoder as BlockDecoder;
        use crate::table::block::{Block, BlockType, Header, ParsedItem};
        use crate::table::data_block::DataBlockParsedItem;

        let items: Vec<InternalValue> = (0u64..500)
            .map(|i| {
                InternalValue::from_components(
                    format!("key-{i:06}").into_bytes(),
                    format!("value-{i:06}").into_bytes(),
                    0,
                    Value,
                )
            })
            .collect();
        let ri = 16u8;
        let block_bytes = DataBlock::encode_into_vec(&items, ri, 0.0).expect("encode");
        let cmp = default_comparator();

        let full = DataBlock::new(Block {
            data: Slice::from(block_bytes.clone()),
            header: Header::test_dummy(BlockType::Data),
        });

        // Entry-region prefix (no trailer), via the real decoder's entries_end.
        let entries_end = BlockDecoder::<InternalValue, DataBlockParsedItem>::new(&Block {
            data: Slice::from(block_bytes.clone()),
            header: Header::test_dummy(BlockType::Data),
        })
        .entries_end_for_test()
        .expect("entries_end");
        let prefix = &block_bytes[..entries_end];

        let synth_bytes = synthesize_block_bytes(prefix, ri).expect("synthesize");
        let synth = DataBlock::new(Block {
            data: Slice::from(synth_bytes),
            header: Header::test_dummy(BlockType::Data),
        });

        // Forward iteration identical (and equals the original items).
        let full_fwd: Vec<InternalValue> = full
            .iter(cmp.clone())
            .map(|x| x.materialize(full.as_slice()))
            .collect();
        let synth_fwd: Vec<InternalValue> = synth
            .iter(cmp.clone())
            .map(|x| x.materialize(synth.as_slice()))
            .collect();
        assert_eq!(full_fwd, items, "sanity: full forward == items");
        assert_eq!(synth_fwd, full_fwd, "synth forward must equal full forward");

        // Backward iteration identical.
        let full_bwd: Vec<InternalValue> = full
            .iter(cmp.clone())
            .rev()
            .map(|x| x.materialize(full.as_slice()))
            .collect();
        let synth_bwd: Vec<InternalValue> = synth
            .iter(cmp.clone())
            .rev()
            .map(|x| x.materialize(synth.as_slice()))
            .collect();
        assert_eq!(
            synth_bwd, full_bwd,
            "synth backward must equal full backward"
        );

        // Seek to arbitrary keys (including misses between keys) identical.
        for needle in [
            b"key-000000".to_vec(),
            b"key-000001".to_vec(),
            b"key-000250".to_vec(),
            b"key-000499".to_vec(),
            b"key-0002".to_vec(),   // prefix / between keys
            b"key-999999".to_vec(), // past end
        ] {
            let mut fi = full.iter(cmp.clone());
            fi.seek(&needle, SeqNo::MAX);
            let f: Vec<InternalValue> = fi.map(|x| x.materialize(full.as_slice())).collect();

            let mut si = synth.iter(cmp.clone());
            si.seek(&needle, SeqNo::MAX);
            let s: Vec<InternalValue> = si.map(|x| x.materialize(synth.as_slice())).collect();

            assert_eq!(s, f, "synth seek({needle:?}) must equal full seek");
        }
    }

    /// `partial_data_block` builds, from a compressed frame, a block covering
    /// `[start, upper]` by decoding only the inner blocks up to `upper`. Its
    /// range scan must equal the full block's, and it must decode strictly fewer
    /// inner blocks for a near-start upper bound.
    #[test]
    fn partial_data_block_range_matches_full_and_skips_trailing() {
        use crate::SeqNo;
        use crate::comparator::default_comparator;
        use crate::table::block::{Block, BlockType, Header, ParsedItem};

        let (frame, ends, block_bytes) = large_block_frame();
        let nblocks = ends.len() as u32;
        let cmp = default_comparator();

        let full = DataBlock::new(Block {
            data: Slice::from(block_bytes),
            header: Header::test_dummy(BlockType::Data),
        });

        // Near-start window so trailing inner blocks are skippable.
        let lower = b"key-000000000010".to_vec();
        let upper = b"key-000000000050".to_vec();

        // Full reference: seek the range on the whole block.
        let mut fi = full.iter(cmp.clone());
        fi.seek(&lower, SeqNo::MAX);
        fi.seek_upper_exclusive(&upper, SeqNo::MAX);
        let reference: Vec<InternalValue> = fi.map(|x| x.materialize(full.as_slice())).collect();
        assert_eq!(reference.len(), 40, "i=10..50 → 40 entries");

        // Partial: build a covering block from the frame, then seek the same range.
        let (partial, blocks) =
            partial_data_block(frame, ends, 16, &cmp, &upper).expect("partial block");
        assert!(
            blocks < nblocks,
            "near-start upper must skip trailing inner blocks: {blocks}/{nblocks}",
        );
        let mut pi = partial.iter(cmp.clone());
        pi.seek(&lower, SeqNo::MAX);
        pi.seek_upper_exclusive(&upper, SeqNo::MAX);
        let got: Vec<InternalValue> = pi.map(|x| x.materialize(partial.as_slice())).collect();

        assert_eq!(
            got, reference,
            "partial range scan must equal the full range scan"
        );
    }

    /// Synthesizing over a prefix that ends mid-entry (inner-block boundaries
    /// need not align with KV entries) must drop the truncated tail and produce
    /// a valid block of the complete-entry prefix.
    #[test]
    fn synthesize_handles_truncated_prefix() {
        use crate::comparator::default_comparator;
        use crate::table::block::Decoder as BlockDecoder;
        use crate::table::block::{Block, BlockType, Header, ParsedItem};
        use crate::table::data_block::DataBlockParsedItem;

        let items: Vec<InternalValue> = (0u64..300)
            .map(|i| {
                InternalValue::from_components(
                    format!("key-{i:06}").into_bytes(),
                    format!("value-{i:06}").into_bytes(),
                    0,
                    Value,
                )
            })
            .collect();
        let ri = 16u8;
        let block_bytes = DataBlock::encode_into_vec(&items, ri, 0.0).expect("encode");
        let cmp = default_comparator();

        let entries_end = BlockDecoder::<InternalValue, DataBlockParsedItem>::new(&Block {
            data: Slice::from(block_bytes.clone()),
            header: Header::test_dummy(BlockType::Data),
        })
        .entries_end_for_test()
        .expect("entries_end");

        // Cut a few bytes into the last entry.
        let prefix = &block_bytes[..entries_end - 3];
        let synth_bytes = synthesize_block_bytes(prefix, ri).expect("synthesize truncated");
        let synth = DataBlock::new(Block {
            data: Slice::from(synth_bytes),
            header: Header::test_dummy(BlockType::Data),
        });

        let got: Vec<InternalValue> = synth
            .iter(cmp)
            .map(|x| x.materialize(synth.as_slice()))
            .collect();
        assert!(!got.is_empty());
        assert!(
            got.len() < items.len(),
            "truncated tail entry must be dropped"
        );
        assert_eq!(
            got,
            items[..got.len()].to_vec(),
            "must be a clean entry prefix"
        );
    }
}
