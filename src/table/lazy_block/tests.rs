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
    // Growing the decoded extent in steps (each a RESUME from the prior
    // snapshot, decoding only the new tail blocks) must always equal the
    // matching prefix of a full decode and converge to the whole block.
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

#[test]
fn lazy_block_resume_across_decoders_equals_full() {
    // The resume path must work across a DROPPED decoder: decode a cold
    // prefix, snapshot the resume payload, drop the LazyBlock, then rebuild a
    // fresh one from the payload and grow it. Each growth resumes from the
    // cached entropy/window snapshot (not block 0) yet must stay byte-
    // identical to a one-shot full decode at every step.
    let (frame, ends, reference) = large_block_frame();
    let total = reference.len();

    // Cold first touch: decode roughly the first quarter.
    let mut lazy = LazyBlock::new(frame.clone(), ends.clone()).expect("new lazy block");
    lazy.ensure_decoded_to((total / 4).max(1))
        .expect("cold decode");
    assert!(lazy.decoded_blocks() >= 1);
    assert_eq!(lazy.decoded(), &reference[..lazy.decoded().len()]);
    let mut payload = lazy.resume_payload();
    let mut prev_len = lazy.decoded().len();
    assert!(
        payload.state.is_some(),
        "mid-frame snapshot must be resumable"
    );
    drop(lazy);

    // Resume across fresh (dropped) decoders, growing in quarter steps.
    let mut target = prev_len;
    while target < total {
        target = (target + total / 4 + 1).min(total);
        let mut resumed = LazyBlock::from_resume(frame.clone(), ends.clone(), payload.clone());
        resumed.ensure_decoded_to(target).expect("resume grow");
        // Must extend (not shrink / restart) and match the full reference.
        assert!(resumed.decoded().len() >= prev_len);
        assert_eq!(resumed.decoded(), &reference[..resumed.decoded().len()]);
        prev_len = resumed.decoded().len();
        payload = resumed.resume_payload();
    }
    assert_eq!(payload.window_prime.as_ref(), reference.as_slice());
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
    let (partial, covered_upper, payload) =
        partial_data_block(frame, ends, 16, &cmp, &upper, None).expect("partial block");
    let blocks = payload.decoded_blocks;
    assert!(
        blocks < nblocks,
        "near-start upper must skip trailing inner blocks: {blocks}/{nblocks}",
    );
    let covered = covered_upper.expect("covering block has a last key");
    assert!(
        cmp.compare(covered.as_ref(), &upper) != std::cmp::Ordering::Less,
        "covered_upper ({covered:?}) must reach at least the query upper",
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

/// `partial_data_block` fed a cached `PartialResume` must grow the covered
/// extent by RESUMING (decoding only the new tail blocks) and produce a block
/// whose wider range scan equals the full block's — proving the cache-resume
/// round-trip is correct end to end.
#[test]
fn partial_data_block_resume_grows_and_matches_full() {
    use crate::SeqNo;
    use crate::comparator::default_comparator;
    use crate::table::block::{Block, BlockType, Header, ParsedItem};

    let (frame, ends, block_bytes) = large_block_frame();
    let cmp = default_comparator();
    let full = DataBlock::new(Block {
        data: Slice::from(block_bytes),
        header: Header::test_dummy(BlockType::Data),
    });

    // First (cold) decode covering a near-start window; capture the resume.
    let narrow = b"key-000000000050".to_vec();
    let (_b0, _c0, payload) =
        partial_data_block(frame.clone(), ends.clone(), 16, &cmp, &narrow, None)
            .expect("cold partial");
    let narrow_blocks = payload.decoded_blocks;

    // Resume-grow to a much wider window; must decode strictly more blocks.
    let lower = b"key-000000000010".to_vec();
    let wide = b"key-000000005000".to_vec();
    let (partial, _covered, payload2) =
        partial_data_block(frame, ends, 16, &cmp, &wide, Some(payload)).expect("resume grow");
    assert!(
        payload2.decoded_blocks > narrow_blocks,
        "resume must extend the decoded extent: {} -> {}",
        narrow_blocks,
        payload2.decoded_blocks,
    );

    // The grown block's range scan must equal the full block's.
    let mut fi = full.iter(cmp.clone());
    fi.seek(&lower, SeqNo::MAX);
    fi.seek_upper_exclusive(&wide, SeqNo::MAX);
    let reference: Vec<InternalValue> = fi.map(|x| x.materialize(full.as_slice())).collect();

    let mut pi = partial.iter(cmp.clone());
    pi.seek(&lower, SeqNo::MAX);
    pi.seek_upper_exclusive(&wide, SeqNo::MAX);
    let got: Vec<InternalValue> = pi.map(|x| x.materialize(partial.as_slice())).collect();

    assert_eq!(got, reference, "resume-grown range scan must equal full");
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
