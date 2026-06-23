use super::*;
use crate::compression::ZstdDictionary;
use test_log::test;

// Pre-generated test vectors for pure-Rust dict decompression.
//
// Generated with the `zstd` C library (crate v0.13, `zdict_builder` feature):
//   - Training corpus: 100 samples × 32 bytes (cycling pattern 0..4)
//   - Plaintext: b"hello world hello world hello world"
//
// Reproducible with:
//   zstd::dict::from_continuous(&training_data, &sizes, 1024)
//   zstd::bulk::Compressor::with_dictionary(3, &dict).compress(plaintext)
const DICT: &[u8] = &[
    55, 164, 48, 236, 98, 64, 12, 7, 42, 16, 120, 62, 7, 204, 192, 51, 240, 12, 60, 3, 207, 192,
    51, 240, 12, 60, 3, 207, 192, 51, 24, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 48,
    165, 148, 2, 227, 76, 8, 33, 132, 16, 66, 136, 136, 136, 60, 84, 160, 64, 65, 65, 65, 65, 65,
    65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 193, 231, 162,
    40, 138, 162, 40, 138, 162, 40, 165, 148, 82, 74, 169, 170, 234, 1, 100, 160, 170, 193, 96, 48,
    24, 12, 6, 131, 193, 96, 48, 12, 195, 48, 12, 195, 48, 12, 195, 48, 198, 24, 99, 140, 153, 29,
    1, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2,
    2, 2, 2, 2, 2, 2,
];

const COMPRESSED: &[u8] = &[
    40, 181, 47, 253, 35, 98, 64, 12, 7, 35, 149, 0, 0, 96, 104, 101, 108, 108, 111, 32, 119, 111,
    114, 108, 100, 32, 1, 0, 175, 75, 18,
];

const PLAINTEXT: &[u8] = b"hello world hello world hello world";

#[test]
fn decompress_with_dict_returns_correct_plaintext() {
    let dict = ZstdDictionary::new(DICT);
    let result = ZstdProvider::decompress_with_dict(COMPRESSED, &dict, PLAINTEXT.len() + 1)
        .expect("decompression should succeed");
    assert_eq!(
        result, PLAINTEXT,
        "decompressed output must equal the original plaintext"
    );
}

#[test]
fn decompress_with_dict_is_idempotent_across_repeated_calls() {
    let dict = ZstdDictionary::new(DICT);
    // Call three times to exercise the TLS caching path (second and third
    // calls must reuse the cached FrameDecoder without re-parsing the dict).
    for _ in 0..3 {
        let result = ZstdProvider::decompress_with_dict(COMPRESSED, &dict, PLAINTEXT.len() + 1)
            .expect("decompression should succeed on every call");
        assert_eq!(result, PLAINTEXT);
    }
}

#[test]
fn decompress_with_dict_rejects_frame_exceeding_capacity() {
    // Capacity smaller than the plaintext — should return an error, not
    // silently return truncated output (regression for the post-decode
    // capacity guard added to `decode_all_to_vec`).
    let dict = ZstdDictionary::new(DICT);
    let too_small = PLAINTEXT.len() / 2;
    let result = ZstdProvider::decompress_with_dict(COMPRESSED, &dict, too_small);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "expected DecompressedSizeTooLarge but got {result:?}",
    );
}

// --- compress_with_dict tests ---

#[test]
fn compress_with_dict_roundtrip_pure_to_pure() {
    // Verify the core contract: data compressed with the pure backend using
    // a dictionary must decompress back to the original plaintext using the
    // same pure backend.
    let dict = ZstdDictionary::new(DICT);

    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, DICT)
        .expect("compression with dict should succeed");

    // The output must be a non-empty zstd frame.
    assert!(
        !compressed.is_empty(),
        "compressed output must not be empty"
    );

    let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
        .expect("decompression with dict should succeed");

    assert_eq!(
        decompressed, PLAINTEXT,
        "round-tripped output must equal the original plaintext"
    );
}

#[test]
fn compress_with_dict_produces_zstd_magic() {
    // zstd frames always start with the little-endian magic number 0xFD2FB528
    // (bytes: 0x28, 0xB5, 0x2F, 0xFD). A mismatched magic means the frame is
    // corrupt or the output is not a valid zstd frame.
    let compressed =
        ZstdProvider::compress_with_dict(PLAINTEXT, 3, DICT).expect("compression should succeed");

    assert!(
        compressed.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]),
        "output must start with zstd magic 0xFD2FB528 (LE); got {:?}",
        compressed.get(..4.min(compressed.len()))
    );
}

#[test]
fn compress_with_dict_roundtrip_representative_levels() {
    // Compression must round-trip correctly at representative points across
    // the valid level range (1=fastest, 3=default, 9=balanced, 19=best).
    let dict = ZstdDictionary::new(DICT);

    for level in [1, 3, 9, 19] {
        let compressed =
            ZstdProvider::compress_with_dict(PLAINTEXT, level, DICT).expect("compress");

        let decompressed =
            ZstdProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
                .expect("decompress");

        assert_eq!(
            decompressed, PLAINTEXT,
            "round-trip failed at compression level={level}"
        );
    }
}

#[test]
fn compress_with_dict_empty_dict_returns_error() {
    // An empty dictionary slice must return an error because there is no
    // content to use as LZ77 history. Both the finalized-format path and
    // the raw-content path reject empty input.
    let result = ZstdProvider::compress_with_dict(PLAINTEXT, 3, b"");
    assert!(
        result.is_err(),
        "expected an error for empty dictionary, got Ok"
    );
}

#[test]
fn compress_with_dict_raw_content_dict_works() {
    // A raw byte sequence (no finalized-dict magic) must be accepted as a
    // raw content dictionary and produce a valid compressed frame.
    let raw_content_dict = b"this is raw content dictionary data for matching";
    let dict = ZstdDictionary::new(raw_content_dict);

    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_content_dict)
        .expect("compression with raw content dict should succeed");

    let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
        .expect("decompression with raw content dict should succeed");

    assert_eq!(
        decompressed, PLAINTEXT,
        "round-trip with raw content dict must equal the original plaintext"
    );
}

#[test]
fn raw_content_dict_substitution_rejected_by_inner_frame_gate() {
    // Defense-in-depth for the dict-substitution threat (#253): the
    // raw-content decode path applies the dictionary via `force_dict`, which
    // ignores the inner zstd frame's embedded Dictionary_ID. Without the
    // `expect_dict_id` gate, a frame body compressed under a DIFFERENT
    // dictionary (spliced in while the block header + AAD dict_id are kept
    // valid) would be decoded against the wrong tables and yield silent
    // garbage. The gate pins the frame id to the decoding dict, so the
    // substitution surfaces a typed error instead of wrong plaintext.
    let dict_a_raw = b"raw content dictionary ALPHA for the substitution test".to_vec();
    let dict_b_raw = b"raw content dictionary BRAVO for the substitution test".to_vec();
    let dict_a = ZstdDictionary::new(&dict_a_raw);
    let dict_b = ZstdDictionary::new(&dict_b_raw);
    assert_ne!(
        dict_a.id(),
        dict_b.id(),
        "distinct raw dicts must hash to distinct ids"
    );

    let payload = b"defense-in-depth payload bytes compressed under a raw dict";

    // Frame genuinely compressed under dict B (carries dict B's synthetic id).
    let frame_b =
        ZstdProvider::compress_with_dict(payload, 3, &dict_b_raw).expect("compress under B");

    // Matching dict B decodes cleanly (the gate's expectation == frame id).
    let ok = ZstdProvider::decompress_with_dict(&frame_b, &dict_b, payload.len() + 1)
        .expect("matching dict must decode");
    assert_eq!(ok, payload, "matching-dict round-trip must be exact");

    // Substitution: decode frame B against dict A. The inner-frame id (B)
    // diverges from the expectation (A) and the gate rejects it before
    // `force_dict` can apply A's tables to B's frame.
    let err = ZstdProvider::decompress_with_dict(&frame_b, &dict_a, payload.len() + 1)
        .expect_err("dict substitution must be rejected by the inner-frame gate");
    // Lock down the exact mismatch payload the gate emits, not just any
    // Decompress error: the id carried is the EXPECTED dict (the one we
    // decoded with), so an unrelated decompression failure would not match.
    let expected_dict_id = dict_a.id().max(1);
    assert!(
        matches!(
            err,
            crate::Error::Decompress(crate::CompressionType::ZstdDict { level: 0, dict_id })
                if dict_id == expected_dict_id
        ),
        "expected Decompress(ZstdDict {{ level: 0, dict_id: {expected_dict_id} }}), got {err:?}"
    );
}

#[test]
fn compress_with_dict_empty_plaintext_roundtrips() {
    // Edge case: compressing an empty payload with a dictionary must round-trip.
    let dict = ZstdDictionary::new(DICT);

    let compressed = ZstdProvider::compress_with_dict(&[], 3, DICT)
        .expect("compression of empty payload should succeed");

    let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, 1)
        .expect("decompression of empty payload should succeed");

    assert!(
        decompressed.is_empty(),
        "decompressed output of empty payload must be empty"
    );
}

#[test]
fn compress_with_dict_raw_content_empty_plaintext_roundtrips_at_capacity_one() {
    // Regression: empty plaintext with a raw-content (non-finalized) dictionary
    // must round-trip when capacity=1. The raw-content path goes through
    // `decode_raw_content_bounded`; this smoke test ensures it handles an empty
    // frame correctly (FCS=0, so the bomb-check is skipped before we enter the
    // loop).
    let raw_dict = b"raw content dictionary for empty payload smoke test";
    let dict = ZstdDictionary::new(raw_dict);

    let compressed = ZstdProvider::compress_with_dict(&[], 3, raw_dict)
        .expect("compression of empty payload with raw-content dict should succeed");

    let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, 1)
        .expect("decompression of empty payload with raw-content dict (capacity=1) should succeed");

    assert!(
        decompressed.is_empty(),
        "decompressed output of empty payload must be empty; got {decompressed:?}"
    );
}

#[test]
fn compress_with_dict_raw_content_empty_plaintext_roundtrips_at_exact_capacity() {
    // Regression: empty plaintext with a raw-content dictionary must succeed even
    // when capacity=0 (exact capacity for 0-byte output). The raw-content path
    // still enters block decoding for empty frames by calling `decode_blocks`
    // with `remaining.max(1)`, then relies on the post-collect size guard to
    // enforce the exact-capacity check after the frame is marked finished.
    let raw_dict = b"raw content dictionary for empty payload exact-capacity test";
    let dict = ZstdDictionary::new(raw_dict);

    let compressed = ZstdProvider::compress_with_dict(&[], 3, raw_dict)
        .expect("compression of empty payload with raw-content dict should succeed");

    let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, 0)
        .expect("decompression of empty payload with raw-content dict (capacity=0) should succeed");

    assert!(
        decompressed.is_empty(),
        "decompressed output of empty payload must be empty; got {decompressed:?}"
    );
}

#[test]
fn decompress_with_dict_raw_content_rejects_frame_exceeding_capacity() {
    // Raw-content dict path: the frame is produced by the pure backend with a
    // raw-content (non-finalized) dictionary. The decompressor must return
    // DecompressedSizeTooLarge when the capacity limit is smaller than the
    // plaintext — exercising the FCS pre-check in decompress_with_dict and
    // the decode_raw_content_bounded loop capacity guard.
    let raw_dict = b"this is raw content dictionary data for matching";

    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
        .expect("compression with raw content dict should succeed");

    let dict = ZstdDictionary::new(raw_dict);
    // Capacity set to half the plaintext length — frame decompresses to
    // more than this limit so the guard must fire.
    let too_small = PLAINTEXT.len() / 2;
    let result = ZstdProvider::decompress_with_dict(&compressed, &dict, too_small);

    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "raw-content path must return DecompressedSizeTooLarge when capacity < plaintext; got {result:?}",
    );
}

#[test]
fn decompress_with_dict_raw_content_rejects_zero_capacity_non_empty() {
    // Capacity=0 with non-empty plaintext must return an error immediately:
    // either the FCS pre-check rejects the frame up front, or the bounded
    // raw-content decode path hits its size/capacity guard when decoding the
    // first block for a zero-capacity output buffer.
    let raw_dict = b"raw content dict for zero-capacity test";

    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
        .expect("compression should succeed");

    let dict = ZstdDictionary::new(raw_dict);
    let result = ZstdProvider::decompress_with_dict(&compressed, &dict, 0);

    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "capacity=0 with non-empty frame must return DecompressedSizeTooLarge; got {result:?}",
    );
}

// --- bounded_read error-path tests ---

/// A reader that always returns an I/O error on the very first `read` call.
struct AlwaysFailReader;
impl std::io::Read for AlwaysFailReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("simulated read error"))
    }
}

/// A 3-state reader used to reach the probe-read error path in `bounded_read`.
///
/// States:
/// 1. `remaining > 0`: returns data bytes (fill loop sees `Ok(n)`).
/// 2. `remaining == 0, !eof_sent`: returns `Ok(0)` once → fill loop `break`s.
/// 3. `remaining == 0, eof_sent`: returns `Err` → probe read fires `Err` arm.
struct FailOnProbeReader {
    remaining: usize,
    eof_sent: bool,
}
impl std::io::Read for FailOnProbeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining > 0 {
            let n = buf.len().min(self.remaining);
            for b in buf.iter_mut().take(n) {
                *b = 0;
            }
            self.remaining -= n;
            Ok(n)
        } else if !self.eof_sent {
            self.eof_sent = true;
            Ok(0) // signals EOF → fill loop breaks
        } else {
            Err(std::io::Error::other("simulated probe-read error"))
        }
    }
}

#[test]
fn bounded_read_propagates_io_error_from_read_loop() {
    // `AlwaysFailReader` triggers the `Err(e) => return Err(Error::Io(e))`
    // arm inside the fill loop of `bounded_read` (the first `reader.read`
    // call returns an error).
    let mut reader = AlwaysFailReader;
    let result = bounded_read(&mut reader, 64);
    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "expected Io error from read loop; got {result:?}",
    );
}

#[test]
fn bounded_read_propagates_io_error_from_probe_read() {
    // `FailOnProbeReader` completes the fill loop normally (returns Ok(0) to
    // signal EOF), then returns Err on the subsequent probe read, exercising
    // the `Err(e) => return Err(Error::Io(e))` arm after the fill loop.
    let mut reader = FailOnProbeReader {
        remaining: 4,
        eof_sent: false,
    };
    let result = bounded_read(&mut reader, 64);
    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "expected Io error from probe read; got {result:?}",
    );
}

#[test]
fn decompress_with_dict_returns_error_on_corrupt_finalized_frame() {
    // Build a real compressed frame, then truncate it to force a decode
    // failure on the finalized-dict path.  Exercises the Io error branch in
    // do_decompress_with_dict when decode_all_to_vec fails.
    let dict = ZstdDictionary::new(DICT);
    let mut frame =
        ZstdProvider::compress_with_dict(PLAINTEXT, 3, DICT).expect("compression must succeed");
    frame.pop(); // truncate last byte → corrupt frame
    let result = ZstdProvider::decompress_with_dict(&frame, &dict, 1024);
    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "corrupt frame must return Err(Io(_)) on finalized dict path; got {result:?}",
    );
}

#[test]
fn decompress_with_dict_returns_error_on_corrupt_raw_content_frame() {
    // Build a real raw-content compressed frame, then truncate it to force a
    // failure on the raw-content path.  Exercises the init()/decode error
    // branch in do_decompress_with_dict.
    let raw_dict = b"some raw content dictionary bytes for testing corruption";
    let dict = ZstdDictionary::new(raw_dict);
    let mut frame =
        ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict).expect("compression must succeed");
    frame.pop(); // truncate last byte → corrupt frame
    let result = ZstdProvider::decompress_with_dict(&frame, &dict, 1024);
    assert!(
        matches!(result, Err(crate::Error::Io(_))),
        "corrupt frame must return Err(Io(_)) on raw-content dict path; got {result:?}",
    );
}

// --- decode_raw_content_bounded direct tests ---
//
// The post-decode `output.len() + can_collect() > capacity` size guard inside
// `decode_raw_content_bounded` is normally bypassed by the FCS pre-check in
// `do_decompress_with_dict`: if the frame embeds a content size that exceeds
// `capacity`, `do_decompress_with_dict` returns early before ever calling
// this helper. `decode_raw_content_bounded` always calls `decode_blocks` with
// `remaining.max(1)` — there is no `remaining == 0` early-return.
//
// To exercise the post-decode size guard directly, the tests below set up a
// `FrameDecoder` manually (mirroring `do_decompress_with_dict`) and call the
// private function directly, bypassing the FCS pre-check.

/// Compute the synthetic raw-content dict id used by both
/// `compress_with_dict` and `do_decompress_with_dict` for raw-content
/// (non-finalized) dictionaries.
#[expect(
    clippy::cast_possible_truncation,
    reason = "intentional: lower 32 bits of xxh3"
)]
fn raw_content_id(dict_raw: &[u8]) -> u32 {
    let h = xxhash_rust::xxh3::xxh3_64(dict_raw) as u32;
    h.max(1)
}

/// Build a `FrameDecoder` initialised with `dict_raw` for the raw-content
/// path, pointing `cursor` at the blocks region of `compressed`.
///
/// Mirrors the setup performed by `do_decompress_with_dict(is_raw_content=true)`.
fn make_raw_content_decoder<'a>(
    dict_raw: &[u8],
    compressed: &'a [u8],
    cursor: &mut std::io::Cursor<&'a [u8]>,
) -> structured_zstd::decoding::FrameDecoder {
    use structured_zstd::decoding::{Dictionary, FrameDecoder};
    let id = raw_content_id(dict_raw);
    let parsed = Dictionary::from_raw_content(id, dict_raw.to_vec())
        .expect("Dictionary::from_raw_content should succeed");
    let mut decoder = FrameDecoder::new();
    decoder.add_dict(parsed).expect("add_dict should succeed");
    *cursor = std::io::Cursor::new(compressed);
    decoder
        .init(cursor)
        .expect("FrameDecoder::init should succeed");
    decoder.force_dict(id).expect("force_dict should succeed");
    decoder
}

#[test]
fn decode_raw_content_bounded_remaining_zero_returns_error() {
    // capacity = 0 on the very first loop iteration.
    // The decoder still advances with `remaining.max(1)`, so it can decode
    // a block and report collected output. Once `can_collect()` becomes
    // non-zero, the `output.len() + can_collect() > capacity` guard returns
    // DecompressedSizeTooLarge.
    //
    // This path is unreachable through the high-level API because the FCS
    // pre-check in do_decompress_with_dict returns early first (frames
    // produced by compress_with_dict include the frame content size).
    let raw_dict = b"raw content dict for remaining-zero test";
    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
        .expect("compression should succeed");

    let mut cursor = std::io::Cursor::new(compressed.as_slice());
    let mut decoder = make_raw_content_decoder(raw_dict, &compressed, &mut cursor);

    let result = decode_raw_content_bounded(&mut decoder, &mut cursor, 0);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "capacity=0 must return DecompressedSizeTooLarge; got {result:?}",
    );
}

// --- compress_with_layout (#257 inner-block layout) tests ---

/// Build a realistic sorted-KV data-block payload of about `target` bytes:
/// monotonically increasing keys + short values, the shape a flushed
/// memtable serialises into a data block.
fn sorted_kv_payload(target: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(target + 64);
    let mut i = 0u64;
    while buf.len() < target {
        buf.extend_from_slice(format!("key-{i:012}").as_bytes());
        buf.push(0);
        buf.extend_from_slice(format!("value-{i:08}-payload").as_bytes());
        buf.push(0);
        i += 1;
    }
    buf
}

#[test]
fn compress_with_layout_single_inner_block_returns_empty_layout() {
    // A small (default-sized) block compresses to a single inner zstd
    // block: nothing to partial-decode, so the layout must be empty and no
    // per-block table is persisted by the caller.
    let payload = sorted_kv_payload(4 * 1024);
    let (frame, layout) =
        ZstdProvider::compress_with_layout(&payload, 3).expect("compress with layout");
    assert!(
        layout.is_empty(),
        "single-inner-block frame must yield an empty layout, got {layout:?}",
    );
    // The compressed bytes still round-trip.
    let back = ZstdProvider::decompress(&frame, payload.len() + 1).expect("decompress");
    assert_eq!(back, payload);
}

#[test]
fn compress_with_layout_multi_inner_block_offsets_are_monotonic_and_total() {
    // A large block at a high level pre-splits into several inner zstd
    // blocks. The layout must be strictly increasing cumulative ends whose
    // last entry equals the total decompressed size.
    let payload = sorted_kv_payload(256 * 1024);
    let (_frame, layout) =
        ZstdProvider::compress_with_layout(&payload, 19).expect("compress with layout");
    assert!(
        layout.len() >= 2,
        "256 KiB @ L19 must split into >= 2 inner blocks, got {} block(s)",
        layout.len(),
    );
    assert!(
        layout.windows(2).all(|w| w[0] < w[1]),
        "cumulative ends must be strictly increasing: {layout:?}",
    );
    assert_eq!(
        *layout.last().expect("non-empty layout") as usize,
        payload.len(),
        "last cumulative end must equal the total decompressed size",
    );
}

#[test]
fn compress_with_layout_subrange_partial_decode_matches_full_slice() {
    // The persisted layout must let a reader decode exactly the inner-block
    // subset and get bytes identical to the matching full-decode slice —
    // the core correctness contract of the partial-decode read path.
    use structured_zstd::decoding::FrameDecoder;

    let payload = sorted_kv_payload(256 * 1024);
    let (frame, layout) =
        ZstdProvider::compress_with_layout(&payload, 19).expect("compress with layout");
    let nblocks = layout.len();
    assert!(nblocks >= 2, "need a multi-block frame");

    // Decode the first inner block only, then assert it equals the prefix
    // of the full payload up to that block's cumulative end.
    let mut src = frame.as_slice();
    let mut dec = FrameDecoder::new();
    dec.reset(&mut src).expect("reset frame header");
    let pd = dec
        .decode_blocks_partial(&mut src, 0, 1, None, false)
        .expect("partial decode of first inner block");
    let want_end = layout[0] as usize;
    assert_eq!(
        pd.data.as_slice(),
        &payload[..want_end],
        "partial-decode [0,1) must equal the full-decode slice [0,{want_end})",
    );
}

#[test]
fn decode_raw_content_bounded_collected_exceeds_capacity_returns_error() {
    // capacity = 5 (smaller than PLAINTEXT = 35 bytes).
    // decode_blocks(UptoBytes(5)) decodes the block containing 35 bytes.
    // Since zstd blocks cannot be split, the decoder collects 35 bytes
    // even though only 5 were requested.  0 + 35 > 5 fires the
    // `can > capacity` guard, returning DecompressedSizeTooLarge.
    //
    // This path is unreachable through the high-level API for the same
    // reason as the test above.
    let raw_dict = b"raw content dict for can-exceeds-capacity test";
    let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
        .expect("compression should succeed");

    let mut cursor = std::io::Cursor::new(compressed.as_slice());
    let mut decoder = make_raw_content_decoder(raw_dict, &compressed, &mut cursor);

    let result = decode_raw_content_bounded(&mut decoder, &mut cursor, 5);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "capacity < plaintext must return DecompressedSizeTooLarge; got {result:?}",
    );
}

#[test]
fn decompress_into_rejects_frame_larger_than_dest() {
    let raw = vec![7u8; 4096];
    let frame = ZstdProvider::compress(&raw, 3).expect("compress");
    // dest is smaller than the frame's output → the excess probe fires.
    let mut dest = vec![0u8; 2048];
    let result = ZstdProvider::decompress_into(&frame, &mut dest);
    assert!(
        matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
        "a frame that decodes past dest must be rejected; got {result:?}",
    );
}

#[test]
fn decompress_into_fills_exact_buffer() {
    let raw = vec![3u8; 4096];
    let frame = ZstdProvider::compress(&raw, 3).expect("compress");
    let mut dest = vec![0u8; raw.len()];
    let written = ZstdProvider::decompress_into(&frame, &mut dest).expect("decompress_into");
    assert_eq!(written, raw.len());
    assert_eq!(dest, raw);
}

#[test]
fn decompress_into_propagates_decode_error() {
    let mut dest = vec![0u8; 64];
    let result = ZstdProvider::decompress_into(b"definitely not a valid zstd frame", &mut dest);
    assert!(result.is_err(), "a corrupt frame must error");
}
