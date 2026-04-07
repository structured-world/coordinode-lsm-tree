// Copyright (c) 2025-present, Structured World Foundation
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! Pure Rust zstd backend via the `structured-zstd` crate.
//!
//! This backend requires no C compiler or system libraries — it compiles
//! with `cargo build` alone.
//!
//! # Notes
//!
//! - Dictionary compression is supported via [`FrameCompressor::set_dictionary_from_bytes`].
//! - Dictionary decompression is supported.
//! - Decompression throughput is ~2–3.5x slower than the C reference.

use super::CompressionProvider;
use std::io::Read;

/// Read at most `capacity` bytes from `reader` into a pre-allocated buffer,
/// then probe for excess data. Returns the filled portion of the buffer.
///
/// The limit is enforced _during_ decode — the Vec never grows beyond
/// `capacity`, preventing unbounded allocation from crafted frames.
fn bounded_read(reader: &mut impl Read, capacity: usize) -> crate::Result<Vec<u8>> {
    let mut output = vec![0u8; capacity];
    let mut filled = 0;

    loop {
        let dest = output
            .get_mut(filled..)
            .ok_or(crate::Error::DecompressedSizeTooLarge {
                declared: filled as u64,
                limit: capacity as u64,
            })?;
        match reader.read(dest) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(crate::Error::Io(e)),
        }
    }

    // Probe for excess data: if the reader still has bytes after filling
    // the buffer, the frame exceeds capacity.
    let mut probe = [0u8; 1];
    match reader.read(&mut probe) {
        Ok(0) => {}
        Ok(_) => {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: (filled + 1) as u64,
                limit: capacity as u64,
            });
        }
        Err(e) => return Err(crate::Error::Io(e)),
    }

    output.truncate(filled);
    Ok(output)
}

/// Pure Rust zstd backend.
pub struct ZstdPureProvider;

impl CompressionProvider for ZstdPureProvider {
    fn compress(data: &[u8], level: i32) -> crate::Result<Vec<u8>> {
        let compressed = structured_zstd::encoding::compress_to_vec(
            std::io::Cursor::new(data),
            structured_zstd::encoding::CompressionLevel::from_level(level),
        );
        Ok(compressed)
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        let mut decoder = structured_zstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        bounded_read(&mut decoder, capacity)
    }

    fn compress_with_dict(data: &[u8], level: i32, dict_raw: &[u8]) -> crate::Result<Vec<u8>> {
        use structured_zstd::decoding::Dictionary;
        use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

        // `FrameCompressor::set_dictionary` accepts a parsed `Dictionary`.
        //
        // Two dictionary formats are supported:
        //
        // 1. **Finalized zstd dictionary** (magic `0x37A430EC` prefix): produced by
        //    `zstd --train` / `zstd::dict::from_continuous` and the C zstd library.
        //    Contains entropy tables (Huffman + FSE) that prime the compressor's
        //    coding state for better ratios. Parsed via `Dictionary::decode_dict`.
        //
        // 2. **Raw content dictionary** (no magic): a bare byte sequence used as
        //    LZ77 history to improve match distances on repetitive data. No entropy
        //    table seeding. Parsed via `Dictionary::from_raw_content`.
        //
        // The C backend's `Compressor::with_dictionary` transparently handles both
        // formats. We replicate this behaviour here so that `ZstdDictionary` values
        // created from raw training corpora (without a finalized header) also work.
        //
        // ID derivation for raw content dictionaries:
        //   - Use the lower 32 bits of the xxh3 hash of `dict_raw` (matching the
        //     formula in `ZstdDictionary::id()`), clamped to at least 1 because
        //     id=0 is rejected by `FrameCompressor::set_dictionary`.
        //   - Both compress and decompress derive the same ID from the same bytes,
        //     so the dict_id written into the frame header is consistent.
        const DICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

        let dictionary = if dict_raw.starts_with(&DICT_MAGIC) {
            Dictionary::decode_dict(dict_raw)
                .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
        } else {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "intentional: lower 32 bits of xxh3 as dict id"
            )]
            let id = {
                let h = xxhash_rust::xxh3::xxh3_64(dict_raw) as u32;
                h.max(1) // id=0 is invalid; collision probability is negligible
            };
            Dictionary::from_raw_content(id, dict_raw.to_vec())
                .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
        };

        let mut compressor = FrameCompressor::new(CompressionLevel::from_level(level));
        compressor
            .set_dictionary(dictionary)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        let mut output = Vec::new();
        compressor.set_source(std::io::Cursor::new(data));
        compressor.set_drain(&mut output);
        compressor.compress();

        Ok(output)
    }

    fn decompress_with_dict(
        data: &[u8],
        dict: &crate::compression::ZstdDictionary,
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        use structured_zstd::decoding::{Dictionary, FrameDecoder};

        // Thread-local `FrameDecoder` with the dictionary pre-loaded.
        //
        // Parsing a zstd dictionary involves building Huffman and FSE decoding
        // tables — expensive relative to per-block decompression for the small
        // 4–64 KiB blocks typical in LSM-trees. Caching the `FrameDecoder`
        // instance across calls amortises this cost: the dictionary is parsed
        // exactly once per thread, per distinct dictionary.
        //
        // Thread-local storage is appropriate because `FrameDecoder` is not
        // `Send` and each thread decompresses independently; no mutex is
        // needed. If the active dictionary changes (e.g. different table),
        // the decoder is re-initialised transparently.
        thread_local! {
            // Keyed by the full 64-bit xxh3 fingerprint (`dict.id64()`), not
            // the truncated 32-bit public fingerprint, to avoid decoder reuse
            // when two distinct dictionaries happen to share the same 32-bit
            // prefix. A 64-bit collision is 2^32× less likely than a 32-bit one.
            static TLS_DECODER: std::cell::RefCell<Option<(u64, FrameDecoder)>> =
                const { std::cell::RefCell::new(None) };
        }

        TLS_DECODER.with(|cell| {
            let mut state = cell.borrow_mut();

            // Re-initialise if this is the first call in this thread or if
            // the dictionary has changed (different id64 → different table).
            if !matches!(&*state, Some((id, _)) if *id == dict.id64()) {
                // Mirror the format-detection logic in `compress_with_dict`:
                // finalized dictionaries (magic `0x37A430EC`) are parsed with
                // `decode_dict`; raw content bytes use `from_raw_content` with
                // the same ID formula so the dict_id in the frame header matches.
                const DICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];
                let parsed = if dict.raw().starts_with(&DICT_MAGIC) {
                    Dictionary::decode_dict(dict.raw())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                } else {
                    // `dict.id()` already returns a normalized non-zero u32
                    // (lower 32 bits of xxh3, clamped to 1); use it directly.
                    Dictionary::from_raw_content(dict.id(), dict.raw().to_vec())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                };
                let mut decoder = FrameDecoder::new();
                decoder
                    .add_dict(parsed)
                    .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                *state = Some((dict.id64(), decoder));
            }

            let Some((_, decoder)) = state.as_mut() else {
                // Unreachable: the branch above always initialises `state`.
                return Err(crate::Error::Io(std::io::Error::other(
                    "TLS_DECODER unexpectedly empty after initialisation",
                )));
            };

            // `decode_all_to_vec` decodes the entire frame in one pass and
            // appends to `output`. The dictionary stored in `decoder.dicts`
            // is reused without re-parsing on every call.
            //
            // The `bounded_read` approach used by the non-dictionary path
            // (`decompress`) is not applicable here: `bounded_read` calls
            // `Read::read` in a loop, which requires the decoder to pull
            // compressed blocks lazily from an internal reader reference.
            // `StreamingDecoder` supports this (it holds a `&[u8]` reference);
            // `FrameDecoder` does not — it processes the full slice at once
            // and its `Read` impl returns 0 bytes after `init` if not used
            // together with `decode_all_to_vec`.
            //
            // The capacity limit is therefore enforced by a post-decode check
            // rather than during streaming. The allocation is bounded by the
            // frame content size: zstd frames embed the decompressed size in
            // their header, so allocations from crafted frames are bounded by
            // that declared size. If the declared size itself is maliciously
            // large, the post-decode check below returns `DecompressedSizeTooLarge`
            // before the data is used.
            let mut output = Vec::with_capacity(capacity);
            decoder.decode_all_to_vec(data, &mut output).map_err(|e| {
                // `decode_all_to_vec` uses the Vec's capacity as a hard
                // allocation limit. When the frame's decompressed content
                // would exceed that limit, it returns `TargetTooSmall`.
                // Normalise this to `DecompressedSizeTooLarge` for a
                // consistent error API with the C FFI backend.
                if matches!(
                    e,
                    structured_zstd::decoding::errors::FrameDecoderError::TargetTooSmall
                ) {
                    crate::Error::DecompressedSizeTooLarge {
                        declared: capacity as u64 + 1,
                        limit: capacity as u64,
                    }
                } else {
                    crate::Error::Io(std::io::Error::other(e))
                }
            })?;

            // Return an error if the frame decompressed to more bytes than
            // the caller declared. Matches the bounded behaviour of
            // `decompress()` and the C FFI backend.
            if output.len() > capacity {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: output.len() as u64,
                    limit: capacity as u64,
                });
            }

            Ok(output)
        })
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
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
        55, 164, 48, 236, 98, 64, 12, 7, 42, 16, 120, 62, 7, 204, 192, 51, 240, 12, 60, 3, 207,
        192, 51, 240, 12, 60, 3, 207, 192, 51, 24, 17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        128, 48, 165, 148, 2, 227, 76, 8, 33, 132, 16, 66, 136, 136, 136, 60, 84, 160, 64, 65, 65,
        65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65,
        193, 231, 162, 40, 138, 162, 40, 138, 162, 40, 165, 148, 82, 74, 169, 170, 234, 1, 100,
        160, 170, 193, 96, 48, 24, 12, 6, 131, 193, 96, 48, 12, 195, 48, 12, 195, 48, 12, 195, 48,
        198, 24, 99, 140, 153, 29, 1, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 3, 3, 3, 3, 3, 3, 3, 3, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2,
    ];

    const COMPRESSED: &[u8] = &[
        40, 181, 47, 253, 35, 98, 64, 12, 7, 35, 149, 0, 0, 96, 104, 101, 108, 108, 111, 32, 119,
        111, 114, 108, 100, 32, 1, 0, 175, 75, 18,
    ];

    const PLAINTEXT: &[u8] = b"hello world hello world hello world";

    #[test]
    fn decompress_with_dict_returns_correct_plaintext() {
        let dict = ZstdDictionary::new(DICT);
        let result = ZstdPureProvider::decompress_with_dict(COMPRESSED, &dict, PLAINTEXT.len() + 1)
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
            let result =
                ZstdPureProvider::decompress_with_dict(COMPRESSED, &dict, PLAINTEXT.len() + 1)
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
        let result = ZstdPureProvider::decompress_with_dict(COMPRESSED, &dict, too_small);
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

        let compressed = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, DICT)
            .expect("compression with dict should succeed");

        // The output must be a non-empty zstd frame.
        assert!(
            !compressed.is_empty(),
            "compressed output must not be empty"
        );

        let decompressed =
            ZstdPureProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
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
        let compressed = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, DICT)
            .expect("compression should succeed");

        assert!(
            compressed.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]),
            "output must start with zstd magic 0xFD2FB528 (LE); got {:?}",
            compressed.get(..4.min(compressed.len()))
        );
    }

    #[test]
    fn compress_with_dict_roundtrip_all_levels() {
        // Compression must round-trip correctly across the full valid level range.
        let dict = ZstdDictionary::new(DICT);

        for level in [1, 3, 9, 19] {
            let compressed =
                ZstdPureProvider::compress_with_dict(PLAINTEXT, level, DICT).expect("compress");

            let decompressed =
                ZstdPureProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
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
        let result = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, b"");
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

        let compressed = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, raw_content_dict)
            .expect("compression with raw content dict should succeed");

        let decompressed =
            ZstdPureProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
                .expect("decompression with raw content dict should succeed");

        assert_eq!(
            decompressed, PLAINTEXT,
            "round-trip with raw content dict must equal the original plaintext"
        );
    }

    #[test]
    fn compress_with_dict_empty_plaintext_roundtrips() {
        // Edge case: compressing an empty payload with a dictionary must round-trip.
        let dict = ZstdDictionary::new(DICT);

        let compressed = ZstdPureProvider::compress_with_dict(&[], 3, DICT)
            .expect("compression of empty payload should succeed");

        let decompressed = ZstdPureProvider::decompress_with_dict(&compressed, &dict, 1)
            .expect("decompression of empty payload should succeed");

        assert!(
            decompressed.is_empty(),
            "decompressed output of empty payload must be empty"
        );
    }
}
