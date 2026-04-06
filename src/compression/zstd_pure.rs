// Copyright (c) 2025-present, Structured World Foundation
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! Pure Rust zstd backend via the `structured-zstd` crate.
//!
//! This backend requires no C compiler or system libraries — it compiles
//! with `cargo build` alone.
//!
//! # Limitations
//!
//! - Dictionary compression is not yet supported (returns an error).
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

    fn compress_with_dict(_data: &[u8], _level: i32, _dict_raw: &[u8]) -> crate::Result<Vec<u8>> {
        Err(crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "zstd dictionary compression is not yet supported by the pure Rust backend \
             (structured-zstd); use the `zstd` feature for dictionary compression",
        )))
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
                let parsed = Dictionary::decode_dict(dict.raw())
                    .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
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

            // `decode_all_to_vec` calls `init(&mut input)` internally — the
            // mutable borrow advances the slice cursor past the frame header
            // before block decoding begins, which is the correct usage of
            // `FrameDecoder`. The dictionary stored in `decoder.dicts` is
            // reused on every call without re-parsing.
            let mut output = Vec::with_capacity(capacity);
            decoder
                .decode_all_to_vec(data, &mut output)
                .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

            // Enforce the same capacity limit as `decompress` and the C backend:
            // reject frames that decompress to more bytes than the caller declared.
            // `decode_all_to_vec` allocates without a hard cap, so this post-decode
            // check prevents unbounded allocation on corrupted or crafted frames.
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
#[expect(clippy::unwrap_used, reason = "test code")]
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
            result.is_err(),
            "expected DecompressedSizeTooLarge but got Ok({:?})",
            result.unwrap()
        );
    }
}
