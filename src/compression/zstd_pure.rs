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
//! - Dictionary compression is supported when configured with a zstd dictionary.
//! - Dictionary decompression is supported.
//! - Decompression throughput is ~2–3.5x slower than the C reference.

// When both `zstd` (C FFI) and `zstd-pure` features are enabled, the C FFI
// backend is selected as `ZstdBackend` and items in this module are not
// referenced from production code paths. They remain compiled so that
// `cargo clippy --all-features` and cross-backend integration tests can
// exercise them. `#[allow]` is used instead of `#[expect]` because
// `#[expect]` on a module declaration does not count inner-item diagnostics
// as fulfilling the expectation — only a direct lint on the declaration itself
// would satisfy it, which never fires here.
#![cfg_attr(all(feature = "zstd-pure", feature = "zstd"), allow(dead_code))]

use super::CompressionProvider;
use std::io::Read;

/// Zstd finalized dictionary magic number (little-endian `0x37A4_30EC`).
///
/// A dictionary blob that begins with these four bytes is a fully trained,
/// finalized zstd dictionary containing entropy tables and must be parsed
/// with [`Dictionary::decode_dict`]. A blob without this prefix is treated
/// as raw content and is loaded via [`Dictionary::from_raw_content`].
const DICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

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

/// Strips the `Dict_ID` field from a zstd frame header.
///
/// structured-zstd rejects `id=0` for [`Dictionary::from_raw_content`], so the
/// pure backend internally assigns a synthetic non-zero id derived from the dict
/// content hash. That id is an implementation detail — it must not be embedded in
/// compressed frames because the C zstd library always records `dictID=0` (absent)
/// for raw-content dicts, and its decompressor rejects frames whose `dictID`
/// doesn't match the loaded `ZSTD_DDict`'s id.
///
/// Stripping the field aligns the output format: both backends produce frames with
/// no embedded dict id for raw-content dicts, enabling mutual cross-backend
/// decompression.
///
/// Returns the frame unchanged if no dict id is present (`Dict_ID_Flag == 0`).
fn strip_dict_id(frame: Vec<u8>) -> Vec<u8> {
    // Minimum valid frame: magic (4 bytes) + Frame_Header_Descriptor (1 byte).
    let Some(&fhd) = frame.get(4) else {
        return frame; // Frame_Header_Descriptor absent — leave unchanged.
    };

    let dict_id_flag = fhd & 0x03; // bits [1:0]: Dict_ID_Flag
    if dict_id_flag == 0 {
        return frame; // No dict ID present.
    }

    // Dict_ID_Flag encodes the byte-width of the Dict_ID field:
    //   1 → 1 byte, 2 → 2 bytes, 3 → 4 bytes.
    let dict_id_len: usize = match dict_id_flag {
        1 => 1,
        2 => 2,
        3 => 4,
        _ => return frame, // unreachable: dict_id_flag is 2 bits
    };

    // Single_Segment_Flag (FHD bit 5): when set, no Window_Descriptor follows FHD.
    let single_segment = (fhd >> 5) & 0x01 != 0;
    let wd_len: usize = usize::from(!single_segment); // 1 if WD present, else 0

    // Dict_ID immediately follows: magic(4) + FHD(1) + optional WD.
    let dict_id_start = 5 + wd_len;
    if frame.len() < dict_id_start + dict_id_len {
        return frame; // Malformed frame — leave unchanged; decompressor will reject it.
    }

    // Build a new frame with Dict_ID_Flag cleared and the Dict_ID bytes removed.
    let new_fhd = fhd & !0x03u8; // Clear bits [1:0]
    let mut out = Vec::with_capacity(frame.len() - dict_id_len);
    if let Some(magic) = frame.get(..4) {
        out.extend_from_slice(magic); // Frame magic
    }
    out.push(new_fhd); // Modified FHD
    if !single_segment {
        // Window_Descriptor is present at byte 5 when !single_segment.
        // frame.len() >= dict_id_start + dict_id_len >= 7 when !single_segment (wd_len=1).
        if let Some(&wd) = frame.get(5) {
            out.push(wd);
        }
    }
    // Skip the Dict_ID bytes; copy the rest (FCS + blocks + optional checksum).
    if let Some(rest) = frame.get(dict_id_start + dict_id_len..) {
        out.extend_from_slice(rest);
    }
    out
}

/// Incrementally decodes `data` using the pre-initialised `decoder` and
/// collects the output, enforcing a hard `capacity` limit even when the frame
/// has no `Frame_Content_Size` field (FCS omitted → `content_size()` == 0).
///
/// Unlike `decode_blocks(All)`, which fills the decoder's internal buffer
/// without bound before the caller can check the size, this uses
/// `BlockDecodingStrategy::UptoBytes(remaining)` to stop decoding once the
/// internal buffer reaches the remaining capacity budget, then drains
/// collectible bytes into the output vector before the next iteration.
fn decode_raw_content_bounded(
    decoder: &mut structured_zstd::decoding::FrameDecoder,
    cursor: &mut std::io::Cursor<&[u8]>,
    capacity: usize,
) -> crate::Result<Vec<u8>> {
    use structured_zstd::decoding::BlockDecodingStrategy;

    let mut output: Vec<u8> = Vec::new();
    loop {
        let remaining = capacity.saturating_sub(output.len());

        if !decoder.is_finished() {
            if remaining == 0 {
                // Frame is still producing data but the budget is exhausted.
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: capacity as u64 + 1,
                    limit: capacity as u64,
                });
            }
            decoder
                .decode_blocks(&mut *cursor, BlockDecodingStrategy::UptoBytes(remaining))
                .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        }

        // Drain collectible bytes from the decoder's internal buffer.
        let can = decoder.can_collect();
        if can > 0 {
            if output.len() + can > capacity {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: (output.len() + can) as u64,
                    limit: capacity as u64,
                });
            }
            let prev_len = output.len();
            output.resize(prev_len + can, 0u8);
            let dest = output.get_mut(prev_len..).ok_or_else(|| {
                crate::Error::Io(std::io::Error::other(
                    "FrameDecoder drain buffer slice out of bounds",
                ))
            })?;
            // `read_exact` ensures all `can` bytes are drained in one call.
            // `Read::read` may do short reads, which would leave zero-filled
            // slack and corrupt capacity accounting on the next iteration.
            decoder.read_exact(dest).map_err(crate::Error::Io)?;
        }

        if decoder.is_finished() && decoder.can_collect() == 0 {
            break;
        }
    }
    Ok(output)
}

/// Executes the actual decompression once the `decoder` has been initialised
/// with the correct dictionary.
///
/// Separated from the `TLS_DECODER.with` closure so that LLVM's coverage
/// instrumentation can attribute lines to this named function (closure bodies
/// are not always attributed correctly by `llvm-cov`).
///
/// Dispatches to the appropriate path based on `is_raw_content`:
/// - `true` → manual `init` + `force_dict` + [`decode_raw_content_bounded`]
/// - `false` → `decode_all_to_vec` (finalized dict, embedded dictID)
fn do_decompress_with_dict(
    decoder: &mut structured_zstd::decoding::FrameDecoder,
    data: &[u8],
    dict_raw: &[u8],
    capacity: usize,
    is_raw_content: bool,
) -> crate::Result<Vec<u8>> {
    if is_raw_content {
        // Raw-content dict path: frames have no embedded dictID
        // (C API and post-strip pure frames both produce dictID=0).
        // `decode_all_to_vec` calls `init` internally, which would
        // skip dict loading for dictID=0. Instead, use the manual
        // flow: `init` → `force_dict` → `decode_blocks` → `collect`.
        //
        // `force_dict` loads the dict unconditionally regardless of
        // the frame's dictID field, handling both backends uniformly:
        //   - Frame produced by C FFI backend (dictID=0 → no id): force_dict loads dict.
        //   - Frame produced by new pure backend (dictID stripped → no id): same.
        //   - Frame produced by old pure backend (dictID=synthetic): force_dict
        //     re-loads same dict (idempotent, since init would already load it).
        let mut cursor = std::io::Cursor::new(data);
        decoder
            .init(&mut cursor)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        // Decompression-bomb guard: if the frame header declares a
        // content size larger than `capacity`, reject before allocating
        // the output buffer. `content_size()` returns 0 when the
        // frame omits the FCS field (size unknown); in that case the
        // post-decode check on `output.len()` below acts as fallback.
        let declared_size = decoder.content_size();
        if declared_size > 0 && declared_size > capacity as u64 {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: declared_size,
                limit: capacity as u64,
            });
        }

        // Derive the same synthetic id used in `add_dict` above.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "intentional: lower 32 bits of xxh3 as internal dict id"
        )]
        let raw_content_id = {
            let h = xxhash_rust::xxh3::xxh3_64(dict_raw) as u32;
            h.max(1)
        };
        decoder
            .force_dict(raw_content_id)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        decode_raw_content_bounded(decoder, &mut cursor, capacity)
    } else {
        // Finalized dict path: the frame embeds the dictID from the
        // dict header; `decode_all_to_vec` → `init` loads the matching
        // dict automatically via the standard dictID lookup.
        //
        // The capacity limit is enforced by `decode_all_to_vec`: it
        // pre-allocates exactly `capacity` bytes (via `Vec::with_capacity`)
        // and `TargetTooSmall` is returned if the frame exceeds that.
        let mut output = Vec::with_capacity(capacity);
        decoder.decode_all_to_vec(data, &mut output).map_err(|e| {
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
        if output.len() > capacity {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: output.len() as u64,
                limit: capacity as u64,
            });
        }
        Ok(output)
    }
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
        use structured_zstd::encoding::{CompressionLevel, FrameCompressor, MatchGeneratorDriver};

        // Thread-local `FrameCompressor` with the dictionary pre-loaded.
        //
        // Parsing a zstd dictionary (especially a finalized one with entropy tables)
        // is expensive relative to per-block compression of 4–64 KiB LSM blocks.
        // This single-entry cache amortises that cost: if the (dict_content, level)
        // pair matches the stored entry the compressor is reused as-is; otherwise
        // the entry is replaced (old dictionary evicted, new one parsed).
        //
        // In practice LSM-tree workloads use one dictionary per level, so the
        // same (dict, level) pair recurs on every block write — the cache
        // almost always hits.
        //
        // `FrameCompressor::compress()` resets internal state at the start of
        // each call (matcher reset, offset history `[1, 4, 8]`) and then re-primes
        // from the stored dictionary, so re-using the compressor across calls is safe.
        //
        // Cache key: (xxh3_64(dict_raw), level). The full 64-bit hash avoids
        // false cache hits when two distinct dictionaries share the same 32-bit
        // truncation.
        //
        // Source type `Cursor<Vec<u8>>`: TLS requires `'static` bounds, so the
        // source must be owned. This costs one O(data.len()) copy per call, which
        // is negligible compared to the dictionary-parsing savings.
        type CachedCompressor =
            FrameCompressor<std::io::Cursor<Vec<u8>>, Vec<u8>, MatchGeneratorDriver>;
        thread_local! {
            static TLS_COMPRESSOR: std::cell::RefCell<Option<(u64, i32, CachedCompressor)>> =
                const { std::cell::RefCell::new(None) };
        }

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
        // Whether the dictionary uses the finalized-dict format (magic header)
        // or raw content. Only raw-content dict frames need post-processing
        // to strip the embedded dictID (see below).
        let is_raw_content = !dict_raw.starts_with(&DICT_MAGIC);

        // ID derivation for raw content dictionaries:
        //   - Use the lower 32 bits of the xxh3 hash of `dict_raw`, clamped
        //     to at least 1. (id=0 is rejected by `FrameCompressor::set_dictionary`
        //     in structured-zstd.) This id is used INTERNALLY only — it is
        //     stripped from the frame output before returning (see `strip_dict_id`).
        //   - Stripping the id makes the frame format match the C API convention
        //     (dictID=0 / absent for raw-content), enabling cross-backend
        //     decompression (pure → C FFI and vice versa).
        let dict_key = xxhash_rust::xxh3::xxh3_64(dict_raw);

        TLS_COMPRESSOR.with(|cell| {
            let mut state = cell.borrow_mut();

            // Re-initialise if this is the first call in this thread or if the
            // dictionary or compression level has changed.
            if !matches!(&*state, Some((k, l, _)) if *k == dict_key && *l == level) {
                let dictionary = if dict_raw.starts_with(&DICT_MAGIC) {
                    Dictionary::decode_dict(dict_raw)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                } else {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "intentional: lower 32 bits of xxh3 as internal dict id"
                    )]
                    let id = {
                        let h = dict_key as u32;
                        h.max(1) // id=0 is rejected by set_dictionary; internal use only
                    };
                    Dictionary::from_raw_content(id, dict_raw.to_vec())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                };

                let mut compressor = FrameCompressor::new(CompressionLevel::from_level(level));
                compressor
                    .set_dictionary(dictionary)
                    .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                *state = Some((dict_key, level, compressor));
            }

            let Some((_, _, compressor)) = state.as_mut() else {
                // Unreachable: the branch above always initialises `state`.
                return Err(crate::Error::Io(std::io::Error::other(
                    "TLS_COMPRESSOR unexpectedly empty after initialisation",
                )));
            };

            // `compress()` resets the matcher and offset history at the start of
            // each call and then re-primes from the stored dictionary, so the same
            // `FrameCompressor` instance can safely be re-used across blocks.
            //
            // Source buffer: after `compress()` the exhausted `Cursor<Vec<u8>>`
            // remains in the compressor (position == len, Vec capacity intact).
            // Recover it with `take_source()`, clear, and refill to reuse the
            // allocation on subsequent calls instead of cloning `data` each time.
            //
            // Drain buffer: the filled `Vec<u8>` is returned to the caller via
            // `take_drain()`, so its capacity cannot be recovered for the next
            // call without an extra copy (which would negate the saving). Using
            // `Vec::new()` is allocation-free at construction; the allocator
            // recycles same-size blocks in practice on the hot path.
            let src_buf = compressor.take_source().map_or_else(
                || data.to_vec(),
                |c| {
                    let mut v = c.into_inner();
                    v.clear();
                    v.extend_from_slice(data);
                    v
                },
            );
            compressor.set_source(std::io::Cursor::new(src_buf));
            compressor.set_drain(Vec::new());
            compressor.compress();

            let compressed = compressor.take_drain().ok_or_else(|| {
                crate::Error::Io(std::io::Error::other("drain missing after compress"))
            })?;

            // For raw-content dicts the pure backend internally assigns a
            // synthetic non-zero dictID (structured-zstd rejects id=0).
            // That ID is an implementation detail: strip it from the frame
            // header so the output matches the C API convention of recording
            // dictID=0 for raw-content dicts. This makes frames produced here
            // decompressible by the C FFI backend and vice versa.
            Ok(if is_raw_content {
                strip_dict_id(compressed)
            } else {
                compressed
            })
        })
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
        // 4–64 KiB blocks typical in LSM-trees. This single-entry cache
        // amortises that cost: if the active dictionary (identified by its
        // 64-bit xxh3 fingerprint) matches the stored entry the decoder is
        // reused; otherwise the entry is replaced.
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

        // For raw-content dicts the compressed frame has no embedded dictID
        // (stripped by `compress_with_dict`; the C FFI backend also omits it).
        // `FrameDecoder::init` treats a missing or zero dictID as "no dict
        // required" and skips dict lookup. We use `force_dict` after `init`
        // to load the dict unconditionally for raw-content frames.
        //
        // For finalized dicts the frame embeds the dictID from the dict header;
        // `init` loads the matching dict automatically. `decode_all_to_vec`
        // handles this via the standard path.
        let is_raw_content = !dict.raw().starts_with(&DICT_MAGIC);

        TLS_DECODER.with(|cell| {
            let mut state = cell.borrow_mut();

            // Re-initialise if this is the first call in this thread or if
            // the dictionary has changed (different id64 → different table).
            if !matches!(&*state, Some((id, _)) if *id == dict.id64()) {
                // Mirror the format-detection logic in `compress_with_dict`:
                // finalized dictionaries (magic `0x37A430EC`) are parsed with
                // `decode_dict`; raw content bytes use `from_raw_content` with
                // the same synthetic id formula as `compress_with_dict` so that
                // `force_dict` can locate the dict in the internal dicts map.
                let parsed = if dict.raw().starts_with(&DICT_MAGIC) {
                    Dictionary::decode_dict(dict.raw())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                } else {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "intentional: lower 32 bits of xxh3 as internal dict id"
                    )]
                    let raw_content_id = {
                        let h = xxhash_rust::xxh3::xxh3_64(dict.raw()) as u32;
                        h.max(1) // id=0 is rejected; used internally for force_dict keying
                    };
                    Dictionary::from_raw_content(raw_content_id, dict.raw().to_vec())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                };
                let mut decoder = FrameDecoder::new();
                decoder
                    .add_dict(parsed)
                    .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                *state = Some((dict.id64(), decoder));
            }

            // Unreachable: the branch above always initialises `state`.
            let Some((_, decoder)) = state.as_mut() else {
                unreachable!("TLS_DECODER always initialised above");
            };

            do_decompress_with_dict(decoder, data, dict.raw(), capacity, is_raw_content)
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

    #[test]
    fn decompress_with_dict_raw_content_rejects_frame_exceeding_capacity() {
        // Raw-content dict path: the frame is produced by the pure backend with a
        // raw-content (non-finalized) dictionary. The decompressor must return
        // DecompressedSizeTooLarge when the capacity limit is smaller than the
        // plaintext — exercising the FCS pre-check in decompress_with_dict and
        // the decode_raw_content_bounded loop capacity guard.
        let raw_dict = b"this is raw content dictionary data for matching";

        let compressed = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
            .expect("compression with raw content dict should succeed");

        let dict = ZstdDictionary::new(raw_dict);
        // Capacity set to half the plaintext length — frame decompresses to
        // more than this limit so the guard must fire.
        let too_small = PLAINTEXT.len() / 2;
        let result = ZstdPureProvider::decompress_with_dict(&compressed, &dict, too_small);

        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "raw-content path must return DecompressedSizeTooLarge when capacity < plaintext; got {result:?}",
        );
    }

    #[test]
    fn decompress_with_dict_raw_content_rejects_zero_capacity_non_empty() {
        // Capacity=0 with non-empty plaintext must return an error immediately:
        // the FCS pre-check (if FCS present) or the remaining==0 branch inside
        // decode_raw_content_bounded catches this before any allocation.
        let raw_dict = b"raw content dict for zero-capacity test";

        let compressed = ZstdPureProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
            .expect("compression should succeed");

        let dict = ZstdDictionary::new(raw_dict);
        let result = ZstdPureProvider::decompress_with_dict(&compressed, &dict, 0);

        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "capacity=0 with non-empty frame must return DecompressedSizeTooLarge; got {result:?}",
        );
    }

    // --- strip_dict_id unit tests ---
    //
    // These tests call `strip_dict_id` directly with crafted frame byte sequences
    // to cover branches that are unreachable via normal compress_with_dict calls
    // (e.g., frames that are too short, have no dict_id, or use narrow dict_id
    // encodings). `strip_dict_id` only reads the header region, so the crafted
    // frames do not need valid block data.

    #[test]
    fn strip_dict_id_unchanged_if_frame_too_short() {
        // Frames shorter than 5 bytes cannot contain a Frame_Header_Descriptor —
        // the function must return the input unchanged.
        let short = vec![0x28_u8, 0xB5, 0x2F];
        assert_eq!(strip_dict_id(short.clone()), short);
    }

    #[test]
    fn strip_dict_id_unchanged_if_no_dict_id_flag() {
        // FHD = 0x20: single_segment=1 (bit5), dict_id_flag=0 (bits[1:0]).
        // No Dict_ID field present — frame must be returned unchanged.
        let frame = vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x20, 0xAA];
        assert_eq!(strip_dict_id(frame.clone()), frame);
    }

    #[test]
    fn strip_dict_id_removes_1byte_dict_id() {
        // FHD = 0x21: single_segment=1, dict_id_flag=1 → 1-byte Dict_ID.
        // Frame layout: magic(4) + FHD(1) + dict_id(1) + rest(1) = 7 bytes.
        let frame = vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x21, 0x42, 0xFF];
        let stripped = strip_dict_id(frame);
        // Expect: magic + FHD (bits[1:0] cleared) + rest; dict_id byte removed.
        assert_eq!(stripped, vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x20, 0xFF]);
    }

    #[test]
    fn strip_dict_id_removes_2byte_dict_id() {
        // FHD = 0x22: single_segment=1, dict_id_flag=2 → 2-byte Dict_ID.
        // Frame layout: magic(4) + FHD(1) + dict_id(2) + rest(1) = 8 bytes.
        let frame = vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x22, 0x01, 0x02, 0xFF];
        let stripped = strip_dict_id(frame);
        // Expect: magic + FHD (bits[1:0] cleared) + rest; 2 dict_id bytes removed.
        assert_eq!(stripped, vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x20, 0xFF]);
    }

    #[test]
    fn strip_dict_id_unchanged_if_malformed_too_short_for_dict_id() {
        // FHD = 0x21 claims a 1-byte dict_id at byte 5, but only 5 bytes total —
        // frame is malformed and must be returned unchanged.
        let frame = vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x21]; // only 5 bytes
        assert_eq!(strip_dict_id(frame.clone()), frame);
    }

    #[test]
    fn strip_dict_id_preserves_window_descriptor_in_multi_segment_frame() {
        // FHD = 0x03: single_segment=0 (bit5 clear), dict_id_flag=3 → 4-byte Dict_ID.
        // Frame layout: magic(4) + FHD(1) + WD(1) + dict_id(4) + rest(1) = 11 bytes.
        let frame = vec![
            0x28_u8, 0xB5, 0x2F, 0xFD, // magic
            0x03, // FHD: single_segment=0, dict_id_flag=3
            0x70, // Window_Descriptor
            0x01, 0x02, 0x03, 0x04, // 4-byte Dict_ID
            0xFF, // rest
        ];
        let stripped = strip_dict_id(frame);
        // Expect: magic + FHD (bits[1:0] cleared to 0x00) + WD (preserved) + rest.
        assert_eq!(stripped, vec![0x28_u8, 0xB5, 0x2F, 0xFD, 0x00, 0x70, 0xFF]);
    }

    #[test]
    fn decompress_with_dict_returns_error_on_corrupt_finalized_frame() {
        // Corrupt input must return an error on the finalized dict path.
        // Exercises the Io error branch in do_decompress_with_dict when
        // decode_all_to_vec returns a non-TargetTooSmall decode failure.
        let dict = ZstdDictionary::new(DICT);
        let corrupt = b"not a valid zstd frame at all";
        let result = ZstdPureProvider::decompress_with_dict(corrupt, &dict, 1024);
        assert!(
            result.is_err(),
            "corrupt frame must return an error on finalized dict path; got {result:?}",
        );
    }

    #[test]
    fn decompress_with_dict_returns_error_on_corrupt_raw_content_frame() {
        // Corrupt input must return an error on the raw-content dict path.
        // Exercises the init() error branch in do_decompress_with_dict.
        let raw_dict = b"some raw content dictionary bytes";
        let dict = ZstdDictionary::new(raw_dict);
        let corrupt = b"not a valid zstd frame at all";
        let result = ZstdPureProvider::decompress_with_dict(corrupt, &dict, 1024);
        assert!(
            result.is_err(),
            "corrupt frame must return an error on raw-content dict path; got {result:?}",
        );
    }
}
