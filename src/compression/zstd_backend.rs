// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, Structured World Foundation

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

use super::CompressionProvider;
use std::io::Read;

/// Zstd finalized dictionary magic number (bytes `37 A4 30 EC`,
/// little-endian `0xEC30_A437`).
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
            // Use `remaining.max(1)` so the decoder advances past empty frames even
            // when `capacity == 0` (remaining would be 0 before any blocks are
            // decoded). For non-empty frames `can_collect()` will be > 0 after this
            // call and the size guard below rejects them; for empty frames the
            // decoder marks itself finished with 0 bytes collectible.
            //
            // Note: `UptoBytes(N)` is a best-effort hint. The decoder may decode
            // one additional zstd block (≤ 128 KiB by the zstd standard) into its
            // internal buffer before returning, so `can_collect()` can transiently
            // exceed `remaining` by up to one block. The `new_len > capacity` guard
            // below immediately rejects such frames and frees the buffer. This is
            // acceptable because this path only decompresses data from SST files
            // that have already passed checksum verification — adversarial frames
            // are not a threat model concern here. FCS-less frames produced by
            // trusted writers are handled by the post-decode check alone.
            decoder
                .decode_blocks(
                    &mut *cursor,
                    BlockDecodingStrategy::UptoBytes(remaining.max(1)),
                )
                .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        }

        // Drain collectible bytes from the decoder's internal buffer.
        let can = decoder.can_collect();
        if can > 0 {
            // Use checked_add to avoid usize overflow when output.len() or can
            // are near usize::MAX.  Overflow is treated as a limit violation.
            let new_len =
                output
                    .len()
                    .checked_add(can)
                    .ok_or(crate::Error::DecompressedSizeTooLarge {
                        declared: u64::MAX,
                        limit: capacity as u64,
                    })?;
            if new_len > capacity {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: new_len as u64,
                    limit: capacity as u64,
                });
            }
            let prev_len = output.len();
            output.resize(new_len, 0u8);
            // Invariant: output was just resized to new_len (= prev_len + can), so
            // prev_len.. is always a valid slice. This cannot fail.
            let dest = output
                .get_mut(prev_len..)
                .unwrap_or_else(|| unreachable!("output resized to new_len above"));
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
    // Pre-computed synthetic id for the raw-content path: `dict.id().max(1)`.
    // Unused on the finalized-dict path (`is_raw_content = false`).
    raw_content_id: u32,
    capacity: usize,
    is_raw_content: bool,
) -> crate::Result<Vec<u8>> {
    if is_raw_content {
        // Raw-content dict path. `FrameDecoder::init` does not
        // auto-resolve a raw-content dictionary, so drive the decode
        // manually: `init` → `force_dict` → `decode_blocks` → `collect`.
        //
        // `force_dict` applies the dictionary regardless of whether the
        // frame header carries a dictID, so one path covers both on-disk
        // shapes:
        //   - newer frames that keep the synthetic xxh3 dictID in the
        //     header, and
        //   - older frames written before the id was retained (header
        //     omits it).
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

        // `raw_content_id` was computed by the caller from `dict.id().max(1)`,
        // reusing the cached xxh3 fingerprint without re-hashing dict_raw.
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
        // `decode_all_to_vec` returns `TargetTooSmall` if the frame would exceed
        // `Vec::with_capacity(capacity)`, so reaching here guarantees
        // output.len() <= capacity. A post-decode size assert is therefore
        // structurally unreachable and is omitted to keep coverage clean.
        Ok(output)
    }
}

/// Extract the inner-block layout (cumulative decompressed END offsets) from a
/// just-emitted frame's [`FrameEmitInfo`].
///
/// Returns an empty `Vec` when the frame has fewer than two inner blocks
/// (nothing to partial-decode), when the encoder did not capture layout
/// (`None`), or when any cumulative offset would not fit `u32` (the per-block
/// table is then skipped and the reader falls back to full decode — never a
/// correctness risk, only a missed optimisation).
fn inner_block_layout(
    info: Option<&structured_zstd::encoding::frame_emit_info::FrameEmitInfo>,
) -> Vec<u32> {
    let Some(info) = info else { return Vec::new() };
    let n = info.blocks.len();
    if n < 2 {
        return Vec::new();
    }
    let mut ends = Vec::with_capacity(n);
    for i in 0..n {
        let Some(range) = info.decompressed_byte_range(i) else {
            return Vec::new();
        };
        let Ok(end) = u32::try_from(range.end) else {
            return Vec::new();
        };
        ends.push(end);
    }
    ends
}

/// Pure Rust zstd backend.
pub struct ZstdProvider;

impl CompressionProvider for ZstdProvider {
    fn compress(data: &[u8], level: i32) -> crate::Result<Vec<u8>> {
        use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

        // Thread-local compressor reused across blocks. `compress_slice_to_vec`
        // builds a fresh `FrameCompressor` (and its matcher tables) on every
        // call; reusing one per thread avoids that per-block reconstruction —
        // most visible at btultra2 / L22, where the tables are large. Keyed by
        // level so a level change rebuilds the compressor.
        //
        // `compress_independent_frame_into` emits a standalone frame: it resets
        // per-frame state and re-derives the source-size hint from `data`, so
        // the L22 small-source parameter set is still selected for the 4-64 KiB
        // blocks an LSM writes (the ~34x win the old size-hint path delivered).
        // Default generics (`R = &'static [u8]`, `W = Vec<u8>`) keep the cached
        // type `'static` for thread-local storage.
        thread_local! {
            static TLS_COMPRESSOR: std::cell::RefCell<Option<(i32, FrameCompressor)>> =
                const { std::cell::RefCell::new(None) };
        }

        TLS_COMPRESSOR.with(|cell| {
            let mut state = cell.borrow_mut();
            if !matches!(&*state, Some((l, _)) if *l == level) {
                *state = Some((
                    level,
                    FrameCompressor::new(CompressionLevel::from_level(level)),
                ));
            }
            let Some((_, compressor)) = state.as_mut() else {
                unreachable!("TLS_COMPRESSOR initialised above");
            };
            // `compress_independent_frame` is the `FrameCompressor` CCtx-style
            // single-frame API (structured-zstd >= 0.0.29); it allocates a fresh
            // output Vec and emits one standalone frame.
            Ok(compressor.compress_independent_frame(data))
        })
    }

    fn compress_with_layout(data: &[u8], level: i32) -> crate::Result<(Vec<u8>, Vec<u32>)> {
        use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

        // Mirrors `compress`'s thread-local reuse, but also reads back the
        // frame layout the encoder recorded during this emit
        // (`last_frame_emit_info`, captured under the `lsm` feature). The
        // layout is only meaningful when the frame split into >= 2 inner zstd
        // blocks; for a single-block frame there is nothing to partial-decode,
        // so an empty layout is returned and no per-block table is persisted.
        thread_local! {
            static TLS_COMPRESSOR: std::cell::RefCell<Option<(i32, FrameCompressor)>> =
                const { std::cell::RefCell::new(None) };
        }

        TLS_COMPRESSOR.with(|cell| {
            let mut state = cell.borrow_mut();
            if !matches!(&*state, Some((l, _)) if *l == level) {
                *state = Some((
                    level,
                    FrameCompressor::new(CompressionLevel::from_level(level)),
                ));
            }
            let Some((_, compressor)) = state.as_mut() else {
                unreachable!("TLS_COMPRESSOR initialised above");
            };
            let frame = compressor.compress_independent_frame(data);
            let layout = inner_block_layout(compressor.last_frame_emit_info());
            Ok((frame, layout))
        })
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        let mut decoder = structured_zstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        bounded_read(&mut decoder, capacity)
    }

    fn compress_with_dict(data: &[u8], level: i32, dict_raw: &[u8]) -> crate::Result<Vec<u8>> {
        use structured_zstd::decoding::Dictionary;
        use structured_zstd::encoding::{
            CompressionLevel, EncoderDictionary, FrameCompressor, MatchGeneratorDriver,
        };

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
            // Single-entry memoizer keyed by (dict_hash, level).
            // Sufficient for the typical case of one dict/level per compaction job per thread.
            // For workloads that interleave multiple dicts in the same thread, a multi-entry
            // keyed cache would avoid re-initialization on key changes (tracked in #231).
            static TLS_COMPRESSOR: std::cell::RefCell<Option<(u64, i32, CachedCompressor)>> =
                const { std::cell::RefCell::new(None) };
        }

        // The encoder attaches the dictionary via the `EncoderDictionary`
        // (CDict-analog) path, which parses for the encode side only.
        //
        // Two dictionary formats are supported:
        //
        // 1. **Finalized zstd dictionary** (magic bytes `37 A4 30 EC` prefix): produced by
        //    `zstd --train` / `zstd::dict::from_continuous` and the C zstd library.
        //    Contains entropy tables (Huffman + FSE) that prime the compressor's
        //    coding state for better ratios. Attached via `set_dictionary_from_bytes`
        //    (which builds an `EncoderDictionary`, skipping the decode lookup tables).
        //
        // 2. **Raw content dictionary** (no magic): a bare byte sequence used as
        //    LZ77 history to improve match distances on repetitive data. No entropy
        //    table seeding. Parsed via `Dictionary::from_raw_content`, then wrapped
        //    in an `EncoderDictionary` and attached via `set_encoder_dictionary`.
        //
        // Both formats end up attached as an `EncoderDictionary`, so a
        // `ZstdDictionary` created from a raw training corpus (without a
        // finalized header) compresses just as one created from a
        // finalized dictionary.
        //
        // ID derivation for raw content dictionaries:
        //   - Use the lower 32 bits of the xxh3 hash of `dict_raw`, clamped
        //     to at least 1. (id=0 is rejected by `FrameCompressor::set_dictionary`
        //     in structured-zstd.)
        //   - The id is written into the frame header (Dict_ID field) and
        //     kept there on disk, so the reader can optionally pin the
        //     inner frame's `Dictionary_ID` against the expected
        //     dictionary (`FrameDecoder::expect_dict_id`) to detect a
        //     block that was compressed under the wrong dictionary.
        let dict_key = xxhash_rust::xxh3::xxh3_64(dict_raw);

        TLS_COMPRESSOR.with(|cell| {
            let mut state = cell.borrow_mut();

            // Re-initialise if this is the first call in this thread or if the
            // dictionary or compression level has changed.
            if !matches!(&*state, Some((k, l, _)) if *k == dict_key && *l == level) {
                let mut compressor = FrameCompressor::new(CompressionLevel::from_level(level));
                if dict_raw.starts_with(&DICT_MAGIC) {
                    // Finalized dict: parse for the ENCODER side only via
                    // `EncoderDictionary` (the `set_dictionary_from_bytes` path),
                    // which skips building the decode lookup tables the encoder
                    // never reads. Cheaper than the old `decode_dict` full parse
                    // on every cache miss — most visible at btultra2 / L22, where
                    // dictionary parsing dominates the miss-path cost.
                    compressor
                        .set_dictionary_from_bytes(dict_raw)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                } else {
                    // Raw-content dict (no magic header): there is no serialized
                    // dictionary blob to parse for encoding, so wrap the
                    // raw-content `Dictionary` as an `EncoderDictionary` and
                    // attach it without a reparse.
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "intentional: lower 32 bits of xxh3 as internal dict id"
                    )]
                    let id = {
                        let h = dict_key as u32;
                        h.max(1) // id=0 is rejected by the attach path; internal use only
                    };
                    let dictionary = Dictionary::from_raw_content(id, dict_raw.to_vec())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                    compressor
                        .set_encoder_dictionary(EncoderDictionary::from_dictionary(dictionary))
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                }
                *state = Some((dict_key, level, compressor));
            }

            // Unreachable: the branch above always initialises `state`.
            let Some((_, _, compressor)) = state.as_mut() else {
                unreachable!("TLS_COMPRESSOR always initialised above");
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
            // Size hint so btultra2 (L22) picks the small-source parameter set
            // for this 4-64 KiB block instead of allocating the full 8 MiB
            // tables — without it dictionary block compression at L22 runs ~34x
            // slower (the matcher reset rebuilds the large tables every call).
            compressor.set_source_size_hint(data.len() as u64);
            compressor.set_source(std::io::Cursor::new(src_buf));
            compressor.set_drain(Vec::new());
            compressor.compress();

            // `set_drain(Vec::new())` is called on every path above, so
            // `take_drain()` always returns `Some`. This cannot fail.
            let compressed = compressor
                .take_drain()
                .unwrap_or_else(|| unreachable!("drain is always set by set_drain() above"));

            // The frame keeps the dictionary id its header declares (the
            // synthetic xxh3-derived id for raw-content dicts, the embedded
            // id for finalized dicts). Retaining it on disk lets the reader
            // pin the inner frame's `Dictionary_ID` against the expected
            // dictionary via `FrameDecoder::expect_dict_id`, surfacing a
            // block compressed under the wrong dictionary as a typed decode
            // error instead of silent wrong output. It also drops the
            // per-block header rewrite the old id-stripping step performed
            // on every dictionary write.
            Ok(compressed)
        })
    }

    fn decompress_with_dict(
        data: &[u8],
        dict: &crate::compression::ZstdDictionary,
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        use structured_zstd::decoding::FrameDecoder;

        // Thread-local `FrameDecoder` with the dictionary pre-loaded.
        //
        // `FrameDecoder` is not `Send`, so we keep one per thread. The cached
        // entry is keyed by the full 64-bit xxh3 fingerprint (`dict.id64()`),
        // not the truncated 32-bit public fingerprint, to avoid decoder reuse
        // when two distinct dictionaries happen to share the same 32-bit
        // prefix. A 64-bit collision is 2^32× less likely than a 32-bit one.
        //
        // On miss we register the *pre-parsed* dictionary handle held by the
        // `ZstdDictionary` itself (lazy-parsed once, shared via Arc inside
        // structured-zstd). This eliminates the per-thread `Dictionary` re-parse
        // the cache used to do on every miss — the dictionary's entropy tables
        // are built once globally and the FrameDecoder just shares the Arc.
        thread_local! {
            static TLS_DECODER: std::cell::RefCell<Option<(u64, FrameDecoder)>> =
                const { std::cell::RefCell::new(None) };
        }

        // For raw-content dicts `FrameDecoder::init` does not auto-resolve
        // the dictionary (it skips dict lookup whether the header carries
        // the synthetic dictID or, in older frames, omits it), so the
        // raw-content branch below uses `force_dict` after `init` to apply
        // it explicitly.
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
                // Pull the shared pre-parsed handle from the dictionary. First
                // caller across all threads parses; everyone after gets an Arc
                // clone of the cached entropy tables.
                let handle = dict.prepared_handle()?;
                let mut decoder = FrameDecoder::new();
                decoder
                    .add_dict_handle(handle)
                    .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                *state = Some((dict.id64(), decoder));
            }

            // Unreachable: the branch above always initialises `state`.
            let Some((_, decoder)) = state.as_mut() else {
                unreachable!("TLS_DECODER always initialised above");
            };

            do_decompress_with_dict(decoder, data, dict.id().max(1), capacity, is_raw_content)
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

        let decompressed =
            ZstdProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
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
        let compressed = ZstdProvider::compress_with_dict(PLAINTEXT, 3, DICT)
            .expect("compression should succeed");

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

        let decompressed =
            ZstdProvider::decompress_with_dict(&compressed, &dict, PLAINTEXT.len() + 1)
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

        let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, 1).expect(
            "decompression of empty payload with raw-content dict (capacity=1) should succeed",
        );

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

        let decompressed = ZstdProvider::decompress_with_dict(&compressed, &dict, 0).expect(
            "decompression of empty payload with raw-content dict (capacity=0) should succeed",
        );

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
        let mut frame = ZstdProvider::compress_with_dict(PLAINTEXT, 3, raw_dict)
            .expect("compression must succeed");
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
            .decode_blocks_partial(&mut src, 0, 1)
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
}
