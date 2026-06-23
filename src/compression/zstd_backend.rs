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
/// with `Dictionary::decode_dict`. A blob without this prefix is treated
/// as raw content and is loaded via `Dictionary::from_raw_content`.
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
            Err(e) => return Err(crate::Error::from(e)),
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
        Err(e) => return Err(crate::Error::from(e)),
    }

    output.truncate(filled);
    Ok(output)
}

/// Like [`bounded_read`], but decodes straight into a caller-provided buffer of
/// the exact expected size — no zero-filled `Vec` and no later copy. Returns the
/// number of bytes written; the caller checks it equals the expected length.
fn bounded_read_into(reader: &mut impl Read, dest: &mut [u8]) -> crate::Result<usize> {
    let mut filled = 0;
    while filled < dest.len() {
        // `filled < dest.len()` by the loop guard, so the slice is non-empty.
        let slot = dest.get_mut(filled..).unwrap_or(&mut []);
        match reader.read(slot) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(crate::Error::from(e)),
        }
    }

    // If the buffer filled exactly, probe for excess: a frame that decodes to
    // more than the declared size is corrupt / over-budget.
    if filled == dest.len() {
        let mut probe = [0u8; 1];
        match reader.read(&mut probe) {
            Ok(0) => {}
            Ok(_) => {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: (filled as u64).saturating_add(1),
                    limit: dest.len() as u64,
                });
            }
            Err(e) => return Err(crate::Error::from(e)),
        }
    }

    Ok(filled)
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
        // Clamp-to-zero: remaining output budget = capacity minus what has been
        // decoded so far (never negative once the cap is reached).
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
                .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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
            decoder.read_exact(dest).map_err(crate::Error::from)?;
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
        // Inner-frame defense-in-depth: pin the frame's embedded Dictionary_ID
        // against the dictionary we are about to decode with. On the raw-content
        // path the frame carries the synthetic `raw_content_id` we wrote at
        // compress time, so a genuine frame always matches; a frame body
        // compressed under a different dictionary diverges here and is rejected
        // BEFORE `force_dict` blindly applies the wrong tables and yields silent
        // garbage. `init` validates the pinned id against the parsed header
        // before any block decode runs.
        //
        // Primary value is correctness, with defense-in-depth on top:
        // - Correctness: `force_dict` applies the given dictionary REGARDLESS of
        //   the frame's id, so a wrong-dictionary configuration would otherwise
        //   decode to silent garbage. The gate turns that into a clear typed
        //   error at the source.
        // - Defense-in-depth: the block-level integrity checksum (a fixed
        //   XXH3-128 over the compressed bytes, verified before we get here) and,
        //   for encrypted blocks, the AAD `dict_id` binding (AEAD verify) already
        //   reject a substituted frame; the embedded Dictionary_ID is an
        //   independent third witness behind them.
        // (Note: the configurable `ChecksumAlgorithm` — `Xxh3_64` / `Xxh3Low32`
        // / `Crc32c` — is the per-KV / per-entry checksum, NOT this block
        // checksum, which is unconditionally XXH3-128.)
        decoder.expect_dict_id(Some(raw_content_id));
        decoder.init(&mut cursor).map_err(|e| {
            if matches!(
                e,
                structured_zstd::decoding::errors::FrameDecoderError::UnexpectedDictId { .. }
            ) {
                // Typed signal: the inner frame was compressed under a different
                // dictionary than this block claims. Never silent wrong output.
                crate::Error::Decompress(crate::CompressionType::ZstdDict {
                    level: 0,
                    dict_id: raw_content_id,
                })
            } else {
                crate::Error::Io(crate::io::Error::other(e.to_string()))
            }
        })?;

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
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;

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
                crate::Error::Io(crate::io::Error::other(e.to_string()))
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
/// just-emitted frame's `FrameEmitInfo`.
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

/// Runs `f` with a thread-local `FrameCompressor` cached by `level` (rebuilt
/// only on a level change). Shared by [`ZstdProvider::compress`] and
/// `compress_with_layout` so interleaving them at the same level reuses one
/// compressor instead of maintaining two separate thread-local caches.
///
/// `compress_slice_to_vec` builds a fresh `FrameCompressor` (and its matcher
/// tables) on every call; reusing one per thread avoids that per-block
/// reconstruction — most visible at btultra2 / L22, where the tables are large.
/// `compress_independent_frame` still resets per-frame state and re-derives the
/// source-size hint from `data`, so the L22 small-source parameter set is still
/// selected for the 4-64 KiB blocks an LSM writes. Default generics keep the
/// cached type `'static` for thread-local storage.
fn with_tls_compressor<R>(
    level: i32,
    f: impl FnOnce(&mut structured_zstd::encoding::FrameCompressor) -> R,
) -> R {
    use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

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
        f(compressor)
    })
}

/// Pure Rust zstd backend.
pub struct ZstdProvider;

impl CompressionProvider for ZstdProvider {
    fn compress(data: &[u8], level: i32) -> crate::Result<Vec<u8>> {
        // `compress_independent_frame` is the `FrameCompressor` CCtx-style
        // single-frame API (structured-zstd >= 0.0.29): it allocates a fresh
        // output Vec and emits one standalone frame.
        Ok(with_tls_compressor(level, |compressor| {
            compressor.compress_independent_frame(data)
        }))
    }

    fn compress_with_layout(data: &[u8], level: i32) -> crate::Result<(Vec<u8>, Vec<u32>)> {
        // Mirrors `compress`, but also reads back the frame layout the encoder
        // recorded during this emit (`last_frame_emit_info`, under the `lsm`
        // feature). The layout is only meaningful when the frame split into
        // >= 2 inner zstd blocks; a single-block frame returns an empty layout
        // (nothing to partial-decode) and no per-block table is persisted.
        Ok(with_tls_compressor(level, |compressor| {
            let frame = compressor.compress_independent_frame(data);
            let layout = inner_block_layout(compressor.last_frame_emit_info());
            (frame, layout)
        }))
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        let mut decoder = structured_zstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
                    compressor
                        .set_encoder_dictionary(EncoderDictionary::from_dictionary(dictionary))
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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
                    .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
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

impl ZstdProvider {
    /// Decompress a frame straight into `dest` (exact expected size), skipping
    /// the zero-filled scratch `Vec` and the later copy that
    /// [`decompress`](CompressionProvider::decompress) pays. Returns the bytes
    /// written; the caller checks it equals the declared uncompressed length.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame header is invalid, an I/O error occurs, or
    /// the frame decodes to more than `dest.len()` bytes.
    pub fn decompress_into(data: &[u8], dest: &mut [u8]) -> crate::Result<usize> {
        let mut decoder = structured_zstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
        bounded_read_into(&mut decoder, dest)
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests;
