// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, Dmitry Prudnikov

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

/// The error a frame compressed under `dict` failed with, decoded into a
/// `capacity`-byte buffer.
///
/// A frame naming another dictionary is a typed [`crate::Error::Decompress`]
/// carrying the dictionary it was read under, never silent wrong output; one
/// decoding to more than `capacity` bytes is
/// [`crate::Error::DecompressedSizeTooLarge`].
fn dict_decode_error(
    e: &structured_zstd::decoding::errors::FrameDecoderError,
    dict: &crate::compression::ZstdDictionary,
    capacity: usize,
) -> crate::Error {
    use structured_zstd::decoding::errors::FrameDecoderError;

    match e {
        FrameDecoderError::UnexpectedDictId { .. } | FrameDecoderError::DictIdMismatch { .. } => {
            crate::Error::Decompress(crate::CompressionType::ZstdDict {
                level: 0,
                dict_id: dict.frame_id(),
            })
        }
        FrameDecoderError::TargetTooSmall => crate::Error::DecompressedSizeTooLarge {
            declared: capacity as u64 + 1,
            limit: capacity as u64,
        },
        e => crate::Error::Io(crate::io::Error::other(e.to_string())),
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
/// The lowest level whose preset selects `btultra2`, the only strategy that
/// runs the two-pass statistics seed. Below it the seed flag has nothing to
/// switch, so the level presets are left exactly as the codec ships them.
const FIRST_TWO_PASS_LEVEL: i32 = 19;

/// Whether a compressor for `level` has to be built with the strategy
/// overridden, which is the only thing the seed setting changes about one.
///
/// This is what the thread-local caches below key on, rather than the setting
/// itself: below level 19 both settings build the identical compressor, so
/// keying on the raw flag would rebuild one every time two jobs carrying
/// different snapshots of the setting alternate on a worker thread, which a
/// toggle during a long compaction makes routine. The rebuild also drops the
/// attached dictionary on the path that has one.
const fn builds_a_single_pass_compressor(level: i32, two_pass_seed: bool) -> bool {
    !two_pass_seed && level >= FIRST_TWO_PASS_LEVEL
}

/// Applies the single-pass strategy to a compressor built for `level`.
///
/// `btultra` is what level 18 selects: the same binary-tree finder and optimal
/// parse as `btultra2`, without the seed pass. Only the strategy is overridden,
/// so every other parameter stays at the preset for the requested level.
fn use_single_pass(
    compressor: &mut structured_zstd::encoding::FrameCompressor,
    level: structured_zstd::encoding::CompressionLevel,
) -> crate::Result<()> {
    use structured_zstd::encoding::{CompressionParameters, Strategy};

    let params = CompressionParameters::builder(level)
        .strategy(Strategy::Btultra)
        .build()
        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
    compressor.set_parameters(&params);
    Ok(())
}

fn with_tls_compressor<R>(
    level: i32,
    two_pass_seed: bool,
    f: impl FnOnce(&mut structured_zstd::encoding::FrameCompressor) -> R,
) -> crate::Result<R> {
    use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

    // The strategy is part of the cache key: a compressor carries the
    // parameters it was built with, so a block written under one strategy must
    // not reuse the compressor configured for the other.
    thread_local! {
        static TLS_COMPRESSOR: std::cell::RefCell<Option<(i32, bool, FrameCompressor)>> =
            const { std::cell::RefCell::new(None) };
    }

    let single_pass = builds_a_single_pass_compressor(level, two_pass_seed);

    TLS_COMPRESSOR.with(|cell| {
        let mut state = cell.borrow_mut();
        if !matches!(&*state, Some((l, cached, _)) if *l == level && *cached == single_pass) {
            let configured = CompressionLevel::from_level(level);
            let mut compressor = FrameCompressor::new(configured);
            if single_pass {
                use_single_pass(&mut compressor, configured)?;
            }
            *state = Some((level, single_pass, compressor));
        }
        let Some((_, _, compressor)) = state.as_mut() else {
            unreachable!("TLS_COMPRESSOR initialised above");
        };
        Ok(f(compressor))
    })
}

/// Pure Rust zstd backend.
pub struct ZstdProvider;

impl CompressionProvider for ZstdProvider {
    fn compress(data: &[u8], level: i32, two_pass_seed: bool) -> crate::Result<Vec<u8>> {
        // `compress_independent_frame` is the `FrameCompressor` CCtx-style
        // single-frame API (structured-zstd >= 0.0.29): it allocates a fresh
        // output Vec and emits one standalone frame.
        with_tls_compressor(level, two_pass_seed, |compressor| {
            compressor.compress_independent_frame(data)
        })
    }

    fn compress_with_layout(
        data: &[u8],
        level: i32,
        two_pass_seed: bool,
    ) -> crate::Result<(Vec<u8>, Vec<u32>)> {
        // Mirrors `compress`, but also reads back the frame layout the encoder
        // recorded during this emit (`last_frame_emit_info`, under the `lsm`
        // feature). The layout is only meaningful when the frame split into
        // >= 2 inner zstd blocks; a single-block frame returns an empty layout
        // (nothing to partial-decode) and no per-block table is persisted.
        with_tls_compressor(level, two_pass_seed, |compressor| {
            let frame = compressor.compress_independent_frame(data);
            let layout = inner_block_layout(compressor.last_frame_emit_info());
            (frame, layout)
        })
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        with_plain_decoder(|decoder| {
            // A malformed frame header surfaces on the first read.
            let mut stream = structured_zstd::decoding::StreamingDecoder::new_with_decoder(
                std::io::Cursor::new(data),
                decoder,
            );
            bounded_read(&mut stream, capacity)
        })
    }

    fn compress_with_dict(
        data: &[u8],
        level: i32,
        dict: &crate::compression::ZstdDictionary,
        two_pass_seed: bool,
    ) -> crate::Result<Vec<u8>> {
        use structured_zstd::encoding::{CompressionLevel, FrameCompressor};

        // One reusable compression context per thread (zstd's `CCtx`) with the
        // dictionary attached, kept while the (dictionary, level) pair holds.
        // The dictionary is prepared once for every thread
        // (`ZstdDictionary::prepared_encoder`), so a miss here builds only the
        // match-finder state and attaches a shared handle: nothing is parsed
        // or copied. Keyed by the full 64-bit fingerprint, so two dictionaries
        // sharing a 32-bit truncation never share a context.
        //
        // One entry: a thread writes one (dictionary, level) pair for a whole
        // compaction job (a multi-entry cache is tracked in #231).
        // The strategy joins the key for the same reason it does on the
        // dictionary-less path: the parameters live in the compressor. A miss
        // costs more here, since it re-attaches the dictionary as well.
        thread_local! {
            static TLS_COMPRESSOR: std::cell::RefCell<Option<(u64, i32, bool, FrameCompressor)>> =
                const { std::cell::RefCell::new(None) };
        }

        let single_pass = builds_a_single_pass_compressor(level, two_pass_seed);

        TLS_COMPRESSOR.with(|cell| {
            let mut state = cell.borrow_mut();
            if let Some((key, l, cached, compressor)) = state.as_mut()
                && *l == level
                && *cached == single_pass
            {
                // Another dictionary at the same parameters, as each blob
                // file brings its own: the compressor keeps its tables and
                // takes the new handle.
                if *key != dict.id64() {
                    compressor
                        .set_encoder_dictionary(dict.prepared_encoder()?)
                        .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
                    *key = dict.id64();
                }
            } else {
                let configured = CompressionLevel::from_level(level);
                let mut compressor = FrameCompressor::new(configured);
                // Before the dictionary: attaching it builds match-finder state
                // from the parameters in force, so the strategy has to be the
                // final one by then.
                if single_pass {
                    use_single_pass(&mut compressor, configured)?;
                }
                compressor
                    .set_encoder_dictionary(dict.prepared_encoder()?)
                    .map_err(|e| crate::Error::Io(crate::io::Error::other(e.to_string())))?;
                *state = Some((dict.id64(), level, single_pass, compressor));
            }
            let Some((_, _, _, compressor)) = state.as_mut() else {
                unreachable!("TLS_COMPRESSOR initialised above");
            };

            // Reads `data` in place and sizes the frame from its length, so a
            // 4-64 KiB block gets the small-source parameter set without a
            // separate hint. Every call is an independent frame the dictionary
            // is primed into. Its header keeps the dictionary id, which the
            // reader pins (`FrameDecoder::expect_dict_id`) to catch a block
            // compressed under the wrong dictionary.
            Ok(compressor.compress_independent_frame(data))
        })
    }

    fn decompress_with_dict(
        data: &[u8],
        dict: &crate::compression::ZstdDictionary,
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        // Straight into the result's buffer, as `decompress_with_dict_into`
        // decodes: the frame's header is read in place and its blocks land
        // where they stay, with no scratch or copy out of one.
        let mut output = vec![0; capacity];
        let written = Self::decompress_with_dict_into(data, dict, &mut output)?;
        output.truncate(written);
        // `capacity` is a limit, not a size: a frame far below it gives the
        // rest back, so the result holds no more than a Vec grown to it would.
        // A limit near the frame's size keeps its buffer without a realloc.
        if output.capacity() / 2 > written {
            output.shrink_to_fit();
        }
        Ok(output)
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
        with_plain_decoder(|decoder| {
            // A malformed frame header surfaces on the first read.
            let mut stream = structured_zstd::decoding::StreamingDecoder::new_with_decoder(
                std::io::Cursor::new(data),
                decoder,
            );
            bounded_read_into(&mut stream, dest)
        })
    }

    /// [`decompress_into`](Self::decompress_into) for a frame compressed under
    /// `dict`: decoded straight into `dest` (exact expected size) with the
    /// dictionary borrowed, so a small value costs neither the growing scratch
    /// `Vec` nor the copy out of it that
    /// [`decompress_with_dict`](CompressionProvider::decompress_with_dict)
    /// pays. A frame naming another dictionary is refused before any block
    /// decodes. Returns the bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame header is invalid, names another
    /// dictionary, does not decode, or decodes to more than `dest.len()` bytes.
    pub fn decompress_with_dict_into(
        data: &[u8],
        dict: &crate::compression::ZstdDictionary,
        dest: &mut [u8],
    ) -> crate::Result<usize> {
        // No bytes are no frame: the whole-input decode would take them for
        // zero frames and return nothing, where even an empty plaintext's
        // frame has a header.
        if data.is_empty() {
            return Err(crate::Error::Decompress(crate::CompressionType::ZstdDict {
                level: 0,
                dict_id: dict.frame_id(),
            }));
        }
        let handle = dict.prepared_handle()?;
        // A raw-content dictionary's frames carry the synthetic id it was
        // compressed under; a finalized one's carry the id of its header,
        // which the decode checks against the handle by itself.
        let is_raw_content = !dict
            .raw()
            .starts_with(&structured_zstd::decoding::DICTIONARY_MAGIC);
        // The frame is handed the dictionary as it starts, whatever the
        // decoder decoded before, so one decoder serves every dictionary. A
        // raw-content dictionary's frames carry the synthetic id they were
        // compressed under, which is pinned; a finalized one's carry its
        // header's id, which the start checks against the handle. Either way
        // a frame compressed under another dictionary is refused before any
        // block decodes. The block checksum, and the AAD for an encrypted
        // block, already reject a substituted frame; the frame's own id is a
        // third witness behind them.
        with_dict_decoder(|decoder| {
            decoder.expect_dict_id(is_raw_content.then(|| dict.frame_id()));
            decoder
                .decode_all_with_dict_handle(data, dest, handle)
                .map_err(|e| dict_decode_error(&e, dict, dest.len()))
        })
    }
}

/// Runs `decode` on this thread's `FrameDecoder` for frames compressed under a
/// dictionary, any dictionary: each frame is handed its dictionary as it
/// starts. Apart from the plain decoder, since a frame started without a
/// dictionary leaves the last one applied in place.
fn with_dict_decoder<T>(
    decode: impl FnOnce(&mut structured_zstd::decoding::FrameDecoder) -> T,
) -> T {
    thread_local! {
        static TLS_DICT_DECODER: core::cell::RefCell<structured_zstd::decoding::FrameDecoder> =
            core::cell::RefCell::new(structured_zstd::decoding::FrameDecoder::new());
    }

    TLS_DICT_DECODER.with(|cell| decode(&mut cell.borrow_mut()))
}

/// Runs `decode` on this thread's `FrameDecoder` for frames without a dictionary.
///
/// `StreamingDecoder::new` constructs a new `FrameDecoder`, which allocates and
/// zero-fills its window + entropy scratch each time: pure fixed overhead on a
/// cold point read's one block, and per value on a blob scan. `init` reuses the
/// thread-local decoder's already-sized buffers after the first call (it only
/// clears leftover state, no realloc), the equivalent of reusing a libzstd
/// `ZSTD_DCtx` across frames. `FrameDecoder` is not `Send`, so the cache is
/// thread-local; this decoder is never handed a dictionary, so it stays on the
/// plain (no-dict) frame path.
fn with_plain_decoder<T>(
    decode: impl FnOnce(&mut structured_zstd::decoding::FrameDecoder) -> T,
) -> T {
    thread_local! {
        static TLS_DECODER: core::cell::RefCell<structured_zstd::decoding::FrameDecoder> =
            core::cell::RefCell::new(structured_zstd::decoding::FrameDecoder::new());
    }

    TLS_DECODER.with(|cell| decode(&mut cell.borrow_mut()))
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod tests;
