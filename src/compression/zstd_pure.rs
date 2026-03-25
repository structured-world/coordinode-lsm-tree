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
//! - Compression uses the `Fastest` level regardless of the requested
//!   level (higher levels are not yet implemented in structured-zstd).
//! - Dictionary compression is not yet supported (returns an error).
//! - Dictionary decompression is supported.
//! - Decompression throughput is ~2–3.5x slower than the C reference.

use super::CompressionProvider;

/// Pure Rust zstd backend.
pub struct ZstdPureProvider;

impl CompressionProvider for ZstdPureProvider {
    fn compress(data: &[u8], _level: i32) -> crate::Result<Vec<u8>> {
        // structured-zstd currently only supports Fastest level;
        // higher levels are accepted but silently map to Fastest.
        let compressed = structured_zstd::encoding::compress_to_vec(
            std::io::Cursor::new(data),
            structured_zstd::encoding::CompressionLevel::Fastest,
        );
        Ok(compressed)
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        let mut decoder = structured_zstd::decoding::FrameDecoder::new();
        let mut output = Vec::with_capacity(capacity);

        decoder
            .decode_all_to_vec(data, &mut output)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        Ok(output)
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
        dict_raw: &[u8],
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        // NOTE: Dictionary is re-parsed from raw bytes on every call.
        // The C FFI backend has the same per-call overhead (Decompressor::with_dictionary
        // also re-initializes). Caching would require adding precompiled dictionary
        // state to the CompressionProvider trait, which is a Phase 2 optimization.
        let dict = structured_zstd::decoding::Dictionary::decode_dict(dict_raw)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        let mut decoder = structured_zstd::decoding::FrameDecoder::new();
        decoder
            .add_dict(dict)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        let mut output = Vec::with_capacity(capacity);
        decoder
            .decode_all_to_vec(data, &mut output)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        Ok(output)
    }
}
