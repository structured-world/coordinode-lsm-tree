// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub(crate) mod binary_index;
// Crate-internal: Decoder, Decodable, ParsedItem are not part of the public API.
// They are re-exported as pub(crate) below; narrowing the module prevents
// external code from reaching these traits via lsm_tree::table::block::decoder::*.
pub(crate) mod decoder;
mod encoder;
pub mod hash_index;
pub(crate) mod header;
mod identity;
pub(crate) mod kv_checksum;
mod offset;
mod trailer;
mod transform;
mod r#type;

pub(crate) use decoder::{Decodable, Decoder, DecoderMeta, ParsedItem};
pub(crate) use encoder::{Encodable, Encoder};
pub use header::Header;
pub use identity::BlockIdentity;
pub use offset::BlockOffset;
pub(crate) use trailer::{TRAILER_START_MARKER, Trailer};
pub use transform::{BlockTransform, CompressionContext, EccParams};
pub use r#type::BlockType;

#[cfg(zstd_any)]
use crate::compression::CompressionProvider as _;

use crate::{
    Checksum, CompressionType, Slice,
    coding::{Decode, Encode},
    fs::FsFile,
    table::BlockHandle,
};
use alloc::borrow::Cow;
// Vec lives in the std prelude on std builds; pull it from alloc on no-std so
// this module's heap buffers resolve under `--no-default-features --features alloc`.
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

/// Safety cap on block payload size (256 MiB).
///
/// Enforced on both write and read paths to prevent producing or accepting
/// blocks that are unreasonably large. Intentionally stricter than the
/// on-disk format limit (`u32::MAX`) to guard against decompression bombs
/// and OOM from crafted/malicious SST files.
///
/// NOTE: Intentionally duplicated in `vlog::blob_file` (writer as `usize`,
/// reader as `usize`) rather than shared, because blocks and blobs are
/// independent storage formats that may diverge in the future.
const MAX_DECOMPRESSION_SIZE: u32 = 256 * 1024 * 1024;

/// Exact Reed-Solomon parity trailer size for a given data length
/// under the (4, 2) scheme.
///
/// `parity_len(N) = shard_bytes(N) * 2` where
/// `shard_bytes(N) = ceil(N / 4) rounded up to the nearest even`.
/// Mirrors `crate::ecc::parity_len` byte-for-byte but is available
/// without the `page_ecc` cargo feature — the parity trailer length is
/// NOT stored in the block header; the read path derives it from
/// `data_length` with this function whenever a block's `ECC_PARITY`
/// flag is set, so writer and reader always agree on the trailer size
/// without a per-block length field to corrupt or forge.
///
/// Returns zero for `data_length == 0` to match
/// `crate::ecc::encode_parity`'s empty-payload short-circuit.
#[inline]
pub(crate) fn expected_parity_len(data_length: u32, params: EccParams) -> u32 {
    let data_shards = u32::from(params.data_shards());
    let parity_shards = u32::from(params.parity_shards());
    if data_length == 0 || data_shards == 0 || parity_shards == 0 {
        return 0;
    }
    // ceil(N / data_shards) — overflow-safe for u32 since `data_length`
    // is already capped at MAX_DECOMPRESSION_SIZE + max overhead by the
    // caller before this function fires.
    let ceil = (data_length / data_shards)
        .saturating_add(u32::from(!data_length.is_multiple_of(data_shards)));
    // Round up to even (the `reed-solomon-simd` engine requires shard
    // sizes that are a multiple of two; XOR shares the layout).
    let shard_bytes = ceil.saturating_add(u32::from(!ceil.is_multiple_of(2)));
    shard_bytes.saturating_mul(parity_shards)
}

/// Whether the on-disk block carries a Reed-Solomon parity trailer.
///
/// Source of truth depends on the block type:
/// - Blocks that carry the `block_flags` byte (`Meta` / `Manifest` /
///   `ManifestFooter` — see [`Header::has_block_flags`]) self-describe parity
///   via the `ECC_PARITY` bit.
/// - SST blocks (`Data` / `Index` / `Filter` / `RangeTombstone`) omit the
///   byte, so
///   parity presence comes from the per-SST descriptor, threaded in via the
///   caller-supplied `transform` (`transform.page_ecc()`).
fn block_has_parity(header: &Header, transform: &BlockTransform<'_>) -> bool {
    if Header::has_block_flags(header.block_type) {
        header.block_flags & header::block_flags::ECC_PARITY != 0
    } else {
        transform.page_ecc()
    }
}

/// The ECC shard scheme to size + recover a block's parity trailer with.
///
/// Self-describing blocks (`Meta` / `Manifest` / `ManifestFooter`) are read
/// at table / manifest open BEFORE any per-SST descriptor is known, so they
/// always use the fixed [`EccParams::RS_4_2`] layout (matching the writer).
/// SST blocks (`Data` / `Index` / `Filter` / `RangeTombstone`) are
/// descriptor-driven: their scheme rides on the caller-supplied `transform`
/// (sourced from the SST's `TableMeta`).
///
/// Not feature-gated: callable from read sizing on all builds (without
/// `page_ecc` it is only reached from dead `block_has_parity == false`
/// branches, but must still compile).
fn block_ecc_params(header: &Header, transform: &BlockTransform<'_>) -> EccParams {
    if Header::has_block_flags(header.block_type) {
        EccParams::RS_4_2
    } else {
        transform.ecc_params().unwrap_or(EccParams::RS_4_2)
    }
}

/// A block whose transform pipeline (compress → encrypt → checksum → ecc)
/// has run, but whose framed bytes have not yet been written to the file.
///
/// Splitting "prepare" from "write" is what lets compaction run the
/// CPU-bound transform stack on worker threads while a single thread keeps
/// the file writes (and the index registration that depends on byte offsets)
/// strictly ordered. The serial path stays zero-copy: `payload` borrows the
/// caller's `data` on the uncompressed/unencrypted path and owns it only when
/// a transform produced a fresh buffer. A worker thread takes ownership via
/// [`PreparedBlock::into_owned`] so the prepared block can outlive `data`.
pub(crate) struct PreparedBlock<'a> {
    header: Header,
    payload: Cow<'a, [u8]>,
    /// Reed-Solomon parity trailer, present only when page-ECC is active and
    /// the payload was non-empty. Always `None` without the `page_ecc` feature.
    parity: Option<Vec<u8>>,
}

impl PreparedBlock<'_> {
    /// Takes ownership of the payload so the prepared block no longer borrows
    /// the source `data`, yielding a `'static` value safe to move to a worker
    /// thread. A no-op allocation when the payload is already owned (a
    /// transform ran); copies once on the borrowed (uncompressed) path.
    #[cfg(feature = "std")] // no-std: parallel compaction unavailable (no threads)
    pub(crate) fn into_owned(self) -> PreparedBlock<'static> {
        PreparedBlock {
            header: self.header,
            payload: Cow::Owned(self.payload.into_owned()),
            parity: self.parity,
        }
    }

    /// Writes the framed block (header + payload + optional parity trailer)
    /// to `writer` and returns the header. This is the single point where
    /// block bytes hit the file, so it must run in on-disk order.
    // no-std: core2::io::Write (whole block-write path migrates together)
    pub(crate) fn write_to<W: std::io::Write>(self, mut writer: &mut W) -> crate::Result<Header> {
        self.header.encode_into(&mut writer)?;
        writer.write_all(&self.payload)?;
        if let Some(parity) = &self.parity {
            writer.write_all(parity)?;
        }

        log::trace!(
            "Writing block with size {}B (on-disk: {}B, ecc: {}B) (excluding header of {}B)",
            self.header.uncompressed_length,
            self.header.data_length,
            self.parity.as_ref().map_or(0, Vec::len),
            Header::header_len(self.header.block_type),
        );

        Ok(self.header)
    }
}

/// A block on disk
///
/// Consists of a fixed-size header and some bytes (the data/payload).
/// Outcome of the ECC check performed while reading a block, distinct from
/// success / failure of the read itself.
///
/// A read returns `Err` only on real payload corruption (checksum mismatch
/// with no available recovery). When the read *succeeds*, this reports
/// whether the block's ECC was usable:
///
/// - [`Self::Ok`] — ECC absent, or a recognized scheme that verified (and
///   recovered, if needed).
/// - [`Self::Unrecognized`] — the block carries an ECC trailer this build
///   cannot interpret (an unimplemented or non-canonical scheme: Secded,
///   page granularity, unknown kind, …). The payload was returned (its
///   checksum passed), but ECC recovery is unavailable for this block;
///   recompaction re-stamps it with a supported scheme. A "typing" warning,
///   not a read failure.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum EccStatus {
    /// ECC absent or a recognized scheme verified normally.
    #[default]
    Ok,
    /// ECC trailer present but its scheme is unrecognized / unusable; the
    /// payload still verified by its checksum. Recommend recompaction.
    Unrecognized,
}

#[derive(Clone)]
pub struct Block {
    pub header: Header,
    pub data: Slice,
}

impl Block {
    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Reads `data_length` payload bytes, then `ecc_length` parity
    /// bytes (when non-zero), verifies the payload checksum against
    /// `expected`, and on mismatch attempts Reed-Solomon recovery
    /// from the parity trailer. Returns the validated payload bytes
    /// (recovered if needed).
    ///
    /// Always consumes exactly `data_length + ecc_length` bytes from
    /// the reader, so callers don't have to track the trailer
    /// independently. When ECC recovery succeeds, the original
    /// checksum-mismatch is logged at WARN level — the block is
    /// returned to the caller as if no corruption ever happened.
    fn read_payload_and_verify<R: std::io::Read>(
        reader: &mut R,
        data_length: u32,
        ecc_length: u32,
        expected: Checksum,
        #[cfg_attr(
            not(feature = "page_ecc"),
            expect(unused_variables, reason = "recovery scheme only used under page_ecc")
        )]
        ecc_params: EccParams,
    ) -> crate::Result<Vec<u8>> {
        let mut data = vec![0u8; data_length as usize];
        reader.read_exact(&mut data)?;

        let computed = Checksum::from_raw(crate::hash::hash128(&data));

        if ecc_length == 0 {
            computed.check(expected).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for block payload, got={computed}, expected={expected}",
                );
            })?;
            return Ok(data);
        }

        // ECC trailer present — always consume the parity bytes so
        // the reader cursor lands on the next block's header even
        // when the happy path doesn't need them.
        let mut parity = vec![0u8; ecc_length as usize];
        reader.read_exact(&mut parity)?;

        if computed == expected {
            return Ok(data);
        }

        // Mismatch — try Reed-Solomon recovery before failing.
        #[cfg(feature = "page_ecc")]
        {
            let expected_raw = expected.into_u128();
            let (data_shards, parity_shards) = ecc_params.as_shards();
            if let Some(recovered) = crate::ecc::try_recover(
                &data,
                &parity,
                data.len(),
                data_shards,
                parity_shards,
                |buf| crate::hash::hash128(buf) == expected_raw,
            )? {
                log::warn!(
                    "recovered block from RS parity after checksum mismatch \
                     (data_len={}, ecc_len={ecc_length})",
                    data.len(),
                );
                return Ok(recovered);
            }
            log::error!(
                "Checksum mismatch on ECC-protected block, recovery failed, \
                 got={computed}, expected={expected}",
            );
            Err(crate::Error::PageEccUnrecoverable {
                got: computed,
                expected,
            })
        }

        #[cfg(not(feature = "page_ecc"))]
        {
            // Block has an ECC trailer but this build can't use it.
            // Discard the parity buffer explicitly so the compiler
            // sees the use, then surface the checksum-mismatch
            // error directly. Earlier in this function we already
            // confirmed `computed != expected` (the `if computed
            // == expected` happy-path returned above), so
            // `computed.check(expected)` is guaranteed to return
            // Err here — return it directly instead of going
            // through `?` followed by an unreachable fallback.
            let _ = parity;
            log::error!(
                "block has ECC trailer (ecc_length={ecc_length}) but this \
                 build lacks the page_ecc feature — cannot attempt recovery; \
                 got={computed}, expected={expected}",
            );
            Err(crate::Error::ChecksumMismatch {
                expected,
                got: computed,
            })
        }
    }

    /// Encodes a block into a writer.
    ///
    /// Pipeline: raw data → compress → encrypt → checksum → write. The
    /// concrete pipeline shape (which steps run) is encoded by the
    /// [`BlockTransform`] variant (see its docs for the four valid
    /// combinations). The previous separate `(compression, encryption,
    /// zstd_dict)` argument triple has been collapsed into this single
    /// transform argument; `CompressionContext`'s constructors enforce
    /// that the dict bundle travels with the `ZstdDict` codec
    /// discriminator (see [`BlockTransform`] module docs), so the
    /// runtime `ZstdDictMismatch` guard inside this function is
    /// defensive only: every public construction path (direct
    /// `BlockTransform::Compressed(CompressionContext::with_dict(..))`
    /// and the [`BlockTransform::from_parts`] legacy helper) catches
    /// the mismatch before the call reaches `write_into`, so the
    /// guard is unreachable from any in-tree caller and exists purely
    /// as a "should-never-fire" assertion.
    pub fn write_into<W: std::io::Write>(
        writer: &mut W,
        data: &[u8],
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Header> {
        // Most blocks carry no caller-supplied transform bits beyond what
        // the `transform` itself implies (compression / encryption / ECC).
        // The per-KV footer is the one bit `write_into` can't derive from
        // the payload, so the data-block writer routes through
        // `write_into_with_flags` to set it.
        Self::write_into_with_flags(writer, data, identity, transform, 0)
    }

    /// Like [`Self::write_into`] but lets the caller OR in
    /// transform-presence bits that aren't derivable from `transform`
    /// (currently only the `KV_CHECKSUM_FOOTER` flag from the `block_flags`
    /// module, since the footer lives in `data` and `write_into` can't see it).
    /// The compression / encryption / ECC bits are still derived from
    /// `transform` here, so the in-memory [`Header::block_flags`] always
    /// reflects the full transform stack regardless of which entry point is
    /// used. Note this is the IN-MEMORY header: only the self-describing block
    /// types (`Meta` / `Manifest` / `ManifestFooter`) serialize the
    /// `block_flags` byte to disk; SST block types omit it and the reader
    /// recovers transform presence from the per-SST meta descriptors, so a
    /// decoded SST header has `block_flags == 0`.
    ///
    /// Crate-internal: the `extra_flags` bag is a raw `u8` whose only valid
    /// bits live in the crate-private `block_flags` module, so an external
    /// caller could only guess magic values and a wrong bit would serialize
    /// a header claiming a transform the payload doesn't carry. External
    /// code uses the safe [`Self::write_into`] wrapper instead.
    pub(crate) fn write_into_with_flags<W: std::io::Write>(
        writer: &mut W,
        data: &[u8],
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
        extra_flags: u8,
    ) -> crate::Result<Header> {
        Self::prepare_with_flags(data, identity, transform, extra_flags)?.write_to(writer)
    }

    /// Runs the block transform pipeline (compress → encrypt → checksum → ecc)
    /// and returns a [`PreparedBlock`] ready to be framed to disk by
    /// [`PreparedBlock::write_to`]. Pure CPU work, no I/O — safe to run on a
    /// worker thread for parallel compaction. See [`Self::write_into_with_flags`]
    /// for the `extra_flags` contract.
    #[expect(
        clippy::too_many_lines,
        reason = "linear transform pipeline: compress → encrypt → checksum → ecc; \
                  each step is small but they share state (header, payload, owned buffers) \
                  so factoring would just hide the data flow"
    )]
    pub(crate) fn prepare_with_flags<'a>(
        data: &'a [u8],
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
        extra_flags: u8,
    ) -> crate::Result<PreparedBlock<'a>> {
        // Unpack the transform back to the (compression, encryption,
        // zstd_dict) triple the implementation below was written
        // against. The transform's accessor methods carry the
        // pattern-match cost; the rest of the function keeps the same
        // shape as before the API collapse.
        let compression = transform.compression();
        let encryption = transform.encryption();
        #[cfg(zstd_any)]
        let zstd_dict = transform.zstd_dict();
        // Pull block_type out of identity so the rest of the
        // function reads exactly like the pre-identity version.
        // table_id / block_offset / dict_id / window_log are
        // accepted-but-not-consumed today — they'll feed AAD
        // construction once AEAD wiring lands. Call sites that
        // compute real values now won't need a second round of
        // edits when AAD goes live.
        let block_type = identity.block_type;
        if data.len() > MAX_DECOMPRESSION_SIZE as usize {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: data.len() as u64,
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        // Self-describe the transform stack in the header. Compression /
        // encryption are derived from the active `transform`; the caller
        // ORs in any bit it can't derive (the per-KV footer). The ECC_PARITY
        // bit is NOT set here: it must agree with whether a parity trailer is
        // actually emitted, which is only known after the parity step below
        // (the ECC encoder short-circuits to a zero-length trailer for an
        // empty payload even when page_ecc is on). It is set after the parity
        // is computed, gated on a non-empty trailer, so the bit stays
        // presence-authoritative.
        let block_flags = {
            use crate::table::block::header::block_flags;
            // The header decoder rejects any bit outside `KNOWN`, so a caller
            // that ORs in a reserved bit here would write a block this build
            // can't read back. Catch that at the source in debug builds.
            debug_assert_eq!(
                extra_flags & !block_flags::KNOWN,
                0,
                "extra_flags must contain only defined block_flags bits",
            );
            let mut f = extra_flags;
            if transform.compression() != CompressionType::None {
                f |= block_flags::COMPRESSED;
            }
            if transform.encryption().is_some() {
                f |= block_flags::ENCRYPTED;
            }
            f
        };

        let mut header = Header {
            block_type,
            block_flags,
            checksum: Checksum::from_raw(0), // <-- NOTE: Is set later on
            data_length: 0,                  // <-- NOTE: Is set later on

            #[expect(clippy::cast_possible_truncation, reason = "blocks are limited to u32")]
            uncompressed_length: data.len() as u32,
        };

        // Compression step — produces an owned Vec when a compressor is active.
        #[cfg(any(feature = "lz4", zstd_any))]
        let mut compressed_buf: Option<Vec<u8>> = None;

        match compression {
            CompressionType::None => {}

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                compressed_buf = Some(lz4_flex::compress(data));
            }

            #[cfg(zstd_any)]
            CompressionType::Zstd(level) => {
                compressed_buf = Some(crate::compression::ZstdBackend::compress(data, level)?);
            }

            #[cfg(zstd_any)]
            CompressionType::ZstdDict { level, dict_id } => {
                let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got: None,
                })?;
                if dict.id() != dict_id {
                    return Err(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: Some(dict.id()),
                    });
                }

                compressed_buf = Some(crate::compression::ZstdBackend::compress_with_dict(
                    data,
                    level,
                    dict.raw(),
                )?);
            }
        }

        // Encryption step — reuses the owned compression buffer via encrypt_vec
        // when available, eliminating one allocation on the compress+encrypt path.
        let encrypted_buf: Option<Vec<u8>>;

        #[cfg(any(feature = "lz4", zstd_any))]
        {
            encrypted_buf = if let Some(enc) = encryption {
                Some(match compressed_buf.take() {
                    Some(owned) => enc.encrypt_vec(owned)?,
                    None => enc.encrypt(data)?,
                })
            } else {
                None
            };
        }

        #[cfg(not(any(feature = "lz4", zstd_any)))]
        {
            encrypted_buf = encryption.map(|enc| enc.encrypt(data)).transpose()?;
        }

        // Determine the final on-disk payload. Owns a fresh buffer when a
        // transform produced one; borrows the caller's `data` otherwise, so the
        // serial uncompressed/unencrypted path stays zero-copy.
        let payload: Cow<'a, [u8]> = if let Some(enc) = encrypted_buf {
            Cow::Owned(enc)
        } else {
            #[cfg(any(feature = "lz4", zstd_any))]
            {
                compressed_buf.map_or(Cow::Borrowed(data), Cow::Owned)
            }
            #[cfg(not(any(feature = "lz4", zstd_any)))]
            {
                Cow::Borrowed(data)
            }
        };

        // Validate the final on-disk payload against the same size limit
        // enforced on the read path (MAX_DECOMPRESSION_SIZE + encryption overhead).
        // Check in u64 first to produce the correct DecompressedSizeTooLarge error,
        // then narrow to u32 for the header field.
        //
        // NOTE: max_overhead() is used only for the LIMIT — the actual ciphertext
        // length is checked against it regardless. A buggy provider that expands
        // beyond max_overhead() will be caught by this check (payload > limit).
        // Cap at u32::MAX to guarantee the subsequent as-u32 cast is safe.
        let max_payload = (u64::from(MAX_DECOMPRESSION_SIZE)
            + encryption.map_or(0u64, |enc| u64::from(enc.max_overhead())))
        .min(u64::from(u32::MAX));

        if payload.len() as u64 > max_payload {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: payload.len() as u64,
                limit: max_payload,
            });
        }

        // Safe: payload.len() <= max_payload <= MAX_DECOMPRESSION_SIZE + overhead,
        // which is well within u32 range.
        #[expect(clippy::cast_possible_truncation, reason = "bounded by check above")]
        let payload_len = payload.len() as u32;

        header.data_length = payload_len;
        header.checksum = Checksum::from_raw(crate::hash::hash128(&payload));

        // Optional Reed-Solomon parity trailer. The parity LENGTH is not
        // stored in the header: it is `expected_parity_len(data_length)`,
        // recomputed by the reader from the presence-authoritative
        // `ECC_PARITY` block flag. On builds without the page_ecc feature,
        // transform.page_ecc() is a constant `false` (the Ecc variants of
        // BlockTransform don't exist), so the entire branch is dead and the
        // compiler folds it out.
        #[cfg(feature = "page_ecc")]
        let parity_buf: Option<Vec<u8>> = if let Some(ecc_params) = transform.ecc_params() {
            let (data_shards, parity_shards) = ecc_params.as_shards();
            let p = crate::ecc::encode_parity(&payload, data_shards, parity_shards)?;
            // parity_len is shard_bytes * RS_PARITY_SHARDS where
            // shard_bytes <= payload.len(). payload_len fits in u32
            // (checked above), so parity_len fits in u32 too; the
            // explicit try_from keeps the truncation path typed.
            let p_len =
                u32::try_from(p.len()).map_err(|_| crate::Error::DecompressedSizeTooLarge {
                    declared: p.len() as u64,
                    limit: u64::from(u32::MAX),
                })?;
            // Presence-authoritative ECC_PARITY bit: set only when a
            // non-empty parity trailer was actually emitted. An empty
            // payload yields a zero-length trailer (the encoder
            // short-circuits), so the bit stays clear and the reader
            // recomputes a zero parity length to match.
            if p_len > 0 {
                header.block_flags |= crate::table::block::header::block_flags::ECC_PARITY;
            }
            Some(p)
        } else {
            None
        };
        #[cfg(not(feature = "page_ecc"))]
        let parity_buf: Option<Vec<u8>> = None;

        Ok(PreparedBlock {
            header,
            payload,
            parity: parity_buf,
        })
    }

    /// Reads a block from a reader.
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress.
    /// When `encryption` is `None`, the decrypt step is skipped.
    ///
    /// Encryption state is determined by the caller (via [`Config`]),
    /// not recorded in the on-disk block header. With an authenticated
    /// encryption provider (such as AES-256-GCM), using the wrong key
    /// or provider will typically surface as a read/validation error
    /// (checksum, length, or decompression failure) rather than
    /// silently producing valid-looking plaintext.
    // The encrypted and unencrypted branches duplicate the checksum
    // verification and compression match because their input types
    // differ: encrypted reads into Vec<u8> (for decrypt_vec in-place
    // reuse), while unencrypted reads into Slice (zero-copy on the
    // None-compression path). Unifying them would require either a
    // Cow/enum wrapper or sacrificing the zero-copy optimization.
    #[expect(
        clippy::too_many_lines,
        reason = "encrypt/no-encrypt branches duplicate compression match — see comment above"
    )]
    pub fn from_reader<R: std::io::Read>(
        reader: &mut R,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Self> {
        let compression = transform.compression();
        let encryption = transform.encryption();
        #[cfg(zstd_any)]
        let zstd_dict = transform.zstd_dict();
        // identity carries table_id / offset / dict_id / window_log
        // for AAD construction once AEAD wiring lands; unused on the
        // read path today (block_type is derived from the parsed
        // header rather than asserted against identity.block_type —
        // mismatch detection is part of the AEAD-bound spec).
        let _ = identity;
        let header = Header::decode_from(reader)?;

        // Validate both size fields before any I/O or hashing to fail fast
        // on malformed headers. The on-disk data_length may include encryption
        // overhead (nonce + auth tag), so allow the provider's declared margin.
        // Use u64 arithmetic to avoid any possibility of u32 overflow
        // (consistent with from_file).
        let enc_overhead = encryption.map_or(0u64, |e| u64::from(e.max_overhead()));
        let max_data_length = u64::from(MAX_DECOMPRESSION_SIZE) + enc_overhead;

        if u64::from(header.data_length) > max_data_length {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.data_length),
                limit: max_data_length,
            });
        }

        if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        // Parity-trailer length is derived, not stored: when the block
        // carries a parity trailer (per `block_has_parity` — header bit for
        // self-describing blocks, descriptor-via-transform for SST blocks) it
        // is `expected_parity_len(data_length)` (the RS(4, 2) scheme is
        // deterministic), otherwise none.
        let ecc_length = if block_has_parity(&header, transform) {
            expected_parity_len(header.data_length, block_ecc_params(&header, transform))
        } else {
            0
        };

        // When encryption is active, read into a Vec so decrypt_vec can
        // reuse the buffer in-place (one allocation instead of two).
        // When no encryption, read into a Slice which may use optimized
        // reference-counted storage.
        let data = if let Some(enc) = encryption {
            // Read payload + optional ECC trailer, verify checksum
            // (with recovery on mismatch when parity is present).
            let raw_vec = Self::read_payload_and_verify(
                reader,
                header.data_length,
                ecc_length,
                header.checksum,
                block_ecc_params(&header, transform),
            )?;

            // Decrypt in-place, reusing the read buffer.
            let decrypted = enc.decrypt_vec(raw_vec)?;

            match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = decrypted.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    Slice::from(decrypted)
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut buf = vec![0u8; header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut buf)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(buf)
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    let decompressed = crate::compression::ZstdBackend::decompress(
                        &decrypted,
                        header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let decompressed = crate::compression::ZstdBackend::decompress_with_dict(
                        &decrypted,
                        dict,
                        header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            }
        } else {
            // Zero-copy fast path for non-ECC blocks (the v0..v5
            // legacy shape); ECC blocks go through the Vec-allocating
            // recovery-capable helper instead.
            let raw_data = if ecc_length == 0 {
                let s = Slice::from_reader(reader, header.data_length as usize)?;
                let checksum = Checksum::from_raw(crate::hash::hash128(&s));
                checksum.check(header.checksum).inspect_err(|_| {
                    log::error!(
                        "Checksum mismatch for <bufreader>, got={}, expected={}",
                        checksum,
                        header.checksum,
                    );
                })?;
                s
            } else {
                Slice::from(Self::read_payload_and_verify(
                    reader,
                    header.data_length,
                    ecc_length,
                    header.checksum,
                    block_ecc_params(&header, transform),
                )?)
            };

            match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = raw_data.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    raw_data
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut buf = vec![0u8; header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&raw_data, &mut buf)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(buf)
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    let decompressed = crate::compression::ZstdBackend::decompress(
                        &raw_data,
                        header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let decompressed = crate::compression::ZstdBackend::decompress_with_dict(
                        &raw_data,
                        dict,
                        header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            }
        };

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress.
    /// When `encryption` is `None`, the decrypt step is skipped.
    // Same duplication rationale as from_reader — see comment there.
    #[expect(
        clippy::too_many_lines,
        reason = "encrypt/no-encrypt branches duplicate compression match — see from_reader"
    )]
    /// Reads a block from a file, discarding the ECC status (warnings are
    /// still logged). Convenience wrapper over [`Self::from_file_with_status`]
    /// for the many call sites that don't act on the status.
    pub fn from_file(
        file: &dyn FsFile,
        handle: BlockHandle,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Self> {
        let (block, _status) = Self::from_file_with_status(file, handle, identity, transform)?;
        Ok(block)
    }

    /// Reads a block from a file and reports its [`EccStatus`].
    ///
    /// Returns `Err` only on real payload corruption (checksum mismatch with
    /// no recovery available). On success, the [`EccStatus`] distinguishes a
    /// clean read ([`EccStatus::Ok`]) from one where the block carried an ECC
    /// trailer this build could not interpret ([`EccStatus::Unrecognized`] —
    /// the payload still verified by its checksum; recompaction re-stamps it
    /// with a supported scheme). A WARN is also logged in the latter case.
    pub fn from_file_with_status(
        file: &dyn FsFile,
        handle: BlockHandle,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<(Self, EccStatus)> {
        let compression = transform.compression();
        let encryption = transform.encryption();
        #[cfg(zstd_any)]
        let zstd_dict = transform.zstd_dict();
        // identity carries AAD context; unused today. handle gives
        // the byte offset (caller already computed it), identity
        // packages it alongside the table_id + compression-context
        // fields so the read path's AEAD verification (#251) can
        // reconstruct the same AAD the writer used.
        let _ = identity;
        // handle.size() includes Header::MIN_LEN + payload +
        // optional ECC parity trailer. Encrypted blocks add
        // provider-specific overhead to the on-disk size, AND ECC
        // parity scales with the (encrypted) payload — about
        // (data_length + enc_overhead) / 2 + 4 bytes.
        //
        // Sum of parts: header + max_payload + parity(max_payload)
        // where max_payload = MAX_DECOMPRESSION_SIZE + enc_overhead.
        // A MAX_DECOMPRESSION_SIZE-only ECC bound would
        // under-approximate by ~enc_overhead/2 and reject legitimate
        // near-limit encrypted+ECC blocks the writer can produce.
        let enc_overhead = encryption.map_or(0u64, |e| u64::from(e.max_overhead()));
        let max_payload = u64::from(MAX_DECOMPRESSION_SIZE) + enc_overhead;
        // ECC parity scales with the (data_shards, parity_shards) scheme, not a
        // fixed RS(4,2). A high-overhead scheme (parity_shards > data_shards /
        // 2, e.g. RS(2,4)) produces a larger trailer than the old `N/2 + 4`
        // RS(4,2) approximation, so that bound would reject a legitimate
        // near-limit block. Size the cap from the actual scheme this block is
        // read with (`transform.ecc_params()`), keeping RS(4,2) in the running
        // max so self-describing blocks (which carry no transform ECC but are
        // written RS(4,2)) stay covered.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "max_payload is MAX_DECOMPRESSION_SIZE (+ enc overhead), well below u32::MAX"
        )]
        let max_payload_u32 = max_payload.min(u64::from(u32::MAX)) as u32;
        let max_ecc_overhead = [Some(EccParams::RS_4_2), transform.ecc_params()]
            .into_iter()
            .flatten()
            .map(|params| u64::from(expected_parity_len(max_payload_u32, params)))
            .max()
            .unwrap_or(0);
        let max_on_disk_size = max_payload + max_ecc_overhead + Header::MAX_LEN as u64;

        if u64::from(handle.size()) > max_on_disk_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: max_on_disk_size,
            });
        }

        // When encryption is active, read the whole block into an owned
        // Vec (single I/O, single allocation), parse the header, then strip
        // the header prefix so decrypt_vec operates on the payload in-place.
        // No intermediate Slice, no overlap of encrypted + decrypted buffers.
        // When no encryption, read into a Slice (zero-copy on the
        // None-compression path).
        let (header, data, ecc_status) = if let Some(enc) = encryption {
            let block_size = handle.size() as usize;

            // Pre-decode lower bound: every header is at least MIN_LEN; the
            // exact length (with or without the block_flags byte) is known
            // only after the block_type is decoded.
            if block_size < Header::MIN_LEN {
                return Err(crate::Error::InvalidHeader("Block"));
            }

            // Zero-init is redundant (read_at overwrites all bytes) but avoids
            // unsafe. The cost is negligible vs I/O + decryption. Unsafe
            // uninitialized allocation (like Slice::builder_unzeroed) could be
            // used here if profiling shows this as a bottleneck.
            let mut buf = vec![0u8; block_size];
            let n = file.read_at(&mut buf, *handle.offset())?;
            if n != block_size {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "block read_at: expected {block_size} bytes, got {n} at offset {}",
                        *handle.offset(),
                    ),
                )));
            }

            // `decode_from` reads exactly the header (variable: 33 or 34
            // bytes per block_type) and stops, leaving the payload untouched.
            let parsed_header = Header::decode_from(&mut &buf[..])?;
            let header_len = Header::header_len(parsed_header.block_type);

            // Parity-trailer length is derived from data_length + parity
            // presence (`block_has_parity`: header bit for self-describing
            // blocks, descriptor-via-transform for SST), not stored. The size
            // check below then implicitly validates it: if the on-disk byte
            // count doesn't equal header + data_length + derived-parity, the
            // header is rejected.
            let ecc_length = if block_has_parity(&parsed_header, transform) {
                expected_parity_len(
                    parsed_header.data_length,
                    block_ecc_params(&parsed_header, transform),
                )
            } else {
                0
            };

            let actual_payload_plus_ecc = block_size.saturating_sub(header_len);
            let actual_data_len = parsed_header.data_length as usize;
            // Classify the trailer (bytes after the payload). `ecc_length` is
            // non-zero only for a recognized scheme — then the trailer must
            // match it exactly (a wrong length is corruption). With no
            // recognized scheme, any extra bytes are an ECC trailer this build
            // can't interpret: the payload is still framed by `data_length` and
            // verified by its checksum, so it reads (WARN), but recovery is
            // unavailable. A short read (payload doesn't fit) is corruption.
            let trailer = actual_payload_plus_ecc
                .checked_sub(actual_data_len)
                .ok_or(crate::Error::InvalidHeader("Block"))?;
            let ecc_status = if ecc_length != 0 {
                if trailer != ecc_length as usize {
                    return Err(crate::Error::InvalidHeader("Block"));
                }
                EccStatus::Ok
            } else if trailer == 0 {
                EccStatus::Ok
            } else {
                log::warn!(
                    "block {handle:?} carries an unrecognized ECC trailer \
                     ({trailer} B); payload verified by checksum but recovery is \
                     unavailable — recompact to re-stamp with a supported scheme",
                );
                EccStatus::Unrecognized
            };

            // Payload-length safety cap. Mirrors the `from_reader`
            // check (see `Block::from_reader` above): the on-disk
            // size cap on `handle.size()` allows for ECC parity
            // overhead, so a malformed non-ECC block could declare
            // `data_length` ≈ `MAX_DECOMPRESSION_SIZE * 1.5`
            // (ECC-inclusive bound) and pass the outer check.
            // Reject those explicitly here, before any further work
            // trusts the declared payload length.
            let max_data_length = u64::from(MAX_DECOMPRESSION_SIZE) + enc_overhead;
            if u64::from(parsed_header.data_length) > max_data_length {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: u64::from(parsed_header.data_length),
                    limit: max_data_length,
                });
            }

            if parsed_header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: u64::from(parsed_header.uncompressed_length),
                    limit: u64::from(MAX_DECOMPRESSION_SIZE),
                });
            }

            // ECC fast path: no recognized parity trailer → in-buffer
            // checksum check + decrypt_vec. The checksum covers exactly the
            // `data_length` payload bytes (NOT any trailing bytes), so an
            // unrecognized opaque trailer is excluded and discarded. With a
            // recognized scheme, run the shared helper against a cursor over
            // the post-header bytes so recovery is available on mismatch.
            let buf = if ecc_length == 0 {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "actual_data_len <= post-header len"
                )]
                let checksum = Checksum::from_raw(crate::hash::hash128(
                    &buf[header_len..header_len + actual_data_len],
                ));
                checksum.check(parsed_header.checksum).inspect_err(|_| {
                    log::error!(
                        "Checksum mismatch for block {handle:?}, got={}, expected={}",
                        checksum,
                        parsed_header.checksum,
                    );
                })?;
                // Strip header prefix + any opaque trailer so buf is the payload.
                buf.copy_within(header_len..header_len + actual_data_len, 0);
                buf.truncate(actual_data_len);
                buf
            } else {
                #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                let mut cursor = std::io::Cursor::new(&buf[header_len..]);
                Self::read_payload_and_verify(
                    &mut cursor,
                    parsed_header.data_length,
                    ecc_length,
                    parsed_header.checksum,
                    block_ecc_params(&parsed_header, transform),
                )?
            };

            let decrypted = enc.decrypt_vec(buf)?;

            let data = match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = decrypted.len() as u32;

                    if parsed_header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    Slice::from(decrypted)
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut decompressed = vec![0u8; parsed_header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut decompressed)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    let decompressed = crate::compression::ZstdBackend::decompress(
                        &decrypted,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let decompressed = crate::compression::ZstdBackend::decompress_with_dict(
                        &decrypted,
                        dict,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            };

            (parsed_header, data, ecc_status)
        } else {
            // Single I/O read — header + payload in one Slice.
            let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;

            let parsed_header = Header::decode_from(&mut &buf[..])?;
            let header_len = Header::header_len(parsed_header.block_type);

            // Parity-trailer length is derived from data_length + parity
            // presence (`block_has_parity`: header bit for self-describing
            // blocks, descriptor-via-transform for SST), not stored. The size
            // check below implicitly validates it (header + data_length +
            // derived-parity must equal the buffer length).
            let ecc_length = if block_has_parity(&parsed_header, transform) {
                expected_parity_len(
                    parsed_header.data_length,
                    block_ecc_params(&parsed_header, transform),
                )
            } else {
                0
            };

            let actual_payload_plus_ecc = buf.len().saturating_sub(header_len);
            let actual_data_len = parsed_header.data_length as usize;
            // Trailer classification (see the encrypted branch above for the
            // full rationale): recognized scheme → exact trailer length;
            // no recognized scheme + extra bytes → unrecognized opaque trailer
            // (WARN, framed by data_length, no recovery); short read → corrupt.
            let trailer = actual_payload_plus_ecc
                .checked_sub(actual_data_len)
                .ok_or(crate::Error::InvalidHeader("Block"))?;
            let ecc_status = if ecc_length != 0 {
                if trailer != ecc_length as usize {
                    return Err(crate::Error::InvalidHeader("Block"));
                }
                EccStatus::Ok
            } else if trailer == 0 {
                EccStatus::Ok
            } else {
                log::warn!(
                    "block {handle:?} carries an unrecognized ECC trailer \
                     ({trailer} B); payload verified by checksum but recovery is \
                     unavailable — recompact to re-stamp with a supported scheme",
                );
                EccStatus::Unrecognized
            };

            if parsed_header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: u64::from(parsed_header.uncompressed_length),
                    limit: u64::from(MAX_DECOMPRESSION_SIZE),
                });
            }

            // Zero-copy fast path for non-recovery blocks (Off or unrecognized
            // opaque trailer); recognized-ECC blocks go through the
            // recovery-capable helper. The checksum covers exactly the
            // `data_length` payload bytes, so an opaque trailer is excluded.
            let payload_slice: Slice = if ecc_length == 0 {
                #[expect(
                    clippy::indexing_slicing,
                    reason = "actual_data_len <= post-header len"
                )]
                let checksum = Checksum::from_raw(crate::hash::hash128(
                    &buf[header_len..header_len + actual_data_len],
                ));
                checksum.check(parsed_header.checksum).inspect_err(|_| {
                    log::error!(
                        "Checksum mismatch for block {handle:?}, got={}, expected={}",
                        checksum,
                        parsed_header.checksum,
                    );
                })?;
                buf.slice(header_len..header_len + actual_data_len)
            } else {
                #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                let mut cursor = std::io::Cursor::new(&buf[header_len..]);
                Slice::from(Self::read_payload_and_verify(
                    &mut cursor,
                    parsed_header.data_length,
                    ecc_length,
                    parsed_header.checksum,
                    block_ecc_params(&parsed_header, transform),
                )?)
            };

            let data = match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = payload_slice.len() as u32;

                    if parsed_header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    payload_slice
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let compressed_data: &[u8] = &payload_slice;

                    let mut decompressed = vec![0u8; parsed_header.uncompressed_length as usize];

                    let bytes_written =
                        lz4_flex::decompress_into(compressed_data, &mut decompressed)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    let compressed_data: &[u8] = &payload_slice;

                    let decompressed = crate::compression::ZstdBackend::decompress(
                        compressed_data,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(zstd_any)]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let compressed_data: &[u8] = &payload_slice;

                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let decompressed = crate::compression::ZstdBackend::decompress_with_dict(
                        compressed_data,
                        dict,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            };

            (parsed_header, data, ecc_status)
        };

        Ok((Self { header, data }, ecc_status))
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::cast_possible_truncation,
    clippy::expect_used,
    reason = "test code"
)]
mod tests {
    use super::*;
    use test_log::test;

    /// Result of [`write_block_to_tempfile`]. Bundles the open
    /// file, the pre-computed [`crate::table::BlockHandle`], and
    /// the owning [`tempfile::TempDir`].
    ///
    /// **Drop-order safety lives entirely in the struct field
    /// order** (Windows portability constraint). Rust drops
    /// struct fields in declaration order, so `file` is first
    /// and `dir` LAST: when a `TempBlock` value goes out of
    /// scope, the open file handle closes before the `TempDir`
    /// removes the directory. Windows rejects directory
    /// removal while a file inside it is still open.
    ///
    /// **Callers SHOULD keep the result as a single binding**
    /// — borrow `&tmp.file` and copy `tmp.handle` (it's `Copy`)
    /// instead of destructuring. Destructuring opens a
    /// foot-gun: local bindings drop in REVERSE declaration
    /// order, so a pattern like
    /// `let TempBlock { file, handle, dir: _dir } = ...?;`
    /// would close `dir` before `file` and break the
    /// invariant. Holding the whole struct as one local
    /// (`let tmp = ...?;`) makes the struct field order the
    /// SOLE source of truth — no pattern can break it.
    struct TempBlock {
        /// Open read-only handle on the persisted block.
        /// Declared first so it drops before `dir`.
        file: std::fs::File,
        /// Pre-computed handle: offset 0, length = header +
        /// payload, ready to pass straight into `Block::from_file`.
        handle: crate::table::BlockHandle,
        /// Drop guard for the tempdir. Kept bound for the test
        /// lifetime so the file inside the directory survives
        /// long enough to be reopened by the test reader, then
        /// dropped at end-of-scope to reclaim the directory.
        /// Declared LAST so the file handle above closes before
        /// this removes the directory.
        ///
        /// Feature-matrix-gated suppression: under `--all-features`
        /// the ECC-corruption tests read `.path()` to flip on-disk
        /// bytes, so the field IS used and an unconditional
        /// `#[allow(dead_code)]` would clash with
        /// `clippy::used_underscore_binding` if we tried to prefix
        /// the field name with `_`. Under default features the
        /// `page_ecc` tests are not compiled, the field genuinely
        /// IS dead, and the suppression silences the warning.
        #[cfg_attr(
            not(feature = "page_ecc"),
            expect(dead_code, reason = "drop guard; only read by page_ecc-gated tests")
        )]
        dir: tempfile::TempDir,
    }

    /// Shared scaffold for `Block::from_file` roundtrip tests: writes
    /// `data` through `Block::write_into` directly into a fresh
    /// tempdir-backed file, reopens it read-only, and returns a
    /// [`TempBlock`] bundling the open file, the pre-computed
    /// [`crate::table::BlockHandle`], and the owning [`tempfile::TempDir`]
    /// (kept bound for the test's lifetime). The streaming write
    /// avoids an intermediate `Vec<u8>` — relevant for the 32 KiB
    /// large-payload encryption test.
    ///
    /// Centralises the ~10× write/sync/reopen/handle boilerplate that
    /// the `from_file` tests below would otherwise duplicate.
    fn write_block_to_tempfile(
        data: &[u8],
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<TempBlock> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        // Scope the write-side file handle so the read-side
        // `File::open` below sees a fully-flushed file. Dropping
        // closes; sync_all flushes before close.
        let header = {
            let mut file = std::fs::File::create(&path)?;
            let header = Block::write_into(&mut file, data, identity, transform)?;
            file.sync_all()?;
            header
        };
        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
        Ok(TempBlock { file, handle, dir })
    }

    #[test]
    fn block_from_file_roundtrip_uncompressed() -> crate::Result<()> {
        let data = b"abcdefabcdefabcdef";
        let tmp = write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_roundtrip_lz4() -> crate::Result<()> {
        let data = b"abcdefabcdefabcdef";
        let tmp = write_block_to_tempfile(
            data,
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_from_file_roundtrip_zstd() -> crate::Result<()> {
        let data = b"abcdefabcdefabcdef";
        let tmp = write_block_to_tempfile(
            data,
            BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        let block = Block::from_file(
            &tmp.file,
            tmp.handle,
            BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;
        assert_eq!(data, &*block.data);
        Ok(())
    }

    #[test]
    fn block_roundtrip_uncompressed() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    None,
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }
    #[test]
    #[cfg(feature = "lz4")]
    fn block_roundtrip_lz4() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    None,
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_reject_absurd_uncompressed_length() {
        use crate::coding::Encode;

        // Write a valid lz4-compressed block first so we get the right header format
        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        )
        .unwrap();

        // Tamper the header: set uncompressed_length to u32::MAX.
        // The block checksum only covers the compressed payload bytes; it does not include
        // header fields. The header itself has its own checksum, which we recompute below
        // by re-encoding the modified header, so the tampered block remains internally
        // consistent while exercising the DecompressedSizeTooLarge path.
        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = u32::MAX;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(
            &mut r,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_zero_uncompressed_length_with_data_fails_decompress() {
        use crate::coding::Encode;

        // Zero uncompressed_length is allowed (valid for empty blocks), but when
        // the compressed payload is non-empty, lz4 decompression will fail because
        // the output buffer is zero-sized.
        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = 0;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(
            &mut r,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn lz4_corrupted_uncompressed_length_triggers_decompress_error() {
        use crate::coding::Encode;
        use std::io::Cursor;

        let payload: &[u8] = b"hello world";

        // Compress with lz4 using the block format
        let compressed = lz4_flex::compress(payload);

        // Build a header with corrupted uncompressed_length (1 byte too large)
        let data_length = compressed.len() as u32;
        let uncompressed_length_correct = payload.len() as u32;
        let uncompressed_length_corrupted = uncompressed_length_correct + 1;

        let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

        let header = Header {
            data_length,
            uncompressed_length: uncompressed_length_corrupted,
            checksum,
            ..Header::test_dummy(BlockType::Data)
        };

        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&compressed);

        let mut cursor = Cursor::new(buf);
        let result = Block::from_reader(
            &mut cursor,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        match result {
            Err(crate::Error::Decompress(CompressionType::Lz4)) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_reject_absurd_uncompressed_length() {
        use crate::coding::Encode;
        use std::io::Write;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        )
        .unwrap();

        // Tamper: set uncompressed_length to u32::MAX.
        // The block checksum only covers the compressed payload bytes; it does not include
        // header fields. The header itself has its own checksum, which we recompute below
        // by re-encoding the modified header, so the tampered block remains internally
        // consistent while exercising the DecompressedSizeTooLarge path.
        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = u32::MAX;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&tampered).unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
        let result = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_zero_uncompressed_length_with_data_fails_decompress() {
        use crate::coding::Encode;
        use std::io::Write;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = 0;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&tampered).unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
        let result = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Lz4,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {:?}",
            result.err(),
        );
    }

    #[test]
    fn block_from_reader_reject_absurd_data_length() {
        use crate::coding::Encode;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let payload: Vec<u8> = reader.to_vec();

        // Set data_length past the limit (no encryption → overhead is 0)
        header.data_length = MAX_DECOMPRESSION_SIZE + 1;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(
            &mut r,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    fn block_from_file_reject_oversized_handle() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"dummy").unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), u32::MAX);
        let result = Block::from_file(
            &file,
            handle,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(zstd_any)]
    fn zstd_corrupted_uncompressed_length_triggers_decompress_error() {
        use crate::coding::Encode;
        use std::io::Cursor;

        let payload: &[u8] = b"hello world";

        // Fully-qualified path resolves the trait method unambiguously without
        // needing `use CompressionProvider` in this test module scope.
        let compressed =
            crate::compression::ZstdBackend::compress(payload, 3).expect("zstd compress failed");

        let data_length = compressed.len() as u32;
        let uncompressed_length_corrupted = payload.len() as u32 + 1;

        let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

        let header = Header {
            data_length,
            uncompressed_length: uncompressed_length_corrupted,
            checksum,
            ..Header::test_dummy(BlockType::Data)
        };

        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&compressed);

        let mut cursor = Cursor::new(buf);
        let result = Block::from_reader(
            &mut cursor,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        match result {
            Err(crate::Error::Decompress(CompressionType::Zstd(_))) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }

    #[test]
    #[cfg(zstd_any)]
    fn zstd_decreased_uncompressed_length_triggers_decompress_error() {
        use crate::coding::Encode;
        use std::io::Cursor;

        let payload: &[u8] = b"hello world hello world hello world";

        let compressed =
            crate::compression::ZstdBackend::compress(payload, 3).expect("zstd compress failed");

        let data_length = compressed.len() as u32;
        // Set uncompressed_length smaller than real decompressed size.
        // The backend decompresses into a buffer of this size; the real output
        // exceeds it, triggering the capacity/length mismatch error.
        let uncompressed_length_too_small = payload.len() as u32 - 1;

        let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

        let header = Header {
            data_length,
            uncompressed_length: uncompressed_length_too_small,
            checksum,
            ..Header::test_dummy(BlockType::Data)
        };

        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&compressed);

        let mut cursor = Cursor::new(buf);
        let result = Block::from_reader(
            &mut cursor,
            crate::table::block::BlockIdentity::for_test(
                0,
                0,
                crate::table::block::BlockType::Data,
            ),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );

        match result {
            Err(crate::Error::Decompress(CompressionType::Zstd(_))) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_roundtrip_zstd() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    None,
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }

    #[test]
    fn block_write_rejects_oversized_payload() {
        let oversized = vec![0u8; MAX_DECOMPRESSION_SIZE as usize + 1];
        let mut sink = std::io::sink();
        let result = Block::write_into(
            &mut sink,
            &oversized,
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::None,
                None,
                #[cfg(zstd_any)]
                None,
            )
            .unwrap(),
        );
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
    }

    #[test]
    #[cfg(zstd_any)]
    fn block_roundtrip_zstd_large_data() -> crate::Result<()> {
        let data = vec![0xABu8; 64 * 1024]; // 64KB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
            &crate::table::block::BlockTransform::from_parts(
                CompressionType::Zstd(3),
                None,
                #[cfg(zstd_any)]
                None,
            )?,
        )?;

        // Verify compression actually reduced size
        assert!(
            writer.len() < data.len(),
            "zstd should compress repeated data"
        );

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    None,
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
        }

        Ok(())
    }

    // --- Encrypted block roundtrip tests ---
    // These exercise the encrypt_vec/decrypt_vec code paths in write_into,
    // from_reader, and from_file that are untouched by the non-encrypted tests.
    //
    // NOTE: The tempfile + write + reopen + handle pattern is duplicated across
    // from_file tests (both encrypted and non-encrypted). Tracked in #127.

    #[cfg(feature = "encryption")]
    mod encrypted {
        use crate::table::block::*;

        fn test_provider() -> crate::encryption::Aes256GcmProvider {
            crate::encryption::Aes256GcmProvider::new(&[0x42; 32])
        }

        #[test]
        fn block_roundtrip_encrypted_uncompressed() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"plaintext block data for encryption test";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_roundtrip_encrypted_lz4() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(zstd_any)]
        fn block_roundtrip_encrypted_zstd() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_uncompressed() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"plaintext block data for from_file encryption test";
            let tmp = super::write_block_to_tempfile(
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            let block = Block::from_file(
                &tmp.file,
                tmp.handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_from_file_encrypted_lz4() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let tmp = super::write_block_to_tempfile(
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            let block = Block::from_file(
                &tmp.file,
                tmp.handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(zstd_any)]
        fn block_from_file_encrypted_zstd() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let tmp = super::write_block_to_tempfile(
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            let block = Block::from_file(
                &tmp.file,
                tmp.handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_wrong_key_fails() -> crate::Result<()> {
            let enc_write = test_provider();
            let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
            let data = b"encrypted block data";
            let tmp = super::write_block_to_tempfile(
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc_write),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            let result = Block::from_file(
                &tmp.file,
                tmp.handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc_read),
                    #[cfg(zstd_any)]
                    None,
                )?,
            );
            assert!(
                matches!(result, Err(crate::Error::Decrypt(_))),
                "expected Decrypt error for wrong key, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_reader_encrypted_wrong_key_fails() -> crate::Result<()> {
            let enc_write = test_provider();
            let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
            let data = b"encrypted block data";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc_write),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let result = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc_read),
                    #[cfg(zstd_any)]
                    None,
                )?,
            );
            assert!(
                matches!(result, Err(crate::Error::Decrypt(_))),
                "expected Decrypt error for wrong key, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_checksum_tamper_detected() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = b"data for tamper test";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            // Tamper a byte in the encrypted payload (after header)
            let mid = Header::MIN_LEN + 1;
            if mid < buf.len() {
                #[expect(clippy::indexing_slicing, reason = "mid < buf.len() checked above")]
                {
                    buf[mid] ^= 0xFF;
                }
            }

            let dir = tempfile::tempdir()?;
            let path = dir.path().join("block");
            let mut file = std::fs::File::create(&path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
            drop(file);

            let file = std::fs::File::open(&path)?;
            let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
            let result = Block::from_file(
                &file,
                handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            );
            assert!(
                matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
                "expected ChecksumMismatch for tampered data, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_undersized_handle_rejected() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("block");
            let mut file = std::fs::File::create(&path)?;
            file.write_all(b"tiny")?;
            file.sync_all()?;
            drop(file);

            let file = std::fs::File::open(&path)?;
            // Handle size smaller than Header::MIN_LEN
            let handle = crate::table::BlockHandle::new(BlockOffset(0), 2);
            let result = Block::from_file(
                &file,
                handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            );

            assert!(
                matches!(result, Err(crate::Error::InvalidHeader(_))),
                "expected InvalidHeader for undersized handle, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_uncompressed_large_payload() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xBB_u8; 32 * 1024]; // 32 KiB
            let tmp = super::write_block_to_tempfile(
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            let block = Block::from_file(
                &tmp.file,
                tmp.handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        fn block_roundtrip_encrypted_uncompressed_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xCC_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::None,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_roundtrip_encrypted_lz4_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xDD_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Lz4,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        #[cfg(zstd_any)]
        fn block_roundtrip_encrypted_zstd_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xEE_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    CompressionType::Zstd(3),
                    Some(&enc),
                    #[cfg(zstd_any)]
                    None,
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd_dict {
        use super::*;
        use crate::compression::ZstdDictionary;
        use test_log::test;

        fn test_dict() -> ZstdDictionary {
            let mut samples = Vec::new();
            for i in 0u32..500 {
                samples.extend_from_slice(format!("key-{i:05}val-{i:05}").as_bytes());
            }
            ZstdDictionary::new(&samples)
        }

        fn test_compression(dict: &ZstdDictionary) -> CompressionType {
            CompressionType::ZstdDict {
                level: 3,
                dict_id: dict.id(),
            }
        }

        #[test]
        fn block_roundtrip_zstd_dict_reader() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_roundtrip_zstd_dict_file() -> crate::Result<()> {
            use std::io::Write;

            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"abcdefabcdefabcdef";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;

            let dir = tempfile::tempdir()?;
            let path = dir.path().join("block");
            let mut file = std::fs::File::create(&path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
            drop(file);

            let file = std::fs::File::open(&path)?;
            let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
            let block = Block::from_file(
                &file,
                handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_roundtrip_zstd_dict_large_data() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = vec![0xAB_u8; 64 * 1024]; // 64 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;

            assert!(
                writer.len() < data.len(),
                "dict compression should reduce size"
            );

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    None,
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        fn block_zstd_dict_wrong_dict_returns_error() {
            // Companion test to
            // `block_transform_from_parts_zstd_dict_missing_returns_error`
            // (below): both assert the BlockTransform::from_parts
            // check that used to live inside Block::write_into /
            // from_reader for the ZstdDict codec. The dict-missing
            // case exercises the `None` half of the dict argument;
            // this one exercises the cross-check between the
            // supplied dictionary id and the
            // ZstdDict { dict_id } discriminator. Both assert
            // directly on the transform-construction result; no
            // Block I/O call is needed to exercise the mismatch
            // path.
            let dict = test_dict();
            let compression = test_compression(&dict);
            let wrong_dict = ZstdDictionary::new(b"completely different dictionary bytes");

            let result = crate::table::block::BlockTransform::from_parts(
                compression,
                None,
                Some(&wrong_dict),
            );
            assert!(
                matches!(
                    result,
                    Err(crate::Error::ZstdDictMismatch { got: Some(_), .. })
                ),
                "expected ZstdDictMismatch with got=Some",
            );
        }

        #[test]
        fn block_transform_from_parts_zstd_dict_missing_returns_error() {
            // The runtime dict-presence check that used to live inside
            // Block::write_into / from_reader for the ZstdDict codec
            // is now centralised in BlockTransform::from_parts. The
            // error therefore surfaces at transform-construction time
            // instead of at the Block I/O call; this test verifies
            // that earlier surface — it no longer exercises
            // Block::write_into / from_reader at all, hence the test
            // name describes from_parts rather than block_write_*.
            // (See block_zstd_dict_wrong_dict_returns_error above for
            // the matching "wrong dict id" path.)
            let dict = test_dict();
            let compression = test_compression(&dict);

            // Try to construct the read-side transform without
            // providing the dict the codec needs.
            let result = crate::table::block::BlockTransform::from_parts(compression, None, None);
            // BlockTransform holds `&dyn EncryptionProvider` which
            // doesn't impl Debug, so we can't print the whole result;
            // surface just the Err side (which IS Debug) on mismatch.
            // Match on `&result` + `.as_ref().err()` so the variant
            // check and the formatter borrow the same value — no
            // need to reason about whether the matches! patterns
            // bind by value.
            assert!(
                matches!(
                    &result,
                    Err(crate::Error::ZstdDictMismatch { got: None, .. })
                ),
                "expected ZstdDictMismatch, got: {:?}",
                result.as_ref().err(),
            );
        }

        #[test]
        #[cfg(feature = "encryption")]
        fn block_roundtrip_zstd_dict_encrypted_reader() -> crate::Result<()> {
            let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"encrypted-dict-compressed-data-for-test";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "encryption")]
        fn block_roundtrip_zstd_dict_encrypted_file() -> crate::Result<()> {
            use std::io::Write;

            let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = vec![0xCC_u8; 16 * 1024]; // 16 KiB
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                &data,
                crate::table::block::BlockIdentity::for_test(0, 0, BlockType::Data),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;

            let dir = tempfile::tempdir()?;
            let path = dir.path().join("block");
            let mut file = std::fs::File::create(&path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
            drop(file);

            let file = std::fs::File::open(&path)?;
            let handle = crate::table::BlockHandle::new(BlockOffset(0), header.on_disk_size());
            let block = Block::from_file(
                &file,
                handle,
                crate::table::block::BlockIdentity::for_test(
                    0,
                    0,
                    crate::table::block::BlockType::Data,
                ),
                &crate::table::block::BlockTransform::from_parts(
                    compression,
                    Some(&enc),
                    #[cfg(zstd_any)]
                    Some(&dict),
                )?,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }
    }

    /// Page ECC integration tests — write a block with the
    /// `BlockTransform::*Ecc` variant, verify the on-disk layout
    /// round-trips through `Block::from_reader`, and verify that
    /// Reed-Solomon recovery kicks in when the payload bytes are
    /// corrupted between write and read.
    #[cfg(feature = "page_ecc")]
    mod page_ecc {
        use super::*;
        use test_log::test;

        const PAYLOAD: &[u8] = b"the quick brown fox jumps over the lazy dog \
                                 0123456789 the quick brown fox jumps over \
                                 the lazy dog 0123456789";

        #[test]
        fn block_roundtrip_plain_ecc_clean_read() -> crate::Result<()> {
            let mut writer = vec![];
            let header = Block::write_into(
                &mut writer,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;

            assert!(
                header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0,
                "PlainEcc writer must set the ECC_PARITY flag",
            );
            assert_eq!(
                writer.len(),
                header.on_disk_size() as usize,
                "on-disk size must equal header + payload + derived parity length",
            );

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            assert_eq!(&*block.data, PAYLOAD);
            Ok(())
        }

        #[test]
        fn block_roundtrip_plain_ecc_recovers_from_single_byte_flip() -> crate::Result<()> {
            let mut writer = vec![];
            let header = Block::write_into(
                &mut writer,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;

            // Flip a single byte inside the payload region (after
            // the header, before the parity trailer) so the on-disk
            // bytes' XXH3 no longer matches header.checksum but the
            // recoverable shape (1 of 6 shards corrupted) holds.
            let header_len = Header::MIN_LEN;
            let flip_at = header_len + (header.data_length as usize) / 2;
            writer[flip_at] ^= 0xFF;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            // ECC recovery reconstructs the original payload despite
            // the in-flight bit-flip.
            assert_eq!(
                &*block.data, PAYLOAD,
                "Reed-Solomon recovery must reconstruct the original \
                 payload from a single-byte data-shard flip",
            );
            Ok(())
        }

        #[test]
        fn block_from_file_plain_ecc_recovers_from_single_byte_flip() -> crate::Result<()> {
            let tmp = super::write_block_to_tempfile(
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            let path = tmp.dir.path().join("block");

            // Flip one byte inside the payload region (after the
            // header, before the parity trailer).
            let mut bytes = std::fs::read(&path)?;
            let payload_start = Header::MIN_LEN;
            bytes[payload_start + 3] ^= 0x80;
            std::fs::write(&path, &bytes)?;

            // Re-open and read via from_file. ECC recovery should
            // reconstruct the original payload.
            let file = std::fs::File::open(&path)?;
            let block = Block::from_file(
                &file,
                tmp.handle,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            assert_eq!(&*block.data, PAYLOAD);
            Ok(())
        }

        /// Three-state read contract: a block written WITH a parity trailer,
        /// read back by a reader that does NOT recognize the scheme (a `Plain`
        /// transform — as happens when the per-SST descriptor decodes to an
        /// unsupported scheme), must still return the payload (framed by
        /// `data_length`, checksum-verified) and report
        /// `EccStatus::Unrecognized` — a soft warning, not a read error. Read
        /// with the matching scheme, it reports `EccStatus::Ok`.
        #[test]
        fn from_file_with_status_soft_warns_on_unrecognized_trailer() -> crate::Result<()> {
            let tmp = super::write_block_to_tempfile(
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;

            // Reader recognizes the scheme → clean read.
            let (block, status) = Block::from_file_with_status(
                &tmp.file,
                tmp.handle,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            assert_eq!(&*block.data, PAYLOAD);
            assert_eq!(status, EccStatus::Ok);

            // Reader does NOT recognize the trailer (Plain transform): the
            // opaque trailer is excluded from the payload, the checksum still
            // verifies, and the status is a warning rather than an error.
            let (block, status) = Block::from_file_with_status(
                &tmp.file,
                tmp.handle,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PLAIN,
            )?;
            assert_eq!(
                &*block.data, PAYLOAD,
                "payload reads despite unknown trailer"
            );
            assert_eq!(status, EccStatus::Unrecognized);
            Ok(())
        }

        /// Same recovery story as `PlainEcc` but with lz4 compression
        /// stacked on top: parity is computed over the
        /// post-compression payload, recovery happens BEFORE
        /// decompression. Catches a regression where the read path
        /// would try to decompress corrupt bytes (lz4 would fail
        /// before recovery had a chance to fire).
        #[cfg(feature = "lz4")]
        #[test]
        fn block_roundtrip_compressed_ecc_recovers_from_byte_flip() -> crate::Result<()> {
            let mut writer = vec![];
            let header = Block::write_into(
                &mut writer,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::CompressedEcc(
                    CompressionContext::new(CompressionType::Lz4)?,
                    EccParams::RS_4_2,
                ),
            )?;
            assert!(header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0);

            // Flip a byte in the compressed-payload region.
            let header_len = Header::MIN_LEN;
            let flip_at = header_len + (header.data_length as usize) / 2;
            writer[flip_at] ^= 0x55;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::CompressedEcc(
                    CompressionContext::new(CompressionType::Lz4)?,
                    EccParams::RS_4_2,
                ),
            )?;
            assert_eq!(
                &*block.data, PAYLOAD,
                "ECC must recover the compressed bytes BEFORE lz4 \
                 decompression, otherwise lz4 would fail on corrupt input",
            );
            Ok(())
        }

        /// ECC-protected encrypted roundtrip with a single-byte
        /// ciphertext flip. Parity is computed over the ciphertext,
        /// so recovery must produce byte-exact reconstruction —
        /// AEAD authentication fails on even a one-bit mismatch.
        /// This catches a regression where ECC recovery rebuilds an
        /// arithmetically valid Reed-Solomon shard that doesn't
        /// bit-identically reproduce the original ciphertext.
        #[cfg(feature = "encryption")]
        #[test]
        fn block_roundtrip_encrypted_ecc_recovers_from_byte_flip() -> crate::Result<()> {
            let enc = crate::encryption::Aes256GcmProvider::new(&[0x42; 32]);
            let mut writer = vec![];
            let header = Block::write_into(
                &mut writer,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::EncryptedEcc(&enc, EccParams::RS_4_2),
            )?;
            assert!(header.block_flags & crate::table::block::header::block_flags::ECC_PARITY != 0);

            // Flip one byte in the ciphertext region.
            let header_len = Header::MIN_LEN;
            let flip_at = header_len + (header.data_length as usize) / 2;
            writer[flip_at] ^= 0x21;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::EncryptedEcc(&enc, EccParams::RS_4_2),
            )?;
            assert_eq!(
                &*block.data, PAYLOAD,
                "ECC must reconstruct ciphertext byte-exactly so AEAD \
                 authentication succeeds on the recovered bytes",
            );
            Ok(())
        }

        /// Asserts the unrecoverable path surfaces
        /// `Error::PageEccUnrecoverable`. Corrupts enough on-disk
        /// bytes to take out more than the RS(4, 2) scheme can
        /// recover (≥ 3 data shards damaged), so every C(6, 4)
        /// subset trial decode fails the xxh3 oracle and
        /// `try_recover` exhausts all 15 candidates.
        #[test]
        fn block_roundtrip_plain_ecc_unrecoverable_when_too_many_shards_corrupt()
        -> crate::Result<()> {
            let mut writer = vec![];
            let header = Block::write_into(
                &mut writer,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;

            // Shard size in bytes — same formula as crate::ecc::shard_bytes
            // (ceil(payload_len / 4) rounded up to even).
            let payload_len = header.data_length as usize;
            let shard_bytes = ((payload_len.div_ceil(4)) + 1) & !1;

            // Flip one byte in EACH of the first 3 data shards.
            // RS(4, 2) recovers up to 2 missing shards; with 3
            // corrupted data shards no subset of 4 intact shards
            // reconstructs the original.
            let payload_start = Header::MIN_LEN;
            for shard_idx in 0..3 {
                let pos = payload_start + shard_idx * shard_bytes;
                if pos < writer.len() {
                    writer[pos] ^= 0xFF;
                }
            }

            let mut reader = &writer[..];
            let result = Block::from_reader(
                &mut reader,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            );
            match result {
                Ok(_) => panic!(
                    "3-shard corruption must exceed RS(4,2) recovery capacity, \
                     but from_reader returned Ok"
                ),
                Err(crate::Error::PageEccUnrecoverable { .. }) => {}
                Err(e) => panic!("expected PageEccUnrecoverable, got {e:?}"),
            }
            Ok(())
        }

        #[test]
        fn ecc_parity_bit_agrees_with_emitted_parity_length() -> crate::Result<()> {
            use crate::table::block::header::block_flags;

            // Empty payload: the Reed-Solomon encoder short-circuits to a
            // zero-length parity trailer even under PlainEcc, so the
            // presence-authoritative ECC_PARITY bit must stay CLEAR — and the
            // derived on-disk size carries no parity (header + 0 payload).
            let mut empty_buf = vec![];
            let empty = Block::write_into(
                &mut empty_buf,
                &[],
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            assert_eq!(
                empty.block_flags & block_flags::ECC_PARITY,
                0,
                "ECC_PARITY must be clear when no parity trailer is emitted",
            );
            assert_eq!(
                empty_buf.len(),
                empty.on_disk_size() as usize,
                "on-disk size matches the derived (zero) parity length",
            );
            assert_eq!(
                empty.on_disk_size() as usize,
                Header::MIN_LEN,
                "empty payload emits no parity, so on-disk size is just the header",
            );

            // Non-empty payload: parity is emitted, so the bit is set and the
            // derived on-disk size includes the parity trailer.
            let mut full_buf = vec![];
            let full = Block::write_into(
                &mut full_buf,
                PAYLOAD,
                BlockIdentity::for_test(0, 0, BlockType::Data),
                &BlockTransform::PlainEcc(EccParams::RS_4_2),
            )?;
            assert_ne!(
                full.block_flags & block_flags::ECC_PARITY,
                0,
                "ECC_PARITY must be set when a parity trailer is emitted",
            );
            assert_eq!(
                full_buf.len(),
                full.on_disk_size() as usize,
                "on-disk size matches header + payload + derived parity",
            );
            assert!(
                full.on_disk_size() as usize > Header::MIN_LEN + full.data_length as usize,
                "non-empty payload emits a parity trailer beyond header + payload",
            );
            Ok(())
        }
    }
}
