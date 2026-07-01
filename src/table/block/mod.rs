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

/// Exact parity trailer size for a given data length under the supplied
/// shard scheme `params` (`data_shards`, `parity_shards`).
///
/// `parity_len(N) = shard_bytes(N) * parity_shards` where
/// `shard_bytes(N) = ceil(N / data_shards) rounded up to the nearest even`.
/// Mirrors `crate::ecc::parity_len` byte-for-byte but is available
/// without the `page_ecc` cargo feature — the parity trailer length is
/// NOT stored in the block header; the read path derives it from
/// `data_length` and the per-SST scheme whenever a block's `ECC_PARITY`
/// flag is set, so writer and reader always agree on the trailer size
/// without a per-block length field to corrupt or forge.
///
/// Returns zero for `data_length == 0` to match
/// `crate::ecc::encode_parity`'s empty-payload short-circuit.
#[inline]
pub(crate) fn expected_parity_len(data_length: u32, params: EccParams) -> u32 {
    // SEC-DED: one parity byte per 8-byte word (matches
    // `crate::secded::block_parity_len`); no shard arithmetic.
    let (data_shards, parity_shards) = match params {
        EccParams::Secded => return data_length.div_ceil(8),
        EccParams::Shard {
            data_shards,
            parity_shards,
        } => (u32::from(data_shards), u32::from(parity_shards)),
    };
    if data_length == 0 || data_shards == 0 || parity_shards == 0 {
        return 0;
    }
    // ceil(N / data_shards). The division never increases the value and the
    // remainder bump only fires when there IS a remainder (so the quotient is
    // already below the dividend), keeping this within u32 — plain arithmetic.
    let ceil = (data_length / data_shards) + u32::from(!data_length.is_multiple_of(data_shards));
    // Round up to even (the `reed-solomon-simd` engine requires shard sizes that
    // are a multiple of two; XOR shares the layout). With `data_shards == 1` and
    // a `u32::MAX` data length, `ceil` is an odd `u32::MAX`, so the +1 must
    // saturate; a corrupt header reaching here is rejected downstream.
    let shard_bytes = ceil.saturating_add(u32::from(!ceil.is_multiple_of(2)));
    // `parity_shards` is a u8 (≤ 255), so for a large block with many parity
    // shards the product CAN exceed u32 — saturate. An over-large parity length
    // is rejected against the actual block downstream, so the clamp is safe.
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

/// On-disk `CompressionType` tag byte (spec §5.1 registry: 0 none, 1 lz4,
/// 3 zstd, 4 zstd+dict). Used as the AAD-bound `compression_type` field so a
/// codec relabel (e.g. forging a different window/dict context) fails AEAD
/// verification. Mirrors the leading byte of [`CompressionType`]'s `Encode`.
#[cfg(zstd_any)]
fn compression_tag_byte(compression: CompressionType) -> u8 {
    match compression {
        CompressionType::None => 0,
        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => 1,
        CompressionType::Zstd(_) => 3,
        CompressionType::ZstdDict { .. } => 4,
    }
}

/// Seal a (post-compression) block payload for the on-disk encryption envelope.
///
/// Under zstd builds this is the AAD-bound `MetadataFrame ‖ BodyFrame` envelope
/// ([`EncryptionProvider::encrypt_block_aad`](crate::EncryptionProvider::encrypt_block_aad)) binding the block identity +
/// transform context. Without zstd the wire-format frame is unavailable, so it
/// falls back to the opaque `[nonce ‖ ciphertext ‖ tag]` form. `owned` is the
/// compressor's output (reused in-place on the opaque path); `borrow` is the
/// raw `data` for the uncompressed case.
#[cfg_attr(
    zstd_any,
    expect(
        clippy::needless_pass_by_value,
        reason = "owned is consumed by encrypt_vec on the non-zstd path; the AAD \
                  path only borrows it, so by-value is needed for the other cfg"
    )
)]
fn encrypt_block_payload(
    enc: &dyn crate::encryption::EncryptionProvider,
    owned: Option<Vec<u8>>,
    borrow: &[u8],
    identity: &BlockIdentity,
    compression: CompressionType,
    block_flags: u8,
) -> crate::Result<Vec<u8>> {
    #[cfg(zstd_any)]
    {
        let plaintext = owned.as_deref().unwrap_or(borrow);
        // The `ECC_PARITY` flag describes the Reed-Solomon parity trailer,
        // which is OUTER framing: parity is computed over the (encrypted)
        // payload AFTER this seal and stripped BEFORE decrypt, so the bit is
        // not part of the encrypted content and must not enter the AAD. It is
        // also unknown here (set only once the trailer is emitted, later in
        // `prepare_with_flags`). Masking it keeps seal == verify regardless of
        // pipeline ordering; its integrity is self-enforced (a flipped bit
        // mis-strips the trailer, so the AEAD then runs over wrong bytes and
        // fails). The plaintext-affecting transform bits (COMPRESSED /
        // ENCRYPTED / KV_CHECKSUM_FOOTER) stay bound.
        let aad_block_flags = block_flags & !crate::table::block::header::block_flags::ECC_PARITY;
        enc.encrypt_block_aad(
            plaintext,
            identity,
            compression_tag_byte(compression),
            aad_block_flags,
        )
    }
    #[cfg(not(zstd_any))]
    {
        let _ = (identity, compression, block_flags);
        match owned {
            Some(buf) => enc.encrypt_vec(buf),
            None => enc.encrypt(borrow),
        }
    }
}

/// Inverse of [`encrypt_block_payload`]: recover the plaintext from the on-disk
/// envelope. Under zstd builds it verifies the AAD binding via
/// [`EncryptionProvider::decrypt_block_aad`](crate::EncryptionProvider::decrypt_block_aad) (the reader supplies only
/// `identity`; the transform fields are read back from the frame); without zstd
/// it falls back to the opaque in-place `decrypt_vec`.
fn decrypt_block_payload(
    enc: &dyn crate::encryption::EncryptionProvider,
    raw: &[u8],
    identity: &BlockIdentity,
) -> crate::Result<Vec<u8>> {
    #[cfg(zstd_any)]
    {
        enc.decrypt_block_aad(raw, identity)
    }
    #[cfg(not(zstd_any))]
    {
        let _ = identity;
        // `decrypt_vec` consumes an owned buffer; the read path now hands us a
        // borrowed Slice, so copy once (this branch is the no-zstd build only).
        enc.decrypt_vec(raw.to_vec())
    }
}

/// Classifies the on-disk trailer (bytes after the `data_length` payload) into
/// an [`EccStatus`], or `Err` on a framing violation.
///
/// The decision keys off whether the block carries a RECOGNIZED ECC layout
/// (`has_recognized_ecc`, from [`block_has_parity`]), NOT off `ecc_length`:
/// a recognized layout on an empty payload also has `ecc_length == 0`, and its
/// trailer must still be exactly zero. So:
/// - recognized layout → require `trailer == ecc_length` exactly (extra bytes
///   are corruption), even when `ecc_length == 0`;
/// - no recognized layout, `trailer == 0` → ECC off, clean;
/// - no recognized layout, `trailer > 0` → an ECC trailer this build can't
///   interpret: the payload is framed by `data_length` and verified by its
///   checksum, so the read succeeds with [`EccStatus::Unrecognized`] (a WARN),
///   but recovery is unavailable;
/// - `actual_payload_plus_ecc < data_length` (payload doesn't fit) → corruption.
fn classify_block_trailer(
    has_recognized_ecc: bool,
    actual_payload_plus_ecc: usize,
    data_length: usize,
    ecc_length: u32,
    handle: &BlockHandle,
) -> crate::Result<EccStatus> {
    let trailer = actual_payload_plus_ecc
        .checked_sub(data_length)
        .ok_or(crate::Error::InvalidHeader("Block"))?;
    if has_recognized_ecc {
        if trailer != ecc_length as usize {
            return Err(crate::Error::InvalidHeader("Block"));
        }
        Ok(EccStatus::Ok)
    } else if trailer == 0 {
        Ok(EccStatus::Ok)
    } else {
        log::warn!(
            "block {handle:?} carries an unrecognized ECC trailer ({trailer} B); \
             payload verified by checksum but recovery is unavailable — recompact \
             to re-stamp with a supported scheme",
        );
        Ok(EccStatus::Unrecognized)
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
    /// Inner zstd-block layout of the compressed payload: cumulative
    /// decompressed END offsets, one per inner block (the last equals the
    /// uncompressed length). Empty unless this is a `Data` block compressed
    /// into >= 2 inner zstd blocks; lets the reader partial-decode a key-range
    /// subset instead of the whole block. See
    /// [`CompressionProvider::compress_with_layout`](crate::compression::CompressionProvider::compress_with_layout).
    pub(crate) layout: Vec<u32>,
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
            layout: self.layout,
        }
    }

    /// Writes the framed block (header + payload + optional parity trailer)
    /// to `writer` and returns the header. This is the single point where
    /// block bytes hit the file, so it must run in on-disk order.
    pub(crate) fn write_to<W: crate::io::Write>(self, mut writer: &mut W) -> crate::Result<Header> {
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
/// - [`Self::Ok`] — the read returned correct bytes with no ECC intervention:
///   ECC was absent, or a recognized scheme verified the payload as-is (no
///   repair needed).
/// - [`Self::Corrected`] — ECC repaired the on-disk payload (the caller saw
///   correct bytes, but the on-disk copy still holds a latent fault). Treat as
///   a signal to confirm persistence and potentially schedule an auto-heal
///   recompaction. Which mechanism did the repair (SEC-DED vs RS shard) is
///   surfaced separately to internal read paths via [`EccRecoveryKind`] for
///   metrics attribution.
/// - [`Self::Unrecognized`] — the block carries an ECC trailer this build
///   cannot interpret (a non-canonical scheme, page granularity, unknown kind,
///   …). The payload was returned (its checksum passed), but ECC recovery is
///   unavailable for this block; recompaction re-stamps it with a supported
///   scheme. A "typing" warning, not a read failure.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub enum EccStatus {
    /// ECC absent or a recognized scheme verified normally.
    #[default]
    Ok,
    /// ECC trailer present but its scheme is unrecognized / unusable; the
    /// payload still verified by its checksum. Recommend recompaction.
    Unrecognized,
    /// The on-disk payload failed its checksum and was REPAIRED by ECC
    /// (SEC-DED single-bit heal or Reed-Solomon shard recovery): the bytes
    /// returned to the caller are correct (they reproduce the stored
    /// checksum), but the on-disk copy still holds the latent fault. A signal
    /// for auto-heal — the caller may re-read to confirm the corruption is
    /// persistent (not a transient read-path fault) and, if so, schedule a
    /// recompaction so the corrected bytes are persisted to a fresh SST.
    Corrected,
}

/// Which ECC mechanism recovered a block whose checksum failed on read.
///
/// Returned alongside [`EccStatus`] by the internal recovery-aware read paths
/// (kept out of the public `EccStatus` to keep that enum stable) so an operator
/// metric can attribute each on-read recovery to the right heal path: the cheap
/// single-bit fast path versus full shard reconstruction.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum EccRecoveryKind {
    /// A single-bit flip healed by the SEC-DED fast path (one XOR-and-recheck
    /// per word, no shard arithmetic).
    Secded,
    /// Recovered from Reed-Solomon shard parity (the general multi-byte path).
    Shard,
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
    fn read_payload_and_verify<R: crate::io::Read>(
        reader: &mut R,
        data_length: u32,
        ecc_length: u32,
        expected: Checksum,
        #[cfg_attr(
            not(feature = "page_ecc"),
            expect(unused_variables, reason = "recovery scheme only used under page_ecc")
        )]
        ecc_params: EccParams,
    ) -> crate::Result<(Slice, Option<EccRecoveryKind>)> {
        // Read straight into the Slice allocation — `read_exact` overwrites every
        // byte, so a zero-filled scratch buffer would be wasted, and returning a
        // Slice lets the no-compression / ECC-recovery callers avoid a later copy.
        let data = Slice::from_reader(reader, data_length as usize)?;

        let computed = Checksum::from_raw(crate::hash::hash128(&data));

        if ecc_length == 0 {
            computed.check(expected).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for block payload, got={computed}, expected={expected}",
                );
            })?;
            return Ok((data, None));
        }

        // ECC trailer present — always consume the parity bytes so
        // the reader cursor lands on the next block's header even
        // when the happy path doesn't need them.
        let parity = Slice::from_reader(reader, ecc_length as usize)?;

        if computed == expected {
            return Ok((data, None));
        }

        // Mismatch — try ECC recovery before failing.
        #[cfg(feature = "page_ecc")]
        {
            let expected_raw = expected.into_u128();

            // SEC-DED fast path: heal a single bit flip per word. The repaired
            // block must reproduce the stored checksum; a double-bit error (or
            // a heal that still mismatches) is surfaced, never silently
            // accepted. This scheme stores no shard parity, so there is no RS
            // fall-through here — the shard schemes are a separate EccParams
            // variant.
            if matches!(ecc_params, crate::table::block::EccParams::Secded) {
                // In-place single-bit heal needs an owned, mutable buffer; the
                // recovery path is rare, so the copy out of the read Slice is fine.
                let mut healed = data.to_vec();
                if crate::secded::try_correct_block(&mut healed, &parity)
                    == crate::secded::SecdedOutcome::Corrected
                    && crate::hash::hash128(&healed) == expected_raw
                {
                    log::warn!(
                        "recovered block via SEC-DED single-bit heal after \
                         checksum mismatch (data_len={}, ecc_len={ecc_length})",
                        data.len(),
                    );
                    return Ok((Slice::from(healed), Some(EccRecoveryKind::Secded)));
                }
                log::error!(
                    "Checksum mismatch on SEC-DED block, heal failed, \
                     got={computed}, expected={expected}",
                );
                return Err(crate::Error::PageEccUnrecoverable {
                    got: computed,
                    expected,
                });
            }

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
                return Ok((Slice::from(recovered), Some(EccRecoveryKind::Shard)));
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
    pub fn write_into<W: crate::io::Write>(
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
    pub(crate) fn write_into_with_flags<W: crate::io::Write>(
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

        // Inner zstd-block layout, captured only for Data blocks compressed with
        // a non-dict zstd codec that split into >= 2 inner blocks (empty
        // otherwise). Enables range-query partial decode of large cold blocks.
        // `mut` is only exercised by the zstd Data arm below.
        #[cfg_attr(
            not(zstd_any),
            expect(unused_mut, reason = "`layout` is only mutated on zstd-enabled builds")
        )]
        let mut layout: Vec<u32> = Vec::new();

        match compression {
            CompressionType::None => {}

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                compressed_buf = Some(lz4_flex::compress(data));
            }

            #[cfg(zstd_any)]
            CompressionType::Zstd(level) => {
                if block_type == BlockType::Data {
                    let (buf, lay) =
                        crate::compression::ZstdBackend::compress_with_layout(data, level)?;
                    compressed_buf = Some(buf);
                    layout = lay;
                } else {
                    compressed_buf = Some(crate::compression::ZstdBackend::compress(data, level)?);
                }
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

        // Encryption step — under zstd this seals the AAD-bound envelope
        // binding the block identity + transform context; otherwise the opaque
        // form, reusing the owned compression buffer when present.
        let encrypted_buf: Option<Vec<u8>>;

        #[cfg(any(feature = "lz4", zstd_any))]
        {
            encrypted_buf = encryption
                .map(|enc| {
                    encrypt_block_payload(
                        enc,
                        compressed_buf.take(),
                        data,
                        &identity,
                        compression,
                        block_flags,
                    )
                })
                .transpose()?;
        }

        #[cfg(not(any(feature = "lz4", zstd_any)))]
        {
            encrypted_buf = encryption
                .map(|enc| {
                    encrypt_block_payload(enc, None, data, &identity, compression, block_flags)
                })
                .transpose()?;
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
            // SEC-DED emits one check byte per 8-byte word; shard schemes emit a
            // Reed-Solomon / XOR trailer. Both produce a parity buffer the
            // reader re-sizes from `data_length` via `expected_parity_len`.
            let p = match ecc_params {
                crate::table::block::EccParams::Secded => {
                    crate::secded::encode_block_parity(&payload)
                }
                crate::table::block::EccParams::Shard { .. } => {
                    let (data_shards, parity_shards) = ecc_params.as_shards();
                    crate::ecc::encode_parity(&payload, data_shards, parity_shards)?
                }
            };
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
            layout,
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
    pub fn from_reader<R: crate::io::Read>(
        reader: &mut R,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Self> {
        let compression = transform.compression();
        let encryption = transform.encryption();
        #[cfg(zstd_any)]
        let zstd_dict = transform.zstd_dict();
        // `identity` (tree/table + dict/window context) feeds AAD
        // reconstruction on the encrypted path; `block_type` is still derived
        // from the parsed header (the frame self-describes it), not asserted
        // against identity.block_type.
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
            // `from_reader` returns no EccStatus, so a heal here is logged but
            // not surfaced; the status-returning `from_file_with_status` path is
            // what auto-heal observes.
            let (raw_vec, _recovery) = Self::read_payload_and_verify(
                reader,
                header.data_length,
                ecc_length,
                header.checksum,
                block_ecc_params(&header, transform),
            )?;

            // Recover the plaintext: AAD-bound envelope under zstd (verifies the
            // block identity), opaque in-place decrypt otherwise.
            let decrypted = decrypt_block_payload(enc, &raw_vec, &identity)?;

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
                    // Decompress straight into the Slice's heap allocation,
                    // skipping both the zero-fill of `vec![0; n]` and the
                    // Vec -> Slice copy.
                    //
                    // SAFETY (load-bearing, do NOT reorder): the block checksum is
                    // verified ABOVE, before this point. That ordering is the
                    // precondition, not an optimization. `lz4_flex` is the
                    // unchecked fast decoder: it may wildcopy a back-reference out
                    // of the output buffer before detecting a malformed frame, so
                    // it must only ever run on a checksum-verified, intact stream.
                    // On such a stream every back-reference targets an
                    // already-written byte, so the uninitialized builder is written
                    // before it is ever read. Never move the decompress ahead of
                    // the checksum check.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder =
                        unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut builder)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    // Decompress straight into the Slice allocation: no
                    // zero-filled scratch Vec and no Vec -> Slice copy. SAFETY:
                    // same checksum-first precondition as the lz4 arm above. The
                    // stream is checksum-verified before this point, so the decoder
                    // only writes the uninitialized builder, never reads it
                    // unwritten. Do not reorder ahead of the checksum check.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder =
                        unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                    let bytes_written =
                        crate::compression::ZstdBackend::decompress_into(&decrypted, &mut builder)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
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
                // from_reader has no EccStatus return; a heal is logged only.
                let (payload, _recovery) = Self::read_payload_and_verify(
                    reader,
                    header.data_length,
                    ecc_length,
                    header.checksum,
                    block_ecc_params(&header, transform),
                )?;
                payload
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
                    // Decompress straight into the Slice's heap allocation,
                    // skipping the zero-fill and the Vec -> Slice copy. SAFETY:
                    // see the matching arm above — the checksum-verified stream
                    // is intact, so wildcopy never reads an unwritten position.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder =
                        unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                    let bytes_written = lz4_flex::decompress_into(&raw_data, &mut builder)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    // Decompress straight into the Slice allocation — no
                    // zero-filled scratch Vec and no Vec -> Slice copy.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder =
                        unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                    let bytes_written =
                        crate::compression::ZstdBackend::decompress_into(&raw_data, &mut builder)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
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
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress. When
    /// `encryption` is `None`, the decrypt step is skipped.
    pub fn from_file_with_status(
        file: &dyn FsFile,
        handle: BlockHandle,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<(Self, EccStatus)> {
        let (block, status, _recovery) =
            Self::from_file_with_recovery(file, handle, identity, transform)?;
        Ok((block, status))
    }

    /// Like [`Self::from_file_with_status`] but additionally reports which ECC
    /// mechanism repaired the block (`Some(kind)` iff the status is
    /// [`EccStatus::Corrected`]). Internal to the read path: the primary read
    /// call sites (`load_block`, the partial-decode path, patrol scrub) use it
    /// to attribute the on-read recovery to the right metric counter, keeping
    /// the public [`EccStatus`] free of the kind.
    // Same duplication rationale as from_reader — see comment there.
    #[expect(
        clippy::too_many_lines,
        reason = "encrypt/no-encrypt branches duplicate compression match — see from_reader"
    )]
    pub(crate) fn from_file_with_recovery(
        file: &dyn FsFile,
        handle: BlockHandle,
        identity: BlockIdentity,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<(Self, EccStatus, Option<EccRecoveryKind>)> {
        let compression = transform.compression();
        let encryption = transform.encryption();
        #[cfg(zstd_any)]
        let zstd_dict = transform.zstd_dict();
        // `identity` (tree/table + compression context) feeds AAD
        // reconstruction on the encrypted read path below.
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
        // Pre-allocation sanity cap on `handle.size()`: reject an absurd on-disk
        // size before allocating the read buffer. The ECC-OFF path adds NO ECC
        // term and runs NO ECC math — when there is no parity, the cap is just
        // payload + header. When ECC is on, the cap allows the block's ACTUAL
        // scheme parity (from the per-SST descriptor carried by `transform`),
        // never a hardcoded scheme. Self-describing blocks (Meta / Manifest)
        // carry their own small RS parity but are orders of magnitude below
        // `max_payload`, so they pass this cap without an explicit ECC term.
        let max_ecc_overhead = match transform.ecc_params() {
            Some(params) => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "max_payload is MAX_DECOMPRESSION_SIZE (+ enc overhead), well below u32::MAX"
                )]
                let max_payload_u32 = max_payload.min(u64::from(u32::MAX)) as u32;
                u64::from(expected_parity_len(max_payload_u32, params))
            }
            None => 0,
        };
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
        let (header, data, ecc_status, recovery) = if let Some(enc) = encryption {
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
                return Err(crate::Error::Io(crate::io::Error::new(
                    crate::io::ErrorKind::UnexpectedEof,
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

            // Parity-trailer presence is keyed on a RECOGNIZED ECC layout
            // (`block_has_parity`: header bit for self-describing blocks,
            // descriptor-via-transform for SST), NOT on `ecc_length` (which is
            // also 0 for a recognized scheme on an empty payload). The trailer
            // length, when recognized, is derived from `data_length` + scheme.
            let has_ecc = block_has_parity(&parsed_header, transform);
            let ecc_length = if has_ecc {
                expected_parity_len(
                    parsed_header.data_length,
                    block_ecc_params(&parsed_header, transform),
                )
            } else {
                0
            };

            // Clamp-to-zero: a block truncated before its header ends has no
            // payload, which `classify_block_trailer` then flags as a mismatch.
            let actual_payload_plus_ecc = block_size.saturating_sub(header_len);
            let actual_data_len = parsed_header.data_length as usize;
            let ecc_status = classify_block_trailer(
                has_ecc,
                actual_payload_plus_ecc,
                actual_data_len,
                ecc_length,
                &handle,
            )?;

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
            let (buf, payload_corrected) = if ecc_length == 0 {
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
                (Slice::from(buf), None)
            } else {
                #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                let mut cursor = crate::io::Cursor::new(&buf[header_len..]);
                Self::read_payload_and_verify(
                    &mut cursor,
                    parsed_header.data_length,
                    ecc_length,
                    parsed_header.checksum,
                    block_ecc_params(&parsed_header, transform),
                )?
            };

            // Fold a successful ECC repair into the reported status; an
            // unrecognized scheme never heals, so the two are exclusive. The
            // recovery mechanism is carried out separately as `payload_corrected`.
            let ecc_status = if payload_corrected.is_some() {
                EccStatus::Corrected
            } else {
                ecc_status
            };

            let decrypted = decrypt_block_payload(enc, &buf, &identity)?;

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
                    // Decompress straight into the Slice allocation (no zero-fill,
                    // no Vec -> Slice copy). SAFETY: see the read-path arm above;
                    // the checksum-verified stream is intact.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut decompressed = unsafe {
                        Slice::builder_unzeroed(parsed_header.uncompressed_length as usize)
                    };

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut decompressed)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    decompressed.freeze().into()
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    // Decompress straight into the Slice allocation — no
                    // zero-filled scratch Vec and no Vec -> Slice copy.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder = unsafe {
                        Slice::builder_unzeroed(parsed_header.uncompressed_length as usize)
                    };

                    let bytes_written =
                        crate::compression::ZstdBackend::decompress_into(&decrypted, &mut builder)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
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

            (parsed_header, data, ecc_status, payload_corrected)
        } else {
            // Single I/O read — header + payload in one Slice.
            let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;

            let parsed_header = Header::decode_from(&mut &buf[..])?;
            let header_len = Header::header_len(parsed_header.block_type);

            // Recognized-ECC presence keys on `block_has_parity`, not on
            // `ecc_length` (which is also 0 for a recognized scheme on an empty
            // payload). See the encrypted branch + `classify_block_trailer`.
            let has_ecc = block_has_parity(&parsed_header, transform);
            let ecc_length = if has_ecc {
                expected_parity_len(
                    parsed_header.data_length,
                    block_ecc_params(&parsed_header, transform),
                )
            } else {
                0
            };

            // Clamp-to-zero: a buffer shorter than the header carries no payload,
            // which the trailer classification then flags as a mismatch.
            let actual_payload_plus_ecc = buf.len().saturating_sub(header_len);
            let actual_data_len = parsed_header.data_length as usize;
            let ecc_status = classify_block_trailer(
                has_ecc,
                actual_payload_plus_ecc,
                actual_data_len,
                ecc_length,
                &handle,
            )?;

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
            let (payload_slice, payload_corrected): (Slice, Option<EccRecoveryKind>) =
                if ecc_length == 0 {
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
                    (buf.slice(header_len..header_len + actual_data_len), None)
                } else {
                    #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                    let mut cursor = crate::io::Cursor::new(&buf[header_len..]);
                    let (payload, recovery) = Self::read_payload_and_verify(
                        &mut cursor,
                        parsed_header.data_length,
                        ecc_length,
                        parsed_header.checksum,
                        block_ecc_params(&parsed_header, transform),
                    )?;
                    (payload, recovery)
                };
            // Fold a successful ECC repair into the status; the recovery
            // mechanism is carried out separately as `payload_corrected`.
            let ecc_status = if payload_corrected.is_some() {
                EccStatus::Corrected
            } else {
                ecc_status
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

                    // Decompress straight into the Slice allocation (no zero-fill,
                    // no Vec -> Slice copy). SAFETY: see the read-path arm above;
                    // the checksum-verified stream is intact.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut decompressed = unsafe {
                        Slice::builder_unzeroed(parsed_header.uncompressed_length as usize)
                    };

                    let bytes_written =
                        lz4_flex::decompress_into(compressed_data, &mut decompressed)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    decompressed.freeze().into()
                }

                #[cfg(zstd_any)]
                CompressionType::Zstd(_) => {
                    let compressed_data: &[u8] = &payload_slice;

                    // Decompress straight into the Slice allocation — no
                    // zero-filled scratch Vec and no Vec -> Slice copy.
                    #[expect(unsafe_code, reason = "fill an uninitialized Slice via decompress")]
                    let mut builder = unsafe {
                        Slice::builder_unzeroed(parsed_header.uncompressed_length as usize)
                    };

                    let bytes_written = crate::compression::ZstdBackend::decompress_into(
                        compressed_data,
                        &mut builder,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    builder.freeze().into()
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

            (parsed_header, data, ecc_status, payload_corrected)
        };

        Ok((Self { header, data }, ecc_status, recovery))
    }

    /// In-place autoheal primitive: read this block's on-disk frame and, if its
    /// checksum fails but Page-ECC recovers it, return the CORRECTED frame bytes
    /// to write back at the same offset.
    ///
    /// The returned frame is `header ++ recovered_data ++ freshly-recomputed
    /// parity` and is byte-length-identical to the on-disk block (RS / SEC-DED
    /// reconstruct the original `data_length` payload, and the parity length is a
    /// deterministic function of it), so it can be written in place without
    /// shifting any later block. Returns `Ok(None)` when the block reads clean (no
    /// heal needed) or carries no recognized parity (nothing to reconstruct from);
    /// `Err` when the checksum fails and parity cannot recover it (uncorrectable)
    /// or the block is unreadable.
    ///
    /// Works purely at the framed-bytes level: the checksum and parity cover the
    /// on-disk (post-compression, post-encryption) data bytes, so no decompress /
    /// decrypt is needed — `transform` is consulted only for the parity scheme.
    #[cfg(feature = "page_ecc")]
    pub(crate) fn heal_frame(
        file: &dyn FsFile,
        handle: BlockHandle,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<Option<(alloc::vec::Vec<u8>, EccRecoveryKind)>> {
        let block_size = handle.size() as usize;
        if block_size < Header::MIN_LEN {
            return Err(crate::Error::InvalidHeader("Block"));
        }
        // Pre-allocation sanity cap on `handle.size()` (mirrors
        // `from_file_with_recovery`): reject an absurd on-disk size from a corrupt
        // handle before allocating the read buffer. Bound = max payload
        // (+ encryption overhead) + its parity + the largest header.
        let enc_overhead = transform
            .encryption()
            .map_or(0u64, |e| u64::from(e.max_overhead()));
        let max_payload = u64::from(MAX_DECOMPRESSION_SIZE) + enc_overhead;
        let max_ecc_overhead = match transform.ecc_params() {
            Some(params) => {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "max_payload is MAX_DECOMPRESSION_SIZE (+ enc overhead), well below u32::MAX"
                )]
                let max_payload_u32 = max_payload.min(u64::from(u32::MAX)) as u32;
                u64::from(expected_parity_len(max_payload_u32, params))
            }
            None => 0,
        };
        let max_on_disk_size = max_payload + max_ecc_overhead + Header::MAX_LEN as u64;
        if u64::from(handle.size()) > max_on_disk_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: max_on_disk_size,
            });
        }
        let mut buf = alloc::vec![0u8; block_size];
        let n = file.read_at(&mut buf, *handle.offset())?;
        if n != block_size {
            return Err(crate::Error::Io(crate::io::Error::new(
                crate::io::ErrorKind::UnexpectedEof,
                "heal_frame: short block read",
            )));
        }
        let header = Header::decode_from(&mut &buf[..])?;
        let header_len = Header::header_len(header.block_type);
        // Heal targets blocks under a recognized parity scheme; without one there
        // is nothing to reconstruct from (a checksum-only block that fails is
        // salvage's job, not in-place heal's).
        if !block_has_parity(&header, transform) {
            return Ok(None);
        }
        let ecc_params = block_ecc_params(&header, transform);
        let ecc_length = expected_parity_len(header.data_length, ecc_params);
        // `read_payload_and_verify` consumes exactly `data_length + ecc_length`
        // bytes from a cursor over the post-header bytes, returning the (recovered)
        // data and whether a correction was applied.
        let post_header = buf
            .get(header_len..)
            .ok_or(crate::Error::InvalidHeader("Block"))?;
        let mut cursor = crate::io::Cursor::new(post_header);
        let (data, recovery) = Self::read_payload_and_verify(
            &mut cursor,
            header.data_length,
            ecc_length,
            header.checksum,
            ecc_params,
        )?;
        let Some(kind) = recovery else {
            // Clean read: nothing to persist.
            return Ok(None);
        };
        // Recompute the parity over the corrected data so the rewritten frame is
        // canonical regardless of whether the fault hit a data or a parity shard.
        let parity = if matches!(ecc_params, EccParams::Secded) {
            crate::secded::encode_block_parity(&data)
        } else {
            let (data_shards, parity_shards) = ecc_params.as_shards();
            crate::ecc::encode_parity(&data, data_shards, parity_shards)?
        };
        let header_bytes = buf
            .get(..header_len)
            .ok_or(crate::Error::InvalidHeader("Block"))?;
        let mut frame = alloc::vec::Vec::with_capacity(header_len + data.len() + parity.len());
        frame.extend_from_slice(header_bytes);
        frame.extend_from_slice(&data);
        frame.extend_from_slice(&parity);
        // An in-place heal overwrites the block at its existing offset, so the
        // rebuilt frame MUST be byte-length-identical to the original — a shorter
        // or longer frame would corrupt the following block. Enforce at runtime
        // (not just `debug_assert`): the recovered payload / recomputed parity are
        // deterministic, so a mismatch means the header disagrees with the on-disk
        // layout; refuse rather than write a wrong-length block in release builds.
        if frame.len() != block_size {
            return Err(crate::Error::InvalidHeader(
                "in-place heal: rebuilt frame length differs from the on-disk block",
            ));
        }
        Ok(Some((frame, kind)))
    }

    /// Reads a data block's verified COMPRESSED payload (the zstd frame) WITHOUT
    /// decompressing it, for partial / lazy decode. Returns the header, the
    /// compressed-frame bytes (checksum-verified; ECC-recovered if a recognized
    /// parity trailer is present), and `Some(kind)` when a recovery occurred so
    /// the caller can schedule auto-heal and count the recovery.
    ///
    /// Non-encrypted blocks only — the caller must ensure
    /// `transform.encryption().is_none()` (an encrypted block's plaintext frame
    /// is only available after a whole-block decrypt, which defeats the lazy
    /// path). Mirrors the non-encrypted read+verify of
    /// [`Self::from_file_with_status`], stopping before decompression.
    ///
    /// # Errors
    ///
    /// Returns an error on a framing / checksum failure (unrecoverable), or if
    /// called for an encrypted transform.
    #[cfg(feature = "zstd")]
    pub(crate) fn read_data_frame(
        file: &dyn FsFile,
        handle: BlockHandle,
        transform: &BlockTransform<'_>,
    ) -> crate::Result<(Header, Slice, Option<EccRecoveryKind>)> {
        if transform.encryption().is_some() {
            return Err(crate::Error::Io(crate::io::Error::other(
                "read_data_frame: encrypted blocks are not supported on the lazy path",
            )));
        }

        // Pre-allocation sanity cap on `handle.size()`, mirroring
        // `from_file_with_status`: reject an absurd on-disk size before
        // allocating the read buffer, so a corrupt handle cannot force a huge
        // allocation. Non-encrypted path (encryption rejected above), so no
        // encryption overhead; the ECC term uses the block's ACTUAL per-SST
        // scheme (never a hardcoded one) and is 0 when there is no parity.
        let max_ecc_overhead = match transform.ecc_params() {
            Some(params) => u64::from(expected_parity_len(MAX_DECOMPRESSION_SIZE, params)),
            None => 0,
        };
        let max_on_disk_size =
            u64::from(MAX_DECOMPRESSION_SIZE) + max_ecc_overhead + Header::MAX_LEN as u64;
        if u64::from(handle.size()) > max_on_disk_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: max_on_disk_size,
            });
        }

        let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;
        let parsed_header = Header::decode_from(&mut &buf[..])?;
        // Reject a header that declares a decompressed size past the cap before
        // the lazy decode path trusts it.
        if parsed_header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(parsed_header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }
        let header_len = Header::header_len(parsed_header.block_type);

        let has_ecc = block_has_parity(&parsed_header, transform);
        let ecc_length = if has_ecc {
            expected_parity_len(
                parsed_header.data_length,
                block_ecc_params(&parsed_header, transform),
            )
        } else {
            0
        };

        // Clamp-to-zero: a buffer shorter than the header carries no payload,
        // which the trailer classification then flags as a mismatch.
        let actual_payload_plus_ecc = buf.len().saturating_sub(header_len);
        let actual_data_len = parsed_header.data_length as usize;
        let _ecc_status = classify_block_trailer(
            has_ecc,
            actual_payload_plus_ecc,
            actual_data_len,
            ecc_length,
            &handle,
        )?;

        let (payload, recovery): (Slice, Option<EccRecoveryKind>) = if ecc_length == 0 {
            #[expect(
                clippy::indexing_slicing,
                reason = "actual_data_len <= post-header len, checked via classify_block_trailer"
            )]
            let checksum = Checksum::from_raw(crate::hash::hash128(
                &buf[header_len..header_len + actual_data_len],
            ));
            checksum.check(parsed_header.checksum)?;
            (buf.slice(header_len..header_len + actual_data_len), None)
        } else {
            #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
            let mut cursor = crate::io::Cursor::new(&buf[header_len..]);
            // `recovery` is surfaced so the partial-decode caller can schedule
            // auto-heal (the recovered bytes are correct but the on-disk copy is
            // still faulty) and count the recovery; a heal is also logged inside
            // read_payload_and_verify.
            let (frame, recovery) = Self::read_payload_and_verify(
                &mut cursor,
                parsed_header.data_length,
                ecc_length,
                parsed_header.checksum,
                block_ecc_params(&parsed_header, transform),
            )?;
            (frame, recovery)
        };

        Ok((parsed_header, payload, recovery))
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
mod tests;
