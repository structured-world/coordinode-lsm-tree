// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Typed errors surfaced by the AAD-bound encrypted block decoder.
//!
//! See `docs/aad-block-format.md` §5.3 and §8: each variant maps to a
//! distinct decode-time failure mode so the caller can react
//! programmatically (e.g. surface `UnknownKeyEpoch` as a key-rotation
//! diagnostic, but `AeadVerificationFailed` as silent corruption /
//! tampering and a forensics trigger).

use core::fmt;

/// Errors raised by the AAD-bound block decoder.
///
/// All variants are decode-side: encode-side failures (AEAD failed
/// to seal, caller-supplied buffer too small, etc.) surface through
/// the existing `crate::Error::Encrypt` channel that the
/// [`super::EncryptionProvider`] trait already uses. The decoder's
/// job is to reject anything that doesn't match the on-disk wire
/// format precisely, before any decryption work happens.
#[derive(Debug)]
#[non_exhaustive]
pub enum DecryptError {
    /// `MetadataFrame` failed a structural check before any AEAD work:
    /// wrong magic, wrong `PayloadLen`, malformed `WindowLog`, or
    /// unknown `BlockType` byte. A non-conformant frame cannot have an
    /// AAD constructed against it, so AEAD verification cannot bind any
    /// context.
    MalformedMetadataFrame(&'static str),

    /// `BodyFrame` failed a structural check: wrong magic, zero-length
    /// payload, or payload exceeding the 256 MiB spec cap. Rejected
    /// before allocating the read buffer to avoid memory amplification
    /// on a forged TOC or a header bit-flip.
    MalformedBodyFrame(&'static str),

    /// `HeaderByte` decoded to a format version this build does not
    /// know how to read. Currently the only supported version is v1
    /// (high nibble `0b0001`); any other value lands here.
    UnsupportedFormatVersion {
        /// The raw byte read from `MetadataPayload` offset 0.
        header_byte: u8,
    },

    /// The on-disk `SuiteID` byte is not registered in this build's
    /// `§7` suite table. The decoder cannot resolve `NONCE_LEN` and
    /// must refuse to read the variable-length tail of the frame.
    UnsupportedSuite {
        /// The raw byte read from `MetadataPayload` offset 11.
        suite_id: u8,
    },

    /// `KeyEpoch` is outside the caller's key chain. Surfaced as a
    /// distinct variant from `AeadVerificationFailed` so an operator
    /// can tell key-rotation drift apart from active tampering.
    UnknownKeyEpoch {
        /// The raw byte read from `MetadataPayload` offset 9.
        key_epoch: u8,
    },

    /// Caller asserted a specific suite expectation (e.g. through a
    /// strongly-typed read API) but the on-disk byte disagrees.
    /// Distinct from `AeadVerificationFailed` because the caller is
    /// reading the wrong contract, not a corrupted block.
    SuiteMismatch {
        /// Suite the caller said the block was sealed under.
        expected: u8,
        /// Suite actually read from `MetadataPayload` offset 11.
        actual: u8,
    },

    /// AEAD primitive rejected the (ciphertext, tag, AAD, nonce, key)
    /// tuple. This is the catch-all for every AAD-bound tampering
    /// attack from `docs/aad-block-format.md` §3: bit-flip, block swap
    /// (intra-file / inter-file / cross-tree), block-type relabel,
    /// codec relabel, dict substitution, AAD format substitution.
    AeadVerificationFailed,
}

impl fmt::Display for DecryptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedMetadataFrame(reason) => {
                write!(f, "malformed AAD `MetadataFrame`: {reason}")
            }
            Self::MalformedBodyFrame(reason) => {
                write!(f, "malformed AAD `BodyFrame`: {reason}")
            }
            Self::UnsupportedFormatVersion { header_byte } => write!(
                f,
                "unsupported AAD format version (HeaderByte=0x{header_byte:02X}, \
                 expected high nibble 0b0001)"
            ),
            Self::UnsupportedSuite { suite_id } => write!(
                f,
                "unsupported AEAD suite (SuiteID=0x{suite_id:02X}, not in v1 registry)"
            ),
            Self::UnknownKeyEpoch { key_epoch } => write!(
                f,
                "unknown KeyEpoch 0x{key_epoch:02X} (not present in caller's key chain)"
            ),
            Self::SuiteMismatch { expected, actual } => write!(
                f,
                "AEAD suite mismatch (expected 0x{expected:02X}, found 0x{actual:02X} on disk)"
            ),
            Self::AeadVerificationFailed => {
                f.write_str("AEAD tag verification failed (corruption, tampering, or wrong key)")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for DecryptError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_lists_byte_values_in_hex() {
        // Operators reading logs need the raw byte values, not just the
        // variant name, to cross-reference the on-disk `MetadataPayload`.
        let err = DecryptError::UnsupportedSuite { suite_id: 0xAB };
        assert!(format!("{err}").contains("0xAB"));

        let err = DecryptError::UnsupportedFormatVersion { header_byte: 0x20 };
        assert!(format!("{err}").contains("0x20"));

        let err = DecryptError::UnknownKeyEpoch { key_epoch: 0x07 };
        assert!(format!("{err}").contains("0x07"));

        let err = DecryptError::SuiteMismatch {
            expected: 0x02,
            actual: 0x03,
        };
        let s = format!("{err}");
        assert!(s.contains("0x02"));
        assert!(s.contains("0x03"));
    }

    #[test]
    fn aead_failure_does_not_leak_byte_values() {
        // AEAD-verification failures intentionally carry no payload:
        // identifying *which* AAD byte caused the mismatch would help
        // an attacker tune their tampering attempts. The variant is
        // pure-marker; the message is a generic operator-facing string.
        let err = DecryptError::AeadVerificationFailed;
        let s = format!("{err}");
        assert!(s.contains("AEAD"));
        // No hex byte values should appear.
        assert!(!s.contains("0x"));
    }
}
