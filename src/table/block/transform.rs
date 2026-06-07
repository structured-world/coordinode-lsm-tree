// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `BlockTransform`: discriminated union of the four valid Block I/O
//! payload transforms.
//!
//! Block I/O has eight valid combinations: four `(compression,
//! encryption)` pipelines, each crossed with the optional
//! Reed-Solomon parity trailer (enabled via the `page_ecc` cargo
//! feature):
//!
//! | Variant                                          | Pipeline                                               |
//! |--------------------------------------------------|-------------------------------------------------------|
//! | [`BlockTransform::Plain`]                        | raw → checksum → disk                                 |
//! | [`BlockTransform::Compressed`]                   | raw → compress → checksum → disk                      |
//! | [`BlockTransform::Encrypted`]                    | raw → encrypt → checksum → disk                       |
//! | [`BlockTransform::CompressedAndEncrypted`]       | raw → compress → encrypt → checksum → disk            |
//! | `BlockTransform::PlainEcc`                       | raw → checksum → ecc parity → disk                    |
//! | `BlockTransform::CompressedEcc`                  | raw → compress → checksum → ecc parity → disk         |
//! | `BlockTransform::EncryptedEcc`                   | raw → encrypt → checksum → ecc parity → disk          |
//! | `BlockTransform::CompressedAndEncryptedEcc`      | raw → compress → encrypt → checksum → ecc parity → disk |
//!
//! The `Ecc` variants are only available when the `page_ecc` cargo
//! feature is enabled; without it, the variant list collapses to
//! the original four. ECC is orthogonal to compression / encryption:
//! parity is computed over the on-disk payload after compression
//! and after encryption, and lives in a trailer that is NOT
//! covered by AEAD authentication and NOT included in the
//! per-block XXH3 over the payload bytes. Tampering with the
//! parity trailer therefore cannot be detected by AEAD or by the
//! block checksum — it only impacts recoverability (a corrupted
//! parity trailer means the codec can't repair payload bit-flips,
//! but the payload itself is still authenticated by its own
//! checksum / AEAD tag and tampering there fails the usual way).
//! In other words: ECC is a best-effort recovery aid for bit-rot
//! in the wire bytes, NOT an integrity primitive.
//!
//! Modelling the four paths as a single enum has two concrete wins
//! over the previous "`(compression, encryption, zstd_dict)` triple"
//! argument shape:
//!
//! 1. **Invalid combinations are unreachable by construction.** The
//!    old API let `ZstdDict { dict_id }` ship without `zstd_dict:
//!    Some(_)`, which only failed at runtime as
//!    `Error::ZstdDictMismatch` deep inside `Block::write_into`.
//!    `CompressionContext`'s fields are now private; the only way
//!    to construct a `ZstdDict` context is via
//!    [`CompressionContext::with_dict`], which takes the dictionary
//!    handle directly and derives the on-disk `dict_id` from
//!    `dict.id()`. The previous "construct a ZstdDict-kind context
//!    without a dict" mistake therefore can't be expressed in the
//!    public API at all. The runtime `ZstdDictMismatch` guards
//!    inside `Block::write_into` / `from_reader` / `from_file`
//!    still execute on every block (they re-check that a dictionary
//!    is attached and that its `id()` matches the on-disk
//!    `dict_id`), but with `BlockTransform` constructed via the
//!    safe API they are defense-in-depth only and cannot fire.
//!    The one remaining path that can legitimately produce
//!    `ZstdDictMismatch` is the legacy-triple helper
//!    [`BlockTransform::from_parts`], which is checked at
//!    construction time (so the error is raised before any
//!    `Block` I/O is attempted) for callers that have not yet
//!    migrated to the typed constructors.
//! 2. **Shrinks the public surface.** `Block::write_into` /
//!    `from_reader` / `from_file` each previously took three
//!    transform-related args; they now take one
//!    `transform: &BlockTransform<'_>`.

#[cfg(zstd_any)]
use crate::compression::ZstdDictionary;
use crate::{CompressionType, encryption::EncryptionProvider};

/// Reed-Solomon / XOR shard layout carried by the `*Ecc` transform
/// variants: `data_shards` data shards plus `parity_shards` parity
/// shards (`parity_shards == 1` is plain-XOR RAID-5).
///
/// Recorded per-SST in the ECC descriptor; the write path takes it from
/// the configured scheme and the read path from the SST's `TableMeta`,
/// so writer and reader always size and recover the parity trailer
/// identically. [`Self::RS_4_2`] is the historical default kept for
/// tests; production picks a low-overhead scheme (XOR single-parity).
///
/// Not feature-gated (a 2-byte POD) so call sites pass it uniformly
/// across the feature matrix; the `*Ecc` variants that store it and
/// the parity codec that consumes it are `page_ecc`-gated.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct EccParams {
    /// Number of data shards the block payload is split into.
    pub data_shards: u8,
    /// Number of parity shards (`1` = XOR single-parity / RAID-5).
    pub parity_shards: u8,
}

impl EccParams {
    /// Legacy fixed RS(4,2) layout (50% overhead). Default for tests
    /// only; never an implicit production default.
    pub const RS_4_2: Self = Self {
        data_shards: 4,
        parity_shards: 2,
    };

    /// `(data_shards, parity_shards)` as `usize`, for the `crate::ecc`
    /// shard-based encode / recover API.
    #[must_use]
    pub const fn as_shards(self) -> (usize, usize) {
        (self.data_shards as usize, self.parity_shards as usize)
    }
}

/// Codec configuration for the compression step of a block payload.
///
/// Fields are private; the only way to construct a value is via
/// [`Self::new`] (non-dict codecs) or [`Self::with_dict`]
/// (dict-required codec, takes the dictionary handle directly). Two
/// invariants the previous open-fields shape relied on are now
/// enforced by the constructors:
///
/// 1. The kind is never [`CompressionType::None`]: "plain payload"
///    is its own [`BlockTransform::Plain`] / [`BlockTransform::Encrypted`]
///    variant; carrying it as a `CompressionContext` would
///    double-encode the same state. `new()` returns
///    [`crate::Error::FeatureUnsupported`] on `None`.
/// 2. `kind == ZstdDict` is unreachable without an attached
///    dictionary: the `new()` constructor refuses `ZstdDict` (returns
///    [`crate::Error::FeatureUnsupported`], forcing callers through
///    [`Self::with_dict`]), and `with_dict` takes the dictionary by
///    reference and derives `dict_id` from `dict.id()` itself. There
///    is therefore no construction path that yields a `ZstdDict`
///    context without a matching dict. The runtime `ZstdDictMismatch`
///    guards inside `Block::write_into` / `from_reader` / `from_file`
///    still execute on every block, but with `CompressionContext`
///    constructed via the safe API they are defense-in-depth only
///    and cannot fire.
pub struct CompressionContext<'a> {
    kind: CompressionType,

    #[cfg(zstd_any)]
    zstd_dict: Option<&'a ZstdDictionary>,

    #[cfg(not(zstd_any))]
    _lifetime: core::marker::PhantomData<&'a ()>,
}

// `'a` is borrowed by `with_dict` (gated behind `zstd_any`). On
// builds with no zstd backend the borrow drops out, but the
// impl-level lifetime stays so method signatures stay valid across
// the feature matrix without per-method `#[cfg]` gymnastics.
//
// Feature-gated `#[expect]` rather than blanket `#[allow]`: under
// any zstd feature `'a` IS used by `with_dict` and the lint does
// NOT fire — wrapping `#[expect]` in `cfg_attr(not(zstd_any), ..)`
// keeps the stricter "lint expectation will self-expire if the
// underlying code stops triggering" semantics, just feature-scoped
// to the build where the lint actually fires.
#[cfg_attr(
    not(zstd_any),
    expect(
        clippy::elidable_lifetime_names,
        reason = "'a kept for cross-feature-matrix signature stability; \
                  used by with_dict under any zstd feature"
    )
)]
impl<'a> CompressionContext<'a> {
    /// Constructs a [`CompressionContext`] for a non-dict codec
    /// (`Lz4`, `Zstd(level)`).
    ///
    /// Reports invalid `kind`s as [`crate::Error::FeatureUnsupported`]
    /// so a caller that builds a [`CompressionType`] from runtime
    /// config (e.g. parsing a table-config file) sees a typed error
    /// rather than a process panic. Use [`BlockTransform::Plain`] /
    /// [`BlockTransform::Encrypted`] for the no-compression case and
    /// [`Self::with_dict`] for `ZstdDict`.
    ///
    /// # Errors
    ///
    /// - `Error::FeatureUnsupported("compression-context-none")` when
    ///   `kind == CompressionType::None`
    /// - `Error::FeatureUnsupported("compression-context-zstd-dict-via-new")`
    ///   when `kind == CompressionType::ZstdDict { .. }`
    ///
    /// See [`crate::Error::FeatureUnsupported`] for the typed variant.
    pub fn new(kind: CompressionType) -> crate::Result<Self> {
        if kind == CompressionType::None {
            return Err(crate::Error::FeatureUnsupported("compression-context-none"));
        }
        #[cfg(zstd_any)]
        if matches!(kind, CompressionType::ZstdDict { .. }) {
            return Err(crate::Error::FeatureUnsupported(
                "compression-context-zstd-dict-via-new",
            ));
        }
        Ok(Self {
            kind,
            #[cfg(zstd_any)]
            zstd_dict: None,
            #[cfg(not(zstd_any))]
            _lifetime: core::marker::PhantomData,
        })
    }

    /// Constructs a `ZstdDict` context with the matching dictionary
    /// handle attached.
    ///
    /// `dict.id()` is taken as the on-disk `dict_id`, so a mismatch
    /// is unreachable by construction. `level` is the zstd
    /// compression level the writer should use; readers don't
    /// consume it (zstd decompression simply doesn't need the
    /// encoder's chosen level — the level controls only the
    /// encoder's CPU / ratio tradeoff and isn't carried in the zstd
    /// frame at all). It's stored here only to keep the on-disk
    /// [`CompressionType::ZstdDict`] discriminator round-trippable
    /// for writers and metadata that DO need to remember the level
    /// (e.g. the per-table compression policy table).
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_dict(level: i32, dict: &'a ZstdDictionary) -> Self {
        Self {
            kind: CompressionType::ZstdDict {
                level,
                dict_id: dict.id(),
            },
            zstd_dict: Some(dict),
        }
    }

    /// On-disk codec discriminator.
    #[must_use]
    pub fn kind(&self) -> CompressionType {
        self.kind
    }

    /// Attached zstd dictionary, if any. Always `Some` for
    /// `kind == ZstdDict` (enforced by the constructors), `None`
    /// for non-dict codecs.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn zstd_dict(&self) -> Option<&ZstdDictionary> {
        self.zstd_dict
    }
}

/// Block I/O payload transform: zero, one, or both of compression and
/// encryption.
///
/// Replaces the previous `(compression, encryption, zstd_dict)` argument
/// triple on `Block::write_into` / `Block::from_reader` /
/// `Block::from_file` with a single discriminated union. Each variant
/// pins a different pipeline (see module docs).
pub enum BlockTransform<'a> {
    /// `raw → checksum → disk`. Used by tests that don't exercise
    /// compression or encryption, and by index / filter blocks
    /// configured with `CompressionType::None` on filter-less builds.
    /// Most call sites just reach for the no-allocation
    /// [`BlockTransform::PLAIN`] constant.
    Plain,

    /// `raw → compress → checksum → disk`.
    Compressed(CompressionContext<'a>),

    /// `raw → encrypt → checksum → disk`.
    Encrypted(&'a dyn EncryptionProvider),

    /// `raw → compress → encrypt → checksum → disk`.
    CompressedAndEncrypted(CompressionContext<'a>, &'a dyn EncryptionProvider),

    /// `raw → checksum → ecc parity → disk`. Same as [`Self::Plain`]
    /// but emits a Reed-Solomon (4, 2) parity trailer after the
    /// on-disk payload. The header's `ECC_PARITY` flag marks the
    /// trailer's presence (its length is derived from `data_length`),
    /// so the reader can verify-and-recover from a single data-shard
    /// loss without a separate sidecar.
    #[cfg(feature = "page_ecc")]
    PlainEcc(EccParams),

    /// `raw → compress → checksum → ecc parity → disk`.
    #[cfg(feature = "page_ecc")]
    CompressedEcc(CompressionContext<'a>, EccParams),

    /// `raw → encrypt → checksum → ecc parity → disk`. The parity is
    /// computed over the encrypted ciphertext and stored in a
    /// trailer outside the AEAD-authenticated region. Tampering with
    /// the ciphertext fails AEAD verification on read the usual way;
    /// tampering with the parity trailer specifically is NOT detected
    /// by AEAD (the trailer isn't part of the authenticated payload
    /// and isn't covered by the per-block XXH3 either) — it only
    /// impacts recoverability. ECC is a best-effort recovery aid for
    /// bit-rot, not an integrity primitive on top of AEAD.
    #[cfg(feature = "page_ecc")]
    EncryptedEcc(&'a dyn EncryptionProvider, EccParams),

    /// `raw → compress → encrypt → checksum → ecc parity → disk`.
    #[cfg(feature = "page_ecc")]
    CompressedAndEncryptedEcc(
        CompressionContext<'a>,
        &'a dyn EncryptionProvider,
        EccParams,
    ),
}

impl BlockTransform<'_> {
    /// Borrow-free "no compression, no encryption" transform.
    ///
    /// Tests that don't exercise either transform reach for this
    /// constant instead of constructing
    /// `BlockTransform::Plain` repeatedly; the constant form makes
    /// the "I have no transform context to plumb" intent louder than
    /// a literal `BlockTransform::Plain` would and matches the
    /// idiom from the design discussion in #248.
    pub const PLAIN: Self = Self::Plain;

    /// Codec discriminator for this transform.
    ///
    /// `Plain` / `Encrypted` (and their `Ecc` siblings) map to
    /// [`CompressionType::None`]; `Compressed` / `CompressedAndEncrypted`
    /// (and their `Ecc` siblings) return the inner codec.
    #[must_use]
    pub fn compression(&self) -> CompressionType {
        match self {
            Self::Plain | Self::Encrypted(_) => CompressionType::None,
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.kind(),
            #[cfg(feature = "page_ecc")]
            Self::PlainEcc(_) | Self::EncryptedEcc(_, _) => CompressionType::None,
            #[cfg(feature = "page_ecc")]
            Self::CompressedEcc(ctx, _) | Self::CompressedAndEncryptedEcc(ctx, _, _) => ctx.kind(),
        }
    }

    /// Optional zstd dictionary reference for this transform.
    ///
    /// Only `Compressed` / `CompressedAndEncrypted` (and their `Ecc`
    /// siblings) variants can carry one; the other variants return
    /// `None`.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn zstd_dict(&self) -> Option<&ZstdDictionary> {
        match self {
            Self::Plain | Self::Encrypted(_) => None,
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.zstd_dict(),
            #[cfg(feature = "page_ecc")]
            Self::PlainEcc(_) | Self::EncryptedEcc(_, _) => None,
            #[cfg(feature = "page_ecc")]
            Self::CompressedEcc(ctx, _) | Self::CompressedAndEncryptedEcc(ctx, _, _) => {
                ctx.zstd_dict()
            }
        }
    }

    /// Optional encryption provider for this transform.
    ///
    /// Only `Encrypted` / `CompressedAndEncrypted` (and their `Ecc`
    /// siblings) variants carry one; the other variants return `None`.
    #[must_use]
    pub fn encryption(&self) -> Option<&dyn EncryptionProvider> {
        match self {
            Self::Plain | Self::Compressed(_) => None,
            Self::Encrypted(enc) | Self::CompressedAndEncrypted(_, enc) => Some(*enc),
            #[cfg(feature = "page_ecc")]
            Self::PlainEcc(_) | Self::CompressedEcc(_, _) => None,
            #[cfg(feature = "page_ecc")]
            Self::EncryptedEcc(enc, _) | Self::CompressedAndEncryptedEcc(_, enc, _) => Some(*enc),
        }
    }

    /// Whether this transform emits a Reed-Solomon parity trailer
    /// after the on-disk payload. Always `false` when the
    /// `page_ecc` feature is disabled (the `Ecc` variants don't
    /// exist in that build, so the match degenerates to a single
    /// arm and the compiler folds the call to a constant).
    #[must_use]
    pub fn page_ecc(&self) -> bool {
        match self {
            Self::Plain
            | Self::Compressed(_)
            | Self::Encrypted(_)
            | Self::CompressedAndEncrypted(_, _) => false,
            #[cfg(feature = "page_ecc")]
            Self::PlainEcc(_)
            | Self::CompressedEcc(_, _)
            | Self::EncryptedEcc(_, _)
            | Self::CompressedAndEncryptedEcc(_, _, _) => true,
        }
    }

    /// Shard layout for the parity trailer when this transform emits
    /// one, else `None`. The write path sizes the trailer from this and
    /// the read path recovers with it (sourced per-SST so both agree).
    ///
    /// Not feature-gated: without `page_ecc` there are no `*Ecc`
    /// variants, so this is always `None` — call sites stay uniform
    /// across the feature matrix.
    #[must_use]
    pub fn ecc_params(&self) -> Option<EccParams> {
        match self {
            Self::Plain
            | Self::Compressed(_)
            | Self::Encrypted(_)
            | Self::CompressedAndEncrypted(_, _) => None,
            #[cfg(feature = "page_ecc")]
            Self::PlainEcc(p)
            | Self::CompressedEcc(_, p)
            | Self::EncryptedEcc(_, p)
            | Self::CompressedAndEncryptedEcc(_, _, p) => Some(*p),
        }
    }

    /// Returns the matching `*Ecc` variant of this transform when
    /// the `page_ecc` cargo feature is enabled, or the transform
    /// unchanged when the feature is off.
    ///
    /// Lets writer call sites stay compact when they need to
    /// conditionally emit a parity trailer based on a runtime flag
    /// (`Config::page_ecc(true)`):
    ///
    /// ```text
    /// let transform = BlockTransform::from_parts(...)?;
    /// let transform = if config.page_ecc {
    ///     transform.with_ecc()
    /// } else {
    ///     transform
    /// };
    /// ```
    ///
    /// On builds without the `page_ecc` feature the Ecc variants
    /// don't exist and this method becomes the identity function —
    /// the compiler folds it out at the call site so the
    /// runtime-flag branch is dead code.
    #[cfg(feature = "page_ecc")]
    #[must_use]
    pub fn with_ecc(self, params: EccParams) -> Self {
        // Non-Ecc variants gain the trailer; already-Ecc variants
        // re-stamp the params (same target variant either way).
        match self {
            Self::Plain | Self::PlainEcc(_) => Self::PlainEcc(params),
            Self::Compressed(ctx) | Self::CompressedEcc(ctx, _) => Self::CompressedEcc(ctx, params),
            Self::Encrypted(enc) | Self::EncryptedEcc(enc, _) => Self::EncryptedEcc(enc, params),
            Self::CompressedAndEncrypted(ctx, enc)
            | Self::CompressedAndEncryptedEcc(ctx, enc, _) => {
                Self::CompressedAndEncryptedEcc(ctx, enc, params)
            }
        }
    }

    /// Identity on builds without the `page_ecc` feature: the `Ecc`
    /// variants don't exist, so callers' `if ecc { t.with_ecc(..) }`
    /// branch is dead and folds out. Takes [`EccParams`] (which is not
    /// feature-gated) so call sites compile across the feature matrix
    /// without per-call `#[cfg]`.
    #[cfg(not(feature = "page_ecc"))]
    #[must_use]
    pub fn with_ecc(self, _params: EccParams) -> Self {
        self
    }
}

impl<'a> BlockTransform<'a> {
    /// Builds a `BlockTransform` from the legacy `(compression,
    /// encryption, zstd_dict)` argument triple.
    ///
    /// Used by intermediate functions that haven't yet been
    /// refactored to receive a `BlockTransform` directly; the type
    /// safety win lives at the call sites that construct
    /// `BlockTransform` from local context (writers), not at every
    /// generic load helper that just forwards what its caller gave
    /// it. Returns an error for the same
    /// `CompressionType::ZstdDict` + `zstd_dict.is_none()` mismatch
    /// the old API surfaced at runtime in `Block::write_into`,
    /// centralised here so every entry point gets the same
    /// diagnostic.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ZstdDictMismatch`] when `compression`
    /// is [`CompressionType::ZstdDict`] but `zstd_dict` is `None` or
    /// has a non-matching dictionary id.
    pub fn from_parts(
        compression: CompressionType,
        encryption: Option<&'a dyn EncryptionProvider>,
        #[cfg(zstd_any)] zstd_dict: Option<&'a ZstdDictionary>,
    ) -> crate::Result<Self> {
        // CompressionType::None → no compression pipeline; pick
        // Plain / Encrypted based on whether an encryption provider
        // is present.
        if compression == CompressionType::None {
            return Ok(match encryption {
                Some(enc) => Self::Encrypted(enc),
                None => Self::Plain,
            });
        }

        // Compressed path. The legacy triple may pass `ZstdDict` with
        // a separate `zstd_dict` argument; that's the only runtime
        // check `from_parts` needs to do (downstream `with_dict`
        // can't fail by construction). Non-dict codecs go through
        // `new()` which is total.
        #[cfg(zstd_any)]
        let ctx = if let CompressionType::ZstdDict { level, dict_id } = compression {
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
            CompressionContext::with_dict(level, dict)
        } else {
            // Non-dict codecs ignore the zstd_dict slot, matching the
            // previous API.
            let _ = zstd_dict;
            CompressionContext::new(compression)?
        };

        #[cfg(not(zstd_any))]
        let ctx = CompressionContext::new(compression)?;

        Ok(match encryption {
            Some(enc) => Self::CompressedAndEncrypted(ctx, enc),
            None => Self::Compressed(ctx),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_transform_reports_no_compression_no_encryption_no_ecc() {
        let t = BlockTransform::Plain;
        assert_eq!(t.compression(), CompressionType::None);
        assert!(t.encryption().is_none());
        assert!(!t.page_ecc());
    }

    #[test]
    fn plain_constant_matches_plain_variant() {
        let t = BlockTransform::PLAIN;
        assert!(matches!(t, BlockTransform::Plain));
        assert!(!t.page_ecc());
    }

    #[cfg(feature = "page_ecc")]
    #[test]
    fn plain_ecc_variant_reports_ecc_enabled_no_other_transform() {
        let t = BlockTransform::PlainEcc(EccParams::RS_4_2);
        assert_eq!(t.compression(), CompressionType::None);
        assert!(t.encryption().is_none());
        assert!(t.page_ecc());
    }

    #[cfg(all(feature = "page_ecc", feature = "lz4"))]
    #[test]
    fn compressed_ecc_carries_compression_kind_and_reports_ecc() {
        let Ok(ctx) = CompressionContext::new(CompressionType::Lz4) else {
            panic!("Lz4 ctx construction is total");
        };
        let t = BlockTransform::CompressedEcc(ctx, EccParams::RS_4_2);
        assert_eq!(t.compression(), CompressionType::Lz4);
        assert!(t.encryption().is_none());
        assert!(t.page_ecc());
    }
}
