// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `BlockTransform`: discriminated union of the four valid Block I/O
//! payload transforms.
//!
//! Block I/O has exactly four valid combinations of compression and
//! encryption:
//!
//! | Variant                       | Pipeline                                       |
//! |-------------------------------|-----------------------------------------------|
//! | [`BlockTransform::Plain`]                     | raw → checksum → disk                     |
//! | [`BlockTransform::Compressed`]                | raw → compress → checksum → disk          |
//! | [`BlockTransform::Encrypted`]                 | raw → encrypt → checksum → disk           |
//! | [`BlockTransform::CompressedAndEncrypted`]    | raw → compress → encrypt → checksum → disk |
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
//!    public API at all; the runtime mismatch check on the
//!    `Block::write_into` happy path is gone. (The legacy-triple
//!    helper [`BlockTransform::from_parts`] still has one runtime
//!    `ZstdDictMismatch` check for callers that haven't yet
//!    migrated; that path is the only place the error can still
//!    fire.)
//! 2. **Shrinks the public surface.** `Block::write_into` /
//!    `from_reader` / `from_file` each previously took three
//!    transform-related args; they now take one
//!    `transform: &BlockTransform<'_>`.

#[cfg(zstd_any)]
use crate::compression::ZstdDictionary;
use crate::{CompressionType, encryption::EncryptionProvider};

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
///    double-encode the same state. `new()` panics on `None`.
/// 2. `kind == ZstdDict` is unreachable without an attached
///    dictionary: the `new()` constructor panics on `ZstdDict`
///    (forcing callers through [`Self::with_dict`]), and `with_dict`
///    takes the dictionary by reference and derives `dict_id` from
///    `dict.id()` itself. There is therefore no construction path
///    that yields a `ZstdDict` context without a matching dict, and
///    the runtime `ZstdDictMismatch` check that used to live on the
///    `Block::write_into` happy path is gone.
pub struct CompressionContext<'a> {
    kind: CompressionType,

    #[cfg(zstd_any)]
    zstd_dict: Option<&'a ZstdDictionary>,

    #[cfg(not(zstd_any))]
    _lifetime: core::marker::PhantomData<&'a ()>,
}

impl<'a> CompressionContext<'a> {
    /// Constructs a [`CompressionContext`] for a non-dict codec
    /// (`Lz4`, `Zstd(level)`).
    ///
    /// # Panics
    ///
    /// Panics if `kind` is [`CompressionType::None`] (use
    /// [`BlockTransform::Plain`] / [`BlockTransform::Encrypted`]
    /// instead) or [`CompressionType::ZstdDict`] (use
    /// [`Self::with_dict`] instead). Both are programmer errors,
    /// not runtime failure modes; they cannot happen via any code
    /// path that flows through the public API correctly.
    #[must_use]
    pub fn new(kind: CompressionType) -> Self {
        assert!(
            kind != CompressionType::None,
            "CompressionContext::new called with CompressionType::None; \
             use BlockTransform::Plain or BlockTransform::Encrypted",
        );
        #[cfg(zstd_any)]
        assert!(
            !matches!(kind, CompressionType::ZstdDict { .. }),
            "CompressionContext::new called with CompressionType::ZstdDict; \
             use CompressionContext::with_dict(level, dict) so the dictionary \
             travels with the codec discriminator",
        );
        Self {
            kind,
            #[cfg(zstd_any)]
            zstd_dict: None,
            #[cfg(not(zstd_any))]
            _lifetime: core::marker::PhantomData,
        }
    }

    /// Constructs a `ZstdDict` context with the matching dictionary
    /// handle attached.
    ///
    /// `dict.id()` is taken as the on-disk `dict_id`, so a mismatch
    /// is unreachable by construction. `level` is the zstd
    /// compression level the writer should use; readers don't
    /// consume it (the zstd frame header advertises the level
    /// itself) but it's stored to keep the on-disk
    /// [`CompressionType::ZstdDict`] discriminator round-trip-able.
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
    /// `Plain` / `Encrypted` map to [`CompressionType::None`];
    /// `Compressed` / `CompressedAndEncrypted` return the inner
    /// codec.
    #[must_use]
    pub fn compression(&self) -> CompressionType {
        match self {
            Self::Plain | Self::Encrypted(_) => CompressionType::None,
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.kind(),
        }
    }

    /// Optional zstd dictionary reference for this transform.
    ///
    /// Only `Compressed` / `CompressedAndEncrypted` variants can
    /// carry one; the other two variants return `None`.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn zstd_dict(&self) -> Option<&ZstdDictionary> {
        match self {
            Self::Plain | Self::Encrypted(_) => None,
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.zstd_dict(),
        }
    }

    /// Optional encryption provider for this transform.
    ///
    /// Only `Encrypted` / `CompressedAndEncrypted` variants carry
    /// one; the other two return `None`.
    #[must_use]
    pub fn encryption(&self) -> Option<&dyn EncryptionProvider> {
        match self {
            Self::Plain | Self::Compressed(_) => None,
            Self::Encrypted(enc) | Self::CompressedAndEncrypted(_, enc) => Some(*enc),
        }
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
            CompressionContext::new(compression)
        };

        #[cfg(not(zstd_any))]
        let ctx = CompressionContext::new(compression);

        Ok(match encryption {
            Some(enc) => Self::CompressedAndEncrypted(ctx, enc),
            None => Self::Compressed(ctx),
        })
    }
}
