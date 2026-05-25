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
//! 1. **Invalid combinations are rejected at the type level.** The
//!    old API let `ZstdDict { dict_id }` ship without `zstd_dict:
//!    Some(_)`, which only failed at runtime as
//!    `Error::ZstdDictMismatch`. With [`CompressionContext`] the
//!    dict bundle travels with the codec discriminator, so missing
//!    it is a *compile error*, not a runtime check.
//! 2. **Shrinks the public surface.** `Block::write_into` /
//!    `from_reader` / `from_file` each previously took three
//!    transform-related args; they now take one
//!    `transform: &BlockTransform<'_>`.

#[cfg(zstd_any)]
use crate::compression::ZstdDictionary;
use crate::{CompressionType, encryption::EncryptionProvider};

/// Codec configuration for the compression step of a block payload.
///
/// Bundles the codec discriminator together with the zstd dictionary
/// reference so the call site can't accidentally pass
/// [`CompressionType::ZstdDict`] without the matching dictionary
/// (and vice versa, for non-dict codecs the dict reference simply
/// isn't there to be misused). Carrying the lifetime parameter
/// even on builds without `zstd_any` keeps the API shape stable
/// across feature configurations; the `PhantomData<&'a ()>` field
/// is the canonical "I do use the lifetime, just not as a real
/// reference" marker.
pub struct CompressionContext<'a> {
    /// Codec discriminator. The [`CompressionType::None`] variant
    /// is rejected by the constructor — see [`Self::new`] —
    /// because "plain payload" is already a distinct
    /// [`BlockTransform`] variant; carrying it as a `CompressionContext`
    /// would double-encode the same state.
    pub kind: CompressionType,

    /// Raw zstd dictionary handle, required when `kind` is
    /// [`CompressionType::ZstdDict`]; ignored otherwise.
    #[cfg(zstd_any)]
    pub zstd_dict: Option<&'a ZstdDictionary>,

    /// Consumes the `'a` lifetime when the `zstd_any` feature is
    /// off so the public type shape stays stable. Without this
    /// marker the type would have an unused lifetime parameter on
    /// no-zstd builds.
    #[cfg(not(zstd_any))]
    _lifetime: core::marker::PhantomData<&'a ()>,
}

impl<'a> CompressionContext<'a> {
    /// Constructs a [`CompressionContext`] for a non-zstd-dict codec.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::InvalidTag`] if `kind` is
    /// [`CompressionType::None`] — "plain payload" is represented
    /// by [`BlockTransform::Plain`] / [`BlockTransform::Encrypted`],
    /// not a `Compressed(_)` variant with `kind = None`.
    pub fn new(kind: CompressionType) -> crate::Result<Self> {
        if kind == CompressionType::None {
            return Err(crate::Error::InvalidTag((
                "CompressionContext::new called with CompressionType::None; \
                 use BlockTransform::Plain or BlockTransform::Encrypted instead",
                0,
            )));
        }
        Ok(Self {
            kind,
            #[cfg(zstd_any)]
            zstd_dict: None,
            #[cfg(not(zstd_any))]
            _lifetime: core::marker::PhantomData,
        })
    }

    /// Attaches a zstd dictionary to this context.
    ///
    /// Required when `kind == CompressionType::ZstdDict { dict_id }`,
    /// where the dictionary's `id()` must equal `dict_id`. Required by
    /// the writer to actually pin the dictionary against the on-disk
    /// `dict_id`; the matching read path takes the same dict via this
    /// builder so encoder and decoder share one source of truth.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_zstd_dict(mut self, dict: &'a ZstdDictionary) -> Self {
        self.zstd_dict = Some(dict);
        self
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
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.kind,
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
            Self::Compressed(ctx) | Self::CompressedAndEncrypted(ctx, _) => ctx.zstd_dict,
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

        // Compressed path: build the CompressionContext with the
        // codec discriminator and (if applicable) the zstd dict.
        // ZstdDict requires the dict; centralise the mismatch check
        // here so every entry point produces the same error.
        #[cfg(zstd_any)]
        let ctx = if let CompressionType::ZstdDict { dict_id, .. } = compression {
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
            CompressionContext::new(compression)?.with_zstd_dict(dict)
        } else {
            // Non-dict codecs ignore the zstd_dict slot, matching the
            // previous API. We intentionally don't reject a stray dict
            // here: keeping the constructor lenient matches the loose
            // contract callers used to rely on.
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
