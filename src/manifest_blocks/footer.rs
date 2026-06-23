// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Footer Block payload — table of contents + manifest layout
//! version + flags.
//!
//! The footer is itself a standard [`Block`](crate::table::block::Block)
//! (so it inherits XXH3 / ECC / encryption from the same pipeline
//! as the section Blocks); this module owns only the payload bytes
//! that go inside the Block.
//!
//! Wire format (little-endian throughout):
//!
//! ```text
//! [0]      manifest_layout_version : u8
//! [1]      flags                    : u8   (bit 0 = head mirror populated)
//! [2..4]   section_count            : u16
//! [4..]    TOC entries, repeated section_count times:
//!            name_len        : u16
//!            name            : [u8; name_len]    (UTF-8, non-empty)
//!            block_offset    : u64               (absolute, from file start)
//!            block_size      : u32               (Block bytes incl. header + ECC trailer)
//!            section_checksum: u128              (XXH3-128 copied verbatim
//!                                                  from the section Block
//!                                                  header at write time;
//!                                                  binds section content
//!                                                  into the CURRENT pointer
//!                                                  digest path, preserving
//!                                                  per-Block ECC recovery)
//! ```
//!
//! Names are interned by exact byte equality — duplicate names are
//! rejected by the writer. The reader trusts the surrounding Block's
//! XXH3 (and optional ECC / AEAD) for integrity; this module only
//! validates structural invariants (UTF-8, non-empty names, no
//! duplicates, bounded sizes).

use crate::io::{LittleEndian, ReadBytesExt, WriteBytesExt};
#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
use crate::manifest_blocks::{MANIFEST_LAYOUT_VERSION_V1, MAX_SECTION_NAME_BYTES};
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
#[cfg(feature = "std")]
use std::io::{Read, Write};

/// One entry in the footer's table of contents — locates a single
/// section Block within the manifest file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TocEntry {
    /// Section name (UTF-8). Mirrors the previous sfa archive
    /// section names so callers continue to look up by the same
    /// string (`"tables"`, `"blob_files"`, `"format_version"`, etc.).
    pub name: String,

    /// Absolute byte offset within the manifest file where the
    /// section Block starts. Set by the writer when the section
    /// Block is appended; consumed by the reader to seek before
    /// calling [`crate::table::block::Block::from_reader`].
    pub block_offset: u64,

    /// Total on-disk Block size in bytes (header + payload +
    /// optional ECC trailer). Lets a range-reader pull only the
    /// section Block of interest without scanning forward.
    pub block_size: u32,

    /// XXH3-128 of the section Block, copied verbatim from
    /// [`crate::table::block::Header::checksum`] at write time.
    ///
    /// **Why it lives in the TOC:** the CURRENT pointer's content-
    /// binding digest is computed over the canonical TOC tuple
    /// `(name, offset, size, section_checksum)`. Including the
    /// section's own XXH3-128 here transitively binds the section's
    /// decoded content into the CURRENT digest without requiring
    /// `get_current_version` to re-hash the raw section byte range
    /// (which would short-circuit per-Block ECC recovery on read).
    /// The reader cross-checks this value against the section
    /// Block's own header on `read_section` for belt-and-braces
    /// defence against TOC entries that point at a different
    /// section by offset.
    ///
    /// Set to zero only by tests that construct synthetic TOC
    /// entries; production writers always carry the real checksum.
    pub section_checksum: u128,
}

/// In-memory representation of the footer Block payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FooterPayload {
    /// Manifest file layout version. Must equal
    /// [`MANIFEST_LAYOUT_VERSION_V1`] for the current writer; older
    /// readers SHOULD reject unknown versions rather than parse
    /// best-effort.
    pub layout_version: u8,

    /// Bit-packed flags, currently only
    /// [`crate::manifest_blocks::FLAG_FOOTER_MIRROR_ENABLED`]
    /// (bit 0). Other bits are reserved for future use and MUST be
    /// preserved verbatim on read (the writer never sets unknown
    /// bits, so non-zero reserved bits in a freshly-written
    /// manifest indicate forgery / corruption).
    pub flags: u8,

    /// Ordered list of section Blocks. Order is the write order;
    /// readers can lookup by name via [`Self::section`].
    pub sections: Vec<TocEntry>,
}

impl FooterPayload {
    /// Build a fresh footer payload for the current writer (always
    /// stamps [`MANIFEST_LAYOUT_VERSION_V1`]).
    #[must_use]
    pub fn new(flags: u8, sections: Vec<TocEntry>) -> Self {
        Self {
            layout_version: MANIFEST_LAYOUT_VERSION_V1,
            flags,
            sections,
        }
    }

    /// Look up a section by name. Returns the first matching entry
    /// — the writer rejects duplicates, so this matches at most one.
    #[must_use]
    pub fn section(&self, name: &str) -> Option<&TocEntry> {
        self.sections.iter().find(|e| e.name == name)
    }

    /// Serialize the payload to the wire format described in the
    /// module docstring.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ManifestFooterInvalid`] for
    /// structural violations (empty, oversized, or duplicate section
    /// names; section count overflowing `u16`) and
    /// [`crate::Error::Io`] on underlying writer failure.
    pub fn encode<W: Write>(&self, mut writer: W) -> crate::Result<()> {
        if self.sections.len() > usize::from(u16::MAX) {
            return Err(crate::Error::ManifestFooterInvalid(
                "section count exceeds u16::MAX",
            ));
        }

        // Reject duplicate names on the encode side too — decode
        // already rejects them, so allowing duplicates here would
        // let the writer emit a manifest its own reader refuses.
        // Symmetry matters; pre-empt the round-trip mismatch.
        let mut seen: crate::HashSet<&str> = crate::HashSet::default();
        for entry in &self.sections {
            if entry.name.is_empty() {
                return Err(crate::Error::ManifestFooterInvalid(
                    "section name must be non-empty",
                ));
            }
            if entry.name.len() > MAX_SECTION_NAME_BYTES {
                return Err(crate::Error::ManifestFooterInvalid(
                    "section name exceeds MAX_SECTION_NAME_BYTES",
                ));
            }
            if !seen.insert(entry.name.as_str()) {
                return Err(crate::Error::ManifestFooterInvalid(
                    "duplicate section name",
                ));
            }
        }

        writer.write_u8(self.layout_version)?;
        writer.write_u8(self.flags)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "section count bounded by u16::MAX check above"
        )]
        writer.write_u16::<LittleEndian>(self.sections.len() as u16)?;

        for entry in &self.sections {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "name length bounded by MAX_SECTION_NAME_BYTES check above"
            )]
            writer.write_u16::<LittleEndian>(entry.name.len() as u16)?;
            writer.write_all(entry.name.as_bytes())?;
            writer.write_u64::<LittleEndian>(entry.block_offset)?;
            writer.write_u32::<LittleEndian>(entry.block_size)?;
            writer.write_u128::<LittleEndian>(entry.section_checksum)?;
        }

        Ok(())
    }

    /// Deserialize from the wire format. Validates structural
    /// invariants (UTF-8 names, non-empty, bounded lengths,
    /// duplicate detection) but trusts the surrounding Block's
    /// XXH3 / AEAD for integrity.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ManifestFooterInvalid`] with a
    /// concrete failure reason when the bytes do not parse cleanly
    /// (unknown layout version, oversized section count, oversized
    /// name length, invalid UTF-8, empty name, duplicate name).
    /// Returns [`crate::Error::Io`] on reader failure.
    pub fn decode<R: Read>(mut reader: R) -> crate::Result<Self> {
        let layout_version = reader.read_u8()?;
        if layout_version != MANIFEST_LAYOUT_VERSION_V1 {
            return Err(crate::Error::ManifestFooterInvalid(
                "unknown manifest_layout_version",
            ));
        }

        let flags = reader.read_u8()?;
        let section_count = usize::from(reader.read_u16::<LittleEndian>()?);
        // Do NOT pre-allocate `Vec::with_capacity(section_count)`: the
        // count comes from inside the (verified-only-as-bytes) footer
        // payload and is an u16 (up to 65535). The footer Block is capped
        // at HEAD_FOOTER_RESERVED_SIZE (4 KiB) on disk, so the real
        // maximum is ~128 entries; trusting `section_count` here would
        // let a malformed footer force a multi-MiB allocation before the
        // parser ever reaches EOF. Grow the vector as entries decode
        // successfully — push reallocation is amortized O(1) and bounded
        // by the actual readable payload.
        let mut sections: Vec<TocEntry> = Vec::new();

        for _ in 0..section_count {
            let name_len = usize::from(reader.read_u16::<LittleEndian>()?);
            if name_len == 0 {
                return Err(crate::Error::ManifestFooterInvalid(
                    "section name length must be non-zero",
                ));
            }
            if name_len > MAX_SECTION_NAME_BYTES {
                return Err(crate::Error::ManifestFooterInvalid(
                    "section name length exceeds MAX_SECTION_NAME_BYTES",
                ));
            }

            let mut name_bytes = vec![0u8; name_len];
            reader.read_exact(&mut name_bytes)?;
            let name = String::from_utf8(name_bytes)
                .map_err(|_| crate::Error::ManifestFooterInvalid("section name not UTF-8"))?;

            let block_offset = reader.read_u64::<LittleEndian>()?;
            let block_size = reader.read_u32::<LittleEndian>()?;
            let section_checksum = reader.read_u128::<LittleEndian>()?;

            if sections.iter().any(|e: &TocEntry| e.name == name) {
                return Err(crate::Error::ManifestFooterInvalid(
                    "duplicate section name",
                ));
            }

            sections.push(TocEntry {
                name,
                block_offset,
                block_size,
                section_checksum,
            });
        }

        Ok(Self {
            layout_version,
            flags,
            sections,
        })
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    reason = "tests panic on the unhappy paths to surface failures loudly; \
              the hand-rolled bad-byte fixtures need direct write_* calls \
              that can't propagate via `?` cleanly; bounded test inputs \
              never approach u16 truncation"
)]
mod tests;
