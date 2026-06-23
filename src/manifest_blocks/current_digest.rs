// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Canonical XXH3-128 digest of a manifest's logical identity.
//!
//! Used by the CURRENT pointer to bind `version_id` plus footer
//! content (`manifest_layout_version`, flags, sorted TOC entries
//! including each section's own Block-level XXH3-128) without
//! re-hashing raw on-disk section bytes.
//!
//! ## Why not raw-byte hashing
//!
//! An earlier design hashed `[HEAD_FOOTER_RESERVED_SIZE, section_end)`
//! verbatim. That defeats per-Block Page ECC repair: a bit-flip
//! inside a section's payload leaves the on-disk bytes corrupted
//! (ECC repairs at decode time only, doesn't rewrite the file), so
//! the CURRENT-pointer re-hash on read mismatches BEFORE
//! `Block::from_reader` ever runs its ECC recovery. With ECC-enabled
//! manifests, the previous design forced sections to be
//! byte-clean on disk — effectively disabling ECC for the manifest
//! layer.
//!
//! ## What this digest binds
//!
//! - `manifest_layout_version` (footer payload byte)
//! - `version_id` (the v{N} the CURRENT pointer refers to)
//! - `flags` (footer mirror bit + reserved)
//! - Sorted list of `(name, block_offset, block_size, section_checksum)`
//!   per TOC entry. Sort order makes the digest insensitive to TOC
//!   reordering that doesn't change the logical manifest. Each
//!   section's content is bound through its own
//!   `Block::header::checksum` — XXH3-128 over the section
//!   Block's payload bytes (post-compression / post-encryption
//!   stream, same bytes the Block layer's own integrity check
//!   covers); the Block header and optional ECC trailer are
//!   outside the hash input. Computed at write time, copied
//!   verbatim into the TOC.
//!
//! ## What this digest does NOT bind
//!
//! - Raw on-disk bytes of section / footer Blocks. Per-Block XXH3
//!   already covers them, with ECC for repair when enabled. The
//!   CURRENT-pointer's job is identity binding (T1 mislinking),
//!   not byte-level corruption detection (T3, handled per-Block).
//!
//! ## Threat coverage
//!
//! - **T1 mislinking** (sysadmin renames `v0` over `v1`): `version_id`
//!   in the digest disambiguates, plus the TOC content differs
//!   between manifests — digest mismatch surfaces immediately.
//! - **T2 half-applied recovery**: caught earlier by
//!   `ManifestArchiveReader::open` returning `Unrecoverable` /
//!   `ManifestFooterInvalid` before the digest runs.
//! - **T3 bit-rot**: caught per-Block by XXH3 on `read_section`;
//!   ECC repairs transparently when enabled. The CURRENT-pointer
//!   digest now leaves this layer to do its job.
//! - **T4 adversarial tampering**: out of scope here — XXH3 is not
//!   a MAC. AEAD (`Config::with_encryption(...)`) provides the
//!   per-Block authenticated-encryption layer for that threat.

#[cfg(not(feature = "std"))]
use crate::io::Write;
use crate::io::{LittleEndian, WriteBytesExt};
use crate::manifest_blocks::footer::{FooterPayload, TocEntry};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::io::Write;

/// Compute the canonical XXH3-128 digest a CURRENT pointer carries
/// for a given (`version_id`, footer payload) pair.
///
/// The serialised form fed into XXH3 is fixed and stable across
/// writers — any change to it requires bumping the manifest layout
/// version. Field order:
///
/// 1. `version_id : u64 LE`
/// 2. `layout_version : u8`
/// 3. `flags : u8`
/// 4. `section_count : u32 LE`
/// 5. For each TOC entry in **sorted-by-name** order:
///    - `name_len : u16 LE`
///    - `name : utf-8 bytes`
///    - `block_offset : u64 LE`
///    - `block_size : u32 LE`
///    - `section_checksum : u128 LE` (XXH3-128 of the section's
///      on-disk Block, copied from the section's
///      [`crate::table::block::Header::checksum`] at write time)
///
/// The sort makes the digest order-independent: a writer that
/// reorders sections without changing their content produces the
/// same digest, so cosmetic ordering changes don't invalidate
/// previously-written CURRENT pointers.
///
/// # Errors
///
/// Propagates `std::io::Error` from the in-memory buffer (cannot
/// realistically fail; the signature is `crate::Result` for
/// composability with callers in `version::persist` and
/// `checkpoint::write_current_for_version`).
pub fn compute(version_id: u64, footer: &FooterPayload) -> crate::Result<u128> {
    let mut canonical: Vec<u8> = Vec::with_capacity(64 + footer.sections.len() * 64);

    // Phase 1: identity prefix.
    canonical.write_u64::<LittleEndian>(version_id)?;
    canonical.write_u8(footer.layout_version)?;
    canonical.write_u8(footer.flags)?;
    // u32 is enough (writer caps section count via the u16 in the
    // footer payload itself; widening to u32 here is a forward-
    // compat headroom).
    let section_count_u32 = u32::try_from(footer.sections.len()).map_err(|_| {
        crate::Error::ManifestFooterInvalid(
            "section count exceeds u32 — manifest layout invariant violated",
        )
    })?;
    canonical.write_u32::<LittleEndian>(section_count_u32)?;

    // Phase 2: sorted TOC. Clone into a Vec<&TocEntry> so the
    // public `footer.sections` isn't reordered as a side effect of
    // digest computation.
    let mut sorted: Vec<&TocEntry> = footer.sections.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));

    for entry in sorted {
        let name_len_u16 = u16::try_from(entry.name.len()).map_err(|_| {
            crate::Error::ManifestFooterInvalid(
                "section name length exceeds u16 — manifest layout invariant violated",
            )
        })?;
        canonical.write_u16::<LittleEndian>(name_len_u16)?;
        canonical.write_all(entry.name.as_bytes())?;
        canonical.write_u64::<LittleEndian>(entry.block_offset)?;
        canonical.write_u32::<LittleEndian>(entry.block_size)?;
        canonical.write_u128::<LittleEndian>(entry.section_checksum)?;
    }

    Ok(xxhash_rust::xxh3::xxh3_128(&canonical))
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "tests panic on the unhappy paths to surface failures loudly"
)]
mod tests;
