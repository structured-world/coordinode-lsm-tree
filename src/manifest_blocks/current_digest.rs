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
//!   [`Block::header::checksum`] (XXH3-128 of the on-disk Block
//!   bytes — computed at write time, copied verbatim into the TOC).
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

use crate::manifest_blocks::footer::{FooterPayload, TocEntry};
use byteorder::{LittleEndian, WriteBytesExt};
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
mod tests {
    use super::*;
    use crate::manifest_blocks::FLAG_FOOTER_MIRROR_ENABLED;

    fn entry(name: &str, offset: u64, size: u32, checksum: u128) -> TocEntry {
        TocEntry {
            name: name.to_string(),
            block_offset: offset,
            block_size: size,
            section_checksum: checksum,
        }
    }

    #[test]
    fn digest_is_deterministic() {
        // Same inputs → same hash. Foundation property — without
        // this nothing else in the layer works.
        let payload = FooterPayload::new(
            FLAG_FOOTER_MIRROR_ENABLED,
            vec![entry("tables", 4096, 128, 0xAA)],
        );
        let h1 = compute(7, &payload).unwrap();
        let h2 = compute(7, &payload).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn digest_differs_for_different_version_id() {
        // T1 mislinking detection: same TOC, different `version_id`
        // → distinct digests. Required so a CURRENT pointing at
        // `v{N}` can't be mistaken for one pointing at `v{M}`
        // even when the underlying manifests have identical TOCs.
        let payload = FooterPayload::new(0, vec![entry("a", 4096, 64, 0xAA)]);
        assert_ne!(compute(0, &payload).unwrap(), compute(1, &payload).unwrap());
    }

    #[test]
    fn digest_differs_when_section_checksum_changes() {
        // Per-section content binding: flipping one section's
        // checksum (which mirrors the section Block's own XXH3-128)
        // must change the CURRENT digest. This is the chain that
        // binds section content into CURRENT without the CURRENT
        // layer having to hash raw section bytes.
        let p1 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xAA)]);
        let p2 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xBB)]);
        assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
    }

    #[test]
    fn digest_is_order_independent() {
        // TOC sorting in the canonical form means two manifests
        // that differ only in the encoded order of their sections
        // produce the same CURRENT digest. Reading order does
        // matter for byte-level layout (offsets differ), but those
        // differences ARE captured in the per-entry offset field
        // — so this property is really "no spurious mismatch from
        // re-ordering a TOC that has the same content".
        let common = vec![entry("a", 4096, 64, 0xAA), entry("b", 4160, 64, 0xBB)];
        let reordered = vec![entry("b", 4160, 64, 0xBB), entry("a", 4096, 64, 0xAA)];
        let p1 = FooterPayload::new(0, common);
        let p2 = FooterPayload::new(0, reordered);
        assert_eq!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
    }

    #[test]
    fn digest_differs_when_section_offset_changes() {
        // Offset / size are part of the canonical form (they bind
        // the on-disk layout into CURRENT). Two TOCs with same
        // name + checksum but different offsets must hash to
        // distinct digests — a file whose sections moved is a
        // different file at the CURRENT-pointer layer.
        let p1 = FooterPayload::new(0, vec![entry("tables", 4096, 64, 0xAA)]);
        let p2 = FooterPayload::new(0, vec![entry("tables", 8192, 64, 0xAA)]);
        assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
    }

    #[test]
    fn digest_differs_when_layout_version_changes() {
        // layout_version bump = format-incompatible manifest. The
        // digest must surface that as a mismatch even if everything
        // else is identical, so a reader can't silently accept a
        // manifest written under a different layout convention.
        let mut p1 = FooterPayload::new(0, vec![entry("a", 4096, 64, 0xAA)]);
        let mut p2 = p1.clone();
        p1.layout_version = 2;
        p2.layout_version = 3;
        assert_ne!(compute(0, &p1).unwrap(), compute(0, &p2).unwrap());
    }
}
