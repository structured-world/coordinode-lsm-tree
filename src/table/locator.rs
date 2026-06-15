// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Per-SST retrieval-ribbon locator section (write side).
//!
//! Optional, format-gated section that maps every key in a table to a packed
//! `(block_id, slot)` locator via a retrieval `BuRR` ribbon (see
//! [`crate::table::filter::ribbon::burr`]). A point read recovers the locator
//! in O(1), fetches the addressed data block, and verifies the key at the slot,
//! skipping both the index-block and in-block binary searches.
//!
//! This module builds and frames the section at table-write time. The point-read
//! consumer (recovering `(block_id, slot)` from the section and resolving it to a
//! value) lives on the read path.
//!
//! The section is written only when the [`crate::config::LocatorPolicy`] enables
//! it for the table's level; an unenabled table emits nothing (the section is
//! absent from the TOC, so on-disk output is byte-identical). The section is a
//! new TOC entry, not a format-version bump.
//!
//! # Section layout
//!
//! ```text
//! offset  size  field
//! ──────  ────  ──────────────────────────────────────────
//! 0       1     section_version (= SECTION_VERSION)
//! 1       1     precision (0 = Restart, 1 = Entry)
//! 2       1     block_id_bits
//! 3       1     slot_bits
//! 4       —     retrieval BuRR wire bytes (its own magic/header follows)
//! ```
//!
//! `r = block_id_bits + slot_bits` is the ribbon width; the locator value is
//! `(block_id << slot_bits) | slot`.

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use crate::config::LocatorPrecision;
use crate::table::filter::ribbon::burr::{BurrBuilder, BurrParams};

/// Section format version. Bumped only if this section's framing changes
/// incompatibly; independent of the SST format version.
pub const SECTION_VERSION: u8 = 1;

/// Fixed section header length: version + precision + `block_id_bits` +
/// `slot_bits`.
pub const SECTION_HEADER_LEN: usize = 4;

/// Resolved locator settings for a single table writer (one level's policy
/// entry, already selected from the per-level [`crate::config::LocatorPolicy`]).
#[derive(Copy, Clone, Debug)]
pub struct LocatorSpec {
    /// What `slot` addresses (restart index vs exact entry index).
    pub precision: LocatorPrecision,
    /// Explicit data-block-id width, or `None` for auto (minimal per SST).
    pub block_id_bits: Option<u8>,
    /// Explicit slot width, or `None` for auto (minimal per SST).
    pub slot_bits: Option<u8>,
}

/// Bits needed to represent every value in `0..=max` (at least 1, so the ribbon
/// width is always valid).
fn bits_for(max: u64) -> u8 {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "64 - leading_zeros() is in 0..=64, fits u8"
    )]
    let bits = (64 - max.leading_zeros()) as u8;
    bits.max(1)
}

/// On-disk precision byte: `slot` is a restart index.
const PRECISION_RESTART: u8 = 0;
/// On-disk precision byte: `slot` is an exact entry index.
const PRECISION_ENTRY: u8 = 1;
/// On-disk precision byte: per-block (no `slot`; `slot_bits == 0`).
const PRECISION_BLOCK: u8 = 2;

/// Maps the precision enum to its on-disk byte.
fn precision_byte(p: LocatorPrecision) -> u8 {
    match p {
        LocatorPrecision::Restart => PRECISION_RESTART,
        LocatorPrecision::Entry => PRECISION_ENTRY,
        LocatorPrecision::Block => PRECISION_BLOCK,
    }
}

/// Build the `locator` section bytes from accumulated `(hash, block_id, slot)`
/// triples (one per unique key, the newest version's position).
///
/// Returns `None` to **gracefully skip** the section (no bytes written, the
/// point read falls back to the index) when:
/// - there are no entries, or
/// - explicit widths cannot represent this SST's actual `block_id` / `slot`
///   range, or the combined width exceeds 64 bits, or
/// - the ribbon build fails (e.g. a u64 hash collision between two distinct
///   keys yields an inconsistent equation).
///
/// `Some(bytes)` is the framed section ready to write. The build never returns
/// an error: every failure degrades to a graceful skip rather than aborting the
/// SST write.
#[must_use]
pub fn build_locator_section(entries: &[(u64, u64, u64)], spec: LocatorSpec) -> Option<Vec<u8>> {
    if entries.is_empty() {
        return None;
    }

    let max_block = entries.iter().map(|e| e.1).max().unwrap_or(0);
    let max_slot = entries.iter().map(|e| e.2).max().unwrap_or(0);

    // Per-block precision drops `slot` entirely (locator = block_id), so its
    // width is 0 regardless of the recorded slot values and there is no slot
    // fit to check.
    let block_only = spec.precision == LocatorPrecision::Block;
    let block_id_bits = spec.block_id_bits.unwrap_or_else(|| bits_for(max_block));
    let slot_bits = if block_only {
        0
    } else {
        spec.slot_bits.unwrap_or_else(|| bits_for(max_slot))
    };

    // Explicit widths that cannot hold the real layout → graceful skip.
    let block_fits = block_id_bits >= bits_for(max_block);
    let slot_fits = block_only || slot_bits >= bits_for(max_slot);
    let r = u16::from(block_id_bits) + u16::from(slot_bits);
    if !block_fits || !slot_fits || r == 0 || r > 64 {
        log::debug!(
            "locator section skipped: widths block_id_bits={block_id_bits} slot_bits={slot_bits} \
             cannot represent max_block={max_block} max_slot={max_slot} (r={r})"
        );
        return None;
    }
    let slot_mask: u64 = if slot_bits == 0 {
        0
    } else if slot_bits >= 64 {
        u64::MAX
    } else {
        (1u64 << slot_bits) - 1
    };
    #[expect(
        clippy::cast_possible_truncation,
        reason = "r is validated to 1..=64 above, fits u8"
    )]
    let r_u8 = r as u8;

    let mut hashes = Vec::with_capacity(entries.len());
    let mut locators = Vec::with_capacity(entries.len());
    for &(hash, block_id, slot) in entries {
        hashes.push(hash);
        // Pack (block_id, slot) into the low r bits. block_id is proven to fit;
        // slot is masked to its width (0 for per-block precision → block_id
        // only).
        locators.push((block_id << slot_bits) | (slot & slot_mask));
    }

    let params = match BurrParams::with_bpk(entries.len(), f32::from(r_u8)) {
        Ok(p) => p,
        Err(e) => {
            log::debug!("locator section skipped: BurrParams::with_bpk failed: {e}");
            return None;
        }
    };
    let builder = match BurrBuilder::new(params) {
        Ok(b) => b,
        Err(e) => {
            log::debug!("locator section skipped: BurrBuilder::new failed: {e}");
            return None;
        }
    };
    let filter = match builder.build_from_hashes_with_values(&hashes, &locators) {
        Ok(f) => f,
        Err(e) => {
            // A u64 hash collision between two distinct keys yields an
            // inconsistent equation; rather than abort the SST, skip the
            // section and let the point read use the index.
            log::debug!("locator section skipped: retrieval ribbon build failed: {e}");
            return None;
        }
    };

    let wire = filter.to_wire_bytes();
    let mut section = Vec::with_capacity(SECTION_HEADER_LEN + wire.len());
    section.push(SECTION_VERSION);
    section.push(precision_byte(spec.precision));
    section.push(block_id_bits);
    section.push(slot_bits);
    section.extend_from_slice(&wire);
    Some(section)
}

/// Recover the `(block_id, slot)` locator stored for `hash` in a framed
/// `locator` section.
///
/// For a key in the table this is exact; for an absent key it is an unspecified
/// pair (the caller verifies the key at the located block, so a stray locator
/// only costs a wasted block read, never a wrong answer). `Ok(None)` means the
/// ribbon could not answer and the caller should fall back to the sorted index.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] if the section is shorter than its
/// fixed header or carries an unrecognised section version, or propagates a
/// wire-parse error from the retrieval ribbon.
#[expect(
    clippy::indexing_slicing,
    reason = "header bytes [0..4) are gated by the `section.len() < SECTION_HEADER_LEN` \
              check on the line above; the wire slice starts at the validated header end"
)]
pub fn locate(section: &[u8], hash: u64) -> crate::Result<Option<(u64, u64)>> {
    use crate::table::filter::ribbon::burr::recover_value_from_bytes;
    if section.len() < SECTION_HEADER_LEN {
        return Err(crate::Error::InvalidHeader("LocatorSection"));
    }
    if section[0] != SECTION_VERSION {
        return Err(crate::Error::InvalidHeader("LocatorSection version"));
    }
    let slot_bits = section[3];
    let Some(packed) = recover_value_from_bytes(&section[SECTION_HEADER_LEN..], hash)? else {
        return Ok(None);
    };
    let slot_mask = if slot_bits == 0 {
        0
    } else if slot_bits >= 64 {
        u64::MAX
    } else {
        (1u64 << slot_bits) - 1
    };
    // Guard the block-id shift the same way the mask is guarded: `packed >> 64`
    // panics in debug and wraps to `>> 0` in release. A valid section always has
    // `slot_bits <= 63`, so this only matters for a checksum-surviving corruption
    // — but it keeps both extractions consistent and panic-free.
    let block_id = if slot_bits >= 64 {
        0
    } else {
        packed >> slot_bits
    };
    Ok(Some((block_id, packed & slot_mask)))
}

/// A loaded `locator` section ready for point-read resolution: the framed
/// section bytes plus an ordinal → data-block-handle map.
///
/// `block_id` is the data block's 0-based ordinal in key (write) order, which
/// the SST index yields in iteration order, so `blocks[block_id]` is the handle
/// of the block the writer addressed. Built once at table open.
#[derive(Debug)]
pub struct LoadedLocator {
    section: crate::Slice,
    blocks: Vec<crate::table::BlockHandle>,
    /// The on-disk precision byte (`PRECISION_*`), governing how a recovered
    /// `slot` is interpreted on the read path.
    precision: u8,
}

/// A resolved locator: the data block holding the key's newest version, plus an
/// optional in-block slot hint. `Some((slot, is_entry))` lets the read jump
/// straight to the restart head (`is_entry` selects entry-index vs restart-index
/// semantics); `None` (per-block precision) leaves the in-block binary search.
pub type Located = (crate::table::BlockHandle, Option<(u64, bool)>);

impl LoadedLocator {
    /// Wrap the framed section bytes and the ordinal → handle map. The precision
    /// is read from the section header (defaulting to per-block, the safe
    /// no-slot-hint mode, if the header is somehow short).
    #[must_use]
    pub fn new(section: crate::Slice, blocks: Vec<crate::table::BlockHandle>) -> Self {
        let precision = section.get(1).copied().unwrap_or(PRECISION_BLOCK);
        Self {
            section,
            blocks,
            precision,
        }
    }

    /// Resolve `key_hash` to the data block holding the key's newest version and
    /// an optional in-block slot hint.
    ///
    /// `Ok(None)` means the ribbon could not answer or the recovered `block_id`
    /// is out of range; the caller falls back to the sorted index. The caller
    /// MUST still verify the key inside the returned block: an absent key yields
    /// a stray locator (a wasted block read), never a wrong answer.
    ///
    /// # Errors
    ///
    /// Propagates a parse error from [`locate`].
    pub fn locate_block(&self, key_hash: u64) -> crate::Result<Option<Located>> {
        let Some((block_id, slot)) = locate(&self.section, key_hash)? else {
            return Ok(None);
        };
        let Some(handle) = usize::try_from(block_id)
            .ok()
            .and_then(|i| self.blocks.get(i))
            .copied()
        else {
            return Ok(None);
        };
        let hint = match self.precision {
            PRECISION_RESTART => Some((slot, false)),
            PRECISION_ENTRY => Some((slot, true)),
            // Per-block (or an unrecognised precision): no slot hint, the block's
            // own point_read does the in-block lookup.
            _ => None,
        };
        Ok(Some((handle, hint)))
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::indexing_slicing,
        reason = "test assertions over known-good fixtures; failure surfaces via panic"
    )]

    use super::*;

    fn key_hash(i: u64) -> u64 {
        crate::hash::hash64(&i.to_le_bytes())
    }

    #[test]
    fn build_returns_none_for_empty_input() {
        let spec = LocatorSpec {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        };
        assert!(build_locator_section(&[], spec).is_none());
    }

    #[test]
    fn auto_width_section_round_trips_block_id_and_slot() {
        // 300 keys across 12 blocks, up to 25 restarts per block.
        let spec = LocatorSpec {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        };
        let entries: Vec<(u64, u64, u64)> =
            (0..300u64).map(|i| (key_hash(i), i % 12, i % 25)).collect();
        let bytes = build_locator_section(&entries, spec).expect("section built");
        // Auto widths: 12 blocks → 4 bits, 25 slots → 5 bits.
        assert_eq!(bytes[2], 4);
        assert_eq!(bytes[3], 5);
        for i in 0..300u64 {
            assert_eq!(
                locate(&bytes, key_hash(i)).unwrap(),
                Some((i % 12, i % 25)),
                "key {i} locator mismatch",
            );
        }
    }

    #[test]
    fn explicit_widths_too_small_skips_gracefully() {
        // max block_id = 100 needs 7 bits; configure only 4 → skip.
        let spec = LocatorSpec {
            precision: LocatorPrecision::Restart,
            block_id_bits: Some(4),
            slot_bits: Some(4),
        };
        let entries: Vec<(u64, u64, u64)> =
            (0..200u64).map(|i| (key_hash(i), i % 101, i % 8)).collect();
        assert!(build_locator_section(&entries, spec).is_none());
    }

    #[test]
    fn explicit_widths_that_fit_round_trip() {
        let spec = LocatorSpec {
            precision: LocatorPrecision::Entry,
            block_id_bits: Some(10),
            slot_bits: Some(12),
        };
        let entries: Vec<(u64, u64, u64)> = (0..500u64)
            .map(|i| (key_hash(i), i % 1000, i % 4000))
            .collect();
        let bytes = build_locator_section(&entries, spec).expect("section built");
        assert_eq!(bytes[2], 10);
        assert_eq!(bytes[3], 12);
        for i in 0..500u64 {
            assert_eq!(
                locate(&bytes, key_hash(i)).unwrap(),
                Some((i % 1000, i % 4000))
            );
        }
    }

    #[test]
    fn block_precision_section_round_trips_block_id_only() {
        // Block precision is the default: `slot` is dropped (locator = block_id),
        // so every key resolves to (block_id, 0). Covers the `block_only` build
        // path and a zero-width slot decode.
        let spec = LocatorSpec {
            precision: LocatorPrecision::Block,
            block_id_bits: None,
            slot_bits: None,
        };
        let entries: Vec<(u64, u64, u64)> =
            (0..300u64).map(|i| (key_hash(i), i % 12, i % 25)).collect();
        let bytes = build_locator_section(&entries, spec).expect("section built");
        assert_eq!(bytes[3], 0, "block precision must record slot_bits = 0");
        for i in 0..300u64 {
            assert_eq!(
                locate(&bytes, key_hash(i)).unwrap(),
                Some((i % 12, 0)),
                "key {i} must resolve to its block with slot 0",
            );
        }
    }

    #[test]
    fn locate_rejects_truncated_section() {
        // A section shorter than the fixed header cannot be parsed.
        let err = locate(&[0u8; SECTION_HEADER_LEN - 1], 123).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidHeader("LocatorSection")));
    }

    #[test]
    fn build_skips_when_ribbon_cannot_satisfy_conflicting_values() {
        // Two entries share a key hash but map to different locators, so the
        // retrieval ribbon has no consistent solution. The build must fail
        // gracefully and skip the section (the point read falls back to the
        // index) rather than abort the SST.
        let spec = LocatorSpec {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        };
        let mut entries: Vec<(u64, u64, u64)> =
            (0..200u64).map(|i| (key_hash(i), i % 8, i % 4)).collect();
        // Re-use entry 0's hash with a different (block_id, slot) → conflict.
        entries.push((key_hash(0), 7, 3));
        assert!(
            build_locator_section(&entries, spec).is_none(),
            "a conflicting hash collision must skip the section, not panic or abort",
        );
    }

    #[test]
    fn locate_does_not_panic_on_forged_slot_bits_64() {
        // The writer never emits slot_bits == 64 (it enforces block_id_bits >= 1
        // and r <= 64), but a checksum-surviving corruption could. The block-id
        // extraction `packed >> slot_bits` must be guarded the same way the slot
        // mask is: `>> 64` panics in debug and wraps to `>> 0` in release. locate
        // must return without panicking for every key.
        let spec = LocatorSpec {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        };
        let entries: Vec<(u64, u64, u64)> =
            (0..100u64).map(|i| (key_hash(i), i % 8, i % 4)).collect();
        let mut bytes = build_locator_section(&entries, spec).expect("section built");
        bytes[3] = 64; // forge slot_bits = 64
        for i in 0..100u64 {
            // Decoded values may be garbage, but the shift must not panic.
            let _ = locate(&bytes, key_hash(i)).expect("locate must not error");
        }
    }

    #[test]
    fn locate_rejects_unknown_version() {
        // A header whose version byte is not the one this build writes is a
        // forward-incompatibility / corruption signal, surfaced as an error.
        let mut section = [0u8; SECTION_HEADER_LEN];
        section[0] = SECTION_VERSION.wrapping_add(1);
        let err = locate(&section, 123).unwrap_err();
        assert!(matches!(
            err,
            crate::Error::InvalidHeader("LocatorSection version")
        ));
    }
}
