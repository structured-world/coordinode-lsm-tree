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

/// Maps the precision enum to its on-disk byte.
fn precision_byte(p: LocatorPrecision) -> u8 {
    match p {
        LocatorPrecision::Restart => 0,
        LocatorPrecision::Entry => 1,
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

    let block_id_bits = spec.block_id_bits.unwrap_or_else(|| bits_for(max_block));
    let slot_bits = spec.slot_bits.unwrap_or_else(|| bits_for(max_slot));

    // Explicit widths that cannot hold the real layout → graceful skip.
    let block_fits = block_id_bits >= bits_for(max_block);
    let slot_fits = slot_bits >= bits_for(max_slot);
    let r = u16::from(block_id_bits) + u16::from(slot_bits);
    if !block_fits || !slot_fits || r == 0 || r > 64 {
        log::debug!(
            "locator section skipped: widths block_id_bits={block_id_bits} slot_bits={slot_bits} \
             cannot represent max_block={max_block} max_slot={max_slot} (r={r})"
        );
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "r is validated to 1..=64 above, fits u8"
    )]
    let r_u8 = r as u8;

    let mut hashes = Vec::with_capacity(entries.len());
    let mut locators = Vec::with_capacity(entries.len());
    for &(hash, block_id, slot) in entries {
        hashes.push(hash);
        // Pack (block_id, slot) into the low r bits. Both are proven to fit
        // their widths by the checks above.
        locators.push((block_id << slot_bits) | slot);
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

/// Test-only reader: parse a framed section and recover `(block_id, slot)` for
/// `hash`. The production reader lands with the point-read path; this mirrors its
/// unpack so the write-side build can be round-trip-tested here and from the
/// table-writer integration test. `None` = the ribbon could not answer.
#[cfg(test)]
#[expect(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "test helper over a freshly-built section; out-of-range or parse \
              failure is a bug to surface via panic"
)]
pub fn locate_in_section(section: &[u8], hash: u64) -> Option<(u64, u64)> {
    use crate::table::filter::ribbon::burr::recover_value_from_bytes;
    assert!(section.len() >= SECTION_HEADER_LEN);
    assert_eq!(section[0], SECTION_VERSION);
    let slot_bits = section[3];
    let packed = recover_value_from_bytes(&section[SECTION_HEADER_LEN..], hash).unwrap()?;
    let slot_mask = if slot_bits == 0 {
        0
    } else if slot_bits >= 64 {
        u64::MAX
    } else {
        (1u64 << slot_bits) - 1
    };
    Some((packed >> slot_bits, packed & slot_mask))
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
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
                locate_in_section(&bytes, key_hash(i)),
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
                locate_in_section(&bytes, key_hash(i)),
                Some((i % 1000, i % 4000))
            );
        }
    }
}
