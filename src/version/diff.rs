// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Compute the [`VersionEdit`] between two consecutive [`Version`]s.
//!
//! [`Version::diff`] is the producer side of the incremental manifest: given the
//! prior version and the one a flush / compaction just built, it emits the
//! delta the engine appends to the edit log. The delta carries the full new run
//! layout of every level that changed (so recovery rebuilds run grouping
//! verbatim — see [`super::edit`]) plus per-id blob add / remove and a GC-stats
//! snapshot when it changed.

use super::Version;
use super::edit::{AddedBlobFile, ChangedLevel, TableDesc, VersionEdit};
use crate::coding::Encode;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

impl Version {
    /// Computes the edit that turns `prior` into `self`.
    ///
    /// A level is emitted (with its full new run layout) only when its layout
    /// differs from `prior`; unchanged levels are omitted. Blob files are a flat
    /// id-keyed list, so they get a per-id add / remove delta. GC stats are
    /// included only when they changed.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding the GC-stats snapshot fails, or
    /// [`crate::Error::Unrecoverable`] if the level count exceeds `u8::MAX` (the
    /// format caps levels at 255, so this is unreachable for any version the
    /// engine builds).
    pub(crate) fn diff(&self, prior: &Self) -> crate::Result<VersionEdit> {
        let level_count = self.level_count().max(prior.level_count());
        let mut changed_levels = Vec::new();
        for idx in 0..level_count {
            let cur = level_runs(self.level(idx));
            let old = level_runs(prior.level(idx));
            if cur != old {
                let level = u8::try_from(idx).map_err(|_| crate::Error::Unrecoverable)?;
                changed_levels.push(ChangedLevel { level, runs: cur });
            }
        }

        // Blob files added or whose checksum changed.
        let mut added_blob_files = Vec::new();
        for bf in self.blob_files.iter() {
            let cur = bf.checksum().into_u128();
            let was = prior
                .blob_files
                .get(bf.id())
                .map(|p| p.checksum().into_u128());
            if was != Some(cur) {
                added_blob_files.push(AddedBlobFile {
                    id: bf.id(),
                    checksum: cur,
                });
            }
        }
        // Blob files present before but gone now.
        let mut removed_blob_file_ids = Vec::new();
        for bf in prior.blob_files.iter() {
            if !self.blob_files.contains_key(bf.id()) {
                removed_blob_file_ids.push(bf.id());
            }
        }

        let gc_stats = if self.gc_stats() == prior.gc_stats() {
            None
        } else {
            let mut buf = Vec::new();
            self.gc_stats().encode_into(&mut buf)?;
            Some(buf)
        };

        Ok(VersionEdit {
            new_version_id: self.id(),
            changed_levels,
            added_blob_files,
            removed_blob_file_ids,
            gc_stats,
        })
    }
}

/// The run layout of one level as plain descriptors, for cheap structural
/// comparison and direct reuse as a [`ChangedLevel`]'s `runs`. An absent level
/// (index past the version's level count) is an empty layout.
fn level_runs(level: Option<&super::Level>) -> Vec<Vec<TableDesc>> {
    level.map_or_else(Vec::new, |lvl| {
        lvl.iter()
            .map(|run| {
                run.iter()
                    .map(|t| TableDesc {
                        id: t.id(),
                        checksum: t.checksum().into_u128(),
                        global_seqno: t.global_seqno(),
                    })
                    .collect()
            })
            .collect()
    })
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::TreeType;
    use crate::blob_tree::FragmentationMap;
    use crate::version::BlobFileList;

    fn empty_version(id: u64) -> Version {
        Version::new(id, TreeType::Standard)
    }

    #[test]
    fn diff_of_two_empty_versions_only_bumps_the_id() {
        let v1 = empty_version(1);
        let v2 = empty_version(2);
        let edit = v2.diff(&v1).expect("diff");
        assert_eq!(edit.new_version_id, 2);
        assert!(edit.changed_levels.is_empty(), "no level changed");
        assert!(edit.added_blob_files.is_empty());
        assert!(edit.removed_blob_file_ids.is_empty());
        assert!(
            edit.gc_stats.is_none(),
            "identical (empty) GC stats are not re-emitted",
        );
    }

    #[test]
    fn diff_emits_gc_stats_only_when_changed() {
        let v1 = empty_version(1);
        // Build v2 with a non-empty GC-stats map (everything else empty).
        let mut gc = FragmentationMap::default();
        gc.insert(7, crate::blob_tree::FragmentationEntry::new(3, 100, 120));
        let v2 = Version::from_levels(2, TreeType::Standard, vec![], BlobFileList::default(), gc);

        let edit = v2.diff(&v1).expect("diff");
        assert!(
            edit.gc_stats.is_some(),
            "changed GC stats must be carried in the edit",
        );
        // And a diff against an identical map drops it again.
        let v3 = Version::from_levels(
            3,
            TreeType::Standard,
            vec![],
            BlobFileList::default(),
            v2.gc_stats().clone(),
        );
        let edit2 = v3.diff(&v2).expect("diff");
        assert!(
            edit2.gc_stats.is_none(),
            "unchanged GC stats between v2 and v3 are not re-emitted",
        );
    }

    #[test]
    fn diff_handles_growing_level_count_without_panic() {
        // prior has 0 levels, self has 2 (both empty) → still no changed levels,
        // and the u8 level-index conversion stays in range.
        let v1 = empty_version(1);
        let v2 = Version::from_levels(
            2,
            TreeType::Standard,
            vec![
                crate::version::Level::from_runs(vec![]),
                crate::version::Level::from_runs(vec![]),
            ],
            BlobFileList::default(),
            FragmentationMap::default(),
        );
        let edit = v2.diff(&v1).expect("diff");
        assert!(
            edit.changed_levels.is_empty(),
            "empty levels on both sides are equal regardless of count",
        );
    }
}
