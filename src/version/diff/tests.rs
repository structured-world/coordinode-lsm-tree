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
