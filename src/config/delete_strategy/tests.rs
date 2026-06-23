use super::*;

#[test]
fn default_is_adaptive_five_percent() {
    assert_eq!(
        DeleteStrategy::default(),
        DeleteStrategy::Adaptive {
            purge_threshold_percent: 5
        }
    );
}

#[test]
fn writes_bitmap_only_for_mor_and_adaptive() {
    assert!(!DeleteStrategy::CopyOnWrite.writes_bitmap());
    assert!(DeleteStrategy::MergeOnRead.writes_bitmap());
    assert!(DeleteStrategy::default_adaptive().writes_bitmap());
}

#[test]
fn policy_get_clamps_to_last_level() {
    let policy =
        DeleteStrategyPolicy::new([DeleteStrategy::MergeOnRead, DeleteStrategy::CopyOnWrite]);
    assert_eq!(policy.get(0), DeleteStrategy::MergeOnRead);
    assert_eq!(policy.get(1), DeleteStrategy::CopyOnWrite);
    // Levels past the end reuse the last entry (read-heavy bottom).
    assert_eq!(policy.get(2), DeleteStrategy::CopyOnWrite);
    assert_eq!(policy.get(99), DeleteStrategy::CopyOnWrite);
}

#[test]
fn all_applies_everywhere() {
    let policy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    assert_eq!(policy.get(0), DeleteStrategy::MergeOnRead);
    assert_eq!(policy.get(7), DeleteStrategy::MergeOnRead);
}

#[test]
#[should_panic(expected = "may not be empty")]
fn new_rejects_empty_policy() {
    let _ = DeleteStrategyPolicy::new(Vec::new());
}

#[test]
#[should_panic(expected = "adaptive purge threshold must be in 0..=100")]
fn all_rejects_threshold_above_100() {
    let _ = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
        purge_threshold_percent: 101,
    });
}

#[test]
#[should_panic(expected = "adaptive purge threshold must be in 0..=100")]
fn new_rejects_threshold_above_100() {
    let _ = DeleteStrategyPolicy::new([
        DeleteStrategy::MergeOnRead,
        DeleteStrategy::Adaptive {
            purge_threshold_percent: 200,
        },
    ]);
}

#[test]
fn threshold_100_is_accepted() {
    // The documented upper bound is inclusive.
    let policy = DeleteStrategyPolicy::all(DeleteStrategy::Adaptive {
        purge_threshold_percent: 100,
    });
    assert_eq!(
        policy.get(0),
        DeleteStrategy::Adaptive {
            purge_threshold_percent: 100
        }
    );
}
