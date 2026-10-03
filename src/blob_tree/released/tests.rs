use super::*;
use test_log::test;

fn handle(blob_file_id: u64, offset: u64) -> ValueHandle {
    ValueHandle {
        blob_file_id,
        offset,
        on_disk_size: 10,
    }
}

/// A release with no read held is not kept: no handed-out reference can be
/// stale through it.
#[test]
fn a_release_without_a_held_read_is_not_kept() {
    let registry = Arc::new(ReleasedObjects::default());
    registry.record(5, [handle(1, 0)], []);
    let token = registry.register(6);
    assert!(!registry.released_since(token.version(), &handle(1, 0)));
}

/// A release after the version a held read took makes its reference stale;
/// a release at or before it does not, and another object is untouched.
#[test]
fn only_a_release_after_the_read_makes_its_reference_stale() {
    let registry = Arc::new(ReleasedObjects::default());
    let early = registry.register(3);
    registry.record(3, [handle(1, 0)], []);
    registry.record(4, [handle(1, 10)], []);
    assert!(
        !registry.released_since(3, &handle(1, 0)),
        "released at the read itself"
    );
    assert!(registry.released_since(3, &handle(1, 10)));
    assert!(
        !registry.released_since(4, &handle(1, 10)),
        "read after the release"
    );
    assert!(
        !registry.released_since(3, &handle(1, 20)),
        "another object"
    );
    drop(early);
}

/// A whole-table drop releases every reference into its files read before it.
#[test]
fn a_file_release_makes_every_reference_into_it_stale() {
    let registry = Arc::new(ReleasedObjects::default());
    let _held = registry.register(2);
    registry.record(3, [], [7]);
    assert!(registry.released_since(2, &handle(7, 0)));
    assert!(registry.released_since(2, &handle(7, 900)));
    assert!(!registry.released_since(2, &handle(8, 0)));
}

/// Records go once the last read older than them is released, so the
/// registry does not grow while no reference is held.
#[test]
fn records_go_with_the_last_read_older_than_them() {
    let registry = Arc::new(ReleasedObjects::default());
    let old = registry.register(2);
    let newer = registry.register(4);
    registry.record(3, [handle(1, 0)], [9]);
    registry.record(5, [handle(1, 10)], []);
    drop(old);
    assert_eq!(registry.records.lock().objects, vec![(5, handle(1, 10))]);
    assert!(registry.records.lock().files.is_empty());
    drop(newer);
    assert!(registry.records.lock().objects.is_empty());
    assert!(registry.records.lock().readers.is_empty());
}
