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

/// The objects a dropped table owned, as its section lists them.
fn owned(objects: &[(u64, u64)]) -> OwnedObjects {
    OwnedObjects::from(objects)
}

/// An object released twice is judged by its newest release: a read between
/// the two releases is stale, whichever order they are recorded in.
#[test]
fn a_second_release_after_the_read_makes_its_reference_stale() {
    let registry = Arc::new(ReleasedObjects::default());
    let _held = registry.register(2);
    registry.record(3, [handle(1, 0)], [owned(&[(7, 50)])]);
    registry.record(6, [handle(1, 0)], [owned(&[(7, 50)])]);
    registry.record(5, [handle(1, 0)], [owned(&[(7, 50)])]);
    assert!(registry.released_since(4, &handle(1, 0)));
    assert!(registry.released_since(4, &handle(7, 50)));
    assert!(!registry.released_since(6, &handle(1, 0)));
    assert!(!registry.released_since(6, &handle(7, 50)));
}

/// A whole-table drop releases exactly the objects the table owned: another
/// object in the same blob file, owned by a kept table, is untouched.
#[test]
fn a_table_drop_releases_only_the_objects_it_owned() {
    let registry = Arc::new(ReleasedObjects::default());
    let _held = registry.register(2);
    registry.record(3, [], [owned(&[(7, 0), (7, 900), (9, 10)])]);
    assert!(registry.released_since(2, &handle(7, 0)));
    assert!(registry.released_since(2, &handle(7, 900)));
    assert!(registry.released_since(2, &handle(9, 10)));
    assert!(
        !registry.released_since(2, &handle(7, 450)),
        "another object of the file"
    );
    assert!(!registry.released_since(2, &handle(8, 0)));
}

/// Whether reads hold references is what decides a drop reads its tables'
/// owned objects at all.
#[test]
fn reads_are_held_only_while_a_token_lives() {
    let registry = Arc::new(ReleasedObjects::default());
    assert!(!registry.holds_reads());
    let token = registry.register(4);
    assert!(registry.holds_reads());
    drop(token);
    assert!(!registry.holds_reads());
}

/// Records go once the last read older than them is released, so the
/// registry does not grow while no reference is held.
#[test]
fn records_go_with_the_last_read_older_than_them() {
    let registry = Arc::new(ReleasedObjects::default());
    let old = registry.register(2);
    let newer = registry.register(4);
    registry.record(3, [handle(1, 0)], [owned(&[(9, 0)])]);
    registry.record(5, [handle(1, 10)], []);
    drop(old);
    assert_eq!(
        registry.records.lock().objects.iter().collect::<Vec<_>>(),
        [(&handle(1, 10), &5)]
    );
    assert!(registry.records.lock().tables.is_empty());
    drop(newer);
    assert!(registry.records.lock().objects.is_empty());
    assert!(registry.records.lock().readers.is_empty());
}
