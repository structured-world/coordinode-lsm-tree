use super::Cache;

#[test]
fn metadata_priority_defaults_on_and_toggles() {
    // On by default.
    assert!(Cache::with_capacity_bytes(1024).metadata_priority());
    // Builder turns it off and back on.
    let off = Cache::with_capacity_bytes(1024).with_metadata_priority(false);
    assert!(!off.metadata_priority());
    assert!(off.with_metadata_priority(true).metadata_priority());
}

/// A blob cache entry is keyed by a POSITION in a blob file, and a corrupt
/// index entry can point a second key at a position that already holds another
/// key's value. A direct read catches that: the reader compares the key it was
/// asked for against the one stored in the record, and refuses the mismatch.
/// The cached path has to reach the same verdict, or a corrupt entry served
/// from cache would silently return the neighbouring key's value instead of
/// reporting the corruption.
#[test]
fn a_blob_lookup_under_a_conflicting_key_misses_rather_than_serving_the_other_value() {
    use crate::vlog::ValueHandle;

    let cache = Cache::with_capacity_bytes(1024 * 1024);
    let vhandle = ValueHandle {
        blob_file_id: 7,
        offset: 4096,
        on_disk_size: 5,
    };

    cache.insert_blob(
        0,
        &vhandle,
        b"real-key",
        crate::UserValue::from(&b"value"[..]),
    );

    // The key it was stored under still finds it.
    assert_eq!(
        cache.get_blob(0, &vhandle, b"real-key").as_deref(),
        Some(&b"value"[..]),
        "the owning key must still hit",
    );

    // A different key pointed at the same position must NOT be served this
    // value; it reads as a miss so the caller does the real read and gets the
    // real error.
    assert!(
        cache.get_blob(0, &vhandle, b"other-key").is_none(),
        "a conflicting key must not be served the value at that offset",
    );
}
