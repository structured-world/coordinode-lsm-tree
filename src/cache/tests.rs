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
