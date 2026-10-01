use super::parent_len;

/// The no-std key keeps the parent `std` gives a `/`-separated path: a
/// root-level key's parent is the root `/`, so a directory sync of a
/// root-level file syncs `/` rather than the current directory, and the root
/// has no parent.
#[test]
fn a_key_keeps_the_parent_std_gives_its_path() {
    for key in ["/edits-0", "/", "/tables/1", "tables/1", "a/b/c", "/a/b"] {
        let parent = std::path::Path::new(key)
            .parent()
            .map(|parent| parent.as_os_str().len());
        assert_eq!(parent_len(key), parent, "{key}");
    }
    // A key with no `/` has no parent in the object-key model, where `std`
    // answers the empty path; the directory of its entry is `.` either way.
    assert_eq!(parent_len("MANIFEST"), None);
    assert_eq!(parent_len(""), None);
}
