/// Regression test: Tree::open must fail with InvalidData when the tree
/// directory contains a file with a non-UTF-8 name (filesystem corruption).
///
/// Before #209, `cleanup_orphaned_version` used `to_string_lossy()` and
/// silently skipped such entries. After migration to the Fs trait,
/// `Fs::read_dir` returns `InvalidData` — this test locks that behavior.
#[cfg(unix)]
#[test]
fn tree_reopen_rejects_non_utf8_filename_in_data_dir() {
    use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tree");

    // Phase 1: create a real tree with flushed data so recovery path is taken on reopen.
    {
        let tree = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()
        .unwrap();

        tree.insert("k", "v", 0);
        tree.flush_active_memtable(0).unwrap();
    }

    // Phase 2: inject a file with invalid UTF-8 bytes in its name.
    let bad_name = OsStr::from_bytes(&[b'v', 0xFF, 0xFE]);
    let bad_path = path.join(bad_name);
    if std::fs::write(&bad_path, b"corrupt").is_err() {
        // Filesystem rejected the non-UTF-8 filename (e.g., overlay or
        // restrictive mount options) — skip gracefully.
        return;
    }

    // Phase 3: reopen must fail because cleanup_orphaned_version calls
    // Fs::read_dir which rejects non-UTF-8 entries with InvalidData.
    let result = Config::new(
        &path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("reopen should fail on non-UTF-8 filename"),
    };
    match err {
        lsm_tree::Error::Io(ref io_err) => {
            assert_eq!(
                io_err.kind(),
                std::io::ErrorKind::InvalidData,
                "expected InvalidData, got: {err:?}"
            );
        }
        other => panic!("expected Io(InvalidData), got: {other:?}"),
    }
}
