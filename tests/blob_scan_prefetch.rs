//! Scan read-ahead over separated values must be invisible in the results.
//!
//! The prefetch reads a window of upcoming blob records in one coalesced read
//! and parses each out of that buffer. That is a different I/O shape from the
//! per-value read it replaces, so these pin the property that matters: whatever
//! the window size, and however the values are spread across blob files, a scan
//! yields exactly what it yields with read-ahead switched off.

use lsm_tree::{
    AbstractTree, Config, Guard as _, KvSeparationOptions, SeqNo, SequenceNumberCounter, UserValue,
    fs::{BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs},
    io,
};
use std::path::Path;

/// Values large enough to be separated out of the index, and distinct enough
/// that a record parsed from the wrong offset in a coalesced buffer cannot
/// accidentally compare equal.
fn value_for(i: usize) -> Vec<u8> {
    let mut v = format!("value-{i:06}-").into_bytes();
    // `i % 17` is 0..=16, so the byte conversion is exact.
    let fill = u8::try_from(i % 17).unwrap_or(0);
    v.resize(400, b'a'.wrapping_add(fill));
    v
}

fn key_for(i: usize) -> String {
    format!("key{i:06}")
}

/// Opens a KV-separated tree with the given read-ahead window and fills it with
/// `n` separated values, flushing every `flush_every` inserts so the values end
/// up spread over that many blob files.
fn tree_with(
    path: &std::path::Path,
    scan_prefetch: u16,
    n: usize,
    flush_every: usize,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .scan_prefetch(scan_prefetch),
        ))
        .open()?;

    for i in 0..n {
        tree.insert(key_for(i).as_bytes(), value_for(i), seqno.next());
        if flush_every > 0 && (i + 1) % flush_every == 0 {
            tree.flush_active_memtable(0)?;
        }
    }
    tree.flush_active_memtable(0)?;

    Ok(tree)
}

fn collect(tree: &lsm_tree::AnyTree) -> lsm_tree::Result<Vec<(Vec<u8>, UserValue)>> {
    tree.iter(SeqNo::MAX, None)
        .map(|guard| {
            let (k, v) = guard.into_inner()?;
            Ok((k.to_vec(), v))
        })
        .collect()
}

/// The headline property, over enough entries that the window refills several
/// times and enough blob files that a window straddles a file boundary (the
/// prefetch has to break its coalesced read there rather than read across it).
#[test_log::test]
fn a_scan_yields_the_same_values_with_and_without_prefetch() -> lsm_tree::Result<()> {
    const N: usize = 500;
    // 7 blob files, and 7 is coprime with the 64-item window so boundaries land
    // at a different position in each window rather than always at its edge.
    const FLUSH_EVERY: usize = 71;

    let off_dir = tempfile::tempdir()?;
    let on_dir = tempfile::tempdir()?;

    let off = collect(&tree_with(off_dir.path(), 0, N, FLUSH_EVERY)?)?;
    let on = collect(&tree_with(on_dir.path(), 64, N, FLUSH_EVERY)?)?;

    assert_eq!(off.len(), N, "scan must yield every inserted entry");
    assert!(
        on.iter()
            .any(|(k, _)| k.as_slice() == key_for(0).as_bytes()),
        "sanity: the scan covers the first key",
    );
    assert_eq!(on, off, "read-ahead changed what the scan returned");

    // And the values are the ones written, not merely equal to each other.
    for (i, (key, value)) in on.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
        assert_eq!(value.as_ref(), value_for(i).as_slice());
    }

    Ok(())
}

/// A backend delegating to [`StdFs`] that records, per batched read, how many
/// requests it carried.
struct BatchCountFs(std::sync::Arc<std::sync::Mutex<Vec<usize>>>);

impl Fs for BatchCountFs {
    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(reqs.len());
        StdFs.read_blocks_batched(reqs)
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        StdFs.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        StdFs.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdFs.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        StdFs.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        StdFs.sync_directory(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdFs.exists(path)
    }
}

/// A window whose records lie in several blob files coalesces into a span per
/// file, and the spans of one window go to the backend in ONE batched read,
/// not one read each.
#[test_log::test]
fn a_window_reads_its_spans_in_one_batched_call() -> lsm_tree::Result<()> {
    const N: usize = 256;
    const WINDOW: u16 = 64;
    // 8 records per blob file: a 64-record window spans 8 files, so 8 spans.
    const FLUSH_EVERY: usize = 8;

    let dir = tempfile::tempdir()?;
    let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seqno = SequenceNumberCounter::default();
    let config = || {
        Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default())
            .with_shared_fs(std::sync::Arc::new(BatchCountFs(std::sync::Arc::clone(
                &calls,
            ))))
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .separation_threshold(1)
                    .scan_prefetch(WINDOW),
            ))
    };
    {
        let tree = config().open()?;
        for i in 0..N {
            tree.insert(key_for(i).as_bytes(), value_for(i), seqno.next());
            if (i + 1) % FLUSH_EVERY == 0 {
                tree.flush_active_memtable(0)?;
            }
        }
    }
    // Reopened, so no value is in the cache the writes filled.
    let tree = config().open()?;
    calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();

    let items = collect(&tree)?;
    assert_eq!(items.len(), N);
    for (i, (key, value)) in items.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
        assert_eq!(value.as_ref(), value_for(i).as_slice());
    }

    let calls = calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let windows = N.div_ceil(usize::from(WINDOW));
    assert!(
        calls.len() <= windows + 1,
        "{} batched reads for {windows} windows: {calls:?}",
        calls.len()
    );
    assert!(
        calls.iter().any(|&reqs| reqs >= 8),
        "a window's spans travel together: {calls:?}"
    );
    Ok(())
}

/// A window wider than the scan has entries left: the refill must stop at the
/// end of the iterator rather than over-read, and the tail must still resolve.
#[test_log::test]
fn a_window_wider_than_the_scan_yields_every_entry() -> lsm_tree::Result<()> {
    const N: usize = 5;

    let dir = tempfile::tempdir()?;
    let tree = tree_with(dir.path(), 4_096, N, 0)?;

    let items = collect(&tree)?;
    assert_eq!(items.len(), N);
    for (i, (key, value)) in items.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
        assert_eq!(value.as_ref(), value_for(i).as_slice());
    }

    Ok(())
}

/// Read-ahead arms itself on the first resolved value, so it must not disturb
/// a scan that never resolves one: the keys still come out complete and in
/// order.
#[test_log::test]
fn a_key_only_scan_is_unaffected_by_prefetch() -> lsm_tree::Result<()> {
    const N: usize = 200;

    let dir = tempfile::tempdir()?;
    let tree = tree_with(dir.path(), 64, N, 37)?;

    let keys: Vec<Vec<u8>> = tree
        .iter(SeqNo::MAX, None)
        .map(|guard| guard.key().map(|k| k.to_vec()))
        .collect::<lsm_tree::Result<_>>()?;

    assert_eq!(keys.len(), N);
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
    }

    Ok(())
}

/// Reverse iteration is served straight from the merge pipeline while the
/// read-ahead buffer holds the front of the range, so it must still deliver
/// every entry exactly once, in reverse.
#[test_log::test]
fn a_reverse_scan_yields_every_entry_with_prefetch_on() -> lsm_tree::Result<()> {
    const N: usize = 300;

    let dir = tempfile::tempdir()?;
    let tree = tree_with(dir.path(), 64, N, 43)?;

    let mut items: Vec<(Vec<u8>, UserValue)> = tree
        .iter(SeqNo::MAX, None)
        .rev()
        .map(|guard| {
            let (k, v) = guard.into_inner()?;
            Ok((k.to_vec(), v))
        })
        .collect::<lsm_tree::Result<_>>()?;

    assert_eq!(items.len(), N);
    items.reverse();
    for (i, (key, value)) in items.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
        assert_eq!(value.as_ref(), value_for(i).as_slice());
    }

    Ok(())
}

/// Interleaving both ends of the same scan: the buffer holds items pulled off
/// the front while the back is still coming from the pipeline, and the two must
/// meet in the middle without dropping or repeating an entry.
#[test_log::test]
fn a_double_ended_scan_meets_in_the_middle_exactly_once() -> lsm_tree::Result<()> {
    const N: usize = 250;

    let dir = tempfile::tempdir()?;
    let tree = tree_with(dir.path(), 64, N, 0)?;

    let mut iter = tree.iter(SeqNo::MAX, None);
    let mut front: Vec<Vec<u8>> = Vec::new();
    let mut back: Vec<Vec<u8>> = Vec::new();

    loop {
        match iter.next() {
            // Resolving arms the read-ahead, which is the point: the buffer is
            // non-empty for the rest of the walk.
            Some(guard) => front.push(guard.into_inner()?.0.to_vec()),
            None => break,
        }
        match iter.next_back() {
            Some(guard) => back.push(guard.into_inner()?.0.to_vec()),
            None => break,
        }
    }

    back.reverse();
    front.extend(back);

    assert_eq!(front.len(), N, "an entry was dropped or repeated");
    for (i, key) in front.iter().enumerate() {
        assert_eq!(key.as_slice(), key_for(i).as_bytes());
    }

    Ok(())
}
