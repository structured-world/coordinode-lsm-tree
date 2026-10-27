use crate::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter, Tree,
    tree::inner::TestHooks,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

/// Long enough for a tree that is not closed to show it.
const SETTLE: Duration = Duration::from_millis(200);

fn config(folder: &Path) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

fn open_standard(folder: &Path) -> crate::Result<Tree> {
    match config(folder).open()? {
        AnyTree::Standard(tree) => Ok(tree),
        AnyTree::Blob(_) => unreachable!("no key-value separation configured"),
    }
}

fn open_blob(folder: &Path) -> crate::Result<crate::BlobTree> {
    match config(folder)
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?
    {
        AnyTree::Blob(tree) => Ok(tree),
        AnyTree::Standard(_) => unreachable!("key-value separation configured"),
    }
}

/// Every file under `folder`, with its length.
fn files(folder: &Path) -> BTreeSet<(PathBuf, u64)> {
    let mut found = BTreeSet::new();
    let mut pending = vec![folder.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                pending.push(entry.path());
            } else {
                found.insert((entry.path(), meta.len()));
            }
        }
    }
    found
}

/// Writes `n` flushed tables of a few keys each, every write a key of its
/// own so a later rewrite of the same keys makes them obsolete.
fn flush_tables(tree: &impl AbstractTree, n: u64, seqno: &mut SeqNo) -> crate::Result<()> {
    for table in 0..n {
        for key in 0..4u64 {
            tree.insert(format!("k{table}-{key}"), "value", *seqno);
            *seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

// The race a caller deleting the directory runs: flushes and a compaction
// leave the table file pool refilling off the claiming thread, and a strong
// clone on another thread outlives the caller's own handle. A `close` +
// `wait` must cover both, so the removal never finds a file made after it.
#[test]
fn close_waits_for_a_refill_and_a_clone_on_another_thread() -> crate::Result<()> {
    for _ in 0..100 {
        let folder = crate::get_tmp_folder();
        let AnyTree::Standard(tree) = config(folder.path()).table_file_pool(4).open()? else {
            unreachable!("no key-value separation configured");
        };
        let mut seqno = 0;
        // Each flush claims a pooled file: the third leaves less than half a
        // batch and starts a refill.
        flush_tables(&tree, 3, &mut seqno)?;
        tree.major_compact(u64::MAX, seqno)?;

        let (go, held) = mpsc::channel::<()>();
        let clone = tree.clone();
        let holder = std::thread::spawn(move || {
            held.recv().unwrap();
            drop(clone);
        });

        let closed = tree.close();
        assert!(!closed.is_closed(), "the clone keeps the tree open");
        go.send(()).unwrap();
        closed.wait();
        holder.join().unwrap();

        std::fs::remove_dir_all(folder.path())?;
    }
    Ok(())
}

// The signal names the moment the last owner went, on the thread that
// dropped it, exactly once; a callback registered after it runs at once.
#[test]
fn on_closed_runs_once_on_the_thread_that_drops_the_last_handle() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let mut seqno = 0;
    flush_tables(&tree, 2, &mut seqno)?;

    let calls = Arc::new(AtomicUsize::new(0));
    let (ran_on, closed_on) = mpsc::channel();
    let (go, held) = mpsc::channel::<()>();
    let clone = tree.clone();
    let holder = std::thread::spawn(move || {
        held.recv().unwrap();
        drop(clone);
        std::thread::current().id()
    });

    let closed = tree.close();
    {
        let calls = Arc::clone(&calls);
        closed.on_closed(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            ran_on.send(std::thread::current().id()).unwrap();
        });
    }
    std::thread::sleep(SETTLE);
    assert_eq!(calls.load(Ordering::SeqCst), 0, "a clone is still alive");

    go.send(()).unwrap();
    let dropper = holder.join().unwrap();
    assert_eq!(closed_on.recv_timeout(SETTLE).unwrap(), dropper);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(closed.is_closed());

    // Registered after closing: runs at once, here.
    let (late, late_on) = mpsc::channel();
    closed.on_closed(move || late.send(std::thread::current().id()).unwrap());
    assert_eq!(late_on.try_recv().unwrap(), std::thread::current().id());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the first callback ran once"
    );
    Ok(())
}

// The last drop runs the callbacks inside a drop of the tree's own parts: a
// callback that panics must neither unwind through that drop nor keep the
// callbacks after it from running.
#[test]
fn a_panicking_callback_neither_escapes_the_drop_nor_skips_the_others() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let clone = tree.clone();
    let closed = tree.close();
    closed.on_closed(|| panic!("a callback that fails"));
    let ran = Arc::new(AtomicUsize::new(0));
    {
        let ran = Arc::clone(&ran);
        closed.on_closed(move || {
            ran.fetch_add(1, Ordering::SeqCst);
        });
    }

    let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(clone)));
    assert!(dropped.is_ok(), "the panic escaped the drop");
    assert_eq!(ran.load(Ordering::SeqCst), 1, "the second callback ran");
    assert!(closed.is_closed());
    Ok(())
}

// A callback registered once the tree is closed runs at once, on the caller's
// thread: its panic is contained the same way, whichever side of the closing
// it was registered on.
#[test]
fn a_panicking_callback_registered_after_closing_is_contained() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let closed = tree.close();
    assert!(closed.wait_timeout(SETTLE));

    let registered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        closed.on_closed(|| panic!("a callback that fails"));
    }));
    assert!(registered.is_ok(), "the panic escaped on_closed");
    Ok(())
}

// A weak handle is what a monitor holds: it must not keep the tree open, and
// gives nothing back once the tree is closed.
#[test]
fn a_weak_handle_does_not_keep_the_tree_open() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let weak = tree.downgrade();
    assert!(weak.upgrade().is_some(), "the tree is open");

    let closed = tree.close();
    assert!(closed.wait_timeout(SETTLE), "only a weak handle is left");
    assert!(weak.upgrade().is_none());
    std::fs::remove_dir_all(folder.path())?;
    Ok(())
}

// A strong clone keeps working after `close` on another handle, but no weak
// handle raises the tree again in between, and the tree closes when the
// clone drops.
#[test]
fn close_stops_upgrades_while_a_clone_finishes() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let weak = tree.downgrade();
    let clone = tree.clone();

    let closed = tree.close();
    assert!(weak.upgrade().is_none(), "closing: no new strong handle");
    clone.insert("a", "1", 0);
    clone.flush_active_memtable(0)?;
    assert!(!closed.wait_timeout(SETTLE), "the clone keeps it open");

    drop(clone);
    assert!(closed.wait_timeout(SETTLE));
    Ok(())
}

// `upgrade` reads the flag clear before `close` sets it: the handle it took
// counts, and the closing waits for it.
#[test]
fn an_upgrade_before_close_keeps_the_tree_open_until_it_drops() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let weak = tree.downgrade();
    let reached = Arc::new(AtomicUsize::new(0));
    {
        let reached = Arc::clone(&reached);
        TestHooks::install(
            &tree.test_hooks.upgrade,
            Box::new(move || {
                reached.fetch_add(1, Ordering::SeqCst);
            }),
        );
    }

    let raised = weak.upgrade().expect("read the flag clear");
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    let closed = tree.close();
    assert!(
        !closed.wait_timeout(SETTLE),
        "the raised handle keeps it open"
    );

    drop(raised);
    assert!(closed.wait_timeout(SETTLE));
    Ok(())
}

// `upgrade` took its strong handle but `close` set the flag before it read
// it: it gives the handle back, and the closing does not wait for it.
#[test]
fn an_upgrade_that_reads_the_flag_set_gives_its_handle_back() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let weak = tree.downgrade();
    let (at_hook, reached) = mpsc::channel::<()>();
    let (release, go) = mpsc::channel::<()>();
    TestHooks::install(
        &tree.test_hooks.upgrade,
        Box::new(move || {
            at_hook.send(()).unwrap();
            go.recv().unwrap();
        }),
    );

    let upgrading = std::thread::spawn(move || weak.upgrade().is_some());
    reached.recv().unwrap();
    // The upgrader holds a strong handle now, and has not read the flag.
    let closed = tree.close();
    assert!(!closed.is_closed(), "the upgrader's handle is still held");
    release.send(()).unwrap();

    assert!(!upgrading.join().unwrap(), "read the flag set");
    assert!(closed.wait_timeout(SETTLE), "the handle was given back");
    Ok(())
}

// An iterator holds the tables it reads without a tree handle: it keeps the
// tree open, and a table compacted away meanwhile is removed only after it.
#[test]
fn an_iterator_outliving_the_tree_keeps_it_open() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let mut seqno = 0;
    flush_tables(&tree, 1, &mut seqno)?;
    let table = tree.current_version().iter_tables().next().unwrap().clone();
    let path = (*table.path).clone();
    drop(table);

    let mut iter = tree.iter(seqno, None);
    assert!(iter.next().is_some());
    flush_tables(&tree, 1, &mut seqno)?;
    tree.major_compact(u64::MAX, seqno)?;

    let closed = tree.close();
    assert!(!closed.wait_timeout(SETTLE), "the iterator keeps it open");
    assert!(path.exists(), "the iterator still reads the table");

    drop(iter);
    assert!(closed.wait_timeout(SETTLE));
    assert!(!path.exists(), "removed once nothing reads it");
    Ok(())
}

// A snapshot of the version holds its tables the same way.
#[test]
fn a_snapshot_outliving_the_tree_keeps_it_open() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let mut seqno = 0;
    flush_tables(&tree, 1, &mut seqno)?;
    let snapshot = tree.current_version();
    let path = (*snapshot.iter_tables().next().unwrap().path).clone();
    flush_tables(&tree, 1, &mut seqno)?;
    tree.major_compact(u64::MAX, seqno)?;

    let closed = tree.close();
    assert!(!closed.wait_timeout(SETTLE), "the snapshot keeps it open");
    assert!(path.exists(), "the snapshot still holds the table");

    drop(snapshot);
    assert!(closed.wait_timeout(SETTLE));
    assert!(!path.exists(), "removed once nothing holds it");
    Ok(())
}

// A pin holds its files and the directory lock after the tree drops: it keeps
// the tree open, and by the signal the lock is released, so the folder opens
// again at once.
#[test]
fn a_pin_outliving_the_tree_keeps_it_open() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let mut seqno = 0;
    flush_tables(&tree, 1, &mut seqno)?;
    let pin = tree.pin();
    let path = pin.tables().first().unwrap().path().to_path_buf();
    flush_tables(&tree, 1, &mut seqno)?;
    tree.major_compact(u64::MAX, seqno)?;

    let closed = tree.close();
    assert!(!closed.wait_timeout(SETTLE), "the pin keeps it open");
    assert!(path.exists(), "the pin still lists the table");

    drop(pin);
    assert!(closed.wait_timeout(SETTLE));
    assert!(!path.exists(), "removed once the pin let it go");
    let reopened = open_standard(folder.path())?;
    reopened.close().wait();
    Ok(())
}

// Closed means done: neither the pool nor the deleter touches the folder
// after the signal.
#[test]
fn nothing_changes_in_the_folder_after_the_tree_is_closed() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let AnyTree::Standard(tree) = config(folder.path()).table_file_pool(4).open()? else {
        unreachable!("no key-value separation configured");
    };
    let mut seqno = 0;
    flush_tables(&tree, 3, &mut seqno)?;
    tree.major_compact(u64::MAX, seqno)?;

    tree.close().wait();
    let at_close = files(folder.path());
    std::thread::sleep(SETTLE);
    assert_eq!(files(folder.path()), at_close);
    Ok(())
}

// A key-value-separated tree closes the same way, its blob folder included,
// and a blob file an iterator holds keeps it open.
#[test]
fn a_blob_tree_closes_once_its_blob_files_are_released() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_blob(folder.path())?;
    let mut seqno = 0;
    flush_tables(&tree, 2, &mut seqno)?;
    assert!(tree.blob_file_count() > 0, "the values went to blob files");
    let weak = tree.downgrade();
    assert!(matches!(weak.upgrade(), Some(AnyTree::Blob(_))));

    let mut iter = tree.iter(seqno, None);
    assert!(iter.next().is_some());
    let closed = tree.close();
    assert!(weak.upgrade().is_none());
    assert!(!closed.wait_timeout(SETTLE), "the iterator keeps it open");

    drop(iter);
    assert!(closed.wait_timeout(SETTLE));
    let at_close = files(folder.path());
    std::thread::sleep(SETTLE);
    assert_eq!(files(folder.path()), at_close);
    std::fs::remove_dir_all(folder.path())?;
    Ok(())
}

// Through `AnyTree`, as most callers hold a tree, of either kind: the weak
// handle raises the kind it was made from, and both handles describe their
// state when printed.
fn any_tree_downgrades_and_closes(blob: bool) -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let config = config(folder.path());
    let config = if blob {
        config.with_kv_separation(Some(KvSeparationOptions::default()))
    } else {
        config
    };
    let tree = config.open()?;
    let weak = tree.downgrade();
    assert_eq!(
        weak.upgrade().map(|tree| matches!(tree, AnyTree::Blob(_))),
        Some(blob)
    );
    assert_eq!(
        format!("{weak:?}"),
        format!("WeakTree {{ blob: {blob}, .. }}")
    );

    let closed = tree.close();
    assert!(closed.wait_timeout(SETTLE));
    assert!(weak.upgrade().is_none());
    assert_eq!(format!("{closed:?}"), "TreeClose { closed: true }");
    Ok(())
}

#[test]
fn any_standard_tree_downgrades_and_closes() -> crate::Result<()> {
    any_tree_downgrades_and_closes(false)
}

#[test]
fn any_blob_tree_downgrades_and_closes() -> crate::Result<()> {
    any_tree_downgrades_and_closes(true)
}

// The signal prints whether the tree is closed yet.
#[test]
fn an_open_tree_prints_as_not_closed() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let tree = open_standard(folder.path())?;
    let clone = tree.clone();
    let closed = tree.close();
    assert_eq!(format!("{closed:?}"), "TreeClose { closed: false }");
    drop(clone);
    assert!(closed.wait_timeout(SETTLE));
    Ok(())
}
