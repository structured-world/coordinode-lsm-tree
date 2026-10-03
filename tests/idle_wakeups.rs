// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! An idle tree leaves its threads asleep: every thread the crate starts
//! waits on an event, so a timer that wakes one periodically shows up here as
//! context switches while nothing happens.

#![cfg(all(feature = "std", target_os = "linux"))]

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

/// Voluntary context switches of the whole process so far: each is a sleep
/// some thread went into.
fn process_wakeups() -> i64 {
    let mut usage = core::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the struct it is handed and nothing else.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage");
    // SAFETY: the successful call above filled it.
    unsafe { usage.assume_init() }.ru_nvcsw
}

/// A tree with every background thread the crate starts (the background
/// deleter, the compaction pool, the io_uring ring thread where the backend
/// is built in) wakes a handful of times at most while it sits idle for three
/// seconds: one periodic tick of 10 Hz would be thirty.
#[test]
fn an_idle_tree_leaves_its_threads_asleep() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .compaction_rate_limit(1_000_000);
    #[cfg(feature = "io-uring")]
    let config = if lsm_tree::fs::is_io_uring_available() {
        config.with_shared_fs(std::sync::Arc::new(lsm_tree::fs::IoUringFs::new()?))
    } else {
        config
    };
    let tree = config.open()?;

    // Start the threads that only exist once there is work: a flush, a
    // compaction over several tables, and a deletion of their inputs.
    for round in 0..3u64 {
        for i in 0..1_000u64 {
            tree.insert(
                format!("key_{i:06}"),
                format!("value_{round}_{i}"),
                round * 1_000 + i,
            );
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(64 * 1_024, 10_000)?;

    let before = process_wakeups();
    std::thread::sleep(std::time::Duration::from_secs(3));
    let woke = process_wakeups() - before;
    assert!(
        woke <= 10,
        "an idle tree woke {woke} times in three seconds"
    );
    drop(tree);
    Ok(())
}
