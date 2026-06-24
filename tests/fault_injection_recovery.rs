//! End-to-end crash-recovery test driving the engine through a torn manifest
//! commit with the fault-injection + crash-simulator backends.
//!
//! `FaultFs` makes the manifest-edit `fsync` of one flush fail; `CrashFs` then
//! discards the edit-log tail that failed `fsync` never made durable, modelling
//! a power loss at that instant. Reopening the engine on the post-crash store
//! must recover to the last durable state: the writes whose manifest commit was
//! acknowledged survive, the one whose commit never became durable is gone, and
//! recovery itself does not error on the orphaned table the failed flush left.

use lsm_tree::fs::{CrashFs, Fault, FaultFs, FaultOp, FaultRule, MemFs};
use lsm_tree::io::ErrorKind;
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn flush_with_torn_manifest_commit_recovers_to_last_durable_state() -> lsm_tree::Result<()> {
    // FaultFs (error injection) over CrashFs (durability tracking) over MemFs.
    // The CrashFs handle is cloned so we keep one to trigger crash()/inner()
    // while the tree owns its own (state- and store-sharing) clone.
    let crash = CrashFs::new(MemFs::new());
    let fault = FaultFs::new(crash.clone());
    let injector = fault.injector();

    let db = "/db";

    {
        let tree = lsm_tree::Config::new(
            db,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::new(fault))
        .open()?;

        // Two flushes whose manifest commits are acknowledged (durable).
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(0)?;

        // Arm a one-shot fault on the next edit-log fsync: the flush of "c"
        // writes and fsyncs its table, then fails to durably commit the manifest
        // edit. CrashFs therefore never records the edit-log's "c" tail as
        // durable.
        injector.arm(
            FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other))
                .on_path("edits")
                .once(),
        );
        tree.insert("c", "3", 2);
        // Assert the failure is the ARMED edit-log fsync, not an earlier error:
        // otherwise the orphaned-table recovery path below would never be
        // exercised.
        match tree.flush_active_memtable(0) {
            Ok(_) => panic!("the injected manifest-commit fault must fail the flush"),
            Err(e) => assert!(
                format!("{e}").contains("injected fault"),
                "flush must fail on the armed edit-log fsync, not an earlier error: {e}"
            ),
        }
        // Drop the tree: release the directory lock and discard in-memory state.
    }

    // Power loss: roll every file back to its last fsync. The edit-log loses its
    // un-synced "c" tail; "c"'s table (fsynced before the manifest edit) survives
    // on disk as an orphan no committed version references.
    crash.crash();

    // Reopen on the raw post-crash store (no fault / crash wrapping).
    let tree = lsm_tree::Config::new(
        db,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(crash.inner())
    .open()?;

    assert!(
        tree.contains_key("a", u64::MAX)?,
        "the first acknowledged write survives the crash"
    );
    assert!(
        tree.contains_key("b", u64::MAX)?,
        "the second acknowledged write survives the crash"
    );
    assert!(
        !tree.contains_key("c", u64::MAX)?,
        "the write whose manifest commit never became durable is absent after recovery"
    );

    Ok(())
}

/// Exhaustive crash-point sweep: for every durability barrier (`sync_all` on a
/// table or the manifest edit log) of a flush workload, fail that one barrier,
/// simulate a power loss, reopen, and assert recovery yields a *consistent
/// prefix* of the inserted keys.
///
/// Each key is inserted and then flushed in order, so the only crash-consistent
/// outcomes are "the first j keys survive, the rest are gone" for some j — never
/// a hole (key i present while key i-1 is missing) and never a failed reopen.
/// The workload halts at the first flush error (an application that stops on a
/// write failure), so a later flush can't durably commit a key past the gap.
#[test]
fn flush_crash_point_sweep_recovers_a_consistent_prefix() -> lsm_tree::Result<()> {
    const KEYS: usize = 4;
    // SST sync + edit-log sync per flush, plus headroom so the tail iterations
    // fail no barrier at all (whole workload commits -> full prefix).
    const SWEEP: usize = 2 * KEYS + 2;

    let key = |i: usize| format!("k{i:02}");

    for fail_after in 0..SWEEP {
        let crash = CrashFs::new(MemFs::new());
        let fault = FaultFs::new(crash.clone());
        let injector = fault.injector();
        let db = "/db";

        {
            let tree = lsm_tree::Config::new(
                db,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(Arc::new(fault))
            .open()?;

            // Arm only AFTER open so the fault counts flush barriers, not the
            // open-time syncs: fail the (fail_after + 1)-th `sync_all`.
            injector.arm(
                FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other))
                    .skip(fail_after as u64)
                    .once(),
            );

            for i in 0..KEYS {
                tree.insert(key(i), "v", i as u64);
                if tree.flush_active_memtable(0).is_err() {
                    // The application stops writing on the first durable-commit
                    // failure; keys past this point are never attempted durably.
                    break;
                }
            }
        }

        crash.crash();

        let tree = lsm_tree::Config::new(
            db,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(crash.inner())
        .open()?;

        // Count the leading run of present keys, then assert the recovered set
        // is EXACTLY that contiguous prefix (no holes, no stragglers).
        let mut prefix = 0;
        while prefix < KEYS && tree.contains_key(key(prefix), u64::MAX)? {
            prefix += 1;
        }
        for i in 0..KEYS {
            assert_eq!(
                tree.contains_key(key(i), u64::MAX)?,
                i < prefix,
                "fail_after={fail_after}: key {i} should be {} (recovered prefix len {prefix})",
                if i < prefix { "present" } else { "absent" },
            );
        }
    }

    Ok(())
}
