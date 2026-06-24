use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, StdFs};
use crate::io::ErrorKind;
use crate::{AbstractTree, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

/// Verifies that a failed `major_compact` leaves no observable compaction
/// state behind, and that the failure is induced at the manifest-commit point
/// rather than the data write.
///
/// The compaction writes its output table fine, then the durable append of the
/// incremental manifest edit (the `fsync` of the edit log) is made to fail via
/// [`FaultFs`]. The version is therefore never committed, so: the under-
/// compaction marker set (`hidden_set`) drains empty, and the externally
/// visible `table_count` matches the pre-compaction snapshot — no half-applied
/// table addition leaks past the failed manifest write.
#[test]
fn level_manifest_atomicity() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    // Wrap the real backend so we can fail a specific I/O operation on demand.
    let fault_fs = FaultFs::new(StdFs);
    let injector = fault_fs.injector();

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fault_fs))
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("a", "a", 1);
    tree.flush_active_memtable(0)?;
    tree.insert("a", "a", 2);
    tree.flush_active_memtable(0)?;

    assert_eq!(3, tree.approximate_len());

    tree.major_compact(u64::MAX, 3)?;

    assert_eq!(1, tree.table_count());

    tree.insert("a", "a", 3);
    tree.flush_active_memtable(0)?;

    let table_count_before_major_compact = tree.table_count();

    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!();
    };

    // Arm the fault only now, so all setup flushes / the first compaction
    // commit succeed: the NEXT durable manifest-edit `fsync` (a `sync_all` on
    // the `edits-*` log) fails, forcing the second compaction's version commit
    // to error after the output table has already been written.
    injector.arm(
        FaultRule::new(FaultOp::SyncAll, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );

    assert!(tree.major_compact(u64::MAX, 4).is_err());

    assert!(tree.compaction_state.lock().hidden_set().is_empty());

    assert_eq!(table_count_before_major_compact, tree.table_count());

    // The pre-compaction data is still readable: the failed commit rolled back
    // cleanly rather than leaving the tree pointing at a half-written version.
    // `u64::MAX` snapshot sees every committed seqno.
    assert!(tree.contains_key("a", u64::MAX)?);

    Ok(())
}
