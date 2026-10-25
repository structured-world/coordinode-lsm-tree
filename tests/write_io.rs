//! The host-write ledger a tree keeps: every byte it writes charged to the
//! origin that wrote it, beside what it took in from its caller.

#![cfg(feature = "metrics")]

use lsm_tree::compaction::MoveDown;
use lsm_tree::fs::{
    CountingFs, Fault, FaultFs, FaultOp, FaultRule, FileWrites, Fs, MemFs, WriteIoSnapshot,
    WriteOrigin,
};
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

fn open(fs: Arc<dyn Fs>) -> lsm_tree::Result<AnyTree> {
    Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    .open()
}

/// The figures that come from the backend alone, without what a tree charges
/// itself: the installed bytes and the payload taken in.
fn backend_view(reading: &WriteIoSnapshot) -> Vec<(Option<WriteOrigin>, FileWrites)> {
    reading
        .iter()
        .map(|(origin, mut files)| {
            files.installed = 0;
            (origin, files)
        })
        .collect()
}

/// Inserts, overwrites, deletes and a range deletion, flushed and compacted:
/// every byte the backend saw is charged to an origin, none unattributed, the
/// tree's counters agree with a counter wrapped around the backend itself, a
/// flush and a compaction installed exactly what they wrote, and what
/// compaction installed is what its work counter measured.
#[test]
fn every_write_is_charged_to_the_origin_that_made_it() -> lsm_tree::Result<()> {
    let backend = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    let tree = open(backend.clone())?;

    let mut payload = 0u64;
    for round in 0..3u32 {
        for i in 0..200u32 {
            let key = format!("key{i:05}");
            let value = format!("value{round}-{i}").repeat(4);
            payload += (key.len() + value.len()) as u64;
            tree.insert(key, value, u64::from(round * 1_000 + i));
        }
        tree.flush_active_memtable(0)?;
    }
    for i in 0..10u32 {
        let key = format!("key{i:05}");
        payload += key.len() as u64;
        tree.remove(key, 5_000 + u64::from(i));
    }
    tree.remove_range("key00100", "key00150", 6_000);
    payload += 16;
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;

    let reading = tree.write_io();
    assert_eq!(
        reading.unattributed,
        FileWrites::default(),
        "nothing unattributed"
    );
    assert_eq!(
        backend_view(&reading),
        backend_view(&backend.stats().snapshot()),
        "the tree's counters see what its backend saw"
    );

    let flushed = reading.origin(WriteOrigin::FlushTable);
    assert!(flushed.bytes > 0);
    assert_eq!(
        flushed.bytes, flushed.installed,
        "a flush installs what it writes"
    );
    assert_eq!(flushed.write_errors + flushed.sync_errors, 0);
    let compacted = reading.origin(WriteOrigin::CompactionTable);
    assert!(compacted.bytes > 0);
    assert_eq!(compacted.bytes, compacted.installed);
    assert_eq!(
        compacted.installed + reading.origin(WriteOrigin::CompactionBlob).installed,
        tree.metrics().compaction_work_measured().written,
        "the ledger and the compaction work counter measure the same install"
    );
    assert!(reading.origin(WriteOrigin::Manifest).bytes > 0);
    assert!(
        reading.origin(WriteOrigin::Metadata).created > 0,
        "the table file pool"
    );
    assert!(reading.directory.syncs > 0);

    assert_eq!(reading.accepted.values, 600);
    assert_eq!(reading.accepted.deletions, 10);
    assert_eq!(reading.accepted.range_deletions, 1);
    assert_eq!(reading.accepted.payload_bytes, payload);
    assert!(
        reading
            .host_write_amplification()
            .is_some_and(|wa| wa > 1.0)
    );
    Ok(())
}

/// A compaction that gives tables another level writes no table: only the
/// manifest records the move.
#[test]
fn a_move_writes_only_the_manifest() -> lsm_tree::Result<()> {
    let tree = open(Arc::new(MemFs::new()))?;
    for i in 0..100u32 {
        tree.insert(format!("key{i:05}"), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let before = tree.write_io();

    tree.compact(Arc::new(MoveDown(0, 1)), SeqNo::MAX)?;

    let moved = tree.write_io().since(&before).expect("one lifetime");
    assert_eq!(tree.level_table_count(1), Some(1), "moved");
    assert_eq!(
        moved.origin(WriteOrigin::CompactionTable),
        FileWrites::default()
    );
    assert!(moved.origin(WriteOrigin::Manifest).bytes > 0);
    Ok(())
}

/// A blob tree's flush writes its separated values to blob files of its own
/// origin and installs every byte of them.
#[test]
fn a_flush_charges_separated_values_to_its_blob_files() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().separation_threshold(16),
    ))
    .open()?;
    for i in 0..100u32 {
        tree.insert(format!("key{i:05}"), vec![b'v'; 1_000], u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let reading = tree.write_io();
    let blobs = reading.origin(WriteOrigin::FlushBlob);
    assert!(blobs.bytes >= 100_000, "every value lands in a blob file");
    assert_eq!(blobs.bytes, blobs.installed);
    assert_eq!(reading.blob_bytes(), blobs.bytes);
    assert_eq!(reading.unattributed, FileWrites::default());
    Ok(())
}

/// A manifest whose edit log rotates writes a snapshot per rotation, each a
/// new file renamed into place, all charged to the manifest.
#[test]
fn manifest_rotation_writes_snapshots_charged_to_the_manifest() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .manifest_log_rotate_bytes(1)
    .open()?;
    let before = tree.write_io();
    for flush in 0..4u32 {
        tree.insert(format!("key{flush}"), "value", u64::from(flush));
        tree.flush_active_memtable(0)?;
    }

    let rotated = tree.write_io().since(&before).expect("one lifetime");
    let manifest = rotated.origin(WriteOrigin::Manifest);
    assert!(manifest.created >= 4, "a snapshot per flush: {manifest:?}");
    assert!(rotated.directory.renamed >= 4, "each renamed into place");
    Ok(())
}

/// A checkpoint on a backend that links copies no table byte; one on a backend
/// that cannot link copies every byte of them, charged to the checkpoint.
#[test]
fn a_checkpoint_links_where_it_can_and_copies_where_it_cannot() -> lsm_tree::Result<()> {
    let checkpoint = |fs: Arc<dyn Fs>| -> lsm_tree::Result<(WriteIoSnapshot, u64, usize)> {
        let tree = open(fs)?;
        for i in 0..500u32 {
            tree.insert(format!("key{i:05}"), "value".repeat(20), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        let before = tree.write_io();
        let info = tree.create_checkpoint(std::path::Path::new("/checkpoint"))?;
        let delta = tree.write_io().since(&before).expect("one lifetime");
        Ok((delta, info.total_bytes, info.sst_files))
    };

    let (linked, total, tables) = checkpoint(Arc::new(MemFs::new()))?;
    assert_eq!(linked.directory.linked, tables as u64);
    assert!(
        linked.origin(WriteOrigin::Checkpoint).bytes < total,
        "no table copied"
    );

    let unlinkable = FaultFs::new(MemFs::new());
    unlinkable.injector().arm(FaultRule::new(
        FaultOp::HardLink,
        Fault::Error(lsm_tree::io::ErrorKind::Unsupported),
    ));
    let (copied, total, _) = checkpoint(Arc::new(unlinkable))?;
    assert_eq!(copied.directory.linked, 0);
    assert!(
        copied.origin(WriteOrigin::Checkpoint).bytes >= total,
        "every table copied"
    );
    Ok(())
}

/// A table sync that fails fails the flush: the failure is counted, and the
/// bytes the flush wrote are written but never installed.
#[test]
fn a_failed_sync_is_counted_and_its_output_never_installed() -> lsm_tree::Result<()> {
    let faulty = FaultFs::new(MemFs::new());
    let injector = faulty.injector();
    let tree = open(Arc::new(faulty))?;
    for i in 0..100u32 {
        tree.insert(format!("key{i:05}"), "value", u64::from(i));
    }
    for op in [FaultOp::SyncAll, FaultOp::SyncData] {
        injector.arm(
            FaultRule::new(op, Fault::Error(lsm_tree::io::ErrorKind::Other))
                .on_path("tables")
                .once(),
        );
    }

    assert!(tree.flush_active_memtable(0).is_err());

    let flush = tree.write_io().origin(WriteOrigin::FlushTable);
    assert_eq!(flush.sync_errors, 1);
    assert!(flush.bytes > 0);
    assert_eq!(flush.installed, 0, "written and given up");
    Ok(())
}

/// A write the backend takes only part of is counted as a short write, and the
/// bytes of the table once: what was written is what was installed.
#[test]
fn a_short_write_charges_its_bytes_once() -> lsm_tree::Result<()> {
    let faulty = FaultFs::new(MemFs::new());
    faulty.injector().arm(
        FaultRule::new(FaultOp::Write, Fault::ShortWrite(100))
            .on_path("tables")
            .once(),
    );
    let tree = open(Arc::new(faulty))?;
    for i in 0..1_000u32 {
        tree.insert(format!("key{i:05}"), "value".repeat(10), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let flush = tree.write_io().origin(WriteOrigin::FlushTable);
    assert_eq!(flush.short_writes, 1);
    assert_eq!(flush.bytes, flush.installed);
    Ok(())
}

/// A reopened tree counts from zero: no reading carries over, and a
/// difference across the reopen is refused rather than misread.
#[test]
fn a_reopen_starts_the_counters_over() -> lsm_tree::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let tree = open(fs.clone())?;
    tree.insert("key", "value", 0);
    tree.flush_active_memtable(0)?;
    let before = tree.write_io();
    drop(tree);

    let reopened = open(fs)?.write_io();
    assert_eq!(reopened.accepted.payload_bytes, 0);
    assert_eq!(
        reopened.origin(WriteOrigin::FlushTable),
        FileWrites::default()
    );
    assert!(reopened.since(&before).is_none());
    Ok(())
}
