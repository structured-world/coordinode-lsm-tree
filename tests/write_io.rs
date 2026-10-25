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
    // Bytes no codec shrinks, so every value's bytes reach the blob file.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..100u32 {
        let value: Vec<u8> = (0..1_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect();
        tree.insert(format!("key{i:05}"), value, u64::from(i));
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

/// Blob garbage collection: a compaction that relocates the live values of a
/// blob file writes them to blob files of its own origin and installs every
/// byte of them, at least the relocated values.
#[test]
fn relocated_values_are_charged_to_compaction_blob_files() -> lsm_tree::Result<()> {
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_fs(MemFs::new())
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().age_cutoff(1.0),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .open()?;
    let value = b"neptune!".repeat(16_000);
    tree.insert("big", &value, 0);
    tree.insert("big2", &value, 0);
    tree.flush_active_memtable(0)?;
    tree.insert("big", b"winter!".repeat(16_000), 1);
    tree.flush_active_memtable(0)?;
    let before = tree.write_io();

    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;

    let compacted = tree.write_io().since(&before).expect("one lifetime");
    let blobs = compacted.origin(WriteOrigin::CompactionBlob);
    let relocated = tree.metrics().blob_bytes_relocated();
    assert!(relocated > 0, "a value was relocated");
    assert!(blobs.bytes >= relocated);
    assert_eq!(blobs.bytes, blobs.installed);
    assert_eq!(compacted.unattributed, FileWrites::default());
    Ok(())
}

/// A manifest whose edit log rotates writes a snapshot per rotation: two files
/// made, the snapshot and its empty log, and the snapshot renamed into place,
/// all charged to the manifest.
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
    for flush in 0..8u32 {
        tree.insert(format!("key{flush}"), "value", u64::from(flush));
        tree.flush_active_memtable(0)?;
    }

    let rotated = tree.write_io().since(&before).expect("one lifetime");
    let manifest = rotated.origin(WriteOrigin::Manifest);
    let rotations = rotated.directory.renamed;
    assert!(rotations >= 2, "the log rotated: {rotated:?}");
    assert_eq!(
        manifest.created,
        2 * rotations,
        "a snapshot and its log each"
    );
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

/// A reopened tree that has since written more than before still refuses a
/// difference across the reopen, as do two counters that never shared a
/// lifetime but counted the same operations: no counter went down, yet the
/// readings come from different counters.
#[test]
fn readings_of_different_counters_have_no_difference() -> lsm_tree::Result<()> {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    let tree = open(fs.clone())?;
    tree.insert("key", "value", 0);
    tree.flush_active_memtable(0)?;
    let before = tree.write_io();
    drop(tree);

    let reopened = open(fs)?;
    for i in 0..10u64 {
        reopened.insert(format!("key{i}"), "a longer value than before", 1 + i);
        reopened.flush_active_memtable(0)?;
    }
    let after = reopened.write_io();
    assert!(
        after.origin(WriteOrigin::FlushTable).bytes > before.origin(WriteOrigin::FlushTable).bytes,
        "self-check: every counter grew past the earlier reading"
    );
    assert!(
        after.since(&before).is_none(),
        "two lifetimes of the counters"
    );

    let first = CountingFs::new(Arc::new(MemFs::new()));
    let second = CountingFs::new(Arc::new(MemFs::new()));
    for fs in [&first, &second] {
        fs.create_dir_all(std::path::Path::new("/d"))?;
    }
    assert!(
        second
            .stats()
            .snapshot()
            .since(&first.stats().snapshot())
            .is_none(),
        "two counter sets"
    );
    Ok(())
}

/// A row of cells is charged its key and its fields' bytes, as an ingested
/// row is, not the row as the tree encodes it with its field descriptors.
#[test]
fn a_row_of_cells_is_charged_its_key_and_field_bytes() -> lsm_tree::Result<()> {
    use lsm_tree::KvSeparationOptions;
    use lsm_tree::blob_tree::field_row::Field;

    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::new()))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;
    let AnyTree::Blob(blob) = &tree else {
        panic!("expected a key-value separated tree");
    };
    blob.insert_cells(
        "doc",
        &[Field::bytes(4, b"body"), Field::bytes(3, b"draft")],
        0,
    )?;

    let accepted = tree.write_io().accepted;
    assert_eq!(accepted.values, 1);
    assert_eq!(accepted.payload_bytes, (3 + 4 + 5) as u64);
    Ok(())
}

/// An ingestion charges each record it takes in: a value its key and value,
/// a tombstone or weak tombstone its key.
#[test]
fn an_ingestion_charges_each_record_it_takes_in() -> lsm_tree::Result<()> {
    let tree = open(Arc::new(MemFs::new()))?;
    let mut ingestion = tree.ingestion()?;
    ingestion.write("a", "abc")?;
    ingestion.write_tombstone("bb")?;
    ingestion.write_weak_tombstone("ccc")?;
    ingestion.finish()?;

    let accepted = tree.write_io().accepted;
    assert_eq!(accepted.ingested, 3);
    assert_eq!(accepted.payload_bytes, (1 + 3) + 2 + 3);
    Ok(())
}

/// A config cloned from an open tree whose routes are reordered counts each
/// route once, into the new tree's counters only: the first tree's wrappers
/// are taken off whichever route now holds them.
#[test]
fn a_cloned_config_with_reordered_routes_counts_into_its_own_tree_only() -> lsm_tree::Result<()> {
    use lsm_tree::config::LevelRoute;

    let warm: Arc<dyn Fs> = Arc::new(MemFs::new());
    let cold: Arc<dyn Fs> = Arc::new(MemFs::new());
    let first = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::new()))
    .level_routes(vec![
        LevelRoute {
            levels: 1..4,
            path: "/warm".into(),
            fs: warm,
        },
        LevelRoute {
            levels: 4..7,
            path: "/cold".into(),
            fs: cold,
        },
    ])
    .open()?;

    let mut config = first.tree_config().clone();
    if let Some(routes) = &mut config.level_routes {
        routes.reverse();
    }
    let second = config.with_shared_fs(Arc::new(MemFs::new())).open()?;
    for i in 0..100u32 {
        second.insert(format!("key{i:03}"), "value", u64::from(i));
    }
    second.flush_active_memtable(0)?;
    second.major_compact(u64::MAX, 0)?;

    assert!(
        second.write_io().origin(WriteOrigin::CompactionTable).bytes > 0,
        "self-check: the second tree compacted into a route"
    );
    assert_eq!(
        first.write_io().origin(WriteOrigin::CompactionTable),
        FileWrites::default(),
        "the first tree compacted nothing"
    );
    Ok(())
}

/// A config cloned from an open tree and given other backends opens on those
/// backends, the primary one and every route's: the counting wrappers of the
/// first tree are taken off only where they are still in place.
#[test]
fn a_cloned_config_opens_on_the_backends_it_was_given() -> lsm_tree::Result<()> {
    use lsm_tree::config::LevelRoute;

    let route = |fs: Arc<dyn Fs>| LevelRoute {
        levels: 1..7,
        path: "/cold".into(),
        fs,
    };
    let first: Arc<dyn Fs> = Arc::new(MemFs::new());
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(first.clone())
    .level_routes(vec![route(Arc::new(MemFs::new()))])
    .open()?;
    let config = tree.tree_config().clone();
    drop(tree);

    let primary = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    let cold = Arc::new(CountingFs::new(Arc::new(MemFs::new())));
    let tree = config
        .with_shared_fs(primary.clone())
        .level_routes(vec![route(cold.clone())])
        .open()?;
    for i in 0..100u32 {
        tree.insert(format!("key{i:03}"), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    assert!(
        primary.stats().snapshot().files().bytes > 0,
        "the primary backend given"
    );
    assert!(
        cold.stats()
            .snapshot()
            .origin(WriteOrigin::CompactionTable)
            .bytes
            > 0,
        "the route's backend given"
    );
    Ok(())
}
