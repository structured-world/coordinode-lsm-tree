//! The host-write ledger: every byte trees write through a counting backend
//! charged to the origin that wrote it, in the counter set of the medium the
//! backend stands for.

use lsm_tree::compaction::MoveDown;
use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{
    CountingFs, Fault, FaultFs, FaultOp, FaultRule, FileWrites, Fs, MemFs, WriteIoSnapshot,
    WriteIoStats, WriteOrigin,
};
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

fn config(path: &str, fs: Arc<dyn Fs>) -> Config {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
}

fn open(fs: Arc<dyn Fs>) -> lsm_tree::Result<AnyTree> {
    config("/db", fs).open()
}

/// A counting backend over `fs`, and the set it counts into.
fn counted(fs: impl Fs + 'static) -> (Arc<dyn Fs>, Arc<WriteIoStats>) {
    let counting = CountingFs::new(Arc::new(fs));
    let stats = Arc::clone(counting.stats());
    (Arc::new(counting), stats)
}

fn fill(tree: &AnyTree, keys: u32, seqno: SeqNo) {
    for i in 0..keys {
        tree.insert(
            format!("key{i:05}"),
            "value".repeat(4),
            seqno + u64::from(i),
        );
    }
}

/// Inserts, overwrites, deletes and a range deletion, flushed and compacted:
/// every byte is charged to an origin, none unattributed, a flush and a
/// compaction installed exactly what they wrote, and what compaction
/// installed is what its work counter measured.
#[test]
fn every_write_is_charged_to_the_origin_that_made_it() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = open(fs)?;

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

    let reading = stats.snapshot();
    assert_eq!(
        reading.unattributed,
        FileWrites::default(),
        "nothing unattributed"
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
    assert!(reading.origin(WriteOrigin::Manifest).bytes > 0);
    assert!(
        reading.origin(WriteOrigin::Metadata).created > 0,
        "the table file pool"
    );
    assert!(reading.directory.syncs > 0);

    #[cfg(feature = "metrics")]
    {
        let metrics = tree.metrics();
        assert_eq!(
            compacted.installed + reading.origin(WriteOrigin::CompactionBlob).installed,
            metrics.compaction_work_measured().written,
            "the ledger and the compaction work counter measure the same install"
        );
        let accepted = metrics.accepted_writes();
        assert_eq!(accepted.values, 600);
        assert_eq!(accepted.deletions, 10);
        assert_eq!(accepted.range_deletions, 1);
        assert_eq!(accepted.payload_bytes, payload);
        assert!(reading.files().bytes > accepted.payload_bytes);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = payload;
    Ok(())
}

/// A tree given a backend that counts nothing has no counter set to find;
/// one given a counting backend under another wrapper still finds its set.
#[test]
fn a_tree_counts_only_through_a_counting_backend() -> lsm_tree::Result<()> {
    let tree = open(Arc::new(MemFs::new()))?;
    fill(&tree, 100, 0);
    tree.flush_active_memtable(0)?;
    assert!(tree.tree_config().fs.write_io_stats().is_none());

    let inner = CountingFs::new(Arc::new(MemFs::new()));
    let stats = Arc::clone(inner.stats());
    let faulty = FaultFs::new(inner);
    assert!(
        faulty
            .write_io_stats()
            .is_some_and(|found| Arc::ptr_eq(found, &stats))
    );
    let tree = open(Arc::new(faulty))?;
    fill(&tree, 100, 0);
    tree.flush_active_memtable(0)?;
    let flushed = stats.snapshot().origin(WriteOrigin::FlushTable);
    assert!(flushed.bytes > 0);
    assert_eq!(
        flushed.bytes, flushed.installed,
        "installed through the wrapper"
    );
    Ok(())
}

/// Two trees on one medium count into one set: each byte of either once.
#[test]
fn trees_on_one_medium_share_its_set() -> lsm_tree::Result<()> {
    let stats = Arc::new(WriteIoStats::default());
    let medium: Arc<dyn Fs> = Arc::new(MemFs::new());
    let on_medium = || -> Arc<dyn Fs> {
        Arc::new(CountingFs::with_stats(
            Arc::clone(&medium),
            Arc::clone(&stats),
        ))
    };
    let first = config("/a", on_medium()).open()?;
    let second = config("/b", on_medium()).open()?;

    fill(&first, 100, 0);
    first.flush_active_memtable(0)?;
    let one = stats.snapshot().origin(WriteOrigin::FlushTable);
    fill(&second, 100, 0);
    second.flush_active_memtable(0)?;
    let both = stats.snapshot().origin(WriteOrigin::FlushTable);

    assert_eq!(one.created, 1);
    assert_eq!(both.created, 2, "a table from each tree");
    assert_eq!(both.bytes, 2 * one.bytes, "the same data written twice");
    assert_eq!(both.bytes, both.installed);
    Ok(())
}

/// A route on another medium counts into that medium's set: what compaction
/// writes and installs there is charged there, and nothing of it to the set
/// of the primary medium.
#[test]
fn a_route_on_another_medium_counts_into_its_set() -> lsm_tree::Result<()> {
    let (hot, hot_stats) = counted(MemFs::new());
    let (cold, cold_stats) = counted(MemFs::new());
    let tree = config("/db", hot)
        .level_routes(vec![LevelRoute {
            levels: 1..7,
            path: "/cold".into(),
            fs: cold,
        }])
        .open()?;
    fill(&tree, 200, 0);
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    let hot = hot_stats.snapshot();
    let cold = cold_stats.snapshot();
    assert!(hot.origin(WriteOrigin::FlushTable).bytes > 0, "L0 is hot");
    assert_eq!(
        hot.origin(WriteOrigin::CompactionTable),
        FileWrites::default(),
        "no compaction output on the hot medium"
    );
    let compacted = cold.origin(WriteOrigin::CompactionTable);
    assert!(compacted.bytes > 0);
    assert_eq!(compacted.bytes, compacted.installed);
    assert_eq!(cold.origin(WriteOrigin::FlushTable), FileWrites::default());
    assert_eq!(cold.unattributed, FileWrites::default());
    Ok(())
}

/// A tree reopened on the same counting backend, from a clone of the config
/// it was opened with, keeps counting into the same set: nothing restarts,
/// and no file is counted twice.
#[test]
fn a_reopen_from_a_cloned_config_keeps_counting_once() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = open(fs)?;
    fill(&tree, 100, 0);
    tree.flush_active_memtable(0)?;
    let config = tree.tree_config().clone();
    let before = stats.snapshot();
    drop(tree);

    let tree = config.open()?;
    fill(&tree, 100, 1_000);
    tree.flush_active_memtable(0)?;

    let after = stats.snapshot();
    let delta = after
        .since(&before)
        .expect("one counter set across the reopen");
    let flushed = delta.origin(WriteOrigin::FlushTable);
    assert_eq!(flushed.created, 1);
    assert_eq!(
        flushed.bytes, flushed.installed,
        "each byte counted once, not once per wrapper"
    );
    assert_eq!(
        flushed.bytes,
        before.origin(WriteOrigin::FlushTable).bytes,
        "the same rows at other seqnos: the same table size"
    );
    Ok(())
}

/// A compaction that gives tables another level writes no table: only the
/// manifest records the move.
#[test]
fn a_move_writes_only_the_manifest() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = open(fs)?;
    for i in 0..100u32 {
        tree.insert(format!("key{i:05}"), "value", u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    let before = stats.snapshot();

    tree.compact(Arc::new(MoveDown(0, 1)), SeqNo::MAX)?;

    let moved = stats.snapshot().since(&before).expect("one counter set");
    assert_eq!(tree.level_table_count(1), Some(1), "moved");
    assert_eq!(
        moved.origin(WriteOrigin::CompactionTable),
        FileWrites::default()
    );
    assert!(moved.origin(WriteOrigin::Manifest).bytes > 0);
    Ok(())
}

/// Bytes no codec shrinks.
fn noise(len: usize, state: &mut u64) -> Vec<u8> {
    (0..len)
        .map(|_| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A blob tree's flush writes its separated values to blob files of its own
/// origin and installs every byte of them.
#[test]
fn a_flush_charges_separated_values_to_its_blob_files() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = config("/db", fs)
        .with_kv_separation(Some(
            lsm_tree::KvSeparationOptions::default().separation_threshold(16),
        ))
        .open()?;
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for i in 0..100u32 {
        tree.insert(format!("key{i:05}"), noise(1_000, &mut state), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let reading = stats.snapshot();
    let blobs = reading.origin(WriteOrigin::FlushBlob);
    assert!(blobs.bytes >= 100_000, "every value lands in a blob file");
    assert_eq!(blobs.bytes, blobs.installed);
    assert_eq!(reading.blob_bytes(), blobs.bytes);
    assert_eq!(reading.unattributed, FileWrites::default());
    Ok(())
}

/// Blob garbage collection: a compaction that relocates the live values of a
/// blob file writes them to blob files of its own origin and installs every
/// byte of them.
#[test]
fn relocated_values_are_charged_to_compaction_blob_files() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = config("/db", fs)
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
    let before = stats.snapshot();

    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;

    let compacted = stats.snapshot().since(&before).expect("one counter set");
    let blobs = compacted.origin(WriteOrigin::CompactionBlob);
    assert!(
        blobs.bytes >= value.len() as u64,
        "the live value was relocated"
    );
    assert_eq!(blobs.bytes, blobs.installed);
    #[cfg(feature = "metrics")]
    assert!(blobs.bytes >= tree.metrics().blob_bytes_relocated());
    assert_eq!(compacted.unattributed, FileWrites::default());
    Ok(())
}

/// A manifest whose edit log rotates writes a snapshot per rotation: two files
/// made, the snapshot and its empty log, and the snapshot renamed into place,
/// all charged to the manifest.
#[test]
fn manifest_rotation_writes_snapshots_charged_to_the_manifest() -> lsm_tree::Result<()> {
    let (fs, stats) = counted(MemFs::new());
    let tree = config("/db", fs).manifest_log_rotate_bytes(1).open()?;
    let before = stats.snapshot();
    for flush in 0..8u32 {
        tree.insert(format!("key{flush}"), "value", u64::from(flush));
        tree.flush_active_memtable(0)?;
    }

    let rotated = stats.snapshot().since(&before).expect("one counter set");
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
    let checkpoint = |fs: Arc<dyn Fs>,
                      stats: &WriteIoStats|
     -> lsm_tree::Result<(WriteIoSnapshot, u64, usize)> {
        let tree = open(fs)?;
        for i in 0..500u32 {
            tree.insert(format!("key{i:05}"), "value".repeat(20), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        let before = stats.snapshot();
        let info = tree.create_checkpoint(std::path::Path::new("/checkpoint"))?;
        let delta = stats.snapshot().since(&before).expect("one counter set");
        Ok((delta, info.total_bytes, info.sst_files))
    };

    let (fs, stats) = counted(MemFs::new());
    let (linked, total, tables) = checkpoint(fs, &stats)?;
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
    let (fs, stats) = counted(unlinkable);
    let (copied, total, _) = checkpoint(fs, &stats)?;
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
    let (fs, stats) = counted(faulty);
    let tree = open(fs)?;
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

    let flush = stats.snapshot().origin(WriteOrigin::FlushTable);
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
    let (fs, stats) = counted(faulty);
    let tree = open(fs)?;
    for i in 0..1_000u32 {
        tree.insert(format!("key{i:05}"), "value".repeat(10), u64::from(i));
    }
    tree.flush_active_memtable(0)?;

    let flush = stats.snapshot().origin(WriteOrigin::FlushTable);
    assert_eq!(flush.short_writes, 1);
    assert_eq!(flush.bytes, flush.installed);
    Ok(())
}

/// Two counter sets that counted the same operations have no difference: no
/// counter went down, yet the readings come from different sets.
#[test]
fn readings_of_different_counter_sets_have_no_difference() -> lsm_tree::Result<()> {
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

/// A row of cells is taken in as its key and its fields' bytes, as an
/// ingested row is, not the row as the tree encodes it.
#[cfg(feature = "metrics")]
#[test]
fn a_row_of_cells_is_taken_in_as_its_key_and_field_bytes() -> lsm_tree::Result<()> {
    use lsm_tree::KvSeparationOptions;
    use lsm_tree::blob_tree::field_row::Field;

    let tree = config("/db", Arc::new(MemFs::new()))
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

    let accepted = tree.metrics().accepted_writes();
    assert_eq!(accepted.values, 1);
    assert_eq!(accepted.payload_bytes, (3 + 4 + 5) as u64);
    Ok(())
}

/// An ingestion takes in each record: a value its key and value, a tombstone
/// or weak tombstone its key.
#[cfg(feature = "metrics")]
#[test]
fn an_ingestion_takes_in_each_record() -> lsm_tree::Result<()> {
    let tree = open(Arc::new(MemFs::new()))?;
    let mut ingestion = tree.ingestion()?;
    ingestion.write("a", "abc")?;
    ingestion.write_tombstone("bb")?;
    ingestion.write_weak_tombstone("ccc")?;
    ingestion.finish()?;

    let accepted = tree.metrics().accepted_writes();
    assert_eq!(accepted.ingested, 3);
    assert_eq!(accepted.payload_bytes, (1 + 3) + 2 + 3);
    Ok(())
}
