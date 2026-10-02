// Repair opens every table of the tree it rebuilds. It has to do that within
// the same descriptor limit an open of that tree needs, or a tree that opens
// fine cannot be repaired.

#![cfg(unix)]

mod common;

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};

/// Twice the descriptors the process may hold during the repair.
const TABLES: usize = 1_000;

/// Above the default descriptor cache plus what the process keeps open on its
/// own, well below one descriptor per table.
const DESCRIPTOR_LIMIT: u64 = 512;

/// The descriptor limit is the process's, so the tests that lower it take
/// turns: under a harness that runs them on threads of one process, each
/// would otherwise repair under the other's limit.
static LIMIT_TURN: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A test's turn at the process's descriptor limit, taken before it builds
/// its tree; the soft limit it found is restored when the turn ends.
struct DescriptorLimit {
    soft: u64,
    hard: u64,
    _turn: std::sync::MutexGuard<'static, ()>,
}

impl DescriptorLimit {
    fn take_turn() -> std::io::Result<Self> {
        let turn = LIMIT_TURN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (soft, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
        Ok(Self {
            soft,
            hard,
            _turn: turn,
        })
    }

    fn lower_to(&self, limit: u64) -> std::io::Result<()> {
        rlimit::setrlimit(rlimit::Resource::NOFILE, limit, self.hard)
    }
}

impl Drop for DescriptorLimit {
    fn drop(&mut self) {
        // Best effort on the way out: a limit left lowered only narrows what
        // later tests of this process may open, and they fail loudly if so.
        if let Err(e) = rlimit::setrlimit(rlimit::Resource::NOFILE, self.soft, self.hard) {
            eprintln!("restoring the descriptor limit failed: {e}");
        }
    }
}

/// A one-byte table target ends a table per data block, and a value as large
/// as a block ends a block per key, so one compaction leaves a table per key.
fn fill_one_table_per_key(tree: &impl AbstractTree) -> lsm_tree::Result<Vec<u8>> {
    let value = vec![7u8; 4096];
    for i in 0..TABLES {
        let key = format!("k{i:05}");
        let seqno = u64::try_from(i).expect("a table index fits u64") + 1;
        tree.insert(key.as_bytes(), &value, seqno);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(1, 0)?;
    assert_eq!(tree.table_count(), TABLES, "every key became a table");
    Ok(value)
}

#[test]
fn repair_of_more_tables_than_open_descriptors_recovers_every_table() -> lsm_tree::Result<()> {
    let limit = DescriptorLimit::take_turn()?;
    let dir = tempfile::tempdir()?;
    let config = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    };

    let value = fill_one_table_per_key(&config().open()?)?;
    common::nuke_manifest(dir.path())?;

    limit.lower_to(DESCRIPTOR_LIMIT)?;
    let report = config().repair()?;
    assert_eq!(
        report.recovered,
        TABLES,
        "every intact table is recovered: {:?}",
        report.unreadable_files.first(),
    );

    let tree = config().open()?;
    assert_eq!(tree.table_count(), TABLES);
    for i in 0..TABLES {
        let key = format!("k{i:05}");
        assert_eq!(
            tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(value.as_slice()),
            "{key} must read back after repair",
        );
    }
    Ok(())
}

/// A descriptor cache shared with an open tree is one budget: the repair of
/// another tree on it opens its tables within that cache, not beside it, so it
/// runs under a limit the open tree's descriptors and one cache's worth fit in.
#[test]
fn repair_on_a_shared_descriptor_cache_stays_within_its_budget() -> lsm_tree::Result<()> {
    const CACHE: usize = 128;
    const LIMIT: u64 = 200;
    let limit = DescriptorLimit::take_turn()?;
    let cache = std::sync::Arc::new(lsm_tree::DescriptorTable::new(CACHE));
    let config = |dir: &std::path::Path| {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_descriptor_table(Some(std::sync::Arc::clone(&cache)))
    };

    // An open tree whose tables fill the shared cache.
    let open_dir = tempfile::tempdir()?;
    let open_tree = config(open_dir.path()).open()?;
    for table in 0..(2 * CACHE as u64) {
        open_tree.insert(format!("t{table:04}"), b"v", table);
        open_tree.flush_active_memtable(0)?;
    }
    for table in 0..(2 * CACHE as u64) {
        assert!(
            open_tree
                .get(format!("t{table:04}"), lsm_tree::MAX_SEQNO)?
                .is_some()
        );
    }

    let repair_dir = tempfile::tempdir()?;
    let value = fill_one_table_per_key(&config(repair_dir.path()).open()?)?;
    common::nuke_manifest(repair_dir.path())?;

    limit.lower_to(LIMIT)?;
    let report = config(repair_dir.path()).repair()?;
    assert_eq!(report.recovered, TABLES);
    let repaired = config(repair_dir.path()).open()?;
    assert_eq!(
        repaired.get(b"k00007", lsm_tree::MAX_SEQNO)?.as_deref(),
        Some(value.as_slice()),
    );
    drop(open_tree);
    Ok(())
}

/// Blob files a KV-separated tree is filled with, more than [`BLOB_LIMIT`].
const BLOB_FLUSHES: u64 = 300;

/// The descriptor limit the blob-file tests run under, above the 16 their
/// descriptor cache keeps and what the process keeps open on its own.
const BLOB_LIMIT: u64 = 128;

/// The value every key of a blob-file test holds.
const BLOB_VALUE: [u8; 64] = [b'v'; 64];

/// A KV-separated tree in `dir` whose descriptor cache keeps 16 files.
fn blob_config(dir: &std::path::Path) -> Config {
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default().separation_threshold(1),
    ))
    .use_descriptor_table(Some(std::sync::Arc::new(lsm_tree::DescriptorTable::new(
        16,
    ))))
}

/// A flush per key, each leaving a table and a blob file.
fn fill_one_blob_file_per_key(dir: &std::path::Path) -> lsm_tree::Result<()> {
    let tree = blob_config(dir).open()?;
    for i in 0..BLOB_FLUSHES {
        tree.insert(format!("k{i:04}"), BLOB_VALUE, i);
        tree.flush_active_memtable(0)?;
    }
    assert_eq!(tree.blob_file_count(), BLOB_FLUSHES as usize);
    Ok(())
}

/// Every key of a blob-file test reads its value back.
fn assert_every_blob_value_reads(dir: &std::path::Path) -> lsm_tree::Result<()> {
    let tree = blob_config(dir).open()?;
    assert_eq!(tree.blob_file_count(), BLOB_FLUSHES as usize);
    for i in 0..BLOB_FLUSHES {
        let key = format!("k{i:04}");
        assert_eq!(
            tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.as_deref(),
            Some(BLOB_VALUE.as_slice()),
            "{key} reads back",
        );
    }
    Ok(())
}

/// Opening a KV-separated tree recovers each blob file without holding its
/// descriptor past reading its metadata: the descriptor cache keeps them from
/// there, so a tree with more blob files than the process may open opens.
#[test]
fn open_of_more_blob_files_than_open_descriptors_reads_every_value() -> lsm_tree::Result<()> {
    let limit = DescriptorLimit::take_turn()?;
    let dir = tempfile::tempdir()?;
    fill_one_blob_file_per_key(dir.path())?;

    limit.lower_to(BLOB_LIMIT)?;
    assert_every_blob_value_reads(dir.path())
}

/// A KV-separated tree's blob files are held until the repair publishes, like
/// its tables, so they too go through its descriptor cache: a tree with more
/// blob files than the process may open is repaired whole.
#[test]
fn repair_of_more_blob_files_than_open_descriptors_recovers_every_value() -> lsm_tree::Result<()> {
    let limit = DescriptorLimit::take_turn()?;
    let dir = tempfile::tempdir()?;
    fill_one_blob_file_per_key(dir.path())?;
    common::nuke_manifest(dir.path())?;

    limit.lower_to(BLOB_LIMIT)?;
    let report = blob_config(dir.path()).repair()?;
    assert_eq!(
        report.recovered,
        BLOB_FLUSHES as usize,
        "every intact table is recovered: {:?}",
        report.unreadable_files.first(),
    );
    assert_every_blob_value_reads(dir.path())
}

/// The replacements a salvaging repair builds are held until it publishes,
/// like the tables it recovers whole, so they too go through its descriptor
/// cache: a tree with more damaged tables than the process may open is
/// salvaged whole.
#[test]
fn salvage_of_more_tables_than_open_descriptors_keeps_every_replacement() -> lsm_tree::Result<()> {
    const DAMAGED: u64 = 300;
    const LIMIT: u64 = 128;
    let limit = DescriptorLimit::take_turn()?;
    let dir = tempfile::tempdir()?;
    let config = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_descriptor_table(Some(std::sync::Arc::new(lsm_tree::DescriptorTable::new(
            16,
        ))))
    };
    {
        let tree = config().open()?;
        let mut seqno = 0;
        for table in 0..DAMAGED {
            // Several data blocks, so one corrupt block leaves the rest.
            for row in 0..500u32 {
                tree.insert(format!("t{table:03}r{row:04}"), [b'v'; 64], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
    }
    for sst in common::sorted_sst_paths(dir.path()) {
        common::corrupt_data_region(&sst)?;
    }
    common::nuke_manifest(dir.path())?;

    limit.lower_to(LIMIT)?;
    let report = config().repair_with_salvage(true)?;
    assert_eq!(
        (report.recovered, report.salvaged),
        (DAMAGED as usize, DAMAGED as usize),
        "every damaged table is salvaged: {:?}",
        report.unreadable_files.first(),
    );
    assert_eq!(config().open()?.table_count(), DAMAGED as usize);
    Ok(())
}
