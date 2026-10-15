// A tree reads wall-clock time through the clock its config names: the age
// stamped on its tables and blob files, and the FIFO TTL cutoff. Two trees
// given the same fixed clock and the same writes produce the same files, byte
// for byte, wherever they live.

mod common;

use common::{corrupt_data_region, nuke_manifest, sorted_sst_paths};
use lsm_tree::{
    AbstractTree, AnyTree, Clock, Config, KvSeparationOptions, SequenceNumberCounter,
    compaction::Fifo,
    file::{BLOBS_FOLDER, TABLES_FOLDER},
    get_tmp_folder,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// A clock the test sets by hand, in whole seconds.
struct ManualClock(AtomicU64);

impl ManualClock {
    fn at(secs: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(secs)))
    }

    fn set(&self, secs: u64) {
        self.0.store(secs, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn unix_time(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::Relaxed))
    }
}

fn config(folder: &Path, clock: Arc<dyn Clock>) -> Config {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_clock(clock)
}

/// Every file of `sub` under `folder`, by name.
fn files(folder: &Path, sub: &str) -> lsm_tree::Result<BTreeMap<String, Vec<u8>>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(folder.join(sub))? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        files.insert(name, std::fs::read(entry.path())?);
    }
    Ok(files)
}

/// Three flushes of overlapping keys, then a major compaction.
fn write(tree: &AnyTree) -> lsm_tree::Result<()> {
    let mut seqno = 0;
    for round in 0..3u32 {
        for i in 0..400u32 {
            let key = format!("k{:05}", (i * 7 + round * 13) % 1_000);
            let value = if i % 5 == 0 {
                format!("{round}-{i}-").repeat(64)
            } else {
                format!("v{round}-{i}")
            };
            tree.insert(key, value, seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, seqno)?;
    Ok(())
}

fn assert_same_files(a: &Path, b: &Path, sub: &str) -> lsm_tree::Result<()> {
    let (a, b) = (files(a, sub)?, files(b, sub)?);
    assert!(!a.is_empty(), "no {sub} written");
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "the trees named their {sub} differently"
    );
    for (name, bytes) in &a {
        assert!(bytes == &b[name], "{sub}/{name} differs between the trees");
    }
    Ok(())
}

#[test]
fn trees_with_the_same_fixed_clock_write_identical_tables() -> lsm_tree::Result<()> {
    let (first, second) = (get_tmp_folder(), get_tmp_folder());
    for folder in [&first, &second] {
        let tree = config(folder.path(), ManualClock::at(1_000)).open()?;
        write(&tree)?;
    }
    assert_same_files(first.path(), second.path(), TABLES_FOLDER)
}

#[cfg(feature = "columnar")]
#[test]
fn trees_with_the_same_fixed_clock_write_identical_columnar_tables() -> lsm_tree::Result<()> {
    let (first, second) = (get_tmp_folder(), get_tmp_folder());
    for folder in [&first, &second] {
        let tree = config(folder.path(), ManualClock::at(1_000)).open()?;
        let AnyTree::Standard(standard) = &tree else {
            panic!("expected a standard tree");
        };
        standard.update_runtime_config(|cfg| cfg.columnar = true)?;
        write(&tree)?;
    }
    assert_same_files(first.path(), second.path(), TABLES_FOLDER)
}

#[test]
fn trees_with_the_same_fixed_clock_write_identical_blob_files() -> lsm_tree::Result<()> {
    let (first, second) = (get_tmp_folder(), get_tmp_folder());
    for folder in [&first, &second] {
        let tree = config(folder.path(), ManualClock::at(1_000))
            .with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(64),
            ))
            .open()?;
        write(&tree)?;
    }
    assert_same_files(first.path(), second.path(), TABLES_FOLDER)?;
    assert_same_files(first.path(), second.path(), BLOBS_FOLDER)
}

/// A table repair salvages is stamped on the tree's clock, as every table the
/// tree writes is: stamped on the process clock it would sit in FIFO's future
/// under a lagging configured clock and never expire.
#[test]
fn repair_salvage_stamps_the_configured_clock() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    {
        let tree = config(folder.path(), ManualClock::at(1_000)).open()?;
        for i in 0..500u64 {
            tree.insert(format!("k{i:05}"), format!("v-{i}"), i);
        }
        tree.flush_active_memtable(0)?;
    }
    let ssts = sorted_sst_paths(folder.path());
    let victim = ssts.first().expect("a table to corrupt");
    corrupt_data_region(victim)?;
    nuke_manifest(folder.path())?;

    let report = config(folder.path(), ManualClock::at(2_000)).repair_with_salvage(true)?;
    assert_eq!(report.salvaged, 1, "{:?}", report.unreadable_files);
    assert_eq!(
        lsm_tree::inspect::read_table_properties(victim)?.created_at_nanos,
        Duration::from_secs(2_000).as_nanos(),
        "the salvaged table was not stamped on the configured clock",
    );
    Ok(())
}

#[test]
fn fifo_ttl_expires_tables_by_the_configured_clock() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let clock = ManualClock::at(1_000);
    let tree = config(folder.path(), clock.clone()).open()?;
    for i in 0..3u64 {
        tree.insert(format!("k{i}"), "v", i);
        tree.flush_active_memtable(0)?;
    }

    let fifo = Arc::new(Fifo::new(u64::MAX, Some(60)));
    clock.set(1_059);
    tree.compact(fifo.clone(), 3)?;
    assert_eq!(3, tree.table_count(), "tables expired before their TTL");

    clock.set(1_061);
    tree.compact(fifo, 3)?;
    assert_eq!(0, tree.table_count(), "tables outlived their TTL");
    Ok(())
}
