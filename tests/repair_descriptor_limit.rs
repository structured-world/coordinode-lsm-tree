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

    let (_, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
    rlimit::setrlimit(rlimit::Resource::NOFILE, DESCRIPTOR_LIMIT, hard)?;

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

/// The replacements a salvaging repair builds are held until it publishes,
/// like the tables it recovers whole, so they too go through its descriptor
/// cache: a tree with more damaged tables than the process may open is
/// salvaged whole.
#[test]
fn salvage_of_more_tables_than_open_descriptors_keeps_every_replacement() -> lsm_tree::Result<()> {
    const DAMAGED: u64 = 300;
    const LIMIT: u64 = 128;
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

    let (_, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE)?;
    rlimit::setrlimit(rlimit::Resource::NOFILE, LIMIT, hard)?;

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
