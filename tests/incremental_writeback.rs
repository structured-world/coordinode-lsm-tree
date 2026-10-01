//! A table or blob file being written starts writing back every
//! `Config::writeback_bytes` it gathers, so its final sync is short; `0`
//! starts none.

use lsm_tree::fs::{FaultFs, FaultInjector, Fs, MemFs};
use lsm_tree::{AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;

/// The bytes a writeback covers at least.
const STEP: u64 = 4 * 1_024;

/// Flushes about 200 KiB of values through a tree configured by `configure`
/// on a recording backend and returns the recorder.
fn flushed(configure: impl FnOnce(Config) -> Config) -> lsm_tree::Result<Arc<FaultInjector>> {
    let fs = FaultFs::new(MemFs::new());
    let injector = fs.injector();
    let config = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs) as Arc<dyn Fs>);
    let tree = configure(config).open()?;
    for key in 0..2_000u32 {
        tree.insert(format!("{key:06}"), vec![b'v'; 100], u64::from(key));
    }
    tree.flush_active_memtable(0)?;
    Ok(injector)
}

/// Asserts `ranges` start at the file's first byte, each picks up where the
/// one before ended, and each covers at least `STEP`.
fn assert_contiguous(ranges: &[(u64, u64)], what: &str) {
    assert!(
        ranges.len() > 1,
        "{what}: several writebacks, got {ranges:?}"
    );
    let mut end = 0;
    for &(offset, len) in ranges {
        assert_eq!(offset, end, "{what}: a gap or an overlap in {ranges:?}");
        assert!(
            len >= STEP,
            "{what}: a writeback below the step in {ranges:?}"
        );
        end = offset + len;
    }
}

#[test]
fn a_table_being_written_starts_writeback_every_step() -> lsm_tree::Result<()> {
    let injector = flushed(|config| config.writeback_bytes(STEP))?;
    assert_contiguous(&injector.writebacks_for("tables"), "table");
    Ok(())
}

#[test]
fn a_blob_file_being_written_starts_writeback_every_step() -> lsm_tree::Result<()> {
    let injector = flushed(|config| {
        config
            .writeback_bytes(STEP)
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;
    assert_contiguous(&injector.writebacks_for("blobs"), "blob file");
    Ok(())
}

/// The sections a table writes after its data, a large filter here, are
/// written back as they are, so the final sync does not flush them in one
/// burst: what is left unhanded when the file is synced is the last few
/// sections, not the filter.
#[test]
fn a_tables_final_sections_are_written_back_as_they_are_written() -> lsm_tree::Result<()> {
    use lsm_tree::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry};

    let fs = FaultFs::new(MemFs::new());
    let injector = fs.injector();
    let fs: Arc<dyn Fs> = Arc::new(fs);
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .writeback_bytes(STEP)
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(50.0),
    )))
    .open()?;
    for key in 0..20_000u32 {
        tree.insert(format!("{key:06}"), b"v", u64::from(key));
    }
    tree.flush_active_memtable(0)?;
    // The meta section, the table of contents and the trailer come after the
    // last section boundary; a few steps hold them.
    assert_tail_within_a_few_steps(&*fs, &injector)
}

/// Asserts that the one table the flush wrote left at most a few steps to its
/// final sync, past the end of the last writeback.
fn assert_tail_within_a_few_steps(fs: &dyn Fs, injector: &FaultInjector) -> lsm_tree::Result<()> {
    // Absolute as the tree makes its folder (`D:\db` on Windows).
    let tables = std::path::absolute("/db/tables").expect("absolute tables folder");
    let table = fs
        .read_dir(&tables)?
        .into_iter()
        .find(|entry| !entry.is_dir)
        .expect("the flush wrote a table");
    let size = fs.metadata(&table.path)?.len;
    let ranges = injector.writebacks_for("tables");
    let handed = ranges.last().map_or(0, |&(offset, len)| offset + len);
    let tail = size - handed;
    assert!(
        tail <= 4 * STEP,
        "{tail} of {size} bytes left to the final sync: {ranges:?}"
    );
    Ok(())
}

/// The top-level index mirror a table writes near its end can itself exceed a
/// step on a large partitioned index; it is written back before the closing
/// sections, not left to the final sync with them.
#[test]
fn a_large_index_mirror_is_written_back_before_the_final_sync() -> lsm_tree::Result<()> {
    use lsm_tree::config::{BlockSizePolicy, PinningPolicy};

    let fs = FaultFs::new(MemFs::new());
    let injector = fs.injector();
    let fs: Arc<dyn Fs> = Arc::new(fs);
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&fs))
    .writeback_bytes(STEP)
    .data_block_size_policy(BlockSizePolicy::all(64))
    .index_block_partitioning_policy(PinningPolicy::all(true))
    .index_block_partition_size_policy(BlockSizePolicy::all(64))
    .open()?;
    for key in 0..20_000u32 {
        tree.insert(format!("{key:06}"), b"v", u64::from(key));
    }
    tree.flush_active_memtable(0)?;
    assert_tail_within_a_few_steps(&*fs, &injector)
}

/// A writeback is a hint: the final sync still makes the file durable and
/// reports a failed write. A backend that refuses the hint must not fail the
/// flush, and a file whose hint was refused is not asked again.
#[test]
fn a_refused_writeback_neither_fails_the_flush_nor_is_asked_again() -> lsm_tree::Result<()> {
    use lsm_tree::fs::{Fault, FaultOp, FaultRule};
    use lsm_tree::io::ErrorKind;

    let fs = FaultFs::new(MemFs::new());
    let injector = fs.injector();
    injector.arm(FaultRule::new(
        FaultOp::StartWriteback,
        Fault::Error(ErrorKind::Unsupported),
    ));
    let tree = Config::new(
        "/db",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(fs) as Arc<dyn Fs>)
    .writeback_bytes(STEP)
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;
    for key in 0..2_000u32 {
        tree.insert(format!("{key:06}"), vec![b'v'; 100], u64::from(key));
    }
    tree.flush_active_memtable(0)?;
    assert_eq!(
        tree.get("001999", u64::MAX)?.as_deref(),
        Some(&[b'v'; 100][..])
    );
    assert_eq!(
        injector.writebacks_for("tables").len(),
        1,
        "one table, asked once"
    );
    assert_eq!(
        injector.writebacks_for("blobs").len(),
        1,
        "one blob file, asked once"
    );
    Ok(())
}

#[test]
fn a_zero_step_starts_no_writeback() -> lsm_tree::Result<()> {
    let injector = flushed(|config| {
        config
            .writeback_bytes(0)
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    })?;
    assert_eq!(injector.writebacks_for(""), Vec::new());
    Ok(())
}
