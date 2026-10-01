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
