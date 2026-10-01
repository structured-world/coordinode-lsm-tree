//! A table written outside a tree, through the public `table::Writer` or by
//! `salvage_sst`, is durable once the call returns: its file and the directory
//! entry that names it survive a power loss, without a tree install to sync
//! that directory.

use lsm_tree::fs::{CrashFs, Fs, MemFs};
use lsm_tree::table::Writer;
use lsm_tree::{InternalValue, ValueType};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Writes a ten-key table at `path` and returns once `finish` did.
fn write_table(path: &Path, fs: &Arc<dyn Fs>) -> lsm_tree::Result<()> {
    let mut writer = Writer::new(path.to_path_buf(), 1, 0, Arc::clone(fs))?;
    for i in 0u64..10 {
        writer.write(InternalValue::from_components(
            format!("key-{i:02}").into_bytes(),
            b"value".to_vec(),
            i,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "the table holds keys");
    Ok(())
}

/// The folder the tables go to, absolute as the writer makes it (`D:\tables`
/// on Windows), so the folder made here is the one written to.
fn folder(fs: &Arc<dyn Fs>) -> lsm_tree::Result<PathBuf> {
    let folder = std::path::absolute("/tables").expect("absolute folder");
    fs.create_dir_all(&folder)?;
    Ok(folder)
}

#[test]
fn a_finished_standalone_table_survives_a_crash() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let fs: Arc<dyn Fs> = Arc::new(crash.clone());
    let path = folder(&fs)?.join("1");
    write_table(&path, &fs)?;

    crash.crash();
    assert!(
        crash.inner().exists(&path)?,
        "a table `finish` returned for is not lost with its directory entry"
    );
    Ok(())
}

/// A salvage whose source's metadata mirrors agree writes straight to its
/// destination, with no temporary to publish, so its own write is what makes
/// the new name durable.
#[test]
fn a_salvaged_table_survives_a_crash() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let fs: Arc<dyn Fs> = Arc::new(crash.clone());
    let folder = folder(&fs)?;
    let source = folder.join("1");
    write_table(&source, &fs)?;

    let dest = folder.join("1.salvaged");
    let report = lsm_tree::salvage::salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "a healthy source is recovered whole");

    crash.crash();
    assert!(
        crash.inner().exists(&dest)?,
        "a table `salvage_sst` returned for is not lost with its directory entry"
    );
    Ok(())
}
