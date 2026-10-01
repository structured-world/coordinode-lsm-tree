//! A table written through the public `table::Writer` is durable once `finish`
//! returns: its file and the directory entry that names it survive a power
//! loss, without a tree install to sync that directory.

use lsm_tree::fs::{CrashFs, Fs, MemFs};
use lsm_tree::table::Writer;
use lsm_tree::{InternalValue, ValueType};
use std::sync::Arc;

#[test]
fn a_finished_standalone_table_survives_a_crash() -> lsm_tree::Result<()> {
    let crash = CrashFs::new(MemFs::new());
    let fs: Arc<dyn Fs> = Arc::new(crash.clone());
    // Absolute as the writer makes it (`D:\tables` on Windows), so the folder
    // made here is the one the table is written to.
    let folder = std::path::absolute("/tables").expect("absolute folder");
    fs.create_dir_all(&folder)?;
    let path = folder.join("1");

    let mut writer = Writer::new(path.clone(), 1, 0, Arc::clone(&fs))?;
    for i in 0u64..10 {
        writer.write(InternalValue::from_components(
            format!("key-{i:02}").into_bytes(),
            b"value".to_vec(),
            i,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "the table holds keys");

    crash.crash();
    assert!(
        crash.inner().exists(&path)?,
        "a table `finish` returned for is not lost with its directory entry"
    );
    Ok(())
}
