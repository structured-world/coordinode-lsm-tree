use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs, StdFs};
use crate::path::Path;
use crate::{Config, SequenceNumberCounter};
use alloc::sync::Arc;
use test_log::test;

/// A failed batch takes back what a tree it made holds while it still holds
/// that tree's directory lock: another opener taking the lock between the
/// batch closing the tree and removing what is in it would have its tree
/// removed from under it. Only the empty folder goes after the lock does.
///
/// On the real file system, whose directory lock excludes another handle in
/// this process as it does another process.
#[test]
fn a_failed_batch_removes_a_tree_it_made_under_the_tree_lock() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let root = folder.path().to_path_buf();
    let fault = Arc::new(FaultFs::new(StdFs));
    let fs: Arc<dyn Fs> = fault.clone();
    let path = |i: usize| root.join(format!("tree{i}"));
    let config = |i: usize| {
        Config::new(
            path(i),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_shared_fs(Arc::clone(&fs))
    };
    // The second tree's snapshot is never installed.
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(path(1).to_string_lossy()),
    );
    // Before each removal of what the first tree holds, another opener tries
    // its lock.
    let attempts = Arc::new(spin::Mutex::new(Vec::new()));
    {
        let fs = Arc::clone(&fs);
        let attempts = Arc::clone(&attempts);
        let tree = path(0);
        let spelled = tree.to_string_lossy().into_owned();
        fault.injector().hook_removal(
            spelled,
            Arc::new(move |op: FaultOp, removed: &Path| {
                if op == FaultOp::RemoveDir && removed == tree {
                    return;
                }
                let taken = crate::config::acquire_directory_lock(&*fs, &tree, true).is_ok();
                attempts.lock().push((removed.to_path_buf(), taken));
            }),
        );
    }

    assert!(
        Config::open_many([config(0), config(1)]).is_err(),
        "the batch fails"
    );
    let attempts = attempts.lock();
    assert!(!attempts.is_empty(), "the first tree is removed");
    assert!(
        attempts.iter().all(|(_, taken)| !taken),
        "no other opener takes the lock while the tree goes: {attempts:?}"
    );
    assert!(!fault.exists(&path(0))?, "the tree folder goes too");
    Ok(())
}
