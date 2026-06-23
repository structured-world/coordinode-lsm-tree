use super::*;
use crate::fs::{Fs, FsOpenOptions, MemFs};
use std::io::Write;

#[test]
fn drains_queued_deletions_on_drop() {
    let fs: Arc<dyn Fs> = Arc::new(MemFs::default());
    let paths: Vec<PathBuf> = (0..16).map(|i| PathBuf::from(format!("/f{i}"))).collect();
    for p in &paths {
        let mut f = fs
            .open(p, &FsOpenOptions::new().write(true).create(true))
            .unwrap();
        f.write_all(b"data").unwrap();
        f.flush().unwrap();
        assert!(fs.open(p, &FsOpenOptions::new().read(true)).is_ok());
    }

    {
        let deleter = BackgroundDeleter::new(None);
        for p in &paths {
            deleter.enqueue(Arc::clone(&fs), p.clone());
        }
        // Drop drains the queue and joins the worker: every enqueued unlink
        // has completed by the time this scope ends.
    }

    for p in &paths {
        assert!(
            fs.open(p, &FsOpenOptions::new().read(true)).is_err(),
            "{} should have been unlinked by the background deleter",
            p.display(),
        );
    }
}
