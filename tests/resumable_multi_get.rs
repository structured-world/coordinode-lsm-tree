// A multi-get its caller drives answers what the blocking multi-get answers,
// holds the version it started on while it is suspended, and can be carried
// across threads with every job it hands out.

use lsm_tree::resumable::{ReadJob, ResumableMultiGet, Step};
use lsm_tree::{
    AbstractTree, AnyTree, Cache, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    UserValue, io,
};
use std::sync::Arc;

/// Drives a read on the calling thread, a job at a time and a block at a
/// time, the way the simplest caller would.
fn drive(mut step: Step) -> lsm_tree::Result<Vec<Option<UserValue>>> {
    loop {
        let mut read = match step {
            Step::Done(values) => return values,
            Step::Pending(read) => read,
        };
        for job in read.take_jobs() {
            let outcome = job.run();
            read.complete_job(outcome);
        }
        for mut block in read.take_reads() {
            let result = block
                .file
                .read_at(&mut block.buf, block.offset)
                .and_then(|n| {
                    if n == block.buf.len() {
                        Ok(())
                    } else {
                        Err(io::ErrorKind::UnexpectedEof.into())
                    }
                });
            read.complete_read(block.tag, result, block.buf);
        }
        step = read.resume();
    }
}

/// A tree over two levels and a memtable: values overwritten, deleted and
/// written again, so the batch holds hits in every layer, misses, tombstones
/// and versions shadowed by newer ones.
fn layered_tree(
    folder: &std::path::Path,
    blob: bool,
    cache_bytes: u64,
) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(cache_bytes)));
    if blob {
        config =
            config.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)));
    }
    let tree = config.open()?;
    let mut seqno = 0;
    for i in 0..2_000u32 {
        tree.insert(format!("k{i:05}"), format!("old-{i}"), seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;
    for i in (0..2_000u32).step_by(3) {
        tree.insert(format!("k{i:05}"), format!("new-{i}"), seqno);
        seqno += 1;
    }
    for i in (0..2_000u32).step_by(7) {
        tree.remove(format!("k{i:05}"), seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0)?;
    for i in (0..2_000u32).step_by(11) {
        tree.insert(format!("k{i:05}"), format!("mem-{i}"), seqno);
        seqno += 1;
    }
    Ok(tree)
}

/// Every key of the batch, present or not, some asked twice.
fn batch() -> Vec<String> {
    let mut keys: Vec<String> = (0..2_400u32)
        .step_by(5)
        .map(|i| format!("k{i:05}"))
        .collect();
    keys.push("k00010".into());
    keys.push("absent".into());
    keys
}

/// The resumable read of a layered tree answers what the blocking read
/// answers: hits in the memtable and in both levels, misses, deleted keys,
/// shadowed versions, repeated keys; with a warm cache and with none.
#[test]
fn a_resumable_read_answers_what_the_blocking_read_answers() -> lsm_tree::Result<()> {
    for cache_bytes in [32 * 1_024 * 1_024, 0] {
        let folder = tempfile::tempdir()?;
        let tree = layered_tree(folder.path(), false, cache_bytes)?;
        let keys = batch();
        let blocking = tree.multi_get(&keys, SeqNo::MAX)?;
        let resumable = drive(tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?)?;
        assert_eq!(resumable, blocking, "cache of {cache_bytes} bytes");
        assert!(resumable.iter().any(Option::is_some));
        assert!(resumable.iter().any(Option::is_none));
    }
    Ok(())
}

/// A blob tree's values live in the value log: the resumable read hands out a
/// job per value and answers what the blocking read answers.
#[test]
fn a_resumable_read_of_a_blob_tree_answers_what_the_blocking_read_answers() -> lsm_tree::Result<()>
{
    let folder = tempfile::tempdir()?;
    let tree = layered_tree(folder.path(), true, 0)?;
    assert!(matches!(tree, AnyTree::Blob(_)));
    let keys = batch();
    let blocking = tree.multi_get(&keys, SeqNo::MAX)?;
    let resumable = drive(tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?)?;
    assert_eq!(resumable, blocking);
    Ok(())
}

/// A batch of one key, and an empty one, go through the same read.
#[test]
fn a_resumable_read_of_one_key_or_none_answers() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = layered_tree(folder.path(), false, 0)?;
    assert_eq!(
        drive(tree.start_multi_get(["k00003"], SeqNo::MAX)?)?,
        tree.multi_get(["k00003"], SeqNo::MAX)?
    );
    assert!(drive(tree.start_multi_get(Vec::<&str>::new(), SeqNo::MAX)?)?.is_empty());
    Ok(())
}

/// A read suspended across a compaction that rewrites every table it planned
/// against, and across writes after it started, answers at its own snapshot.
#[test]
fn a_suspended_read_answers_at_its_snapshot_across_a_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = layered_tree(folder.path(), false, 0)?;
    let keys = batch();
    let seqno = SeqNo::MAX;
    let before = tree.multi_get(&keys, seqno)?;

    let Step::Pending(mut read) = tree.start_multi_get(keys.iter().map(String::as_str), seqno)?
    else {
        panic!("a cold read has work to hand out");
    };
    // The read is parked with its work out while the tree moves on.
    let jobs = read.take_jobs();
    let reads = read.take_reads();
    for key in &keys {
        tree.insert(key.as_str(), "after", 1_000_000);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    for job in jobs {
        let outcome = job.run();
        read.complete_job(outcome);
    }
    for mut block in reads {
        let result = block.file.read_at(&mut block.buf, block.offset).map(|_| ());
        read.complete_read(block.tag, result, block.buf);
    }
    assert_eq!(drive(read.resume())?, before);
    Ok(())
}

/// A blob tree's read parked across a compaction that rewrites every value
/// and leaves the blob files it read from unreferenced by the tree still
/// reads its snapshot's values from them.
#[test]
fn a_suspended_blob_read_answers_at_its_snapshot_across_blob_gc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = layered_tree(folder.path(), true, 0)?;
    tree.flush_active_memtable(0)?;
    let keys = batch();
    let before = tree.multi_get(&keys, SeqNo::MAX)?;

    let Step::Pending(mut read) =
        tree.start_multi_get(keys.iter().map(String::as_str), SeqNo::MAX)?
    else {
        panic!("a cold read has work to hand out");
    };
    let jobs = read.take_jobs();
    let reads = read.take_reads();
    let files_before = tree.blob_file_count();
    for key in &keys {
        tree.insert(key.as_str(), "rewritten", 2_000_000);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    assert!(
        tree.stale_blob_bytes() > 0 || tree.blob_file_count() != files_before,
        "the compaction left the old values' blob files stale or dropped"
    );

    for job in jobs {
        let outcome = job.run();
        read.complete_job(outcome);
    }
    for mut block in reads {
        let result = block.file.read_at(&mut block.buf, block.offset).map(|_| ());
        read.complete_read(block.tag, result, block.buf);
    }
    assert_eq!(drive(read.resume())?, before);
    Ok(())
}

/// A read and the jobs it hands out move to other threads: a caller's
/// scheduler holds the read, and a blocking pool runs the jobs.
#[test]
fn a_read_and_its_jobs_cross_threads() {
    fn sendable<T: Send + 'static>() {}
    sendable::<ResumableMultiGet>();
    sendable::<ReadJob>();
    sendable::<Step>();
}
