// A compaction that relocates blob files writes the values its filter
// rewrites into the same blob output as the values it relocates: the filter
// adds no blob files of its own, and every value reads back.

use lsm_tree::{
    AbstractTree, AnyTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
    compaction::filter::{
        CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
    },
    get_tmp_folder,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use test_log::test;

/// Replaces the value of every key starting with `rewrite` with
/// [`rewritten`] while `on` is set.
struct Rewrite(Arc<AtomicBool>);

impl CompactionFilter for Rewrite {
    fn filter_item(
        &mut self,
        item: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        if self.0.load(Ordering::Relaxed) && item.key().starts_with(b"rewrite") {
            Ok(Verdict::ReplaceValue(rewritten(item.key()).into()))
        } else {
            Ok(Verdict::Keep)
        }
    }
}

struct RewriteFactory(Arc<AtomicBool>);

impl Factory for RewriteFactory {
    fn name(&self) -> &str {
        "rewrite"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(Rewrite(Arc::clone(&self.0)))
    }
}

/// A separated value written by the application, distinct per key and round.
fn written(key: &[u8], round: u32) -> Vec<u8> {
    let mut value = format!("{round}:").into_bytes();
    value.extend_from_slice(key);
    value.repeat(4_096)
}

/// The value the filter puts in place of `key`'s.
fn rewritten(key: &[u8]) -> Vec<u8> {
    let mut value = b"rewritten:".to_vec();
    value.extend_from_slice(key);
    value.repeat(4_096)
}

fn open(path: &std::path::Path, filter_on: &Arc<AtomicBool>) -> lsm_tree::Result<AnyTree> {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .age_cutoff(1.0)
            .staleness_threshold(0.1),
    ))
    .blob_compression(lsm_tree::CompressionType::None)
    .with_compaction_filter_factory(Some(Arc::new(RewriteFactory(Arc::clone(filter_on)))))
    .open()
}

const KEYS: [&[u8]; 4] = [b"keep-a", b"keep-b", b"rewrite-a", b"rewrite-b"];

/// Writes every key, then overwrites `keep-a` so the first blob file holds a
/// stale value, and compacts with the filter off: the run records that, so
/// the next major compaction relocates the file.
fn stale_first_file(tree: &AnyTree) -> lsm_tree::Result<()> {
    for key in KEYS {
        tree.insert(key, written(key, 0), 0);
    }
    tree.flush_active_memtable(0)?;
    tree.insert(KEYS[0], written(KEYS[0], 1), 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert!(
        tree.current_version().blob_files.contains_key(0),
        "the first blob file is still live before the relocation"
    );
    Ok(())
}

/// The blob files after a relocating compaction, with the filter on or off
/// during it.
fn relocate(filter: bool) -> lsm_tree::Result<(AnyTree, tempfile::TempDir)> {
    let folder = get_tmp_folder();
    let filter_on = Arc::new(AtomicBool::new(false));
    let tree = open(folder.path(), &filter_on)?;
    stale_first_file(&tree)?;

    filter_on.store(filter, Ordering::Relaxed);
    tree.major_compact(64_000_000, 1_000)?;
    assert!(
        !tree.current_version().blob_files.contains_key(0),
        "the compaction relocated the first blob file"
    );
    Ok((tree, folder))
}

#[test]
fn relocating_compaction_with_filter_rewrites_writes_one_set_of_blob_files() -> lsm_tree::Result<()>
{
    let (plain, _plain_dir) = relocate(false)?;
    let (filtered, _filtered_dir) = relocate(true)?;

    assert_eq!(
        filtered.blob_file_count(),
        plain.blob_file_count(),
        "the filter's rewrites added blob files of their own"
    );

    assert_eq!(
        filtered.get(KEYS[0], SeqNo::MAX)?.as_deref(),
        Some(written(KEYS[0], 1).as_slice())
    );
    assert_eq!(
        filtered.get(KEYS[1], SeqNo::MAX)?.as_deref(),
        Some(written(KEYS[1], 0).as_slice())
    );
    for key in &KEYS[2..] {
        assert_eq!(
            filtered.get(key, SeqNo::MAX)?.as_deref(),
            Some(rewritten(key).as_slice()),
            "{}",
            String::from_utf8_lossy(key)
        );
    }
    Ok(())
}

/// A value written inline before the separation threshold came down to its
/// size reaches a relocating compaction inline: the pass separates it into its
/// blob output under the threshold now in force, and it reads back.
#[test]
fn relocating_compaction_separates_inline_value_above_lowered_threshold() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let early = b"early!".repeat(400);
    let options = |threshold: u32| {
        KvSeparationOptions::default()
            .separation_threshold(threshold)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
    };
    let open = |threshold: u32| {
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(options(threshold)))
        .blob_compression(lsm_tree::CompressionType::None)
        .open()
    };

    {
        // Above the early value's size: it stays inline.
        let tree = open(4_096)?;
        tree.insert("early", &early, 0);
        tree.insert("big-a", written(b"big-a", 0), 0);
        tree.insert("big-b", written(b"big-b", 0), 0);
        tree.flush_active_memtable(0)?;
    }

    let tree = open(1_024)?;
    tree.insert("big-a", written(b"big-a", 1), 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 1_000)?;
    tree.major_compact(64_000_000, 1_000)?;
    assert!(
        !tree.current_version().blob_files.contains_key(0),
        "the compaction relocated the first blob file"
    );

    assert_eq!(
        tree.get("early", SeqNo::MAX)?.as_deref(),
        Some(early.as_slice())
    );
    // `big-a`'s newest version, `big-b`, and the early value, now separated:
    // the relocated file and the stale `big-a` it held are gone.
    let blob_values: u64 = tree
        .current_version()
        .blob_files
        .iter()
        .map(lsm_tree::BlobFile::len)
        .sum();
    assert_eq!(blob_values, 3, "the early value was not separated");
    Ok(())
}

/// Rounds that each overwrite one key and compact, so every compaction
/// relocates the file the round before left stale, while the filter rewrites
/// the same values in every one of them: the rewrites leave no more blob files
/// behind than the same rounds without the filter.
#[test]
fn filter_heavy_churn_with_relocation_keeps_blob_file_count() -> lsm_tree::Result<()> {
    let churn = |filter: bool| -> lsm_tree::Result<usize> {
        let folder = get_tmp_folder();
        let filter_on = Arc::new(AtomicBool::new(false));
        let tree = open(folder.path(), &filter_on)?;
        stale_first_file(&tree)?;
        filter_on.store(filter, Ordering::Relaxed);
        let mut seqno = 2;
        for round in 2..8 {
            tree.insert(KEYS[0], written(KEYS[0], round), seqno);
            seqno += 1;
            tree.flush_active_memtable(0)?;
            tree.major_compact(64_000_000, seqno)?;
        }
        assert_eq!(
            tree.get(KEYS[0], SeqNo::MAX)?.as_deref(),
            Some(written(KEYS[0], 7).as_slice())
        );
        assert_eq!(
            tree.get(KEYS[1], SeqNo::MAX)?.as_deref(),
            Some(written(KEYS[1], 0).as_slice())
        );
        for key in &KEYS[2..] {
            let expected = if filter {
                rewritten(key)
            } else {
                written(key, 0)
            };
            assert_eq!(
                tree.get(key, SeqNo::MAX)?.as_deref(),
                Some(expected.as_slice())
            );
        }
        Ok(tree.blob_file_count())
    };

    let plain = churn(false)?;
    let filtered = churn(true)?;
    assert!(
        filtered <= plain,
        "{filtered} blob files with the filter, {plain} without"
    );
    Ok(())
}
