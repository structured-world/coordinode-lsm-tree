// A blob tree merges a key's operands onto its base wherever the base lives:
// inline in the memtable, or separated into the value log on flush, whatever
// table holds the operands and however the key is read.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, KvSeparationOptions, MergeOperator, SeqNo,
    SequenceNumberCounter, UserValue, ValueType, get_tmp_folder,
};
use std::sync::Arc;
use test_log::test;

struct ConcatMerge;

impl MergeOperator for ConcatMerge {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut result = base.unwrap_or_default().to_vec();
        for op in operands {
            result.extend_from_slice(op);
        }
        Ok(result.into())
    }
}

/// Above the threshold below, so a flush separates it into the value log.
const BASE_LEN: usize = 200;

fn base() -> Vec<u8> {
    vec![b'x'; BASE_LEN]
}

fn merged(operands: &[&[u8]]) -> Vec<u8> {
    let mut value = base();
    for op in operands {
        value.extend_from_slice(op);
    }
    value
}

fn open(folder: &std::path::Path) -> lsm_tree::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .open()
}

/// Where the base and its two operands are when the key is read.
#[derive(Clone, Copy, Debug)]
enum Layout {
    /// Everything in the memtable: the base is still inline.
    Memtable,
    /// The base flushed and separated, the operands in the memtable.
    BaseFlushed,
    /// The base and the operands flushed into one table.
    FlushedTogether,
    /// The base and each operand flushed into a table of its own.
    TablePerVersion,
}

/// Writes `a` (inline), `k` (base, then operands `_A` at seqno 3 and `_B` at
/// seqno 4) and `z` (separated) in the given layout.
fn fill(tree: &AnyTree, layout: Layout) -> lsm_tree::Result<()> {
    tree.insert("a", "small", 0);
    tree.insert("k", base(), 1);
    tree.insert("z", vec![b'z'; BASE_LEN], 2);
    if matches!(layout, Layout::BaseFlushed | Layout::TablePerVersion) {
        tree.flush_active_memtable(0)?;
    }
    tree.merge("k", "_A", 3);
    if matches!(layout, Layout::TablePerVersion) {
        tree.flush_active_memtable(0)?;
    }
    tree.merge("k", "_B", 4);
    if matches!(layout, Layout::FlushedTogether | Layout::TablePerVersion) {
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// The value of `k` among the pairs an iterator yields, after checking that
/// the iterator yields `a`, `k` and `z` once each.
fn value_of_k(pairs: Vec<(lsm_tree::UserKey, UserValue)>, what: &str) -> Vec<u8> {
    let keys: Vec<&[u8]> = pairs.iter().map(|(k, _)| k.as_ref()).collect();
    assert_eq!(keys, [b"a".as_slice(), b"k", b"z"], "keys, {what}");
    pairs
        .into_iter()
        .find(|(k, _)| k.as_ref() == b"k")
        .map(|(_, v)| v.to_vec())
        .unwrap_or_default()
}

fn collect(
    iter: impl Iterator<Item = lsm_tree::IterGuardImpl>,
) -> lsm_tree::Result<Vec<(lsm_tree::UserKey, UserValue)>> {
    iter.map(Guard::into_inner).collect()
}

/// Every read form answers `expected` for `k` at `seqno`.
fn assert_reads(tree: &AnyTree, seqno: SeqNo, expected: &[u8], what: &str) -> lsm_tree::Result<()> {
    assert_eq!(
        tree.get("k", seqno)?.as_deref(),
        Some(expected),
        "get, {what}"
    );
    assert_eq!(
        tree.size_of("k", seqno)?,
        Some(u32::try_from(expected.len()).expect("small value")),
        "size_of, {what}"
    );
    assert_eq!(
        tree.multi_get(["k"], seqno)?[0].as_deref(),
        Some(expected),
        "batch of one, {what}"
    );
    assert_eq!(
        tree.multi_get(["k", "absent"], seqno)?[0].as_deref(),
        Some(expected),
        "batch of two, {what}"
    );
    assert_eq!(
        tree.multi_get(["a", "k", "z"], seqno)?[1].as_deref(),
        Some(expected),
        "batch of three, {what}"
    );

    let forward = collect(tree.iter(seqno, None))?;
    assert_eq!(value_of_k(forward, what), expected, "iter, {what}");

    let mut backward = collect(tree.iter(seqno, None).rev())?;
    backward.reverse();
    assert_eq!(value_of_k(backward, what), expected, "reverse iter, {what}");

    let range = collect(tree.range("a"..="z", seqno, None))?;
    assert_eq!(value_of_k(range, what), expected, "range, {what}");

    let prefix = collect(tree.prefix("k", seqno, None))?;
    assert_eq!(
        prefix
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>(),
        [(b"k".to_vec(), expected.to_vec())],
        "prefix, {what}"
    );

    let seekable = collect(tree.range_seekable::<&str, _>(.., seqno, None))?;
    assert_eq!(
        value_of_k(seekable, what),
        expected,
        "seekable range, {what}"
    );

    let batched = collect(tree.batch_range_scan(["a"..="z"], seqno, None))?;
    assert_eq!(
        value_of_k(batched, what),
        expected,
        "batch range scan, {what}"
    );

    Ok(())
}

#[test]
fn blob_tree_merges_operands_onto_its_base_in_every_layout() -> lsm_tree::Result<()> {
    for layout in [
        Layout::Memtable,
        Layout::BaseFlushed,
        Layout::FlushedTogether,
        Layout::TablePerVersion,
    ] {
        let folder = get_tmp_folder();
        let tree = open(folder.path())?;
        fill(&tree, layout)?;

        let what = format!("{layout:?}, latest");
        assert_reads(&tree, SeqNo::MAX, &merged(&[b"_A", b"_B"]), &what)?;
        let what = format!("{layout:?}, before the second operand");
        assert_reads(&tree, 4, &merged(&[b"_A"]), &what)?;
        let what = format!("{layout:?}, before any operand");
        assert_reads(&tree, 3, &base(), &what)?;
    }
    Ok(())
}

/// The index of a blob tree merges onto a separated base too, through its own
/// point reads: alone, pinned, and in batches of every size.
#[test]
fn blob_tree_index_merges_operands_onto_a_separated_base() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let AnyTree::Blob(tree) = open(folder.path())? else {
        panic!("a blob tree");
    };
    let any = AnyTree::Blob(tree.clone());
    fill(&any, Layout::BaseFlushed)?;
    let index = &tree.index;
    let expected = merged(&[b"_A", b"_B"]);
    let expected = Some(expected.as_slice());
    assert_eq!(index.get("k", SeqNo::MAX)?.as_deref(), expected, "get");
    assert_eq!(
        index
            .get_pinned("k", SeqNo::MAX)?
            .as_ref()
            .map(AsRef::as_ref),
        expected,
        "get_pinned"
    );
    assert_eq!(
        index.multi_get(["k", "absent"], SeqNo::MAX)?[0].as_deref(),
        expected,
        "batch of two"
    );
    assert_eq!(
        index.multi_get(["a", "k", "z"], SeqNo::MAX)?[1].as_deref(),
        expected,
        "batch of three"
    );
    Ok(())
}

/// A compaction that may not fold the operands (every version is above its
/// watermark) leaves the key readable as before.
#[test]
fn blob_tree_merge_onto_a_separated_base_survives_a_compaction_that_keeps_versions()
-> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    fill(&tree, Layout::TablePerVersion)?;
    tree.major_compact(64_000_000, 0)?;

    assert_reads(&tree, SeqNo::MAX, &merged(&[b"_A", b"_B"]), "latest")?;
    assert_reads(&tree, 4, &merged(&[b"_A"]), "before the second operand")?;
    assert_reads(&tree, 3, &base(), "before any operand")?;
    Ok(())
}

/// A compaction past every version folds the operands onto the separated
/// base: the key's newest version is a value again, separated into the value
/// log as a put of its size would be, the old base's blob is no longer
/// referenced, and the key reads the same, also after a reopen.
#[test]
fn blob_tree_compaction_folds_operands_onto_a_separated_base() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    {
        let tree = open(folder.path())?;
        fill(&tree, Layout::TablePerVersion)?;
        assert_eq!(tree.stale_blob_bytes(), 0, "nothing stale before the fold");
        tree.major_compact(64_000_000, 5)?;

        let newest = tree
            .get_internal_entry(b"k", SeqNo::MAX)?
            .expect("k is present");
        assert_eq!(
            newest.key.value_type,
            ValueType::Indirection,
            "folded into a separated value"
        );
        assert!(
            tree.stale_blob_bytes() > 0,
            "the base's blob is stale once the fold replaces it"
        );
        assert_reads(&tree, SeqNo::MAX, &merged(&[b"_A", b"_B"]), "compacted")?;
    }
    let tree = open(folder.path())?;
    assert_reads(&tree, SeqNo::MAX, &merged(&[b"_A", b"_B"]), "reopened")?;
    Ok(())
}

/// A fold onto an inline base whose value reaches the separation threshold
/// goes to the value log too, not inline into the index.
#[test]
fn blob_tree_compaction_separates_a_folded_value_of_separation_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    // Each version below the threshold, the fold above it.
    let operand = vec![b'o'; 60];
    tree.insert("k", "BASE", 0);
    tree.flush_active_memtable(0)?;
    tree.merge("k", operand.as_slice(), 1);
    tree.flush_active_memtable(0)?;
    tree.merge("k", operand.as_slice(), 2);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 3)?;

    let newest = tree
        .get_internal_entry(b"k", SeqNo::MAX)?
        .expect("k is present");
    assert_eq!(
        newest.key.value_type,
        ValueType::Indirection,
        "a folded value of separation size is separated"
    );
    let expected = [b"BASE".as_slice(), &operand, &operand].concat();
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(expected.as_slice())
    );
    Ok(())
}

/// A fold a deleting compaction drops (a newer range tombstone covers it)
/// writes nothing to the value log: the operator here makes a value of
/// separation size even from nothing, and the dropped result must not leave a
/// blob file no table references.
#[test]
fn blob_tree_compaction_writes_no_blob_for_a_fold_a_tombstone_deletes() -> lsm_tree::Result<()> {
    /// Concatenates, and stands in a value of separation size for nothing.
    struct FillMerge;
    impl MergeOperator for FillMerge {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> lsm_tree::Result<UserValue> {
            let mut result = base.unwrap_or_default().to_vec();
            for op in operands {
                result.extend_from_slice(op);
            }
            if result.is_empty() {
                result = vec![b'f'; BASE_LEN];
            }
            Ok(result.into())
        }
    }

    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(FillMerge)))
    .open()?;
    tree.insert("k", "BASE", 0);
    tree.flush_active_memtable(0)?;
    tree.merge("k", "_A", 1);
    tree.flush_active_memtable(0)?;
    tree.remove_range("j", "l", 2);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 10)?;

    assert_eq!(
        tree.get("k", SeqNo::MAX)?,
        None,
        "the range tombstone deletes k"
    );
    assert_eq!(
        tree.blob_file_count(),
        0,
        "the dropped fold left a blob file behind"
    );
    Ok(())
}

/// A filter rewrite of an entry a deleting compaction then drops (a newer
/// range tombstone covers it) writes nothing to the value log either.
#[test]
fn blob_tree_compaction_writes_no_blob_for_a_filter_rewrite_a_tombstone_deletes()
-> lsm_tree::Result<()> {
    use lsm_tree::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};

    struct Grow;
    impl CompactionFilter for Grow {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
            Ok(Verdict::ReplaceValue(vec![b'r'; BASE_LEN].into()))
        }
    }
    struct GrowFactory;
    impl Factory for GrowFactory {
        fn name(&self) -> &str {
            "grow"
        }
        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(Grow)
        }
    }

    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_compaction_filter_factory(Some(Arc::new(GrowFactory)))
    .open()?;
    tree.insert("k", "small", 0);
    tree.flush_active_memtable(0)?;
    tree.remove_range("j", "l", 1);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 10)?;

    assert_eq!(
        tree.get("k", SeqNo::MAX)?,
        None,
        "the range tombstone deletes k"
    );
    assert_eq!(
        tree.blob_file_count(),
        0,
        "the dropped rewrite left a blob file behind"
    );
    Ok(())
}

/// `contains_key` asks whether the key exists, not what it merges to: it
/// answers without reading the value log or calling the operator, so an
/// operator that fails does not make an existing key an error.
#[test]
fn blob_tree_contains_key_does_not_merge() -> lsm_tree::Result<()> {
    struct FailMerge;
    impl MergeOperator for FailMerge {
        fn merge(&self, _: &[u8], _: Option<&[u8]>, _: &[&[u8]]) -> lsm_tree::Result<UserValue> {
            Err(lsm_tree::Error::MergeOperator)
        }
    }

    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(FailMerge)))
    .open()?;
    tree.insert("k", base(), 0);
    tree.flush_active_memtable(0)?;
    tree.merge("k", "_A", 1);

    assert!(tree.contains_key("k", SeqNo::MAX)?, "k exists");
    assert!(!tree.contains_key("absent", SeqNo::MAX)?, "absent does not");
    Ok(())
}

/// A compaction's rate limit covers the bytes a fold writes to the value log,
/// not only the small pointer it emits: 40 KiB of folded values under a
/// 20 KiB/s limit take about a second past the one-second burst.
#[test]
fn blob_tree_compaction_charges_separated_folds_to_the_rate_limit() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .compaction_rate_limit(20_000)
    .open()?;
    let operand = vec![b'o'; 1_000];
    for i in 0..40u64 {
        tree.insert(format!("k{i:02}"), "B", i);
    }
    tree.flush_active_memtable(0)?;
    for i in 0..40u64 {
        tree.merge(format!("k{i:02}"), operand.as_slice(), 40 + i);
    }
    tree.flush_active_memtable(0)?;

    let started = std::time::Instant::now();
    tree.major_compact(64_000_000, 100)?;
    let took = started.elapsed();

    assert!(tree.blob_file_count() > 0, "the folds were separated");
    assert!(
        took >= std::time::Duration::from_millis(800),
        "the folded payload was throttled, took {took:?}"
    );
    Ok(())
}

/// An operand of separation size stays an operand through a flush: it is
/// kept inline, and the key still reads as the operand merged onto its base.
#[test]
fn blob_tree_flush_keeps_an_operand_of_separation_size_an_operand() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(folder.path())?;
    let operand = vec![b'o'; BASE_LEN];
    tree.insert("k", "BASE", 0);
    tree.merge("k", operand.as_slice(), 1);
    tree.flush_active_memtable(0)?;

    let newest = tree
        .get_internal_entry(b"k", SeqNo::MAX)?
        .expect("k is present");
    assert_eq!(
        newest.key.value_type,
        ValueType::MergeOperand,
        "still an operand"
    );
    let expected = [b"BASE".as_slice(), &operand].concat();
    assert_reads_single(&tree, &expected, "flushed")?;
    Ok(())
}

/// An operand a compaction filter rewrites to a value of separation size
/// stays an operand: it is kept inline, and the key reads as the rewritten
/// operand merged onto its base.
#[test]
fn blob_tree_filter_rewrite_of_an_operand_keeps_it_an_operand() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};

    struct GrowOperand;
    impl CompactionFilter for GrowOperand {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _: &Context,
        ) -> lsm_tree::Result<Verdict> {
            Ok(if item.value()?.as_ref() == b"_A" {
                Verdict::ReplaceValue(vec![b'r'; BASE_LEN].into())
            } else {
                Verdict::Keep
            })
        }
    }
    struct GrowOperandFactory;
    impl Factory for GrowOperandFactory {
        fn name(&self) -> &str {
            "grow-operand"
        }
        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(GrowOperand)
        }
    }

    let folder = get_tmp_folder();
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 100,
        ..Default::default()
    }))
    .with_merge_operator(Some(Arc::new(ConcatMerge)))
    .with_compaction_filter_factory(Some(Arc::new(GrowOperandFactory)))
    .open()?;
    tree.insert("k", "BASE", 0);
    tree.flush_active_memtable(0)?;
    tree.merge("k", "_A", 1);
    tree.flush_active_memtable(0)?;
    // Every version above the watermark: the filter rewrites the operand and
    // nothing folds it.
    tree.major_compact(64_000_000, 0)?;

    let newest = tree
        .get_internal_entry(b"k", SeqNo::MAX)?
        .expect("k is present");
    assert_eq!(
        newest.key.value_type,
        ValueType::MergeOperand,
        "still an operand"
    );
    let expected = [b"BASE".as_slice(), &[b'r'; BASE_LEN]].concat();
    assert_reads_single(&tree, &expected, "rewritten")?;
    Ok(())
}

/// `get`, `size_of` and a scan answer `expected` for `k`, the tree's only key.
fn assert_reads_single(tree: &AnyTree, expected: &[u8], what: &str) -> lsm_tree::Result<()> {
    assert_eq!(
        tree.get("k", SeqNo::MAX)?.as_deref(),
        Some(expected),
        "get, {what}"
    );
    assert_eq!(
        tree.size_of("k", SeqNo::MAX)?,
        Some(u32::try_from(expected.len()).expect("small value")),
        "size_of, {what}"
    );
    let scanned = collect(tree.iter(SeqNo::MAX, None))?;
    assert_eq!(scanned.len(), 1, "one key, {what}");
    assert_eq!(scanned[0].1.as_ref(), expected, "iter, {what}");
    Ok(())
}
