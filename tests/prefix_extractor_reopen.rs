// A table's filter indexes the prefixes of the extractor it was written with.
// Reopened under another extractor, or under one where there was none, those
// tables must neither drop out of a prefix scan nor be judged corrupt.

use lsm_tree::{AbstractTree, AnyTree, Config, PrefixExtractor, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

/// Prefixes at each ':' boundary: `a/b:c` yields `a/b:`.
struct ColonPrefix;

impl PrefixExtractor for ColonPrefix {
    fn name(&self) -> &'static str {
        "colon-separated"
    }

    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

/// Prefixes at each '/' boundary: `a/b:c` yields `a/`.
struct SlashPrefix;

impl PrefixExtractor for SlashPrefix {
    fn name(&self) -> &'static str {
        "slash-separated"
    }

    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b'/')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

fn config(folder: &tempfile::TempDir, extractor: Option<Arc<dyn PrefixExtractor>>) -> Config {
    let config = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    match extractor {
        Some(extractor) => config.prefix_extractor(extractor),
        None => config,
    }
}

/// Two tables of 20 keys each: one of `user/` keys, and one of `aaa/` and
/// `zzz/` keys whose key range spans `user/`, so only its filter can skip it
/// on a scan of a `user/` prefix.
fn write(folder: &tempfile::TempDir, extractor: Option<Arc<dyn PrefixExtractor>>) {
    let tree = config(folder, extractor).open().expect("open");
    let mut seqno = 0;
    for groups in [&["user"][..], &["aaa", "zzz"][..]] {
        for group in groups {
            for i in 0..20 / groups.len() {
                tree.insert(format!("{group}/{i:02}:name"), "v", seqno);
                seqno += 1;
            }
        }
        tree.flush_active_memtable(0).expect("flush");
    }
}

fn keys(
    rows: impl Iterator<Item = lsm_tree::Result<(lsm_tree::UserKey, lsm_tree::UserValue)>>,
) -> Vec<Vec<u8>> {
    rows.map(|row| row.expect("row").0.to_vec()).collect()
}

/// The keys a prefix scan of `prefix` returns, and the keys the range over the
/// same prefix returns.
fn prefix_and_range(tree: &AnyTree, prefix: &str) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let by_prefix = keys(
        tree.prefix(prefix, SeqNo::MAX, None)
            .map(lsm_tree::Guard::into_inner),
    );
    let mut end = prefix.as_bytes().to_vec();
    *end.last_mut().expect("a prefix") += 1;
    let by_range = keys(
        tree.range(prefix.as_bytes().to_vec()..end, SeqNo::MAX, None)
            .map(lsm_tree::Guard::into_inner),
    );
    (by_prefix, by_range)
}

#[test]
fn a_prefix_scan_after_adding_an_extractor_returns_every_old_row() {
    let folder = tempfile::tempdir().expect("tempdir");
    write(&folder, None);

    let tree = config(&folder, Some(Arc::new(ColonPrefix)))
        .open()
        .expect("reopen");
    let (by_prefix, by_range) = prefix_and_range(&tree, "user/03:");
    assert_eq!(by_range.len(), 1);
    assert_eq!(by_prefix, by_range, "the prefix scan dropped rows");
}

#[test]
fn a_prefix_scan_after_changing_the_extractor_returns_every_old_row() {
    let folder = tempfile::tempdir().expect("tempdir");
    write(&folder, Some(Arc::new(ColonPrefix)));

    let tree = config(&folder, Some(Arc::new(SlashPrefix)))
        .open()
        .expect("reopen");
    let (by_prefix, by_range) = prefix_and_range(&tree, "user/");
    assert_eq!(by_range.len(), 20);
    assert_eq!(by_prefix, by_range, "the prefix scan dropped rows");
}

#[cfg(feature = "metrics")]
#[test]
fn a_prefix_scan_under_the_same_extractor_still_skips_tables() {
    let folder = tempfile::tempdir().expect("tempdir");
    write(&folder, Some(Arc::new(SlashPrefix)));

    let tree = config(&folder, Some(Arc::new(SlashPrefix)))
        .open()
        .expect("reopen");
    let before = tree.metrics().prefix_bloom_skips();
    let (by_prefix, by_range) = prefix_and_range(&tree, "user/");
    assert_eq!(by_range.len(), 20);
    assert_eq!(by_prefix, by_range);
    assert!(
        tree.metrics().prefix_bloom_skips() > before,
        "the table holding no user/ key was skipped by its filter"
    );
}

#[test]
fn repair_after_adding_an_extractor_keeps_every_table() {
    let folder = tempfile::tempdir().expect("tempdir");
    write(&folder, None);

    let config = config(&folder, Some(Arc::new(ColonPrefix)));
    let report = config.repair().expect("repair");
    assert_eq!(
        report.unreadable, 0,
        "healthy tables were judged corrupt: {:?}",
        report.unreadable_files
    );
    let tree = config.open().expect("open");
    let all = keys(tree.iter(SeqNo::MAX, None).map(lsm_tree::Guard::into_inner));
    assert_eq!(all.len(), 40, "rows lost to the repair");
}
