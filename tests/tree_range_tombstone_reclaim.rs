use lsm_tree::{
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter,
    compaction::{CompactionAction, CompactionResult, Fifo, Leveled},
    get_tmp_folder,
};
use std::sync::Arc;

fn open(folder: &tempfile::TempDir) -> lsm_tree::Result<AnyTree> {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
}

/// One table per key `a..=e`, written at seqnos 1 to 5.
fn populate_tables(tree: &AnyTree) -> lsm_tree::Result<()> {
    for (seqno, key) in (1..).zip('a'..='e') {
        tree.insert([key as u8], "", seqno);
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// A compaction at `watermark` whose strategy drops nothing, so the only work
/// it can do is reclaiming the tables range tombstones delete.
fn reclaim(tree: &AnyTree, watermark: SeqNo) -> lsm_tree::Result<CompactionResult> {
    tree.compact(Arc::new(Fifo::new(u64::MAX, None)), watermark)
}

/// The tables a flushed tombstone spans whole go without being read, and the
/// keys it deletes stay deleted while the keys past it stay.
#[test]
fn remove_range_reclaims_the_tables_it_spans_whole() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    populate_tables(&tree)?;
    tree.remove_range("a", "d", 6);
    tree.flush_active_memtable(0)?;
    assert_eq!(6, tree.table_count());

    let result = reclaim(&tree, 7)?;

    assert_eq!(CompactionAction::Dropped, result.action);
    assert_eq!(3, result.tables_in, "the tables of a, b and c go");
    assert_eq!(0, result.work.measured.read, "a reclaim reads nothing");
    assert_eq!(3, tree.table_count(), "d, e and the tombstone stay");
    for key in ["a", "b", "c"] {
        assert!(!tree.contains_key(key, SeqNo::MAX)?, "{key} came back");
    }
    for key in ["d", "e"] {
        assert!(tree.contains_key(key, SeqNo::MAX)?, "{key} was lost");
    }
    Ok(())
}

/// A tombstone inside one table deletes exactly its keys at once; the table
/// holds others, so it stays for a merge to rewrite.
#[test]
fn remove_range_deletes_inside_a_table_it_does_not_span() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    for key in ["a", "b", "c", "d", "e"] {
        tree.insert(key, "", 1);
    }
    tree.flush_active_memtable(0)?;
    tree.remove_range("b", "d", 2);
    tree.flush_active_memtable(0)?;

    let result = reclaim(&tree, 3)?;

    assert_eq!(CompactionAction::Nothing, result.action);
    assert_eq!(2, tree.table_count());
    for key in ["b", "c"] {
        assert!(!tree.contains_key(key, SeqNo::MAX)?, "{key} came back");
    }
    for key in ["a", "d", "e"] {
        assert!(tree.contains_key(key, SeqNo::MAX)?, "{key} was lost");
    }
    Ok(())
}

/// A tombstone still in the memtable is not durable: a drop it justified
/// could outlive it through a crash. It deletes at once but reclaims nothing.
#[test]
fn remove_range_reclaims_nothing_before_the_tombstone_is_flushed() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    populate_tables(&tree)?;
    tree.remove_range("a", "d", 6);

    let result = reclaim(&tree, 7)?;

    assert_eq!(CompactionAction::Nothing, result.action);
    assert_eq!(5, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    Ok(())
}

/// The oldest snapshot still served reads at the watermark; while the
/// tombstone is not below it, that snapshot reads the tables, so they stay.
#[test]
fn remove_range_reclaims_nothing_while_the_watermark_is_not_above_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    populate_tables(&tree)?;
    tree.remove_range("a", "d", 6);
    tree.flush_active_memtable(0)?;

    let result = reclaim(&tree, 6)?;

    assert_eq!(CompactionAction::Nothing, result.action);
    assert_eq!(6, tree.table_count());
    assert!(tree.contains_key("a", 6)?, "a snapshot below the tombstone");
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    Ok(())
}

/// Reclaiming a newer table under the tombstone must not uncover the older
/// version a table reaching past the tombstone keeps: the tombstone stays
/// while that table overlaps it, and a later bottom merge applies it.
#[test]
fn remove_range_keeps_older_versions_deleted_after_reclaim() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    tree.insert("k", "old", 1);
    tree.insert("z", "outside", 2);
    tree.flush_active_memtable(0)?;
    tree.insert("k", "new", 3);
    tree.flush_active_memtable(0)?;
    tree.remove_range("a", "m", 4);
    tree.flush_active_memtable(0)?;

    let result = reclaim(&tree, 5)?;

    assert_eq!(1, result.tables_in, "only the table of k = new goes");
    assert_eq!(2, tree.table_count());
    assert_eq!(None, tree.get("k", SeqNo::MAX)?, "an older k came back");
    assert_eq!(
        Some("outside".as_bytes().into()),
        tree.get("z", SeqNo::MAX)?
    );

    tree.major_compact(u64::MAX, 5)?;

    assert_eq!(None, tree.get("k", SeqNo::MAX)?, "an older k came back");
    assert_eq!(
        Some("outside".as_bytes().into()),
        tree.get("z", SeqNo::MAX)?
    );
    Ok(())
}

/// A reclaim refuses exactly the snapshots that could still have read a
/// dropped table: those up to the tombstone.
#[test]
fn remove_range_reclaim_refuses_snapshots_up_to_the_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    populate_tables(&tree)?;
    tree.remove_range("a", "d", 6);
    tree.flush_active_memtable(0)?;

    reclaim(&tree, 10)?;

    assert_eq!(6, tree.retention_floor());
    assert!(matches!(
        tree.get("d", 6),
        Err(lsm_tree::Error::SnapshotBelowRetention { .. })
    ));
    assert!(tree.contains_key("d", 7)?);
    Ok(())
}

/// The drop is one manifest edit: a reopen sees it whole.
#[test]
fn remove_range_reclaim_survives_a_reopen() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    {
        let tree = open(&folder)?;
        populate_tables(&tree)?;
        tree.remove_range("a", "d", 6);
        tree.flush_active_memtable(0)?;
        reclaim(&tree, 7)?;
    }

    let tree = open(&folder)?;

    assert_eq!(3, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    Ok(())
}

/// Reverses the byte order of keys.
struct ReverseComparator;

impl lsm_tree::UserComparator for ReverseComparator {
    fn name(&self) -> &'static str {
        "reverse"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
}

/// Spans and overlaps follow the tree's comparator: under a reversed order
/// the tombstone `[d, a)` holds `d`, `c` and `b`, whose tables go, while in
/// byte order it would be empty.
#[test]
fn remove_range_reclaims_in_the_trees_key_order() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .comparator(Arc::new(ReverseComparator))
    .open()?;
    populate_tables(&tree)?;
    tree.remove_range("d", "a", 6);
    tree.flush_active_memtable(0)?;

    let result = reclaim(&tree, 7)?;

    assert_eq!(3, result.tables_in, "the tables of d, c and b go");
    for key in ["b", "c", "d"] {
        assert!(!tree.contains_key(key, SeqNo::MAX)?, "{key} came back");
    }
    for key in ["a", "e"] {
        assert!(tree.contains_key(key, SeqNo::MAX)?, "{key} was lost");
    }
    Ok(())
}

/// Four overlapping L0 tables of `j` and `k`, written at seqnos 1 to 4.
fn overlapping_l0(tree: &AnyTree) -> lsm_tree::Result<()> {
    for seqno in 1..=4 {
        tree.insert("j", "v", seqno);
        tree.insert("k", "v", seqno);
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

/// The reclaim runs before the strategy chooses: a leveled compaction that
/// would merge the covered L0 tables reads none of them.
#[test]
fn remove_range_reclaim_runs_before_the_strategy_chooses() -> lsm_tree::Result<()> {
    let premise_folder = get_tmp_folder();
    let premise = open(&premise_folder)?;
    overlapping_l0(&premise)?;
    let merged = premise.compact(Arc::new(Leveled::default()), 6)?;
    assert!(
        merged.work.measured.read > 0,
        "premise: leveled merges these tables, reading them"
    );

    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    overlapping_l0(&tree)?;
    tree.remove_range("a", "m", 5);
    tree.flush_active_memtable(0)?;

    let result = tree.compact(Arc::new(Leveled::default()), 6)?;

    assert_eq!(0, result.work.measured.read, "a covered table was read");
    assert_eq!(1, tree.table_count(), "only the tombstone stays");
    assert_eq!(None, tree.get("k", SeqNo::MAX)?);
    Ok(())
}
