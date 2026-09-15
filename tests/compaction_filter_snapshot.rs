use lsm_tree::compaction::filter::{
    CompactionFilter, Context as CompactionFilterContext, Factory, ItemAccessor, Verdict,
};
use lsm_tree::{AbstractTree, Guard, SeqNo, SequenceNumberCounter, get_tmp_folder};
use std::sync::Arc;
use test_log::test;

struct NukeFilter;

impl CompactionFilter for NukeFilter {
    fn filter_item(
        &mut self,
        _: ItemAccessor<'_>,
        _ctx: &CompactionFilterContext,
    ) -> lsm_tree::Result<Verdict> {
        // data? what data?
        Ok(Verdict::Remove)
    }
}

struct NukeFilterFactory;

impl Factory for NukeFilterFactory {
    fn name(&self) -> &str {
        "Nuke"
    }

    fn make_filter(&self, _ctx: &CompactionFilterContext) -> Box<dyn CompactionFilter> {
        Box::new(NukeFilter)
    }
}

#[test]
fn compaction_filter_removal_refuses_older_snapshots_but_not_an_open_reader() -> lsm_tree::Result<()>
{
    // A filter removes rows regardless of any watermark, so its install raises
    // the retention floor to its own seqno: a new read at an older snapshot is
    // refused rather than answered without the rows. A reader that resolved
    // its version before the compaction keeps reading it.
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let config = lsm_tree::Config::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .with_compaction_filter_factory(Some(Arc::new(NukeFilterFactory)));
    let tree = config.open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());
    let mut reader = tree.iter(snapshot_seqno, None);

    tree.major_compact(u64::MAX, 0)?;

    assert!(matches!(
        tree.get("a", snapshot_seqno),
        Err(lsm_tree::Error::SnapshotBelowRetention { .. })
    ));
    assert!(tree.get("a", SeqNo::MAX)?.is_none());

    let (key, _) = reader
        .next()
        .expect("the reader's first row")
        .into_inner()?;
    assert_eq!(&*key, b"a", "the open reader still sees the removed row");

    Ok(())
}
