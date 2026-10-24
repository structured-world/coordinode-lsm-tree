use super::*;
use crate::{InternalValue, SeqNo, Table, ValueType, fs::Fs, range_tombstone::RangeTombstone};
use std::sync::Arc;
use test_log::test;

/// A table `id` holding `keys` at `seqno` and the range tombstones `rts`
/// (`start`, `end`, `seqno`), read with `global_seqno` as its base.
#[expect(clippy::expect_used, reason = "test code")]
fn table(
    fs: &Arc<dyn Fs>,
    id: TableId,
    keys: &[&str],
    seqno: SeqNo,
    rts: &[(&str, &str, SeqNo)],
    global_seqno: SeqNo,
) -> crate::Result<Table> {
    let path = std::path::absolute(format!("/covered/{id}"))?;
    let mut writer = crate::table::Writer::new(path.clone(), id, 0, Arc::clone(fs))?;
    for &(start, end, rt_seqno) in rts {
        writer.write_range_tombstone(RangeTombstone::new(start.into(), end.into(), rt_seqno));
    }
    for key in keys {
        writer.write(InternalValue::from_components(
            *key,
            "v",
            seqno,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = writer.finish()?.expect("the table is not empty");
    let mut params = crate::table::RecoverParams::new(
        path,
        checksum,
        id,
        Arc::clone(fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
    );
    params.global_seqno = global_seqno;
    Table::recover(params)
}

fn fs() -> crate::Result<Arc<dyn Fs>> {
    let fs = crate::fs::MemFs::new();
    fs.create_dir_all(&std::path::absolute("/covered")?)?;
    Ok(Arc::new(fs))
}

/// A version whose L0 holds one run per table, newest first.
fn version(tables: Vec<Table>) -> Version {
    use crate::version::{DEFAULT_LEVEL_COUNT, Level, Run};

    let mut levels = vec![Level::from_runs(
        tables
            .into_iter()
            .filter_map(|table| Run::new(vec![table]))
            .map(Arc::new)
            .collect(),
    )];
    levels.extend((1..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()));
    Version::from_levels(
        0,
        crate::TreeType::Standard,
        levels,
        crate::version::BlobFileList::new(crate::HashMap::default()),
        crate::blob_tree::FragmentationMap::default(),
    )
}

/// The tables the rule drops from `version` at `watermark`, and the watermark
/// the drop collects below; `None` when it drops nothing.
fn covered(version: &Version, watermark: SeqNo) -> Option<(Vec<TableId>, SeqNo)> {
    covered_with(version, watermark, &HiddenSet::default())
}

#[expect(clippy::panic, reason = "test code")]
fn covered_with(
    version: &Version,
    watermark: SeqNo,
    hidden: &HiddenSet,
) -> Option<(Vec<TableId>, SeqNo)> {
    let cmp = crate::comparator::default_comparator();
    covered_tables(version, watermark, hidden, cmp.as_ref()).map(|(ids, effect)| match effect {
        RetentionEffect::GcBelow(below) => (ids, below),
        other => panic!("a covered drop collects below a watermark, not {other:?}"),
    })
}

/// A tombstone of another table, below the watermark and newer than every
/// row of a table it spans whole, drops that table; snapshots up to the
/// tombstone could still read it.
#[test]
fn covered_drops_an_older_table_a_flushed_tombstone_spans() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        table(&fs, 1, &["b", "c"], 3, &[], 0)?,
    ]);

    assert_eq!(Some((vec![1], 10)), covered(&version, 10));
    Ok(())
}

/// A tombstone at or above the watermark is not seen by the oldest snapshot
/// still served, which reads the table: nothing goes.
#[test]
fn covered_keeps_a_table_while_its_tombstone_is_not_below_the_watermark() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        table(&fs, 1, &["b", "c"], 3, &[], 0)?,
    ]);

    assert_eq!(None, covered(&version, 9));
    Ok(())
}

/// A row at or above the tombstone's seqno outlives it, so a table holding
/// one stays.
#[test]
fn covered_keeps_a_table_not_older_than_the_tombstone() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        table(&fs, 1, &["b", "c"], 9, &[], 0)?,
    ]);

    assert_eq!(None, covered(&version, 20));
    Ok(())
}

/// A table reaching past the tombstone holds rows it does not delete.
#[test]
fn covered_keeps_a_table_reaching_past_the_tombstone() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        table(&fs, 1, &["b", "m"], 3, &[], 0)?,
    ]);

    assert_eq!(None, covered(&version, 20));
    Ok(())
}

/// A table a running compaction holds is that compaction's to rewrite.
#[test]
fn covered_keeps_a_hidden_table() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        table(&fs, 1, &["b", "c"], 3, &[], 0)?,
    ]);
    let mut hidden = HiddenSet::default();
    hidden.hide([1]);

    assert_eq!(None, covered_with(&version, 20, &hidden));
    Ok(())
}

/// A table whose bounds overflow the seqno space once its base is added (a
/// damaged manifest can pair them) has no provable age, so no tombstone
/// proves it older.
#[test]
fn covered_keeps_a_table_whose_seqno_range_overflows() -> crate::Result<()> {
    let fs = fs()?;
    let overflowing = table(&fs, 1, &["b"], 7, &[], SeqNo::MAX - 4)?;
    assert_eq!(
        None,
        overflowing.seqno_range(),
        "premise: the range overflows"
    );
    let version = version(vec![
        table(&fs, 2, &["z"], 10, &[("a", "m", 9)], 0)?,
        overflowing,
    ]);

    assert_eq!(None, covered(&version, 20));
    Ok(())
}

/// A table's own tombstones can reach past its key range (a compaction cuts
/// them at the next output's first key without widening the range up to
/// it), and dropping the table drops them: a newer tombstone over its keys
/// but not over those spans would uncover what they hid in older tables.
#[test]
fn covered_keeps_a_table_whose_own_tombstone_reaches_past_the_cover() -> crate::Result<()> {
    let fs = fs()?;
    let reaching = table(&fs, 1, &["b"], 1, &[("b", "q", 2)], 0)?;
    assert_eq!(
        reaching.key_range().max().as_ref(),
        b"b",
        "premise: the own tombstone reaches past the key range"
    );
    let version = version(vec![
        table(&fs, 2, &["z"], 6, &[("a", "m", 5)], 0)?,
        reaching,
    ]);

    assert_eq!(None, covered(&version, 20));
    Ok(())
}

/// A point read consults a table's range tombstones wherever they reach, not
/// only inside its key range: a compaction can cut a tombstone at the next
/// output's first key without widening the range up to it, and a scan
/// already honours such a tombstone, so a point read that skipped it would
/// return a key the scan reports deleted.
#[test]
fn point_read_honours_a_tombstone_past_its_tables_key_range() -> crate::Result<()> {
    use crate::{memtable::Memtable, version::SuperVersion};

    let fs = fs()?;
    let reaching = table(&fs, 1, &["b"], 3, &[("b", "q", 5)], 0)?;
    assert_eq!(
        reaching.key_range().max().as_ref(),
        b"b",
        "premise: the tombstone reaches past the key range"
    );
    let comparator = crate::comparator::default_comparator();
    let super_version = SuperVersion {
        active_memtable: Arc::new(Memtable::new(0, comparator.clone())),
        sealed_memtables: Arc::default(),
        version: version(vec![reaching, table(&fs, 2, &["o"], 1, &[], 0)?]),
        seqno: 0,
    };

    assert!(
        crate::Tree::is_suppressed_by_range_tombstones(
            &super_version,
            b"o",
            1,
            SeqNo::MAX,
            comparator.as_ref(),
        ),
        "o lies under the tombstone [b, q)@5 and is older than it"
    );
    Ok(())
}

/// A table's own tombstone never drops it: its seqno counts in the table's
/// bounds, so the table is never older than it.
#[test]
fn covered_never_drops_a_table_by_its_own_tombstone() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![table(&fs, 1, &["b"], 3, &[("a", "m", 9)], 0)?]);

    assert_eq!(None, covered(&version, 20));
    Ok(())
}

/// A dropped table may hold the tombstone that covers another, when a newer
/// tombstone spans it whole: the newer one spans everything the older one
/// did. The newest covering tombstone's table stays, and the floor is the
/// newest tombstone any dropped table needs, here the one dropping table 2.
#[test]
fn covered_drops_a_tombstone_holder_a_newer_tombstone_spans() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 3, &["z"], 6, &[("a", "y", 5)], 0)?,
        table(&fs, 2, &["c"], 2, &[("a", "f", 3)], 0)?,
        table(&fs, 1, &["b"], 1, &[], 0)?,
    ]);

    assert_eq!(Some((vec![1, 2], 6)), covered(&version, 20));
    Ok(())
}

/// Each dropped table owes a refusal only up to the OLDEST tombstone covering
/// it, since a snapshot past that one already reads it as deleted.
#[test]
fn covered_floor_takes_the_oldest_tombstone_covering_each_table() -> crate::Result<()> {
    let fs = fs()?;
    let version = version(vec![
        table(&fs, 3, &["z"], 9, &[("a", "m", 8)], 0)?,
        table(&fs, 2, &["y"], 5, &[("a", "m", 4)], 0)?,
        table(&fs, 1, &["b"], 1, &[], 0)?,
    ]);

    assert_eq!(Some((vec![1], 5)), covered(&version, 20));
    Ok(())
}
