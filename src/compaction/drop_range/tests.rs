use super::*;
use crate::{TableId, compaction::CompactionStrategy, fs::Fs};
use std::sync::Arc;
use test_log::test;

/// A table `id` in `fs` holding `keys` (sorted) at local seqno 7, read with
/// `global_seqno` as the base the manifest gives it.
#[expect(clippy::expect_used, reason = "test code")]
fn table_at(
    fs: &Arc<dyn Fs>,
    id: TableId,
    keys: &[&str],
    global_seqno: crate::SeqNo,
) -> crate::Result<Table> {
    use crate::{InternalValue, ValueType};

    let path = std::path::absolute(format!("/drop/{id}"))?;
    let mut writer = crate::table::Writer::new(path.clone(), id, 0, Arc::clone(fs))?;
    for key in keys {
        writer.write(InternalValue::from_components(
            *key,
            "v",
            7,
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

/// A version whose L0 holds one run per table, newest first.
fn version_with_l0(tables: Vec<Table>) -> Version {
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

/// A table whose bounds overflow the seqno space once its base is added (a
/// damaged manifest can pair them) proves nothing about its age, so it stays
/// while an older-looking table over its keys reaches past the bounds. Adding
/// the base unchecked panics with overflow checks, and in a release build wraps
/// to a tiny seqno that reads as older than the other table, dropping it and
/// uncovering that table's version of `k`.
#[test]
fn drop_range_keeps_a_table_whose_seqno_range_overflows() -> crate::Result<()> {
    let fs: Arc<dyn Fs> = {
        let fs = crate::fs::MemFs::new();
        fs.create_dir_all(&std::path::absolute("/drop")?)?;
        Arc::new(fs)
    };
    let inside = table_at(&fs, 1, &["k"], crate::SeqNo::MAX - 4)?;
    let past_the_bounds = table_at(&fs, 2, &["k", "z"], 0)?;
    assert_eq!(None, inside.seqno_range(), "premise: the range overflows");

    let version = version_with_l0(vec![inside, past_the_bounds]);
    let strategy = Strategy::new(OwnedBounds {
        start: Bound::Included("a".into()),
        end: Bound::Excluded("m".into()),
    });
    let choice = strategy.choose(&version, &Config::default(), &CompactionState::default());

    assert!(
        matches!(choice, Choice::DoNothing),
        "a table of unknown age was dropped over an older version of its key"
    );
    Ok(())
}
