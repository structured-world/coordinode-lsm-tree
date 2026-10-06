use super::*;
use crate::TreeType;

fn empty_version(id: u64) -> Version {
    Version::new(id, TreeType::Standard)
}

/// A one-entry table `id` holding `key` at seqno 7; `recency: None` is a table
/// written before tables carried the key, which a flush and a compaction
/// output both look like.
#[expect(clippy::expect_used, reason = "test code")]
fn l0_table(
    fs: &Arc<dyn crate::fs::Fs>,
    id: TableId,
    key: &[u8],
    recency: Option<TableId>,
) -> crate::Result<Table> {
    use crate::{InternalValue, ValueType};

    let path = std::path::absolute(format!("/l0/{id}"))?;
    let mut writer =
        crate::table::Writer::new(path.clone(), id, 0, Arc::clone(fs))?.use_recency(recency);
    writer.write(InternalValue::from_components(
        key.to_vec(),
        id.to_le_bytes().to_vec(),
        7,
        ValueType::Value,
    ))?;
    let (_, checksum) = writer.finish()?.expect("the table is not empty");
    Table::recover(crate::table::RecoverParams::new(
        path,
        checksum,
        id,
        Arc::clone(fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
    ))
}

fn memfs() -> crate::Result<Arc<dyn crate::fs::Fs>> {
    use crate::fs::Fs;

    let fs = crate::fs::MemFs::new();
    fs.create_dir_all(&std::path::absolute("/l0")?)?;
    Ok(Arc::new(fs))
}

fn l0_with(runs: Vec<Vec<Table>>) -> Version {
    let mut levels = vec![Level::from_runs(
        runs.into_iter()
            .filter_map(Run::new)
            .map(Arc::new)
            .collect(),
    )];
    levels.extend((1..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()));
    Version::from_levels(
        0,
        TreeType::Standard,
        levels,
        BlobFileList::new(crate::HashMap::default()),
        FragmentationMap::default(),
    )
}

/// L0 table ids, run by run, front to back.
fn l0_ids(version: &Version) -> Vec<Vec<TableId>> {
    version
        .l0()
        .iter()
        .map(|run| run.iter().map(Table::id).collect())
        .collect()
}

/// A table without a recency key may be a compaction output whose id was
/// allocated before a newer flush with a lower id installed, so its id says
/// nothing about its age. The order a manifest persisted is the only record of
/// it, and an open keeps it: here the flush `1` ahead of the overlapping
/// recency-less table `2` that the live tree placed behind it.
#[test]
fn open_keeps_the_persisted_l0_order_of_a_table_without_a_recency_key() -> crate::Result<()> {
    use super::recovery::{RecoveredTable, Recovery, RecoveryStats};

    let fs = memfs()?;
    let flush = l0_table(&fs, 1, b"k", Some(1))?;
    let output = l0_table(&fs, 2, b"k", None)?;
    let entry = |table: &Table| RecoveredTable {
        id: table.id(),
        checksum: table.checksum(),
        global_seqno: 0,
    };
    let mut table_ids = vec![vec![vec![entry(&flush)], vec![entry(&output)]]];
    table_ids.resize(usize::from(DEFAULT_LEVEL_COUNT), Vec::new());
    let recovery = Recovery {
        tree_type: TreeType::Standard,
        snapshot_id: 0,
        curr_version_id: 0,
        table_ids,
        blob_file_ids: Vec::new(),
        gc_stats: FragmentationMap::default(),
        restrictions: crate::HashMap::default(),
        blob_restrictions: crate::HashMap::default(),
        retention_floor: 0,
        dicts: Vec::new(),
        stats: RecoveryStats::default(),
    };

    let version = Version::from_recovery(recovery, &[flush, output], &[])?;
    assert_eq!(l0_ids(&version), vec![vec![1], vec![2]]);
    Ok(())
}

/// The same pair under a flush: the new table goes in front, and the two
/// overlapping tables already in L0 keep their order.
#[test]
fn a_flush_keeps_the_l0_order_of_a_table_without_a_recency_key() -> crate::Result<()> {
    let fs = memfs()?;
    let flushed = l0_table(&fs, 1, b"k", Some(1))?;
    let output = l0_table(&fs, 2, b"k", None)?;
    let newer = l0_table(&fs, 3, b"z", Some(3))?;
    let version = l0_with(vec![vec![flushed], vec![output]]);

    let cmp = crate::comparator::default_comparator();
    let version = version.with_new_l0_run(&[newer], None, None, &TransformContext::new(&*cmp));
    let ids = l0_ids(&version);
    let at = |id: TableId| ids.iter().position(|run| run.contains(&id));
    assert!(
        at(1) < at(2),
        "the flush stays ahead of the table it overlaps: {ids:?}"
    );
    Ok(())
}

/// Installs the intra-L0 output `output` of inputs `inputs` into `version`.
fn merge_into_l0(version: &Version, inputs: &[TableId], output: Table) -> Version {
    let cmp = crate::comparator::default_comparator();
    version.with_merge(
        inputs,
        &[output],
        0,
        None,
        Vec::new(),
        &crate::HashSet::default(),
        &TransformContext::new(&*cmp),
    )
}

/// A flush landing while an intra-L0 compaction runs is newer than every
/// input, and may join an input's run when it does not overlap that input:
/// here `F` (9) shares `I`'s run and overlaps the other input `J`. The output
/// takes `J`'s data for `m`, so it must stay behind `F` rather than take the
/// place of `I`'s run.
#[test]
fn an_intra_l0_output_stays_behind_a_flush_that_landed_in_an_inputs_run() -> crate::Result<()> {
    let fs = memfs()?;
    let flush = l0_table(&fs, 9, b"m", Some(9))?;
    let i = l0_table(&fs, 2, b"a", Some(2))?;
    let j = l0_table(&fs, 1, b"m", Some(1))?;
    let version = l0_with(vec![vec![i, flush], vec![j]]);
    let output = l0_table(&fs, 10, b"m", Some(2))?;

    let version = merge_into_l0(&version, &[2, 1], output);
    assert_eq!(l0_ids(&version), vec![vec![9], vec![10]]);
    Ok(())
}

/// A heal rewrites one L0 table and its copy takes that table's place. Here
/// none of the tables carries a recency key, so nothing but their position
/// tells that `X` is newer than the healed `T` and `Y` older.
#[test]
fn a_healed_l0_table_keeps_its_place() -> crate::Result<()> {
    let fs = memfs()?;
    let x = l0_table(&fs, 5, b"k", None)?;
    let t = l0_table(&fs, 3, b"k", None)?;
    let y = l0_table(&fs, 1, b"k", None)?;
    let version = l0_with(vec![vec![x], vec![t], vec![y]]);
    let healed = l0_table(&fs, 9, b"k", Some(3))?;

    let version = merge_into_l0(&version, &[3], healed);
    assert_eq!(l0_ids(&version), vec![vec![5], vec![9], vec![1]]);
    Ok(())
}

/// No table ahead of an intra-L0 compaction's inputs overlaps them, so a table
/// left behind them that overlaps them is older and the output goes ahead of
/// it, whether or not it carries a recency key.
#[test]
fn an_intra_l0_output_goes_ahead_of_the_older_tables_behind_its_inputs() -> crate::Result<()> {
    let fs = memfs()?;
    let input = l0_table(&fs, 4, b"k", Some(4))?;
    let older = l0_table(&fs, 3, b"k", Some(3))?;
    let legacy = l0_table(&fs, 2, b"k", None)?;
    let version = l0_with(vec![vec![input], vec![older], vec![legacy]]);
    let output = l0_table(&fs, 5, b"k", Some(4))?;

    let version = merge_into_l0(&version, &[4], output);
    assert_eq!(l0_ids(&version), vec![vec![5], vec![3], vec![2]]);
    Ok(())
}

/// A snapshot stores a level's run count in one byte. A level wider than that
/// must be refused, not written with a wrapped count that no reopen can walk.
#[test]
fn encode_into_refuses_a_level_with_more_than_255_runs() -> crate::Result<()> {
    use crate::{AbstractTree, Config, SequenceNumberCounter, fs::StdFs};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..256u64 {
        let key = format!("k{i:05}");
        // Rewriting `zzz` on every flush makes every table overlap every
        // other, so each one stays its own L0 run.
        tree.insert(key.as_bytes(), key.as_bytes(), 2 * i + 1);
        tree.insert(b"zzz", key.as_bytes(), 2 * i + 2);
        tree.flush_active_memtable(2 * i + 2)?;
    }
    let version = tree.current_version();
    assert_eq!(version.l0().run_count(), 256);
    assert!(!version.fits_snapshot());

    let out = tempfile::tempdir()?;
    let mut writer = crate::manifest_blocks::writer::ManifestArchiveWriter::create(
        &out.path().join("v0"),
        &StdFs,
        Arc::new(crate::runtime_config::RuntimeConfig::default()),
        None,
        crate::fs::SyncMode::Normal,
    )?;
    let Err(crate::Error::Io(err)) = version.encode_into(&mut writer, "default") else {
        panic!("a level of 256 runs must be refused");
    };
    assert_eq!(err.kind(), crate::io::ErrorKind::InvalidInput);
    Ok(())
}

#[test]
fn set_retention_floor_below_the_current_one_is_a_no_op() {
    let mut v = empty_version(1).with_retention_floor(40);
    v.set_retention_floor(10);
    assert_eq!(v.retention_floor(), 40, "the floor never goes down");
    v.set_retention_floor(40);
    assert_eq!(v.retention_floor(), 40, "equal is not above");
}

#[test]
fn set_retention_floor_on_a_sole_owner_writes_through() {
    let mut v = empty_version(1);
    let before = Arc::as_ptr(&v.inner);

    v.set_retention_floor(40);

    assert_eq!(v.retention_floor(), 40);
    assert!(
        core::ptr::eq(before, Arc::as_ptr(&v.inner)),
        "a sole owner keeps its allocation instead of rebuilding it",
    );
}

#[test]
fn set_retention_floor_on_a_shared_handle_leaves_the_other_owner_alone() {
    let mut v = empty_version(1);
    // A second handle on the same allocation, which is what an install whose
    // mutator returned the prior version untouched holds.
    let shared = v.clone();

    v.set_retention_floor(40);

    assert_eq!(
        v.retention_floor(),
        40,
        "the raised handle carries the floor"
    );
    assert_eq!(
        shared.retention_floor(),
        0,
        "the other owner must not see a floor it never recorded",
    );
    assert!(
        !core::ptr::eq(Arc::as_ptr(&v.inner), Arc::as_ptr(&shared.inner)),
        "a shared handle rebuilds rather than mutating in place",
    );
}
