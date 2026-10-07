use super::*;
use crate::TreeType;

fn empty_version(id: u64) -> Version {
    Version::new(id, TreeType::Standard)
}

/// A one-entry table `id` holding `key`, in the in-memory filesystem `fs`.
#[expect(clippy::expect_used, reason = "test code")]
fn mem_table(fs: &Arc<dyn crate::fs::Fs>, id: TableId, key: &[u8]) -> crate::Result<Table> {
    use crate::{InternalValue, ValueType};

    let path = std::path::absolute(format!("/tables/{id}"))?;
    let mut writer = crate::table::Writer::new(path.clone(), id, 0, Arc::clone(fs))?;
    writer.write(InternalValue::from_components(
        key.to_vec(),
        b"v".to_vec(),
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

/// A change that leaves a level's tables alone leaves its layout alone: a drop
/// from L1 compares no key of a wide L0, rather than laying all of L0 out
/// again on every install.
#[test]
fn a_drop_below_l0_compares_no_key_of_l0() -> crate::Result<()> {
    use crate::fs::Fs;
    use core::sync::atomic::{AtomicU64, Ordering};

    /// Counts the keys it compares.
    struct Counting(AtomicU64);
    impl UserComparator for Counting {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            self.0.fetch_add(1, Ordering::Relaxed);
            a.cmp(b)
        }
    }

    let mem = crate::fs::MemFs::new();
    mem.create_dir_all(&std::path::absolute("/tables")?)?;
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(mem);
    let l0: Vec<Table> = (0..64u64)
        .map(|id| mem_table(&fs, id, format!("k{id:04}").as_bytes()))
        .collect::<crate::Result<_>>()?;
    let l1 = mem_table(&fs, 100, b"z")?;
    let mut levels = vec![
        Level::from_runs(Run::new(l0).into_iter().map(Arc::new).collect()),
        Level::from_runs(Run::new(vec![l1]).into_iter().map(Arc::new).collect()),
    ];
    levels.extend((2..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()));
    let version = Version::from_levels(
        0,
        TreeType::Standard,
        levels,
        BlobFileList::new(crate::HashMap::default()),
        FragmentationMap::default(),
    );

    let cmp = Counting(AtomicU64::new(0));
    let dropped = version.with_dropped(
        &[100],
        &mut Vec::new(),
        &|_| false,
        &TransformContext::new(&cmp),
    )?;
    assert_eq!(cmp.0.load(Ordering::Relaxed), 0, "L0 was laid out again");
    assert_eq!(
        dropped.l0().iter().map(|run| run.len()).collect::<Vec<_>>(),
        vec![64]
    );
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
