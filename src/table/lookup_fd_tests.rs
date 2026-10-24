//! A point lookup that reads several blocks of one table from the file takes
//! the table's descriptor from the descriptor cache once, not once per block.

use super::{Table, Writer};
use crate::table::RecoverParams;
use crate::{Cache, DescriptorTable, InternalValue, SeqNo, ValueType, fs::StdFs, hash::hash64};
use alloc::sync::Arc;
use core::sync::atomic::Ordering::Relaxed;
use tempfile::tempdir;
use test_log::test;

/// Writes keys `key000000..key000998` (even numbers) into one table shaped by
/// `shape`, and opens it with nothing pinned and a block cache that keeps
/// nothing, so every block a lookup needs is read from the file.
fn cold_table(dir: &std::path::Path, shape: impl Fn(Writer) -> Writer) -> crate::Result<Table> {
    let file = dir.join("table");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut writer = shape(Writer::new(file.clone(), 0, 0, Arc::clone(&fs))?);
    for i in (0u32..1000).step_by(2) {
        writer.write(InternalValue::from_components(
            format!("key{i:06}").into_bytes(),
            b"v".to_vec(),
            1,
            ValueType::Value,
        ))?;
    }
    let (_, checksum) = writer.finish()?.expect("the keys make a table");
    let mut params = RecoverParams::new(
        file,
        checksum,
        0,
        fs,
        crate::comparator::default_comparator(),
        Arc::new(Cache::with_capacity_bytes(0)),
    );
    params.descriptor_table = Some(Arc::new(DescriptorTable::new(10)));
    params.pin_filter = false;
    params.pin_index = false;
    Table::recover(params)
}

/// Descriptor-cache accesses the table has counted so far, through the
/// counts the metrics publish.
fn descriptor_accesses(table: &Table) -> usize {
    table.metrics.table_file_cached_count() + table.metrics.table_file_opened_count()
}

/// The published counts tell a descriptor taken from the cache from one that
/// had to be opened: the first read of a table opens its file, the next one
/// finds it cached.
#[test]
fn descriptor_counts_split_cache_hits_from_opens() -> crate::Result<()> {
    let dir = tempdir()?;
    let table = cold_table(dir.path(), |w| w)?;
    let (cached, opened) = (
        table.metrics.table_file_cached_count(),
        table.metrics.table_file_opened_count(),
    );
    let key = b"key000500";
    assert!(table.get(key, SeqNo::MAX, hash64(key))?.is_some());
    assert_eq!(
        table.metrics.table_file_opened_count() - opened,
        1,
        "opened"
    );
    assert_eq!(
        table.metrics.table_file_cached_count() - cached,
        0,
        "cached"
    );
    assert!(table.get(key, SeqNo::MAX, hash64(key))?.is_some());
    assert_eq!(
        table.metrics.table_file_opened_count() - opened,
        1,
        "opened"
    );
    assert_eq!(
        table.metrics.table_file_cached_count() - cached,
        1,
        "cached"
    );
    Ok(())
}

/// A table shape: its name and how the writer is set up.
type Shape = (&'static str, fn(Writer) -> Writer);

/// Table shapes whose point lookups load a filter, index and data block (or
/// their partitions) from the file.
fn shapes() -> Vec<Shape> {
    let mut shapes: Vec<Shape> = vec![
        ("monolithic", |w| w),
        ("partitioned", |w| {
            w.use_data_block_size(64)
                .use_partitioned_filter()
                .use_filter_partition_size(64)
                .use_partitioned_index()
                .use_index_partition_size(64)
        }),
    ];
    #[cfg(feature = "columnar")]
    shapes.push(("columnar", |w| w.use_columnar(true)));
    shapes
}

#[test]
fn cold_point_reads_take_the_descriptor_once_per_lookup() -> crate::Result<()> {
    let key = b"key000500";
    let hash = hash64(key);
    for (name, shape) in shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;

        let before = descriptor_accesses(&table);
        let blocks = table.metrics.block_load_io_count();
        assert!(table.get(key, SeqNo::MAX, hash)?.is_some(), "{name}: get");
        assert!(
            table.metrics.block_load_io_count() - blocks >= 3,
            "{name}: the lookup did not read filter, index and data from the file"
        );
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: get");

        let before = descriptor_accesses(&table);
        assert!(
            table.get_value(key, SeqNo::MAX, hash)?.is_some(),
            "{name}: get_value"
        );
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: get_value");

        let before = descriptor_accesses(&table);
        assert!(
            table.get_with_block(key, SeqNo::MAX, hash)?.is_some(),
            "{name}: get_with_block"
        );
        assert_eq!(
            descriptor_accesses(&table) - before,
            1,
            "{name}: get_with_block"
        );

        // A batch over one table is one lookup, however many blocks it reads.
        let keys: Vec<(&[u8], u64)> = [b"key000002".as_slice(), b"key000500", b"key000998"]
            .into_iter()
            .map(|k| (k, hash64(k)))
            .collect();
        let before = descriptor_accesses(&table);
        let found = table.batch_get(&keys, SeqNo::MAX)?;
        assert!(found.iter().all(Option::is_some), "{name}: batch_get");
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: batch_get");
    }
    Ok(())
}

/// Table shapes whose scans read many data blocks (or row groups) and, when
/// partitioned, many index partitions from the file.
fn scan_shapes() -> Vec<Shape> {
    let mut shapes: Vec<Shape> = vec![
        ("monolithic", |w| w.use_data_block_size(64)),
        ("partitioned", |w| {
            w.use_data_block_size(64)
                .use_partitioned_index()
                .use_index_partition_size(64)
        }),
    ];
    #[cfg(feature = "columnar")]
    shapes.push(("columnar", |w| w.use_columnar(true).use_row_group_size(256)));
    shapes
}

/// A scan reads every block of the table through one descriptor: it takes it
/// from the descriptor cache at its first block read from the file and reads
/// the later ones through it, in either direction.
#[test]
fn a_cold_scan_takes_the_descriptor_once() -> crate::Result<()> {
    for (name, shape) in scan_shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;

        let before = descriptor_accesses(&table);
        let blocks = table.metrics.block_load_io_count();
        assert_eq!(table.iter().count(), 500, "{name}: forward");
        assert!(
            table.metrics.block_load_io_count() - blocks > 4,
            "{name}: the scan did not read several blocks from the file"
        );
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: forward");

        let before = descriptor_accesses(&table);
        assert_eq!(table.iter().rev().count(), 500, "{name}: backward");
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: backward");
    }
    Ok(())
}

/// A descriptor the cache let go of, as the in-place heal does once it has
/// renamed a private copy over the table's path, is not read through again:
/// the scan takes the table's file afresh at its next block and still hands
/// out every row.
#[test]
fn a_scan_takes_the_descriptor_again_once_the_cache_drops_it() -> crate::Result<()> {
    for (name, shape) in scan_shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;

        let before = descriptor_accesses(&table);
        let mut iter = table.iter();
        let head = iter.by_ref().take(100).count();
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}: head");
        table.file_accessor.remove_for_table(&table.global_id());
        let blocks = table.metrics.block_load_io_count();
        assert_eq!(head + iter.count(), 500, "{name}: rows");
        assert!(
            table.metrics.block_load_io_count() > blocks,
            "{name}: the case under test: the scan reads from the file after the drop"
        );
        assert_eq!(descriptor_accesses(&table) - before, 2, "{name}: after");
    }
    Ok(())
}

/// Between its blocks a scan holds no reference that keeps the descriptor
/// open: the cache alone decides how long a table file stays open, so scans
/// in flight never hold more descriptors than it allows.
#[test]
fn a_scan_does_not_keep_the_descriptor_open() -> crate::Result<()> {
    for (name, shape) in scan_shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;

        let mut iter = table.iter();
        assert!(iter.next().is_some(), "{name}: first row");
        let cached = table
            .file_accessor
            .as_descriptor_table()
            .and_then(|d| d.access_for_table(&table.global_id()))
            .expect("the scan's read cached the descriptor");
        assert_eq!(
            Arc::strong_count(&cached),
            2,
            "{name}: the cache and this test hold the descriptor, the scan does not"
        );
        drop(cached);
        assert_eq!(iter.count(), 499, "{name}: rest");
    }
    Ok(())
}

/// The chunked multi-get planner checks the filters, walks the index and hands
/// back the file the planned blocks are then read through: one lookup, one
/// descriptor access.
#[test]
fn cold_block_task_plan_takes_the_descriptor_once() -> crate::Result<()> {
    let keys: Vec<(&[u8], u64)> = [b"key000002".as_slice(), b"key000500", b"key000998"]
        .into_iter()
        .map(|k| (k, hash64(k)))
        .collect();
    for (name, shape) in shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;

        let before = descriptor_accesses(&table);
        let filters = table.metrics.filter_block_load_io.load(Relaxed);
        let indexes = table.metrics.index_block_load_io.load(Relaxed);
        let plan = table.plan_block_tasks(
            &keys,
            SeqNo::MAX,
            &mut crate::table::probe_stats::PlanCounts::default(),
        )?;
        assert!(plan.is_some(), "{name}: the keys are planned");
        assert!(
            table.metrics.filter_block_load_io.load(Relaxed) > filters
                && table.metrics.index_block_load_io.load(Relaxed) > indexes,
            "{name}: the plan did not read its filter and index blocks from the file"
        );
        assert_eq!(descriptor_accesses(&table) - before, 1, "{name}");
    }
    Ok(())
}

/// A lookup the filter rules out reads only the filter, and a key absent from
/// a block the filter let through still reads index and data: either way the
/// descriptor is taken once.
#[test]
fn cold_lookups_that_miss_take_the_descriptor_at_most_once() -> crate::Result<()> {
    for (name, shape) in shapes() {
        let dir = tempdir()?;
        let table = cold_table(dir.path(), shape)?;
        for i in (1u32..1000).step_by(50) {
            let key = format!("key{i:06}");
            let before = descriptor_accesses(&table);
            assert!(
                table
                    .get(key.as_bytes(), SeqNo::MAX, hash64(key.as_bytes()))?
                    .is_none(),
                "{name}: {key} is absent"
            );
            assert!(
                descriptor_accesses(&table) - before <= 1,
                "{name}: {key} took the descriptor more than once"
            );
        }
    }
    Ok(())
}
