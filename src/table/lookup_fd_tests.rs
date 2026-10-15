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

/// Descriptor-cache accesses the table has counted so far.
fn descriptor_accesses(table: &Table) -> usize {
    table.metrics.table_file_opened_cached.load(Relaxed)
        + table.metrics.table_file_opened_uncached.load(Relaxed)
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
