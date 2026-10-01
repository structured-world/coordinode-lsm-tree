#![expect(clippy::expect_used, reason = "test code")]

use super::*;
use crate::table::{RecoverParams, Writer};
use crate::{Cache, DescriptorTable, InternalValue, ValueType, fs::StdFs, hash::hash64};
use alloc::sync::Arc;
use tempfile::tempdir;
use test_log::test;

/// Every block a staged read walks through for a key batch, in the order it
/// asked for them, with the stage that asked.
type Asked = Vec<(BlockType, u64)>;

/// A staged read's plan (snapshot, data blocks with their keys, filter
/// answers) and what it asked for.
type Driven = (SeqNo, Vec<(BlockHandle, Vec<usize>)>, PlanCounts, Asked);

/// A table shape: its name, how the writer is set up, and whether the filter
/// and the index are pinned.
type Shape = (&'static str, fn(Writer) -> Writer, bool, bool);

/// Writes 500 keys `key000000..key000998` (even numbers only, so every odd
/// number is a miss inside the key range) into one table shaped by `shape`,
/// and opens it with a cache of `cache_bytes`, pinning as asked.
fn table(
    dir: &std::path::Path,
    shape: impl Fn(Writer) -> Writer,
    pin_filter: bool,
    pin_index: bool,
    cache_bytes: u64,
) -> Table {
    let file = dir.join("table");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);
    let mut writer = shape(Writer::new(file.clone(), 0, 0, Arc::clone(&fs)).expect("writer"));
    for i in (0u32..1000).step_by(2) {
        writer
            .write(InternalValue::from_components(
                format!("key{i:06}").into_bytes(),
                b"v".to_vec(),
                1,
                ValueType::Value,
            ))
            .expect("write");
    }
    let checksum = writer.finish().expect("finish").expect("a table").1;
    let mut params = RecoverParams::new(
        file,
        checksum,
        0,
        fs,
        crate::comparator::default_comparator(),
        Arc::new(Cache::with_capacity_bytes(cache_bytes)),
    );
    params.descriptor_table = Some(Arc::new(DescriptorTable::new(10)));
    params.pin_filter = pin_filter;
    params.pin_index = pin_index;
    Table::recover(params).expect("recover")
}

/// Drives a staged read of `table` for `keys` to its plan, serving each
/// block it asks for from the table file, and returns the plan and what it
/// asked for.
fn drive(table: &Table, keys: &[(&[u8], u64)]) -> Option<Driven> {
    let StagedStart::Staged(mut read) = StagedRead::start(table, keys, SeqNo::MAX) else {
        return None;
    };
    let file = std::fs::read(&*table.path).expect("table file");
    let mut asked = Asked::new();
    loop {
        let (block_type, need) = read.need();
        let need: Vec<BlockHandle> = need.to_vec();
        for handle in need {
            asked.push((block_type, *handle.offset()));
            let start = usize::try_from(*handle.offset()).expect("offset fits");
            let bytes = file
                .get(start..)
                .and_then(|rest| rest.get(..handle.size() as usize))
                .expect("the block lies in the table file");
            read.supply(handle, bytes).expect("supply");
        }
        if read.is_done() {
            break;
        }
        read.advance(keys).expect("advance");
        // Past its stage, a filter block is never consulted again.
        assert!(
            read.stage == Stage::Filter
                || read
                    .held
                    .iter()
                    .all(|(_, block)| block.header.block_type != BlockType::Filter),
            "a read past its filters still holds a filter block"
        );
        if read.is_done() && read.need().1.is_empty() {
            break;
        }
    }
    assert!(read.held.is_empty(), "a planned read holds no block");
    let (seqno, blocks, tally) = read.into_plan();
    Some((seqno, blocks, tally, asked))
}

/// A plan as comparable values: each block's offset and size with its keys.
fn plan_of(blocks: &[(BlockHandle, Vec<usize>)]) -> Vec<(u64, u32, Vec<usize>)> {
    blocks
        .iter()
        .map(|(handle, keys)| (*handle.offset(), handle.size(), keys.clone()))
        .collect()
}

/// Hits and misses spread over the table: every tenth even key and the odd
/// one after it.
fn batch() -> Vec<(Vec<u8>, u64)> {
    (0u32..1000)
        .step_by(20)
        .flat_map(|i| [i, i + 1])
        .map(|i| {
            let key = format!("key{i:06}").into_bytes();
            let hash = hash64(&key);
            (key, hash)
        })
        .collect()
}

/// The table shapes: filter whole or partitioned, pinned or not; index
/// pinned whole, whole read on demand, or partitioned.
fn shapes() -> Vec<Shape> {
    fn whole(w: Writer) -> Writer {
        w
    }
    fn partitioned_filter(w: Writer) -> Writer {
        w.use_partitioned_filter().use_meta_partition_size(8)
    }
    fn partitioned_index(w: Writer) -> Writer {
        w.use_adaptive_index(0)
    }
    fn partitioned_both(w: Writer) -> Writer {
        w.use_partitioned_filter()
            .use_meta_partition_size(8)
            .use_adaptive_index(0)
    }
    vec![
        ("pinned filter, pinned index", whole, true, true),
        ("pinned filter, index read", whole, true, false),
        ("filter read, index read", whole, false, false),
        (
            "filter partitions, index read",
            partitioned_filter,
            false,
            false,
        ),
        (
            "pinned filter, index partitions",
            partitioned_index,
            true,
            false,
        ),
        (
            "filter and index partitions",
            partitioned_both,
            false,
            false,
        ),
    ]
}

/// A staged read plans the same data blocks, for the same keys, with the same
/// filter answers, as the serial planner, on every table shape.
#[test]
fn a_staged_read_plans_what_the_serial_planner_plans() -> crate::Result<()> {
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    for (name, shape, pin_filter, pin_index) in shapes() {
        let dir = tempdir()?;
        let serial_table = table(dir.path(), shape, pin_filter, pin_index, 1_000_000);
        let mut serial_tally = PlanCounts::default();
        let (_, serial_seqno, _, serial_blocks) = serial_table
            .plan_block_tasks(&keys, SeqNo::MAX, &mut serial_tally)?
            .expect("keys in range");

        let dir = tempdir()?;
        let staged_table = table(dir.path(), shape, pin_filter, pin_index, 1_000_000);
        let (seqno, blocks, tally, asked) = drive(&staged_table, &keys).expect("staged");
        // A cold table's unpinned filter and index are read in their stages.
        assert_eq!(
            !pin_filter,
            asked.iter().any(|(t, _)| *t == BlockType::Filter),
            "{name}: filter stage asked for {asked:?}"
        );
        assert_eq!(
            !pin_index,
            asked.iter().any(|(t, _)| *t == BlockType::Index),
            "{name}: index stage asked for {asked:?}"
        );
        assert_eq!(serial_seqno, seqno, "{name}");
        assert_eq!(plan_of(&serial_blocks), plan_of(&blocks), "{name}");
        assert_eq!(serial_tally, tally, "{name}");
    }
    Ok(())
}

/// A handle whose size no block of the table can have is refused before a
/// buffer is allocated for it, as the load path refuses it: a corrupt size
/// is an error, not a request for gigabytes of memory.
#[test]
fn a_block_buffer_is_refused_for_a_size_no_block_can_have() {
    use crate::table::block::BlockOffset;

    let dir = tempdir().expect("dir");
    let table = table(dir.path(), |w| w, false, false, 0);
    assert!(
        table
            .block_buffer(&BlockHandle::new(BlockOffset(0), u32::MAX))
            .is_err()
    );
    let buf = table
        .block_buffer(&BlockHandle::new(BlockOffset(0), 4_096))
        .expect("a block's size");
    assert_eq!(4_096, buf.len());
}

/// A table file a staged read opens is counted in the descriptor cache's
/// hits and misses, as the load path counts the file it opens.
#[cfg(feature = "metrics")]
#[test]
fn opening_a_table_file_for_a_staged_read_counts_in_the_descriptor_cache() {
    use core::sync::atomic::Ordering::Relaxed;

    let dir = tempdir().expect("dir");
    let table = table(dir.path(), |w| w, false, false, 0);
    let opened = |t: &Table| {
        t.metrics.table_file_opened_cached.load(Relaxed)
            + t.metrics.table_file_opened_uncached.load(Relaxed)
    };
    let before = opened(&table);
    table.open_file().expect("open");
    table.open_file().expect("open");
    assert_eq!(before + 2, opened(&table));
}

/// Tiny data blocks and index partitions, so the table's index is split into
/// dozens of partitions.
fn many_partitions(w: Writer) -> Writer {
    w.use_data_block_size(64)
        .use_adaptive_index(0)
        .use_index_partition_size(64)
}

/// Two keys at either end of the table.
fn sparse_batch() -> Vec<(Vec<u8>, u64)> {
    ["key000000", "key000998"]
        .iter()
        .map(|key| (key.as_bytes().to_vec(), hash64(key.as_bytes())))
        .collect()
}

/// The serial planner and the serial batch read over the same sparse batch
/// seek the index at each key too: with nothing cached, each loads only the
/// index partitions its keys fall in.
#[cfg(feature = "metrics")]
#[test]
fn a_sparse_serial_read_loads_only_the_index_partitions_its_keys_fall_in() -> crate::Result<()> {
    let batch = sparse_batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let dir = tempdir()?;
    let table = table(dir.path(), many_partitions, true, false, 0);

    let before = table.metrics.index_block_load_count();
    table.plan_block_tasks(&keys, SeqNo::MAX, &mut PlanCounts::default())?;
    let planned = table.metrics.index_block_load_count() - before;
    assert!(
        planned <= 3,
        "the planner loaded {planned} index partitions"
    );

    let before = table.metrics.index_block_load_count();
    let found = table.batch_get(&keys, SeqNo::MAX)?;
    let read = table.metrics.index_block_load_count() - before;
    assert!(read <= 3, "the batch read loaded {read} index partitions");
    assert!(found.iter().all(Option::is_some), "both keys are found");
    Ok(())
}

/// A sparse batch over a table with many index partitions reads the
/// partitions its keys fall in, not every partition between its first key and
/// its last: the walk is sought at each key instead of stepped through the
/// index, and plans what the serial planner plans.
#[test]
fn a_sparse_batch_reads_only_the_index_partitions_its_keys_fall_in() -> crate::Result<()> {
    let batch = sparse_batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();

    let dir = tempdir()?;
    let serial_table = table(dir.path(), many_partitions, true, false, 0);
    let mut serial_tally = PlanCounts::default();
    let (_, _, _, serial_blocks) = serial_table
        .plan_block_tasks(&keys, SeqNo::MAX, &mut serial_tally)?
        .expect("keys in range");

    let dir = tempdir()?;
    let staged_table = table(dir.path(), many_partitions, true, false, 0);
    let (_, blocks, _, asked) = drive(&staged_table, &keys).expect("staged");
    let partitions = asked.iter().filter(|(t, _)| *t == BlockType::Index).count();
    assert!(
        partitions <= 3,
        "two keys asked for {partitions} index partitions: {asked:?}"
    );
    assert_eq!(plan_of(&serial_blocks), plan_of(&blocks));
    Ok(())
}

/// A warm table asks for nothing: every filter and index block is taken from
/// the cache.
#[test]
fn a_warm_table_is_planned_without_a_read() {
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    for (name, shape, pin_filter, pin_index) in shapes() {
        let dir = tempdir().expect("dir");
        let table = table(dir.path(), shape, pin_filter, pin_index, 1_000_000);
        let (_, cold, _, _) = drive(&table, &keys).expect("staged");
        let (_, warm, _, asked) = drive(&table, &keys).expect("staged");
        assert_eq!(plan_of(&cold), plan_of(&warm), "{name}");
        assert!(asked.is_empty(), "{name}: a warm table asked for {asked:?}");
    }
}

/// With a cache that keeps nothing, the read answers from the blocks it holds:
/// each block is asked for once, and the plan is the one a warm read makes.
#[test]
fn a_read_answers_from_what_it_holds_when_the_cache_keeps_nothing() {
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    for (name, shape, pin_filter, pin_index) in shapes() {
        let dir = tempdir().expect("dir");
        let cached = table(dir.path(), shape, pin_filter, pin_index, 1_000_000);
        let (_, expected, _, _) = drive(&cached, &keys).expect("staged");

        let dir = tempdir().expect("dir");
        let uncached = table(dir.path(), shape, pin_filter, pin_index, 0);
        let (_, blocks, _, asked) = drive(&uncached, &keys).expect("staged");
        assert_eq!(plan_of(&expected), plan_of(&blocks), "{name}");
        let mut offsets: Vec<u64> = asked.iter().map(|(_, offset)| *offset).collect();
        offsets.sort_unstable();
        let before = offsets.len();
        offsets.dedup();
        assert_eq!(before, offsets.len(), "{name}: a block was asked for twice");
    }
}

/// A table whose blocks need the load path's own recovery or reconstruction is
/// read serially, and a snapshot below the table reads nothing of it.
#[test]
fn a_staged_read_starts_only_where_it_can_answer() {
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let dir = tempdir().expect("dir");
    let table = table(dir.path(), |w| w, false, false, 1_000_000);
    assert!(matches!(
        StagedRead::start(&table, &keys, 1),
        StagedStart::Nothing
    ));
    assert!(matches!(
        StagedRead::start(&table, &[], SeqNo::MAX),
        StagedStart::Nothing
    ));
}
