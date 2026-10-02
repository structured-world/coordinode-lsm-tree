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

/// A level read bounds the blocks a read holds by their decoded bytes: a
/// compressed index block is held as far more than it is read as, and a
/// filter block stops counting once the filters have answered.
#[cfg(feature = "lz4")]
#[test]
fn a_read_holds_its_blocks_at_their_decoded_size() {
    let dir = tempdir().expect("tempdir");
    let table = table(
        dir.path(),
        // A data block per few keys: an index of hundreds of similar entries.
        |w| {
            w.use_data_block_size(64)
                .use_index_block_compression(crate::CompressionType::Lz4)
        },
        false,
        false,
        1_000_000,
    );
    let keys = batch();
    let keys: Vec<(&[u8], u64)> = keys.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let StagedStart::Staged(mut read) = StagedRead::start(&table, &keys, SeqNo::MAX) else {
        panic!("an unpinned table is staged");
    };
    let file = std::fs::read(&*table.path).expect("table file");
    let supply = |read: &mut StagedRead<'_>| -> u64 {
        let need: Vec<BlockHandle> = read.need().1.to_vec();
        let mut read_as = 0;
        for handle in need {
            let start = usize::try_from(*handle.offset()).expect("offset fits");
            let bytes = file
                .get(start..)
                .and_then(|rest| rest.get(..handle.size() as usize))
                .expect("the block lies in the table file");
            read.supply(handle, bytes).expect("supply");
            read_as += u64::from(handle.size());
        }
        read_as
    };

    supply(&mut read);
    assert!(read.held_bytes() > 0, "the filter block is held");
    read.advance(&keys).expect("advance");
    assert_eq!(read.held_bytes(), 0, "the answered filters count no more");

    let read_as = supply(&mut read);
    let decoded: u64 = read
        .held
        .iter()
        .map(|(_, block)| block.data.len() as u64)
        .sum();
    assert_eq!(read.held_bytes(), decoded);
    assert!(
        decoded > read_as,
        "the compressed index ({read_as} bytes read) is held decoded ({decoded} bytes)"
    );
}

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
    table_with(dir, shape, pin_filter, pin_index, cache_bytes, |_| {})
}

/// [`table`], with `tune` applied to the parameters it is opened with.
fn table_with(
    dir: &std::path::Path,
    shape: impl Fn(Writer) -> Writer,
    pin_filter: bool,
    pin_index: bool,
    cache_bytes: u64,
    tune: impl FnOnce(&mut RecoverParams),
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
    tune(&mut params);
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

/// A handle whose size no block of the table can have is refused by the check
/// a staged read makes before it allocates a buffer, as the load path refuses
/// it: a corrupt size is an error, not a request for gigabytes of memory.
#[test]
fn a_block_size_no_block_can_have_is_refused() {
    use crate::table::block::BlockOffset;

    let dir = tempdir().expect("dir");
    let table = table(dir.path(), |w| w, false, false, 0);
    assert!(
        table
            .check_block_size(&BlockHandle::new(BlockOffset(0), u32::MAX))
            .is_err()
    );
    assert!(
        table
            .check_block_size(&BlockHandle::new(BlockOffset(0), 4_096))
            .is_ok()
    );
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

/// The last key of every other index partition, so the partition after each
/// holds no key of the batch: a version of such a key may continue past the
/// partition's end, so the walk steps into a partition the batch never asked
/// for.
fn partition_end_batch(table: &Table) -> Vec<(Vec<u8>, u64)> {
    let BlockIndexImpl::TwoLevel(index) = &*table.block_index else {
        panic!("a partitioned index");
    };
    let ends: Vec<Vec<u8>> = OwnedIndexBlockIter::from_block_with_bounds(
        index.top_level_index.clone(),
        table.comparator.clone(),
        None,
        None,
    )
    .expect("the top-level index decodes")
    .expect("a top-level index")
    .map(|handle| handle.end_key().to_vec())
    .collect();
    assert!(ends.len() > 4, "many index partitions: {}", ends.len());
    // The last partition has none after it.
    let inner = ends.get(..ends.len() - 1).expect("partitions");
    inner
        .iter()
        .step_by(2)
        .map(|key| (key.clone(), hash64(key)))
        .collect()
}

/// A key ending an index partition is read on into the next partition, which
/// the read asks for once its walk reaches it, cold, and takes from the cache
/// without a read, warm; both plan what the serial planner plans.
#[test]
fn a_key_ending_a_partition_walks_on_into_the_next_one() -> crate::Result<()> {
    let dir = tempdir()?;
    let cold = table(dir.path(), many_partitions, true, false, 0);
    let batch = partition_end_batch(&cold);
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let mut serial_tally = PlanCounts::default();
    let (_, _, _, serial_blocks) = cold
        .plan_block_tasks(&keys, SeqNo::MAX, &mut serial_tally)?
        .expect("keys in range");

    let (_, blocks, _, asked) = drive(&cold, &keys).expect("staged");
    let partitions = asked.iter().filter(|(t, _)| *t == BlockType::Index).count();
    assert!(
        partitions > keys.len(),
        "{} keys at partition ends asked for {partitions} partitions: {asked:?}",
        keys.len()
    );
    assert_eq!(plan_of(&serial_blocks), plan_of(&blocks), "cold");

    let dir = tempdir()?;
    let warm = table(dir.path(), many_partitions, true, false, 1_000_000);
    drive(&warm, &keys).expect("staged");
    let (_, blocks, _, asked) = drive(&warm, &keys).expect("staged");
    assert!(asked.is_empty(), "a warm table asked for {asked:?}");
    assert_eq!(plan_of(&serial_blocks), plan_of(&blocks), "warm");
    Ok(())
}

/// A key past the last filter partition is ruled out by the partition index
/// alone: nothing is read for it and no data block is planned.
#[test]
fn a_key_past_the_last_filter_partition_reads_nothing() {
    let dir = tempdir().expect("dir");
    let table = table(
        dir.path(),
        |w| w.use_partitioned_filter().use_meta_partition_size(8),
        false,
        false,
        0,
    );
    let key = b"key999999".as_slice();
    let (_, blocks, tally, asked) = drive(&table, &[(key, hash64(key))]).expect("staged");
    assert!(
        asked.is_empty(),
        "a key past the partitions asked for {asked:?}"
    );
    assert!(blocks.is_empty(), "nothing is planned: {blocks:?}");
    assert_eq!(
        1, tally.filter_skips,
        "the partition index ruled the key out"
    );
}

/// A table without a filter rules no key out: its read goes straight to the
/// index and plans what the serial planner plans.
#[test]
fn a_table_without_a_filter_is_planned_from_its_index() -> crate::Result<()> {
    use crate::config::BloomConstructionPolicy;

    let no_filter = |w: Writer| w.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0.0));
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let dir = tempdir()?;
    let serial_table = table(dir.path(), no_filter, false, false, 0);
    let mut serial_tally = PlanCounts::default();
    let (_, _, _, serial_blocks) = serial_table
        .plan_block_tasks(&keys, SeqNo::MAX, &mut serial_tally)?
        .expect("keys in range");

    let dir = tempdir()?;
    let staged_table = table(dir.path(), no_filter, false, false, 0);
    let (_, blocks, tally, asked) = drive(&staged_table, &keys).expect("staged");
    assert!(
        asked.iter().all(|(t, _)| *t != BlockType::Filter),
        "no filter to read: {asked:?}"
    );
    assert_eq!(plan_of(&serial_blocks), plan_of(&blocks));
    assert_eq!(serial_tally, tally);
    Ok(())
}

/// A table restricted to keys from a bound on, as a tight-space compaction
/// leaves its input, answers nothing below the bound in any batch read, as a
/// point read answers nothing there: the blocks below are punched out and
/// their rows live in the table that superseded them.
#[test]
fn a_key_below_a_restriction_is_read_nowhere() -> crate::Result<()> {
    let dir = tempdir()?;
    let full = table(dir.path(), |w| w, false, false, 1_000_000);
    let restricted = full.with_restriction(crate::UserKey::from(b"key000500".as_slice()));
    let below = b"key000100".as_slice();
    let above = b"key000600".as_slice();
    let keys = [(below, hash64(below)), (above, hash64(above))];
    let planned = |blocks: &[(BlockHandle, Vec<usize>)]| -> Vec<usize> {
        blocks
            .iter()
            .flat_map(|(_, keys)| keys.iter().copied())
            .collect()
    };

    assert!(restricted.get(below, SeqNo::MAX, hash64(below))?.is_none());

    let found = restricted.batch_get(&keys, SeqNo::MAX)?;
    assert_eq!(
        found.iter().map(Option::is_some).collect::<Vec<_>>(),
        [false, true],
        "the batch read"
    );

    let mut tally = PlanCounts::default();
    let (_, _, _, serial) = restricted
        .plan_block_tasks(&keys, SeqNo::MAX, &mut tally)?
        .expect("a key is in range");
    assert_eq!(planned(&serial), [1], "the serial planner");

    let (_, staged, _, _) = drive(&restricted, &keys).expect("staged");
    assert_eq!(planned(&staged), [1], "the staged read");
    Ok(())
}

/// A snapshot below a table's global seqno, as of an ingested table, sees
/// nothing of it.
#[test]
fn a_snapshot_below_the_global_seqno_reads_nothing() {
    let batch = batch();
    let keys: Vec<(&[u8], u64)> = batch.iter().map(|(k, h)| (k.as_slice(), *h)).collect();
    let dir = tempdir().expect("dir");
    let table = table_with(
        dir.path(),
        |w| w,
        false,
        false,
        0,
        |params| {
            params.global_seqno = 100;
        },
    );
    assert!(matches!(
        StagedRead::start(&table, &keys, 50),
        StagedStart::Nothing
    ));
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
