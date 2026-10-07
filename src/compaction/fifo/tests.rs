use super::Strategy;
use crate::{
    AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, time::with_test_clock,
};
use std::sync::Arc;

#[test]
fn fifo_empty_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn fifo_below_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    let fifo = Arc::new(Strategy::new(u64::MAX, None));
    tree.compact(fifo, 4)?;

    assert_eq!(before, tree.table_count());
    Ok(())
}

#[test]
fn fifo_more_than_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    // Very small limit forces dropping oldest tables
    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 4)?;

    assert!(tree.table_count() < before);
    Ok(())
}

#[test]
fn fifo_more_than_limit_blobs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "$", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let before = tree.table_count();
    let fifo = Arc::new(Strategy::new(1, None));
    tree.compact(fifo, 3)?;

    assert!(tree.table_count() < before);
    Ok(())
}

#[test]
fn fifo_ttl() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        // Freeze time and create first (older) table at t=1000s
        clock.set_secs(1_000);
        tree.insert("a", "1", 0);
        tree.flush_active_memtable(0)?;

        // Advance time and create second (newer) table at t=1005s
        clock.set_secs(1_005);
        tree.insert("b", "2", 1);
        tree.flush_active_memtable(1)?;

        // Now set current time to t=1011s; with TTL=10s, cutoff=1001s => drop first only
        clock.set_secs(1_011);

        assert_eq!(2, tree.table_count());

        let fifo = Arc::new(Strategy::new(u64::MAX, Some(10)));
        tree.compact(fifo, 2)?;

        assert_eq!(1, tree.table_count());
        Ok(())
    })
}

/// Two flushes whose key ranges overlap (`a..c`, then `b`) leave L0 not
/// disjoint, which non-monotonic keys produce routinely. FIFO must compact
/// such a tree instead of panicking the worker, and enforce its limit by
/// dropping the oldest table first.
#[test]
fn fifo_overlapping_l0_compacts_without_panic_and_drops_oldest_first() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // The drop order follows the flushes' ages on the real clock: holding the
    // clock keeps another test's override out of them.
    with_test_clock(|_| {
        tree.insert("a", "old", 0);
        tree.insert("c", "old", 1);
        tree.flush_active_memtable(1)?;
        tree.insert("b", "new", 2);
        tree.flush_active_memtable(2)?;

        // Nothing to drop: the tree is left as it is.
        tree.compact(Arc::new(Strategy::new(u64::MAX, None)), 3)?;
        assert_eq!(2, tree.table_count());

        // A limit just below the total keeps the newer table and drops the older.
        let newest_size = tree
            .current_version()
            .iter_tables()
            .max_by_key(|t| t.get_highest_seqno())
            .map(crate::table::Table::file_size)
            .unwrap_or_default();
        tree.compact(Arc::new(Strategy::new(newest_size, None)), 3)?;
        assert_eq!(1, tree.table_count());
        assert!(tree.get("b", 3)?.is_some(), "the newer table must survive");
        assert!(tree.get("a", 3)?.is_none(), "the older table goes first");
        Ok(())
    })
}

/// After `major_compact` the tables sit in the last level, not L0. The size
/// limit must still apply to them.
#[test]
fn fifo_limit_applies_after_major_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }
    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(0, tree.current_version().l0().table_count());
    assert!(tree.table_count() > 0);

    tree.compact(Arc::new(Strategy::new(1, None)), 4)?;
    assert_eq!(
        0,
        tree.table_count(),
        "tables below L0 must count against the limit"
    );
    Ok(())
}

/// After `major_compact` the TTL must still expire tables below L0.
#[test]
fn fifo_ttl_applies_after_major_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        clock.set_secs(1_000);
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        tree.major_compact(u64::MAX, 0)?;

        clock.set_secs(1_011);
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 3)?;

        assert_eq!(0, tree.table_count(), "expired tables below L0 must drop");
        Ok(())
    })
}

/// A major compaction rewrites the data in key order and stamps each output
/// with the time it was written. With keys inserted in decreasing order the
/// newest data has the lowest keys and lands in the first output written, so
/// the creation time would rank it oldest. The size limit must still drop
/// the oldest data first.
#[test]
fn fifo_after_major_compaction_drops_oldest_data_first_for_decreasing_keys() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Incompressible values so the compaction splits into several outputs.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut value = || {
        (0..256)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                state.to_be_bytes()[0]
            })
            .collect::<Vec<u8>>()
    };

    // Seqno 0 carries key 399, the last seqno carries key 0.
    let keys = 400u32;
    // The outputs' ages come from the flushes' on the real clock: holding the
    // clock keeps another test's override out of them.
    with_test_clock(|_| {
        for key in (0..keys).rev() {
            let seqno = u64::from(keys - 1 - key);
            tree.insert(key.to_be_bytes().as_slice(), value(), seqno);
            if key % 100 == 0 {
                tree.flush_active_memtable(seqno)?;
            }
        }
        let watermark = u64::from(keys);
        tree.major_compact(16 * 1024, 0)?;
        assert!(
            tree.table_count() > 1,
            "the compaction must produce several outputs"
        );

        let newest = tree
            .current_version()
            .iter_tables()
            .max_by_key(|t| t.get_highest_seqno())
            .map(crate::table::Table::file_size)
            .unwrap_or_default();
        tree.compact(Arc::new(Strategy::new(newest, None)), watermark)?;

        assert!(
            tree.get(0u32.to_be_bytes(), watermark)?.is_some(),
            "the newest key must survive"
        );
        assert!(
            tree.get((keys - 1).to_be_bytes(), watermark)?.is_none(),
            "the oldest key goes first"
        );
        Ok(())
    })
}

/// A table another compaction holds is not FIFO's to drop, and holding it
/// must not panic: `choose` skips hidden tables and drops only the others.
#[test]
fn fifo_choose_skips_hidden_tables_instead_of_panicking() -> crate::Result<()> {
    use super::super::{Choice, CompactionStrategy, state::CompactionState};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // FIFO orders the tables by their flushes' ages on the real clock: holding
    // the clock keeps another test's override out of them.
    with_test_clock(|_| {
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        crate::Result::Ok(())
    })?;
    let version = tree.current_version();
    let Some(newest) = version
        .iter_tables()
        .max_by_key(|t| t.get_highest_seqno())
        .map(crate::table::Table::id)
    else {
        panic!("three tables were flushed");
    };
    let ids: Vec<_> = version.iter_tables().map(crate::table::Table::id).collect();

    let mut state = CompactionState::default();
    state.hidden_set_mut().hide([newest]);
    let choice = Strategy::new(1, None).choose(&version, &Config::default(), &state);
    let Choice::Drop(dropped) = choice else {
        panic!("expected a drop of the older tables not held");
    };
    assert!(
        !dropped.contains(&newest),
        "a held table must not be dropped"
    );
    assert_eq!(2, dropped.len(), "both older tables go");

    let mut all_held = CompactionState::default();
    all_held.hidden_set_mut().hide(ids.iter().copied());
    assert!(matches!(
        Strategy::new(1, None).choose(&version, &Config::default(), &all_held),
        Choice::DoNothing
    ));
    Ok(())
}

/// The oldest table is held by another compaction and its bytes alone push
/// the tree over the limit. Dropping the newer tables in its place would lose
/// recent data while the older stays, so FIFO must wait for a later round.
#[test]
fn fifo_choose_drops_no_newer_table_while_the_oldest_is_held() -> crate::Result<()> {
    use super::super::{Choice, CompactionStrategy, state::CompactionState};

    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // FIFO orders the tables by their flushes' ages on the real clock: holding
    // the clock keeps another test's override out of them.
    with_test_clock(|_| {
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        crate::Result::Ok(())
    })?;
    let version = tree.current_version();
    let Some((oldest, oldest_size)) = version
        .iter_tables()
        .min_by_key(|t| t.get_highest_seqno())
        .map(|t| (t.id(), t.file_size()))
    else {
        panic!("three tables were flushed");
    };
    let total = version
        .iter_tables()
        .map(crate::table::Table::file_size)
        .sum::<u64>();

    let mut state = CompactionState::default();
    state.hidden_set_mut().hide([oldest]);
    // Over the limit by exactly the held table's bytes.
    let choice =
        Strategy::new(total - oldest_size, None).choose(&version, &Config::default(), &state);
    assert!(
        matches!(choice, Choice::DoNothing),
        "no newer table may go while the oldest is held"
    );
    Ok(())
}

/// A major compaction under a watermark above every live sequence number
/// zeroes them at the last level, so they no longer order the outputs. Each
/// output keeps the age of the flushes its keys came from, so with keys
/// inserted in decreasing order the newest flush's keys survive a limit that
/// holds them, and the oldest flush's go first.
#[test]
fn fifo_after_a_seqno_zeroing_major_compaction_drops_oldest_data_first() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Incompressible values so the compaction splits into several outputs.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut value = || {
        (0..256)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                state.to_be_bytes()[0]
            })
            .collect::<Vec<u8>>()
    };
    let keys = 400u32;
    with_test_clock(|clock| {
        // Seqno 0 carries key 399 and is flushed first, at t=1000s; each
        // later flush of lower keys is a second newer.
        for key in (0..keys).rev() {
            let seqno = u64::from(keys - 1 - key);
            tree.insert(key.to_be_bytes().as_slice(), value(), seqno);
            if key % 100 == 0 {
                clock.set_secs(1_000 + seqno / 100);
                tree.flush_active_memtable(seqno)?;
            }
        }
        let watermark = u64::from(keys);
        tree.major_compact(16 * 1024, watermark)?;
        let version = tree.current_version();
        assert!(version.iter_tables().count() > 1, "several outputs");
        assert!(
            version.iter_tables().all(|t| t.get_highest_seqno() == 0),
            "the compaction zeroed every sequence number"
        );
        // The outputs that carry the newest flush's keys. Their order among
        // themselves is key order, as nothing else is left to tell their
        // data apart, so the limit keeps all of them.
        let Some(newest_age) = version.iter_tables().map(|t| t.metadata.created_at).max() else {
            panic!("the compaction wrote tables");
        };
        let newest = version
            .iter_tables()
            .filter(|t| t.metadata.created_at == newest_age)
            .map(crate::table::Table::file_size)
            .sum::<u64>();
        drop(version);

        tree.compact(Arc::new(Strategy::new(newest, None)), watermark)?;
        assert!(
            tree.get(0u32.to_be_bytes(), watermark)?.is_some(),
            "the newest key must survive"
        );
        assert!(
            tree.get((keys - 1).to_be_bytes(), watermark)?.is_none(),
            "the oldest key goes first"
        );
        Ok(())
    })
}

/// A major compaction shortly before data expires must not restart its TTL:
/// the outputs keep the age of the data they carry, not the time they were
/// written, so the data expires on its own time.
#[test]
fn fifo_ttl_counts_from_the_data_age_not_from_a_later_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        clock.set_secs(1_000);
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        // Rewritten at t=1009s, a second before a 10s TTL runs out.
        clock.set_secs(1_009);
        tree.major_compact(u64::MAX, 0)?;

        clock.set_secs(1_011);
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 3)?;
        assert_eq!(
            0,
            tree.table_count(),
            "data written at 1000s expires at 1010s whatever rewrote it"
        );
        Ok(())
    })
}

/// A clock at zero is no clock, which leaves TTL off: tables written
/// meanwhile carry time zero too and must not all count as expired.
#[test]
fn fifo_ttl_is_off_while_the_clock_reads_zero() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        clock.set_secs(0);
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        tree.major_compact(u64::MAX, 0)?;
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 3)?;
        assert!(
            tree.table_count() > 0,
            "no table may expire without a clock"
        );
        assert!(tree.get([b'k', 0].as_slice(), 3)?.is_some());
        Ok(())
    })
}

/// Tables written while the clock read zero carry no age, and a clock that
/// starts later must not read that missing age as the epoch and expire them.
#[test]
fn fifo_ttl_spares_tables_stamped_before_the_clock_started() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        clock.set_secs(0);
        for i in 0..3u8 {
            tree.insert([b'k', i].as_slice(), "v", u64::from(i));
            tree.flush_active_memtable(u64::from(i))?;
        }
        tree.major_compact(u64::MAX, 0)?;
        clock.set_secs(1_000_000);
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 3)?;
        assert!(
            tree.get([b'k', 0].as_slice(), 3)?.is_some(),
            "a table with no age must not expire once the clock starts"
        );
        Ok(())
    })
}

/// The outputs of a major compaction of a KV-separated tree share its blob
/// file, which goes only with the last of them. Counting its bytes as freed
/// by the first output dropped stops the round with the file still on disk
/// and the tree over its limit.
#[test]
fn fifo_counts_a_shared_blob_file_freed_only_with_its_last_table() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    // Incompressible values, so the blob file outweighs the tables.
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut value = || {
        (0..256)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_be_bytes()[0]
            })
            .collect::<Vec<u8>>()
    };
    // Enough pointers that the tables split into several outputs, each far
    // smaller than the one blob file they share.
    let keys = 4_000u32;
    for key in 0..keys {
        tree.insert(key.to_be_bytes().as_slice(), value(), u64::from(key));
    }
    tree.flush_active_memtable(u64::from(keys))?;
    tree.major_compact(4 * 1024, 0)?;

    let disk = || {
        let version = tree.current_version();
        version
            .iter_tables()
            .map(crate::table::Table::file_size)
            .sum::<u64>()
            + version.blob_files.on_disk_size()
    };
    let version = tree.current_version();
    assert!(version.iter_tables().count() > 1, "several outputs");
    assert_eq!(1, version.blob_files.len(), "sharing one blob file");
    drop(version);

    let limit = disk() / 2;
    tree.compact(Arc::new(Strategy::new(limit, None)), u64::from(keys) + 1)?;
    let after = disk();
    assert!(
        after <= limit,
        "one round must bring the tree within its limit: {after} > {limit}"
    );
    Ok(())
}

/// A blob file whose last table goes stays while a memtable row borrows an
/// object in it, so dropping that table frees the table alone. Counting the
/// file as freed stops the round with the file still on disk and the tree over
/// its limit, though dropping the newer table would have brought it within.
#[test]
fn fifo_counts_no_blob_file_a_memtable_row_keeps() -> crate::Result<()> {
    use crate::blob_tree::field_row::{Cell, FIRST_FIELD_COLUMN, Field};

    let status = FIRST_FIELD_COLUMN;
    let body_column = FIRST_FIELD_COLUMN + 1;
    let dir = tempfile::tempdir()?;
    let crate::AnyTree::Blob(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .blob_compression(crate::CompressionType::None)
    .open()?
    else {
        panic!("a tree with kv separation opens as a blob tree");
    };

    let body = vec![b'b'; 64 * 1024];
    with_test_clock(|_| {
        // The older table owns the blob file; the newer one holds no blob.
        tree.insert_cells(
            "doc",
            &[
                Field::bytes(status, b"draft"),
                Field::bytes(body_column, &body),
            ],
            0,
        )?;
        tree.flush_active_memtable(0)?;
        tree.insert("z", "v", 1);
        tree.flush_active_memtable(1)?;
        crate::Result::Ok(())
    })?;
    // A memtable row that borrows the body from the blob file.
    let Some(row) = tree.get_cells("doc", 2)? else {
        panic!("the row was flushed");
    };
    let Some(borrowed) = row
        .fields()?
        .into_iter()
        .find(|field| field.column == body_column && matches!(field.cell, Cell::Ref(_)))
    else {
        panic!("the body was separated");
    };
    tree.insert_cells("doc", &[Field::bytes(status, b"final"), borrowed], 2)?;
    drop(row);

    let version = tree.current_version();
    let blob_bytes = version.blob_files.on_disk_size();
    let Some(newer) = version
        .iter_tables()
        .max_by_key(|table| table.get_highest_seqno())
        .map(crate::table::Table::file_size)
    else {
        panic!("two tables were flushed");
    };
    drop(version);
    let disk = || {
        let version = tree.current_version();
        version
            .iter_tables()
            .map(crate::table::Table::file_size)
            .sum::<u64>()
            + version.blob_files.on_disk_size()
    };
    // Within reach once both tables go, as the kept file stays either way.
    let limit = blob_bytes + newer / 2;
    tree.compact(Arc::new(Strategy::new(limit, None)), 3)?;
    let after = disk();
    assert!(
        after <= limit,
        "one round must bring the tree within its limit: {after} > {limit}"
    );
    assert_eq!(1, tree.blob_file_count(), "the borrowed file stays");
    Ok(())
}

#[test]
fn fifo_ttl_then_limit_additional_drops_blob_unit() -> crate::Result<()> {
    with_test_clock(|clock| {
        // Two tables, each with its own blob file: the older one written long
        // before the newer one, so a one-second TTL expires the older alone.
        // Returns the tree and the on-disk size of the newer table's unit.
        let two_tables = |dir: &std::path::Path| -> crate::Result<(crate::AnyTree, u64)> {
            let tree = Config::new(
                dir,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;
            let disk = |tree: &crate::AnyTree| {
                let version = tree.current_version();
                version
                    .iter_tables()
                    .map(crate::table::Table::file_size)
                    .sum::<u64>()
                    + version.blob_files.on_disk_size()
            };
            clock.set_secs(1_000);
            tree.insert("a", "$", 0);
            tree.flush_active_memtable(0)?;
            let older = disk(&tree);
            clock.set_secs(10_000_000);
            tree.insert("b", "$", 1);
            tree.flush_active_memtable(1)?;
            let newer = disk(&tree) - older;
            Ok((tree, newer))
        };

        // A limit the newer unit just fits: the TTL drop brings the tree
        // within it, so the newer table stays. Counting the expired table's
        // bytes against the limit as well would drop the newer one too.
        let dir = tempfile::tempdir()?;
        let (tree, newer) = two_tables(dir.path())?;
        tree.compact(Arc::new(Strategy::new(newer, Some(1))), 2)?;
        assert_eq!(1, tree.table_count(), "the TTL drop alone fits the limit");
        assert_eq!(1, tree.blob_file_count());

        // A one-byte limit: after the TTL drop the tree is still over it, so
        // the same round drops the newer table too, with its blob file.
        let dir = tempfile::tempdir()?;
        let (tree, _) = two_tables(dir.path())?;
        tree.compact(Arc::new(Strategy::new(1, Some(1))), 2)?;
        assert_eq!(0, tree.table_count());
        assert_eq!(
            0,
            tree.blob_file_count(),
            "the blob unit goes with its table"
        );
        Ok(())
    })
}

/// A compaction output that takes in a table stamped while the clock read
/// zero is dated by the compaction that writes it: none of its data is newer
/// than that, so a TTL counted from it expires nothing early, and the output
/// still expires rather than staying forever.
#[test]
fn fifo_ttl_counts_an_undated_input_from_the_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    with_test_clock(|clock| {
        clock.set_secs(0);
        tree.insert("a", "v", 0);
        tree.flush_active_memtable(0)?;
        clock.set_secs(1_000);
        tree.insert("b", "v", 1);
        tree.flush_active_memtable(1)?;
        clock.set_secs(5_000);
        tree.major_compact(u64::MAX, 0)?;
        assert_eq!(1, tree.table_count());

        // The dated input alone would have expired the output at 1_010s.
        clock.set_secs(5_009);
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 2)?;
        assert_eq!(1, tree.table_count(), "nothing expires before its time");

        clock.set_secs(5_011);
        tree.compact(Arc::new(Strategy::new(u64::MAX, Some(10))), 2)?;
        assert_eq!(
            0,
            tree.table_count(),
            "the output expires 10s after it was written"
        );
        Ok(())
    })
}
