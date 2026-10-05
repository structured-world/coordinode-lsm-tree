// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Range reads through every index shape and index restart interval: an upper
//! bound covers every block that may hold a key up to it.

use lsm_tree::config::{BlockSizePolicy, PinningPolicy, RestartIntervalPolicy};
use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter, get_tmp_folder};

/// The index shapes a table can be read through: pinned whole, loaded per
/// read, and partitioned.
#[derive(Clone, Copy, Debug)]
enum IndexShape {
    Pinned,
    Volatile,
    Partitioned,
}

const SHAPES: [IndexShape; 3] = [
    IndexShape::Pinned,
    IndexShape::Volatile,
    IndexShape::Partitioned,
];

fn configure(config: Config, interval: u8, shape: IndexShape) -> Config {
    let config = config.index_block_restart_interval_policy(RestartIntervalPolicy::all(interval));
    match shape {
        IndexShape::Pinned => config,
        IndexShape::Volatile => config.index_block_pinning_policy(PinningPolicy::all(false)),
        IndexShape::Partitioned => {
            // The writer keeps an index in one block until it outgrows the
            // spill threshold, far above these tables: spill at once.
            let mut runtime = lsm_tree::runtime_config::RuntimeConfig::default();
            runtime.index_partition_spill_threshold = 0;
            config
                .index_block_partitioning_policy(PinningPolicy::all(true))
                .with_runtime_config(runtime)
        }
    }
}

/// A tree with 256-byte data blocks and index partitions, so a few keys
/// spread over many blocks.
fn small_block_tree(
    folder: &std::path::Path,
    interval: u8,
    shape: IndexShape,
) -> lsm_tree::Result<lsm_tree::AnyTree> {
    let mut config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(256));
    config.index_block_partition_size_policy = BlockSizePolicy::all(256);
    configure(config, interval, shape).open()
}

/// The versions of the range's upper-bound key span several blocks, and the
/// oldest sit in the block after the last one ending at that key. A snapshot
/// that sees only an old version still finds it, forward and in reverse.
#[test]
fn a_range_ending_at_a_key_reads_its_versions_in_the_next_block() -> lsm_tree::Result<()> {
    for (interval, shape) in [1u8, 4].into_iter().flat_map(|i| SHAPES.map(|s| (i, s))) {
        let dir = get_tmp_folder();
        let tree = small_block_tree(dir.path(), interval, shape)?;
        tree.insert("a", "v", 0);
        for seqno in 1..=300u64 {
            tree.insert("k", format!("{seqno:0>40}"), seqno);
        }
        tree.insert("z", "v", 301);
        // GC watermark 0: every version of "k" is kept on flush.
        tree.flush_active_memtable(0)?;

        for (snapshot, want) in [(2u64, 1u64), (150, 149), (302, 300)] {
            let want = format!("{want:0>40}").into_bytes();
            let got: Vec<(Vec<u8>, Vec<u8>)> = tree
                .range("b"..="k", snapshot, None)
                .map(|guard| guard.into_inner().map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect::<lsm_tree::Result<_>>()?;
            assert_eq!(
                vec![(b"k".to_vec(), want.clone())],
                got,
                "interval {interval}, {shape:?}: snapshot {snapshot}"
            );
            let got_rev: Vec<Vec<u8>> = tree
                .range("b"..="k", snapshot, None)
                .rev()
                .map(|guard| guard.into_inner().map(|(_, v)| v.to_vec()))
                .collect::<lsm_tree::Result<_>>()?;
            assert_eq!(
                vec![want],
                got_rev,
                "interval {interval}, {shape:?}: reverse, snapshot {snapshot}"
            );
        }
    }
    Ok(())
}

/// Many small blocks: ranges inside one index entry, across a few and across
/// the table read exactly their keys.
#[test]
fn ranges_over_many_blocks_read_exactly_their_keys() -> lsm_tree::Result<()> {
    let keys: Vec<String> = (0..2_000).map(|i| format!("k{i:05}")).collect();
    for (interval, shape) in [1u8, 4].into_iter().flat_map(|i| SHAPES.map(|s| (i, s))) {
        let dir = get_tmp_folder();
        let tree = small_block_tree(dir.path(), interval, shape)?;
        for (seqno, key) in keys.iter().enumerate() {
            tree.insert(key, "v", seqno as u64);
        }
        tree.flush_active_memtable(0)?;

        for (lo, hi) in [
            (0, 0),
            (7, 9),
            (100, 100),
            (500, 900),
            (0, 1_999),
            (1_999, 1_999),
        ] {
            let got: Vec<Vec<u8>> = tree
                .range(keys[lo].as_str()..=keys[hi].as_str(), SeqNo::MAX, None)
                .map(|guard| guard.key().map(|k| k.to_vec()))
                .collect::<lsm_tree::Result<_>>()?;
            let want: Vec<Vec<u8>> = keys[lo..=hi]
                .iter()
                .map(|k| k.as_bytes().to_vec())
                .collect();
            assert_eq!(
                want, got,
                "interval {interval}, {shape:?}: range {lo}..={hi}"
            );
        }
    }
    Ok(())
}

/// A single-block table: every range lies inside its one index entry.
#[test]
fn a_range_inside_one_index_entry_reads_exactly_its_keys() -> lsm_tree::Result<()> {
    for (interval, shape) in [1u8, 2, 16]
        .into_iter()
        .flat_map(|i| SHAPES.map(|s| (i, s)))
    {
        let dir = get_tmp_folder();
        let tree = configure(
            Config::new(
                dir.path(),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            ),
            interval,
            shape,
        )
        .open()?;
        for (seqno, key) in ["a", "b", "c"].into_iter().enumerate() {
            tree.insert(key, "v", seqno as u64);
        }
        tree.flush_active_memtable(0)?;

        let keys = |range: (std::ops::Bound<&str>, std::ops::Bound<&str>)| {
            tree.range::<&str, _>(range, SeqNo::MAX, None)
                .map(|guard| guard.key().map(|k| k.to_vec()))
                .collect::<lsm_tree::Result<Vec<_>>>()
        };
        use std::ops::Bound::{Included, Unbounded};
        assert_eq!(
            vec![b"b".to_vec()],
            keys((Included("b"), Included("b")))?,
            "interval {interval}, {shape:?}: single-key range"
        );
        assert_eq!(
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            keys((Included("a"), Included("c")))?,
            "interval {interval}, {shape:?}: whole-table range"
        );
        assert_eq!(
            vec![b"b".to_vec(), b"c".to_vec()],
            keys((Included("b"), Unbounded))?,
            "interval {interval}, {shape:?}: lower bound only"
        );
    }
    Ok(())
}
