// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What range tombstones cost a read, by how many there are and how many merge
//! operands each key holds.
//!
//! One flushed table of `KEYS` keys, each a base and its operands, under
//! tombstones written before them: narrow ones that delete nothing, so every
//! row reaches the reader and every operand is checked against them.
//! - `first_row`: a scan built and read to its first row, its setup;
//! - `forward` and `reverse`: whole scans, per row;
//! - `get`: point reads, per read.
//!
//! Timings are printed directly, the median of `SAMPLES` passes.

use lsm_tree::{
    AbstractTree, AnyTree, Config, Guard, MergeOperator, SeqNo, SequenceNumberCounter, UserValue,
};
use std::sync::Arc;
use std::time::Instant;

const KEYS: u32 = 20_000;
const GETS: u32 = 2_000;
const SAMPLES: usize = 5;

struct Concat;

impl MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let len = base_value.map_or(0, <[u8]>::len) + operands.len();
        Ok(vec![0_u8; len].into())
    }
}

fn key(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

/// A tree of `KEYS` keys of `operands` operands each over `tombstones` range
/// tombstones older than all of them, and the seqno reading all of it.
fn fill(
    folder: &tempfile::TempDir,
    tombstones: u32,
    operands: u32,
) -> lsm_tree::Result<(AnyTree, SeqNo)> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()?;

    let mut seqno = 1;
    for i in 0..tombstones {
        let start = (u64::from(i) * u64::from(KEYS) / u64::from(tombstones)) as u32;
        tree.remove_range(key(start).to_vec(), key(start + 1).to_vec(), seqno);
        seqno += 1;
    }
    for i in 0..KEYS {
        tree.insert(key(i), [0_u8; 8], seqno);
        seqno += 1;
        for _ in 0..operands {
            tree.merge(key(i), [1_u8; 8], seqno);
            seqno += 1;
        }
    }
    tree.flush_active_memtable(0)?;
    Ok((tree, seqno))
}

/// The median over `SAMPLES` passes of `pass`, in ns per `per`.
fn median(per: u32, mut pass: impl FnMut() -> lsm_tree::Result<()>) -> lsm_tree::Result<f64> {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        pass()?;
        samples.push(start.elapsed().as_nanos() as f64 / f64::from(per));
    }
    samples.sort_by(f64::total_cmp);
    Ok(samples[SAMPLES / 2])
}

fn main() -> lsm_tree::Result<()> {
    for tombstones in [0, 1, 4, 16, 64, 256, 1_000, 10_000] {
        for operands in [0, 1, 16] {
            let folder = tempfile::tempdir()?;
            let (tree, seqno) = fill(&folder, tombstones, operands)?;

            let first_row = median(1, || {
                let row = tree.iter(seqno, None).next();
                assert!(row.is_some());
                Ok(())
            })?;
            let forward = median(KEYS, || {
                let mut rows = 0;
                for row in tree.iter(seqno, None) {
                    row.into_inner()?;
                    rows += 1;
                }
                assert_eq!(rows, KEYS);
                Ok(())
            })?;
            let reverse = median(KEYS, || {
                let mut rows = 0;
                for row in tree.iter(seqno, None).rev() {
                    row.into_inner()?;
                    rows += 1;
                }
                assert_eq!(rows, KEYS);
                Ok(())
            })?;
            let get = median(GETS, || {
                for i in 0..GETS {
                    assert!(tree.get(key(i * (KEYS / GETS)), seqno)?.is_some());
                }
                Ok(())
            })?;

            println!(
                "tombstones {tombstones} operands {operands}: first_row {first_row:.0} ns, \
                 forward {forward:.0} ns/row, reverse {reverse:.0} ns/row, get {get:.0} ns",
            );
        }
    }
    Ok(())
}
