// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What range tombstones cost a read, by how many there are and how many merge
//! operands each key holds.
//!
//! One flushed table of `KEYS` keys, each a base and its operands, and narrow
//! tombstones between the keys, which delete nothing, so every row reaches the
//! reader and every operand is checked against them. The tombstones come
//! `older` than the data or `newer`, the case that cannot be told by seqno.
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

/// Key `i`, leaving the odd keys to the tombstones.
fn key(i: u32) -> [u8; 4] {
    (i * 2).to_be_bytes()
}

/// A tree of `KEYS` keys of `operands` operands each and `tombstones` range
/// tombstones between them, and the seqno reading all of it.
fn fill(
    folder: &tempfile::TempDir,
    tombstones: u32,
    operands: u32,
    newer: bool,
) -> lsm_tree::Result<(AnyTree, SeqNo)> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()?;

    let mut seqno = 1;
    let delete = |seqno: &mut SeqNo| {
        for i in 0..tombstones {
            let at = (u64::from(i) * u64::from(KEYS) / u64::from(tombstones)) as u32 * 2 + 1;
            tree.remove_range(
                at.to_be_bytes().to_vec(),
                (at + 1).to_be_bytes().to_vec(),
                *seqno,
            );
            *seqno += 1;
        }
    };
    if !newer {
        delete(&mut seqno);
    }
    for i in 0..KEYS {
        tree.insert(key(i), [0_u8; 8], seqno);
        seqno += 1;
        for _ in 0..operands {
            tree.merge(key(i), [1_u8; 8], seqno);
            seqno += 1;
        }
    }
    if newer {
        delete(&mut seqno);
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
    for (tombstones, operands, newer) in [0, 1, 4, 16, 64, 256, 1_000, 10_000]
        .into_iter()
        .flat_map(|tombstones| [0, 1, 16].map(|operands| (tombstones, operands)))
        .flat_map(|(tombstones, operands)| [false, true].map(|newer| (tombstones, operands, newer)))
    {
        {
            let folder = tempfile::tempdir()?;
            let (tree, seqno) = fill(&folder, tombstones, operands, newer)?;
            let age = if newer { "newer" } else { "older" };

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
                "tombstones {tombstones} {age} operands {operands}: first_row {first_row:.0} ns, \
                 forward {forward:.0} ns/row, reverse {reverse:.0} ns/row, get {get:.0} ns",
            );
        }
    }
    Ok(())
}
