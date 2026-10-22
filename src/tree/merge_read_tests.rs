// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::chunk_order_tests::ctx_for;
use super::multi_get_read::MultiGetRead;
use super::read_job::{JobDone, MergeLocated, ReadMachine, ReadWork};
use crate::table::BlockHandle;
use crate::{AbstractTree, Config, SequenceNumberCounter};
use alloc::sync::Arc;

/// Appends the operands to the base.
struct Concat;

impl crate::MergeOperator for Concat {
    fn merge(
        &self,
        _key: &[u8],
        base: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> crate::Result<crate::UserValue> {
        let mut merged = base.map(<[u8]>::to_vec).unwrap_or_default();
        for operand in operands {
            merged.extend_from_slice(operand);
        }
        Ok(merged.into())
    }
}

/// A block a merge is to read whose indexed size no block of its table can
/// have is refused before a buffer is allocated for it, as a level read
/// refuses it: the merge goes to the path that resolves the key alone, which
/// reports what is wrong, and no read of that size is asked for.
#[test]
fn a_merge_block_of_a_size_no_block_can_have_is_refused_before_its_buffer() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    let any = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(Concat)))
    .open()?;
    let crate::AnyTree::Standard(tree) = &any else {
        panic!("a standard tree");
    };
    tree.insert("key", "base", 1);
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), 0)?;
    tree.merge("key", "+", 2);
    tree.flush_active_memtable(0)?;

    let mut ctx = ctx_for(tree, alloc::vec!["key"])?;
    ctx.merge_operator = Some(Arc::new(Concat));
    let mut read = MultiGetRead::new(&ctx);
    let mut work = ReadWork::new();
    let mut largest = 0usize;
    let mut forged = None;
    let answer = loop {
        if let Some(answer) = read.pump(&mut work) {
            break answer;
        }
        for job in core::mem::take(&mut work.jobs) {
            let done = match job.run(&ctx) {
                // The base table's one block, its size forged past the cap.
                JobDone::MergeLocated {
                    tag,
                    located:
                        Ok(MergeLocated::Blocks {
                            filter,
                            file,
                            blocks,
                        }),
                } => {
                    let Some(real) = blocks.first() else {
                        panic!("the base's table holds a block of the key");
                    };
                    // The table is neither encrypted nor ECC-protected.
                    let Err(crate::Error::DecompressedSizeTooLarge { limit, .. }) =
                        crate::table::block::check_on_disk_size(u64::from(u32::MAX), None, None)
                    else {
                        panic!("a size past the cap is refused");
                    };
                    let size = u32::try_from(limit + 1).expect("the cap fits a u32");
                    forged = Some(size as usize);
                    JobDone::MergeLocated {
                        tag,
                        located: Ok(MergeLocated::Blocks {
                            filter,
                            file,
                            blocks: alloc::vec![BlockHandle::new(real.offset(), size)],
                        }),
                    }
                }
                done => done,
            };
            read.job_done(done, &mut work);
        }
        for mut block in core::mem::take(&mut work.reads) {
            largest = largest.max(block.buf.len());
            let result = block.file.read_at(&mut block.buf, block.offset).map(|_| ());
            read.read_done(crate::fs::ReadDone {
                tag: block.tag,
                buf: block.buf,
                result,
            });
        }
    };

    let forged = forged.expect("the merge located its base's table");
    assert!(
        largest < forged,
        "no buffer of the forged size is allocated: {largest} of {forged}"
    );
    let values = answer?;
    assert_eq!(
        values.first().and_then(Option::as_deref),
        Some(b"base+".as_slice()),
        "the merge resolves on its own path"
    );
    Ok(())
}
