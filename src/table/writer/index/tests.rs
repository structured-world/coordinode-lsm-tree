// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{AdaptiveIndexWriter, BlockIndexWriter, FullIndexWriter, PartitionedIndexWriter};
use crate::table::{BlockHandle, BlockOffset, index_block::KeyedBlockHandle};

type W = std::io::Cursor<Vec<u8>>;

fn handle(i: u64) -> KeyedBlockHandle {
    KeyedBlockHandle::new(
        format!("{i:08}").into_bytes().into(),
        0,
        BlockHandle::new(BlockOffset(i * 4_096), 4_096),
    )
}

/// 129 handles leave a vector grown to 256 slots, and the writer holds every
/// slot of that allocation, not only the 129 in use.
fn assert_counts_capacity(mut writer: Box<dyn BlockIndexWriter<W>>) -> crate::Result<()> {
    for i in 0..129 {
        writer.register_data_block(handle(i))?;
    }
    let slots = 256 * core::mem::size_of::<KeyedBlockHandle>() as u64;
    assert!(
        writer.held_bytes() >= slots,
        "{} bytes held for {slots} bytes of slots",
        writer.held_bytes(),
    );
    Ok(())
}

#[test]
fn a_full_index_counts_its_vector_capacity() -> crate::Result<()> {
    assert_counts_capacity(Box::new(FullIndexWriter::new()))
}

#[test]
fn an_adaptive_index_counts_its_buffer_capacity() -> crate::Result<()> {
    assert_counts_capacity(Box::new(AdaptiveIndexWriter::<W>::new(u64::MAX)))
}

/// With partitions large enough, every handle stays in the open partition.
#[test]
fn a_partitioned_index_counts_its_open_partition_capacity() -> crate::Result<()> {
    assert_counts_capacity(Box::new(PartitionedIndexWriter::new()).use_partition_size(u32::MAX))
}
