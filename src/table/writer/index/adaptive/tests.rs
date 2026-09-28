// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::AdaptiveIndexWriter;
use crate::table::{
    BlockHandle, BlockOffset, index_block::KeyedBlockHandle, writer::index::BlockIndexWriter,
};

type Writer = AdaptiveIndexWriter<std::io::Cursor<Vec<u8>>>;

fn handle(i: u64) -> KeyedBlockHandle {
    KeyedBlockHandle::new(
        format!("{i:032}").into_bytes().into(),
        0,
        BlockHandle::new(BlockOffset(i * 4_096), 4_096),
    )
}

/// Spilling to a partitioned index hands the buffered handles over and frees
/// the buffer: its capacity would otherwise stay held, uncounted, for the rest
/// of the table.
#[test]
fn a_spill_frees_the_buffer() -> crate::Result<()> {
    let mut writer = Writer::new(1_024);
    let mut i = 0;
    while writer.spilled.is_none() {
        writer.register_data_block(handle(i))?;
        i += 1;
    }
    assert_eq!(writer.buffer.capacity(), 0);
    Ok(())
}

/// Before a spill the writer holds the buffered handles once; `finish` hands
/// them to the full writer and adds only their encoded block, which the table
/// writes twice, each copy under its block header.
#[test]
fn an_unspilled_index_counts_its_encoding_twice_in_the_output() -> crate::Result<()> {
    let mut writer = Writer::new(u64::MAX);
    for i in 0..100 {
        writer.register_data_block(handle(i))?;
    }
    let held = BlockIndexWriter::held_bytes(&writer);
    let scratch = BlockIndexWriter::finish_scratch_bytes(&writer);
    let output = BlockIndexWriter::finish_output_bytes(&writer);
    // 100 handles in a buffer grown to 128 slots.
    assert_eq!(
        held,
        writer.buffered_bytes + 28 * core::mem::size_of::<KeyedBlockHandle>() as u64,
    );
    let header = crate::table::block::Header::header_len(crate::table::block::BlockType::Index);
    assert_eq!(output, 2 * (scratch + header as u64));
    // 32-byte keys, each encoded with a handful of varints and a restart
    // pointer: under the in-memory entry, above the key alone.
    assert!(scratch > 100 * 32 && scratch < held, "{scratch} vs {held}");
    Ok(())
}
