// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::PartitionedIndexWriter;
use crate::table::{
    BlockHandle, BlockOffset, index_block::KeyedBlockHandle, writer::index::BlockIndexWriter,
};

type W = std::io::Cursor<Vec<u8>>;

/// `finish` cuts the open partition, which adds a top-level entry under its
/// last key before the top-level index is encoded: the scratch counts that
/// entry too, as the output does.
#[test]
fn the_scratch_counts_the_top_level_entry_of_the_open_partition() -> crate::Result<()> {
    use crate::table::block::{BlockType, framed_len_bound};

    let mut writer = PartitionedIndexWriter::new();
    writer.partition_size = u32::MAX;
    let handle = KeyedBlockHandle::new(
        vec![b'k'; 5_000].into(),
        0,
        BlockHandle::new(BlockOffset(0), 4_096),
    );
    let entry = handle.encoded_len_bound_unplaced() as u64;
    BlockIndexWriter::<W>::register_data_block(&mut writer, handle)?;
    let frame = framed_len_bound(
        writer.open_encoded as u64,
        BlockType::Index,
        writer.compression,
        None,
        None,
    );
    // The open partition, its frame and the partition buffer it grows into.
    let open = writer.open_encoded as u64 + 2 * frame;
    let scratch = BlockIndexWriter::<W>::finish_scratch_bytes(&writer);
    assert!(
        scratch >= open + entry,
        "scratch {scratch} for an open partition of {open} and a {entry}-byte top-level entry",
    );
    Ok(())
}

/// The top-level entries hold offsets relative to the partition buffer until
/// `finish` shifts them to where the index lands in the file, which widens
/// their varints. What the writer counts for them covers any such shift.
#[test]
fn the_top_level_count_covers_any_file_offset() -> crate::Result<()> {
    let mut writer = PartitionedIndexWriter::new();
    writer.partition_size = 64;
    for i in 0..500 {
        BlockIndexWriter::<W>::register_data_block(
            &mut writer,
            KeyedBlockHandle::new(
                format!("{i:08}").into_bytes().into(),
                0,
                BlockHandle::new(BlockOffset(i * 4_096), 4_096),
            ),
        )?;
    }
    let far = BlockOffset(u64::MAX / 2);
    let shifted: usize = writer
        .tli_handles
        .iter()
        .map(|handle| {
            let mut handle = handle.clone();
            handle.shift(far);
            handle.encoded_len_bound()
        })
        .sum();
    assert!(
        writer.tli_encoded >= shifted,
        "{} counted, {shifted} once placed",
        writer.tli_encoded,
    );
    Ok(())
}
