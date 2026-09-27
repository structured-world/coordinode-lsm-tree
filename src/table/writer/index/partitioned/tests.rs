// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::PartitionedIndexWriter;
use crate::table::{
    BlockHandle, BlockOffset, index_block::KeyedBlockHandle, writer::index::BlockIndexWriter,
};

type W = std::io::Cursor<Vec<u8>>;

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
