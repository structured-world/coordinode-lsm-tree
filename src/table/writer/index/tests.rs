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

/// A partitioned index records its partitions at offsets relative to its own
/// buffer and shifts them to file offsets when it writes the top-level index,
/// which can widen every offset varint. Written after 3 MiB of data, the
/// top-level index still fits what the writer counted for it.
#[test]
fn a_partitioned_top_level_index_fits_its_estimate_at_a_far_offset() -> crate::Result<()> {
    use std::io::Write;

    let mut writer: Box<dyn BlockIndexWriter<W>> =
        Box::new(PartitionedIndexWriter::new()).use_partition_size(64);
    for i in 0..2_000 {
        writer.register_data_block(handle(i))?;
    }
    let counted = writer.finish_scratch_bytes();
    let mut file = crate::sfa::Writer::from_writer(crate::checksum::ChecksummedWriter::new(
        std::io::Cursor::new(Vec::new()),
    ));
    file.start("data")?;
    file.write_all(&alloc::vec![0; 3 << 20])?;
    let (_, tli) = writer.finish(&mut file)?;
    assert!(
        tli.len() as u64 <= counted,
        "{} top-level index bytes, {counted} counted",
        tli.len(),
    );
    Ok(())
}

/// A compressed index block keeps its compressed form even when that came out
/// larger than its input, as it does for keys that do not compress. The
/// index the writer counts still covers the block it writes.
#[cfg(feature = "lz4")]
#[test]
fn a_compressed_index_that_expands_fits_its_estimate() -> crate::Result<()> {
    use std::io::Seek;

    let mut writer: Box<dyn BlockIndexWriter<W>> =
        Box::new(FullIndexWriter::new()).use_compression(crate::CompressionType::Lz4);
    for i in 0..500_u64 {
        let key: alloc::vec::Vec<u8> = (0..4)
            .flat_map(|j| crate::hash::hash64(&(i * 4 + j).to_le_bytes()).to_le_bytes())
            .collect();
        writer.register_data_block(KeyedBlockHandle::new(
            key.into(),
            0,
            BlockHandle::new(BlockOffset(i * 4_096), 4_096),
        ))?;
    }
    // Written twice by the table, once here.
    let counted = writer.finish_output_bytes() / 2;
    let mut file = crate::sfa::Writer::from_writer(crate::checksum::ChecksummedWriter::new(
        std::io::Cursor::new(Vec::new()),
    ));
    file.start("data")?;
    let before = file.get_mut().stream_position()?;
    writer.finish(&mut file)?;
    let written = file.get_mut().stream_position()? - before;
    assert!(
        written <= counted,
        "{written} bytes written, {counted} counted"
    );
    Ok(())
}

/// A transformed index block is framed into a second buffer while the encoded
/// one is still held, so its peak counts both.
#[cfg(feature = "lz4")]
#[test]
fn a_transformed_index_counts_its_framed_copy() -> crate::Result<()> {
    let fill = |mut writer: Box<dyn BlockIndexWriter<W>>| -> crate::Result<u64> {
        for i in 0..100 {
            writer.register_data_block(handle(i))?;
        }
        Ok(writer.finish_scratch_bytes())
    };
    let plain = fill(Box::new(FullIndexWriter::new()))?;
    let compressed =
        fill(Box::new(FullIndexWriter::new()).use_compression(crate::CompressionType::Lz4))?;
    assert!(
        compressed > plain,
        "{compressed} with a codec, {plain} without"
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
