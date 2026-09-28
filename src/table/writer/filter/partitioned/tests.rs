// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::PartitionedFilterWriter;
use crate::{config::BloomConstructionPolicy, table::writer::filter::FilterWriter};

type W = std::io::Cursor<Vec<u8>>;

/// `finish` spills the open partition, which adds a top-level entry under its
/// last key before the top-level index is encoded: the scratch counts that
/// entry, as the output does. The open partition itself does not depend on
/// the key's length, so the scratch grows with it by the entry alone.
#[test]
fn the_scratch_counts_the_top_level_entry_of_the_open_partition() -> crate::Result<()> {
    let scratch = |key_len: usize| -> crate::Result<u64> {
        let mut writer = PartitionedFilterWriter::new(BloomConstructionPolicy::default());
        FilterWriter::<W>::register_key(&mut writer, &vec![b'k'; key_len].into())?;
        Ok(FilterWriter::<W>::finish_scratch_bytes(&writer))
    };
    let short = scratch(10)?;
    let long = scratch(5_000)?;
    assert!(
        long >= short + 4_990,
        "scratch {long} for a 5 000-byte last key against {short} for a 10-byte one",
    );
    Ok(())
}

/// A partition closes by the bytes it encodes to, one word per slot, not by
/// its bits per key: under the default policy the partitions average the
/// partition size instead of several times past it. One partition may land a
/// layer either side of it, as its build takes a later layer whole or not.
#[test]
fn a_filter_partition_closes_near_the_partition_size() -> crate::Result<()> {
    const PARTITION: u64 = 4_096;
    let mut writer = PartitionedFilterWriter::new(BloomConstructionPolicy::default());
    for i in 0..50_000u32 {
        FilterWriter::<W>::register_key(&mut writer, &format!("key{i:08}").into_bytes().into())?;
    }
    let sizes: Vec<u64> = writer
        .tli_handles
        .iter()
        .map(|handle| u64::from(handle.size()))
        .collect();
    assert!(sizes.len() > 10, "the keys fill many partitions");
    for &size in &sizes {
        assert!(
            (PARTITION / 2..=PARTITION * 8 / 5).contains(&size),
            "a {size}-byte partition for a {PARTITION}-byte target",
        );
    }
    let average = sizes.iter().sum::<u64>() / sizes.len() as u64;
    assert!(
        (PARTITION * 85 / 100..=PARTITION * 115 / 100).contains(&average),
        "partitions average {average} bytes for a {PARTITION}-byte target",
    );
    Ok(())
}

/// The keys a partition holds follow the policy and the partition size in
/// whichever order they are set: a writer created under a policy that builds
/// no filter, then given an active one, still closes its partitions, so the
/// open partition, which `finish` builds, stays one partition's worth.
#[test]
fn a_policy_set_after_the_writer_still_closes_partitions() -> crate::Result<()> {
    let writer: Box<dyn FilterWriter<W>> = Box::new(PartitionedFilterWriter::new(
        BloomConstructionPolicy::BitsPerKey(0.0),
    ));
    let mut writer = writer
        .use_partition_size(4_096)
        .set_filter_policy(BloomConstructionPolicy::default());
    for i in 0..50_000u32 {
        writer.register_key(&format!("key{i:08}").into_bytes().into())?;
    }
    let scratch = writer.finish_scratch_bytes();
    assert!(
        scratch < 64 * 1_024,
        "an open partition of {scratch} scratch bytes after 50 000 keys",
    );
    Ok(())
}
