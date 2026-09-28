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
