// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::FullFilterWriter;
use crate::{config::BloomConstructionPolicy, prefix::PrefixExtractor};
use alloc::sync::Arc;

type W = std::io::Cursor<Vec<u8>>;

struct UpToColon;

impl PrefixExtractor for UpToColon {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        let end = key.iter().position(|b| *b == b':').map_or(0, |i| i + 1);
        Box::new(key.get(..end).into_iter())
    }
}

/// Under page ECC the filter bytes are framed with their parity into a
/// second buffer while the first is still held, so the peak counts both.
#[cfg(feature = "page_ecc")]
#[test]
fn a_filter_with_parity_counts_its_framed_copy() -> crate::Result<()> {
    use super::FilterWriter;
    let fill = |writer: &mut FullFilterWriter| -> crate::Result<u64> {
        for i in 0..1_000 {
            FilterWriter::<W>::register_key(writer, &format!("{i:04}").into_bytes().into())?;
        }
        Ok(FilterWriter::<W>::finish_scratch_bytes(writer))
    };
    let plain = fill(&mut FullFilterWriter::new(
        BloomConstructionPolicy::default(),
    ))?;
    let mut with_parity = FullFilterWriter::new(BloomConstructionPolicy::default());
    with_parity.ecc = Some(crate::table::block::EccParams::RS_4_2);
    let parity = fill(&mut with_parity)?;
    assert!(parity > plain, "{parity} with parity, {plain} without");
    Ok(())
}

/// Sorted keys sharing a prefix are adjacent, so the prefix is buffered once
/// for the run of them: 1000 keys under one prefix buffer 1000 key hashes and
/// one prefix hash, not a prefix hash per key.
#[test]
fn a_prefix_shared_by_adjacent_keys_is_buffered_once() -> crate::Result<()> {
    let mut writer = FullFilterWriter::new(BloomConstructionPolicy::default());
    writer.prefix_extractor = Some(Arc::new(UpToColon));
    for i in 0..1_000 {
        super::FilterWriter::<W>::register_key(
            &mut writer,
            &format!("p:{i:04}").into_bytes().into(),
        )?;
    }
    assert_eq!(writer.bloom_hash_buffer.len(), 1_001);
    Ok(())
}
