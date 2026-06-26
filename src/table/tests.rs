// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#![allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    reason = "test code"
)]
#![expect(clippy::expect_used, reason = "test code")]

use super::*;
use crate::{config::BloomConstructionPolicy, fs::StdFs, hash::hash64};
use tempfile::tempdir;
use test_log::test;

fn test_with_table(
    items: &[InternalValue],
    f: impl Fn(Table) -> crate::Result<()>,
    rotate_every: Option<usize>,
    config_writer: Option<impl Fn(Writer) -> Writer>,
) -> crate::Result<()> {
    test_with_table_impl(
        items,
        f,
        rotate_every,
        config_writer,
        #[cfg(zstd_any)]
        None,
    )
}

#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::unwrap_used
)]
fn test_with_table_impl(
    items: &[InternalValue],
    f: impl Fn(Table) -> crate::Result<()>,
    rotate_every: Option<usize>,
    config_writer: Option<impl Fn(Writer) -> Writer>,
    #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
) -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;

        #[cfg(zstd_any)]
        if zstd_dictionary.is_some() {
            writer = writer.use_zstd_dictionary(zstd_dictionary.clone());
        }

        if let Some(f) = &config_writer {
            writer = f(writer);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every
                && idx % rotate == 0
            {
                writer.spill_block()?;
            }
            writer.write(item.clone())?;
        }
        let (_, checksum) = writer.finish()?.unwrap();

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                false,
                false,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert_eq!(0, table.pinned_block_index_size(), "should not pin index");
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                true,
                false,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert_eq!(0, table.pinned_block_index_size(), "should not pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                false,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                true,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
                Arc::new(StdFs),
                true,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(table.file_accessor, FileAccessor::File(..)));

            f(table)?;
        }
    }

    std::fs::remove_file(&file)?;

    // Test with partitioned indexes
    {
        let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?.use_partitioned_index();

        #[cfg(zstd_any)]
        if zstd_dictionary.is_some() {
            writer = writer.use_zstd_dictionary(zstd_dictionary.clone());
        }

        if let Some(f) = config_writer {
            writer = f(writer);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every
                && idx % rotate == 0
            {
                writer.spill_block()?;
            }
            writer.write(item.clone())?;
        }
        let (_, checksum) = writer.finish()?.unwrap();

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                false,
                false,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                true,
                false,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                false,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                Arc::new(StdFs),
                true,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary.clone(),
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable { .. }
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file,
                checksum,
                0,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
                Arc::new(StdFs),
                true,
                true,
                None,
                #[cfg(zstd_any)]
                zstd_dictionary,
                crate::comparator::default_comparator(),
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(table.file_accessor, FileAccessor::File(..)));

            f(table)?;
        }
    }

    Ok(())
}

#[cfg(feature = "zstd")]
fn test_with_table_and_zstd_dictionary(
    items: &[InternalValue],
    f: impl Fn(Table) -> crate::Result<()>,
    rotate_every: Option<usize>,
    config_writer: Option<impl Fn(Writer) -> Writer>,
    zstd_dictionary: Arc<crate::compression::ZstdDictionary>,
) -> crate::Result<()> {
    test_with_table_impl(items, f, rotate_every, config_writer, Some(zstd_dictionary))
}

#[cfg(feature = "zstd")]
fn make_test_dictionary() -> crate::compression::ZstdDictionary {
    let mut samples = Vec::new();
    for i in 0u32..500 {
        let key = format!("key-{i:05}");
        let val = format!("value-{i:05}-padding-to-make-it-longer");
        samples.extend_from_slice(key.as_bytes());
        samples.extend_from_slice(val.as_bytes());
    }
    crate::compression::ZstdDictionary::new(&samples)
}

/// A table with large data blocks compressed at a high zstd level splits each
/// block into several inner zstd blocks. The writer must persist their
/// cumulative decompressed-end layout in the `block_layout` section, and the
/// reader must reload it on open. A default small-block table must NOT carry
/// the section. This is the write → persist → reload contract for range-query
/// partial decode.
#[cfg(feature = "zstd")]
#[test]
#[expect(clippy::unwrap_used)]
fn block_layout_section_roundtrips_for_large_zstd_blocks() {
    use crate::cache::Cache;
    use crate::fs::StdFs;
    #[cfg(feature = "metrics")]
    use crate::metrics::Metrics;
    use crate::table::Writer;

    // 256 KiB blocks at L19 split into many inner zstd blocks (the cold-tier
    // shape); ~600 KiB of sorted KV yields at least one full multi-inner-block
    // data block.
    let items: Vec<crate::InternalValue> = (0u64..20_000)
        .map(|i| {
            crate::InternalValue::from_components(
                format!("key-{i:012}").into_bytes(),
                format!("value-{i:08}-payload").into_bytes(),
                1,
                crate::ValueType::Value,
            )
        })
        .collect();

    let dir = tempdir().unwrap();
    let file = dir.path().join("table");

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))
        .unwrap()
        .use_data_block_size(256 * 1024)
        .use_data_block_compression(crate::CompressionType::Zstd(19));
    for item in &items {
        writer.write(item.clone()).unwrap();
    }
    let (_, checksum) = writer.finish().unwrap().unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(Metrics::default());
    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(4_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics,
    )
    .unwrap();

    assert!(
        table.regions.block_layout.is_some(),
        "large multi-inner-block table must carry a block_layout section",
    );
    assert!(
        table.block_layout.len() >= 1,
        "at least one data block must have a recorded inner-block layout",
    );
    // Every recorded entry must have strictly increasing cumulative ends whose
    // last value is the block's uncompressed length (a non-trivial split).
    for offset in table.block_layout.offsets() {
        let ends = table
            .block_layout
            .ends_for(offset)
            .expect("offsets() entries must resolve via ends_for");
        assert!(
            ends.len() >= 2,
            "recorded block must have >= 2 inner blocks"
        );
        assert!(
            ends.windows(2).all(|w| w[0] < w[1]),
            "cumulative ends must be strictly increasing: {ends:?}",
        );
    }

    // Negative control: a default small-block (4 KiB) zstd table must NOT carry
    // the section — each tiny block compresses into a single inner zstd block,
    // so there is nothing to partial-decode and no layout is persisted.
    let small_file = dir.path().join("table-small");
    let mut small_writer = Writer::new(small_file.clone(), 0, 0, Arc::new(StdFs))
        .unwrap()
        .use_data_block_size(4 * 1024)
        .use_data_block_compression(crate::CompressionType::Zstd(19));
    for item in &items {
        small_writer.write(item.clone()).unwrap();
    }
    let (_, small_checksum) = small_writer.finish().unwrap().unwrap();

    #[cfg(feature = "metrics")]
    let small_metrics = Arc::new(Metrics::default());
    let small_table = Table::recover(
        small_file,
        small_checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(4_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        small_metrics,
    )
    .unwrap();

    assert!(
        small_table.regions.block_layout.is_none(),
        "default small-block table must NOT carry a block_layout section",
    );
    assert_eq!(
        small_table.block_layout.len(),
        0,
        "small-block table's layout map must be empty",
    );
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read() -> crate::Result<()> {
    let items = [crate::InternalValue::from_components(
        b"abc",
        b"asdasdasd",
        3,
        crate::ValueType::Value,
    )];

    test_with_table(
        &items,
        |table| {
            assert_eq!(
                b"abc",
                &*table
                    .get(b"abc", SeqNo::MAX, hash64(b"abc"))?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(None, table.get(b"def", SeqNo::MAX, hash64(b"def"))?,);
            assert_eq!(None, table.get(b"____", SeqNo::MAX, hash64(b"____"))?,);

            assert_eq!(
                table.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"abc".into())),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn restricted_view_clamps_point_and_range_reads() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"v", 0, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            // Restrict the view to keys >= "c" (the prefix a, b is punched out
            // and superseded by a merged output table in tight-space reclaim).
            let restricted = table.with_restriction(crate::UserKey::from(&b"c"[..]));

            // Point reads below the bound miss (so the read falls through to the
            // superseding output); at/above the bound they hit.
            assert_eq!(None, restricted.get(b"a", SeqNo::MAX, hash64(b"a"))?);
            assert_eq!(None, restricted.get(b"b", SeqNo::MAX, hash64(b"b"))?);
            assert!(restricted.get(b"c", SeqNo::MAX, hash64(b"c"))?.is_some());
            assert!(restricted.get(b"d", SeqNo::MAX, hash64(b"d"))?.is_some());

            // The unrestricted view of the same physical SST is unaffected.
            assert!(table.get(b"a", SeqNo::MAX, hash64(b"a"))?.is_some());

            // A full scan yields only keys >= the bound, in order — the iterator
            // never walks into the punched prefix.
            let keys: Vec<_> = restricted
                .range(..)
                .map(|r| r.unwrap().key.user_key)
                .collect();
            assert_eq!(
                keys,
                vec![
                    crate::UserKey::from(&b"c"[..]),
                    crate::UserKey::from(&b"d"[..]),
                    crate::UserKey::from(&b"e"[..]),
                ],
            );

            let cmp = crate::comparator::default_comparator();
            // A query entirely below the bound does not overlap the live range.
            assert!(!restricted.check_key_range_overlap_cmp(
                &(
                    core::ops::Bound::Unbounded,
                    core::ops::Bound::Excluded(&b"c"[..]),
                ),
                cmp.as_ref(),
            ));
            // A query reaching into [bound, hi] does overlap.
            assert!(restricted.check_key_range_overlap_cmp(
                &(
                    core::ops::Bound::Included(&b"d"[..]),
                    core::ops::Bound::Unbounded,
                ),
                cmp.as_ref(),
            ));

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn reopen_restricted_yields_a_distinct_clamped_view() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"v", 0, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            // Re-open as a distinct Inner over the same file, clamped to >= "c".
            let restricted = table.reopen_restricted(crate::UserKey::from(&b"c"[..]))?;

            assert_eq!(None, restricted.get(b"a", SeqNo::MAX, hash64(b"a"))?);
            assert_eq!(None, restricted.get(b"b", SeqNo::MAX, hash64(b"b"))?);
            assert!(restricted.get(b"c", SeqNo::MAX, hash64(b"c"))?.is_some());
            assert!(restricted.get(b"e", SeqNo::MAX, hash64(b"e"))?.is_some());

            // The original view of the same file is unaffected.
            assert!(table.get(b"a", SeqNo::MAX, hash64(b"a"))?.is_some());

            // A full scan of the re-opened view yields only keys >= the bound.
            let keys: Vec<_> = restricted
                .range(..)
                .map(|r| r.unwrap().key.user_key)
                .collect();
            assert_eq!(
                keys,
                vec![
                    crate::UserKey::from(&b"c"[..]),
                    crate::UserKey::from(&b"d"[..]),
                    crate::UserKey::from(&b"e"[..]),
                ],
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn punch_offset_for_locates_the_first_block_reaching_a_key() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"v", 0, crate::ValueType::Value),
    ];

    // rotate_every Some(1) spills a block before every item, so each key lands
    // in its own data block with a strictly increasing offset.
    test_with_table(
        &items,
        |table| {
            // "a" is in the first block at offset 0 (nothing below it to punch).
            assert_eq!(0, table.punch_offset_for(b"a")?);

            let pb = table.punch_offset_for(b"b")?;
            let pc = table.punch_offset_for(b"c")?;
            let pe = table.punch_offset_for(b"e")?;
            assert!(pb > 0, "punching up to b reclaims a's block");
            assert!(pc > pb, "offsets advance with the key");
            assert!(pe > pc);

            // A key past the last block reports the end of the data region, so
            // the whole data area is punchable.
            let beyond = table.punch_offset_for(b"zzz")?;
            assert!(
                beyond >= pe,
                "a key beyond the last block punches every data block",
            );

            Ok(())
        },
        Some(1),
        Some(|x| x),
    )
}

/// Writes `items` through an adaptive-index writer with the given spill
/// threshold and recovers the resulting [`Table`]. Returns the table plus
/// the backing temp dir (kept alive by the caller).
#[cfg(test)]
#[expect(clippy::unwrap_used)]
fn recover_adaptive_table(
    items: &[crate::InternalValue],
    spill_threshold: u64,
) -> crate::Result<(Table, tempfile::TempDir)> {
    // Writer::new opens the file exclusively, so the path must not exist yet.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("table");

    let mut writer =
        Writer::new(path.clone(), 0, 0, Arc::new(StdFs))?.use_adaptive_index(spill_threshold);
    for item in items {
        writer.write(item.clone())?;
    }
    let (_, checksum) = writer.finish()?.unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(Metrics::default());
    let table = Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics,
    )?;
    Ok((table, dir))
}

/// Adaptive index, small SST: a high spill threshold keeps the index
/// single-level (no separate index region), and every key reads back.
#[test]
#[expect(clippy::unwrap_used)]
fn adaptive_index_small_sst_is_single_level() -> crate::Result<()> {
    let items: Vec<_> = (0u32..500)
        .map(|i| {
            let key = format!("key-{i:08}");
            crate::InternalValue::from_components(
                key.as_bytes(),
                b"some-value-payload",
                0,
                crate::ValueType::Value,
            )
        })
        .collect();

    // Threshold far above any plausible index size for 500 keys.
    let (table, _dir) = recover_adaptive_table(&items, u64::MAX)?;

    assert!(
        table.regions.index.is_none(),
        "small index must stay single-level (Full), got a two-level index region",
    );
    assert_eq!(
        items.len(),
        usize::try_from(table.metadata.item_count).unwrap()
    );

    for i in 0u32..500 {
        let key = format!("key-{i:08}");
        let got = table.get(key.as_bytes(), SeqNo::MAX, hash64(key.as_bytes()))?;
        assert_eq!(
            b"some-value-payload",
            &*got.unwrap().value,
            "single-level read mismatch for {key}",
        );
    }
    Ok(())
}

/// Adaptive index, forced spill: a zero spill threshold forces the
/// two-level (partitioned) layout, and the same keys still read back —
/// proving both layouts round-trip identically.
#[test]
#[expect(clippy::unwrap_used)]
fn adaptive_index_zero_threshold_spills_to_two_level() -> crate::Result<()> {
    let items: Vec<_> = (0u32..500)
        .map(|i| {
            let key = format!("key-{i:08}");
            crate::InternalValue::from_components(
                key.as_bytes(),
                b"some-value-payload",
                0,
                crate::ValueType::Value,
            )
        })
        .collect();

    // Threshold 0 → spill on the first index entry → partitioned.
    let (table, _dir) = recover_adaptive_table(&items, 0)?;

    assert!(
        table.regions.index.is_some(),
        "zero threshold must spill to a two-level (partitioned) index",
    );
    assert_eq!(
        items.len(),
        usize::try_from(table.metadata.item_count).unwrap()
    );

    for i in 0u32..500 {
        let key = format!("key-{i:08}");
        let got = table.get(key.as_bytes(), SeqNo::MAX, hash64(key.as_bytes()))?;
        assert_eq!(
            b"some-value-payload",
            &*got.unwrap().value,
            "two-level read mismatch for {key}",
        );
    }
    Ok(())
}

#[test]
fn table_point_read_index_block_restart_interval() -> crate::Result<()> {
    let items: Vec<_> = (0u32..24)
        .map(|i| {
            let key = format!("adj:out:vertex-0001:edge-{i:04}");
            let value = format!("value-{i:04}");
            crate::InternalValue::from_components(
                key.as_bytes(),
                value.as_bytes(),
                u64::from(i),
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            assert_eq!(
                b"value-0011",
                &*table
                    .get(
                        b"adj:out:vertex-0001:edge-0011",
                        SeqNo::MAX,
                        hash64(b"adj:out:vertex-0001:edge-0011"),
                    )?
                    .expect("test assertion: expected value for edge-0011")
                    .value,
            );

            let range = table
                .range(
                    UserKey::from("adj:out:vertex-0001:edge-0008")
                        ..=UserKey::from("adj:out:vertex-0001:edge-0012"),
                )
                .flatten()
                .collect::<Vec<_>>();

            assert_eq!(items[8..=12], range);

            Ok(())
        },
        Some(1),
        Some(|writer: Writer| {
            writer
                .use_data_block_size(128)
                .use_index_block_restart_interval(4)
        }),
    )
}

#[test]
#[cfg(feature = "zstd")]
fn table_point_read_zstd_dictionary() -> crate::Result<()> {
    let dict = Arc::new(make_test_dictionary());
    let expected_dict_id = dict.id();
    let compression = crate::CompressionType::zstd_dict(3, expected_dict_id)?;
    let items = [
        crate::InternalValue::from_components(
            b"key-00001",
            b"value-00001-padding-to-make-it-longer",
            3,
            crate::ValueType::Value,
        ),
        crate::InternalValue::from_components(
            b"key-00002",
            b"value-00002-padding-to-make-it-longer",
            2,
            crate::ValueType::Value,
        ),
    ];

    test_with_table_and_zstd_dictionary(
        &items,
        |table| {
            assert!(matches!(
                table.metadata.data_block_compression,
                crate::CompressionType::ZstdDict { dict_id, .. } if dict_id == expected_dict_id
            ));
            assert_eq!(items, &*table.iter().flatten().collect::<Vec<_>>());
            assert_eq!(
                b"value-00001-padding-to-make-it-longer",
                &*table
                    .get(b"key-00001", SeqNo::MAX, hash64(b"key-00001"),)?
                    .expect("test assertion: expected value for key-00001")
                    .value,
            );
            Ok(())
        },
        None,
        Some(|writer: Writer| writer.use_data_block_compression(compression)),
        dict,
    )
}

#[test]
fn table_range_exclusive_bounds() -> crate::Result<()> {
    use core::ops::Bound::{Excluded, Included};

    let items = [
        crate::InternalValue::from_components(b"a", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"v", 0, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            let res = table
                .range((Excluded(UserKey::from("b")), Included(UserKey::from("d"))))
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items.iter().skip(2).take(2).cloned().collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Included(UserKey::from("d"))))
                .rev()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items
                    .iter()
                    .skip(2)
                    .take(2)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Excluded(UserKey::from("d"))))
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Excluded(UserKey::from("d"))))
                .rev()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items
                    .iter()
                    .skip(2)
                    .take(1)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*res,
            );

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_data_block_size(1)),
    )
}

#[test]
fn writer_records_effective_page_ecc_descriptor() -> crate::Result<()> {
    // descriptor#page_ecc must record the EFFECTIVE (compiled) setting, not
    // the requested flag. Without the `page_ecc` cargo feature,
    // use_page_ecc(true) is a no-op (with_ecc() is identity, no parity is
    // emitted and no ECC_PARITY bit is set), so the persisted descriptor
    // must read false to stay consistent with the actual on-disk blocks.
    // With the feature it reads true. `cfg!(feature = "page_ecc")` is the
    // effective value either way.
    let items = [crate::InternalValue::from_components(
        b"a",
        b"v",
        0,
        crate::ValueType::Value,
    )];
    test_with_table(
        &items,
        |table| {
            assert_eq!(
                table.metadata.page_ecc,
                cfg!(feature = "page_ecc"),
                "descriptor#page_ecc must reflect the effective (compiled) page_ecc setting",
            );
            Ok(())
        },
        None,
        Some(|w: Writer| {
            w.use_page_ecc(
                true,
                crate::runtime_config::EccScheme::ReedSolomon {
                    data_shards: 4,
                    parity_shards: 2,
                },
            )
        }),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read_mvcc_block_boundary() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"5", 5, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"4", 4, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"3", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"2", 2, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"1", 1, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(2, table.metadata.data_block_count);

            let key_hash = hash64(b"a");

            assert_eq!(
                b"5",
                &*table.get(b"a", SeqNo::MAX, key_hash)?.unwrap().value
            );
            assert_eq!(b"4", &*table.get(b"a", 5, key_hash)?.unwrap().value);
            assert_eq!(b"3", &*table.get(b"a", 4, key_hash)?.unwrap().value);
            assert_eq!(b"2", &*table.get(b"a", 3, key_hash)?.unwrap().value);
            assert_eq!(b"1", &*table.get(b"a", 2, key_hash)?.unwrap().value);

            Ok(())
        },
        Some(3),
        Some(|x| x),
    )
}

#[test]
fn table_scan() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(items, &*table.scan()?.flatten().collect::<Vec<_>>());

            assert_eq!(
                table.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"xyz".into())),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_iter_simple() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(items, &*table.iter().flatten().collect::<Vec<_>>());
            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*table.iter().rev().flatten().collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_simple() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(
                items.iter().skip(1).cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..)
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..)
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_ping_pong() -> crate::Result<()> {
    let items = (0u64..10)
        .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value))
        .collect::<Vec<_>>();

    test_with_table(
        &items,
        |table| {
            let mut iter =
                table.range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

            let mut count = 0;

            for x in 0.. {
                if x % 2 == 0 {
                    let Some(_) = iter.next() else {
                        break;
                    };

                    count += 1;
                } else {
                    let Some(_) = iter.next_back() else {
                        break;
                    };

                    count += 1;
                }
            }

            assert_eq!(5, count);

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_multiple_data_blocks() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(5, table.metadata.data_block_count);

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items
                    .iter()
                    .skip(1)
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_data_block_size(1)),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read_partitioned_filter_smoke_test() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(1, table.metadata.data_block_count);

            for item in &items {
                let key_hash = hash64(&item.key.user_key);

                assert_eq!(
                    item.value,
                    table
                        .get(&item.key.user_key, SeqNo::MAX, key_hash)
                        .unwrap()
                        .unwrap()
                        .value,
                );
            }

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_partitioned_filter()),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_partitioned_filter() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", "a7", 7, Value),
        InternalValue::from_components("a", "a6", 6, Value),
        InternalValue::from_components("a", "a5", 5, Value),
        InternalValue::from_components("a", "a4", 4, Value),
        InternalValue::from_components("a", "a3", 3, Value),
        InternalValue::from_components("b", "b5", 5, Value),
        InternalValue::from_components("c", "c8", 8, Value),
        InternalValue::from_components("d", "d10", 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert!(table.regions.filter.is_some(), "filter should exist");
            assert!(
                table.regions.filter_tli.is_some(),
                "filter TLI should exist"
            );

            assert_eq!(b"a7", &*table.get(b"a", 8, hash64(b"a"))?.unwrap().value,);
            assert_eq!(b"a6", &*table.get(b"a", 7, hash64(b"a"))?.unwrap().value,);
            assert_eq!(b"a5", &*table.get(b"a", 6, hash64(b"a"))?.unwrap().value,);
            assert_eq!(b"a4", &*table.get(b"a", 5, hash64(b"a"))?.unwrap().value,);
            assert_eq!(b"a3", &*table.get(b"a", 4, hash64(b"a"))?.unwrap().value,);
            assert_eq!(b"b5", &*table.get(b"b", 6, hash64(b"b"))?.unwrap().value,);
            assert_eq!(b"c8", &*table.get(b"c", 9, hash64(b"c"))?.unwrap().value,);
            assert_eq!(b"d10", &*table.get(b"d", 11, hash64(b"d"))?.unwrap().value,);
            Ok(())
        },
        None,
        Some(|x: Writer| x.use_partitioned_filter().use_meta_partition_size(3)),
    )
}

#[test]
#[expect(clippy::unwrap_used, reason = "test code")]
fn plan_block_tasks_propagates_a_faulted_bloom_probe() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    // A partitioned, unpinned filter makes `check_bloom` read a filter partition
    // block from disk on lookup; that read is the planning I/O that can fail.
    let items: Vec<InternalValue> = (0..64u32)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:04}").into_bytes(),
                b"v".to_vec(),
                1,
                crate::ValueType::Value,
            )
        })
        .collect();

    let dir = tempdir()?;
    let file = dir.path().join("table");
    let injector = Arc::new(FaultInjector::new());
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(FaultFs::with_injector(StdFs, Arc::clone(&injector)));

    let checksum = {
        let mut writer = Writer::new(file.clone(), 0, 0, Arc::clone(&fs))?
            .use_partitioned_filter()
            .use_meta_partition_size(8);
        for item in &items {
            writer.write(item.clone())?;
        }
        writer.finish()?.unwrap().1
    };

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::clone(&fs),
        false, // do not pin index
        false, // do not pin filter: partition blocks read lazily
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(Metrics::default()),
    )?;

    // Recovery is done (its reads passed cleanly); now fail the NEXT positional
    // read of the table file: the filter partition block `check_bloom` loads.
    injector.arm(FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other)).on_path("table"));

    let key = b"key0000".as_slice();
    let sorted = [(key, hash64(key))];
    let result = table.plan_block_tasks(&sorted, SeqNo::MAX);

    // The serial path propagates a bloom-probe error via `?`; the chunked planner
    // must too, so a faulted probe surfaces as Err instead of a swallowed miss
    // that would let a stale lower level answer.
    assert!(
        result.is_err(),
        "a faulted bloom-probe read must surface as Err, not a swallowed miss"
    );
    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test code")]
fn plan_block_tasks_propagates_a_faulted_index_read() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    // 500 keys + adaptive-index threshold 0 spill to a two-level (partitioned)
    // block index whose partition blocks are read lazily, so `block_iter.next()`
    // does a disk read that can fail. A pinned filter keeps check_bloom off disk,
    // so the planner's first faulted positional read is the index read.
    let items: Vec<InternalValue> = (0u32..500)
        .map(|i| {
            InternalValue::from_components(
                format!("key{i:06}").into_bytes(),
                b"v".to_vec(),
                1,
                crate::ValueType::Value,
            )
        })
        .collect();

    let dir = tempdir()?;
    let file = dir.path().join("table");
    let injector = Arc::new(FaultInjector::new());
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(FaultFs::with_injector(StdFs, Arc::clone(&injector)));

    let checksum = {
        let mut writer = Writer::new(file.clone(), 0, 0, Arc::clone(&fs))?.use_adaptive_index(0);
        for item in &items {
            writer.write(item.clone())?;
        }
        writer.finish()?.unwrap().1
    };

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::clone(&fs),
        true,  // pin filter: keep check_bloom off disk
        false, // do not pin index: partition blocks read lazily
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(Metrics::default()),
    )?;

    // Recovery is done; fail the next positional read of the table file, which
    // the block-index walk performs.
    injector.arm(FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other)).on_path("table"));

    let key = b"key000250".as_slice();
    let sorted = [(key, hash64(key))];
    let result = table.plan_block_tasks(&sorted, SeqNo::MAX);

    // batch_get propagates the same iterator error via `?`; the chunked planner
    // must too, so a faulted index read surfaces as Err instead of a swallowed
    // end-of-index that would let a stale lower level answer.
    assert!(
        result.is_err(),
        "a faulted block-index read must surface as Err, not a swallowed end-of-index"
    );
    Ok(())
}

#[test]
fn table_seqnos() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", nanoid::nanoid!().as_bytes(), 7, Value),
        InternalValue::from_components("b", nanoid::nanoid!().as_bytes(), 5, Value),
        InternalValue::from_components("c", nanoid::nanoid!().as_bytes(), 8, Value),
        InternalValue::from_components("d", nanoid::nanoid!().as_bytes(), 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(5, table.metadata.seqnos.0);
            assert_eq!(10, table.metadata.seqnos.1);
            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_zero_bpk() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", nanoid::nanoid!().as_bytes(), 7, Value),
        InternalValue::from_components("b", nanoid::nanoid!().as_bytes(), 5, Value),
        InternalValue::from_components("c", nanoid::nanoid!().as_bytes(), 8, Value),
        InternalValue::from_components("d", nanoid::nanoid!().as_bytes(), 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert!(table.regions.filter.is_none());
            Ok(())
        },
        None,
        Some(|x: Writer| x.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0.0))),
    )
}

#[test]
#[expect(
    clippy::unreadable_literal,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
#[cfg(not(feature = "metrics"))]
fn table_read_fuzz_1() -> crate::Result<()> {
    use crate::Slice;
    use crate::ValueType::{Tombstone, Value};

    let items = [
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            18340908174618760209,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            18054235897395861447,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([103]),
            17820711698989577060,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            17652351990810576660,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            17576667967203573449,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([30]),
            16889403751796995588,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([186]),
            15595956295177086731,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            15512796775024989213,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([188, 156, 59, 85, 13]),
            15149465603839159843,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([174, 71]),
            15102256701513339307,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([35, 148]),
            15091160407760527013,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14675333203365509622,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([245]),
            14571905818510788533,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14541113699969547298,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14486387191240337417,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14112006182482717758,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([159]),
            13992512869528291746,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13915106262991388976,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13597506620670366065,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13064400463180401957,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            12969967266897711474,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            12508372658468564628,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([138]),
            11795269606598686255,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([18]),
            10730214428751858128,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([236]),
            10124645034840293700,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([216, 81]),
            9559308046784608794,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([79]),
            8607115510826103394,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7963767336149785641,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7882646634183551394,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7719307175583565930,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([111]),
            7522791039398476411,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([227, 164, 129]),
            7410771579448817672,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7003757491682295965,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5723101273557106371,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5581364419922287132,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([119, 29]),
            5541782075650463683,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5136199042703471864,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5051972816573966850,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([162]),
            5020119417385108821,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([69]),
            4325966282181409009,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            4238714774310338082,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            4200824275757201410,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([92, 145, 251, 240, 133]),
            3894954012280195585,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([14]),
            3814525464013269105,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            3766663710061910506,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([129]),
            3749655073597306832,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([231]),
            3319226033273656005,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            3274394613296787928,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            2045761581956846404,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([78]),
            1704041985603476880,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            1441130125005023946,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([164, 136]),
            1225420702887300153,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([55]),
            974698856173325051,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([238, 237]),
            47340610649818236,
            Value,
        ),
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([0, 161]),
            Slice::from([]),
            17872519117933825384,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0, 161]),
            Slice::from([]),
            4494664966150999400,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([1]),
            Slice::from([]),
            15373275907316083975,
            Value,
        ),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let data_block_size = 97;

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0, Arc::new(StdFs))
        .unwrap()
        .use_data_block_size(data_block_size);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        0,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        Arc::new(StdFs),
        true,
        true,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
    )
    .unwrap();

    let item_count_usize = table.metadata.item_count as usize;
    assert_eq!(item_count_usize, items.len());

    assert_eq!(items.len(), item_count_usize);
    let items = items.into_iter().collect::<Vec<_>>();

    assert_eq!(items, table.iter().collect::<Result<Vec<_>, _>>().unwrap());
    assert_eq!(
        items.iter().rev().cloned().collect::<Vec<_>>(),
        table.iter().rev().collect::<Result<Vec<_>, _>>().unwrap(),
    );

    {
        let lo = 0;
        let hi = 54;

        let lo_key = &items[lo].key.user_key;
        let hi_key = &items[hi].key.user_key;

        assert_eq!(lo_key, hi_key);

        let expected_range: Vec<_> = items[lo..=hi].to_vec();

        let iter = table.range(lo_key..=hi_key);

        assert_eq!(expected_range, iter.collect::<Result<Vec<_>, _>>().unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_partitioned_index() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", "a7", 7, Value),
        InternalValue::from_components("a", "a6", 6, Value),
        InternalValue::from_components("a", "a5", 5, Value),
        InternalValue::from_components("a", "a4", 4, Value),
        InternalValue::from_components("a", "a3", 3, Value),
        InternalValue::from_components("b", "b5", 5, Value),
        InternalValue::from_components("c", "c8", 8, Value),
        InternalValue::from_components("d", "d10", 10, Value),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0, Arc::new(StdFs))
        .unwrap()
        .use_partitioned_index()
        .use_data_block_size(5)
        .use_meta_partition_size(3);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        0,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        Arc::new(StdFs),
        true,
        true,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::default(),
    )
    .unwrap();

    assert!(
        table.regions.index.is_some(),
        "2nd-level index should exist",
    );

    assert!(
        table.metadata.index_block_count > 1,
        "should use partitioned index",
    );

    assert_eq!(b"a7", &*table.get(b"a", 8, hash64(b"a"))?.unwrap().value,);
    assert_eq!(b"a6", &*table.get(b"a", 7, hash64(b"a"))?.unwrap().value,);
    assert_eq!(b"a5", &*table.get(b"a", 6, hash64(b"a"))?.unwrap().value,);
    assert_eq!(b"a4", &*table.get(b"a", 5, hash64(b"a"))?.unwrap().value,);
    assert_eq!(b"a3", &*table.get(b"a", 4, hash64(b"a"))?.unwrap().value,);
    assert_eq!(b"b5", &*table.get(b"b", 6, hash64(b"b"))?.unwrap().value,);
    assert_eq!(b"c8", &*table.get(b"c", 9, hash64(b"c"))?.unwrap().value,);
    assert_eq!(b"d10", &*table.get(b"d", 11, hash64(b"d"))?.unwrap().value,);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_global_seqno() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a0", "a0", 0, Value),
        InternalValue::from_components("a1", "a1", 1, Value),
        InternalValue::from_components("b", "b", 8, Value),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0, Arc::new(StdFs))
        .unwrap()
        .use_partitioned_filter()
        .use_data_block_size(1)
        .use_meta_partition_size(1);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        7,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        Arc::new(StdFs),
        true,
        true,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::default(),
    )
    .unwrap();

    // global seqno is 7, so a1 is = 8 -> can not be read by snapshot=8
    assert!(table.get(b"a1", 8, hash64(b"a1"))?.is_none());

    assert_eq!(b"a0", &*table.get(b"a0", 8, hash64(b"a0"))?.unwrap().value,);

    Ok(())
}

/// Pins `Table::get` returning items with **global** seqno coordinates
/// even when the on-disk block carries `seqno = 0`. Mirrors the upstream
/// regression test for the equivalent fix in fjall-rs/lsm-tree (commit
/// bad4fe0a). Our fork's structural fix lives at the `Table::get` /
/// `get_with_block` / `batch_get` boundary (each call site adds
/// `global_seqno` back via `saturating_add` after the table-local
/// `point_read`), rather than inside `point_read` itself, but the
/// caller-observable contract is the same: a recovered ingested item
/// is returned with its effective global seqno, not the on-disk
/// table-local seqno.
///
/// A regression that drops the `saturating_add(global_seqno)` step
/// (e.g. a refactor that flattens `point_read` directly into `get`
/// without re-applying the offset) would fail this test by returning
/// `seqno = 0` instead of `seqno = SEQNO`.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn table_return_global_seqno() -> crate::Result<()> {
    use crate::ValueType::Value;
    use crate::fs::StdFs;

    const SEQNO: SeqNo = 15;

    let items = [InternalValue::from_components("abc", "abc", 0, Value)];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;

    for item in items {
        writer.write(item)?;
    }

    let _trailer = writer.finish()?;

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        SEQNO,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        Arc::new(StdFs),
        true,
        true,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::default(),
    )?;

    // On disk: seqno = 0. Effective global seqno: 0 + SEQNO = SEQNO.
    // Snapshot = 2 * SEQNO is above the effective seqno, so the read sees the item.
    // Returned value MUST carry the effective global seqno (= SEQNO),
    // not the table-local seqno (= 0) it has on disk.
    assert_eq!(
        InternalValue::from_components("abc", "abc", SEQNO, Value),
        table.get(b"abc", 2 * SEQNO, hash64(b"abc"))?.unwrap(),
    );

    Ok(())
}

/// Build a [`Block`] from raw bytes for `decode_range_tombstones` tests.
#[expect(
    clippy::expect_used,
    reason = "test helper: data length is controlled and fits in u32"
)]
fn rt_block(data: Vec<u8>) -> Block {
    let data_length = u32::try_from(data.len()).expect("test buffer fits in u32");
    Block {
        header: block::Header {
            data_length,
            uncompressed_length: data_length,
            ..block::Header::test_dummy(block::BlockType::RangeTombstone)
        },
        data: data.into(),
    }
}

/// Assert `decode_range_tombstones` returns [`RangeTombstoneDecode`](crate::Error::RangeTombstoneDecode)
/// with the given field and expected byte offset.
fn assert_rt_decode_error(data: Vec<u8>, expected_field: &str, expected_offset: u64) {
    let block = rt_block(data);
    // Uses DefaultUserComparator: tests verify structural decode errors
    // (truncation, missing fields), not comparator-dependent ordering.
    match Table::decode_range_tombstones(&block, &crate::comparator::DefaultUserComparator) {
        Err(crate::Error::RangeTombstoneDecode { field, offset }) => {
            assert_eq!(
                field, expected_field,
                "expected field '{expected_field}', got '{field}'"
            );
            assert_eq!(
                offset, expected_offset,
                "expected offset {expected_offset}, got {offset}"
            );
        }
        other => panic!(
            "expected RangeTombstoneDecode {{ field: \"{expected_field}\" }}, got: {other:?}"
        ),
    }
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_invalid_interval_returns_error() {
    use crate::io::{LE, WriteBytesExt};

    // Build a single tombstone where start ("z") >= end ("a")
    let mut buf = Vec::new();
    buf.write_u16::<LE>(1).unwrap(); // start_len
    buf.extend_from_slice(b"z");
    buf.write_u16::<LE>(1).unwrap(); // end_len
    buf.extend_from_slice(b"a");
    buf.write_u64::<LE>(1).unwrap(); // seqno

    assert_rt_decode_error(buf, "interval", 0);
}

#[test]
fn decode_range_tombstones_truncated_start_len_returns_error() {
    // Only 1 byte — not enough for u16 start_len; offset = 0 (entry start)
    assert_rt_decode_error(vec![0x01], "start_len", 0);
}

#[test]
fn decode_range_tombstones_empty_block_returns_error() {
    // Empty RT block payload is corruption — writer only creates an RT block
    // handle when at least one tombstone exists.
    assert_rt_decode_error(Vec::new(), "start_len", 0);
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_start_len_exceeds_remaining_returns_error() {
    use crate::io::{LE, WriteBytesExt};

    // start_len = 100 but only 1 byte of data follows; offset = 0 (entry start)
    let mut buf = Vec::new();
    buf.write_u16::<LE>(100).unwrap();
    buf.push(0xFF);

    assert_rt_decode_error(buf, "start_len", 0);
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_truncated_end_len_returns_error() {
    use crate::io::{LE, WriteBytesExt};

    // Valid start_len + start, then truncated before end_len completes
    // offset = 3 (after u16 start_len + 1-byte key)
    let mut buf = Vec::new();
    buf.write_u16::<LE>(1).unwrap(); // start_len = 1
    buf.push(b'a'); // start key
    buf.push(0x01); // only 1 byte of end_len (need 2)

    assert_rt_decode_error(buf, "end_len", 3);
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_end_len_exceeds_remaining_returns_error() {
    use crate::io::{LE, WriteBytesExt};

    // Valid start, then end_len = 100 but only 1 byte follows
    // offset = 3 (after u16 start_len + 1-byte key)
    let mut buf = Vec::new();
    buf.write_u16::<LE>(1).unwrap(); // start_len
    buf.push(b'a'); // start key
    buf.write_u16::<LE>(100).unwrap(); // end_len = 100
    buf.push(0xFF); // only 1 byte

    assert_rt_decode_error(buf, "end_len", 3);
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_truncated_seqno_returns_error() {
    use crate::io::{LE, WriteBytesExt};

    // Valid start + end, but seqno truncated (only 4 of 8 bytes)
    // offset = 6 (after u16+1+u16+1 = 6 bytes for start/end fields)
    let mut buf = Vec::new();
    buf.write_u16::<LE>(1).unwrap(); // start_len
    buf.push(b'a'); // start key
    buf.write_u16::<LE>(1).unwrap(); // end_len
    buf.push(b'z'); // end key
    buf.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // 4 bytes of seqno (need 8)

    assert_rt_decode_error(buf, "seqno", 6);
}

/// Exercises the `load_block` cache-miss and cache-hit paths for
/// `BlockType::RangeTombstone`, verifying that the dedicated RT metrics
/// counters are incremented instead of the data-block counters.
#[test]
#[cfg(feature = "metrics")]
fn load_block_range_tombstone_metrics() -> crate::Result<()> {
    use crate::{
        CompressionType,
        cache::Cache,
        descriptor_table::DescriptorTable,
        range_tombstone::RangeTombstone,
        table::{block::BlockType, util::load_block},
    };
    use core::sync::atomic::Ordering::Relaxed;

    let dir = tempdir()?;
    let file = dir.path().join("table");

    // Build a table that contains a range tombstone block.
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    writer.write(InternalValue::from_components(
        b"a",
        b"v1",
        1,
        crate::ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"z",
        b"v2",
        2,
        crate::ValueType::Value,
    ))?;
    writer.write_range_tombstone(RangeTombstone::new(b"b".into(), b"y".into(), 3));
    #[expect(
        clippy::unwrap_used,
        reason = "finish() returns Some after writing data items"
    )]
    let (_, checksum) = writer.finish()?.unwrap();

    let metrics = Arc::new(crate::metrics::Metrics::default());

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        0,
        // Recovery bypasses load_block() (reads via Block::from_file() directly),
        // so it intentionally does NOT increment block-load metrics — consistent
        // with how filter and index recovery reads are handled.
        Arc::new(Cache::with_capacity_bytes(10_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics.clone(),
    )?;

    let rt_handle = table
        .regions
        .range_tombstones
        .expect("table should have range tombstone block");

    let table_id = table.global_id();

    // Recovery does NOT increment block-load counters (bypasses load_block).
    assert_eq!(0, metrics.range_tombstone_block_load_io.load(Relaxed));

    // Use a fresh cache so the first load_block() call is a cache miss.
    let fresh_cache = Arc::new(Cache::with_capacity_bytes(10_000_000));

    // load_block cache miss → IO path
    let _block = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &rt_handle,
        BlockType::RangeTombstone,
        CompressionType::None,
        None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
        #[cfg(feature = "metrics")]
        &metrics,
    )?;

    assert_eq!(1, metrics.range_tombstone_block_load_io.load(Relaxed));
    assert_eq!(0, metrics.range_tombstone_block_load_cached.load(Relaxed));
    assert!(metrics.range_tombstone_block_io_requested.load(Relaxed) > 0);
    assert_eq!(0, metrics.data_block_load_io.load(Relaxed));

    // load_block cache hit (block was inserted into fresh_cache by previous call)
    let _block = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &rt_handle,
        BlockType::RangeTombstone,
        CompressionType::None,
        None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
        #[cfg(feature = "metrics")]
        &metrics,
    )?;

    assert_eq!(1, metrics.range_tombstone_block_load_io.load(Relaxed));
    assert_eq!(1, metrics.range_tombstone_block_load_cached.load(Relaxed));
    assert_eq!(0, metrics.data_block_load_cached.load(Relaxed));

    Ok(())
}

/// Regression test for <https://github.com/structured-world/coordinode-lsm-tree/issues/198>:
/// `load_block` must validate the cached block's `block_type` against the
/// caller's expected type.  Before the fix the cache-hit path returned the
/// block unconditionally, so a corrupted block handle pointing at a cached
/// block of the wrong type (e.g. a data block at an index block offset)
/// would slip through without an error.
#[test]
fn load_block_cache_hit_rejects_wrong_block_type() -> crate::Result<()> {
    use crate::{
        CompressionType,
        cache::Cache,
        descriptor_table::DescriptorTable,
        table::{block::BlockType, util::load_block},
    };

    let dir = tempdir()?;
    let file = dir.path().join("table");

    // Build a minimal table with one data block.
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    writer.write(InternalValue::from_components(
        b"a",
        b"v1",
        1,
        crate::ValueType::Value,
    ))?;
    let (_, checksum) = writer
        .finish()?
        .expect("finish() returns Some after writing data items");

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::metrics::Metrics::default());

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(10_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics.clone(),
    )?;

    let table_id = table.global_id();

    // The range-tombstone block handle is type-specific, but every table has a
    // TLI (top-level index) block whose handle we can reuse.  Load it first as
    // an Index block (correct type) so it lands in the cache.
    let tli_handle = table.regions.tli;
    let fresh_cache = Arc::new(Cache::with_capacity_bytes(10_000_000));

    let _block = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &tli_handle,
        BlockType::Index,
        CompressionType::None,
        None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
        #[cfg(feature = "metrics")]
        &metrics,
    )?;

    // Now request the same offset but claim it is a Data block.  The block is
    // already cached (as Index), so the cache-hit path must detect the type
    // mismatch and return `Error::InvalidTag`.
    let result = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &tli_handle,
        BlockType::Data,
        CompressionType::None,
        None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
        #[cfg(feature = "metrics")]
        &metrics,
    );

    assert!(
        matches!(&result, Err(crate::Error::InvalidTag(("BlockType", _)))),
        "expected InvalidTag for block type mismatch on cache hit, got Ok or wrong Err",
    );

    Ok(())
}

/// A read that recovers a data block from its Page-ECC parity, and confirms the
/// on-disk fault persists across a cache-bypassing re-read, must record the SST
/// in the heal sink for a healing recompaction. A clean read records nothing.
#[cfg(feature = "page_ecc")]
#[test]
fn load_block_records_heal_hint_on_persistent_ecc_correction() -> crate::Result<()> {
    use crate::{
        Cache, InternalValue,
        descriptor_table::DescriptorTable,
        fs::StdFs,
        heal_hints::HealHints,
        table::{
            BlockHandle,
            block::{BlockType, EccParams, Header},
            util::load_block,
        },
    };

    let dir = tempdir()?;
    let file = dir.path().join("table");

    // Build a table whose data blocks carry RS(4,2) parity.
    let scheme = EccParams::RS_4_2;
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?.use_ecc(Some(scheme));
    for i in 0..200u32 {
        let key = format!("key{i:05}");
        writer.write(InternalValue::from_components(
            key.as_bytes(),
            b"value-payload-bytes",
            u64::from(i) + 1,
            crate::ValueType::Value,
        ))?;
    }
    #[expect(
        clippy::unwrap_used,
        reason = "finish() returns Some after writing items"
    )]
    let (_, checksum) = writer.finish()?.unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::metrics::Metrics::default());
    let table = Table::recover(
        file.clone(),
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(10_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics.clone(),
    )?;

    let table_id = table.global_id();
    let compression = table.metadata.data_block_compression;

    // First data block.
    #[expect(clippy::unwrap_used, reason = "table has at least one data block")]
    let keyed = table.block_index.iter().next().unwrap()?;
    let handle = BlockHandle::new(keyed.offset(), keyed.size());

    // Clean read with an enabled sink: a non-corrected read records nothing.
    {
        let clean_sink = HealHints::default();
        clean_sink.set_enabled(true);
        let fresh_cache = Cache::with_capacity_bytes(10_000_000);
        let _block = load_block(
            table_id,
            &table.path,
            &table.file_accessor,
            &fresh_cache,
            &handle,
            BlockType::Data,
            compression,
            None,
            table.metadata.ecc_params,
            #[cfg(zstd_any)]
            None,
            Some(&clean_sink),
            #[cfg(feature = "metrics")]
            &metrics,
        )?;
        assert!(
            clean_sink.snapshot().is_empty(),
            "a clean read must not record a heal hint",
        );
    }

    // Flip one payload byte of the first data block so the read must repair it
    // via RS parity, and drop any cached fd so the re-read re-opens the tampered
    // file from disk (confirming the fault is persistent).
    let mut bytes = std::fs::read(&file)?;
    // `as usize` is a target-conditional truncation (only narrows on 32-bit
    // pointer widths); `allow`, not `expect`, so it stays clean on the 64-bit
    // host where clippy frames it purely as a portability note.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "in-file block offset fits usize; only narrows on 32-bit targets"
    )]
    let pos = handle.offset().0 as usize + Header::MIN_LEN + 3;
    bytes[pos] ^= 0x80;
    std::fs::write(&file, &bytes)?;
    table.file_accessor.remove_for_table(&table_id);

    let sink = HealHints::default();
    sink.set_enabled(true);
    let fresh_cache = Cache::with_capacity_bytes(10_000_000);
    let block = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &handle,
        BlockType::Data,
        compression,
        None,
        table.metadata.ecc_params,
        #[cfg(zstd_any)]
        None,
        Some(&sink),
        #[cfg(feature = "metrics")]
        &metrics,
    )?;
    assert_eq!(
        block.header.block_type,
        BlockType::Data,
        "repaired read still yields a valid data block",
    );
    assert_eq!(
        sink.snapshot(),
        vec![table_id],
        "a persistent ECC correction must queue the SST for healing",
    );

    // A DISABLED sink (auto_heal off) corrects on read but records nothing.
    table.file_accessor.remove_for_table(&table_id);
    let off_sink = HealHints::default(); // enabled == false
    let fresh_cache = Cache::with_capacity_bytes(10_000_000);
    let block = load_block(
        table_id,
        &table.path,
        &table.file_accessor,
        &fresh_cache,
        &handle,
        BlockType::Data,
        compression,
        None,
        table.metadata.ecc_params,
        #[cfg(zstd_any)]
        None,
        Some(&off_sink),
        #[cfg(feature = "metrics")]
        &metrics,
    )?;
    assert_eq!(
        block.header.block_type,
        BlockType::Data,
        "disabled auto-heal still returns repaired data",
    );
    assert!(
        off_sink.snapshot().is_empty(),
        "auto_heal off must not schedule a rewrite",
    );

    Ok(())
}

/// End-to-end corruption test: tamper on-disk `seqno#kv_max` so it exceeds
/// `seqno#max`, then verify that `ParsedMeta::load_with_handle` rejects the
/// file with an `InvalidData` error.
///
/// Covers the validation path in `validated_kv_seqno` via the real on-disk
/// deserialization pipeline (not just the unit-level helper).
#[test]
#[expect(
    clippy::expect_used,
    reason = "test invariants: key and value patterns must exist in the meta block"
)]
#[expect(
    clippy::indexing_slicing,
    reason = "test fixture: deliberate slice operations on controlled meta block bytes"
)]
fn meta_seqno_kv_max_corruption_returns_invalid_data() -> crate::Result<()> {
    use super::block::Header;
    use super::meta::ParsedMeta;
    use super::regions::ParsedRegions;
    use crate::coding::{Decode, Encode};
    use std::io::{Seek, Write};

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table");

    // Write a valid table with KV entries at seqnos 1..=5.
    // Both seqno#max and seqno#kv_max will be 5.
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    for (i, key) in (b'a'..=b'e').enumerate() {
        writer.write(InternalValue::from_components(
            [key],
            b"val",
            (i as u64) + 1,
            crate::ValueType::Value,
        ))?;
    }
    #[expect(
        clippy::unwrap_used,
        reason = "finish() returns Some after writing data items"
    )]
    let _ = writer.finish()?.unwrap();

    // Find the meta block region, tamper the seqno#kv_max value in the
    // payload, recompute the block checksum so the corruption reaches
    // the metadata validation layer (not caught by block checksum).
    {
        let mut f = std::fs::File::open(&file)?;
        let trailer = crate::sfa::Reader::from_reader(&mut f)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;
        let meta_handle = regions.metadata;

        let raw_block =
            crate::file::read_exact(&f, *meta_handle.offset(), meta_handle.size() as usize)?;

        // Meta blocks carry the block_flags byte, so their header is
        // header_len(Meta), not the SST MIN_LEN.
        let header_len = Header::header_len(crate::table::block::BlockType::Meta);
        let payload = &raw_block[header_len..];

        // Find the seqno#kv_max value bytes in the payload and replace
        // with u64::MAX (exceeds seqno#max = 5).
        let needle = b"seqno#kv_max";
        let key_pos = payload
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("seqno#kv_max key must be present in the meta block payload");

        // Meta entries are stored in a DataBlock with restart_interval = 1, so
        // keys are written as full keys followed by an InternalValue payload
        // (value_type, seqno, value length, etc.) encoded using varints.  We do
        // not rely on the exact field layout here; instead, we scan forward from
        // the end of the key string to find the first occurrence of the LE-encoded
        // u64 value in the payload.
        let search_start = key_pos + needle.len();
        let original_le = 5u64.to_le_bytes();
        let val_rel = payload[search_start..]
            .windows(original_le.len())
            .position(|w| w == original_le)
            .expect("original LE value must appear after the key");
        let val_offset_in_payload = search_start + val_rel;

        let mut tampered_payload = payload.to_vec();
        tampered_payload[val_offset_in_payload..val_offset_in_payload + 8]
            .copy_from_slice(&u64::MAX.to_le_bytes());

        // Rebuild the header with the correct checksum over the
        // tampered payload so Block::from_file accepts the block.
        let mut orig_header = Header::decode_from(&mut &raw_block[..header_len])?;
        orig_header.checksum = crate::Checksum::from_raw(crate::hash::hash128(&tampered_payload));
        let new_header = orig_header.encode_into_vec();

        // Write the tampered block back into the file at the meta
        // block's original offset.
        let mut wf = std::fs::OpenOptions::new().write(true).open(&file)?;
        wf.seek(std::io::SeekFrom::Start(*meta_handle.offset()))?;
        wf.write_all(&new_header)?;
        wf.write_all(&tampered_payload)?;
        wf.sync_all()?;
    }

    // Re-open the (now corrupted) file and attempt to load metadata.
    {
        let mut f = std::fs::File::open(&file)?;
        let trailer = crate::sfa::Reader::from_reader(&mut f)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        let result = ParsedMeta::load_with_handle(&f, &regions.metadata, None, None);

        let err = result.expect_err("corrupted seqno#kv_max should cause an error");
        assert!(
            matches!(&err, crate::Error::Io(e) if e.kind() == crate::io::ErrorKind::InvalidData),
            "expected InvalidData, got: {err:?}",
        );
    }

    Ok(())
}

/// `meta_mid` and `meta` (TAIL) must encode the SAME `created_at`. The
/// writer was generating `unix_timestamp()` independently inside each
/// `write_meta_section` call, so MID and TAIL would observe slightly
/// different wall-clock values. After recovery via MID the table would
/// report a different creation time than after recovery via TAIL,
/// which silently shifts TTL / FIFO ordering depending on which copy
/// the reader fell back to.
#[test]
fn meta_mid_and_tail_have_identical_created_at() -> crate::Result<()> {
    use super::meta::ParsedMeta;
    use super::regions::ParsedRegions;

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table");

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    for (i, key) in (b'a'..=b'e').enumerate() {
        writer.write(InternalValue::from_components(
            [key],
            b"val",
            (i as u64) + 1,
            crate::ValueType::Value,
        ))?;
    }
    #[expect(
        clippy::unwrap_used,
        reason = "finish() returns Some after writing data items"
    )]
    let _ = writer.finish()?.unwrap();

    let mut f = std::fs::File::open(&file)?;
    let trailer = crate::sfa::Reader::from_reader(&mut f)?;
    let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

    let tail = ParsedMeta::load_with_handle(&f, &regions.metadata, None, None)?;
    let mid_handle = regions
        .metadata_mid
        .expect("writer must emit meta_mid alongside meta");
    let mid = ParsedMeta::load_with_handle(&f, &mid_handle, None, None)?;

    assert_eq!(
        tail.created_at, mid.created_at,
        "MID and TAIL meta copies must share an identical created_at \
         (writer must snapshot the timestamp once and pass it to both \
         write_meta_section calls; observed tail={:?} mid={:?})",
        tail.created_at, mid.created_at,
    );

    Ok(())
}

/// `meta_mid` and `meta` (TAIL) must encode the SAME `file_size`. The
/// writer was stamping MID with a 0 sentinel and the reader was
/// patching the value with `std::fs::metadata(path).len()` on MID
/// fallback — that path (a) bypasses the pluggable `Fs` backend (so
/// `Table::recover` would fail on MemFs / io_uring trees the moment it
/// touched the MID fallback branch), and (b) reported the entire
/// physical file length (including TOC, trailer, and the TAIL meta
/// block itself), while TAIL stores `self.meta.file_pos` taken before
/// any of those tail sections were written. Recovered tables therefore
/// reported wildly different sizes depending on which meta copy survived.
///
/// `self.meta.file_pos` is only ever incremented inside `spill_block()`
/// (data-block writes). The index/tli/filter/range-tombstone writes,
/// the MID meta block itself, the linked_blob_files / table_version /
/// meta_separator raw sections, and the TAIL meta block all leave it
/// unchanged. So the value is identical at MID and TAIL write time —
/// MID can encode it directly, no recovery-time patching, no
/// `std::fs::metadata` call.
#[test]
fn meta_mid_and_tail_have_identical_file_size() -> crate::Result<()> {
    use super::meta::ParsedMeta;
    use super::regions::ParsedRegions;

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table");

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    for (i, key) in (b'a'..=b'e').enumerate() {
        writer.write(InternalValue::from_components(
            [key],
            b"val",
            (i as u64) + 1,
            crate::ValueType::Value,
        ))?;
    }
    #[expect(
        clippy::unwrap_used,
        reason = "finish() returns Some after writing data items"
    )]
    let _ = writer.finish()?.unwrap();

    let mut f = std::fs::File::open(&file)?;
    let trailer = crate::sfa::Reader::from_reader(&mut f)?;
    let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

    let tail = ParsedMeta::load_with_handle(&f, &regions.metadata, None, None)?;
    let mid_handle = regions
        .metadata_mid
        .expect("writer must emit meta_mid alongside meta");
    let mid = ParsedMeta::load_with_handle(&f, &mid_handle, None, None)?;

    assert_eq!(
        tail.file_size, mid.file_size,
        "MID and TAIL meta copies must store an identical file_size \
         (both observe the same `self.meta.file_pos` because no \
         post-data section bumps it); observed tail={} mid={}",
        tail.file_size, mid.file_size,
    );
    assert_ne!(
        mid.file_size, 0,
        "MID file_size must not be the legacy 0 sentinel — that pushed \
         the recovery path through std::fs::metadata, which bypasses \
         the pluggable Fs backend"
    );

    Ok(())
}

/// `bloom_may_contain_key` with full (non-partitioned) filter delegates to
/// `bloom_may_contain_hash`. Both methods agree for full filters.
#[test]
fn bloom_may_contain_key_full_filter() -> crate::Result<()> {
    let items: Vec<InternalValue> = ["a", "c", "e"]
        .iter()
        .enumerate()
        .map(|(i, &k)| {
            InternalValue::from_components(k, "v", i as u64 + 1, crate::ValueType::Value)
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            let hash_a = hash64(b"a");
            let hash_b = hash64(b"b");

            // Existing key: both methods must accept
            assert!(
                table.bloom_may_contain_key(b"a", hash_a)?,
                "bloom_may_contain_key must not reject existing key"
            );
            assert!(
                table.bloom_may_contain_key_hash(hash_a)?,
                "bloom_may_contain_key_hash must not reject existing key"
            );

            // For full filters, bloom_may_contain_key delegates to the same
            // hash-only path, so both methods return the same result.
            let key_result = table.bloom_may_contain_key(b"b", hash_b)?;
            let hash_result = table.bloom_may_contain_key_hash(hash_b)?;
            assert_eq!(
                key_result, hash_result,
                "full filter: key-based and hash-only should agree"
            );

            Ok(())
        },
        None,
        Some(|w: Writer| w.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(10.0))),
    )
}

/// `bloom_may_contain_key` with partitioned filter seeks the correct partition
/// and returns Ok(false) for a key beyond all partition boundaries.
///
/// Contrast: `bloom_may_contain_key_hash` returns Ok(true) conservatively
/// for the same key because it cannot seek partitions by hash alone.
/// This is the core behavioral improvement introduced by this PR.
#[test]
fn bloom_may_contain_key_partitioned_filter() -> crate::Result<()> {
    let items: Vec<InternalValue> = (0u64..100)
        .map(|i| {
            let key = format!("key_{i:04}");
            InternalValue::from_components(key, "v", i + 1, crate::ValueType::Value)
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            // Key that exists: both methods must accept
            let hash_exist = hash64(b"key_0050");
            assert!(
                table.bloom_may_contain_key(b"key_0050", hash_exist)?,
                "bloom must not reject existing key in partitioned filter"
            );

            // Key beyond all partitions: with a pinned partition index, key-based
            // seek finds no ceiling and must return Ok(false).
            // Note: pinned_filter_index is always loaded when filter_tli exists
            // (unconditional in Table::recover), so this is always the partition-aware path.
            let hash_beyond = hash64(b"zzz_beyond");
            assert!(
                !table.bloom_may_contain_key(b"zzz_beyond", hash_beyond)?,
                "key beyond all partitions should be rejected when partition index is available"
            );

            // Hash-only path always returns Ok(true) conservatively for partitioned filters
            assert!(
                table.bloom_may_contain_key_hash(hash_beyond)?,
                "hash-only bloom check should remain conservative for partitioned filters"
            );

            Ok(())
        },
        None,
        Some(|w: Writer| {
            w.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(10.0))
                .use_partitioned_filter()
        }),
    )
}

/// Regression test for #194: two-level index scan stops prematurely when
/// `from_block_with_bounds` returns `Ok(None)` for a child partition whose
/// entries are all outside the requested `[lo, hi]` window.
///
/// We build a table with a partitioned (two-level) index containing multiple
/// child partitions and then iterate through the block index with bounds that
/// span several partitions. Both forward (`next`) and reverse (`next_back`)
/// directions are verified to yield the correct block handle sequences.
///
/// NOTE: The `Ok(None)` child path cannot be triggered with well-formed
/// block data regardless of `restart_interval` — `trim_back_to_upper_bound`
/// always restores a covering entry when the stack empties, so
/// `seek_upper_bound_cursor` returns `true`. The `Ok(None)` branch fires
/// only when `fill_stack` or `advance_upper_restart_interval` encounters
/// a corrupt/malformed block (empty stack after decode failure). The fix
/// is therefore a defensive guard; this test validates overall iteration
/// correctness through the two-level path.
#[test]
fn two_level_index_scan_skips_empty_child_partition() -> crate::Result<()> {
    use crate::ValueType::Value;
    use crate::table::block_index::{BlockIndex, BlockIndexIter};

    // Eight distinct keys, each gets its own data block (block_size=1 byte).
    // meta_partition_size=3 is a very small byte budget for partitioned index
    // metadata, so the index writer splits child partitions aggressively
    // (effectively on or before the first handle), yielding multiple child
    // partitions for this test.
    let items: Vec<InternalValue> = ["a", "b", "c", "d", "e", "f", "g", "h"]
        .iter()
        .enumerate()
        .map(|(i, k)| InternalValue::from_components(*k, format!("v{i}"), (i + 1) as u64, Value))
        .collect();

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("two_level_skip");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_partitioned_index()
        .use_data_block_size(1)
        .use_meta_partition_size(3);

    for item in items.iter().cloned() {
        writer.write(item)?;
    }
    writer.finish()?;

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        0,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        Arc::new(StdFs),
        true,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::default(),
    )?;

    assert!(
        table.regions.index.is_some(),
        "table must use partitioned (two-level) index",
    );
    assert!(
        table.metadata.index_block_count > 1,
        "table must have >1 index partitions, got {}",
        table.metadata.index_block_count,
    );

    // --- full scan without bounds: collect all block handles ---
    let all_handles: Vec<_> = {
        let it = table.block_index.iter();
        it.collect::<Result<Vec<_>, _>>()?
    };
    assert_eq!(
        all_handles.len(),
        items.len(),
        "full scan should yield one block handle per data block",
    );

    // --- forward scan with lo bound ---
    // Seek past the first partition(s) to exercise the case where earlier
    // child partitions are empty after applying bounds.
    {
        let mut it = table.block_index.iter();
        assert!(it.seek_lower(b"d", u64::MAX));
        let forward_keys: Vec<_> = it
            .map(|r| r.map(|h| h.end_key().to_vec()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            forward_keys,
            vec![
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec(),
                b"g".to_vec(),
                b"h".to_vec(),
            ],
            "forward scan from 'd' should yield exactly d..h",
        );
    }

    // --- backward scan with hi bound ---
    // seek_upper("e", 0) positions the back cursor at the first handle
    // whose end_key > "e", which is "f". Reverse iteration starts from
    // "f" and works down to "a".
    {
        let mut it = table.block_index.iter();
        assert!(it.seek_upper(b"e", 0));
        let mut backward_keys = Vec::new();
        while let Some(res) = it.next_back() {
            backward_keys.push(res?.end_key().to_vec());
        }
        assert_eq!(
            backward_keys,
            vec![
                b"f".to_vec(),
                b"e".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ],
            "backward scan up to 'e' should yield f..a in reverse",
        );
    }

    // --- mixed forward + backward with both bounds ---
    {
        let mut it = table.block_index.iter();
        assert!(it.seek_lower(b"c", u64::MAX));
        assert!(it.seek_upper(b"f", 0));

        let mut forward_keys = vec![];
        let mut backward_keys = vec![];

        // Consume two from front
        if let Some(res) = it.next() {
            forward_keys.push(res?.end_key().to_vec());
        }
        if let Some(res) = it.next() {
            forward_keys.push(res?.end_key().to_vec());
        }

        // Consume from back
        while let Some(res) = it.next_back() {
            backward_keys.push(res?.end_key().to_vec());
        }

        // The block index is a sparse index: seek_upper positions the back
        // cursor at the first block whose end_key > hi, so next_back()
        // starts from "g" (the first handle past "f"), then works down
        // through "f" and "e" until the cursors meet.
        assert_eq!(forward_keys, vec![b"c".to_vec(), b"d".to_vec()]);
        assert_eq!(
            backward_keys,
            vec![b"g".to_vec(), b"f".to_vec(), b"e".to_vec()]
        );
        assert!(it.next().is_none(), "iterator should be exhausted");
    }

    Ok(())
}

#[test]
fn batch_get_empty_input_returns_empty_results() -> crate::Result<()> {
    let items = [crate::InternalValue::from_components(
        b"a",
        b"v",
        0,
        crate::ValueType::Value,
    )];
    test_with_table(
        &items,
        |table| {
            let r = table.batch_get(&[], SeqNo::MAX)?;
            assert!(r.is_empty(), "empty input must yield empty result vec");
            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn batch_get_single_block_multiple_keys_returns_in_input_order() -> crate::Result<()> {
    // Three keys, all fall in the same data block (default block
    // size is much larger than the few bytes here).
    let items: Vec<_> = ["a", "b", "c"]
        .iter()
        .enumerate()
        .map(|(i, k)| {
            crate::InternalValue::from_components(
                k.as_bytes(),
                format!("val-{k}").as_bytes(),
                u64::try_from(i).expect("test fixture index fits in u64"),
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            let batch: Vec<(&[u8], u64)> = vec![
                (b"a", hash64(b"a")),
                (b"b", hash64(b"b")),
                (b"c", hash64(b"c")),
            ];
            let results = table.batch_get(&batch, SeqNo::MAX)?;
            assert_eq!(results.len(), 3, "one result slot per input key");
            assert_eq!(&*results[0].as_ref().unwrap().value, b"val-a");
            assert_eq!(&*results[1].as_ref().unwrap().value, b"val-b");
            assert_eq!(&*results[2].as_ref().unwrap().value, b"val-c");
            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn batch_get_keys_spread_across_blocks_return_correct_values() -> crate::Result<()> {
    // Force one item per data block via tiny block size +
    // rotate_every=1. Then a batch covering keys from different
    // blocks must produce the correct value for each key. This
    // test asserts CORRECTNESS only — the "block loaded at most
    // once for the entire batch" perf claim is a property of the
    // implementation, verifiable through the block cache's
    // hit-rate counters under metrics instrumentation, but
    // deliberately not asserted here (the test would need to
    // hook the cache to count loads, which would couple to
    // internal cache mechanics).
    let items: Vec<_> = (0u32..8)
        .map(|i| {
            let key = format!("key-{i:04}");
            let value = format!("val-{i:04}");
            crate::InternalValue::from_components(
                key.as_bytes(),
                value.as_bytes(),
                u64::from(i),
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            // Pick 4 keys spread across the 8 blocks.
            let queries: Vec<(&[u8], u64)> = vec![
                (b"key-0000" as &[u8], hash64(b"key-0000")),
                (b"key-0002" as &[u8], hash64(b"key-0002")),
                (b"key-0005" as &[u8], hash64(b"key-0005")),
                (b"key-0007" as &[u8], hash64(b"key-0007")),
            ];
            let results = table.batch_get(&queries, SeqNo::MAX)?;
            assert_eq!(results.len(), 4);
            assert_eq!(&*results[0].as_ref().unwrap().value, b"val-0000");
            assert_eq!(&*results[1].as_ref().unwrap().value, b"val-0002");
            assert_eq!(&*results[2].as_ref().unwrap().value, b"val-0005");
            assert_eq!(&*results[3].as_ref().unwrap().value, b"val-0007");
            Ok(())
        },
        Some(1),
        Some(|writer: Writer| writer.use_data_block_size(64)),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn batch_get_missing_keys_return_none_present_keys_return_some() -> crate::Result<()> {
    let items: Vec<_> = ["b", "d", "f"]
        .iter()
        .enumerate()
        .map(|(i, k)| {
            crate::InternalValue::from_components(
                k.as_bytes(),
                format!("val-{k}").as_bytes(),
                u64::try_from(i).expect("test fixture index fits in u64"),
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            // Mix present and absent keys, sorted ascending.
            let batch: Vec<(&[u8], u64)> = vec![
                (b"a" as &[u8], hash64(b"a")), // absent (before any key)
                (b"b" as &[u8], hash64(b"b")), // present
                (b"c" as &[u8], hash64(b"c")), // absent (between b and d)
                (b"d" as &[u8], hash64(b"d")), // present
                (b"f" as &[u8], hash64(b"f")), // present (last key)
                (b"g" as &[u8], hash64(b"g")), // absent (after last key)
            ];
            let results = table.batch_get(&batch, SeqNo::MAX)?;
            assert_eq!(results.len(), 6);
            assert!(results[0].is_none(), "key 'a' is absent");
            assert_eq!(&*results[1].as_ref().unwrap().value, b"val-b");
            assert!(results[2].is_none(), "key 'c' is absent");
            assert_eq!(&*results[3].as_ref().unwrap().value, b"val-d");
            assert_eq!(&*results[4].as_ref().unwrap().value, b"val-f");
            assert!(results[5].is_none(), "key 'g' is absent");
            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn batch_get_matches_per_key_get() -> crate::Result<()> {
    // Cross-check: for every input key, `batch_get` and a per-key
    // `get` loop must produce identical results. This is the
    // regression guard against the batch path diverging from the
    // single-key path on any edge case (bloom misses, seqno
    // skew, block boundaries).
    let items: Vec<_> = (0u32..20)
        .map(|i| {
            let key = format!("k-{i:03}");
            let value = format!("v-{i:03}");
            crate::InternalValue::from_components(
                key.as_bytes(),
                value.as_bytes(),
                u64::from(i),
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            // Build a query batch with a mix of present, absent,
            // and out-of-range keys.
            let keys: Vec<Vec<u8>> = (0..25).map(|i| format!("k-{i:03}").into_bytes()).collect();
            let batch: Vec<(&[u8], u64)> = keys.iter().map(|k| (k.as_slice(), hash64(k))).collect();

            let batch_results = table.batch_get(&batch, SeqNo::MAX)?;
            let single_results: Vec<_> = batch
                .iter()
                .map(|&(k, h)| table.get(k, SeqNo::MAX, h))
                .collect::<crate::Result<Vec<_>>>()?;

            assert_eq!(batch_results.len(), single_results.len());
            for (i, (b, s)) in batch_results.iter().zip(&single_results).enumerate() {
                assert_eq!(
                    b,
                    s,
                    "batch/single divergence at index {i} (key={})",
                    String::from_utf8_lossy(&keys[i]),
                );
            }
            Ok(())
        },
        Some(2),
        Some(|writer: Writer| writer.use_data_block_size(96)),
    )
}

#[test]
fn batch_get_same_user_key_across_block_boundary_finds_older_visible_version() -> crate::Result<()>
{
    // Regression for the multi-block MVCC walk bug in batch_get.
    //
    // The bug: when batch_get's inner loop hits a key with
    // `key == block.end_key` AND `point_read` returns None
    // (no visible entry in this block), the loop advanced `p`
    // unconditionally — so the walk skipped to the NEXT batch
    // key without checking whether the SAME user key continues
    // into the NEXT block. `Table::get` handles this case via
    // `point_read_inner`'s end-key boundary check; the batch
    // path must mirror it.
    //
    // To trigger the bug we need:
    //   1. `forward_reader` lands at a block whose end_key
    //      equals some batched key K, and
    //   2. that block has no visible version of K at the query
    //      seqno, and
    //   3. the next block contains the visible version of K.
    //
    // Single-key fixtures don't reproduce: `forward_reader` is
    // seqno-aware enough to seek past a block that has no
    // visible entries for the lone passing key, so the iter
    // lands at block 1 directly. We need a SECOND batched key
    // earlier in the order to force the seek to land at
    // block 0 (which IS the block for that earlier key), so
    // the later batched key then exercises the equal-end-key /
    // None-point_read / "look in next block" path.
    //
    // Fixture: user keys "0" (one version at seqno=1) +
    // five versions of "a" (seqno 5 → 1), `rotate_every=3`.
    // Internal-key sort puts "0" before any "a"; the resulting
    // blocks are:
    //   block 0: [0@1, a@5, a@4]   end_key="a"
    //   block 1: [a@3, a@2, a@1]   end_key="a"
    //
    // Query batch = [("0", h0), ("a", ha)] at snapshot seqno=3.
    // forward_reader seeks to block 0 to satisfy "0".
    // The "a" then sees end_key="a" with no visible version
    // in block 0 (all seqnos ≥ 3) — the fix must keep the
    // walk going into block 1 where a@2 is visible.
    let items = [
        crate::InternalValue::from_components(b"0", b"zero", 1, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"5", 5, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"4", 4, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"3", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"2", 2, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"1", 1, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(2, table.metadata.data_block_count);

            let batch: Vec<(&[u8], u64)> = vec![(b"0", hash64(b"0")), (b"a", hash64(b"a"))];

            // snapshot seqno=3: visible seqnos < 3.
            //   "0" → 0@1 (only version, visible)
            //   "a" → a@2 (largest visible; a@5/4/3 are not)
            let results = table.batch_get(&batch, 3)?;
            assert_eq!(results.len(), 2);
            assert_eq!(
                &*results[0]
                    .as_ref()
                    .expect("0@1 must be found in block 0")
                    .value,
                b"zero",
            );
            assert_eq!(
                &*results[1]
                    .as_ref()
                    .expect("a@2 must be found via block 1")
                    .value,
                b"2",
                "batch_get must walk past block 0 (end_key=a, but all a-seqnos ≥3) \
                 into block 1 (end_key=a, seqnos 2 and 1) to find the visible version \
                 at snapshot 3",
            );

            // Sanity: cross-check against Table::get for both keys.
            let single_zero = table.get(b"0", 3, hash64(b"0"))?;
            let single_a = table.get(b"a", 3, hash64(b"a"))?;
            assert_eq!(
                results[0], single_zero,
                "batch_get must match Table::get for '0'"
            );
            assert_eq!(
                results[1], single_a,
                "batch_get must match Table::get for 'a'"
            );
            Ok(())
        },
        Some(3),
        Some(|x| x),
    )
}

/// Builds an SST from `items`, optionally with parallel block compression
/// (`parallel_threads`), applying `config` to the writer, then recovers it.
/// Returns the table plus the temp dir (kept alive for the table's lifetime).
#[cfg(all(test, feature = "parallel"))]
#[expect(clippy::unwrap_used, reason = "test code")]
fn build_and_recover(
    items: &[crate::InternalValue],
    parallel_threads: Option<usize>,
    config: impl Fn(Writer) -> Writer,
) -> crate::Result<(Table, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("table");

    let mut writer = config(Writer::new(path.clone(), 0, 0, Arc::new(StdFs))?);
    if let Some(threads) = parallel_threads {
        let spawner = Arc::new(crate::table::writer::RayonSpawner::with_threads(threads)?);
        writer = writer.use_parallel_compression(spawner, threads);
    }
    for item in items {
        writer.write(item.clone())?;
    }
    let (_, checksum) = writer.finish()?.unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(Metrics::default());
    let table = Table::recover(
        path,
        checksum,
        0,
        0,
        0,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics,
    )?;
    Ok((table, dir))
}

/// The parallel block-compression pipeline must produce an SST functionally
/// identical to the serial path: workers compress out of order, but the writer
/// drains and frames blocks strictly in submission order, so block boundaries,
/// scan order, contents and index entries are unchanged. (The on-disk data
/// section is in fact byte-identical; only the `created_at` metadata timestamp
/// varies between builds, so we compare recovered content rather than raw
/// bytes.) Checked across the encode + transform variations that flow through
/// the pipeline.
#[cfg(feature = "parallel")]
#[test]
fn parallel_compression_matches_serial_output() -> crate::Result<()> {
    // Enough keys, with small blocks, to force many data-block spills so the
    // pipeline genuinely reorders work across its 4 workers.
    let items: Vec<_> = (0u32..4000)
        .map(|i| {
            crate::InternalValue::from_components(
                format!("key{i:08}").as_bytes(),
                format!("value-{i}-some-payload-bytes").as_bytes(),
                u64::from(i),
                crate::ValueType::Value,
            )
        })
        .collect();

    let check = |config: &dyn Fn(Writer) -> Writer, label: &str| -> crate::Result<()> {
        let (serial, _ds) = build_and_recover(&items, None, config)?;
        let (parallel, _dp) = build_and_recover(&items, Some(4), config)?;

        // Identical block boundaries and item count.
        assert_eq!(
            serial.metadata.data_block_count, parallel.metadata.data_block_count,
            "{label}: data_block_count must match"
        );
        assert_eq!(
            serial.metadata.item_count, parallel.metadata.item_count,
            "{label}: item_count must match"
        );

        // Identical scan content and order.
        let s: Vec<_> = serial.iter().collect::<crate::Result<_>>()?;
        let p: Vec<_> = parallel.iter().collect::<crate::Result<_>>()?;
        assert_eq!(s.len(), items.len(), "{label}: all items must scan back");
        assert_eq!(s, p, "{label}: scan content/order must match serial");

        // Index resolves point reads identically (sampled across the key space).
        for i in (0..items.len()).step_by(137) {
            let key = format!("key{i:08}");
            let hash = hash64(key.as_bytes());
            assert_eq!(
                serial.get(key.as_bytes(), crate::SeqNo::MAX, hash)?,
                parallel.get(key.as_bytes(), crate::SeqNo::MAX, hash)?,
                "{label}: point read for {key} must match"
            );
        }
        Ok(())
    };

    check(&|w| w.use_data_block_size(256), "plain")?;
    check(
        &|w| w.use_data_block_size(256).use_seqno_in_index(true),
        "seqno_in_index",
    )?;
    #[cfg(feature = "lz4")]
    check(
        &|w| {
            w.use_data_block_size(256)
                .use_data_block_compression(CompressionType::Lz4)
        },
        "lz4",
    )?;

    Ok(())
}

#[test]
fn zone_map_section_roundtrips_one_entry_per_block() -> crate::Result<()> {
    // 200 keys with frequent block rotation force many data blocks, so the
    // section must carry several entries that survive write + reopen.
    let items: Vec<crate::InternalValue> = (0..200u32)
        .map(|i| {
            crate::InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                0,
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            let zm = &table.zone_map;
            assert!(!zm.is_empty(), "zone map should be populated when enabled");
            assert!(
                zm.len() >= 2,
                "rotation should yield several blocks, got {}",
                zm.len()
            );
            Ok(())
        },
        Some(20),
        Some(|w: Writer| w.use_zone_map(true)),
    )
}

#[test]
fn zone_map_corrupt_section_falls_back_instead_of_failing_open() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let checksum = {
        let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?.use_zone_map(true);
        for i in 0..200u32 {
            if i % 20 == 0 {
                writer.spill_block()?;
            }
            writer.write(crate::InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                b"v".to_vec(),
                0,
                crate::ValueType::Value,
            ))?;
        }
        writer.finish()?.expect("table written").1
    };

    let recover = || -> crate::Result<Table> {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());
        Table::recover(
            file.clone(),
            checksum,
            0,
            0,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Some(Arc::new(DescriptorTable::new(10))),
            Arc::new(StdFs),
            false,
            false,
            None,
            #[cfg(zstd_any)]
            None,
            crate::comparator::default_comparator(),
            #[cfg(feature = "metrics")]
            metrics,
        )
    };

    // First open: the zone-map section is present and populated.
    let zm_handle = {
        let table = recover()?;
        assert!(!table.zone_map.is_empty(), "zone map should be populated");
        table.regions.zone_map.expect("zone-map section present")
    };

    // Corrupt one byte inside the zone-map section block on disk so its
    // checksum / AEAD rejects it on the next open.
    let mut bytes = std::fs::read(&file)?;
    let corrupt_at = usize::try_from(zm_handle.offset().0).expect("offset fits usize") + 4;
    *bytes
        .get_mut(corrupt_at)
        .expect("corruption offset within file") ^= 0xFF;
    std::fs::write(&file, &bytes)?;

    // Second open: a corrupt OPTIONAL zone-map is derived, non-authoritative
    // metadata — it must NOT fail table open. It degrades to no block-skip (an
    // empty map), and the rest of the table still loads intact.
    let table = recover()?;
    assert!(
        table.zone_map.is_empty(),
        "corrupt zone-map section should disable block-skip, not fail open"
    );
    assert_eq!(
        table.metadata.item_count, 200,
        "the rest of the table must still load with a corrupt zone map"
    );

    Ok(())
}

#[test]
fn zone_map_absent_without_policy() -> crate::Result<()> {
    let items: Vec<crate::InternalValue> = (0..50u32)
        .map(|i| {
            crate::InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                b"v".to_vec(),
                0,
                crate::ValueType::Value,
            )
        })
        .collect();

    test_with_table(
        &items,
        |table| {
            assert!(
                table.zone_map.is_empty(),
                "no zone map should be loaded without the policy"
            );
            Ok(())
        },
        None,
        None::<fn(Writer) -> Writer>,
    )
}

/// Helper: recover a freshly written table from `file` with default test config.
fn recover_test_table(file: &std::path::Path, checksum: Checksum) -> crate::Result<Table> {
    recover_test_table_with_id(file, checksum, 0)
}

fn recover_test_table_with_id(
    file: &std::path::Path,
    checksum: Checksum,
    table_id: TableId,
) -> crate::Result<Table> {
    #[cfg(feature = "metrics")]
    let metrics = Arc::new(Metrics::default());
    Table::recover(
        file.to_path_buf(),
        checksum,
        0,
        0,
        table_id,
        Arc::new(Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
        Arc::new(StdFs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        metrics,
    )
}

#[test]
fn delete_bitmap_section_round_trips() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let keys: [&[u8]; 8] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
    // Row positions follow write order: rows 0, 2, 5 are keys a, c, f.
    let deleted_rows = [0u32, 2, 5];

    // A delete-bitmap SST must carry a zone map (per-block row counts power the
    // positional masking; recovery enforces this).
    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?.use_zone_map(true);
    for key in keys {
        writer.write(crate::InternalValue::from_components(
            key,
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
    }
    for &row in &deleted_rows {
        writer.delete_bitmap_mut().insert(row);
    }
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    assert!(
        table.regions.delete_bitmap.is_some(),
        "delete-bitmap section must be present when rows are deleted"
    );
    let dv = table.delete_bitmap();
    assert_eq!(dv.len(), deleted_rows.len() as u64);
    for row in 0..8u32 {
        assert_eq!(
            dv.contains(row),
            deleted_rows.contains(&row),
            "row {row} membership mismatch after reopen"
        );
    }
    Ok(())
}

#[test]
fn delete_bitmap_section_absent_when_no_deletes() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?;
    writer.write(crate::InternalValue::from_components(
        b"a",
        b"v",
        1,
        crate::ValueType::Value,
    ))?;
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    assert!(
        table.regions.delete_bitmap.is_none(),
        "no delete-bitmap section when the segment has no deletes"
    );
    assert!(table.delete_bitmap().is_empty());
    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn delete_bitmap_masks_rows_in_columnar_scan() -> crate::Result<()> {
    use crate::table::columnar::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, column_batch_to_entries,
    };

    let dir = tempdir()?;
    let file = dir.path().join("table");

    let n = 64u32;
    // Row positions follow write (= key) order: these are keys k0003/k0010/k0050.
    let deleted = [3u32, 10, 50];

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        writer.write(crate::InternalValue::from_components(
            key,
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
    }
    for &row in &deleted {
        writer.delete_bitmap_mut().insert(row);
    }
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    let batches =
        table.columnar_scan(&[COL_USER_KEY, COL_SEQNO, COL_VALUE_TYPE, COL_VALUE], None)?;

    let mut got: Vec<Vec<u8>> = Vec::new();
    for batch in &batches {
        for entry in column_batch_to_entries(batch)? {
            got.push(entry.key.user_key.to_vec());
        }
    }

    let expected: Vec<Vec<u8>> = (0..n)
        .filter(|i| !deleted.contains(i))
        .map(|i| format!("k{i:04}").into_bytes())
        .collect();
    assert_eq!(
        got, expected,
        "deleted row positions must be masked out of the columnar scan"
    );
    Ok(())
}

#[test]
fn copy_on_write_strategy_suppresses_the_delete_bitmap_section() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .delete_strategy(crate::config::DeleteStrategy::CopyOnWrite);
    for key in [b"a".as_ref(), b"b", b"c"] {
        writer.write(crate::InternalValue::from_components(
            key,
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
    }
    // Mark a row deleted; copy-on-write drops rows instead of masking, so it must
    // not persist a bitmap section even though a position was marked.
    writer.delete_bitmap_mut().insert(1);
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    assert!(
        table.regions.delete_bitmap.is_none(),
        "copy-on-write must not persist a delete-bitmap section"
    );
    assert!(table.delete_bitmap().is_empty());
    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn delete_bitmap_masks_rows_in_range_scan() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let n = 64u32;
    // Row positions follow write (= key) order: keys k0003 / k0010 / k0050.
    let deleted = [3u32, 10, 50];

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        writer.write(crate::InternalValue::from_components(
            key,
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
    }
    for &row in &deleted {
        writer.delete_bitmap_mut().insert(row);
    }
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    // A full forward range scan goes through the columnar reconstruction +
    // positional mask; deleted positions must never be yielded.
    let got: Vec<Vec<u8>> = table
        .range_iter(..)
        .map(|r| r.map(|kv| kv.key.user_key.to_vec()))
        .collect::<crate::Result<Vec<_>>>()?;

    let expected: Vec<Vec<u8>> = (0..n)
        .filter(|i| !deleted.contains(i))
        .map(|i| format!("k{i:04}").into_bytes())
        .collect();
    assert_eq!(
        got, expected,
        "deleted row positions must be masked out of the range scan"
    );
    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn delete_bitmap_masks_deleted_key_in_point_read() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let n = 64u32;
    // Row positions follow write (= key) order: keys k0003 / k0010 / k0050.
    let deleted = [3u32, 10, 50];

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        writer.write(crate::InternalValue::from_components(
            key,
            b"v",
            1,
            crate::ValueType::Value,
        ))?;
    }
    for &row in &deleted {
        writer.delete_bitmap_mut().insert(row);
    }
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;

    // A deleted key reads as absent; a live key is still found.
    for i in 0..n {
        let key = format!("k{i:04}").into_bytes();
        let got = table.get(&key, SeqNo::MAX, hash64(&key))?;
        if deleted.contains(&i) {
            assert!(got.is_none(), "deleted key {i} must read as absent");
        } else {
            assert!(got.is_some(), "live key {i} must be found");
        }
    }
    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn write_columnar_batch_stores_value_subcolumns_and_round_trips() -> crate::Result<()> {
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch, unframe_value_cells};

    let dir = tempdir()?;
    let file = dir.path().join("table");

    // A consumer batch: the intrinsic columns for two sorted keys, with the
    // single value column replaced by two value sub-columns (a fixed-4 + a bytes).
    // Per-row seqnos are 0 (the ingest contract; the table assigns the seqno).
    let mut batch = entries_to_column_batch(&[
        crate::InternalValue::from_components(b"k0", b"ignored", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"k1", b"ignored", 0, crate::ValueType::Value),
    ])?;
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: vec![1, 0, 0, 0, 2, 0, 0, 0],
    });
    let mut bytes_data = Vec::new();
    for off in [0u32, 2, 5] {
        bytes_data.extend_from_slice(&off.to_le_bytes());
    }
    bytes_data.extend_from_slice(b"aabbb");
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    writer.write_columnar_batch(&batch, &crate::comparator::default_comparator())?;
    let (_, checksum) = writer.finish()?.expect("table written");

    let table = recover_test_table(&file, checksum)?;
    assert!(table.metadata.columnar, "segment must be columnar");

    // Point reads reconstruct the framed value, which unframes to the original
    // sub-cells.
    let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
    let v0 = table
        .get(b"k0", SeqNo::MAX, hash64(b"k0"))?
        .expect("k0 present");
    assert_eq!(
        unframe_value_cells(v0.value.as_ref(), &tags)?,
        vec![&[1, 0, 0, 0][..], &b"aa"[..]],
    );
    let v1 = table
        .get(b"k1", SeqNo::MAX, hash64(b"k1"))?
        .expect("k1 present");
    assert_eq!(
        unframe_value_cells(v1.value.as_ref(), &tags)?,
        vec![&[2, 0, 0, 0][..], &b"bbb"[..]],
    );
    Ok(())
}

/// A positional delete-bitmap masks whole rows of a value-sub-column segment in
/// both the point and the projection read paths: the mask is value-agnostic, so
/// deleting by position hides each row's intrinsic key and every sub-column
/// while survivors keep their reconstructed sub-cells and projected bytes.
#[cfg(feature = "columnar")]
#[test]
fn delete_bitmap_masks_value_subcolumns_in_point_and_projection_reads() -> crate::Result<()> {
    use crate::fs::SyncMode;
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch, unframe_value_cells};
    use crate::table::delete_bitmap::DeleteBitmap;

    let dir = tempdir()?;
    let src = dir.path().join("src");
    let out = dir.path().join("out");

    // Five rows; the value is split into a fixed-4 (col 3) and a bytes (col 4)
    // sub-column. fixed values 10,20,30,40,50; bytes "a","bb","ccc","dddd","eeeee".
    let fixed: [u32; 5] = [10, 20, 30, 40, 50];
    let payloads: [&[u8]; 5] = [b"a", b"bb", b"ccc", b"dddd", b"eeeee"];
    let mut batch = entries_to_column_batch(
        &(0..5u32)
            .map(|i| {
                crate::InternalValue::from_components(
                    format!("k{i}").into_bytes(),
                    b"x",
                    0, // ingest contract: per-row seqno is 0
                    crate::ValueType::Value,
                )
            })
            .collect::<Vec<_>>(),
    )?;
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(4),
        validity: None,
        data: fixed.iter().flat_map(|v| v.to_le_bytes()).collect(),
    });
    let mut bytes_data = Vec::new();
    let mut acc = 0u32;
    bytes_data.extend_from_slice(&acc.to_le_bytes());
    for p in payloads {
        acc += u32::try_from(p.len()).unwrap();
        bytes_data.extend_from_slice(&acc.to_le_bytes());
    }
    for p in payloads {
        bytes_data.extend_from_slice(p);
    }
    batch.columns.push(Column {
        column_id: 4,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: bytes_data,
    });

    let mut writer = Writer::new(src.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    writer.write_columnar_batch(&batch, &crate::comparator::default_comparator())?;
    let (_, checksum) = writer.finish()?.expect("source written");
    let source = recover_test_table(&src, checksum)?;

    // Mask rows 1 and 3 by position (value-agnostic) and relocate.
    let mut bitmap = DeleteBitmap::new();
    bitmap.insert(1);
    bitmap.insert(3);
    let out_checksum =
        source.relocate_columnar_with_deletes(&out, &StdFs, 1, &bitmap, SyncMode::Normal)?;
    let relocated = recover_test_table_with_id(&out, out_checksum, 1)?;

    // Point path: masked rows read absent; survivors reconstruct their sub-cells.
    let tags = [TypeTag::Fixed(4), TypeTag::Bytes];
    for i in 0..5u32 {
        let key = format!("k{i}").into_bytes();
        let got = relocated.get(&key, SeqNo::MAX, hash64(&key))?;
        if i == 1 || i == 3 {
            assert!(got.is_none(), "masked row {i} must read absent");
        } else {
            let v = got.expect("survivor present");
            assert_eq!(
                unframe_value_cells(v.value.as_ref(), &tags)?,
                vec![&fixed[i as usize].to_le_bytes()[..], payloads[i as usize]],
                "survivor {i} sub-cells",
            );
        }
    }

    // Projection path: a scan over sub-column 3 yields the survivors only, with
    // their fixed bytes; the masked rows never appear.
    let batches = relocated.columnar_scan(&[3], None)?;
    let mut col3 = Vec::new();
    let mut rows = 0u32;
    for b in &batches {
        assert!(
            b.columns.iter().all(|c| c.column_id == 3),
            "projection decodes only sub-column 3",
        );
        rows += b.row_count;
        for c in b.columns.iter().filter(|c| c.column_id == 3) {
            col3.extend_from_slice(&c.data);
        }
    }
    assert_eq!(rows, 3, "two of five rows masked out of the projection");
    let want: Vec<u8> = [10u32, 30, 50]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    assert_eq!(col3, want, "projected fixed bytes are the survivors");

    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn write_columnar_batch_accounts_tombstones_seqno_bounds_and_restart_locator() -> crate::Result<()>
{
    use crate::config::{LocatorPolicyEntry, LocatorPrecision};
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch};

    let dir = tempdir()?;
    let file = dir.path().join("t");
    // Distinct keys with mixed value types plus one fixed value sub-column,
    // written with seqno-in-index and restart-precision locator enabled so the
    // tombstone / weak-tombstone accounting, the seqno-bounds, and the
    // restart-precision locator branches all run.
    let mut batch = entries_to_column_batch(&[
        crate::InternalValue::from_components(b"k0", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"k1", b"", 0, crate::ValueType::Tombstone),
        crate::InternalValue::from_components(b"k2", b"", 0, crate::ValueType::WeakTombstone),
    ])?;
    batch.columns.pop();
    batch.columns.push(Column {
        column_id: 3,
        type_tag: TypeTag::Fixed(2),
        validity: None,
        data: vec![1, 1, 2, 2, 3, 3],
    });

    let mut writer = Writer::new(file.clone(), 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_seqno_in_index(true)
        .use_locator(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        });
    writer.write_columnar_batch(&batch, &crate::comparator::default_comparator())?;
    let (_, checksum) = writer.finish()?.expect("table written");
    let table = recover_test_table(&file, checksum)?;

    // is_tombstone covers Tombstone + WeakTombstone; weak count is just the latter.
    assert_eq!(table.metadata.tombstone_count, 2, "two tombstone-kind rows");
    assert_eq!(table.metadata.weak_tombstone_count, 1, "one weak tombstone");
    // The seqno-bounds section is written and loaded: every ingested row carries
    // seqno 0, so the recovered bounds are exactly (0, 0). This proves the
    // use_seqno_in_index path ran rather than relying on the point read alone
    // (which can succeed through the normal index path regardless).
    assert_eq!(
        table.metadata.seqnos,
        (0, 0),
        "columnar ingest writes local seqno bounds of (0, 0)",
    );
    assert!(
        table.get(b"k0", SeqNo::MAX, hash64(b"k0"))?.is_some(),
        "the live row reads back",
    );
    Ok(())
}

#[cfg(feature = "columnar")]
#[test]
fn write_columnar_batch_on_an_empty_batch_writes_no_block() -> crate::Result<()> {
    use crate::table::columnar::{Column, TypeTag};

    let dir = tempdir()?;
    let file = dir.path().join("t");
    // An empty batch (zero rows) carrying the intrinsic columns plus a value
    // sub-column writes no block and returns no last key.
    let empty = crate::table::columnar::ColumnBatch {
        row_count: 0,
        columns: vec![
            Column {
                column_id: 0,
                type_tag: TypeTag::Bytes,
                validity: None,
                data: 0u32.to_le_bytes().to_vec(),
            },
            Column {
                column_id: 1,
                type_tag: TypeTag::Fixed(8),
                validity: None,
                data: Vec::new(),
            },
            Column {
                column_id: 2,
                type_tag: TypeTag::Fixed(1),
                validity: None,
                data: Vec::new(),
            },
            Column {
                column_id: 3,
                type_tag: TypeTag::Fixed(4),
                validity: None,
                data: Vec::new(),
            },
        ],
    };
    let mut writer = Writer::new(file, 0, 0, Arc::new(StdFs))?
        .use_columnar(true)
        .use_zone_map(true);
    assert!(
        writer
            .write_columnar_batch(&empty, &crate::comparator::default_comparator())?
            .is_none(),
        "an empty batch yields no last key",
    );
    // Finishing must also produce no SST: a None last key alone does not prove
    // the "writes no block" contract (a buggy writer could emit a table yet
    // still return None).
    assert!(
        writer.finish()?.is_none(),
        "an empty batch must not produce an SST",
    );
    Ok(())
}
