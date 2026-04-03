// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#![allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    reason = "test code"
)]
#![expect(clippy::expect_used, reason = "test code")]

use super::*;
use crate::{
    config::BloomConstructionPolicy, fs::StdFs,
    table::filter::standard_bloom::Builder as BloomBuilder,
};
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
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
                FileAccessor::DescriptorTable(..)
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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
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
                    .get(b"abc", SeqNo::MAX, BloomBuilder::get_hash(b"abc"))?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                None,
                table.get(b"def", SeqNo::MAX, BloomBuilder::get_hash(b"def"))?,
            );
            assert_eq!(
                None,
                table.get(b"____", SeqNo::MAX, BloomBuilder::get_hash(b"____"))?,
            );

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
                        BloomBuilder::get_hash(b"adj:out:vertex-0001:edge-0011"),
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
                    .get(
                        b"key-00001",
                        SeqNo::MAX,
                        BloomBuilder::get_hash(b"key-00001"),
                    )?
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
    use std::ops::Bound::{Excluded, Included};

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

            let key_hash = BloomBuilder::get_hash(b"a");

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
                let key_hash = BloomBuilder::get_hash(&item.key.user_key);

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

            assert_eq!(
                b"a7",
                &*table
                    .get(b"a", 8, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a6",
                &*table
                    .get(b"a", 7, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a5",
                &*table
                    .get(b"a", 6, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a4",
                &*table
                    .get(b"a", 5, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a3",
                &*table
                    .get(b"a", 4, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"b5",
                &*table
                    .get(b"b", 6, BloomBuilder::get_hash(b"b"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"c8",
                &*table
                    .get(b"c", 9, BloomBuilder::get_hash(b"c"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"d10",
                &*table
                    .get(b"d", 11, BloomBuilder::get_hash(b"d"))?
                    .unwrap()
                    .value,
            );
            Ok(())
        },
        None,
        Some(|x: Writer| x.use_partitioned_filter().use_meta_partition_size(3)),
    )
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
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
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
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
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

    assert_eq!(
        b"a7",
        &*table
            .get(b"a", 8, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a6",
        &*table
            .get(b"a", 7, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a5",
        &*table
            .get(b"a", 6, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a4",
        &*table
            .get(b"a", 5, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a3",
        &*table
            .get(b"a", 4, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"b5",
        &*table
            .get(b"b", 6, BloomBuilder::get_hash(b"b"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"c8",
        &*table
            .get(b"c", 9, BloomBuilder::get_hash(b"c"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"d10",
        &*table
            .get(b"d", 11, BloomBuilder::get_hash(b"d"))?
            .unwrap()
            .value,
    );

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
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
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
    assert!(
        table
            .get(b"a1", 8, BloomBuilder::get_hash(b"a1"))?
            .is_none()
    );

    assert_eq!(
        b"a0",
        &*table
            .get(b"a0", 8, BloomBuilder::get_hash(b"a0"))?
            .unwrap()
            .value,
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
            block_type: block::BlockType::RangeTombstone,
            checksum: crate::Checksum::from_raw(0),
            data_length,
            uncompressed_length: data_length,
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
    use byteorder::{LE, WriteBytesExt};

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
    use byteorder::{LE, WriteBytesExt};

    // start_len = 100 but only 1 byte of data follows; offset = 0 (entry start)
    let mut buf = Vec::new();
    buf.write_u16::<LE>(100).unwrap();
    buf.push(0xFF);

    assert_rt_decode_error(buf, "start_len", 0);
}

#[test]
#[expect(clippy::unwrap_used)]
fn decode_range_tombstones_truncated_end_len_returns_error() {
    use byteorder::{LE, WriteBytesExt};

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
    use byteorder::{LE, WriteBytesExt};

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
    use byteorder::{LE, WriteBytesExt};

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
    use std::sync::atomic::Ordering::Relaxed;

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
        // Recovery bypasses load_block() (reads via Block::from_file() directly),
        // so it intentionally does NOT increment block-load metrics — consistent
        // with how filter and index recovery reads are handled.
        Arc::new(Cache::with_capacity_bytes(10_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
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
        #[cfg(zstd_any)]
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
        #[cfg(zstd_any)]
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
        Arc::new(Cache::with_capacity_bytes(10_000_000)),
        Some(Arc::new(DescriptorTable::new(10))),
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
        #[cfg(zstd_any)]
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
        #[cfg(zstd_any)]
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
        let trailer = sfa::Reader::from_reader(&mut f)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;
        let meta_handle = regions.metadata;

        let raw_block =
            crate::file::read_exact(&f, *meta_handle.offset(), meta_handle.size() as usize)?;

        let header_len = Header::serialized_len();
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
        let trailer = sfa::Reader::from_reader(&mut f)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        let result = ParsedMeta::load_with_handle(&f, &regions.metadata, None);

        let err = result.expect_err("corrupted seqno#kv_max should cause an error");
        assert!(
            matches!(&err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::InvalidData),
            "expected InvalidData, got: {err:?}",
        );
    }

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
            let hash_a = BloomBuilder::get_hash(b"a");
            let hash_b = BloomBuilder::get_hash(b"b");

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
            let hash_exist = BloomBuilder::get_hash(b"key_0050");
            assert!(
                table.bloom_may_contain_key(b"key_0050", hash_exist)?,
                "bloom must not reject existing key in partitioned filter"
            );

            // Key beyond all partitions: with a pinned partition index, key-based
            // seek finds no ceiling and must return Ok(false).
            // Note: pinned_filter_index is always loaded when filter_tli exists
            // (unconditional in Table::recover), so this is always the partition-aware path.
            let hash_beyond = BloomBuilder::get_hash(b"zzz_beyond");
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
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
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
