#[expect(clippy::expect_used)]
mod tests {
    use crate::comparator::default_comparator;
    use crate::{
        Checksum, InternalValue, SeqNo, Slice,
        ValueType::{Tombstone, Value},
        table::{
            Block, DataBlock,
            block::{BlockType, Header, ParsedItem},
        },
    };
    use test_log::test;

    #[test]
    fn data_block_wtf() -> crate::Result<()> {
        let keys = [
            [0, 0, 0, 0, 0, 0, 0, 108],
            [0, 0, 0, 0, 0, 0, 0, 109],
            [0, 0, 0, 0, 0, 0, 0, 110],
            [0, 0, 0, 0, 0, 0, 0, 111],
            [0, 0, 0, 0, 0, 0, 0, 112],
            [0, 0, 0, 0, 0, 0, 0, 113],
            [0, 0, 0, 0, 0, 0, 0, 114],
            [0, 0, 0, 0, 0, 0, 0, 115],
            [0, 0, 0, 0, 0, 0, 0, 116],
            [0, 0, 0, 0, 0, 0, 0, 117],
            [0, 0, 0, 0, 0, 0, 0, 118],
            [0, 0, 0, 0, 0, 0, 0, 119],
            [0, 0, 0, 0, 0, 0, 0, 120],
            [0, 0, 0, 0, 0, 0, 0, 121],
            [0, 0, 0, 0, 0, 0, 0, 122],
            [0, 0, 0, 0, 0, 0, 0, 123],
            [0, 0, 0, 0, 0, 0, 0, 124],
            [0, 0, 0, 0, 0, 0, 0, 125],
            [0, 0, 0, 0, 0, 0, 0, 126],
            [0, 0, 0, 0, 0, 0, 0, 127],
            [0, 0, 0, 0, 0, 0, 0, 128],
            [0, 0, 0, 0, 0, 0, 0, 129],
            [0, 0, 0, 0, 0, 0, 0, 130],
            [0, 0, 0, 0, 0, 0, 0, 131],
            [0, 0, 0, 0, 0, 0, 0, 132],
            [0, 0, 0, 0, 0, 0, 0, 133],
            [0, 0, 0, 0, 0, 0, 0, 134],
            [0, 0, 0, 0, 0, 0, 0, 135],
            [0, 0, 0, 0, 0, 0, 0, 136],
            [0, 0, 0, 0, 0, 0, 0, 137],
            [0, 0, 0, 0, 0, 0, 0, 138],
            [0, 0, 0, 0, 0, 0, 0, 139],
            [0, 0, 0, 0, 0, 0, 0, 140],
            [0, 0, 0, 0, 0, 0, 0, 141],
            [0, 0, 0, 0, 0, 0, 0, 142],
            [0, 0, 0, 0, 0, 0, 0, 143],
        ];

        let items = keys
            .into_iter()
            .map(|key| InternalValue::from_components(key, "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::table::data_block::Iter<'_> =
                    data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
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

                assert_eq!(3, count);
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_range() -> crate::Result<()> {
        let items = (100u64..110)
            .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::table::data_block::Iter<'_> =
                    data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
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

                assert_eq!(10, count);
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_range_ping_pong() -> crate::Result<()> {
        let items = (0u64..100)
            .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());
            iter.seek(&5u64.to_be_bytes(), SeqNo::MAX);
            iter.seek_upper(&9u64.to_be_bytes(), SeqNo::MAX);

            let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
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
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let iter = data_block
                .iter(default_comparator())
                .map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_rev() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let iter = data_block
                .iter(default_comparator())
                .rev()
                .map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_rev_seek_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.rev().collect();

            assert_eq!(
                items.iter().rev().skip(2).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_range_edges() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(!iter.seek(b"a", SeqNo::MAX), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(!iter.seek_upper(b"g", SeqNo::MAX), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"b", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().rev().take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"c", SeqNo::MAX), "should seek");
            assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(1).take(2).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_only_first() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek_upper(b"b", SeqNo::MAX), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().take(1).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_range_same_key() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.rev().collect();

                assert_eq!(
                    items
                        .iter()
                        .rev()
                        .skip(2)
                        .take(1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.rev().collect();

                assert_eq!(
                    items
                        .iter()
                        .rev()
                        .skip(2)
                        .take(1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &*real_items,
                );
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_range_empty() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");
                iter.seek_upper(b"e", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");
                iter.seek_upper(b"e", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"e", SeqNo::MAX), "should seek");
                iter.seek(b"f", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"e", SeqNo::MAX), "should seek");
                iter.seek(b"f", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_restart_head() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"b", SeqNo::MAX), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_in_interval() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"d", SeqNo::MAX), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(2).cloned().collect::<Vec<_>>(),
                real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_last() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"f", SeqNo::MAX), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(4).cloned().collect::<Vec<_>>(),
                real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_before_first() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(!iter.seek(b"a", SeqNo::MAX), "should not find exact match");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_after_last() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(!iter.seek(b"g", SeqNo::MAX), "should not find exact match");

            assert!(iter.next().is_none(), "should not collect any items");
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_consume_last_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_consume_last_forwards() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_ping_pong_exhaust() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("a", "a", 0, Value),
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 0, Value),
            InternalValue::from_components("e", "e", 0, Value),
        ];

        for restart_interval in 1..=u8::MAX {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"a", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"e", &*iter.next().expect("should exist").key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"e", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"a", &*iter.next_back().expect("should exist").key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"a", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"e", &*iter.next().expect("should exist").key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"e", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"a", &*iter.next_back().expect("should exist").key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_fuzz_3() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::from([
                    255, 255, 255, 255, 5, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                    255, 255, 255, 255, 255,
                ]),
                Slice::from([0, 0, 192]),
                18_446_744_073_701_163_007,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::from([255, 255, 255, 255, 255, 255, 0]),
                Slice::from([]),
                0,
                Value,
            ),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 5, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        Ok(())
    }

    #[test]
    fn data_block_iter_fuzz_4() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::new(&[0]),
                Slice::empty(),
                3_834_029_160_418_063_669,
                Value,
            ),
            InternalValue::from_components(Slice::new(&[0]), Slice::new(&[]), 127, Tombstone),
            InternalValue::from_components(
                Slice::new(&[53, 53, 53]),
                Slice::empty(),
                18_446_744_073_709_551_615,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::new(&[255]),
                Slice::empty(),
                18_446_744_069_414_584_831,
                Tombstone,
            ),
            InternalValue::from_components(Slice::new(&[255, 255]), Slice::empty(), 47, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        Ok(())
    }

    #[test]
    fn data_block_seek_closed_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::new(&[0, 161]), Slice::empty(), 1, Tombstone),
            InternalValue::from_components(Slice::new(&[0, 161]), Slice::empty(), 0, Tombstone),
            InternalValue::from_components(Slice::new(&[1]), Slice::empty(), 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 100, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        let mut iter = data_block.iter(default_comparator());
        iter.seek(&[0], SeqNo::MAX);
        iter.seek_upper(&[0], SeqNo::MAX);

        assert_eq!(0, iter.count());

        Ok(())
    }

    /// Verifies that `seek(needle, seqno)` with a seqno-aware predicate still
    /// positions the iterator correctly when a key has many versions spanning
    /// multiple restart intervals.
    #[test]
    fn data_block_seek_seqno_aware() -> crate::Result<()> {
        // Build a block where key "b" has 10 versions (seqno 10..1) with
        // restart_interval=2, so versions span 5 restart intervals.
        let mut items = Vec::new();
        for seqno in (1..=10).rev() {
            items.push(InternalValue::from_components(b"b", b"", seqno, Value));
        }

        for restart_interval in [1, 2, 3, 5] {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // With SeqNo::MAX, seek behaves like key-only (no seqno filtering).
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(
                    iter.seek(b"b", SeqNo::MAX),
                    "should find key with MAX seqno"
                );
                let entry = iter.next().expect("should have entry");
                let materialized = entry.materialize(&data_block.inner.data);
                assert_eq!(materialized.key.user_key.as_ref(), b"b");
                // First version returned is the newest (seqno 10).
                assert_eq!(materialized.key.seqno, 10);
            }

            // With a specific snapshot seqno, the binary search lands on the
            // restart interval containing (or nearest to) the target seqno.
            // The first entry returned is the head of that interval.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek(b"b", 5), "should find key with snapshot seqno 5");
                let entry = iter.next().expect("should have entry");
                let materialized = entry.materialize(&data_block.inner.data);
                assert_eq!(materialized.key.user_key.as_ref(), b"b");
                // The landing entry's seqno must be >= the snapshot boundary,
                // proving the seqno-aware predicate skipped past older intervals.
                assert!(
                    materialized.key.seqno >= 5,
                    "restart_interval={restart_interval}: landing seqno {} should be >= snapshot 5",
                    materialized.key.seqno,
                );
                // With restart_interval=1 each entry is its own interval, so
                // the predicate lands exactly on the target seqno — a key-only
                // seek would land on seqno 10 instead.
                if restart_interval == 1 {
                    assert_eq!(
                        materialized.key.seqno, 5,
                        "with restart_interval=1, seqno-aware seek must land exactly on target"
                    );
                }
            }
        }

        Ok(())
    }

    /// Verifies that `seek` with seqno still works correctly when the block
    /// contains multiple distinct keys with versions.
    #[test]
    fn data_block_seek_seqno_aware_mixed_keys() -> crate::Result<()> {
        let items = vec![
            InternalValue::from_components(b"a", b"", 10, Value),
            InternalValue::from_components(b"a", b"", 5, Value),
            InternalValue::from_components(b"b", b"", 10, Value),
            InternalValue::from_components(b"b", b"", 7, Value),
            InternalValue::from_components(b"b", b"", 3, Value),
            InternalValue::from_components(b"c", b"", 10, Value),
        ];

        for restart_interval in [1, 2, 3] {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // Forward seek with seqno narrows restart interval selection.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek(b"b", 5), "should find b at snapshot 5");
                let entry = iter.next().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"b");
                // Landing seqno must be >= snapshot boundary.
                assert!(
                    mat.key.seqno >= 5,
                    "restart_interval={restart_interval}: seqno {} should be >= 5",
                    mat.key.seqno,
                );
                // With restart_interval=1, seqno-aware seek lands on (b,7) —
                // the last head with seqno >= 5 — whereas key-only would land
                // on (b,10).
                if restart_interval == 1 {
                    assert_eq!(mat.key.seqno, 7);
                }
            }

            // Exclusive forward seek with seqno.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(
                    iter.seek_exclusive(b"b", 5),
                    "should find entry > b at snapshot 5"
                );
                let entry = iter.next().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"c");
            }

            // Upper seek still works with seqno (predicate unchanged for backward).
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek_upper(b"b", 5), "should find upper bound b");
                let entry = iter.next_back().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"b");
            }
        }

        Ok(())
    }

    // Regression tests for binary-search-predicate devirtualization on the
    // lexicographic fast path.
    //
    // The implementation branches once on `cmp.is_lexicographic()` per seek
    // entry point and picks a closure that does direct slice comparison on
    // the lex path (no vtable). These tests use a counting comparator
    // wrapper to ASSERT that:
    //   1. when `is_lexicographic() == true`, the comparator's `compare()`
    //      is never invoked during the binary-search probe loop (lex closure
    //      bypasses it)
    //   2. when `is_lexicographic() == false`, `compare()` IS invoked
    //      (preserves correctness for custom orderings)
    //
    // Behavioural-equivalence between lex and dyn paths is already exercised
    // by the broader `custom_comparator*` integration tests; these tests
    // specifically guard against accidental fallback into the dyn path on
    // the default comparator (which would silently regress performance
    // without any observable test failure).
    mod devirt {
        use crate::comparator::UserComparator;
        use crate::{
            Checksum, InternalValue, SeqNo,
            ValueType::Value,
            table::{
                Block, DataBlock,
                block::{BlockType, Header, ParsedItem},
            },
        };
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct CountingComparator {
            /// Counts `compare()` invocations — proves the lex devirt path
            /// successfully bypasses the `dyn UserComparator::compare` vtable.
            count: Arc<AtomicUsize>,
            /// Counts `is_lexicographic()` invocations. The lex devirt
            /// strategy gates each entry point on `is_lexicographic()`, so
            /// this counter PROVES the lex branch was actually selected (a
            /// regression that always selected the dyn branch would leave
            /// this at 0 in lex tests AND keep `count` at 0, both passing
            /// the original weaker assertion). Required to fully discriminate
            /// "lex branch ran" from "no seeks happened at all".
            is_lex_count: Arc<AtomicUsize>,
            lex: bool,
        }

        impl UserComparator for CountingComparator {
            fn name(&self) -> &'static str {
                "counting"
            }
            fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                self.count.fetch_add(1, AtomicOrdering::Relaxed);
                a.cmp(b)
            }
            fn is_lexicographic(&self) -> bool {
                self.is_lex_count.fetch_add(1, AtomicOrdering::Relaxed);
                self.lex
            }
        }

        /// Block tuned to make BINARY-SEARCH PROBES dominate any potential
        /// linear-scan contribution to the call count:
        ///   - 128 keys, `restart_interval=1` → 128 restart heads
        ///   - binary search: log2(128) = 7 probes minimum
        ///   - linear scan after BS lands: 0-1 iterations (each restart head
        ///     IS an item, so the scan either returns immediately or steps once)
        ///
        /// Discrimination math:
        ///   - dyn path working correctly: count >= 7 (BS) + 0..1 (scan)
        ///   - lex closure leaked into dyn BS: count = 0 (BS) + 0..1 (scan) ≤ 1
        ///
        /// `assert count >= 2` cleanly distinguishes the two cases, ruling out
        /// the linear-scan-only false-positive that a naive `> 0` would miss.
        fn build_block_bs_dominated() -> crate::Result<DataBlock> {
            let items: Vec<_> = (0_u64..128)
                .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
                .collect();
            let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;
            Ok(DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            }))
        }

        /// Minimum number of `compare()` calls a working dyn binary-search
        /// is expected to make on the BS-dominated block: `⌈log2(128)⌉ = 7`
        /// probes against restart heads.
        ///
        /// Discrimination math (paired with an ABOVE-MAX needle in the
        /// dyn tests for `seek_upper` / `seek_upper_exclusive`):
        ///   - working dyn path:  7 BS probes + 1 linear-scan `compare_key` = 8
        ///   - lex closure leak:  0 BS probes + 1 linear-scan `compare_key` = 1
        ///
        /// `assert delta >= 7` cleanly catches the lex-leak even after
        /// accounting for the bounded linear-scan contribution. A weaker
        /// threshold (e.g. `>= 2`) would fail to discriminate for needles
        /// that produce 2+ linear-scan calls — for example an exact-key
        /// needle hits in `seek_upper_exclusive` reverse scan
        /// (Equal-skip then Less-return), which can satisfy `>= 2` even
        /// when the BS predicate accidentally took the lex path.
        const DYN_MIN_BS_PROBES: usize = 7;

        /// Builds a needle that sorts strictly above the maximum encoded
        /// key (key 127 in [`build_block_bs_dominated`]). Choosing this
        /// needle bounds the reverse linear-scan contribution to exactly
        /// 1 `compare_key` call (`peek_back` lands on key 127 → Less →
        /// returns immediately), keeping the dyn / lex-leak discrimination
        /// math above clean.
        fn above_max_needle() -> Vec<u8> {
            let mut v = 127_u64.to_be_bytes().to_vec();
            v.push(0xFF);
            v
        }

        #[test]
        fn data_block_seek_lex_path_skips_vtable() -> crate::Result<()> {
            // is_lexicographic() == true must route ALL 3 devirtualized entry
            // points through the static-dispatch closures. Snapshotting count
            // per-entry-point (instead of a single end-of-test check) localises
            // a regression to the offending entry point and catches the case
            // where only one closure accidentally falls back to the dyn path.
            let data_block = build_block_bs_dominated()?;
            let count = Arc::new(AtomicUsize::new(0));
            let is_lex_count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: is_lex_count.clone(),
                lex: true,
            });
            let needle = 64_u64.to_be_bytes();

            let before = count.load(AtomicOrdering::Relaxed);
            let before_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = data_block.iter(cmp.clone());
                let _ = iter.seek(&needle, SeqNo::MAX);
            }
            let after_seek = count.load(AtomicOrdering::Relaxed);
            let after_seek_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            assert_eq!(
                after_seek - before,
                0,
                "seek (forward seqno-aware) lex path leaked into dyn: {} compare() calls",
                after_seek - before,
            );
            assert!(
                after_seek_lex - before_lex >= 1,
                "seek lex path must consult is_lexicographic() to select the lex closure, got {} calls — branch may have been hardcoded?",
                after_seek_lex - before_lex,
            );

            {
                let mut iter = data_block.iter(cmp.clone());
                let _ = iter.seek_upper(&needle, SeqNo::MAX);
            }
            let after_upper = count.load(AtomicOrdering::Relaxed);
            let after_upper_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            assert_eq!(
                after_upper - after_seek,
                0,
                "seek_upper lex path leaked into dyn: {} compare() calls",
                after_upper - after_seek,
            );
            assert!(
                after_upper_lex - after_seek_lex >= 1,
                "seek_upper lex path must consult is_lexicographic(), got {} calls",
                after_upper_lex - after_seek_lex,
            );

            {
                let mut iter = data_block.iter(cmp);
                let _ = iter.seek_upper_exclusive(&needle, SeqNo::MAX);
            }
            let after_excl = count.load(AtomicOrdering::Relaxed);
            let after_excl_lex = is_lex_count.load(AtomicOrdering::Relaxed);
            assert_eq!(
                after_excl - after_upper,
                0,
                "seek_upper_exclusive lex path leaked into dyn: {} compare() calls",
                after_excl - after_upper,
            );
            assert!(
                after_excl_lex - after_upper_lex >= 1,
                "seek_upper_exclusive lex path must consult is_lexicographic(), got {} calls",
                after_excl_lex - after_upper_lex,
            );
            Ok(())
        }

        #[test]
        fn data_block_seek_to_key_seqno_dyn_path_invokes_compare() -> crate::Result<()> {
            // `seek_to_key_seqno` is the FORWARD seqno-aware binary search
            // exposed WITHOUT an attached linear scan — perfect isolation
            // for the BS predicate. Working dyn path → exactly 7 BS probes.
            // Lex closure leak → 0 calls.
            let data_block = build_block_bs_dominated()?;
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = 64_u64.to_be_bytes();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = data_block.iter(cmp);
                let _ = iter.seek_to_key_seqno(&needle, SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "seek_to_key_seqno dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times \
                 (log2(128 restart heads) probes, no linear scan), got {delta} — lex closure leaked into dyn BS?",
            );
            Ok(())
        }

        #[test]
        fn data_block_seek_upper_dyn_path_invokes_compare() -> crate::Result<()> {
            // `seek_upper` does binary search + reverse linear scan. In dyn
            // path the linear scan also calls `compare_key` (which goes
            // through `cmp.compare()` via its own dyn branch). To prevent
            // the linear scan from inflating the count to a value that a
            // lex-BS-leak could also reach via scan-only contribution, we
            // use an ABOVE-MAX needle (see `above_max_needle`): BS lands on
            // key 127, peek_back returns key 127, compare_key → Less → loop
            // exits → exactly 1 linear-scan call.
            //
            //   working dyn:  7 BS probes + 1 linear = 8  ✓ delta >= 7
            //   lex-leak:     0 BS probes + 1 linear = 1  ✗ delta >= 7
            let data_block = build_block_bs_dominated()?;
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = data_block.iter(cmp);
                let _ = iter.seek_upper(&needle, SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "seek_upper dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times \
                 (log2(128 restart heads) probes), got {delta} — lex closure leaked into dyn BS?",
            );
            Ok(())
        }

        #[test]
        fn data_block_seek_upper_exclusive_dyn_path_invokes_compare() -> crate::Result<()> {
            // Same above-max-needle strategy as `seek_upper`. For
            // `seek_upper_exclusive` (last key < needle) with needle
            // above-max: BS lands on key 127, peek_back returns key 127,
            // compare_key → Less → return true. Exactly 1 linear-scan call.
            //
            // The earlier version used `needle = 64` (exact-key match),
            // which caused the reverse scan to consume Equal (key 64) THEN
            // continue to key 63 (Less, return) — 2 linear calls. With the
            // old `>= 2` threshold, a leaked-lex BS would still pass
            // (0 + 2 = 2). The above-max needle bounds linear contribution
            // to 1, restoring the discrimination math.
            let data_block = build_block_bs_dominated()?;
            let count = Arc::new(AtomicUsize::new(0));
            let cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: count.clone(),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });
            let needle = above_max_needle();

            let before = count.load(AtomicOrdering::Relaxed);
            {
                let mut iter = data_block.iter(cmp);
                let _ = iter.seek_upper_exclusive(&needle, SeqNo::MAX);
            }
            let delta = count.load(AtomicOrdering::Relaxed) - before;
            assert!(
                delta >= DYN_MIN_BS_PROBES,
                "seek_upper_exclusive dyn BS must call compare() at least {DYN_MIN_BS_PROBES} times \
                 (log2(128 restart heads) probes), got {delta} — lex closure leaked into dyn BS?",
            );
            Ok(())
        }

        // Smaller block reused by the equivalence test where boundary needles
        // matter more than BS-vs-scan call-count discrimination.
        fn build_block_for_equivalence() -> crate::Result<DataBlock> {
            let items: Vec<_> = (0_u64..64)
                .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
                .collect();
            let bytes = DataBlock::encode_into_vec(&items, 8, 1.33)?;
            Ok(DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            }))
        }

        #[test]
        fn data_block_seek_lex_and_dyn_agree_on_landing_position() -> crate::Result<()> {
            // Equivalence check: lex and dyn paths must produce IDENTICAL
            // landing positions for every probe. If the lex closure ever
            // disagrees with `compare() != Greater` semantics (e.g. wrong
            // operator), this test catches it before integration suites do.
            //
            // Encoded keys are 64 entries of `u64::to_be_bytes()` (8-byte
            // fixed-width). Needles are raw byte slices that cover the full
            // `partition_point` boundary table:
            //   - empty slice                              → BELOW the minimum
            //     (sorts before any non-empty byte sequence) → left == 0
            //   - 8 zero bytes (== key 0)                  → exact-min hit
            //   - between-key needle (9 bytes, prefix of key 17 + 0x00) →
            //     sorts strictly between [0…0,17] and [0…0,18] → predicate
            //     transition mid-range
            //   - 8-byte key 32                            → exact mid-hit
            //   - 8-byte key 63                            → exact-tail hit
            //     (last key, left == len exercise)
            //   - 9-byte above-key-63 needle               → above max, no match
            //
            // The earlier version of this test used only `u64::to_be_bytes()`
            // values; all were exact-keys plus one above-max, missing the
            // genuine below-min and between-key partition_point boundary
            // cases that this test now claims to cover.
            let data_block = build_block_for_equivalence()?;
            let lex: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: Arc::new(AtomicUsize::new(0)),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: true,
            });
            let dyn_cmp: Arc<dyn UserComparator> = Arc::new(CountingComparator {
                count: Arc::new(AtomicUsize::new(0)),
                is_lex_count: Arc::new(AtomicUsize::new(0)),
                lex: false,
            });

            // (label, needle_bytes)
            let between_17_and_18: Vec<u8> = {
                let mut v = 17_u64.to_be_bytes().to_vec();
                v.push(0); // 9 bytes: > 17, < 18 lexicographically
                v
            };
            let above_max: Vec<u8> = {
                let mut v = 63_u64.to_be_bytes().to_vec();
                v.push(0xFF); // 9 bytes after the largest 8-byte key
                v
            };
            let needles: Vec<(&str, Vec<u8>)> = vec![
                ("below-min (empty slice)", vec![]),
                ("exact-min (key 0)", 0_u64.to_be_bytes().to_vec()),
                ("between keys 17 and 18", between_17_and_18),
                ("exact-mid (key 32)", 32_u64.to_be_bytes().to_vec()),
                ("exact-tail (key 63)", 63_u64.to_be_bytes().to_vec()),
                ("above-max (key 63 + 0xFF)", above_max),
            ];

            for (label, needle) in &needles {
                let mut lex_iter = data_block.iter(lex.clone());
                let lex_seek = lex_iter.seek(needle, SeqNo::MAX);
                let lex_landing = lex_iter
                    .next()
                    .map(|e| e.materialize(data_block.as_slice()).key.user_key);

                let mut dyn_iter = data_block.iter(dyn_cmp.clone());
                let dyn_seek = dyn_iter.seek(needle, SeqNo::MAX);
                let dyn_landing = dyn_iter
                    .next()
                    .map(|e| e.materialize(data_block.as_slice()).key.user_key);

                assert_eq!(
                    lex_seek, dyn_seek,
                    "seek result must match for needle {label} ({needle:?})",
                );
                assert_eq!(
                    lex_landing, dyn_landing,
                    "landing position must match for needle {label} ({needle:?})",
                );
            }
            Ok(())
        }
    }
}
