use super::*;
use crate::{ValueType, value::InternalValue};
use test_log::test;

macro_rules! stream {
    ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
        let mut values = Vec::new();
        let mut counters = std::collections::HashMap::new();

        $(
            #[expect(clippy::string_lit_as_bytes)]
            let key = $key.as_bytes();

            #[expect(clippy::string_lit_as_bytes)]
            let sub_key = $sub_key.as_bytes();

            let value_type = match $value_type {
                "V" => ValueType::Value,
                "T" => ValueType::Tombstone,
                "W" => ValueType::WeakTombstone,
                "M" => ValueType::MergeOperand,
                "I" => ValueType::Indirection,
                _ => panic!("Unknown value type"),
            };

            let counter = counters.entry($key).and_modify(|x| { *x -= 1 }).or_insert(999);
            values.push(InternalValue::from_components(key, sub_key, *counter, value_type));
        )*

        values
    }};
}

macro_rules! iter_closed {
    ($iter:expr) => {
        assert!($iter.next().is_none(), "iterator should be closed (done)");
    };
}

#[derive(Default)]
struct TrackCallback {
    items: Vec<InternalValue>,
}

impl DroppedKvCallback for TrackCallback {
    fn on_dropped(&mut self, kv: &InternalValue) {
        self.items.push(kv.clone());
    }
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_expired_callback_1() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "", "T",
      "a", "", "T",
    ];

    let mut my_watcher = TrackCallback::default();

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000).with_drop_callback(&mut my_watcher);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    assert_eq!(
        [
            InternalValue::from_components("a", "", 998, ValueType::Tombstone),
            InternalValue::from_components("a", "", 997, ValueType::Tombstone),
        ],
        &*my_watcher.items,
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_seqno_zeroing_1() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "3", "V",
      "a", "2", "V",
      "a", "1", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000).zero_seqnos(true);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"3", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
fn compaction_stream_queue_weak_tombstones() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
      "b", "", "W",
      "b", "old", "V",
      "c", "", "W",
      "c", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_050);

    iter_closed!(iter);
}

/// GC should not evict tombstones, unless they are covered up
#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_tombstone_no_gc() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "b", "", "T",
      "c", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000_000);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_old_tombstone() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "", "T",
      "b", "", "T",
      "b", "", "T",
      "c", "", "T",
      "c", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 998);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 998, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 998, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 998, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_tombstone_overwrite_gc() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "val", "V",
      "a", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 999);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"val", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    // The tombstone at 998 is the newest version below the threshold, so the
    // fold keeps it: a read at snapshot 999 resolves to exactly this entry.
    // Here it happens to be observationally equal to dropping it, since every
    // older version goes too and an absent key reads the same as a deleted one.
    // The fold cannot tell those apart from one key's versions, and the rule
    // that can, keep the newest below the threshold, is the one that also holds
    // when that version is a value rather than a tombstone.
    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 998, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_bottom_level_tombstone_above_threshold_keeps_the_shadowed_value()
-> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "val", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // Threshold 0: nothing is collectable, so the bottom level may drop nothing.
    let mut iter = CompactionStream::new(iter, 0).evict_tombstones(true);

    // A read at snapshot 999 resolves to the value, not to the tombstone above
    // it. Dropping the pair because the level is the bottom would answer that
    // read with an absent key, which is indistinguishable from a genuine delete.
    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"val", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
fn compaction_stream_bottom_level_tombstone_below_threshold_takes_the_key_with_it() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "val", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // Threshold 1000 puts the tombstone itself below the watermark, so it is the
    // newest version any servable snapshot resolves to, and it reads as absent.
    let mut iter = CompactionStream::new(iter, 1_000).evict_tombstones(true);

    iter_closed!(iter);
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_weak_tombstone_simple() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 0);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_weak_tombstone_no_gc() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 998);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_weak_tombstone_above_threshold_keeps_the_consumed_value() -> crate::Result<()>
{
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // Threshold 999 records a floor of 998, so snapshot 999 is servable and
    // resolves to the value: the weak delete above it is not visible there yet.
    let mut iter = CompactionStream::new(iter, 999);

    // Annihilating the pair on the older sibling's seqno alone would answer that
    // read with an absent key. The pair leaves together only once the weak
    // tombstone ITSELF is below the watermark, which is the same condition the
    // plain-value fold states.
    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
fn compaction_stream_weak_tombstone_evict() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // 1000 puts the weak tombstone itself below the watermark; at 999 the value
    // is still the answer to a servable snapshot (see the test above).
    let mut iter = CompactionStream::new(iter, 1_000);

    // NOTE: Weak tombstone is consumed because value is GC'ed

    iter_closed!(iter);
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_weak_tombstone_evict_next_value() -> crate::Result<()> {
    #[rustfmt::skip]
    let mut vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];
    vec.push(InternalValue::from_components(
        "b",
        "other",
        999,
        ValueType::Value,
    ));

    let iter = vec.iter().cloned().map(Ok);
    // 1000 puts the weak tombstone itself below the watermark, which is what
    // lets the pair leave together.
    let mut iter = CompactionStream::new(iter, 1_000);

    // NOTE: Weak tombstone is consumed because value is GC'ed

    assert_eq!(
        InternalValue::from_components(*b"b", *b"other", 999, ValueType::Value),
        iter.next().unwrap()?,
    );

    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_no_evict_simple() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "old", "V",
      "b", "old", "V",
      "c", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 0);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"old", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"old", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"old", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_no_evict_simple_multi_keys() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "new", "V",
      "a", "old", "V",
      "b", "new", "V",
      "b", "old", "V",
      "c", "newnew", "V",
      "c", "new", "V",
      "c", "old", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 0);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"a", *b"old", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"new", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"old", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"newnew", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"new", 998, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"old", 997, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    Ok(())
}

#[test]
fn compaction_stream_filter_1() {
    struct Filter(&'static [u8]);
    impl StreamFilter for Filter {
        fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
            if value.key.user_key == b"b" {
                Ok(StreamFilterVerdict::Drop)
            } else if value.value < self.0 {
                Ok(StreamFilterVerdict::Replace((
                    ValueType::Tombstone,
                    UserValue::empty(),
                )))
            } else {
                Ok(StreamFilterVerdict::Keep)
            }
        }
    }

    #[rustfmt::skip]
    let vec = stream![
        "a", "9", "V",
        "a", "8", "V",
        "a", "7", "V",
        // subsequent values will be filtered out
        "a", "6", "V",
        "a", "5", "V",
        // subsequent values below gc threshold after filter
        "a", "4", "V",

        // this value will be dropped without leaving a tombstone
        "b", "b", "V",
    ];

    let mut drop_cb = TrackCallback { items: vec![] };
    let iter = vec.iter().cloned().map(Ok);
    let iter = CompactionStream::new(iter, 995)
        .with_filter(Filter(b"7"))
        .with_drop_callback(&mut drop_cb);

    let out: Vec<_> = iter.map(Result::unwrap).collect();

    // Six entries, not five: every version at or above the threshold (999 down
    // to 995) plus the newest one below it (994). This expectation used to stop
    // at 995, which encoded the fold's older rule of discarding by the seqno of
    // the next-older sibling. That rule dropped exactly the version a read just
    // above the recorded retention floor resolves to.
    #[rustfmt::skip]
    assert_eq!(out, stream![
        "a", "9", "V",
        "a", "8", "V",
        "a", "7", "V",
        "a", "", "T",
        "a", "", "T",
        "a", "", "T",
    ]);

    let fc = InternalValue::from_components;

    #[rustfmt::skip]
    assert_eq!(drop_cb.items, [
        fc(b"a", b"6", 996, ValueType::Value),
        fc(b"a", b"5", 995, ValueType::Value),
        fc(b"a", b"4", 994, ValueType::Value),
        fc(b"b", b"b", 999, ValueType::Value),
    ]);
}

pub mod custom_mvcc {
    use super::*;
    use crate::io::{BE, ReadBytesExt, WriteBytesExt};
    use test_log::test;

    /// MVCC trailer size (anything but user key)
    const TRAILER_SIZE: usize = 10;

    // Our keys become a multi map of: <key>#<seqno>
    //
    // (type does not really matter for ordering, because key+seqno are unique anyway)
    fn kv(key: &[u8], seqno: SeqNo, value: &[u8], tomb: bool) -> InternalValue {
        InternalValue::from_components(
            {
                use std::io::Write;

                let len = key.len() + TRAILER_SIZE;

                let mut key_builder = unsafe { UserKey::builder_unzeroed(len) };
                let mut cursor = std::io::Cursor::new(&mut key_builder[..]);

                cursor.write_all(key).unwrap();
                cursor.write_u8(0).unwrap(); // Keys are variable size so we need a \0 delimiter
                cursor
                    .write_u64::<BE>(
                        // IMPORTANT: Invert the seqno for correct descending sort
                        !seqno,
                    )
                    .unwrap();
                cursor.write_u8(u8::from(tomb)).unwrap();

                debug_assert_eq!(len, usize::try_from(cursor.position()).unwrap());

                key_builder.freeze()
            },
            value,
            2_353, // does not matter for us
            ValueType::Value,
        )
    }

    struct Filter {
        /// The previous user key
        ///
        /// Note that the user key is NOT the full KV key
        /// because we embed MVCC information into the key (`user_key#seqno#type`).
        prev_user_key: Option<UserKey>,

        /// MVCC watermark we can safely delete if an item < watermark
        /// is covered by a newer version.
        mvcc_watermark: SeqNo,
    }

    impl StreamFilter for Filter {
        fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
            let l = value.key.user_key.len();

            // User key len
            let ukl = l - TRAILER_SIZE;

            if let Some(prev) = &self.prev_user_key {
                let user_key = &value.key.user_key[..ukl];

                if prev == &user_key {
                    // We found another, older version of the previous key
                    let mut seqno = &value.key.user_key[(ukl + 1)..l - 1];
                    debug_assert_eq!(8, seqno.len());

                    // IMPORTANT: Invert the seqno back to normal value
                    let seqno = !seqno.read_u64::<BE>().unwrap();

                    if seqno < self.mvcc_watermark {
                        return Ok(StreamFilterVerdict::Drop);
                    }
                } else {
                    let user_key = &value.key.user_key.slice(..ukl);
                    self.prev_user_key = Some(user_key.clone());
                }
            } else {
                let user_key = &value.key.user_key.slice(..ukl);
                self.prev_user_key = Some(user_key.clone());
            }

            Ok(StreamFilterVerdict::Keep)
        }
    }

    #[test]
    fn compaction_filter_custom_mvcc() {
        let vec = vec![
            kv(b"abc", 4, b"c", false),
            kv(b"abc", 3, b"b", false),
            kv(b"abc", 2, b"a", false),
        ];

        let mut drop_cb = TrackCallback { items: vec![] };
        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 995)
            .with_filter(Filter {
                mvcc_watermark: 5,
                prev_user_key: None,
            })
            .with_drop_callback(&mut drop_cb);

        let out: Vec<_> = iter.map(Result::unwrap).collect();

        #[rustfmt::skip]
        assert_eq!(out, vec![
            kv(b"abc", 4, b"c", false),
        ]);
    }

    #[test]
    fn compaction_filter_custom_mvcc_multi_keys() {
        let vec = vec![
            kv(b"a", 4, b"c", false),
            kv(b"a", 3, b"b", false),
            kv(b"a", 2, b"a", false),
            //
            kv(b"b", 4, b"c", false),
            kv(b"b", 3, b"b", false),
            kv(b"b", 2, b"a", false),
            //
            kv(b"c", 1, b"c", false),
            //
            kv(b"d", 0, b"c", false),
        ];

        let mut drop_cb = TrackCallback { items: vec![] };
        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 995)
            .with_filter(Filter {
                mvcc_watermark: 3,
                prev_user_key: None,
            })
            .with_drop_callback(&mut drop_cb);

        let out: Vec<_> = iter.map(Result::unwrap).collect();

        #[rustfmt::skip]
        assert_eq!(out, vec![
            kv(b"a", 4, b"c", false),
            kv(b"a", 3, b"b", false),
            //
            kv(b"b", 4, b"c", false),
            kv(b"b", 3, b"b", false),
            //
            kv(b"c", 1, b"c", false),
            //
            kv(b"d", 0, b"c", false),
        ]);
    }
}

mod merge_operator_tests {
    use super::*;
    use std::sync::Arc;
    use test_log::test;

    /// Concatenation merge operator: joins all operands with ","
    struct ConcatMerge;

    impl crate::merge_operator::MergeOperator for ConcatMerge {
        fn merge(
            &self,
            _key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<UserValue> {
            let mut result = match base_value {
                Some(b) => String::from_utf8_lossy(b).to_string(),
                None => String::new(),
            };
            for op in operands {
                if !result.is_empty() {
                    result.push(',');
                }
                result.push_str(&String::from_utf8_lossy(op));
            }
            Ok(result.into_bytes().into())
        }
    }

    fn merge_op() -> Arc<dyn crate::merge_operator::MergeOperator> {
        Arc::new(ConcatMerge)
    }

    /// The same concatenation, declaring that its operands compose, so a
    /// compaction without a proven base folds them instead of keeping them.
    struct ComposingConcat;

    impl crate::merge_operator::MergeOperator for ComposingConcat {
        fn merge(
            &self,
            key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<UserValue> {
            ConcatMerge.merge(key, base_value, operands)
        }

        fn composes_operands(&self) -> bool {
            true
        }
    }

    fn composing_merge_op() -> Arc<dyn crate::merge_operator::MergeOperator> {
        Arc::new(ComposingConcat)
    }

    /// No tables outside the inputs, so every chain in front of the stream is
    /// the whole chain. Models a compaction that read everything overlapping
    /// the keys it touches.
    fn inputs_are_complete() -> crate::compaction::stream::InputCompleteness<'static> {
        &|_key, _oldest, _newest| true
    }

    #[test]
    fn composing_operator_with_every_operand_range_deleted_drops_the_key() -> crate::Result<()> {
        // A tombstone covering the oldest operand is a chain boundary, so the
        // composition is declined and the operands are re-emitted rather than
        // folded into one that would outlive the tombstone. Here it covers all
        // of them, so the emit path then drops each in turn and the key leaves.
        // This is also what keeps the fold from ever seeing an empty operand
        // list: emptying one means the oldest was covered, which declines the
        // composition before the coverage filter runs.
        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"op2".as_ref(),
                999,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"op1".as_ref(),
                998,
                ValueType::MergeOperand,
            ),
        ];

        let mut callback = TrackCallback::default();
        let cmp = crate::comparator::default_comparator();
        // Above both operands so it covers them, below the watermark so it is
        // applied at all.
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            1_500,
        );

        let iter = entries.into_iter().map(Ok);
        {
            let mut iter = CompactionStream::new(iter, 2_000)
                .with_merge_operator(Some(composing_merge_op()))
                .with_range_tombstone_application(vec![rt], cmp)
                // Complete inputs, so only the barrier can decline the fold.
                .with_input_completeness(inputs_are_complete())
                .with_drop_callback(&mut callback);

            iter_closed!(iter);
        }

        assert!(
            callback.items.iter().any(|kv| &*kv.value == b"op1"),
            "the older range-deleted operand must reach the drop callback",
        );
        assert!(
            callback.items.iter().any(|kv| &*kv.value == b"op2"),
            "the newer range-deleted operand must reach the drop callback",
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn composing_operator_across_a_range_tombstone_keeps_the_operands() -> crate::Result<()> {
        // "3"@3 above a range tombstone at 2, "1"@1 below it. A read resolves
        // that to "3": the tombstone hides the older operand. Composing the two
        // would write "1,3"@3, which outranks the tombstone and would read back
        // whole. The barrier is installed WITHOUT deletion rights, which is
        // what a compaction off the last level gets: it may not drop the
        // covered operand, only refuse to fold across it.
        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"3".as_ref(),
                3,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"1".as_ref(),
                1,
                ValueType::MergeOperand,
            ),
        ];

        let cmp = crate::comparator::default_comparator();
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            2,
        );

        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(composing_merge_op()))
            .with_range_tombstone_barriers(vec![rt], cmp)
            .with_input_completeness(inputs_are_complete());

        // Both operands come through untouched: nothing folded, nothing deleted.
        let first = iter.next().unwrap()?;
        assert_eq!(first.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*first.value, b"3");
        let second = iter.next().unwrap()?;
        assert_eq!(second.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*second.value, b"1");
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn composed_operands_each_pass_the_compaction_filter() -> crate::Result<()> {
        // Only the head of a chain goes through the filter in `next_inner`; the
        // rest are taken straight off the input. Before composing they were
        // re-emitted and so each got its verdict on the way out, but a composed
        // operand is written once and the entries inside it never come back. A
        // filter dropping an expired operand must therefore be honoured here,
        // or the expired delta is persisted inside the composition.
        struct DropOldest;
        impl StreamFilter for DropOldest {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                if &*value.value == b"expired" {
                    Ok(StreamFilterVerdict::Drop)
                } else {
                    Ok(StreamFilterVerdict::Keep)
                }
            }
        }

        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"keep".as_ref(),
                3,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"expired".as_ref(),
                2,
                ValueType::MergeOperand,
            ),
        ];

        let cmp = crate::comparator::default_comparator();
        let mut callback = TrackCallback::default();
        let iter = entries.into_iter().map(Ok);
        {
            let mut iter = CompactionStream::new(iter, 1_000)
                .with_filter(DropOldest)
                .with_merge_operator(Some(composing_merge_op()))
                .with_range_tombstone_barriers(Vec::new(), cmp)
                .with_input_completeness(inputs_are_complete())
                .with_drop_callback(&mut callback);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::MergeOperand);
            assert_eq!(
                &*item.value, b"keep",
                "the filtered operand must not be folded into the composition",
            );
            iter_closed!(iter);
        }
        assert!(
            callback.items.iter().any(|kv| &*kv.value == b"expired"),
            "the dropped operand must reach the drop callback, as it would on the emit path",
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn a_filter_replacing_a_collected_operand_composes_the_replacement() -> crate::Result<()> {
        // The verdict has to reach the fold, not just the head: composing the
        // original would persist the value the filter rewrote away.
        struct Rewrite;
        impl StreamFilter for Rewrite {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                if &*value.value == b"old" {
                    Ok(StreamFilterVerdict::Replace((
                        ValueType::MergeOperand,
                        UserValue::from(b"new".as_ref()),
                    )))
                } else {
                    Ok(StreamFilterVerdict::Keep)
                }
            }
        }

        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"head".as_ref(),
                3,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"old".as_ref(),
                2,
                ValueType::MergeOperand,
            ),
        ];

        let cmp = crate::comparator::default_comparator();
        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_filter(Rewrite)
            .with_merge_operator(Some(composing_merge_op()))
            .with_range_tombstone_barriers(Vec::new(), cmp)
            .with_input_completeness(inputs_are_complete());

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.value, b"new,head");
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn a_filter_error_on_a_collected_operand_fails_the_compaction() -> crate::Result<()> {
        // A filter error is the caller's to see: unlike a refused composition
        // there is no correct output to fall back to, because the verdict for
        // that operand is unknown.
        struct Failing;
        impl StreamFilter for Failing {
            fn filter_item(&mut self, value: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                if &*value.value == b"boom" {
                    Err(crate::Error::MergeOperator)
                } else {
                    Ok(StreamFilterVerdict::Keep)
                }
            }
        }

        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"head".as_ref(),
                3,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"boom".as_ref(),
                2,
                ValueType::MergeOperand,
            ),
        ];

        let cmp = crate::comparator::default_comparator();
        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_filter(Failing)
            .with_merge_operator(Some(composing_merge_op()))
            .with_range_tombstone_barriers(Vec::new(), cmp)
            .with_input_completeness(inputs_are_complete());

        assert!(
            iter.next().is_some_and(|item| item.is_err()),
            "the filter's error must surface rather than being folded away",
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_with_a_range_deleted_base_off_the_last_level_keeps_the_entries() -> crate::Result<()> {
        // The base is hidden by a range tombstone above it, so the operand must
        // not fold onto it. Off the last level the tombstone may not delete the
        // base either, so the only correct outcome is to leave both entries
        // alone and let the fold happen where the tombstone has been applied.
        // Folding here would resurrect the deleted base under the head's seqno.
        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"op".as_ref(),
                3,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(b"a".as_ref(), b"base".as_ref(), 1, ValueType::Value),
        ];

        let cmp = crate::comparator::default_comparator();
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            2,
        );

        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(merge_op()))
            .with_range_tombstone_barriers(vec![rt], cmp);

        let first = iter.next().unwrap()?;
        assert_eq!(first.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*first.value, b"op");
        let second = iter.next().unwrap()?;
        assert_eq!(
            second.key.value_type,
            ValueType::Value,
            "the range-deleted base must survive here: this compaction may not delete it",
        );
        assert_eq!(&*second.value, b"base");
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn composing_operator_above_a_range_tombstone_still_folds() -> crate::Result<()> {
        // The same barrier, but both operands sit above it, so no chain crosses
        // it and the fold is free to happen.
        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"3".as_ref(),
                5,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"1".as_ref(),
                4,
                ValueType::MergeOperand,
            ),
        ];

        let cmp = crate::comparator::default_comparator();
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            2,
        );

        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(composing_merge_op()))
            .with_range_tombstone_barriers(vec![rt], cmp)
            .with_input_completeness(inputs_are_complete());

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"1,3");
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn composing_operator_without_a_proven_base_emits_one_operand() -> crate::Result<()> {
        // No boundary in this stream and not the bottom level, so the base is
        // unproven. A composing operator folds anyway, and the result stays a
        // MergeOperand: calling it a Value would assert the base is empty.
        //
        // Range-tombstone application is installed with nothing in it, which is
        // what a compaction that can see the tombstones and finds none looks
        // like. Composing requires that: a tombstone between two operands ends
        // the chain, and a stream that cannot see one must not fold across it.
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
        ];

        let cmp = crate::comparator::default_comparator();
        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(composing_merge_op()))
            .with_range_tombstone_application(Vec::new(), cmp)
            .with_input_completeness(inputs_are_complete());

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1,op2");
        iter_closed!(iter);

        Ok(())
    }

    #[test]
    fn operands_without_a_proven_base_are_kept_as_they_are() -> crate::Result<()> {
        // No boundary in this stream and not the bottom level, so the base may
        // sit lower down. Folding here would hand the operator an empty base it
        // cannot tell from a proven-absent one, so the operands travel on
        // untouched, each with its own seqno, to the level that holds the base.
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
        let out: Vec<_> = iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(2, out.len(), "both operands survive");
        assert_eq!(
            vec![b"op2".to_vec(), b"op1".to_vec()],
            out.iter().map(|e| e.value.to_vec()).collect::<Vec<_>>(),
            "unfolded, in stream order",
        );
        assert!(
            out.iter()
                .all(|e| e.key.value_type == ValueType::MergeOperand),
            "still operands, not a value",
        );
        assert_eq!(
            vec.iter().map(|e| e.key.seqno).collect::<Vec<_>>(),
            out.iter().map(|e| e.key.seqno).collect::<Vec<_>>(),
            "each keeps its own seqno",
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn operands_fold_onto_a_proven_absent_base_at_the_bottom_level() -> crate::Result<()> {
        // Same stream at the bottom level: there is no level below to hold a
        // base, so absence is proven and the chain becomes the key's value.
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .evict_tombstones(true)
            .with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"op1,op2");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_with_base_below_gc() -> crate::Result<()> {
        // Merge operands + base value, all below gc threshold
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
            "a", "base", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"base,op1,op2");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_base_deleted_by_range_tombstone() -> crate::Result<()> {
        // op@999 and mid@998 operands above a base@997; a range tombstone over
        // the key at seqno 998 deletes the base (997 < 998), while the operands
        // survive (999, 998 are not below 998). The operands fold onto an empty
        // base, and the dropped base value reaches the callback.
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "a", "mid", "M",
            "a", "base", "V",
        ];

        let mut callback = TrackCallback::default();
        let cmp = crate::comparator::default_comparator();
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            998,
        );

        let iter = vec.iter().cloned().map(Ok);
        {
            let mut iter = CompactionStream::new(iter, 1_000)
                .with_merge_operator(Some(merge_op()))
                .with_range_tombstone_application(vec![rt], cmp)
                .with_drop_callback(&mut callback);

            let item = iter.next().unwrap()?;
            assert_eq!(item.key.value_type, ValueType::Value);
            assert_eq!(&*item.value, b"mid,op");
            assert!(iter.next().is_none());
        }
        assert!(
            callback.items.iter().any(|kv| &*kv.value == b"base"),
            "the range-deleted base value must reach the drop callback"
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_drops_operands_below_range_tombstone() -> crate::Result<()> {
        // M@100 is above the range tombstone; M@80 and the base V@70 are below
        // it (and deleted by it). Only M@100 survives, folding onto an empty
        // base, so the result is just that operand. Built with explicit seqnos
        // because the range tombstone must sit between the operands.
        let entries = vec![
            InternalValue::from_components(
                b"a".as_ref(),
                b"hi".as_ref(),
                100,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(
                b"a".as_ref(),
                b"lo".as_ref(),
                80,
                ValueType::MergeOperand,
            ),
            InternalValue::from_components(b"a".as_ref(), b"base".as_ref(), 70, ValueType::Value),
        ];

        let cmp = crate::comparator::default_comparator();
        let rt = RangeTombstone::new(
            UserKey::from(b"a".as_ref()),
            UserKey::from(b"b".as_ref()),
            90,
        );

        let iter = entries.into_iter().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(merge_op()))
            .with_range_tombstone_application(vec![rt], cmp);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(
            &*item.value, b"hi",
            "only the operand above the range tombstone survives"
        );
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_with_tombstone_below_gc() -> crate::Result<()> {
        // Merge operand above tombstone → merge with no base
        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
            "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"op1");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_above_gc_preserved() -> crate::Result<()> {
        // Entries above gc_watermark → NOT merged, preserved as-is
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 0) // gc_watermark=0, nothing expired
            .with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2");

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_lone_operand_below_gc() -> crate::Result<()> {
        // Single merge operand (only entry for key) below gc → partial merge
        let vec = vec![
            InternalValue::from_components("a", "lone_op", 5, ValueType::MergeOperand),
            InternalValue::from_components("b", "regular", 6, ValueType::Value),
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        // Partial merge (no base boundary) → stays MergeOperand
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"lone_op");
        assert_eq!(&*item.key.user_key, b"a");

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"b");
        assert_eq!(&*item.value, b"regular");

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_last_item_operand() -> crate::Result<()> {
        // Last item in entire stream is a merge operand below gc
        let vec = vec![InternalValue::from_components(
            "z",
            "last",
            5,
            ValueType::MergeOperand,
        )];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        // Partial merge (no base boundary) → stays MergeOperand
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"last");

        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn compaction_merge_mixed_keys() -> crate::Result<()> {
        // Multiple keys, some with merge operands, some without
        let vec = vec![
            InternalValue::from_components("a", "val_a", 10, ValueType::Value),
            InternalValue::from_components("b", "op2", 9, ValueType::MergeOperand),
            InternalValue::from_components("b", "op1", 8, ValueType::MergeOperand),
            InternalValue::from_components("b", "base_b", 7, ValueType::Value),
            InternalValue::from_components("c", "val_c", 6, ValueType::Value),
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let out: Vec<_> = iter.map(Result::unwrap).collect();

        assert_eq!(out.len(), 3);
        assert_eq!(&*out[0].key.user_key, b"a");
        assert_eq!(&*out[0].value, b"val_a");
        assert_eq!(&*out[1].key.user_key, b"b");
        assert_eq!(&*out[1].value, b"base_b,op1,op2");
        assert_eq!(&*out[2].key.user_key, b"c");
        assert_eq!(&*out[2].value, b"val_c");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_no_operator_passthrough() -> crate::Result<()> {
        // Without merge operator, MergeOperand entries pass through unchanged
        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_with_weak_tombstone() -> crate::Result<()> {
        // Merge operand above weak tombstone → merge with no base
        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
            "a", "", "W",
            "a", "old_val", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"op1");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_seqno_zeroing() -> crate::Result<()> {
        // Merged value should get seqno zeroed when below threshold
        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
            "a", "base", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(merge_op()))
            .zero_seqnos(true);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.seqno, 0);
        assert_eq!(&*item.value, b"base,op1");

        Ok(())
    }

    /// When merge operands sit above an Indirection base, compaction must
    /// preserve ALL entries unchanged — no operand may be dropped.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_indirection_base_preserves_all() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
            "a", "blob_ptr", "I",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));

        // All three entries must be emitted unchanged
        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2");

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1");

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");
        assert_eq!(item.key.value_type, ValueType::Indirection);
        assert_eq!(&*item.value, b"blob_ptr");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Exact GC boundary: head.seqno == gc_watermark should NOT merge.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_at_exact_gc_boundary() -> crate::Result<()> {
        // gc_watermark=999; head.seqno=999 (NOT below threshold)
        // Entries should be preserved as-is
        let vec = vec![
            InternalValue::from_components("a", "op2", 999, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 998, ValueType::MergeOperand),
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 999).with_merge_operator(Some(merge_op()));

        // head.seqno == gc_watermark → NOT below → preserved as MergeOperand
        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2");

        Ok(())
    }

    /// DroppedKvCallback receives dropped merge operands during compaction.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_dropped_callback() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
            "a", "base", "V",
        ];

        let mut callback = TrackCallback::default();

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000)
            .with_merge_operator(Some(merge_op()))
            .with_drop_callback(&mut callback);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"base,op1,op2");
        assert!(iter.next().is_none());

        // The base Value is consumed by merge (not dropped);
        // operands are consumed by merge (not dropped via callback).
        // DroppedKvCallback fires for entries DRAINED after base is found.
        // In this case there are no entries after the base, so callback
        // should have no items.
        assert!(callback.items.is_empty());

        Ok(())
    }

    /// Head above GC, peeked below GC: head must be preserved as MergeOperand.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_merge_head_above_gc_peeked_below() -> crate::Result<()> {
        // head.seqno=10 (above gc=7), peeked.seqno=5 (below gc=7)
        let vec = vec![
            InternalValue::from_components("a", "op_new", 10, ValueType::MergeOperand),
            InternalValue::from_components("a", "op_old", 5, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 2, ValueType::Value),
        ];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 7).with_merge_operator(Some(merge_op()));

        // Head is above GC → emit as-is (MergeOperand)
        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op_new");

        // Remaining entries preserved for future merge resolution
        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");

        Ok(())
    }

    /// Merge operator error propagates correctly during compaction.
    #[test]
    fn compaction_merge_error_propagation() {
        struct FailMerge;
        impl crate::merge_operator::MergeOperator for FailMerge {
            fn merge(
                &self,
                _key: &[u8],
                _base_value: Option<&[u8]>,
                _operands: &[&[u8]],
            ) -> crate::Result<crate::UserValue> {
                Err(crate::Error::MergeOperator)
            }
        }

        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
            "a", "base", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
            Some(Arc::new(FailMerge));
        let mut iter = CompactionStream::new(iter, 1_000).with_merge_operator(fail_op);

        assert!(matches!(
            iter.next(),
            Some(Err(crate::Error::MergeOperator))
        ));
    }

    /// A key whose base is in the stream folds; a key whose base may be below
    /// keeps its operands. One stream, both outcomes.
    #[test]
    fn a_key_folds_only_where_its_base_is_proven() -> crate::Result<()> {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op1", "M",
            "a", "base", "V",
            "b", "op2", "M",
            "b", "op1", "M",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
        let out: Vec<_> = iter.map(Result::unwrap).collect();

        assert_eq!(out.len(), 3);
        // "a": the base is right here, so the fold is the key's value.
        assert_eq!(out[0].key.value_type, ValueType::Value);
        assert_eq!(&*out[0].value, b"base,op1");
        // "b": no base in this stream and not the bottom level, so both
        // operands travel on.
        assert_eq!(out[1].key.value_type, ValueType::MergeOperand);
        assert_eq!(&*out[1].value, b"op2");
        assert_eq!(out[2].key.value_type, ValueType::MergeOperand);
        assert_eq!(&*out[2].value, b"op1");

        Ok(())
    }

    /// Stream filter that replaces values preserves MergeOperand type.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn compaction_filter_preserves_merge_operand_type() -> crate::Result<()> {
        struct UpperFilter;
        impl StreamFilter for UpperFilter {
            fn filter_item(&mut self, _item: &InternalValue) -> crate::Result<StreamFilterVerdict> {
                Ok(StreamFilterVerdict::Replace((
                    ValueType::Value,
                    b"REPLACED".to_vec().into(),
                )))
            }
        }

        let vec = vec![InternalValue::from_components(
            "a",
            "op1",
            5,
            ValueType::MergeOperand,
        )];

        let iter = vec.iter().cloned().map(Ok);
        let mut iter = CompactionStream::new(iter, 1_000).with_filter(UpperFilter);

        let item = iter.next().unwrap()?;
        // Filter tried to set Value, but MergeOperand type must be preserved
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"REPLACED");

        Ok(())
    }

    /// A merge resolution swallows the base inline, so the key's tail is
    /// already empty by the time the fold would drain it. The run still lost a
    /// version and has to say so, or a reopened tree admits the snapshot that
    /// resolved to the base and answers it with the merged entry's newer seqno.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_merge_fold_swallows_base_is_positive() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "a", "base", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        // Both versions below the watermark, so the fold runs and consumes the
        // base into the merged result.
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert!(
            balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
            "the base folded into the merge result is collected history",
        );
    }

    /// The same shape with a single operand and no base: nothing is consumed
    /// beyond the head, so nothing was collected and the floor must not move.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_lone_merge_operand_is_zero() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "b", "vb", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
    }

    /// The tombstone a merge fold resolves against is consumed by the fold, and
    /// at the bottom level that is the same neutrality the plain fold already
    /// excuses: the output no longer carries the tombstone, and the key reads
    /// absent for every snapshot that used to resolve to it.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_merge_fold_over_a_bottom_level_tombstone_is_zero() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000)
            .evict_tombstones(true)
            .with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
    }

    /// Off the bottom level the same fold is not neutral: a lower level may hold
    /// the version the tombstone was hiding, and dropping the tombstone without
    /// a floor resurrects it for the snapshots the floor would refuse.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_merge_fold_over_a_tombstone_off_the_bottom_level_is_positive() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000).with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert!(
            balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
            "off the bottom level a folded-away tombstone is collected history",
        );
    }

    /// A value under the tombstone makes the fold collection even at the bottom
    /// level: a snapshot below the tombstone resolved to that value and no
    /// longer can, since the merged result carries the head's seqno.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_merge_fold_over_a_tombstone_hiding_a_value_is_positive() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op", "M",
            "a", "", "T",
            "a", "val", "V",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000)
            .evict_tombstones(true)
            .with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert!(
            balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
            "a value under the folded tombstone is collected history",
        );
    }

    /// An operand between the head and the tombstone is folded into one result
    /// carrying the head's seqno, so a snapshot that resolved to that operand
    /// alone loses it. The tombstone excuse covers the tombstone tail only.
    #[test]
    #[expect(clippy::expect_used, reason = "test assertion")]
    fn gc_balance_merge_fold_swallowing_an_operand_over_a_tombstone_is_positive() {
        #[rustfmt::skip]
        let vec = stream![
            "a", "op2", "M",
            "a", "op1", "M",
            "a", "", "T",
        ];

        let iter = vec.iter().cloned().map(Ok);
        let iter = CompactionStream::new(iter, 1_000)
            .evict_tombstones(true)
            .with_merge_operator(Some(merge_op()));
        let balance = iter.gc_balance();
        for item in iter {
            item.expect("stream must not error");
        }

        assert!(
            balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
            "an operand consumed into the merged result is collected history",
        );
    }
}

/// Regression: the "is the peeked entry a different key?" check used a bytewise
/// `>` on the user keys instead of key identity. Under a comparator whose order
/// differs from byte order (reverse-lexicographic here), a following DIFFERENT
/// key sorts bytewise-lower, so the old check classified it as "same key": a
/// `WeakTombstone` head then saw the other key's `Value` as its annihilation
/// partner and was dropped, resurrecting older versions below.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn compaction_stream_custom_comparator_weak_tombstone_not_annihilated_across_keys()
-> crate::Result<()> {
    // Comparator-ascending order for reverse-lex: "b" sorts before "a".
    let vec = vec![
        InternalValue::from_components("b", "", 999, ValueType::WeakTombstone),
        InternalValue::from_components("a", "va", 998, ValueType::Value),
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000);

    // The weak tombstone has NO same-key value after it — it must survive.
    let first = iter.next().unwrap()?;
    assert_eq!(first.key.value_type, ValueType::WeakTombstone);
    assert_eq!(&*first.key.user_key, b"b");

    let second = iter.next().unwrap()?;
    assert_eq!(second.key.value_type, ValueType::Value);
    assert_eq!(&*second.key.user_key, b"a");

    iter_closed!(iter);
    Ok(())
}

/// Runs the stream to exhaustion and reports how many watermark-driven drops
/// it counted.
#[expect(clippy::expect_used, reason = "test assertion")]
fn collected_count(vec: &[InternalValue], gc_watermark: u64) -> u64 {
    let iter = vec.iter().cloned().map(Ok);
    let iter = CompactionStream::new(iter, gc_watermark);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }
    balance.load(core::sync::atomic::Ordering::Relaxed)
}

#[test]
fn gc_balance_watermark_collects_nothing_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "new", "V",
      "a", "old", "V",
    ];

    // Threshold 998 leaves both versions at or above the watermark, so the
    // fold keeps them and nothing is collected.
    assert_eq!(collected_count(&vec, 998), 0);
}

#[test]
fn gc_balance_one_version_per_key_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "va", "V",
      "b", "vb", "V",
    ];

    // Nothing to fold at any watermark: each key has a single version, so a
    // run over this input must not claim it collected history.
    assert_eq!(collected_count(&vec, 1_000), 0);
    assert_eq!(collected_count(&vec, 0), 0);
}

#[test]
fn gc_balance_watermark_drops_a_version_is_positive() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "new", "V",
      "a", "old", "V",
    ];

    // Threshold 1000 puts both below the watermark, so the older one is
    // collected and the run has to say so.
    assert_eq!(collected_count(&vec, 1_000), 1);
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_lone_bottom_level_tombstone_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "b", "vb", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let iter = CompactionStream::new(iter, 0).evict_tombstones(true);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }

    // The tombstone shadows nothing, so dropping it answers every snapshot the
    // way keeping it would. That is not collected history and must not raise a
    // floor, least of all at a watermark of 0. The balance counts every
    // consumed version, so this arm has to excuse itself explicitly.
    assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_tombstone_only_chain_at_the_bottom_level_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // Both tombstones below the watermark at the bottom level: the eviction arm
    // takes the head and drains the rest of the chain.
    let iter = CompactionStream::new(iter, 1_000).evict_tombstones(true);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }

    // The key read as absent at every snapshot before the drop and reads absent
    // after it, exactly as for a lone tombstone. Dropping a whole chain of them
    // is the same neutrality and must not cost a floor.
    assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_tombstone_tail_under_an_emitted_head_at_the_bottom_level_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "val", "V",
      "a", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let iter = CompactionStream::new(iter, 1_000).evict_tombstones(true);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }

    // Every snapshot below the value read "absent" through the tombstone and
    // reads "absent" from nothing after the fold. Nothing observable changed,
    // so no floor is owed.
    assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_tombstone_tail_off_the_bottom_level_is_positive() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "val", "V",
      "a", "", "T",
    ];

    let iter = vec.iter().cloned().map(Ok);
    // NOT the bottom level: a lower level may still hold a version the drained
    // tombstone was hiding, so dropping it without a floor would resurrect that
    // version for the snapshots the floor refuses.
    let iter = CompactionStream::new(iter, 1_000);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }

    assert!(
        balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
        "off the bottom level a dropped tombstone is collected history",
    );
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_tombstone_chain_over_a_value_is_positive() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "", "T",
      "a", "val", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let iter = CompactionStream::new(iter, 1_000).evict_tombstones(true);
    let balance = iter.gc_balance();
    for item in iter {
        item.expect("stream must not error");
    }

    // A value anywhere under the chain makes the drop collection: a snapshot
    // below the oldest tombstone resolved to it and no longer can.
    assert!(
        balance.load(core::sync::atomic::Ordering::Relaxed) > 0,
        "a chain that also swallowed a value is collected history",
    );
}

#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn gc_balance_abandoned_after_one_item_is_zero() {
    #[rustfmt::skip]
    let vec = stream![
      "a", "va", "V",
      "b", "vb", "V",
    ];

    let iter = vec.iter().cloned().map(Ok);
    let mut iter = CompactionStream::new(iter, 1_000);
    let balance = iter.gc_balance();

    // Take one and walk away, which is what a compaction stopped by its signal
    // does before installing what it wrote. The peek that looked ahead at "b"
    // must not leave that version on the balance: this run collected nothing.
    iter.next()
        .expect("first item")
        .expect("stream must not error");
    drop(iter);

    assert_eq!(balance.load(core::sync::atomic::Ordering::Relaxed), 0);
}
