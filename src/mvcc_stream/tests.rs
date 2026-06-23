use super::*;
use crate::{ValueType, value::InternalValue};
use test_log::test;

macro_rules! stream {
  ($($key:expr, $sub_key:expr, $value_type:expr),* $(,)?) => {{
      let mut values = Vec::new();
      let mut counters = std::collections::HashMap::new();

      $(
          let key = $key.as_bytes();
          let sub_key = $sub_key.as_bytes();
          let value_type = match $value_type {
              "V" => ValueType::Value,
              "T" => ValueType::Tombstone,
              "W" => ValueType::WeakTombstone,
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
        assert!(
            $iter.next_back().is_none(),
            "iterator should be closed (done)"
        );
    };
}

/// Tests that the iterator emit the same stuff forwards and backwards, just in reverse
macro_rules! test_reverse {
    ($v:expr) => {
        let iter = Box::new($v.iter().cloned().map(Ok));
        let iter = MvccStream::new(iter, None);
        let mut forwards = iter.flatten().collect::<Vec<_>>();
        forwards.reverse();

        let iter = Box::new($v.iter().cloned().map(Ok));
        let iter = MvccStream::new(iter, None);
        let backwards = iter.rev().flatten().collect::<Vec<_>>();

        assert_eq!(forwards, backwards);
    };
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_error() -> crate::Result<()> {
    {
        let vec = [
            Ok(InternalValue::from_components(
                "a",
                "new",
                999,
                ValueType::Value,
            )),
            Err(crate::Error::Io(crate::io::Error::other("test error"))),
        ];

        let iter = Box::new(vec.into_iter());
        let mut iter = MvccStream::new(iter, None);

        // Because next calls drain_key_min, the error is immediately first, even though
        // the first item is technically Ok
        assert!(matches!(iter.next().unwrap(), Err(crate::Error::Io(_))));
        iter_closed!(iter);
    }

    {
        let vec = [
            Ok(InternalValue::from_components(
                "a",
                "new",
                999,
                ValueType::Value,
            )),
            Err(crate::Error::Io(crate::io::Error::other("test error"))),
        ];

        let iter = Box::new(vec.into_iter());
        let mut iter = MvccStream::new(iter, None);

        assert!(matches!(
            iter.next_back().unwrap(),
            Err(crate::Error::Io(_))
        ));
        assert_eq!(
            InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
            iter.next_back().unwrap()?,
        );
        iter_closed!(iter);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue_reverse_almost_gone() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "", 1, ValueType::Tombstone),
        InternalValue::from_components("b", "b", 0, ValueType::Value),
        InternalValue::from_components("c", "", 1, ValueType::Tombstone),
        InternalValue::from_components("c", "c", 0, ValueType::Value),
        InternalValue::from_components("d", "", 1, ValueType::Tombstone),
        InternalValue::from_components("d", "d", 0, ValueType::Value),
        InternalValue::from_components("e", "", 1, ValueType::Tombstone),
        InternalValue::from_components("e", "e", 0, ValueType::Value),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue_almost_gone_2() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "", 1, ValueType::Tombstone),
        InternalValue::from_components("c", "", 1, ValueType::Tombstone),
        InternalValue::from_components("d", "", 1, ValueType::Tombstone),
        InternalValue::from_components("e", "", 1, ValueType::Tombstone),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "b", 0, ValueType::Value),
        InternalValue::from_components("c", "c", 0, ValueType::Value),
        InternalValue::from_components("d", "d", 0, ValueType::Value),
        InternalValue::from_components("e", "", 1, ValueType::Tombstone),
        InternalValue::from_components("e", "e", 0, ValueType::Value),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"b", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"c", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"d", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue_weak_almost_gone() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("b", "b", 0, ValueType::Value),
        InternalValue::from_components("c", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("c", "c", 0, ValueType::Value),
        InternalValue::from_components("d", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("d", "d", 0, ValueType::Value),
        InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("e", "e", 0, ValueType::Value),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue_weak_almost_gone_2() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("c", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("d", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_queue_weak_reverse() -> crate::Result<()> {
    let vec = [
        InternalValue::from_components("a", "a", 0, ValueType::Value),
        InternalValue::from_components("b", "b", 0, ValueType::Value),
        InternalValue::from_components("c", "c", 0, ValueType::Value),
        InternalValue::from_components("d", "d", 0, ValueType::Value),
        InternalValue::from_components("e", "", 1, ValueType::WeakTombstone),
        InternalValue::from_components("e", "e", 0, ValueType::Value),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"a", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"b", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"c", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"d", *b"d", 0, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"e", *b"", 1, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_simple() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "new", "V",
      "a", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_simple_multi_keys() -> crate::Result<()> {
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

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"new", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"new", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"newnew", 999, ValueType::Value),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_tombstone() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_tombstone_multi_keys() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "old", "V",
      "b", "", "T",
      "b", "old", "V",
      "c", "", "T",
      "c", "", "T",
      "c", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

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

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_weak_tombstone_simple() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_weak_tombstone_resurrection() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "new", "V",
      "a", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_weak_tombstone_priority() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "T",
      "a", "", "W",
      "a", "new", "V",
      "a", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::Tombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn mvcc_stream_weak_tombstone_multi_keys() -> crate::Result<()> {
    #[rustfmt::skip]
    let vec = stream![
      "a", "", "W",
      "a", "old", "V",
      "b", "", "W",
      "b", "old", "V",
      "c", "", "W",
      "c", "old", "V",
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));

    let mut iter = MvccStream::new(iter, None);

    assert_eq!(
        InternalValue::from_components(*b"a", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"b", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    assert_eq!(
        InternalValue::from_components(*b"c", *b"", 999, ValueType::WeakTombstone),
        iter.next().unwrap()?,
    );
    iter_closed!(iter);

    test_reverse!(vec);

    Ok(())
}

#[allow(clippy::doc_markdown, clippy::unnecessary_wraps)]
mod merge_operator_tests {
    use super::*;
    use std::sync::Arc;
    use test_log::test;

    /// Concatenation merge operator for testing
    struct ConcatMerge;

    impl crate::merge_operator::MergeOperator for ConcatMerge {
        fn merge(
            &self,
            _key: &[u8],
            base_value: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
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

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_forward_operands_only() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"op1,op2");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_forward_with_base() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.value, b"base,op1,op2");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_forward_with_tombstone() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "", 2, ValueType::Tombstone),
            InternalValue::from_components("a", "old", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        // Merge above tombstone: no base
        let item = iter.next().unwrap()?;
        assert_eq!(&*item.value, b"op1");
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_forward_mixed_keys() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "val_a", 5, ValueType::Value),
            InternalValue::from_components("b", "op2", 4, ValueType::MergeOperand),
            InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("c", "val_c", 2, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let iter = MvccStream::new(iter, Some(merge_op()));
        let out: Vec<_> = iter.map(Result::unwrap).collect();

        assert_eq!(out.len(), 3);
        assert_eq!(&*out[0].value, b"val_a");
        assert_eq!(&*out[1].value, b"op1,op2");
        assert_eq!(&*out[2].value, b"val_c");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_reverse_operands_with_base() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.value, b"base,op1,op2");
        assert!(iter.next_back().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_reverse_operands_only() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.value, b"op1,op2");
        assert!(iter.next_back().is_none());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_reverse_mixed_keys() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "val_a", 5, ValueType::Value),
            InternalValue::from_components("b", "op2", 4, ValueType::MergeOperand),
            InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("c", "val_c", 2, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let iter = MvccStream::new(iter, Some(merge_op()));
        let out: Vec<_> = iter.rev().map(Result::unwrap).collect();

        // Reverse: c, b(merged), a
        assert_eq!(out.len(), 3);
        assert_eq!(&*out[0].value, b"val_c");
        assert_eq!(&*out[1].value, b"op1,op2");
        assert_eq!(&*out[2].value, b"val_a");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_reverse_single_operand_last() -> crate::Result<()> {
        // Single merge operand as last item in reverse iteration
        let vec = vec![InternalValue::from_components(
            "a",
            "op1",
            1,
            ValueType::MergeOperand,
        )];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.value, b"op1");
        assert_eq!(item.key.value_type, ValueType::Value);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_no_operator_passthrough() -> crate::Result<()> {
        // Without merge operator, MergeOperand entries returned as-is (latest version wins)
        let vec = vec![
            InternalValue::from_components("a", "op2", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 1, ValueType::MergeOperand),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, None);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2"); // latest only
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn mvcc_merge_reverse_single_operand_with_different_key() -> crate::Result<()> {
        // Single merge operand key followed by regular key in reverse
        let vec = vec![
            InternalValue::from_components("a", "val_a", 5, ValueType::Value),
            InternalValue::from_components("b", "op1", 3, ValueType::MergeOperand),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        // Reverse: b(merged), a
        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.key.user_key, b"b");
        assert_eq!(&*item.value, b"op1");
        assert_eq!(item.key.value_type, ValueType::Value);

        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");

        assert!(iter.next_back().is_none());

        Ok(())
    }

    /// Forward: MergeOperand above an Indirection base must return the
    /// MergeOperand unchanged — indirection bytes are internal blob
    /// pointers, not user data.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_indirection_base_returns_head() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");
        // Must return head MergeOperand unchanged, NOT merged with blob pointer
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Reverse: MergeOperand above an Indirection base must return the
    /// newest MergeOperand unchanged.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_reverse_indirection_base_returns_newest() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next_back().unwrap()?;
        assert_eq!(&*item.key.user_key, b"a");
        // Must return newest MergeOperand unchanged
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op2");

        assert!(iter.next_back().is_none());
        Ok(())
    }

    /// Merge operator error must propagate through forward iteration.
    #[test]
    fn merge_forward_error_propagation() {
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

        let vec = vec![
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
            Some(Arc::new(FailMerge));
        let mut iter = MvccStream::new(iter, fail_op);

        assert!(matches!(
            iter.next(),
            Some(Err(crate::Error::MergeOperator))
        ));
    }

    /// Merge operator error must propagate through reverse iteration.
    #[test]
    fn merge_reverse_error_propagation() {
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

        let vec = vec![
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let fail_op: Option<Arc<dyn crate::merge_operator::MergeOperator>> =
            Some(Arc::new(FailMerge));
        let mut iter = MvccStream::new(iter, fail_op);

        assert!(matches!(
            iter.next_back(),
            Some(Err(crate::Error::MergeOperator))
        ));
    }

    /// WeakTombstone stops base search same as regular Tombstone.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_weak_tombstone_stops_base() -> crate::Result<()> {
        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "", 2, ValueType::WeakTombstone),
            InternalValue::from_components("a", "old_base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        let item = iter.next().unwrap()?;
        // WeakTombstone blocks base — merge with no base
        assert_eq!(item.key.value_type, ValueType::Value);
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Forward: RT-suppressed base value is excluded from merge.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_rt_suppresses_base() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;

        // RT covers key "a" at seqno 2 → base@1 is suppressed
        let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 2);

        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op())).with_range_tombstones(vec![(rt, 4)]);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        // base@1 is RT-suppressed → merge with no base
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Forward: RT-suppressed operand stops collection (treated as boundary).
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_rt_suppresses_operand() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;

        // RT at seqno 3 → operand@2 and base@1 are suppressed
        let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 3);

        let vec = vec![
            InternalValue::from_components("a", "op2", 4, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op())).with_range_tombstones(vec![(rt, 5)]);

        let item = iter.next().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        // Only op2 survives; op1 and base are RT-suppressed
        assert_eq!(&*item.value, b"op2");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Reverse: RT-suppressed entries are excluded from merge.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_reverse_rt_suppresses_base() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;

        let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 2);

        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op())).with_range_tombstones(vec![(rt, 4)]);

        let item = iter.next_back().unwrap()?;
        assert_eq!(item.key.value_type, ValueType::Value);
        // base@1 suppressed → merge with no base
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next_back().is_none());
        Ok(())
    }

    /// Forward: if the newest MergeOperand is RT-suppressed, skip merge
    /// entirely — pass through for the post-filter to suppress.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_rt_suppresses_head() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;

        // RT at seqno 5 covers "a" → head@3 is suppressed
        let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 5);

        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op())).with_range_tombstones(vec![(rt, 6)]);

        let item = iter.next().unwrap()?;
        // Head is RT-suppressed → merge skipped, head returned as-is
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next().is_none());
        Ok(())
    }

    /// Reverse: if the newest MergeOperand is RT-suppressed, skip merge.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_reverse_rt_suppresses_head() -> crate::Result<()> {
        use crate::range_tombstone::RangeTombstone;

        let rt = RangeTombstone::new(b"a".to_vec().into(), b"b".to_vec().into(), 5);

        let vec = vec![
            InternalValue::from_components("a", "op1", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "base", 1, ValueType::Value),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op())).with_range_tombstones(vec![(rt, 6)]);

        let item = iter.next_back().unwrap()?;
        // Head is RT-suppressed → merge skipped
        assert_eq!(item.key.value_type, ValueType::MergeOperand);
        assert_eq!(&*item.value, b"op1");

        assert!(iter.next_back().is_none());
        Ok(())
    }
}
