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

/// A caller that spells the stream's type out names it by its input alone:
/// the reader of separated bases has a default.
#[test]
fn mvcc_stream_is_named_by_its_input_alone() {
    type Boxed = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>>>;
    let iter: Boxed = Box::new(core::iter::empty());
    let mut stream: MvccStream<Boxed> = MvccStream::new(iter, None);
    assert!(stream.next().is_none());
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

fn io_error() -> crate::Error {
    crate::Error::Io(crate::io::Error::other("test error"))
}

/// The items a forward pass yields: `Ok` keys as text, errors as `"err"`.
fn forward_items<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>>(
    stream: MvccStream<I>,
) -> Vec<String> {
    stream
        .map(|item| match item {
            Ok(kv) => String::from_utf8_lossy(&kv.value).into_owned(),
            Err(_) => "err".to_owned(),
        })
        .collect()
}

/// An error met while draining a key does not stop the drain: the older
/// versions of that key never surface as its value, and every error a source
/// raised is still yielded once.
#[test]
fn mvcc_stream_error_while_draining_skips_the_rest_of_the_key() {
    let vec = [
        Ok(InternalValue::from_components(
            "a",
            "new",
            999,
            ValueType::Value,
        )),
        Err(io_error()),
        Err(io_error()),
        Ok(InternalValue::from_components(
            "a",
            "old",
            998,
            ValueType::Value,
        )),
        Ok(InternalValue::from_components(
            "b",
            "b",
            1,
            ValueType::Value,
        )),
    ];
    let stream = MvccStream::new(Box::new(vec.into_iter()), None);
    assert_eq!(forward_items(stream), ["err", "err", "b"]);
}

/// The same while collecting a merge chain: an error among the operands drains
/// the key, so its base never surfaces as the key's value.
#[test]
fn mvcc_stream_error_while_merging_skips_the_rest_of_the_key() {
    struct Concat;
    impl crate::merge_operator::MergeOperator for Concat {
        fn merge(
            &self,
            _key: &[u8],
            base: Option<&[u8]>,
            operands: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
            let mut out = base.unwrap_or_default().to_vec();
            for op in operands {
                out.extend_from_slice(op);
            }
            Ok(out.into())
        }
    }

    let vec = [
        Ok(InternalValue::from_components(
            "a",
            "op",
            999,
            ValueType::MergeOperand,
        )),
        Err(io_error()),
        Err(io_error()),
        Ok(InternalValue::from_components(
            "a",
            "old",
            998,
            ValueType::Value,
        )),
        Ok(InternalValue::from_components(
            "b",
            "b",
            1,
            ValueType::Value,
        )),
    ];
    let stream = MvccStream::new(Box::new(vec.into_iter()), Some(Arc::new(Concat)));
    assert_eq!(forward_items(stream), ["err", "err", "b"]);

    // Backward, an error after the base has been buffered drains the newer
    // versions of the key too: they never merge without their base.
    let vec = [
        Ok(InternalValue::from_components(
            "a",
            "op",
            999,
            ValueType::MergeOperand,
        )),
        Err(io_error()),
        Ok(InternalValue::from_components(
            "a",
            "old",
            998,
            ValueType::Value,
        )),
        Ok(InternalValue::from_components(
            "b",
            "b",
            1,
            ValueType::Value,
        )),
    ];
    let backward: Vec<String> = MvccStream::new(Box::new(vec.into_iter()), Some(Arc::new(Concat)))
        .rev()
        .map(|item| match item {
            Ok(kv) => String::from_utf8_lossy(&kv.value).into_owned(),
            Err(_) => "err".to_owned(),
        })
        .collect();
    assert_eq!(backward, ["b", "err"]);
}

/// A source that plays back a script, counts how often it is pulled and
/// switches to a second script when repositioned.
struct Scripted {
    items: std::collections::VecDeque<crate::Result<InternalValue>>,
    after_reseek: Vec<crate::Result<InternalValue>>,
    pulls: std::rc::Rc<std::cell::Cell<usize>>,
}

impl Scripted {
    fn new(items: Vec<crate::Result<InternalValue>>) -> Self {
        Self {
            items: items.into(),
            after_reseek: Vec::new(),
            pulls: std::rc::Rc::default(),
        }
    }
}

impl Iterator for Scripted {
    type Item = crate::Result<InternalValue>;
    fn next(&mut self) -> Option<Self::Item> {
        self.pulls.set(self.pulls.get() + 1);
        self.items.pop_front()
    }
}

impl DoubleEndedIterator for Scripted {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.pulls.set(self.pulls.get() + 1);
        self.items.pop_back()
    }
}

impl crate::reseek::Reseekable for Scripted {
    fn reseek(&mut self, _ctx: &crate::reseek::ReseekCtx) {
        self.items = core::mem::take(&mut self.after_reseek).into();
    }
}

fn named_error(name: &str) -> crate::Result<InternalValue> {
    Err(crate::Error::Io(crate::io::Error::other(name.to_owned())))
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "a script step is an item, Ok or Err, as the stream reads it"
)]
fn kv(key: &str, value: &str, seqno: SeqNo, value_type: ValueType) -> crate::Result<InternalValue> {
    Ok(InternalValue::from_components(
        key.as_bytes(),
        value.as_bytes(),
        seqno,
        value_type,
    ))
}

/// What an item shows: the value, or the error's message.
fn shown(item: crate::Result<InternalValue>) -> String {
    match item {
        Ok(kv) => String::from_utf8_lossy(&kv.value).into_owned(),
        Err(crate::Error::Io(e)) => {
            let text = e.to_string();
            text.strip_prefix("other error: ")
                .unwrap_or(&text)
                .to_owned()
        }
        Err(e) => format!("{e:?}"),
    }
}

/// An error ends the key's resolution at once: the call returns without
/// pulling the rest of a long run of errors, which later calls yield one by
/// one.
#[test]
fn mvcc_stream_error_is_returned_without_draining_the_run_behind_it() {
    let mut items = vec![kv("a", "new", 999, ValueType::Value)];
    for _ in 0..1_000 {
        items.push(named_error("e"));
    }
    let source = Scripted::new(items);
    let pulls = std::rc::Rc::clone(&source.pulls);
    let mut stream = MvccStream::new(source, None);

    assert_eq!(stream.next().map(shown).as_deref(), Some("e"));
    assert!(pulls.get() < 10, "pulled {} times", pulls.get());
    assert_eq!(stream.next().map(shown).as_deref(), Some("e"));
}

/// Errors met while skipping a failed key come out in the order they were
/// met.
#[test]
fn mvcc_stream_errors_of_a_failed_key_keep_their_order() {
    struct Concat;
    impl crate::merge_operator::MergeOperator for Concat {
        fn merge(
            &self,
            _: &[u8],
            base: Option<&[u8]>,
            ops: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
            let mut out = base.unwrap_or_default().to_vec();
            for op in ops {
                out.extend_from_slice(op);
            }
            Ok(out.into())
        }
    }

    let source = Scripted::new(vec![
        kv("a", "op", 999, ValueType::MergeOperand),
        named_error("e1"),
        named_error("e2"),
        named_error("e3"),
        kv("a", "old", 998, ValueType::Value),
        kv("b", "b", 1, ValueType::Value),
    ]);
    let stream = MvccStream::new(source, Some(Arc::new(Concat)));
    let items: Vec<String> = stream.map(shown).collect();
    assert_eq!(items, ["e1", "e2", "e3", "b"]);
}

/// A key skipped after an error at the front does not hold up the back: the
/// back yields its own items, not the front's errors.
#[test]
fn mvcc_stream_front_error_stays_at_the_front() {
    let source = Scripted::new(vec![
        kv("a", "new", 999, ValueType::Value),
        named_error("e1"),
        named_error("e2"),
        kv("a", "old", 998, ValueType::Value),
        kv("b", "b", 1, ValueType::Value),
        kv("c", "c", 1, ValueType::Value),
    ]);
    let mut stream = MvccStream::new(source, None);

    assert_eq!(stream.next().map(shown).as_deref(), Some("e1"));
    assert_eq!(stream.next_back().map(shown).as_deref(), Some("c"));
    assert_eq!(stream.next().map(shown).as_deref(), Some("e2"));
    assert_eq!(stream.next().map(shown).as_deref(), Some("b"));
    assert!(stream.next().is_none());
}

/// A key that failed at one end yields nothing when the other end reaches
/// it: the versions left are the ones the failure shadows.
#[test]
fn mvcc_stream_failed_key_is_skipped_from_the_other_end() {
    let source = Scripted::new(vec![
        kv("a", "new", 999, ValueType::Value),
        named_error("e"),
        kv("a", "old", 998, ValueType::Value),
        kv("b", "b", 1, ValueType::Value),
    ]);
    let mut stream = MvccStream::new(source, None);
    assert_eq!(stream.next().map(shown).as_deref(), Some("e"));
    let rest: Vec<String> = stream.rev().map(shown).collect();
    assert_eq!(rest, ["b"], "front failure, back drain");

    let source = Scripted::new(vec![
        kv("x", "x", 1, ValueType::Value),
        kv("a", "new", 999, ValueType::Value),
        named_error("e"),
        kv("a", "old", 998, ValueType::Value),
    ]);
    let mut stream = MvccStream::new(source, None);
    assert_eq!(stream.next_back().map(shown).as_deref(), Some("e"));
    let rest: Vec<String> = stream.map(shown).collect();
    assert_eq!(rest, ["x"], "back failure, front drain");
}

/// What is left of a key that failed at one end is skipped by the other end
/// before it is resolved: resolving it could only fail again (here the
/// operator always fails) and report a second error for the same key.
#[test]
fn mvcc_stream_failed_key_is_skipped_before_the_other_end_resolves_it() {
    struct FailMerge;
    impl crate::merge_operator::MergeOperator for FailMerge {
        fn merge(
            &self,
            _: &[u8],
            _: Option<&[u8]>,
            _: &[&[u8]],
        ) -> crate::Result<crate::UserValue> {
            Err(crate::Error::MergeOperator)
        }
    }

    let source = Scripted::new(vec![
        kv("a", "op3", 999, ValueType::MergeOperand),
        named_error("e"),
        kv("a", "op2", 998, ValueType::MergeOperand),
        kv("a", "base", 997, ValueType::Value),
        kv("b", "b", 1, ValueType::Value),
    ]);
    let mut stream = MvccStream::new(source, Some(Arc::new(FailMerge)));
    assert_eq!(stream.next().map(shown).as_deref(), Some("e"));
    let rest: Vec<String> = stream.rev().map(shown).collect();
    assert_eq!(rest, ["b"], "front failure, back drain");

    let source = Scripted::new(vec![
        kv("x", "x", 1, ValueType::Value),
        kv("a", "op", 999, ValueType::MergeOperand),
        named_error("e"),
        kv("a", "base", 997, ValueType::Value),
    ]);
    let mut stream = MvccStream::new(source, Some(Arc::new(FailMerge)));
    assert_eq!(stream.next_back().map(shown).as_deref(), Some("e"));
    let rest: Vec<String> = stream.map(shown).collect();
    assert_eq!(rest, ["x"], "back failure, front drain");
}

/// A reposition starts fresh: what was left to skip at the old position
/// does not surface at the new one.
#[test]
fn mvcc_stream_reseek_forgets_the_failed_key() {
    use crate::reseek::Reseekable;

    let mut source = Scripted::new(vec![
        kv("a", "new", 999, ValueType::Value),
        named_error("e1"),
        named_error("e2"),
        kv("a", "old", 998, ValueType::Value),
    ]);
    source.after_reseek = vec![kv("x", "x", 1, ValueType::Value)];
    let mut stream = MvccStream::new(source, None);

    assert_eq!(stream.next().map(shown).as_deref(), Some("e1"));
    stream.reseek(&crate::reseek::ReseekCtx {
        user: (core::ops::Bound::Unbounded, core::ops::Bound::Unbounded),
        internal: (core::ops::Bound::Unbounded, core::ops::Bound::Unbounded),
    });
    assert_eq!(stream.next().map(shown).as_deref(), Some("x"));
    assert!(stream.next().is_none());
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

/// `DoubleEndedIterator` requires that the two ends meet in the middle exactly
/// once: every item comes out of one end or the other, and none comes out of
/// both. Taking one item off the front and then draining from the back is the
/// smallest walk that mixes the directions, and it used to lose the entry that
/// the forward step had left parked in the front peek slot.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertion")]
fn one_forward_step_then_a_backward_drain_yields_every_key() -> crate::Result<()> {
    let vec = stream![
        "a", "", "V", //
        "b", "", "V", //
        "c", "", "V", //
        "d", "", "V", //
        "e", "", "V",
    ];

    let mut iter = MvccStream::new(Box::new(vec.into_iter().map(Ok)), None);

    let mut seen = vec![iter.next().unwrap()?.key.user_key];
    while let Some(item) = iter.next_back() {
        seen.push(item?.key.user_key);
    }
    seen.sort_unstable();

    let keys: Vec<&[u8]> = seen.iter().map(|k| &**k).collect();
    assert_eq!(
        keys,
        [b"a".as_ref(), b"b", b"c", b"d", b"e"],
        "an entry was dropped between the two ends",
    );

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

    /// Forward: a stream with no value log refuses to merge onto a base kept
    /// there, instead of answering with an operand or handing the pointer
    /// bytes to the operator as the value.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_forward_onto_indirection_without_value_log_is_refused() {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        assert!(matches!(
            iter.next().unwrap(),
            Err(crate::Error::FeatureUnsupported(_))
        ));
    }

    /// Reverse: the same refusal.
    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertion")]
    fn merge_reverse_onto_indirection_without_value_log_is_refused() {
        let vec = vec![
            InternalValue::from_components("a", "op2", 3, ValueType::MergeOperand),
            InternalValue::from_components("a", "op1", 2, ValueType::MergeOperand),
            InternalValue::from_components("a", "blob_ptr", 1, ValueType::Indirection),
        ];

        let iter = Box::new(vec.into_iter().map(Ok));
        let mut iter = MvccStream::new(iter, Some(merge_op()));

        assert!(matches!(
            iter.next_back().unwrap(),
            Err(crate::Error::FeatureUnsupported(_))
        ));
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

/// Regression: reverse iteration must detect the key boundary by key IDENTITY,
/// not by bytewise `<` on the user keys. Under a comparator whose order differs
/// from byte order (reverse-lexicographic here), the previous entry of a
/// DIFFERENT key sorts bytewise-higher, so the old bytewise check classified it
/// as "same key" and silently skipped it — a reverse scan dropped rows.
#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn mvcc_stream_reverse_scan_custom_comparator_emits_every_key() -> crate::Result<()> {
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "reverse-test"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
            b.cmp(a)
        }
    }

    // Comparator-ascending order for reverse-lex: "b" sorts before "a".
    let vec = [
        InternalValue::from_components("b", "vb", 1, ValueType::Value),
        InternalValue::from_components("a", "va", 1, ValueType::Value),
    ];

    let iter = Box::new(vec.iter().cloned().map(Ok));
    let iter =
        MvccStream::new_with_comparator(iter, None, alloc::sync::Arc::new(ReverseComparator));
    let backwards = iter.rev().collect::<crate::Result<Vec<_>>>()?;

    assert_eq!(
        2,
        backwards.len(),
        "reverse scan must emit both keys, got {backwards:?}"
    );
    assert_eq!(b"a", &*backwards.first().expect("len checked").key.user_key);
    assert_eq!(b"b", &*backwards.get(1).expect("len checked").key.user_key);
    Ok(())
}
