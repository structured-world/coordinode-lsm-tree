use super::*;
use crate::ValueType::Value;
use crate::comparator;
use test_log::test;

#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_simple() -> crate::Result<()> {
    #[rustfmt::skip]
    let a = vec![
        Ok(InternalValue::from_components("a", b"", 0, Value)),
    ];
    #[rustfmt::skip]
    let b = vec![
        Ok(InternalValue::from_components("b", b"", 0, Value)),
    ];

    let mut iter = Merger::new(
        vec![a.into_iter(), b.into_iter()],
        comparator::default_comparator(),
    );

    assert_eq!(
        iter.next().unwrap()?,
        InternalValue::from_components("a", b"", 0, Value),
    );
    assert_eq!(
        iter.next().unwrap()?,
        InternalValue::from_components("b", b"", 0, Value),
    );
    assert!(iter.next().is_none(), "iter should be closed");

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_interleaved() -> crate::Result<()> {
    let a = vec![
        Ok(InternalValue::from_components("a", b"", 0, Value)),
        Ok(InternalValue::from_components("c", b"", 0, Value)),
        Ok(InternalValue::from_components("e", b"", 0, Value)),
    ];
    let b = vec![
        Ok(InternalValue::from_components("b", b"", 0, Value)),
        Ok(InternalValue::from_components("d", b"", 0, Value)),
    ];

    let iter = Merger::new(
        vec![a.into_iter(), b.into_iter()],
        comparator::default_comparator(),
    );

    let keys: Vec<String> = iter
        .map(|r| {
            let v = r.unwrap();
            String::from_utf8_lossy(&v.key.user_key).to_string()
        })
        .collect();
    assert_eq!(keys, ["a", "b", "c", "d", "e"]);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_many_sources() -> crate::Result<()> {
    let iter = Merger::new(
        (0..8)
            .map(|i| {
                vec![Ok(InternalValue::from_components(
                    format!("{}", (b'a' + i) as char),
                    b"",
                    0,
                    Value,
                ))]
                .into_iter()
            })
            .collect(),
        comparator::default_comparator(),
    );

    let keys: Vec<String> = iter
        .map(|r| {
            let v = r.unwrap();
            String::from_utf8_lossy(&v.key.user_key).to_string()
        })
        .collect();
    assert_eq!(keys, ["a", "b", "c", "d", "e", "f", "g", "h"]);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_seqno_ordering() -> crate::Result<()> {
    // Same key, different seqnos — higher seqno must come first.
    let a = vec![Ok(InternalValue::from_components("k", b"v1", 3, Value))];
    let b = vec![Ok(InternalValue::from_components("k", b"v2", 7, Value))];
    let c = vec![Ok(InternalValue::from_components("k", b"v3", 1, Value))];

    let iter = Merger::new(
        vec![a.into_iter(), b.into_iter(), c.into_iter()],
        comparator::default_comparator(),
    );

    let seqnos: Vec<u64> = iter.map(|r| r.unwrap().key.seqno).collect();
    assert_eq!(seqnos, [7, 3, 1]);

    Ok(())
}

/// One source is handed through without the tournament: each entry once, in
/// its order, and then the end.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_single_source_is_handed_through() -> crate::Result<()> {
    let a = vec![
        Ok(InternalValue::from_components("a", b"", 0, Value)),
        Ok(InternalValue::from_components("b", b"", 0, Value)),
        Ok(InternalValue::from_components("c", b"", 0, Value)),
    ];
    let mut iter = Merger::new(vec![a.into_iter()], comparator::default_comparator());
    let k = |v: InternalValue| String::from_utf8_lossy(&v.key.user_key).to_string();

    assert_eq!(k(iter.next().unwrap()?), "a");
    assert_eq!(k(iter.next().unwrap()?), "b");
    assert_eq!(k(iter.next().unwrap()?), "c");
    assert!(iter.next().is_none(), "should be exhausted");

    Ok(())
}

/// Entries with the same key and seqno come out in the order of their inputs:
/// a compaction lists its inputs newest first and keeps the first it sees.
/// Five inputs, so the tournament pads its leaves, and the equal entries meet
/// at every level of it.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_equal_entries_come_out_in_input_order() {
    let inputs: Vec<_> = (0..5u8)
        .map(|input| {
            vec![
                Ok(InternalValue::from_components("k", [input], 7, Value)),
                Ok(InternalValue::from_components("m", [input], 7, Value)),
            ]
            .into_iter()
        })
        .collect();
    let values: Vec<(Vec<u8>, u8)> = Merger::new(inputs, comparator::default_comparator())
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.key.user_key.to_vec(),
                entry.value.first().copied().unwrap(),
            )
        })
        .collect();
    let expected: Vec<(Vec<u8>, u8)> = [b"k", b"m"]
        .into_iter()
        .flat_map(|key| (0..5u8).map(move |input| (key.to_vec(), input)))
        .collect();
    assert_eq!(values, expected);
}

/// Bytewise order backwards: a comparator the merge cannot treat as bytes.
struct Backwards;

impl crate::comparator::UserComparator for Backwards {
    fn name(&self) -> &'static str {
        "merge-tests-backwards"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        b.cmp(a)
    }
}

/// The merge of inputs of uneven lengths, some empty, is the sort of all their
/// entries: under the byte order with and without a declared common prefix
/// (keys that tie in the compared word and differ after it, keys shorter than
/// a word past the prefix, several versions of a key spread over the inputs),
/// and under an order that is not the byte order. Every count of inputs from
/// two to nine, so the padded and the full tournament are both played.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_of_uneven_inputs_is_their_sort() {
    use alloc::sync::Arc;

    // Keys of one shared prefix: short ones, and long ones that differ only
    // past the first eight bytes after it.
    let keys: Vec<Vec<u8>> = (0..120u32)
        .map(|i| {
            let mut key = b"pre/".to_vec();
            if i % 3 == 0 {
                key.extend_from_slice(format!("{:02}", i % 50).as_bytes());
            } else {
                key.extend_from_slice(b"same-eight-bytes");
                key.extend_from_slice(&(i % 40).to_be_bytes());
            }
            key
        })
        .collect();
    for backwards in [false, true] {
        let comparator: SharedComparator = if backwards {
            Arc::new(Backwards)
        } else {
            comparator::default_comparator()
        };
        for fanin in 2..=9usize {
            for prefix in [0, 4] {
                // Entry `i` goes to an input picked unevenly; one input stays
                // empty when there are more than three.
                let mut inputs: Vec<Vec<InternalValue>> = vec![Vec::new(); fanin];
                for (i, key) in keys.iter().enumerate() {
                    let mut input = (i * i + i / 7) % fanin;
                    if fanin > 3 && input == 2 {
                        input = 0;
                    }
                    inputs
                        .get_mut(input)
                        .unwrap()
                        .push(InternalValue::from_components(
                            key.clone(),
                            b"",
                            i as u64,
                            Value,
                        ));
                }
                let order = |a: &InternalValue, b: &InternalValue| {
                    a.key.compare_with(&b.key, comparator.as_ref())
                };
                for input in &mut inputs {
                    input.sort_by(order);
                }
                let mut expected: Vec<InternalValue> = inputs.iter().flatten().cloned().collect();
                expected.sort_by(order);

                let merged: Vec<InternalValue> = Merger::new(
                    inputs
                        .into_iter()
                        .map(|input| input.into_iter().map(Ok))
                        .collect(),
                    comparator.clone(),
                )
                .with_common_prefix(prefix)
                .map(Result::unwrap)
                .collect();
                assert_eq!(
                    merged, expected,
                    "{fanin} inputs, prefix {prefix}, backwards {backwards}"
                );
            }
        }
    }
}

/// An input's error comes out in the place of its next entry, the entry it
/// stood at is dropped with the input, and the other inputs go on in order.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_yields_an_inputs_error_and_goes_on() {
    let a = vec![
        Ok(InternalValue::from_components("a", b"", 0, Value)),
        Err(crate::Error::Unrecoverable),
        Ok(InternalValue::from_components("z", b"", 0, Value)),
    ];
    let b = vec![
        Ok(InternalValue::from_components("b", b"", 0, Value)),
        Ok(InternalValue::from_components("d", b"", 0, Value)),
    ];
    let c = vec![Ok(InternalValue::from_components("c", b"", 0, Value))];
    let mut iter = Merger::new(
        vec![a.into_iter(), b.into_iter(), c.into_iter()],
        comparator::default_comparator(),
    );
    // Taking "a" asks its input for the next entry, which is the error.
    assert!(matches!(
        iter.next().unwrap(),
        Err(crate::Error::Unrecoverable)
    ));
    let rest: Vec<String> = iter
        .map(|entry| String::from_utf8_lossy(&entry.unwrap().key.user_key).to_string())
        .collect();
    assert_eq!(rest, ["b", "c", "d"]);
}

/// Errors the inputs give for their first entries all come out, before any
/// entry, and the inputs that gave an entry are then merged.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_yields_every_first_entry_error_then_the_entries() {
    let failing = || vec![Err(crate::Error::Unrecoverable)].into_iter();
    let a = vec![
        Ok(InternalValue::from_components("b", b"", 0, Value)),
        Ok(InternalValue::from_components("c", b"", 0, Value)),
    ];
    let b = vec![Ok(InternalValue::from_components("a", b"", 0, Value))];
    let mut iter = Merger::new(
        vec![failing(), a.into_iter(), failing(), b.into_iter()],
        comparator::default_comparator(),
    );
    assert!(matches!(
        iter.next().unwrap(),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(
        iter.next().unwrap(),
        Err(crate::Error::Unrecoverable)
    ));
    let rest: Vec<String> = iter
        .map(|entry| String::from_utf8_lossy(&entry.unwrap().key.user_key).to_string())
        .collect();
    assert_eq!(rest, ["a", "b", "c"]);
}

/// A merge of inputs that are all empty ends at once, and stays ended.
#[test]
fn merge_of_empty_inputs_is_empty() {
    let empty = || Vec::<IterItem>::new().into_iter();
    let mut iter = Merger::new(
        vec![empty(), empty(), empty()],
        comparator::default_comparator(),
    );
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}
