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

/// Inputs that win in runs merge to their sort too: a run is taken against the
/// runner-up alone, and ends where the runner-up's entry comes first, where
/// an equal entry of an earlier input must come first, and where the input
/// runs out. An entry equal to the runner-up's from an earlier input stays
/// ahead of it; from a later input it goes behind.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_of_inputs_that_win_in_runs_is_their_sort() {
    let entry = |key: u32, seqno: u64| {
        InternalValue::from_components(format!("key-{key:04}"), b"", seqno, Value)
    };
    // Two long runs with a gap, the gap's keys, and a sparse input whose
    // entries equal ones of the first (same key and seqno) or lie between.
    let first: Vec<InternalValue> = (0..50).chain(100..150).map(|k| entry(k, 9)).collect();
    let gap: Vec<InternalValue> = (50..100).map(|k| entry(k, 9)).collect();
    let sparse: Vec<InternalValue> = (0..150).step_by(7).map(|k| entry(k, 9)).collect();
    let older: Vec<InternalValue> = (40..60).map(|k| entry(k, 3)).collect();
    for inputs in [
        vec![first.clone(), gap.clone(), sparse.clone(), older.clone()],
        vec![sparse.clone(), older.clone(), first.clone(), gap.clone()],
        vec![gap.clone(), first.clone()],
        vec![older, sparse, gap, first, Vec::new()],
    ] {
        // Stable, so equal entries keep the order of their inputs.
        let mut expected: Vec<(usize, InternalValue)> = inputs
            .iter()
            .enumerate()
            .flat_map(|(input, held)| held.iter().cloned().map(move |entry| (input, entry)))
            .collect();
        expected.sort_by(|a, b| a.1.key.cmp(&b.1.key));

        // Each entry's value names its input, so equal entries are told apart.
        let merged: Vec<(usize, InternalValue)> = Merger::new(
            inputs
                .into_iter()
                .enumerate()
                .map(|(input, held)| {
                    held.into_iter().map(move |mut entry| {
                        entry.value = [u8::try_from(input).unwrap()].into();
                        Ok(entry)
                    })
                })
                .collect(),
            comparator::default_comparator(),
        )
        .with_common_prefix(4)
        .map(|entry| {
            let entry = entry.unwrap();
            (usize::from(entry.value.first().copied().unwrap()), entry)
        })
        .collect();
        assert_eq!(merged.len(), expected.len());
        for (at, (got, want)) in merged.iter().zip(&expected).enumerate() {
            assert_eq!(
                (got.0, &got.1.key),
                (want.0, &want.1.key),
                "entry {at} of the merge"
            );
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

/// Keys whose compared word is the greatest there is, the rank an empty slot
/// carries, still come out, each once and in order: against a slot whose
/// input ran out, and against the slots that pad three inputs to four.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_keeps_entries_of_the_greatest_key_word() {
    let top = |tail: u8| {
        let mut key = vec![0xFF; 8];
        key.push(tail);
        InternalValue::from_components(key, b"", 0, Value)
    };
    // Every key starts with eight 0xFF bytes: all of one rank, the empty
    // slot's. The first input runs out while the others still hold entries.
    let a = vec![Ok(top(1))];
    let b = vec![Ok(top(0)), Ok(top(3)), Ok(top(5))];
    let c = vec![Ok(top(2)), Ok(top(4))];
    let tails: Vec<u8> = Merger::new(
        vec![a.into_iter(), b.into_iter(), c.into_iter()],
        comparator::default_comparator(),
    )
    .map(|entry| entry.unwrap().key.user_key.last().copied().unwrap())
    .collect();
    assert_eq!(tails, [0, 1, 2, 3, 4, 5]);
}

/// Errors the inputs give for their first entries come out in the order of
/// the inputs: a caller that stops at the first one sees the earliest input's.
#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_yields_first_entry_errors_in_input_order() {
    let ok = vec![Ok(InternalValue::from_components("a", b"", 0, Value))];
    let mut iter = Merger::new(
        vec![
            vec![Err(crate::Error::Unrecoverable)].into_iter(),
            ok.into_iter(),
            vec![Err(crate::Error::Cancelled)].into_iter(),
        ],
        comparator::default_comparator(),
    );
    assert!(matches!(
        iter.next().unwrap(),
        Err(crate::Error::Unrecoverable)
    ));
    assert!(matches!(iter.next().unwrap(), Err(crate::Error::Cancelled)));
    assert_eq!(
        iter.next().unwrap().unwrap().key.user_key.as_ref(),
        b"a".as_slice()
    );
    assert!(iter.next().is_none());
}

/// An input whose first pull panics leaves the merger refusing: a caller that
/// caught the panic and asks again gets an error, never the entries of the
/// other inputs with one of them gone, and never a clean end.
#[cfg(feature = "std")]
#[test]
fn merge_refuses_after_an_inputs_first_pull_panicked() {
    let a: Box<dyn Iterator<Item = IterItem>> =
        Box::new(vec![Ok(InternalValue::from_components("a", b"", 0, Value))].into_iter());
    let mut pulls = 0;
    let b: Box<dyn Iterator<Item = IterItem>> = Box::new(core::iter::from_fn(move || {
        pulls += 1;
        assert!(pulls != 1, "the input's first pull fails");
        (pulls == 2).then(|| Ok(InternalValue::from_components("b", b"", 0, Value)))
    }));
    let mut iter = Merger::new(vec![a, b], comparator::default_comparator());

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iter.next()));
    assert!(unwound.is_err(), "the input's panic carries on");
    for _ in 0..3 {
        assert!(
            matches!(iter.next(), Some(Err(_))),
            "a merger a panic left half built answers with an error"
        );
    }
}

/// Orders keys by their bytes and panics when asked about `b` and `c`.
#[cfg(feature = "std")]
struct PanicsOnBAndC;

#[cfg(feature = "std")]
impl crate::comparator::UserComparator for PanicsOnBAndC {
    fn name(&self) -> &'static str {
        "merge-tests-panics-on-b-and-c"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        assert!(
            !matches!((a, b), (b"b", b"c") | (b"c", b"b")),
            "this comparator fails on b and c"
        );
        a.cmp(b)
    }
}

/// A comparator that panics while the tournament is replayed leaves the merger
/// refusing. Four inputs of one entry each: taking `a` empties its slot, `b`
/// wins the lower game, and the game at the root, `b` against `c`, panics with
/// the lower node rewritten and the winner not yet. Asked again, the merger
/// answers with an error, not with a clean end while `b`, `c` and `d` wait.
#[cfg(feature = "std")]
#[test]
fn merge_refuses_after_a_comparator_panicked_in_a_replay() {
    use alloc::sync::Arc;

    let inputs: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|key| vec![Ok(InternalValue::from_components(key, b"", 0, Value))].into_iter())
        .collect();
    let mut iter = Merger::new(inputs, Arc::new(PanicsOnBAndC));

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| iter.next()));
    assert!(unwound.is_err(), "the comparator's panic carries on");
    for _ in 0..3 {
        assert!(
            matches!(iter.next(), Some(Err(_))),
            "a merger a panic left half replayed answers with an error"
        );
    }
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

/// The work a merger reports is the games it played: the benches divide by
/// these counts, so a game counted twice or not at all skews every figure.
/// Two versions of one key: the build ties on the word and goes to the keys,
/// then each input running out replays one game, the first of them a loss.
#[cfg(feature = "diagnostics")]
#[test]
fn merge_reports_the_games_it_played() {
    let newer = vec![Ok(InternalValue::from_components("k", b"", 1, Value))];
    let older = vec![Ok(InternalValue::from_components("k", b"", 0, Value))];
    let mut iter = Merger::new(
        vec![newer.into_iter(), older.into_iter()],
        comparator::default_comparator(),
    );
    assert_eq!(iter.by_ref().count(), 2);

    let work = iter.work();
    assert_eq!(work.word_compares, 3);
    assert_eq!(work.key_compares, 1);
    assert_eq!(work.moves, 1);
}
