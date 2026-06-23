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

#[test]
#[expect(clippy::unwrap_used, reason = "test assertions")]
fn merge_mixed_direction() -> crate::Result<()> {
    // Two sources with non-overlapping keys: a,c,e and b,d,f.
    // Interleave next() and next_back() to exercise shared heap state.
    let a = vec![
        Ok(InternalValue::from_components("a", b"", 0, Value)),
        Ok(InternalValue::from_components("c", b"", 0, Value)),
        Ok(InternalValue::from_components("e", b"", 0, Value)),
    ];
    let b = vec![
        Ok(InternalValue::from_components("b", b"", 0, Value)),
        Ok(InternalValue::from_components("d", b"", 0, Value)),
        Ok(InternalValue::from_components("f", b"", 0, Value)),
    ];

    let mut iter = Merger::new(
        vec![a.into_iter(), b.into_iter()],
        comparator::default_comparator(),
    );

    // Consume from both ends, meeting in the middle.
    let k = |v: InternalValue| String::from_utf8_lossy(&v.key.user_key).to_string();

    assert_eq!(k(iter.next().unwrap()?), "a");
    assert_eq!(k(iter.next_back().unwrap()?), "f");
    assert_eq!(k(iter.next().unwrap()?), "b");
    assert_eq!(k(iter.next_back().unwrap()?), "e");
    assert_eq!(k(iter.next().unwrap()?), "c");
    assert_eq!(k(iter.next_back().unwrap()?), "d");
    assert!(iter.next().is_none(), "should be exhausted");

    Ok(())
}
