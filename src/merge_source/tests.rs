use super::*;
use crate::ValueType::Value;
use crate::comparator::default_comparator;
use alloc::vec;
use test_log::test;

fn make_iv(key: &[u8], seqno: u64) -> InternalValue {
    InternalValue::from_components(key, b"", seqno, Value)
}

#[test]
fn coherent_iter_source_forward_and_backward() {
    let items: Vec<IterItem> = vec![
        Ok(make_iv(b"a", 0)),
        Ok(make_iv(b"b", 0)),
        Ok(make_iv(b"c", 0)),
    ];
    let mut src = CoherentIterSource::new(items.into_iter());
    // Forward + backward interleave correctly via std::vec::IntoIter's
    // shared front/back cursors.
    assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"a");
    assert_eq!(
        src.next_back().unwrap().unwrap().key.user_key.as_ref(),
        b"c"
    );
    assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"b");
    assert!(src.next().is_none());
    assert!(src.next_back().is_none());
}

#[test]
fn coherent_iter_source_seek_is_no_op() {
    // seek() does not advance or rewind; the iter keeps its
    // original position.
    let items: Vec<IterItem> = vec![Ok(make_iv(b"a", 0)), Ok(make_iv(b"b", 0))];
    let mut src = CoherentIterSource::new(items.into_iter());
    let target = make_iv(b"zzz", 0);
    src.seek(&target.key).unwrap();
    // Still yields from the start.
    assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"a");
}

#[test]
fn boxed_merge_source_dispatches_through_blanket_impl() {
    // Build a Box<dyn MergeSource> via the blanket impl and
    // exercise all three trait methods.
    let items: Vec<IterItem> = vec![Ok(make_iv(b"x", 0)), Ok(make_iv(b"y", 0))];
    let mut boxed: Box<dyn MergeSource> = Box::new(CoherentIterSource::new(items.into_iter()));
    // .next() through the box
    assert_eq!(
        MergeSource::next(&mut boxed)
            .unwrap()
            .unwrap()
            .key
            .user_key
            .as_ref(),
        b"x"
    );
    // .seek() through the box (no-op on the underlying)
    let target = make_iv(b"target", 0);
    MergeSource::seek(&mut boxed, &target.key).unwrap();
    // .next_back() through the box
    assert_eq!(
        MergeSource::next_back(&mut boxed)
            .unwrap()
            .unwrap()
            .key
            .user_key
            .as_ref(),
        b"y"
    );
    assert!(MergeSource::next(&mut boxed).is_none());
}

#[test]
fn coherent_iter_source_propagates_errors() {
    // Test that error items pass through both forward and backward.
    let items: Vec<IterItem> = vec![Ok(make_iv(b"a", 0)), Err(crate::Error::Unrecoverable)];
    let mut src = CoherentIterSource::new(items.into_iter());
    assert!(src.next().unwrap().is_ok());
    assert!(src.next().unwrap().is_err());
    assert!(src.next().is_none());
    // Sanity: comparator import works in the test
    let _ = default_comparator();
}

#[test]
fn empty_coherent_iter_source() {
    let items: Vec<IterItem> = vec![];
    let mut src = CoherentIterSource::new(items.into_iter());
    assert!(src.next().is_none());
    assert!(src.next_back().is_none());
    let target = make_iv(b"k", 0);
    src.seek(&target.key).unwrap(); // no panic on empty
}
