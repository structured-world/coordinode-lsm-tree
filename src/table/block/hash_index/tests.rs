use super::*;
use test_log::test;

#[test]
fn hash_index_build_simple() {
    let mut hash_index = Builder::with_bucket_count(100);

    hash_index.set(b"a", 5);
    hash_index.set(b"b", 8);
    hash_index.set(b"c", 10);

    let bytes = hash_index.into_inner();

    // NOTE: Hash index bytes need to be consistent across machines and compilations etc.
    assert_eq!(
        [
            254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 10, 254, 254, 254, 8, 254, 254,
            254, 5, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
            254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
            254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
            254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
            254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254
        ],
        &*bytes
    );

    let reader = Reader::new(&bytes, 0, 100);
    assert_eq!(0, reader.conflict_count());

    assert_eq!(5, reader.get(b"a"));
    assert_eq!(8, reader.get(b"b"));
    assert_eq!(10, reader.get(b"c"));
    assert_eq!(MARKER_FREE, reader.get(b"d"));
}

#[test]
fn hash_index_build_conflict() {
    let mut hash_index = Builder::with_bucket_count(1);

    hash_index.set(b"a", 5);
    hash_index.set(b"b", 8);

    let bytes = hash_index.into_inner();

    assert_eq!([255], &*bytes);

    assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
}

#[test]
fn hash_index_build_same_offset() {
    let mut hash_index = Builder::with_bucket_count(1);

    hash_index.set(b"a", 5);
    hash_index.set(b"b", 5);

    let bytes = hash_index.into_inner();

    assert_eq!([5], &*bytes);

    let reader = Reader::new(&bytes, 0, 1);
    assert_eq!(0, reader.conflict_count());
    assert_eq!(5, reader.get(b"a"));
    assert_eq!(5, reader.get(b"b"));
}

#[test]
fn hash_index_build_mix() {
    let mut hash_index = Builder::with_bucket_count(1);

    hash_index.set(b"a", 5);
    hash_index.set(b"b", 5);
    hash_index.set(b"c", 6);

    let bytes = hash_index.into_inner();

    assert_eq!([255], &*bytes);

    assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
}

#[test]
fn hash_index_read_conflict() {
    let mut hash_index = Builder::with_bucket_count(1);

    hash_index.set(b"a", 5);
    hash_index.set(b"b", 8);

    let bytes = hash_index.into_inner();

    let reader = Reader::new(&bytes, 0, 1);
    assert_eq!(MARKER_CONFLICT, reader.get(b"a"));
    assert_eq!(MARKER_CONFLICT, reader.get(b"b"));
    assert_eq!(MARKER_CONFLICT, reader.get(b"c"));

    assert_eq!(1, Reader::new(&bytes, 0, 1).conflict_count());
}
