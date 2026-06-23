use super::Slice;
use std::{fmt::Debug, sync::Arc};
use test_log::test;

fn assert_slice_handles<T>(v: T)
where
    T: Clone + Debug,
    Slice: From<T> + PartialEq<T> + PartialOrd<T>,
{
    // verify slice arc roundtrips
    let slice: Slice = v.clone().into();
    assert_eq!(slice, v, "slice_arc: {slice:?}, v: {v:?}");
    assert!(slice >= v, "slice_arc: {slice:?}, v: {v:?}");
}

#[test]
fn slice_empty() {
    assert_eq!(Slice::empty(), []);
}

#[test]
fn slice_fuse_empty() {
    let bytes = Slice::fused(&[], &[]);
    assert_eq!(&*bytes, &[] as &[u8]);
}

#[test]
fn slice_fuse_one() {
    let bytes = Slice::fused(b"abc", &[]);
    assert_eq!(&*bytes, b"abc");
}

#[test]
fn slice_fuse_two() {
    let bytes = Slice::fused(b"abc", b"def");
    assert_eq!(&*bytes, b"abcdef");
}

#[test]
#[expect(unsafe_code)]
fn slice_with_size() {
    assert_eq!(
        &*unsafe {
            let mut b = Slice::builder_unzeroed(5);
            b.fill(0);
            b.freeze()
        },
        [0; 5],
    );
    assert_eq!(
        &*unsafe {
            let mut b = Slice::builder_unzeroed(50);
            b.fill(0);
            b.freeze()
        },
        [0; 50],
    );
    assert_eq!(
        &*unsafe {
            let mut b = Slice::builder_unzeroed(50);
            b.fill(77);
            b.freeze()
        },
        [77; 50],
    );
}

/// This test verifies that we can create a `Slice` from various types and compare a `Slice` with them.
#[test]
fn test_slice_instantiation() {
    // - &[u8]
    assert_slice_handles::<&[u8]>(&[1, 2, 3, 4]);
    // - Arc<u8>
    assert_slice_handles::<Arc<[u8]>>(Arc::new([1, 2, 3, 4]));
    // - Vec<u8>
    assert_slice_handles::<Vec<u8>>(vec![1, 2, 3, 4]);
    // - &str
    assert_slice_handles::<&str>("hello");
    // - String
    assert_slice_handles::<String>("hello".to_string());
    // - [u8; N]
    assert_slice_handles::<[u8; 4]>([1, 2, 3, 4]);

    // Special case for these types
    // - Iterator<Item = u8>
    let slice = Slice::from_iter(vec![1, 2, 3, 4]);
    assert_eq!(slice, vec![1, 2, 3, 4]);

    // - Arc<str>
    let arc_str: Arc<str> = Arc::from("hello");
    let slice = Slice::from(arc_str.clone());
    assert_eq!(slice.as_ref(), arc_str.as_bytes());

    // - io::Read
    let mut reader = std::io::Cursor::new(vec![1, 2, 3, 4]);
    let slice = Slice::from_reader(&mut reader, 4).expect("read");
    assert_eq!(slice, vec![1, 2, 3, 4]);
}
