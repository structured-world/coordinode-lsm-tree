use super::{ByteView, HeapAllocationHeader};
use std::io::Cursor;

/*#[test]
#[cfg(not(miri))]
fn test_rykv() {
    use rkyv::{rancor::Error, Archive, Deserialize, Serialize};

    #[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
    #[rkyv(archived = ArchivedPerson)]
    struct Person {
        id: i64,
        name: String,
    }

    // NOTE: Short Repr
    {
        let a = Person {
            id: 1,
            name: "Alicia".to_string(),
        };

        let bytes = rkyv::to_bytes::<Error>(&a).unwrap();
        let bytes = ByteView::from(&*bytes);

        let archived: &ArchivedPerson = rkyv::access::<_, Error>(&bytes).unwrap();
        assert_eq!(archived.id, a.id);
        assert_eq!(archived.name, a.name);
    }

    // NOTE: Long Repr
    {
        let a = Person {
            id: 1,
            name: "Alicia I need a very long string for heap allocation".to_string(),
        };

        let bytes = rkyv::to_bytes::<Error>(&a).unwrap();
        let bytes = ByteView::from(&*bytes);

        let archived: &ArchivedPerson = rkyv::access::<_, Error>(&bytes).unwrap();
        assert_eq!(archived.id, a.id);
        assert_eq!(archived.name, a.name);
    }
}*/

#[test]
#[cfg(target_pointer_width = "64")]
fn memsize() {
    use super::{LongRepr, ShortRepr, Trailer};

    assert_eq!(
        core::mem::size_of::<ShortRepr>(),
        core::mem::size_of::<LongRepr>()
    );
    assert_eq!(
        core::mem::size_of::<Trailer>(),
        core::mem::size_of::<LongRepr>()
    );

    assert_eq!(24, core::mem::size_of::<ByteView>());
    assert_eq!(
        32,
        core::mem::size_of::<ByteView>() + core::mem::size_of::<HeapAllocationHeader>()
    );
}

#[test]
fn sliced_clone() {
    let s = ByteView::from([
        1, 255, 255, 255, 251, 255, 255, 255, 255, 255, 1, 21, 255, 255, 255, 255, 5, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 4, 3, 255, 255,
        0, 0, 255, 0, 0, 0, 254, 2, 0, 0, 0, 5, 2, 42, 0, 0, 0, 1, 0, 0, 0, 44, 0, 0, 0, 2, 0, 0,
        0,
    ]);
    let slice = s.slice(12..(12 + 21));

    #[allow(clippy::redundant_clone)]
    let cloned = slice.clone();

    assert_eq!(slice.prefix(), cloned.prefix());
    assert_eq!(slice, cloned);
}

#[test]
fn fuse_empty() {
    let bytes = ByteView::fused(&[], &[]);
    assert_eq!(&*bytes, &[] as &[u8]);
}

#[test]
fn fuse_one() {
    let bytes = ByteView::fused(b"abc", &[]);
    assert_eq!(&*bytes, b"abc");
}

#[test]
fn fuse_two() {
    let bytes = ByteView::fused(b"abc", b"def");
    assert_eq!(&*bytes, b"abcdef");
}

#[test]
fn empty_slice() {
    let bytes = ByteView::with_size_zeroed(0);
    assert_eq!(&*bytes, &[] as &[u8]);
}

#[test]
fn dealloc_order() {
    let bytes = ByteView::new(&(0..32).collect::<Vec<_>>());
    let bytes_slice = bytes.slice(..31);
    drop(bytes);
    drop(bytes_slice);
}

#[test]
fn dealloc_order_2() {
    let bytes = ByteView::new(&(0..32).collect::<Vec<_>>());
    let bytes_slice = bytes.slice(..31);
    let bytes_slice_2 = bytes.slice(..5);
    let bytes_slice_3 = bytes.slice(..6);

    drop(bytes);
    drop(bytes_slice);
    drop(bytes_slice_2);
    drop(bytes_slice_3);
}

#[test]
fn from_reader_1() -> std::io::Result<()> {
    let str = b"abcdef";
    let mut cursor = Cursor::new(str);

    let a = ByteView::from_reader(&mut cursor, 6)?;
    assert!(&*a == b"abcdef");

    Ok(())
}

#[test]
fn cmp_misc_1() {
    let a = ByteView::from("abcdef");
    let b = ByteView::from("abcdefhelloworldhelloworld");
    assert!(a < b);
}

#[test]
fn get_mut() {
    let mut slice = ByteView::with_size(4);
    assert_eq!(4, slice.len());
    assert_eq!([0, 0, 0, 0], &*slice);

    {
        let mut mutator = slice.get_mut().unwrap();
        mutator[0] = 1;
        mutator[1] = 2;
        mutator[2] = 3;
        mutator[3] = 4;
    }

    assert_eq!(4, slice.len());
    assert_eq!([1, 2, 3, 4], &*slice);
    assert_eq!([1, 2, 3, 4], slice.prefix());
}

#[test]
fn get_mut_long() {
    let mut slice = ByteView::with_size(30);
    assert_eq!(30, slice.len());
    assert_eq!([0; 30], &*slice);

    {
        let mut mutator = slice.get_mut().unwrap();
        mutator[0] = 1;
        mutator[1] = 2;
        mutator[2] = 3;
        mutator[3] = 4;
    }

    assert_eq!(30, slice.len());
    assert_eq!(
        [
            1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0
        ],
        &*slice
    );
    assert_eq!([1, 2, 3, 4], slice.prefix());
}

#[test]
fn nostr() {
    let slice = ByteView::from("");
    assert_eq!(0, slice.len());
    assert_eq!(&*slice, b"");
    assert_eq!(1, slice.ref_count());
    assert!(slice.is_inline());
}

#[test]
fn default_str() {
    let slice = ByteView::default();
    assert_eq!(0, slice.len());
    assert_eq!(&*slice, b"");
    assert_eq!(1, slice.ref_count());
    assert!(slice.is_inline());
}

#[test]
fn short_str() {
    let slice = ByteView::from("abcdef");
    assert_eq!(6, slice.len());
    assert_eq!(&*slice, b"abcdef");
    assert_eq!(1, slice.ref_count());
    assert_eq!(&slice.prefix(), b"abcd");
    assert!(slice.is_inline());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_str() {
    let slice = ByteView::from("abcdefabcdef");
    assert_eq!(12, slice.len());
    assert_eq!(&*slice, b"abcdefabcdef");
    assert_eq!(1, slice.ref_count());
    assert_eq!(&slice.prefix(), b"abcd");
    assert!(slice.is_inline());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_long_str() {
    let slice = ByteView::from("abcdefabcdefabcdabcd");
    assert_eq!(20, slice.len());
    assert_eq!(&*slice, b"abcdefabcdefabcdabcd");
    assert_eq!(1, slice.ref_count());
    assert_eq!(&slice.prefix(), b"abcd");
    assert!(slice.is_inline());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_str_clone() {
    let slice = ByteView::from("abcdefabcdefabcdefab");
    let copy = slice.clone();
    assert_eq!(slice, copy);
    assert_eq!(copy.prefix(), slice.prefix());

    assert_eq!(1, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());
}

#[test]
fn long_str() {
    let slice = ByteView::from("abcdefabcdefabcdefababcd");
    assert_eq!(24, slice.len());
    assert_eq!(&*slice, b"abcdefabcdefabcdefababcd");
    assert_eq!(1, slice.ref_count());
    assert_eq!(&slice.prefix(), b"abcd");
    assert!(!slice.is_inline());
}

#[test]
fn long_str_clone() {
    let slice = ByteView::from("abcdefabcdefabcdefababcd");
    let copy = slice.clone();
    assert_eq!(slice, copy);
    assert_eq!(copy.prefix(), slice.prefix());

    assert_eq!(2, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());
}

#[test]
fn long_str_slice_full() {
    let slice = ByteView::from("helloworld_thisisalongstring");

    let copy = slice.slice(..);
    assert_eq!(copy, slice);

    assert_eq!(2, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice() {
    let slice = ByteView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!(b"thisisalongstring", &*copy);
    assert_eq!(&copy.prefix(), b"this");

    assert_eq!(1, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice_twice() {
    let slice = ByteView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!(b"thisisalongstring", &*copy);

    let copycopy = copy.slice(..);
    assert_eq!(copy, copycopy);

    assert_eq!(1, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());

    drop(slice);
    assert_eq!(1, copycopy.ref_count());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice_downgrade() {
    let slice = ByteView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!(b"thisisalongstring", &*copy);

    let copycopy = copy.slice(0..4);
    assert_eq!(b"this", &*copycopy);

    {
        let copycopy = copy.slice(0..=4);
        assert_eq!(b"thisi", &*copycopy);
        assert_eq!(b't', *copycopy.first().unwrap());
    }

    assert_eq!(1, slice.ref_count());

    drop(copy);
    assert_eq!(1, slice.ref_count());

    drop(copycopy);
    assert_eq!(1, slice.ref_count());
}

#[test]
fn short_str_clone() {
    let slice = ByteView::from("abcdef");
    let copy = slice.clone();
    assert_eq!(slice, copy);

    assert_eq!(1, slice.ref_count());

    drop(slice);
    assert_eq!(&*copy, b"abcdef");

    assert_eq!(1, copy.ref_count());
}

#[test]
fn short_str_slice_full() {
    let slice = ByteView::from("abcdef");
    let copy = slice.slice(..);
    assert_eq!(slice, copy);

    assert_eq!(1, slice.ref_count());

    drop(slice);
    assert_eq!(&*copy, b"abcdef");

    assert_eq!(1, copy.ref_count());
}

#[test]
fn short_str_slice_part() {
    let slice = ByteView::from("abcdef");
    let copy = slice.slice(3..);

    assert_eq!(1, slice.ref_count());

    drop(slice);
    assert_eq!(&*copy, b"def");

    assert_eq!(1, copy.ref_count());
}

#[test]
fn short_str_slice_empty() {
    let slice = ByteView::from("abcdef");
    let copy = slice.slice(0..0);

    assert_eq!(1, slice.ref_count());

    drop(slice);
    assert_eq!(&*copy, b"");

    assert_eq!(1, copy.ref_count());
}

#[test]
fn tiny_str_starts_with() {
    let a = ByteView::from("abc");
    assert!(a.starts_with(b"ab"));
    assert!(!a.starts_with(b"b"));
}

#[test]
fn long_str_starts_with() {
    let a = ByteView::from("abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef");
    assert!(a.starts_with(b"abcdef"));
    assert!(!a.starts_with(b"def"));
}

#[test]
fn tiny_str_cmp() {
    let a = ByteView::from("abc");
    let b = ByteView::from("def");
    assert!(a < b);
}

#[test]
fn tiny_str_eq() {
    let a = ByteView::from("abc");
    let b = ByteView::from("def");
    assert!(a != b);
}

#[test]
fn long_str_eq() {
    let a = ByteView::from("abcdefabcdefabcdefabcdef");
    let b = ByteView::from("xycdefabcdefabcdefabcdef");
    assert!(a != b);
}

#[test]
fn long_str_cmp() {
    let a = ByteView::from("abcdefabcdefabcdefabcdef");
    let b = ByteView::from("xycdefabcdefabcdefabcdef");
    assert!(a < b);
}

#[test]
fn long_str_eq_2() {
    let a = ByteView::from("abcdefabcdefabcdefabcdef");
    let b = ByteView::from("abcdefabcdefabcdefabcdef");
    assert!(a == b);
}

#[test]
fn long_str_cmp_2() {
    let a = ByteView::from("abcdefabcdefabcdefabcdef");
    let b = ByteView::from("abcdefabcdefabcdefabcdeg");
    assert!(a < b);
}

#[test]
fn long_str_cmp_3() {
    let a = ByteView::from("abcdefabcdefabcdefabcde");
    let b = ByteView::from("abcdefabcdefabcdefabcdef");
    assert!(a < b);
}

#[test]
fn cmp_fuzz_1() {
    let a = ByteView::from([0]);
    let b = ByteView::from([]);

    assert!(a > b);
    assert!(a != b);
}

#[test]
fn cmp_fuzz_2() {
    let a = ByteView::from([0, 0]);
    let b = ByteView::from([0]);

    assert!(a > b);
    assert!(a != b);
}

#[test]
fn cmp_fuzz_3() {
    let a = ByteView::from([255, 255, 12, 255, 0]);
    let b = ByteView::from([255, 255, 12, 255]);

    assert!(a > b);
    assert!(a != b);
}

#[test]
fn cmp_fuzz_4() {
    let a = ByteView::from([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]);
    let b = ByteView::from([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
    ]);

    assert!(a > b);
    assert!(a != b);
}
