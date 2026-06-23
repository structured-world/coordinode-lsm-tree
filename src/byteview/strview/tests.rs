use super::StrView;
use std::collections::HashMap;

#[cfg(feature = "serde")]
#[test]
fn serde_roundtrip() {
    let a = StrView::from("abcdef");
    let b: StrView = serde_json::from_slice(&serde_json::to_vec(&a).unwrap()).unwrap();
    assert_eq!(a, b);
}

#[test]
fn strview_hash() {
    let a = StrView::from("abcdef");

    let mut map = HashMap::new();
    map.insert(a, 0);
    assert!(map.contains_key("abcdef"));
}

#[test]
fn cmp_misc_1() {
    let a = StrView::from("abcdef");
    let b = StrView::from("abcdefhelloworldhelloworld");
    assert!(a < b);
}

#[test]
fn nostr() {
    let slice = StrView::from("");
    assert_eq!(0, slice.len());
    assert_eq!(&*slice, "");
}

#[test]
fn default_str() {
    let slice = StrView::default();
    assert_eq!(0, slice.len());
    assert_eq!(&*slice, "");
}

#[test]
fn short_str() {
    let slice = StrView::from("abcdef");
    assert_eq!(6, slice.len());
    assert_eq!(&*slice, "abcdef");
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_str() {
    let slice = StrView::from("abcdefabcdef");
    assert_eq!(12, slice.len());
    assert_eq!(&*slice, "abcdefabcdef");
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_long_str() {
    let slice = StrView::from("abcdefabcdefabcdabcd");
    assert_eq!(20, slice.len());
    assert_eq!(&*slice, "abcdefabcdefabcdabcd");
}

#[test]
#[cfg(target_pointer_width = "64")]
fn medium_str_clone() {
    let slice = StrView::from("abcdefabcdefabcdefa");

    #[allow(clippy::redundant_clone)]
    let copy = slice.clone();

    assert_eq!(slice, copy);
}

#[test]
fn long_str() {
    let slice = StrView::from("abcdefabcdefabcdefababcd");
    assert_eq!(24, slice.len());
    assert_eq!(&*slice, "abcdefabcdefabcdefababcd");
}

#[test]
fn long_str_clone() {
    let slice = StrView::from("abcdefabcdefabcdefababcd");

    #[allow(clippy::redundant_clone)]
    let copy = slice.clone();

    assert_eq!(slice, copy);
}

#[test]
fn long_str_slice_full() {
    let slice = StrView::from("helloworld_thisisalongstring");

    let copy = slice.slice(..);
    assert_eq!(copy, slice);
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice() {
    let slice = StrView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!("thisisalongstring", &*copy);
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice_twice() {
    let slice = StrView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!("thisisalongstring", &*copy);

    let copycopy = copy.slice(..);
    assert_eq!(copy, copycopy);
}

#[test]
#[cfg(target_pointer_width = "64")]
fn long_str_slice_downgrade() {
    let slice = StrView::from("helloworld_thisisalongstring");

    let copy = slice.slice(11..);
    assert_eq!("thisisalongstring", &*copy);

    let copycopy = copy.slice(0..4);
    assert_eq!("this", &*copycopy);

    {
        let copycopy = copy.slice(0..=4);
        assert_eq!("thisi", &*copycopy);
        assert_eq!('t', copycopy.chars().next().unwrap());
    }
}

#[test]
fn short_str_clone() {
    let slice = StrView::from("abcdef");
    let copy = slice.clone();
    assert_eq!(slice, copy);

    drop(slice);
    assert_eq!(&*copy, "abcdef");
}

#[test]
fn short_str_slice_full() {
    let slice = StrView::from("abcdef");
    let copy = slice.slice(..);
    assert_eq!(slice, copy);

    drop(slice);
    assert_eq!(&*copy, "abcdef");
}

#[test]
fn short_str_slice_part() {
    let slice = StrView::from("abcdef");
    let copy = slice.slice(3..);

    drop(slice);
    assert_eq!(&*copy, "def");
}

#[test]
fn short_str_slice_empty() {
    let slice = StrView::from("abcdef");
    let copy = slice.slice(0..0);

    drop(slice);
    assert_eq!(&*copy, "");
}

#[test]
fn tiny_str_starts_with() {
    let a = StrView::from("abc");
    assert!(a.starts_with("ab"));
    assert!(!a.starts_with("b"));
}

#[test]
fn long_str_starts_with() {
    let a = StrView::from("abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef");
    assert!(a.starts_with("abcdef"));
    assert!(!a.starts_with("def"));
}

#[test]
fn tiny_str_cmp() {
    let a = StrView::from("abc");
    let b = StrView::from("def");
    assert!(a < b);
}

#[test]
fn tiny_str_eq() {
    let a = StrView::from("abc");
    let b = StrView::from("def");
    assert!(a != b);
}

#[test]
fn long_str_eq() {
    let a = StrView::from("abcdefabcdefabcdefabcdef");
    let b = StrView::from("xycdefabcdefabcdefabcdef");
    assert!(a != b);
}

#[test]
fn long_str_cmp() {
    let a = StrView::from("abcdefabcdefabcdefabcdef");
    let b = StrView::from("xycdefabcdefabcdefabcdef");
    assert!(a < b);
}

#[test]
fn long_str_eq_2() {
    let a = StrView::from("abcdefabcdefabcdefabcdef");
    let b = StrView::from("abcdefabcdefabcdefabcdef");
    assert!(a == b);
}

#[test]
fn long_str_cmp_2() {
    let a = StrView::from("abcdefabcdefabcdefabcdef");
    let b = StrView::from("abcdefabcdefabcdefabcdeg");
    assert!(a < b);
}

#[test]
fn long_str_cmp_3() {
    let a = StrView::from("abcdefabcdefabcdefabcde");
    let b = StrView::from("abcdefabcdefabcdefabcdef");
    assert!(a < b);
}
