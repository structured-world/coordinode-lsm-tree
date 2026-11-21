use super::*;

/// What each kind of value hands the memtable: bytes to copy for everything
/// a caller builds, the buffer itself for a `Slice` (so a caller holding one
/// pays no copy), and the same bytes either way.
#[test]
fn bytes_are_copied_and_a_slice_is_kept() {
    let copied = |bytes: InsertBytes<'_>| match bytes {
        InsertBytes::Borrowed(bytes) => Some(bytes.to_vec()),
        InsertBytes::Shared(_) => None,
    };
    let v = b"value".to_vec();
    assert_eq!(
        b"value".as_slice().with_insert_bytes(copied),
        Some(v.clone())
    );
    assert_eq!(b"value".with_insert_bytes(copied), Some(v.clone()));
    assert_eq!((*b"value").with_insert_bytes(copied), Some(v.clone()));
    assert_eq!("value".with_insert_bytes(copied), Some(v.clone()));
    assert_eq!(
        String::from("value").with_insert_bytes(copied),
        Some(v.clone())
    );
    assert_eq!(
        (&String::from("value")).with_insert_bytes(copied),
        Some(v.clone())
    );
    assert_eq!(v.clone().with_insert_bytes(copied), Some(v.clone()));
    assert_eq!((&v).with_insert_bytes(copied), Some(v.clone()));
    assert_eq!(
        Arc::<[u8]>::from(v.as_slice()).with_insert_bytes(copied),
        Some(v.clone())
    );
    assert_eq!(Arc::<str>::from("value").with_insert_bytes(copied), Some(v));

    let slice = Slice::from(b"value");
    let kept = |bytes: InsertBytes<'_>| match bytes {
        InsertBytes::Shared(value) => Some(value),
        InsertBytes::Borrowed(_) => None,
    };
    assert_eq!((&slice).with_insert_bytes(kept), Some(slice.clone()));
    assert_eq!(slice.clone().with_insert_bytes(kept), Some(slice));
}
