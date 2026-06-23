use super::*;

#[test]
fn new_zeroed_4k_aligned() {
    let buf = AlignedBuf::new_zeroed(8192, 4096).unwrap();
    assert_eq!(buf.capacity(), 8192);
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.alignment(), 4096);
    assert_eq!(buf.as_ptr().addr() % 4096, 0, "pointer not 4 KiB aligned");
    assert!(buf.is_empty());
}

#[test]
fn new_zeroed_rounds_capacity_up_to_alignment() {
    // 5000 bytes requested at 4 KiB alignment → rounded to 8 KiB.
    let buf = AlignedBuf::new_zeroed(5000, 4096).unwrap();
    assert_eq!(buf.capacity(), 8192);
    // Already a multiple → no rounding.
    let buf = AlignedBuf::new_zeroed(8192, 4096).unwrap();
    assert_eq!(buf.capacity(), 8192);
}

#[test]
fn new_zeroed_returns_zeroed_memory() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    // `as_capacity_mut` covers the full capacity — safe API,
    // no need for raw-pointer slicing in tests.
    let cap = buf.as_capacity_mut();
    assert!(cap.iter().all(|&b| b == 0));
}

#[test]
fn new_zeroed_rejects_non_power_of_two_alignment() {
    assert!(AlignedBuf::new_zeroed(4096, 3000).is_none());
    assert!(AlignedBuf::new_zeroed(4096, 0).is_none());
}

#[test]
fn new_zeroed_rejects_excessive_alignment() {
    // isize::MAX + 1 is a power of two but exceeds the cap.
    assert!(AlignedBuf::new_zeroed(4096, (isize::MAX as usize) + 1).is_none());
}

#[test]
fn new_zeroed_zero_capacity_returns_dangling() {
    // Zero-byte AlignedBuf is allowed and never touches the
    // allocator; the dangling sentinel must still satisfy the
    // alignment promise so callers that pass it to FFI don't
    // surprise the kernel.
    let buf = AlignedBuf::new_zeroed(0, 4096).unwrap();
    assert_eq!(buf.capacity(), 0);
    assert_eq!(buf.as_ptr().addr() % 4096, 0);
    assert!(buf.as_slice().is_empty());
}

#[test]
fn set_len_grows_visible_slice() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    assert_eq!(buf.as_slice().len(), 0);
    buf.set_len(1024);
    assert_eq!(buf.as_slice().len(), 1024);
    assert_eq!(buf.len(), 1024);
}

#[test]
#[should_panic(expected = "AlignedBuf::set_len exceeds capacity")]
fn set_len_panics_past_capacity() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    buf.set_len(buf.capacity() + 1);
}

#[test]
fn clear_resets_len_but_preserves_capacity() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    buf.set_len(2048);
    buf.clear();
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.capacity(), 4096);
}

#[test]
fn as_capacity_mut_covers_full_capacity() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    let cap = buf.as_capacity_mut();
    assert_eq!(cap.len(), 4096);
    *cap.first_mut().unwrap() = 0xAB;
    *cap.last_mut().unwrap() = 0xCD;
    buf.set_len(4096);
    let slice = buf.as_slice();
    assert_eq!(slice.first().copied(), Some(0xAB));
    assert_eq!(slice.last().copied(), Some(0xCD));
}

#[test]
fn send_sync_compile_check() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AlignedBuf>();
}

#[test]
fn pointer_stays_stable_across_writes() {
    let mut buf = AlignedBuf::new_zeroed(4096, 4096).unwrap();
    let initial = buf.as_ptr();
    // Write some content + set_len; pointer must not move
    // (no reallocation: AlignedBuf has no growth API).
    *buf.as_capacity_mut().first_mut().unwrap() = 1;
    buf.set_len(1);
    assert_eq!(buf.as_ptr(), initial);
}
