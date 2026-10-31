use super::ReadBuf;
use core::mem::MaybeUninit;

/// Safe code gets zeroes, never what the memory held, so a reader that looks
/// before it writes sees nothing it should not.
#[test]
fn init_mut_zeroes_what_is_uninitialized() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    assert_eq!(buf.init_mut(), &[0; 8]);
}

/// Asking twice keeps what the first read wrote: zeroing again would pay
/// twice and lose it.
#[test]
fn init_mut_keeps_what_is_initialized() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 4];
    let mut buf = ReadBuf::uninit(&mut memory);
    buf.init_mut().fill(7);
    assert_eq!(buf.init_mut(), &[7; 4]);
}

/// Safe code cannot claim more than it was handed: a claim past the
/// initialized bytes would freeze memory nobody wrote.
#[test]
#[should_panic(expected = "filled past the initialized bytes")]
fn set_filled_refuses_what_was_never_initialized() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    buf.set_filled(1);
}

#[test]
fn set_filled_takes_what_was_initialized() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    buf.init_mut();
    buf.set_filled(8);
    assert_eq!(buf.filled(), 8);
}

/// A portion of an initialized buffer starts initialized, and one of an
/// uninitialized buffer does not: its zeroing is its own.
#[test]
fn a_portion_knows_what_of_it_is_initialized() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    buf.init_mut().fill(3);
    let mut portion = buf.portion(2, 4);
    assert_eq!(portion.capacity(), 4);
    portion.set_filled(4);
    assert_eq!(portion.init_mut(), &[3; 4]);

    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    assert_eq!(buf.portion(0, 4).init_mut(), &[0; 4]);
}

#[test]
#[should_panic(expected = "range end index")]
fn a_portion_past_the_buffer_panics() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 4];
    let mut buf = ReadBuf::uninit(&mut memory);
    let _ = buf.portion(2, 4);
}

#[test]
fn portions_add_up_to_what_they_filled() {
    let mut memory = [MaybeUninit::<u8>::uninit(); 8];
    let mut buf = ReadBuf::uninit(&mut memory);
    buf.init_mut();
    buf.extend_from_portion(3);
    buf.extend_from_portion(2);
    assert_eq!(buf.filled(), 5);
}
