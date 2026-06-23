use crate::table::block::TRAILER_START_MARKER;
use strum::IntoEnumIterator;
use test_log::test;

#[test]
fn value_type_never_block_trailer_start_marker() {
    for variant in crate::ValueType::iter() {
        let n: u8 = variant.into();
        assert_ne!(n, TRAILER_START_MARKER);
    }
}
