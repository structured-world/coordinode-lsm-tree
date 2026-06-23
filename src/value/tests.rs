use super::*;
use test_log::test;

#[test]
fn pik_cmp_user_key() {
    let a = InternalKey::new(*b"a", 0, ValueType::Value);
    let b = InternalKey::new(*b"b", 0, ValueType::Value);
    assert!(a < b);
}

#[test]
fn pik_cmp_seqno() {
    let a = InternalKey::new(*b"a", 0, ValueType::Value);
    let b = InternalKey::new(*b"a", 1, ValueType::Value);
    assert!(a > b);
}
