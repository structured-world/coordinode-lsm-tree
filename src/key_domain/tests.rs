use super::{DomainCursor, KeyDomains};
use crate::UserKey;
use crate::comparator::default_comparator;
use alloc::sync::Arc;

/// One domain per first byte; `0xFF` runs to the end of the key space.
struct FirstByte;

impl KeyDomains for FirstByte {
    fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
        match key.first() {
            Some(&b) if b < u8::MAX => Some(UserKey::from([b + 1].as_slice())),
            _ => None,
        }
    }
}

fn cursor() -> DomainCursor {
    DomainCursor::new(Arc::new(FirstByte))
}

/// Before an output's first key there is no domain to leave.
#[test]
fn a_fresh_cursor_crosses_nothing() {
    let cmp = default_comparator();
    assert_eq!(cursor().crosses(b"z", cmp.as_ref()), None);
}

/// A key of the output's domain stays; the next domain's first key and any
/// key past it cross, at the domain's end; a key below the end does not.
#[test]
fn crosses_at_and_past_the_domain_end_only() {
    let cmp = default_comparator();
    let mut cursor = cursor();
    cursor.enter(b"a1", false);
    assert_eq!(cursor.crosses(b"a9", cmp.as_ref()), None, "same domain");
    assert_eq!(
        cursor.crosses(b"b", cmp.as_ref()).map(|end| end.to_vec()),
        Some(b"b".to_vec()),
        "the end itself is the next domain",
    );
    assert_eq!(
        cursor.crosses(b"c0", cmp.as_ref()).map(|end| end.to_vec()),
        Some(b"b".to_vec()),
        "past the end, which is still the end crossed",
    );
}

/// Entering without a crossing keeps the output's domain; a crossing takes
/// the new key's.
#[test]
fn enter_moves_the_domain_only_on_a_crossing() {
    let cmp = default_comparator();
    let mut cursor = cursor();
    cursor.enter(b"a1", false);
    cursor.enter(b"a5", false);
    assert!(
        cursor.crosses(b"b0", cmp.as_ref()).is_some(),
        "still a's domain"
    );
    cursor.enter(b"b0", true);
    assert_eq!(cursor.crosses(b"b9", cmp.as_ref()), None, "now b's domain");
    assert!(cursor.crosses(b"c", cmp.as_ref()).is_some());
}

/// The last domain runs to the end of the key space: nothing crosses it.
#[test]
fn the_last_domain_is_never_left() {
    let cmp = default_comparator();
    let mut cursor = cursor();
    cursor.enter(b"\xff\x01", false);
    assert_eq!(cursor.crosses(b"\xff\xff\xff", cmp.as_ref()), None);
}

/// A run of keys from `first` to `last` stays in one domain exactly when
/// `last` is below `first`'s domain end.
#[cfg(feature = "columnar")]
#[test]
fn holds_a_run_within_one_domain_only() {
    let shared = default_comparator();
    let cmp: &dyn crate::comparator::UserComparator = shared.as_ref();
    let cursor = cursor();
    assert!(cursor.holds(b"a0", b"a9", cmp));
    assert!(
        !cursor.holds(b"a0", b"b", cmp),
        "the next domain's first key"
    );
    assert!(
        cursor.holds(b"\xff", b"\xff\xff", cmp),
        "the open last domain"
    );
}
