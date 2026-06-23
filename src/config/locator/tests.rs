use super::*;

fn is_enabled(entry: LocatorPolicyEntry) -> bool {
    matches!(entry, LocatorPolicyEntry::Enabled { .. })
}

#[test]
fn disabled_policy_reports_no_level_enabled() {
    let policy = LocatorPolicy::disabled();
    assert!(!is_enabled(policy.get(0)));
    assert!(!is_enabled(policy.get(7)));
}

#[test]
fn get_beyond_length_falls_back_to_last_entry() {
    let policy = LocatorPolicy::new(vec![
        LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        },
        LocatorPolicyEntry::None,
    ]);
    assert!(is_enabled(policy.get(0)));
    assert!(!is_enabled(policy.get(1)));
    // Level 5 has no explicit entry → falls back to the last (None).
    assert!(!is_enabled(policy.get(5)));
}

#[test]
fn all_applies_one_entry_to_every_level() {
    let policy = LocatorPolicy::all(LocatorPolicyEntry::Enabled {
        precision: LocatorPrecision::Entry,
        block_id_bits: Some(20),
        slot_bits: Some(10),
    });
    for level in 0..4 {
        assert_eq!(
            policy.get(level),
            LocatorPolicyEntry::Enabled {
                precision: LocatorPrecision::Entry,
                block_id_bits: Some(20),
                slot_bits: Some(10),
            },
        );
    }
}

#[test]
#[should_panic(expected = "locator policy may not be empty")]
fn new_rejects_empty_policy() {
    let _ = LocatorPolicy::new(Vec::new());
}
