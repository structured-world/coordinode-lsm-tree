use super::*;

#[test]
fn display_lists_byte_values_in_hex() {
    // Operators reading logs need the raw byte values, not just the
    // variant name, to cross-reference the on-disk `MetadataPayload`.
    let err = DecryptError::UnsupportedSuite { suite_id: 0xAB };
    assert!(format!("{err}").contains("0xAB"));

    let err = DecryptError::UnsupportedFormatVersion { header_byte: 0x20 };
    assert!(format!("{err}").contains("0x20"));

    let err = DecryptError::UnknownKeyEpoch { key_epoch: 0x07 };
    assert!(format!("{err}").contains("0x07"));

    let err = DecryptError::SuiteMismatch {
        expected: 0x02,
        actual: 0x03,
    };
    let s = format!("{err}");
    assert!(s.contains("0x02"));
    assert!(s.contains("0x03"));
}

#[test]
fn aead_failure_does_not_leak_byte_values() {
    // AEAD-verification failures intentionally carry no payload:
    // identifying *which* AAD byte caused the mismatch would help
    // an attacker tune their tampering attempts. The variant is
    // pure-marker; the message is a generic operator-facing string.
    let err = DecryptError::AeadVerificationFailed;
    let s = format!("{err}");
    assert!(s.contains("AEAD"));
    // No hex byte values should appear.
    assert!(!s.contains("0x"));
}
