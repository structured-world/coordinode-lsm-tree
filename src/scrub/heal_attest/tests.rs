#![expect(clippy::unwrap_used, reason = "test asserts on known-present values")]

use super::{attest_path, attests, remove, write};
use crate::Checksum;
use crate::fs::{Fs, StdFs};
use std::sync::Arc;

fn ck(v: u128) -> Checksum {
    Checksum::from_raw(v)
}

#[test]
fn attests_roundtrips_a_plaintext_attestation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // Trusted only when file digest == post, manifest == pre, and the id matches.
    assert!(
        attests(&*fs, &path, None, 7, ck(200), ck(100)),
        "a matching attestation is trusted",
    );
}

#[test]
fn attests_rejects_a_mismatched_digest_or_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // Wrong current (file) digest: the file is not the attested healed version.
    assert!(!attests(&*fs, &path, None, 7, ck(999), ck(100)));
    // Wrong manifest digest: something else moved the manifest since the heal.
    assert!(!attests(&*fs, &path, None, 7, ck(200), ck(999)));
    // Wrong table id.
    assert!(!attests(&*fs, &path, None, 8, ck(200), ck(100)));
}

#[test]
fn attests_is_false_without_a_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    assert!(!attests(&*fs, &path, None, 7, ck(200), ck(100)));
}

#[test]
fn attests_rejects_a_truncated_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // Drop a trailing byte so the plaintext payload is shorter than the fixed
    // 40-byte record: `deserialize`'s `get(..)?` bound must reject it (the
    // encrypted tamper test never reaches this path — AEAD fails first).
    let ap = attest_path(&path);
    let bytes = std::fs::read(&ap).unwrap();
    let Some(short) = bytes.get(..bytes.len() - 1) else {
        panic!("the written sidecar is non-empty");
    };
    std::fs::write(&ap, short).unwrap();
    assert!(
        !attests(&*fs, &path, None, 7, ck(200), ck(100)),
        "a short attestation must be rejected",
    );
}

#[test]
fn remove_deletes_the_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    assert!(attest_path(&path).exists());
    remove(&*fs, &path);
    assert!(!attest_path(&path).exists());
}

/// The encrypted attestation is AEAD-sealed: a tampered ciphertext or a wrong
/// key fails the open, so an offline attacker without the key cannot forge one.
#[cfg(feature = "encryption")]
#[test]
fn attests_rejects_a_tampered_or_wrong_key_encrypted_sidecar() {
    use crate::encryption::Aes256GcmProvider;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc = Aes256GcmProvider::new(&[7u8; 32]);

    // A valid sealed attestation is trusted.
    write(&*fs, &path, Some(&enc), 7, ck(100), ck(200)).unwrap();
    assert!(attests(&*fs, &path, Some(&enc), 7, ck(200), ck(100)));

    // A flipped ciphertext byte fails the AEAD open.
    let ap = attest_path(&path);
    let mut bytes = std::fs::read(&ap).unwrap();
    if let Some(b) = bytes.last_mut() {
        *b ^= 0xFF;
    }
    std::fs::write(&ap, &bytes).unwrap();
    assert!(
        !attests(&*fs, &path, Some(&enc), 7, ck(200), ck(100)),
        "a tampered encrypted attestation must be rejected",
    );

    // A sidecar sealed under a DIFFERENT key is rejected too.
    write(&*fs, &path, Some(&enc), 7, ck(100), ck(200)).unwrap();
    let wrong = Aes256GcmProvider::new(&[9u8; 32]);
    assert!(
        !attests(&*fs, &path, Some(&wrong), 7, ck(200), ck(100)),
        "an attestation sealed under a different key must be rejected",
    );
}
