#![expect(clippy::unwrap_used, reason = "test asserts on known-present values")]

use super::{AttestResult, attest_path, attests, remove, write, write_in_progress};
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
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Attests
        ),
        "a matching attestation is trusted",
    );
}

#[test]
fn attests_rejects_a_mismatched_digest_or_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // A present-but-mismatched marker is conclusively non-attesting (Absent),
    // not inconclusive: the sidecar read cleanly, its values just do not match.
    // Wrong current (file) digest: the file is not the attested healed version.
    assert!(matches!(
        attests(&*fs, &path, None, 7, ck(999), ck(100)),
        AttestResult::Absent
    ));
    // Wrong manifest digest: something else moved the manifest since the heal.
    assert!(matches!(
        attests(&*fs, &path, None, 7, ck(200), ck(999)),
        AttestResult::Absent
    ));
    // Wrong table id.
    assert!(matches!(
        attests(&*fs, &path, None, 8, ck(200), ck(100)),
        AttestResult::Absent
    ));
}

#[test]
fn attests_is_false_without_a_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A conclusively absent sidecar (no file) is Absent, safe to clear.
    assert!(matches!(
        attests(&*fs, &path, None, 7, ck(200), ck(100)),
        AttestResult::Absent
    ));
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
    // A malformed (truncated) sidecar is INCONCLUSIVE, not Absent: it does not
    // attest (so the digest is not reconciled), but it must not be treated as a
    // clean absence that would license deleting a possibly-recoverable marker.
    assert!(
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "a short attestation is inconclusive, not attesting",
    );
}

#[test]
fn attests_rejects_an_oversized_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A valid completed marker whose prefix `deserialize` would accept, followed
    // by a long garbage tail. Without a read bound the sidecar reader would slurp
    // the whole attacker-controlled length and then accept the leading record,
    // both a memory-exhaustion vector and a laundered oversized marker.
    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    let ap = attest_path(&path);
    let mut bytes = std::fs::read(&ap).unwrap();
    bytes.extend(std::iter::repeat_n(0u8, 4096));
    std::fs::write(&ap, &bytes).unwrap();

    // An oversized sidecar is INCONCLUSIVE (rejected), never a trusted marker:
    // a valid plaintext record is exactly the fixed length, so extra bytes mean
    // the file is not a marker this build wrote.
    assert!(
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "an oversized attestation is inconclusive, not attesting",
    );
}

#[test]
fn attests_rejects_a_legacy_in_progress_marker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A pre-only (in-progress) marker with the CORRECT table id and pre-digest:
    // only its `kind` differs from a completed marker. It must never authorize a
    // reconcile, so `attests` rejects it through the Absent branch (a bare
    // pre-only marker is safe to clear, it does not bind a post-heal digest).
    write_in_progress(&*fs, &path, None, 7, ck(100)).unwrap();
    assert!(
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Absent
        ),
        "a legacy in-progress marker is non-attesting (Absent)",
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

/// A completed-attestation write that FAILS must leave an already-durable
/// in-progress marker intact. The completed sidecar publishes through a temp +
/// atomic rename, so a mid-write failure never touches the live sidecar; the
/// in-progress marker survives to bridge the crash window. Without this the
/// completed write truncates the marker in place before failing, destroying it
/// and leaving the healed SST with a stale manifest digest and no valid
/// attestation, permanently unreconcilable.
#[test]
fn a_failed_completed_write_preserves_the_in_progress_marker() {
    use super::{attests_in_progress, write_in_progress};
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    // A durable in-progress marker exists (file hashes to the manifest `pre`).
    write_in_progress(&*fs, &path, None, 7, ck(100)).unwrap();
    assert!(
        attests_in_progress(&*fs, &path, None, 7, ck(100)),
        "the in-progress marker is written and durable",
    );

    // Now the completed write fails mid-write (its temp sidecar write faults).
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path(".heal-attest"),
    );
    assert!(
        write(&*fs, &path, None, 7, ck(100), ck(200)).is_err(),
        "the faulted completed-attestation write must fail",
    );

    // The in-progress marker must still be intact: the failed completed write
    // never truncated the live sidecar.
    assert!(
        attests_in_progress(&*fs, &path, None, 7, ck(100)),
        "a failed completed write must leave the in-progress marker intact",
    );
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
    assert!(matches!(
        attests(&*fs, &path, Some(&enc), 7, ck(200), ck(100)),
        AttestResult::Attests
    ));

    // A flipped ciphertext byte fails the AEAD open: inconclusive (an AEAD
    // rejection is not treated as a clean absence that licenses deletion).
    let ap = attest_path(&path);
    let mut bytes = std::fs::read(&ap).unwrap();
    if let Some(b) = bytes.last_mut() {
        *b ^= 0xFF;
    }
    std::fs::write(&ap, &bytes).unwrap();
    assert!(
        matches!(
            attests(&*fs, &path, Some(&enc), 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "a tampered encrypted attestation is inconclusive, not attesting",
    );

    // A sidecar sealed under a DIFFERENT key fails the open too: inconclusive.
    write(&*fs, &path, Some(&enc), 7, ck(100), ck(200)).unwrap();
    let wrong = Aes256GcmProvider::new(&[9u8; 32]);
    assert!(
        matches!(
            attests(&*fs, &path, Some(&wrong), 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "an attestation sealed under a different key is inconclusive",
    );
}
