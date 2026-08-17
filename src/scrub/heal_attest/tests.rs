#![expect(clippy::unwrap_used, reason = "test asserts on known-present values")]

use super::{AttestResult, attest_path, attests, attests_post, remove, write, write_in_progress};
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
fn attests_returns_absent_without_a_sidecar() {
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

    // A valid completed marker followed by a large garbage tail. This is
    // FAIL-FIRST for the pre-read bound: `deserialize` reads its fields with
    // `get(1..9)` / `get(9..25)` / `get(25..41)`, which accept ANY buffer of at
    // least the record length and ignore trailing bytes, so without the bound
    // `read_sidecar` slurps the whole (attacker-controlled) tail, `deserialize`
    // accepts the leading record, and the padded file is trusted as a marker.
    // The tail is sized far past `ATTEST_LEN + max overhead` to stand in for the
    // memory-exhaustion vector the bound closes.
    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    let ap = attest_path(&path);
    let mut bytes = std::fs::read(&ap).unwrap();
    bytes.extend(std::iter::repeat_n(0u8, 1 << 20));
    std::fs::write(&ap, &bytes).unwrap();

    // An oversized sidecar is INCONCLUSIVE (rejected before the read), never a
    // trusted marker: a valid plaintext record is exactly the fixed length, so
    // extra bytes mean the file is not a marker this build wrote.
    assert!(
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "an oversized attestation is inconclusive, not attesting",
    );
}

#[test]
fn attests_is_inconclusive_when_the_sidecar_metadata_read_fails() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // Fault the sidecar's metadata probe (the size bound the read-side enforces):
    // a metadata failure must be fail-closed INCONCLUSIVE, never a clean read
    // that could authorize deleting a possibly-valid marker.
    injector.arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(ErrorKind::Other)).on_path(".heal-attest"),
    );
    assert!(
        matches!(
            attests(&*fs, &path, None, 7, ck(200), ck(100)),
            AttestResult::Inconclusive
        ),
        "a metadata probe failure is inconclusive, not attesting",
    );
}

/// `attests_post` matches on the recorded POST digest (and id) alone, ignoring the
/// recorded PRE: a marker written with any pre attests as long as the post equals
/// the file's current digest.
#[test]
fn attests_post_attests_on_a_matching_post_regardless_of_pre() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Written with pre=100; `attests_post` never looks at pre.
    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    assert!(
        matches!(
            attests_post(&*fs, &path, None, 7, ck(200)),
            AttestResult::Attests
        ),
        "a matching post attests independent of the recorded pre",
    );
}

/// A present marker whose post (or id) does not match, and an absent marker, are
/// both conclusively non-attesting: `Absent`, safe to clear.
#[test]
fn attests_post_returns_absent_on_a_nonmatching_post_or_id() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    // Wrong post: the file is not the attested healed version.
    assert!(matches!(
        attests_post(&*fs, &path, None, 7, ck(999)),
        AttestResult::Absent
    ));
    // Wrong id: the marker belongs to another table identity.
    assert!(matches!(
        attests_post(&*fs, &path, None, 8, ck(200)),
        AttestResult::Absent
    ));
    // No marker at all.
    remove(&*fs, &path);
    assert!(matches!(
        attests_post(&*fs, &path, None, 7, ck(200)),
        AttestResult::Absent
    ));
}

/// A metadata-probe failure on the marker is fail-closed `Inconclusive`: the
/// caller must preserve the marker and retry, never mistake it for a clean
/// non-attesting read.
#[test]
fn attests_post_is_inconclusive_when_the_metadata_read_fails() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("7");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    write(&*fs, &path, None, 7, ck(100), ck(200)).unwrap();
    injector.arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(ErrorKind::Other)).on_path(".heal-attest"),
    );
    assert!(
        matches!(
            attests_post(&*fs, &path, None, 7, ck(200)),
            AttestResult::Inconclusive
        ),
        "a metadata probe failure is inconclusive, not non-attesting",
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
