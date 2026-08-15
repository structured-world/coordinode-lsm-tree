#![expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]

use super::*;
use crate::fs::StdFs;
use test_log::test;

/// A written bound round-trips: `read` returns the same `(table_id, bound)`.
#[test]
fn write_then_read_roundtrips_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    write(&StdFs, &sst, None, 7, b"k00130", SyncMode::Normal).unwrap();
    match read(&StdFs, &sst, None).unwrap() {
        SidecarRead::Present(id, bound) => {
            assert_eq!(id, 7);
            assert_eq!(bound, b"k00130");
        }
        _ => panic!("expected Present"),
    }
}

/// The sidecar carries the table id in its own payload, so `read` returns the
/// STORED id verbatim, not one inferred from the file name. A foreign id (distinct
/// from the SST's numeric name) must round-trip, so repair's `id == table_id`
/// cross-check (which tells a sidecar for THIS table apart from a stale one left
/// behind by a reused id) compares against the true stored id.
#[test]
fn write_then_read_roundtrips_a_foreign_table_id() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    let foreign_id = 0xDEAD_BEEF_u64;
    write(&StdFs, &sst, None, foreign_id, b"k00130", SyncMode::Normal).unwrap();
    match read(&StdFs, &sst, None).unwrap() {
        SidecarRead::Present(id, bound) => {
            assert_eq!(
                id, foreign_id,
                "the stored id round-trips, not the file name"
            );
            assert_eq!(bound, b"k00130");
        }
        _ => panic!("expected Present"),
    }
}

/// An absent sidecar reads as `Missing` (the SST is unrestricted), never an error.
#[test]
fn absent_sidecar_reads_missing() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Missing
    ));
}

/// A corrupted payload (flipped byte in the body) fails the checksum and reads as
/// `Corrupt`, never a wrong `Present` bound.
#[test]
fn corrupted_payload_reads_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    write(&StdFs, &sst, None, 7, b"k00130", SyncMode::Normal).unwrap();

    // Flip a byte in the middle of the sidecar (inside the bound region).
    let path = sidecar_path(&sst);
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Corrupt
    ));
}

/// A truncated sidecar (shorter than header + checksum) reads as `Corrupt`.
#[test]
fn truncated_sidecar_reads_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    write(&StdFs, &sst, None, 7, b"k00130", SyncMode::Normal).unwrap();
    let path = sidecar_path(&sst);
    std::fs::write(&path, b"short").unwrap();
    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Corrupt
    ));
}

/// An OVER-LONG sidecar (larger than a valid header + max-length bound + checksum)
/// reads as `Corrupt` and never forces a large allocation: the read is bounded, so
/// a corrupt / padded file is rejected at the size branch before it is
/// materialized.
#[test]
fn oversized_sidecar_reads_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    write(&StdFs, &sst, None, 7, b"k00130", SyncMode::Normal).unwrap();

    // One byte past the largest a valid plaintext sidecar can be.
    let over = HEADER_LEN + u16::MAX as usize + CHECKSUM_LEN + 1;
    std::fs::write(sidecar_path(&sst), vec![0u8; over]).unwrap();

    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Corrupt
    ));
}

/// `remove` deletes the sidecar; a subsequent read is `Missing`.
#[test]
fn remove_deletes_the_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    write(&StdFs, &sst, None, 7, b"k00130", SyncMode::Normal).unwrap();
    assert!(exists(&StdFs, &sst).unwrap());
    remove(&StdFs, &sst, SyncMode::Normal);
    assert!(!exists(&StdFs, &sst).unwrap());
    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Missing
    ));
}

/// A publish whose atomic RENAME fails must leave NO artifact: the synced temp is
/// removed (best-effort) and a later `read` sees `Missing`, never a half-published
/// sidecar.
#[test]
fn failed_publish_cleans_up_the_temp() {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    let fault = FaultFs::new(StdFs);
    fault.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other)).on_path("restrict-bound"),
    );

    // The final rename that publishes the sidecar fails, so `write` errors...
    assert!(write(&fault, &sst, None, 7, b"k00130", SyncMode::Normal).is_err());
    // ...leaving no temp behind...
    assert!(
        !sidecar_tmp_path(&sst).exists(),
        "the synced temp must be cleaned up after a failed publish",
    );
    // ...and the sidecar reads as absent, never half-published.
    assert!(matches!(
        read(&StdFs, &sst, None).unwrap(),
        SidecarRead::Missing
    ));
}

/// An ENCRYPTED sidecar round-trips through the provider, and a WRONG key fails
/// the AEAD open (`Corrupt`), never a forged bound.
#[cfg(feature = "encryption")]
#[test]
fn encrypted_sidecar_roundtrips_and_rejects_a_wrong_key() {
    use crate::encryption::{Aes256GcmProvider, EncryptionProvider};
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let sst = dir.path().join("7");
    let provider: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&[3u8; 32]));
    write(
        &StdFs,
        &sst,
        Some(provider.as_ref()),
        7,
        b"k00130",
        SyncMode::Normal,
    )
    .unwrap();

    match read(&StdFs, &sst, Some(provider.as_ref())).unwrap() {
        SidecarRead::Present(id, bound) => {
            assert_eq!(id, 7);
            assert_eq!(bound, b"k00130");
        }
        _ => panic!("expected Present with the right key"),
    }

    let wrong: Arc<dyn EncryptionProvider> = Arc::new(Aes256GcmProvider::new(&[9u8; 32]));
    assert!(matches!(
        read(&StdFs, &sst, Some(wrong.as_ref())).unwrap(),
        SidecarRead::Corrupt,
    ));
}
