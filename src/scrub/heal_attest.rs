// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Sidecar heal-attestation lockfile.
//!
//! An in-place ECC heal writes and syncs corrected bytes, then
//! [`refresh_healed_checksum`](super::refresh_healed_checksum) reconciles the
//! manifest digest. If the process crashes (or the refresh fails) BETWEEN the
//! block write-back and the manifest update, the next scrub reads a CLEAN file:
//! nothing is written that pass, so the heal is not attributable, and the
//! fail-closed digest gate would reject the stale manifest digest FOREVER —
//! `verify` / repair keep reporting the healed table as corrupt until a full
//! rewrite installs a legitimate digest.
//!
//! This sidecar bridges that window. Before the reconciliation, the heal writes
//! `(table_id, pre_heal_digest, post_heal_digest)` next to the SST and syncs it;
//! after a successful refresh it is removed. A later scrub trusts it ONLY when
//! the file now hashes to `post_heal_digest` AND the manifest still holds
//! `pre_heal_digest` — i.e. the file is exactly the recorded healed version and
//! nothing else moved the manifest since.
//!
//! # Threat model
//!
//! For an ENCRYPTED table the payload is AEAD-sealed with the table's provider,
//! so an offline attacker without the key cannot forge an attestation that
//! `decrypt` accepts — the security boundary the digest gate defends stays
//! intact. For an UNENCRYPTED table the sidecar is plaintext: forging it needs
//! the same directory write access that could re-stamp the SST (or the manifest)
//! directly, which is outside the on-disk-tamper model, so the plaintext form is
//! sufficient there.

use crate::Checksum;
use crate::encryption::EncryptionProvider;
use crate::fs::{Fs, FsOpenOptions};
use alloc::vec::Vec;
use std::path::{Path, PathBuf};

/// A COMPLETED attestation: the heal finished and its exact `(pre, post)`
/// digests are recorded, so a reconcile only trusts the file that hashes to
/// `post`.
const KIND_COMPLETED: u8 = 0;
/// An IN-PROGRESS marker: a legacy pre-only kind that bound only `pre ==
/// manifest`, not the healed bytes. The heal now records a completed marker with
/// the deterministic post-heal digest UP FRONT instead, so this kind is never
/// written in production; [`attests`] rejects it (it is not [`KIND_COMPLETED`]),
/// which is what stops a bare pre-only marker from authorizing an unrelated
/// forge. Retained only so the tests can forge one and prove it is ignored.
#[cfg(test)]
const KIND_IN_PROGRESS: u8 = 1;

/// Serialized payload length: `kind` (u8) + `table_id` (u64) + pre digest (u128)
/// + post digest (u128).
const ATTEST_LEN: usize = 1 + 8 + 16 + 16;

/// The attestation sits next to the SST with a `.heal-attest` suffix APPENDED
/// (not a replaced extension), so it never collides with the SST's own name.
fn attest_path(table_path: &Path) -> PathBuf {
    let mut name = table_path.as_os_str().to_os_string();
    name.push(".heal-attest");
    PathBuf::from(name)
}

/// The staging path the sidecar is written + synced to before an atomic rename
/// onto [`attest_path`]. A crash between the temp write and the rename leaves
/// this behind; tree recovery sweeps `{id}.heal-attest.tmp` (it is disposable:
/// either the live sidecar it would have replaced still bridges the crash
/// window, or the heal is simply re-run).
fn attest_tmp_path(table_path: &Path) -> PathBuf {
    let mut name = attest_path(table_path).into_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

fn serialize(kind: u8, table_id: u64, pre: u128, post: u128) -> Vec<u8> {
    let mut out = Vec::with_capacity(ATTEST_LEN);
    out.push(kind);
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&pre.to_le_bytes());
    out.extend_from_slice(&post.to_le_bytes());
    out
}

fn deserialize(plain: &[u8]) -> Option<(u8, u64, u128, u128)> {
    let kind = *plain.first()?;
    let id = u64::from_le_bytes(plain.get(1..9)?.try_into().ok()?);
    let pre = u128::from_le_bytes(plain.get(9..25)?.try_into().ok()?);
    let post = u128::from_le_bytes(plain.get(25..41)?.try_into().ok()?);
    Some((kind, id, pre, post))
}

/// Seals `plain` (AEAD for a keyed table, else plaintext) and publishes it as
/// the sidecar next to `table_path` through a synced temp + atomic rename.
///
/// The temp is written, flushed, and fsynced in full, THEN atomically renamed
/// onto the live sidecar path. A mid-write or sync failure therefore leaves the
/// partial bytes in the temp (removed best-effort) and never touches the live
/// sidecar: an already-durable in-progress marker survives a failed completed
/// write, instead of being truncated in place and destroyed, which would leave
/// a healed SST with a stale manifest digest and no valid attestation, forever
/// unreconcilable.
///
/// The temp is ALWAYS fully synced with `sync_all`, regardless of
/// [`Config::sync_mode`](crate::Config): the attestation is a crash-recovery
/// anchor (recovery reconciles a healed table's digest through it), so it must be
/// durable before the rename even when the tree runs at a relaxed sync mode.
fn write_sidecar(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    plain: &[u8],
) -> crate::Result<()> {
    let content = match encryption {
        Some(enc) => enc.encrypt(plain)?,
        None => plain.to_vec(),
    };
    let path = attest_path(table_path);
    let tmp = attest_tmp_path(table_path);
    let publish = (|| -> crate::Result<()> {
        let mut file = fs.open(
            &tmp,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        file.write_all(&content)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        // Atomic replace: the live sidecar is either the old marker or the fully
        // written new one, never a truncated in-between. `std::fs::rename`
        // REPLACES an existing destination FILE on every supported platform
        // (on Windows via replace-existing semantics; only a destination
        // DIRECTORY fails there), so re-publishing over the in-progress marker
        // — the completed marker lands on the same path — works everywhere.
        fs.rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            fs.sync_directory(parent)?;
        }
        Ok(())
    })();
    if publish.is_err() {
        // The rename did not land, so the live sidecar is untouched; drop the
        // partial temp (a crash that skips this is swept by tree recovery).
        let _ = fs.remove_file(&tmp);
    }
    publish
}

/// Outcome of reading the sidecar. Distinguishes a CONCLUSIVELY absent sidecar
/// from one that could not be read: a caller deciding whether to DELETE the
/// marker must not treat an unreadable / undecryptable / malformed sidecar as
/// "absent", because that could be a transiently-unreadable VALID marker whose
/// deletion would strand a healed table under a stale digest forever.
enum SidecarRead {
    /// Decoded `(kind, id, pre, post)`.
    Present(u8, u64, u128, u128),
    /// Conclusively absent (the file does not exist).
    Missing,
    /// Present but not conclusively readable (an open error other than
    /// not-found, a read error, an AEAD rejection, or a malformed payload).
    /// Retryable.
    Inconclusive,
}

/// Reads and opens the sidecar. See [`SidecarRead`] for the missing-vs-
/// inconclusive distinction the removal decision relies on.
fn read_sidecar(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
) -> SidecarRead {
    let path = attest_path(table_path);
    let mut file = match fs.open(&path, &FsOpenOptions::new().read(true)) {
        Ok(file) => file,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return SidecarRead::Missing,
        Err(_) => return SidecarRead::Inconclusive,
    };
    // Bound the read: a valid sidecar is exactly `ATTEST_LEN` bytes (plaintext)
    // or `ATTEST_LEN + max AEAD overhead` (encrypted). Reject anything larger
    // BEFORE reading it, so a corrupt / attacker-replaced sidecar cannot force an
    // allocation of its full (attacker-controlled) length, and a valid record
    // padded with a long garbage tail cannot be laundered past `deserialize`.
    let max_len =
        ATTEST_LEN as u64 + u64::from(encryption.map_or(0, EncryptionProvider::max_overhead));
    match file.metadata() {
        Ok(meta) if meta.len > max_len => return SidecarRead::Inconclusive,
        Ok(_) => {}
        Err(_) => return SidecarRead::Inconclusive,
    }
    let mut content = Vec::new();
    // Cap the reader at the same bound as the metadata probe, so the allocation
    // limit holds even if the file grew after the probe (a concurrent writer, or a
    // backend whose metadata disagrees with its data). Reading `max_len + 1` bytes
    // lets an over-long payload surface as Inconclusive below.
    if std::io::Read::read_to_end(
        &mut std::io::Read::take(&mut file, max_len.saturating_add(1)),
        &mut content,
    )
    .is_err()
    {
        return SidecarRead::Inconclusive;
    }
    if content.len() as u64 > max_len {
        return SidecarRead::Inconclusive;
    }
    let plain = match encryption {
        // A tampered or wrong-key sidecar fails the AEAD open: inconclusive, not
        // absent, so never delete a marker on this basis.
        Some(enc) => match enc.decrypt(&content) {
            Ok(plain) => plain,
            Err(_) => return SidecarRead::Inconclusive,
        },
        None => content,
    };
    match deserialize(&plain) {
        Some((kind, id, pre, post)) => SidecarRead::Present(kind, id, pre, post),
        None => SidecarRead::Inconclusive,
    }
}

/// Whether an attestation attests the current bytes, is absent, or could not be
/// read. See [`attests`].
pub enum AttestResult {
    /// A completed marker binds exactly `(pre == manifest, post == current)`
    /// for this table: the stale manifest digest is safe to reconcile.
    Attests,
    /// No attesting marker: conclusively missing, or present but binding
    /// different values / a non-completed kind. Safe to clear.
    Absent,
    /// The sidecar could not be read conclusively. Retryable: the caller must
    /// NOT delete the marker on this basis (it may be a valid marker).
    Inconclusive,
}

/// Forges a legacy IN-PROGRESS marker (pre-only). Test-only: production never
/// writes this kind — the heal records a completed marker with the post-heal
/// digest up front. The tests use it to prove [`attests`] ignores a bare
/// pre-only marker, so it can never authorize an unrelated forge.
///
/// # Errors
///
/// Propagates the AEAD seal error and any write / sync I/O error.
#[cfg(test)]
pub fn write_in_progress(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    pre: Checksum,
) -> crate::Result<()> {
    let plain = serialize(KIND_IN_PROGRESS, table_id, pre.into_u128(), 0);
    write_sidecar(fs, table_path, encryption, &plain)
}

/// Whether a marker is a legacy IN-PROGRESS (pre-only) attestation. Test-only:
/// production attribution consults [`attests`] alone, which requires
/// [`KIND_COMPLETED`], so a pre-only marker is never trusted.
#[cfg(test)]
pub(super) fn attests_in_progress(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    manifest: Checksum,
) -> bool {
    match read_sidecar(fs, table_path, encryption) {
        SidecarRead::Present(kind, id, pre, _post) => {
            kind == KIND_IN_PROGRESS && id == table_id && pre == manifest.into_u128()
        }
        SidecarRead::Missing | SidecarRead::Inconclusive => false,
    }
}

/// Writes and syncs a heal attestation next to `table_path`.
///
/// # Errors
///
/// Propagates the AEAD seal error (encrypted tables) and any I/O error from the
/// write / sync. The caller must FAIL CLOSED on such an error: the heal /
/// reconciliation must not proceed, `refresh_healed_checksum` returns
/// `ScrubError::ChecksumRefreshFailed` without updating the manifest digest, and
/// the `.heal-attest` sidecar is left in place for the next scrub to retry. A
/// silently-dropped attestation would let a crash mid-refresh strand a stale
/// digest with no marker to reconcile it.
pub fn write(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    pre: Checksum,
    post: Checksum,
) -> crate::Result<()> {
    let plain = serialize(KIND_COMPLETED, table_id, pre.into_u128(), post.into_u128());
    write_sidecar(fs, table_path, encryption, &plain)
}

/// Whether a valid attestation proves the CURRENT file (hashing to `current`)
/// is the healed version recorded against the manifest's `manifest` digest.
///
/// [`AttestResult::Attests`] means the stale manifest digest is safe to
/// reconcile to `current`. [`AttestResult::Absent`] means no marker attests
/// these values (conclusively missing, wrong id, wrong digests, or a
/// non-completed kind), safe to clear. [`AttestResult::Inconclusive`] means
/// the sidecar could not be read (I/O / AEAD / malformed): the caller must keep
/// the marker, since it may be a transiently-unreadable valid one.
pub(super) fn attests(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    current: Checksum,
    manifest: Checksum,
) -> AttestResult {
    match read_sidecar(fs, table_path, encryption) {
        SidecarRead::Present(kind, id, pre, post) => {
            if kind == KIND_COMPLETED
                && id == table_id
                && post == current.into_u128()
                && pre == manifest.into_u128()
            {
                AttestResult::Attests
            } else {
                AttestResult::Absent
            }
        }
        SidecarRead::Missing => AttestResult::Absent,
        SidecarRead::Inconclusive => AttestResult::Inconclusive,
    }
}

/// Whether a COMPLETED marker beside `table_path` already attests `post` as the
/// healed digest for `table_id` (regardless of its recorded `pre`). Lets the
/// heal path recognize that re-healing a not-matched file back to an
/// already-attested `post` is safe — the existing marker still reconciles it —
/// so such a heal need not be skipped as "diverging".
///
/// Tri-state so a TRANSIENT sidecar read (`Inconclusive`) is never collapsed to
/// "does not attest": a caller that skipped the heal on that basis would leave
/// the file diverged from the marker's `post`, and the reconcile that follows
/// would reread the now-readable marker, find it no longer matches the current
/// bytes, and delete it — stranding the table with neither attribution nor
/// marker. On `Inconclusive` the caller must preserve the marker and retry.
///
/// Gated to match its only caller, the Page-ECC in-place heal: without that
/// feature nothing performs a heal, so nothing has a marker to re-attest.
#[cfg(any(feature = "page_ecc", test))]
pub fn attests_post(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    post: Checksum,
) -> AttestResult {
    match read_sidecar(fs, table_path, encryption) {
        SidecarRead::Present(kind, id, _pre, marker_post)
            if kind == KIND_COMPLETED && id == table_id && marker_post == post.into_u128() =>
        {
            AttestResult::Attests
        }
        SidecarRead::Present(..) | SidecarRead::Missing => AttestResult::Absent,
        SidecarRead::Inconclusive => AttestResult::Inconclusive,
    }
}

/// Removes the attestation (best-effort), syncing the parent so the removal is
/// durable — an un-synced unlink can resurrect the sidecar after a power loss,
/// where it would otherwise linger in `tables/` across opens. Called both when a
/// reconciliation it authorized has installed a legitimate digest AND when a
/// marker must be invalidated (a heal that touched no block, or a refused
/// reconciliation), so a stale marker never authorizes an unrelated later
/// mismatch.
pub fn remove(fs: &dyn Fs, table_path: &Path) {
    let path = attest_path(table_path);
    if fs.remove_file(&path).is_ok()
        && let Some(parent) = path.parent()
    {
        let _ = fs.sync_directory(parent);
    }
}

/// Whether a pending attestation sidecar exists for `table_path`. A probe
/// FAILURE propagates as an error (fail-closed): a caller that reconciles
/// pending heals before a checkpoint must abort rather than mistake an
/// unreadable probe for "no pending heal" and snapshot a stale digest.
///
/// # Errors
///
/// Propagates the underlying [`Fs::exists`] error.
pub fn exists(fs: &dyn Fs, table_path: &Path) -> crate::io::Result<bool> {
    fs.exists(&attest_path(table_path))
}

#[cfg(test)]
mod tests;
