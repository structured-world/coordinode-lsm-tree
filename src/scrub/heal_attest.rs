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
        // written new one, never a truncated in-between.
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

/// Reads and opens the sidecar, returning its decoded `(kind, id, pre, post)` or
/// `None` for a missing / unreadable / wrong-length / AEAD-rejected sidecar.
fn read_sidecar(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
) -> Option<(u8, u64, u128, u128)> {
    let path = attest_path(table_path);
    let mut file = fs.open(&path, &FsOpenOptions::new().read(true)).ok()?;
    let mut content = Vec::new();
    file.read_to_end(&mut content).ok()?;
    let plain = match encryption {
        // A tampered or wrong-key sidecar fails the AEAD open: reject it.
        Some(enc) => enc.decrypt(&content).ok()?,
        None => content,
    };
    deserialize(&plain)
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
        Some((kind, id, pre, _post)) => {
            kind == KIND_IN_PROGRESS && id == table_id && pre == manifest.into_u128()
        }
        None => false,
    }
}

/// Writes and syncs a heal attestation next to `table_path`.
///
/// # Errors
///
/// Propagates the AEAD seal error (encrypted tables) and any I/O error from the
/// write / sync. The caller treats a failure as best-effort — it only means a
/// crashed refresh will not be recoverable by the next scrub (the pre-sidecar
/// behavior), never that the heal itself failed.
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
/// Returns `false` for a missing / unreadable sidecar, a wrong length, a
/// `table_id` mismatch, or — for an encrypted table — a payload the AEAD open
/// rejects (tampered or wrong key). A `true` result means the stale manifest
/// digest is safe to reconcile to `current`.
pub(super) fn attests(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    current: Checksum,
    manifest: Checksum,
) -> bool {
    match read_sidecar(fs, table_path, encryption) {
        Some((kind, id, pre, post)) => {
            kind == KIND_COMPLETED
                && id == table_id
                && post == current.into_u128()
                && pre == manifest.into_u128()
        }
        None => false,
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
/// failure grades `false` (no pending heal to reconcile) rather than an error.
pub(crate) fn exists(fs: &dyn Fs, table_path: &Path) -> bool {
    fs.exists(&attest_path(table_path)).unwrap_or(false)
}

#[cfg(test)]
mod tests;
