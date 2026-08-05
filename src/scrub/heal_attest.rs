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

/// Serialized payload length: `table_id` (u64) + pre digest (u128) + post digest
/// (u128).
const ATTEST_LEN: usize = 8 + 16 + 16;

/// The attestation sits next to the SST with a `.heal-attest` suffix APPENDED
/// (not a replaced extension), so it never collides with the SST's own name.
fn attest_path(table_path: &Path) -> PathBuf {
    let mut name = table_path.as_os_str().to_os_string();
    name.push(".heal-attest");
    PathBuf::from(name)
}

fn serialize(table_id: u64, pre: u128, post: u128) -> Vec<u8> {
    let mut out = Vec::with_capacity(ATTEST_LEN);
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&pre.to_le_bytes());
    out.extend_from_slice(&post.to_le_bytes());
    out
}

fn deserialize(plain: &[u8]) -> Option<(u64, u128, u128)> {
    let id = u64::from_le_bytes(plain.get(0..8)?.try_into().ok()?);
    let pre = u128::from_le_bytes(plain.get(8..24)?.try_into().ok()?);
    let post = u128::from_le_bytes(plain.get(24..40)?.try_into().ok()?);
    Some((id, pre, post))
}

/// Writes and syncs a heal attestation next to `table_path`.
///
/// # Errors
///
/// Propagates the AEAD seal error (encrypted tables) and any I/O error from the
/// write / sync. The caller treats a failure as best-effort — it only means a
/// crashed refresh will not be recoverable by the next scrub (the pre-sidecar
/// behavior), never that the heal itself failed.
pub(super) fn write(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    pre: Checksum,
    post: Checksum,
) -> crate::Result<()> {
    let plain = serialize(table_id, pre.into_u128(), post.into_u128());
    let content = match encryption {
        Some(enc) => enc.encrypt(&plain)?,
        None => plain,
    };
    let path = attest_path(table_path);
    let mut file = fs.open(
        &path,
        &FsOpenOptions::new().write(true).create(true).truncate(true),
    )?;
    file.write_all(&content)?;
    file.flush()?;
    file.sync_all()?;
    if let Some(parent) = path.parent() {
        fs.sync_directory(parent)?;
    }
    Ok(())
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
    let path = attest_path(table_path);
    let Ok(mut file) = fs.open(&path, &FsOpenOptions::new().read(true)) else {
        return false;
    };
    let mut content = Vec::new();
    if file.read_to_end(&mut content).is_err() {
        return false;
    }
    let plain = match encryption {
        // A tampered or wrong-key sidecar fails the AEAD open: reject it.
        Some(enc) => match enc.decrypt(&content) {
            Ok(plain) => plain,
            Err(_) => return false,
        },
        None => content,
    };
    let Some((id, pre, post)) = deserialize(&plain) else {
        return false;
    };
    id == table_id && post == current.into_u128() && pre == manifest.into_u128()
}

/// Removes the attestation (best-effort) once the reconciliation it authorized
/// has installed a legitimate digest.
pub(super) fn remove(fs: &dyn Fs, table_path: &Path) {
    let _ = fs.remove_file(&attest_path(table_path));
}

#[cfg(test)]
mod tests;
