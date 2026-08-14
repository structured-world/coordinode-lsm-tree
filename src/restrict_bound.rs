// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Sidecar recording a tight-space-punched SST's restriction lower bound.
//!
//! Tight-space compaction reclaims a table's consumed prefix data blocks in
//! place (hole-punched, reading back as zeros) and restricts the surviving view
//! to keys `>= bound`. The bound lives in the manifest, but manifest repair
//! rebuilds the manifest from the on-disk files alone and so cannot recover it.
//!
//! This sidecar makes the bound recoverable WITHOUT mutating the SST. Writing the
//! bound into the SST's own meta would change the SST bytes while the manifest
//! still references it by a whole-file checksum, opening a crash window where the
//! two disagree (a scrub then reports false corruption and a checkpoint can
//! hard-link the modified bytes under the stale digest). A separate sidecar
//! avoids that entirely: the SST is never touched, so its manifest checksum stays
//! valid in every crash window, and the sidecar is published through a single
//! atomic `temp + fsync + rename` — never two mirrors that can diverge.
//!
//! # Threat model
//!
//! For an ENCRYPTED table the payload is AEAD-sealed with the table's provider,
//! so an offline attacker without the key cannot forge a bound `decrypt` accepts.
//! For an UNENCRYPTED table the payload carries an XXH3-128 checksum that detects
//! corruption; forging a *valid* sidecar needs the same directory write access
//! that could tamper with the SST or manifest directly. Either way, repair does
//! not trust the bound on the sidecar alone: it independently verifies that the
//! prefix below the bound is ACTUALLY hole-punched (reads as zeros) before
//! applying the restriction, so a bound with no physical punch behind it — a
//! forgery, or a stale sidecar on an unpunched file — is rejected.

use crate::encryption::EncryptionProvider;
use crate::fs::{Fs, FsOpenOptions, SyncMode};
use alloc::vec::Vec;
use std::path::{Path, PathBuf};

/// Fixed header length: `table_id` (u64) + `bound_len` (u32).
const HEADER_LEN: usize = 8 + 4;
/// Trailing integrity checksum length: XXH3-128.
const CHECKSUM_LEN: usize = 16;

/// The sidecar sits next to the SST with a `.restrict-bound` suffix APPENDED (not
/// a replaced extension), so it never collides with the SST's own numeric name.
#[must_use]
pub fn sidecar_path(table_path: &Path) -> PathBuf {
    let mut name = table_path.as_os_str().to_os_string();
    name.push(".restrict-bound");
    PathBuf::from(name)
}

/// The staging path the sidecar is written + synced to before its atomic rename
/// onto [`sidecar_path`]. A crash between the temp write and the rename leaves
/// this `{id}.restrict-bound.tmp` behind; tree recovery sweeps it (it is
/// disposable — either the live sidecar it would have replaced still exists, or
/// the compaction is simply re-run).
#[must_use]
fn sidecar_tmp_path(table_path: &Path) -> PathBuf {
    let mut name = sidecar_path(table_path).into_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

/// Serializes `(table_id, bound)` with a trailing XXH3-128 checksum over the
/// header + bound (the checksum detects accidental corruption; an encrypted
/// payload is additionally AEAD-authenticated by the provider).
fn serialize(table_id: u64, bound: &[u8]) -> Vec<u8> {
    // `bound` is a user key; its length is bounded by the same limits the writer
    // enforces on stored keys, well within u32.
    let bound_len = u32::try_from(bound.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(HEADER_LEN + bound.len() + CHECKSUM_LEN);
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&bound_len.to_le_bytes());
    out.extend_from_slice(bound);
    let checksum = crate::hash::hash128(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

/// Parses a serialized payload, validating the checksum and length framing.
/// Returns `(table_id, bound)` or `None` on any malformation.
fn deserialize(plain: &[u8]) -> Option<(u64, Vec<u8>)> {
    if plain.len() < HEADER_LEN + CHECKSUM_LEN {
        return None;
    }
    let (body, checksum_bytes) = plain.split_at(plain.len() - CHECKSUM_LEN);
    let stored = u128::from_le_bytes(checksum_bytes.try_into().ok()?);
    if crate::hash::hash128(body) != stored {
        return None;
    }
    let table_id = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
    let bound_len = u32::from_le_bytes(body.get(8..12)?.try_into().ok()?) as usize;
    let bound = body.get(HEADER_LEN..)?;
    if bound.len() != bound_len {
        return None;
    }
    Some((table_id, bound.to_vec()))
}

/// Publishes the restriction bound for `table_id` next to `table_path`.
///
/// Written through an atomic synced `temp + rename`, and MUST be called (and
/// returned) BEFORE the prefix is hole-punched, so a crash leaves the sidecar
/// either fully present or absent — never partial — beside an SST whose bytes were
/// never touched.
///
/// # Errors
///
/// Propagates encryption or filesystem failures from the atomic write.
pub fn write(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
    table_id: u64,
    bound: &[u8],
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let plain = serialize(table_id, bound);
    let content = match encryption {
        Some(enc) => enc.encrypt(&plain)?,
        None => plain,
    };
    // Write to a sidecar-specific `.restrict-bound.tmp` and atomically rename it
    // onto the live path. A dedicated temp suffix (not a generic `.tmp_*`) lets
    // tree recovery recognize and sweep an abandoned temp in the tables folder by
    // name, instead of failing to parse it as a table id.
    let path = sidecar_path(table_path);
    let tmp = sidecar_tmp_path(table_path);
    let publish = (|| -> crate::Result<()> {
        let mut file = fs.open(
            &tmp,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        file.write_all(&content)?;
        file.flush()?;
        crate::fs::FsFile::sync_all_with(&*file, sync_mode)?;
        drop(file);
        fs.rename(&tmp, &path)?;
        if let Some(parent) = path.parent() {
            fs.sync_directory_with(parent, sync_mode)?;
        }
        Ok(())
    })();
    if publish.is_err() {
        let _ = fs.remove_file(&tmp);
    }
    publish
}

/// Outcome of reading the sidecar.
///
/// A genuine I/O failure (open other than not-found, or a read failure) is
/// returned as `Err` from [`read`] so the caller can classify it (transient →
/// retry, persistent → salvage); these variants cover the conclusive on-disk
/// states.
pub enum SidecarRead {
    /// Decoded and checksum-validated `(table_id, bound)`.
    Present(u64, Vec<u8>),
    /// Conclusively absent (the file does not exist): the SST is unrestricted.
    Missing,
    /// The sidecar was read but its payload is malformed — a failed checksum,
    /// bad length framing, an AEAD rejection, or an over-long file. PERSISTENT
    /// (a retry reads the same bytes): the bound is unrecoverable, so a caller
    /// facing a genuinely punched SST must fail closed rather than guess.
    Corrupt,
}

/// Reads and validates the restriction sidecar beside `table_path`.
///
/// # Errors
///
/// Returns the underlying I/O error when the sidecar exists but its open / read
/// fails (not a not-found, which is [`SidecarRead::Missing`]). The caller
/// classifies it — transient (retry) vs persistent (salvage). A payload that
/// reads but does not validate is [`SidecarRead::Corrupt`], not an error.
pub fn read(
    fs: &dyn Fs,
    table_path: &Path,
    encryption: Option<&dyn EncryptionProvider>,
) -> crate::Result<SidecarRead> {
    let path = sidecar_path(table_path);
    let mut file = match fs.open(&path, &FsOpenOptions::new().read(true)) {
        Ok(file) => file,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(SidecarRead::Missing),
        Err(e) => return Err(crate::Error::from(e)),
    };
    // Bound the read: a valid sidecar is header + bound + checksum (plaintext) or
    // that plus the provider's AEAD overhead. A user key is at most `u16::MAX`
    // bytes (the writer's key-size limit); reject anything larger BEFORE reading,
    // so a corrupt / attacker-padded sidecar cannot force a huge allocation.
    let max_len = HEADER_LEN as u64
        + u64::from(u16::MAX)
        + CHECKSUM_LEN as u64
        + u64::from(encryption.map_or(0, EncryptionProvider::max_overhead));
    if file.metadata()?.len > max_len {
        return Ok(SidecarRead::Corrupt);
    }
    let mut content = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut content).map_err(crate::Error::from)?;
    let plain = match encryption {
        Some(enc) => match enc.decrypt(&content) {
            Ok(plain) => plain,
            Err(_) => return Ok(SidecarRead::Corrupt),
        },
        None => content,
    };
    match deserialize(&plain) {
        Some((table_id, bound)) => Ok(SidecarRead::Present(table_id, bound)),
        None => Ok(SidecarRead::Corrupt),
    }
}

/// Removes the sidecar (best-effort), syncing the parent so the removal is durable.
///
/// Called when a restricted table is compacted away / deleted, so a stale sidecar
/// does not linger to restrict an unrelated later file that reuses the id.
pub fn remove(fs: &dyn Fs, table_path: &Path, sync_mode: SyncMode) {
    let path = sidecar_path(table_path);
    if fs.remove_file(&path).is_ok()
        && let Some(parent) = path.parent()
    {
        let _ = fs.sync_directory_with(parent, sync_mode);
    }
}

/// True when a `.restrict-bound` sidecar exists beside `table_path`.
///
/// # Errors
///
/// Propagates the underlying [`Fs::exists`] error.
pub fn exists(fs: &dyn Fs, table_path: &Path) -> crate::io::Result<bool> {
    fs.exists(&sidecar_path(table_path))
}

#[cfg(test)]
mod tests;
