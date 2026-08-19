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
//! that could tamper with the SST or manifest directly, so it opens no new attack
//! surface. Repair uses the physical punch to grade the bound, not to gate it:
//! when the prefix below a valid bound reads as fully hole-punched the bound is
//! exact; when it does NOT (the crash window between a durable install and the
//! punch that follows it, or a stale sidecar over a never-restricted file) the two
//! are indistinguishable on disk, so recovery follows the resurrection policy. By
//! default it HONORS the bound (restricting, dropping the ambiguous prefix, which
//! never resurrects a superseded row); with `allow_resurrection` it keeps the
//! whole table. See `docs/manifest-recovery.md`.

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
    // A bound is a user key, at most `u16::MAX` bytes. `read` rejects a sidecar
    // whose payload exceeds that limit as corrupt, so reject an oversized bound
    // HERE rather than publishing a sidecar a later repair would read as corrupt,
    // silently dropping the restriction and resurrecting the punched prefix.
    if bound.len() > u16::MAX as usize {
        return Err(crate::Error::InvalidHeader(
            "restriction bound exceeds the maximum key length",
        ));
    }
    let plain = serialize(table_id, bound);
    let content = match encryption {
        Some(enc) => enc.encrypt(&plain)?,
        None => plain,
    };
    publish_raw(fs, table_path, &content, sync_mode)
}

/// Atomically publishes already-encoded sidecar `content` beside `table_path`
/// through the synced `temp + rename` protocol [`write`] uses. Also the
/// re-publish path for raw bytes captured from an existing sidecar (a
/// quarantine restore whose direct sidecar rename failed), which must land
/// verbatim — they are already serialized (and possibly encrypted).
///
/// # Errors
///
/// Propagates filesystem failures from the atomic write; a failed publish
/// leaves no partial sidecar (the temp is removed best-effort).
pub(crate) fn publish_raw(
    fs: &dyn Fs,
    table_path: &Path,
    content: &[u8],
    sync_mode: SyncMode,
) -> crate::Result<()> {
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
        file.write_all(content)?;
        file.flush()?;
        crate::fs::FsFile::sync_all_with(&*file, sync_mode)?;
        drop(file);
        // `std::fs::rename` REPLACES an existing destination FILE on every
        // supported platform (on Windows via MOVEFILE_REPLACE_EXISTING /
        // POSIX-semantics rename; only a destination DIRECTORY fails there),
        // so re-publishing over an earlier slice's sidecar is atomic
        // everywhere — no remove-then-rename window.
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

/// The largest on-disk size a VALID sidecar can have: header + bound + checksum
/// (plaintext), plus the provider's AEAD overhead when encrypted. A user key is
/// at most `u16::MAX` bytes (the writer's key-size limit). Everything that
/// reads sidecar bytes — [`read`] and the repair restore's raw rescue capture —
/// rejects anything larger BEFORE allocating, so a corrupt / attacker-padded
/// sidecar cannot force a huge allocation.
pub(crate) fn max_encoded_len(encryption: Option<&dyn EncryptionProvider>) -> u64 {
    HEADER_LEN as u64
        + u64::from(u16::MAX)
        + CHECKSUM_LEN as u64
        + u64::from(encryption.map_or(0, EncryptionProvider::max_overhead))
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
    let max_len = max_encoded_len(encryption);
    if file.metadata()?.len > max_len {
        return Ok(SidecarRead::Corrupt);
    }
    let mut content = Vec::new();
    // Cap the reader at the same bound as the metadata probe, so the allocation
    // limit holds even if the file grew after the probe (a concurrent writer, or a
    // backend whose metadata disagrees with its data). Reading `max_len + 1` bytes
    // lets an over-long payload surface as `Corrupt` below.
    std::io::Read::read_to_end(
        &mut std::io::Read::take(&mut file, max_len.saturating_add(1)),
        &mut content,
    )
    .map_err(crate::Error::from)?;
    if content.len() as u64 > max_len {
        return Ok(SidecarRead::Corrupt);
    }
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
