// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The tree's compression dictionaries on disk.
//!
//! A table records the id of the dictionary it was compressed with, and that id
//! is all it records. This module is what turns the id back into bytes across a
//! reopen: one file per dictionary under [`DICTS_FOLDER`], named by the id.
//!
//! ## Why the name is the checksum
//!
//! A dictionary id is the truncated xxh3 of its own bytes, so re-hashing the
//! file and comparing against its name is a full integrity check: a bit flip
//! changes the hash and the file stops answering to the name it is filed under.
//! That is why no separate digest is stored. Detecting corruption matters more
//! here than elsewhere in the tree, because a silently altered dictionary does
//! not fail a read, it decompresses every block written against it into
//! plausible-looking garbage.
//!
//! ## Encrypted trees
//!
//! A dictionary keeps literal stretches of the records it was trained on, so on
//! a tree with an encryption provider the file is sealed with that provider,
//! the same way the manifest is. The name check runs over the decrypted bytes.
//! A tree is encrypted throughout or not at all, so the provider alone says
//! which form a file is in.

use crate::compression::{ZstdDictionaries, ZstdDictionary};
use crate::encryption::EncryptionProvider;
use crate::file::{DICT_TMP_SUFFIX, DictDirEntry, DictId};
use crate::fs::{Fs, FsFile, FsOpenOptions, SyncMode};
#[cfg(not(feature = "std"))]
use crate::io::{Read, Write};
use crate::path::{Path, PathBuf};
use alloc::sync::Arc;
use alloc::vec::Vec;
#[cfg(feature = "std")]
use std::io::{Read, Write};

/// The file `id` is stored under.
#[must_use]
fn path_of(folder: &Path, id: DictId) -> PathBuf {
    folder.join(id.to_string())
}

/// Writes `dict` into the tree's dictionary folder, creating the folder if this
/// is the first one, sealed with `encryption` when the tree has a provider.
///
/// Published by an atomic rename, so a crash mid-write leaves a `.tmp` the
/// sweep disposes of rather than a half-written dictionary under a live name.
/// Writing an id the folder already holds is a no-op ONLY when the bytes match.
/// The id is the truncated xxh3 of the content, so that is the overwhelmingly
/// common case; a DIFFERENT dictionary under the same id is a collision of the
/// truncation, and is refused.
///
/// # Errors
///
/// [`crate::Error::ZstdDictMismatch`] when the folder already holds a different
/// dictionary under this id. Accepting it silently would be the worst outcome
/// the store can produce: the caller's new dictionary would be selected for
/// WRITES while every read resolved the id back to the old bytes, so new blocks
/// would decompress into plausible garbage. Otherwise propagates the provider's
/// encryption failure and the create / write / sync / rename failures of the
/// backend.
pub fn write(
    fs: &dyn Fs,
    folder: &Path,
    dict: &ZstdDictionary,
    encryption: Option<&dyn EncryptionProvider>,
    sync_mode: SyncMode,
) -> crate::Result<()> {
    let final_path = path_of(folder, dict.id());
    if fs.exists(&final_path)? {
        // Same id: prove it is the same DICTIONARY before treating the write as
        // done. `read_one` re-hashes, so a corrupt file is caught here too.
        let held = read_one(fs, folder, dict.id(), encryption)?;
        if held.raw() != dict.raw() {
            return Err(crate::Error::ZstdDictMismatch {
                expected: dict.id(),
                got: Some(held.id()),
            });
        }
        // Sync even though the bytes were already there. The file may be a
        // PREVIOUS attempt that died between its rename and its syncs, leaving
        // the entries not yet durable; returning without syncing would let the
        // caller durably register the id on top of a file a power loss can
        // still lose.
        return sync_entries(fs, folder, sync_mode);
    }

    if !fs.exists(folder)? {
        fs.create_dir_all(folder)?;
    }

    // Sealed before the temp file exists, so a provider failure leaves nothing
    // behind to clean up.
    let sealed = encryption.map(|e| e.encrypt(dict.raw())).transpose()?;
    let bytes = match &sealed {
        Some(ciphertext) => ciphertext.as_slice(),
        None => dict.raw(),
    };

    let tmp_path = folder.join(format!("{}{DICT_TMP_SUFFIX}", dict.id()));
    // `create(true)` rather than `create_new(true)`: a `.tmp` left by a crashed
    // registration is disposable by definition (it is referenced by no version),
    // so overwriting it is the recovery, not a hazard.
    let mut file = fs.open(
        &tmp_path,
        &FsOpenOptions::new().write(true).create(true).truncate(true),
    )?;
    let written = file
        .write_all(bytes)
        .map_err(crate::io::Error::from)
        .and_then(|()| file.flush().map_err(crate::io::Error::from))
        .and_then(|()| FsFile::sync_all_with(&*file, sync_mode));
    drop(file);
    if let Err(e) = written {
        let _ = fs.remove_file(&tmp_path);
        return Err(e.into());
    }

    if let Err(e) = fs.rename(&tmp_path, &final_path) {
        let _ = fs.remove_file(&tmp_path);
        return Err(e.into());
    }
    sync_entries(fs, folder, sync_mode)
}

/// Makes the file's entry in `dicts/` durable, and the entry of `dicts/` in the
/// tree root.
///
/// Both, on every write, whether or not this call created the folder: an
/// earlier attempt may have created it and died before its own syncs, and a
/// retry that took the existing folder as durable would let the caller register
/// the id over a folder a power loss can still take away. Syncing an already
/// durable directory costs one call; this runs once per registration.
fn sync_entries(fs: &dyn Fs, folder: &Path, sync_mode: SyncMode) -> crate::Result<()> {
    fs.sync_directory_with(folder, sync_mode)?;
    if let Some(parent) = folder.parent() {
        fs.sync_directory_with(parent, sync_mode)?;
    }
    Ok(())
}

/// Reads the dictionary stored under `id`, opening the seal with `encryption`
/// when the tree has a provider.
///
/// # Errors
///
/// [`crate::Error::ZstdDictMismatch`] when the bytes on disk do not hash to the
/// id they are filed under, which is this store's integrity check.
/// [`crate::Error::Decrypt`] when the file does not open under `encryption`.
/// Otherwise propagates the open / read failures of the backend, including
/// `NotFound` when the tree does not hold that dictionary.
pub fn read_one(
    fs: &dyn Fs,
    folder: &Path,
    id: DictId,
    encryption: Option<&dyn EncryptionProvider>,
) -> crate::Result<ZstdDictionary> {
    let mut file = fs.open(&path_of(folder, id), &FsOpenOptions::new().read(true))?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)?;
    if let Some(e) = encryption {
        raw = e.decrypt_vec(raw)?;
    }

    let dict = ZstdDictionary::new(&raw);
    if dict.id() != id {
        // The name IS the digest, so a mismatch is corruption of the bytes (or
        // a file placed under a name it does not own), never a stale name.
        return Err(crate::Error::ZstdDictMismatch {
            expected: id,
            got: Some(dict.id()),
        });
    }
    Ok(dict)
}

/// Loads every dictionary the folder holds.
///
/// The READ side scans rather than following the version's id list, so a
/// dictionary whose file landed before the version edit that registers it (a
/// crash in between) still resolves, and an open never fails on an id the list
/// and the folder disagree about. The list is what says which files are still
/// OWED to a reader, which is a collection question, not a read one.
///
/// # Errors
///
/// Propagates the directory read failure, and [`read_one`]'s integrity check:
/// a corrupt dictionary fails the open rather than silently dropping out of the
/// set, which would turn into "unknown dictionary id" on the first table that
/// needs it.
pub fn read_all(
    fs: &dyn Fs,
    folder: &Path,
    encryption: Option<&dyn EncryptionProvider>,
) -> crate::Result<ZstdDictionaries> {
    if !fs.exists(folder)? {
        return Ok(ZstdDictionaries::new());
    }
    let mut set = ZstdDictionaries::new();
    for dirent in fs.read_dir(folder)? {
        if let DictDirEntry::Dict(id) = DictDirEntry::classify(&dirent.file_name) {
            set = set.with(Arc::new(read_one(fs, folder, id, encryption)?));
        }
    }
    Ok(set)
}

/// Lists the ids the folder holds, without reading a single dictionary.
///
/// The collector's view: it decides what to UNLINK, and for that the names are
/// the whole answer. Reading the bytes would make it hash every dictionary in
/// the tree, and would fail the pass on a corrupt one it may be about to remove.
///
/// # Errors
///
/// Propagates the directory read failure.
pub fn list_ids(fs: &dyn Fs, folder: &Path) -> crate::Result<Vec<DictId>> {
    if !fs.exists(folder)? {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for dirent in fs.read_dir(folder)? {
        if let DictDirEntry::Dict(id) = DictDirEntry::classify(&dirent.file_name) {
            ids.push(id);
        }
    }
    Ok(ids)
}

/// Removes the dictionary stored under `id`, treating an already-absent file as
/// success.
///
/// # Errors
///
/// Propagates the backend's removal failures other than `NotFound`.
pub fn remove(fs: &dyn Fs, folder: &Path, id: DictId, sync_mode: SyncMode) -> crate::Result<()> {
    match fs.remove_file(&path_of(folder, id)) {
        Ok(()) => {}
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    fs.sync_directory_with(folder, sync_mode)?;
    Ok(())
}

/// Deletes every unpublished `.tmp` in the dictionary folder.
///
/// A `.tmp` survives only a crash between write and rename, and is referenced
/// by no version, so it is always disposable. Foreign names are never touched:
/// the folder may hold an operator's files and they are not engine state.
///
/// # Errors
///
/// Propagates the directory read failure. A file that cannot be removed is
/// logged and skipped rather than failing the open: a leftover temp costs
/// space, not correctness.
pub fn sweep_temps(fs: &dyn Fs, folder: &Path) -> crate::Result<()> {
    if !fs.exists(folder)? {
        return Ok(());
    }
    for dirent in fs.read_dir(folder)? {
        let name = dirent.file_name;
        if let DictDirEntry::Tmp(id) = DictDirEntry::classify(&name) {
            let path = folder.join(&name);
            if let Err(e) = fs.remove_file(&path) {
                log::warn!("Could not remove unpublished dictionary temp {id}: {e:?}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;
