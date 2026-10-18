// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The durable copy of a repair's report, kept beside the tree's snapshots
//! from before the rebuilt snapshot is installed until the report is handed
//! to the caller.
//!
//! The report is the caller's only account of what the repair lost, and so
//! of the WAL replay it owes. When the install's outcome is unknown (its
//! directory sync and the withdrawal's both failed), the repair ends in an
//! error, yet a power loss may still keep the rebuilt snapshot; the next open
//! then finds a healthy tree and runs no repair. The copy, named after the
//! snapshot it describes, outlives that: [`Config::open_or_repair`] hands it
//! out once that snapshot is the installed one, and an open sweeps a copy
//! whose snapshot never was.
//!
//! The copy describes the tree as the commit leaves it: a repair that stops
//! there has run none of its post-commit cleanup. It is sealed with the
//! tree's encryption provider, since its key ranges are the tree's keys.

use super::RepairReport;
use crate::config::Config;
use crate::file::{REPAIR_REPORT_PREFIX, repair_report_name};
use crate::fs::FsOpenOptions;
use crate::{Slice, UserKey};
use std::path::{Path, PathBuf};

/// Leading bytes of a copy, so a file of another kind under the name is
/// refused rather than misread.
const MAGIC: &[u8; 8] = b"LSMRPRT1";

/// Length of the trailing XXH3-64 over everything before it.
const CHECKSUM_LEN: usize = 8;

/// The copy of the report on rebuilt snapshot `id`.
pub(super) fn path(folder: &Path, id: u64) -> PathBuf {
    folder.join(format!("{REPAIR_REPORT_PREFIX}{id}"))
}

/// Writes the copy of `report` for the rebuilt snapshot `id`: under a
/// temporary name, synced, renamed onto its name, and the folder synced, so
/// it is durable before the snapshot it describes can be.
///
/// # Errors
///
/// Propagates the encryption or filesystem failure; nothing is published then.
pub(super) fn write(config: &Config, id: u64, report: &RepairReport) -> crate::Result<()> {
    let plain = encode(report);
    let content = match &config.encryption {
        Some(enc) => enc.encrypt(&plain)?,
        None => plain,
    };
    let path = path(&config.path, id);
    let mut temporary = path.clone().into_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    let fs = &*config.fs;
    let publish = (|| -> crate::Result<()> {
        let mut file = fs.open(
            &temporary,
            &FsOpenOptions::new().write(true).create(true).truncate(true),
        )?;
        file.write_all(&content)?;
        file.flush()?;
        crate::fs::FsFile::sync_all_with(&*file, config.sync_mode)?;
        drop(file);
        fs.rename(&temporary, &path)?;
        fs.sync_directory_with(&config.path, config.sync_mode)?;
        Ok(())
    })();
    if publish.is_err()
        && let Err(e) = fs.remove_file(&temporary)
        && e.kind() != crate::io::ErrorKind::NotFound
    {
        log::warn!(
            "repair: could not remove {} ({e}); the next open sweeps it",
            temporary.display(),
        );
    }
    publish
}

/// The copies in the tree folder (only the one on snapshot `only`, when
/// given), read back: the report each holds, or, for one that does not
/// decode, a report that leaves the loss unscoped. More than one are folded
/// into one.
///
/// # Errors
///
/// Propagates a failure to list the folder or read a copy.
pub(super) fn read_all(config: &Config, only: Option<u64>) -> crate::Result<Option<RepairReport>> {
    let mut found: Option<RepairReport> = None;
    for entry in config.fs.read_dir(&config.path)? {
        let Some((id, false)) = repair_report_name(&entry.file_name) else {
            continue;
        };
        if entry.is_dir || only.is_some_and(|only| only != id) {
            continue;
        }
        let report = read(config, &entry.path)?;
        match &mut found {
            Some(into) => super::fold_report(into, report),
            None => found = Some(report),
        }
    }
    Ok(found)
}

/// Reads the copy at `path`.
fn read(config: &Config, path: &Path) -> crate::Result<RepairReport> {
    let file = config.fs.open(path, &FsOpenOptions::new().read(true))?;
    let len = usize::try_from(file.metadata()?.len)
        .map_err(|_| crate::Error::InvalidHeader("repair report copy length"))?;
    let content = crate::file::read_exact(&*file, 0, len)?;
    let plain = match &config.encryption {
        Some(enc) => match enc.decrypt(&content) {
            Ok(plain) => Some(plain),
            Err(e) => {
                log::error!(
                    "repair: the report copy {} does not decrypt: {e}",
                    path.display()
                );
                None
            }
        },
        None => Some(content.to_vec()),
    };
    Ok(plain
        .as_deref()
        .and_then(decode)
        .unwrap_or_else(|| unreadable_copy(path)))
}

/// What a copy that does not decode still says: a repair lost something it
/// can no longer scope. The copy's own path stands for the loss, which makes
/// [`RepairReport::wal_replay_scope`] answer
/// [`FullHistory`](super::WalReplayScope::FullHistory).
fn unreadable_copy(path: &Path) -> RepairReport {
    log::error!(
        "repair: the report copy {} is damaged; its losses are unscoped",
        path.display(),
    );
    RepairReport {
        recovered: 0,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: Vec::new(),
        unknowable_losses: vec![path.to_path_buf()],
        blob_files_salvaged: Vec::new(),
        damaged_dictionaries: Vec::new(),
        method: super::REPAIR_METHOD,
        warnings: vec![super::WARN_REPORT_COPY_DAMAGED],
    }
}

/// How the removal of the copies ended when it did not end well.
pub(super) enum Retire {
    /// A copy is still there, so it still carries the report: an open hands
    /// it out, and handing it out now as well would hand it out twice.
    Kept(crate::Error),
    /// Every copy is gone, but the folder sync that makes that durable
    /// failed: hand the report out now, since a copy may not come back.
    Unsynced(crate::Error),
}

/// Removes every copy and its temporary, then syncs the folder once.
pub(super) fn retire(config: &Config) -> Result<(), Retire> {
    let entries = config
        .fs
        .read_dir(&config.path)
        .map_err(|e| Retire::Kept(e.into()))?;
    let mut removed = false;
    for entry in entries {
        if entry.is_dir || repair_report_name(&entry.file_name).is_none() {
            continue;
        }
        match config.fs.remove_file(&entry.path) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Retire::Kept(e.into())),
        }
    }
    if removed {
        config
            .fs
            .sync_directory_with(&config.path, config.sync_mode)
            .map_err(|e| Retire::Unsynced(e.into()))?;
    }
    Ok(())
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A path as the platform spells it: bytes on Unix, UTF-16 units on Windows.
fn put_path(out: &mut Vec<u8>, path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        put_bytes(out, path.as_os_str().as_bytes());
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let units: Vec<u8> = path
            .as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect();
        put_bytes(out, &units);
    }
    #[cfg(not(any(unix, windows)))]
    put_bytes(out, path.to_string_lossy().as_bytes());
}

fn put_notes(out: &mut Vec<u8>, notes: &[(PathBuf, String)]) {
    put_u64(out, notes.len() as u64);
    for (path, note) in notes {
        put_path(out, path);
        put_bytes(out, note.as_bytes());
    }
}

fn encode(report: &RepairReport) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    put_u64(&mut out, report.recovered as u64);
    put_u64(&mut out, report.salvaged as u64);
    put_notes(&mut out, &report.unreadable_files);
    put_notes(&mut out, &report.excluded_files);
    put_u64(&mut out, report.lost_coverage.len() as u64);
    for (path, lo, hi, bound) in &report.lost_coverage {
        put_path(&mut out, path);
        put_bytes(&mut out, lo);
        put_bytes(&mut out, hi);
        match bound {
            Some(seqno) => {
                out.push(1);
                put_u64(&mut out, *seqno);
            }
            None => out.push(0),
        }
    }
    put_u64(&mut out, report.unknowable_losses.len() as u64);
    for path in &report.unknowable_losses {
        put_path(&mut out, path);
    }
    put_notes(&mut out, &report.blob_files_salvaged);
    put_notes(&mut out, &report.damaged_dictionaries);
    let warnings: Vec<u8> = report
        .warnings
        .iter()
        .filter_map(|warning| {
            let index = super::WARNINGS.iter().position(|known| known == warning);
            debug_assert!(index.is_some(), "a repair warning missing from WARNINGS");
            index.and_then(|i| u8::try_from(i).ok())
        })
        .collect();
    put_bytes(&mut out, &warnings);
    let checksum = crate::hash::hash64(&out);
    put_u64(&mut out, checksum);
    out
}

/// A cursor over a copy's bytes; every read is bounds-checked.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(n)?;
        self.0 = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1)?.first().copied()
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// A count of entries each at least `min` bytes long: one the remaining
    /// bytes cannot hold is damage, refused before anything is allocated.
    fn count(&mut self, min: usize) -> Option<usize> {
        let count = usize::try_from(self.u64()?).ok()?;
        (count <= self.0.len() / min).then_some(count)
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u64()?).ok()?;
        self.take(len)
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?.to_vec()).ok()
    }

    fn path(&mut self) -> Option<PathBuf> {
        let bytes = self.bytes()?;
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Some(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt;
            if bytes.len() % 2 != 0 {
                return None;
            }
            let units: Vec<u16> = bytes
                .chunks_exact(2)
                .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
                .collect();
            Some(PathBuf::from(std::ffi::OsString::from_wide(&units)))
        }
        #[cfg(not(any(unix, windows)))]
        {
            Some(PathBuf::from(String::from_utf8(bytes.to_vec()).ok()?))
        }
    }

    fn key(&mut self) -> Option<UserKey> {
        Some(Slice::from(self.bytes()?))
    }

    fn notes(&mut self) -> Option<Vec<(PathBuf, String)>> {
        let count = self.count(16)?;
        let mut notes = Vec::with_capacity(count);
        for _ in 0..count {
            notes.push((self.path()?, self.string()?));
        }
        Some(notes)
    }
}

fn decode(plain: &[u8]) -> Option<RepairReport> {
    let (body, checksum) = plain.split_at_checked(plain.len().checked_sub(CHECKSUM_LEN)?)?;
    if crate::hash::hash64(body) != u64::from_le_bytes(checksum.try_into().ok()?) {
        return None;
    }
    let mut r = Reader(body);
    if r.take(MAGIC.len())? != MAGIC {
        return None;
    }
    let recovered = usize::try_from(r.u64()?).ok()?;
    let salvaged = usize::try_from(r.u64()?).ok()?;
    let unreadable_files = r.notes()?;
    let excluded_files = r.notes()?;
    let count = r.count(25)?;
    let mut lost_coverage = Vec::with_capacity(count);
    for _ in 0..count {
        let path = r.path()?;
        let lo = r.key()?;
        let hi = r.key()?;
        let bound = match r.u8()? {
            0 => None,
            1 => Some(r.u64()?),
            _ => return None,
        };
        lost_coverage.push((path, lo, hi, bound));
    }
    let count = r.count(8)?;
    let mut unknowable_losses = Vec::with_capacity(count);
    for _ in 0..count {
        unknowable_losses.push(r.path()?);
    }
    let blob_files_salvaged = r.notes()?;
    let damaged_dictionaries = r.notes()?;
    let warnings = r
        .bytes()?
        .iter()
        .map(|&i| super::WARNINGS.get(usize::from(i)).copied())
        .collect::<Option<Vec<_>>>()?;
    if !r.0.is_empty() {
        return None;
    }
    Some(RepairReport {
        recovered,
        salvaged,
        unreadable: unreadable_files.len(),
        unreadable_files,
        excluded_files,
        lost_coverage,
        unknowable_losses,
        blob_files_salvaged,
        damaged_dictionaries,
        method: super::REPAIR_METHOD,
        warnings,
    })
}

#[cfg(test)]
mod tests;
