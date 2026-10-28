// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The encoding of a repair's report, as the snapshot that owes it holds it.
//!
//! The report is the caller's only account of what the repair lost, and so of
//! the WAL replay it owes. The rebuilt snapshot carries it in its
//! `repair_report` section, from the commit until the caller acknowledges it
//! and a version that no longer owes it is installed: living in the snapshot,
//! it is there exactly when that snapshot is installed, whatever the install's
//! outcome. The snapshot is sealed with the tree's encryption provider, which
//! covers the report's key ranges with it.

use super::RepairReport;
use crate::{Slice, UserKey};
use std::path::{Path, PathBuf};

/// Leading bytes of an encoded report, so bytes of another kind are refused
/// rather than misread.
const MAGIC: &[u8; 8] = b"LSMRPRT1";

/// Length of the trailing XXH3-64 over everything before it.
const CHECKSUM_LEN: usize = 8;

/// The id of the report `bytes` encode, as [`decode_owed`] gives it.
pub(super) fn owed_id(bytes: &[u8]) -> u128 {
    decode(bytes).map_or_else(|| xxhash_rust::xxh3::xxh3_128(bytes), |report| report.id)
}

/// The report `bytes` encode, or, when they do not decode, a report that
/// leaves the loss unscoped: a repair lost something it can no longer say.
/// `tree` stands for the loss, which makes
/// [`RepairReport::wal_replay_scope`] answer
/// [`FullHistory`](super::WalReplayScope::FullHistory).
pub(super) fn decode_owed(bytes: &[u8], tree: &Path) -> RepairReport {
    decode(bytes).unwrap_or_else(|| {
        log::error!(
            "repair: the report owed by {} is damaged; its losses are unscoped",
            tree.display(),
        );
        // The same for every hand-out of these bytes, and distinct from
        // another repair's damaged report, which a caller deduplicating by
        // id must not skip.
        unscoped(tree, xxhash_rust::xxh3::xxh3_128(bytes))
    })
}

/// The report `tree` may owe when its snapshot `v{snapshot_id}`, or the
/// section of it that holds a report, does not read back: a repair may have
/// lost something no one can say any more. Its id is that snapshot's, so
/// every hand-out from it carries one id.
pub(super) fn unreadable_owed(tree: &Path, snapshot_id: u64) -> RepairReport {
    log::error!(
        "repair: whatever report {} owes does not read back; its losses are unscoped",
        tree.display(),
    );
    let mut seed = b"unreadable owed report".to_vec();
    seed.extend_from_slice(&snapshot_id.to_le_bytes());
    unscoped(tree, xxhash_rust::xxh3::xxh3_128(&seed))
}

/// A report that leaves the loss unscoped, under `id`.
fn unscoped(tree: &Path, id: u128) -> RepairReport {
    RepairReport {
        recovered: 0,
        salvaged: 0,
        unreadable: 0,
        unreadable_files: Vec::new(),
        excluded_files: Vec::new(),
        lost_coverage: Vec::new(),
        unknowable_losses: vec![tree.to_path_buf()],
        blob_files_salvaged: Vec::new(),
        damaged_dictionaries: Vec::new(),
        method: super::REPAIR_METHOD,
        warnings: vec![super::WARN_REPORT_COPY_DAMAGED],
        id,
        includes: Vec::new(),
    }
}

/// The id of `report`, rebuilt as version `version_id`: a 128-bit digest of
/// what it says, that version and the time on the tree's `clock`, so two
/// repairs never share one.
pub(super) fn id_for(
    report: &RepairReport,
    version_id: u64,
    clock: Option<&alloc::sync::Arc<dyn crate::time::Clock>>,
) -> u128 {
    let mut seed = encode(report);
    seed.extend_from_slice(&version_id.to_le_bytes());
    seed.extend_from_slice(&crate::time::now_on(clock).as_nanos().to_le_bytes());
    xxhash_rust::xxh3::xxh3_128(&seed)
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

/// The bytes the snapshot that owes `report` holds.
pub(super) fn encode(report: &RepairReport) -> Vec<u8> {
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
    out.extend_from_slice(&report.id.to_le_bytes());
    put_u64(&mut out, report.includes.len() as u64);
    for id in &report.includes {
        out.extend_from_slice(&id.to_le_bytes());
    }
    let checksum = crate::hash::hash64(&out);
    put_u64(&mut out, checksum);
    out
}

/// A cursor over an encoded report; every read is bounds-checked.
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

    fn u128(&mut self) -> Option<u128> {
        Some(u128::from_le_bytes(self.take(16)?.try_into().ok()?))
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
            let (units, odd) = bytes.as_chunks::<2>();
            if !odd.is_empty() {
                return None;
            }
            let units: Vec<u16> = units.iter().map(|&unit| u16::from_le_bytes(unit)).collect();
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
    let id = r.u128()?;
    let count = r.count(16)?;
    let mut includes = Vec::with_capacity(count);
    for _ in 0..count {
        includes.push(r.u128()?);
    }
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
        id,
        includes,
    })
}

#[cfg(test)]
mod tests;
