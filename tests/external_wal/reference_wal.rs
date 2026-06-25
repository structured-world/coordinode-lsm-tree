// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Minimal append-only reference WAL — the worked example for the external-WAL
//! recipe in `docs/external-wal.md`.
//!
//! This is **illustrative, not a production WAL**: it is `std`-only (a test/dev
//! surface, never the `no_std` production path), single-threaded, and rewrites
//! the whole file on trim. It exists to make the documented recipe executable
//! and self-verifying, not to be fast or crash-hardened in its own right.
//!
//! The contract it embodies (see `docs/external-wal.md`):
//!
//! - A record stores the **original operation** plus the **caller-assigned
//!   seqno** it was applied at. Recovery replays the original op, never
//!   collapsing everything to `insert`.
//! - [`append`](ReferenceWal::append) fsyncs before returning, so a record that
//!   survives a crash is exactly one whose `append` had returned (log-before-apply).
//! - [`trim_through`](ReferenceWal::trim_through) drops every record with
//!   `seqno <= W`, where `W` is the caller's gap-free applied-and-persisted
//!   watermark. Records above `W` (incl. a logged-but-unapplied gap below a
//!   flushed higher seqno) are retained for replay.
//! - [`records`](ReferenceWal::records) reads the survivors back in append order
//!   so recovery can replay each one exactly once, strictly above `W`.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;

/// A single entry inside a logged [`WalOp::Batch`]. A batch shares one seqno
/// across all its entries, so entries carry no seqno of their own. Range
/// tombstones are not batchable, mirroring [`crate::WriteBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchEntry {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    RemoveWeak { key: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
}

/// The original write operation a record was logged for. Replay applies the
/// matching engine call (`insert` / `remove` / `remove_weak` / `remove_range` /
/// `merge` / `apply_batch`), never collapsing to `insert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalOp {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Remove { key: Vec<u8> },
    RemoveWeak { key: Vec<u8> },
    RemoveRange { start: Vec<u8>, end: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
    Batch { entries: Vec<BatchEntry> },
}

/// One logged write: the caller-assigned seqno plus the original operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub seqno: u64,
    pub op: WalOp,
}

/// An append-only file WAL. Each [`append`](Self::append) encodes one
/// [`WalRecord`] and fsyncs before returning.
pub struct ReferenceWal {
    path: PathBuf,
    file: File,
}

impl ReferenceWal {
    /// Creates a fresh, empty WAL at `path` (truncating any existing file).
    pub fn create(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        file.sync_all()?;
        Ok(Self { path, file })
    }

    /// Reopens an existing WAL for replay + further appends after a crash. The
    /// file is opened for append so recovery can keep logging once it resumes.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        Ok(Self { path, file })
    }

    /// Appends one record and fsyncs. On return the record is durable, so it is
    /// safe to apply the matching engine write (log-before-apply).
    pub fn append(&mut self, record: &WalRecord) -> io::Result<()> {
        let mut buf = Vec::new();
        encode_record(record, &mut buf);
        // Frame: [u32 payload length][payload]. The length prefix lets the
        // reader recover the record boundary without a delimiter scan.
        let len = u32::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL record too large"))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&buf)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Reads every surviving record back in append order.
    pub fn records(&self) -> io::Result<Vec<WalRecord>> {
        let mut file = File::open(&self.path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        decode_records(&bytes)
    }

    /// Drops every record with `seqno <= w`, keeping the rest in order. `w` is
    /// the caller's gap-free applied-and-persisted watermark: records at or
    /// below it are durable in the engine's SSTs and never need replay, while
    /// records above it (including a logged-but-unapplied seqno that sits below
    /// a flushed higher one) are retained.
    ///
    /// Rewrites the whole file for simplicity — a production WAL would rotate
    /// segments instead.
    pub fn trim_through(&mut self, w: u64) -> io::Result<()> {
        let kept: Vec<WalRecord> = self
            .records()?
            .into_iter()
            .filter(|r| r.seqno > w)
            .collect();

        let tmp = self.path.with_extension("wal.tmp");
        {
            let mut out = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            for record in &kept {
                let mut buf = Vec::new();
                encode_record(record, &mut buf);
                let len = u32::try_from(buf.len()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "WAL record too large")
                })?;
                out.write_all(&len.to_le_bytes())?;
                out.write_all(&buf)?;
            }
            out.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        // Reopen the live handle on the rewritten file so later appends land
        // after the retained records rather than at the stale offset.
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.file.sync_all()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Encoding: length-prefixed, dependency-free. Each byte string is `[u32 len][bytes]`.
// ---------------------------------------------------------------------------

const TAG_INSERT: u8 = 0;
const TAG_REMOVE: u8 = 1;
const TAG_REMOVE_WEAK: u8 = 2;
const TAG_REMOVE_RANGE: u8 = 3;
const TAG_MERGE: u8 = 4;
const TAG_BATCH: u8 = 5;

const BATCH_INSERT: u8 = 0;
const BATCH_REMOVE: u8 = 1;
const BATCH_REMOVE_WEAK: u8 = 2;
const BATCH_MERGE: u8 = 3;

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("test key/value fits in u32");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode_record(record: &WalRecord, out: &mut Vec<u8>) {
    out.extend_from_slice(&record.seqno.to_le_bytes());
    match &record.op {
        WalOp::Insert { key, value } => {
            out.push(TAG_INSERT);
            put_bytes(out, key);
            put_bytes(out, value);
        }
        WalOp::Remove { key } => {
            out.push(TAG_REMOVE);
            put_bytes(out, key);
        }
        WalOp::RemoveWeak { key } => {
            out.push(TAG_REMOVE_WEAK);
            put_bytes(out, key);
        }
        WalOp::RemoveRange { start, end } => {
            out.push(TAG_REMOVE_RANGE);
            put_bytes(out, start);
            put_bytes(out, end);
        }
        WalOp::Merge { key, value } => {
            out.push(TAG_MERGE);
            put_bytes(out, key);
            put_bytes(out, value);
        }
        WalOp::Batch { entries } => {
            out.push(TAG_BATCH);
            let count = u32::try_from(entries.len()).expect("test batch fits in u32");
            out.extend_from_slice(&count.to_le_bytes());
            for entry in entries {
                match entry {
                    BatchEntry::Insert { key, value } => {
                        out.push(BATCH_INSERT);
                        put_bytes(out, key);
                        put_bytes(out, value);
                    }
                    BatchEntry::Remove { key } => {
                        out.push(BATCH_REMOVE);
                        put_bytes(out, key);
                    }
                    BatchEntry::RemoveWeak { key } => {
                        out.push(BATCH_REMOVE_WEAK);
                        put_bytes(out, key);
                    }
                    BatchEntry::Merge { key, value } => {
                        out.push(BATCH_MERGE);
                        put_bytes(out, key);
                        put_bytes(out, value);
                    }
                }
            }
        }
    }
}

/// A cursor over the raw WAL bytes. Returns `None` from any read once the buffer
/// is exhausted, so a torn tail record (a crash mid-append before fsync, or a
/// truncated frame) is dropped rather than panicking — matching how a real WAL
/// discards a partial trailing record on recovery.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let slice = self.bytes.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }

    fn u32(&mut self) -> Option<u32> {
        let raw: [u8; 4] = self.take(4)?.try_into().ok()?;
        Some(u32::from_le_bytes(raw))
    }

    fn u64(&mut self) -> Option<u64> {
        let raw: [u8; 8] = self.take(8)?.try_into().ok()?;
        Some(u64::from_le_bytes(raw))
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = self.u32()? as usize;
        Some(self.take(len)?.to_vec())
    }
}

fn decode_batch_entries(cur: &mut Cursor) -> Option<Vec<BatchEntry>> {
    let count = cur.u32()? as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = *cur.take(1)?.first()?;
        let entry = match tag {
            BATCH_INSERT => BatchEntry::Insert {
                key: cur.bytes()?,
                value: cur.bytes()?,
            },
            BATCH_REMOVE => BatchEntry::Remove { key: cur.bytes()? },
            BATCH_REMOVE_WEAK => BatchEntry::RemoveWeak { key: cur.bytes()? },
            BATCH_MERGE => BatchEntry::Merge {
                key: cur.bytes()?,
                value: cur.bytes()?,
            },
            _ => return None,
        };
        entries.push(entry);
    }
    Some(entries)
}

fn decode_one(payload: &[u8]) -> Option<WalRecord> {
    let mut cur = Cursor {
        bytes: payload,
        pos: 0,
    };
    let seqno = cur.u64()?;
    let tag = *cur.take(1)?.first()?;
    let op = match tag {
        TAG_INSERT => WalOp::Insert {
            key: cur.bytes()?,
            value: cur.bytes()?,
        },
        TAG_REMOVE => WalOp::Remove { key: cur.bytes()? },
        TAG_REMOVE_WEAK => WalOp::RemoveWeak { key: cur.bytes()? },
        TAG_REMOVE_RANGE => WalOp::RemoveRange {
            start: cur.bytes()?,
            end: cur.bytes()?,
        },
        TAG_MERGE => WalOp::Merge {
            key: cur.bytes()?,
            value: cur.bytes()?,
        },
        TAG_BATCH => WalOp::Batch {
            entries: decode_batch_entries(&mut cur)?,
        },
        _ => return None,
    };
    Some(WalRecord { seqno, op })
}

fn decode_records(bytes: &[u8]) -> io::Result<Vec<WalRecord>> {
    let mut frame = Cursor { bytes, pos: 0 };
    let mut out = Vec::new();
    // Stop at the first short/torn frame: a crash between framing the length and
    // fsyncing the payload leaves a partial tail that must be discarded, not
    // decoded. Every fully-fsynced record precedes it, so this is lossless for
    // the recipe (which only relies on records whose `append` had returned).
    while let Some(len) = frame.u32() {
        let Some(payload) = frame.take(len as usize) else {
            break;
        };
        let Some(record) = decode_one(payload) else {
            break;
        };
        out.push(record);
    }
    Ok(out)
}
