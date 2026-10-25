// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Tight-space compaction on a real filesystem, the one under `TMPDIR`.
//!
//! `MemFs` frees punched bytes exactly, so only a real filesystem shows what a
//! tight-space pass gives back and what it pays for it. Each case builds a tree
//! of overlapping flushed tables, sets a storage quota a little above its
//! footprint so a normal merge does not fit, and times one `major_compact`
//! that the quota turns into a sliced, punched pass. It prints, per case:
//!
//! - the wall time and the tables before and after;
//! - the syncs: file syncs, directory syncs, and the restriction sidecars
//!   among the file syncs;
//! - the punch calls and the bytes they cover;
//! - the allocated bytes at the start, at the peak and at the end, sampled
//!   while the pass runs.
//!
//! Linux only: elsewhere `StdFs` punches no holes and the pass never runs.

#[cfg(target_os = "linux")]
fn main() -> lsm_tree::Result<()> {
    cases::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("tight_space_disk: punching holes needs Linux");
}

#[cfg(target_os = "linux")]
mod cases {
    use lsm_tree::fs::{FaultFs, StdFs, SyncKind};
    use lsm_tree::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Instant;

    /// Rows each flushed table holds, over the same keys in every table.
    const ROWS: u32 = 60_000;

    /// One case: how many overlapping tables, how much room past the
    /// footprint the quota leaves, and whether values go to blob files.
    struct Case {
        inputs: u32,
        headroom_pct: u64,
        blobs: bool,
    }

    /// The bytes allocated to every file under `dir`.
    fn allocated(dir: &Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| match entry.metadata() {
                Ok(meta) if meta.is_dir() => allocated(&entry.path()),
                // `st_blocks` is in 512-byte units.
                Ok(meta) => meta.blocks() * 512,
                Err(_) => 0,
            })
            .sum()
    }

    fn value(i: u32, input: u32, blobs: bool) -> Vec<u8> {
        let len = if blobs { 400 } else { 120 };
        // Varied bytes, so a codec has as much to do as with real values.
        let mut s = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(input);
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s >> 56) as u8
            })
            .collect()
    }

    fn one(case: &Case) -> lsm_tree::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = FaultFs::new(StdFs);
        let injector = fs.injector();
        let mut config = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_fs(fs);
        if case.blobs {
            config = config.with_kv_separation(Some(
                KvSeparationOptions::default().separation_threshold(64),
            ));
        }
        let tree = config.open()?;
        let mut seqno = 0;
        for input in 0..case.inputs {
            for i in 0..ROWS {
                tree.insert(
                    format!("key{i:08}").into_bytes(),
                    value(i, input, case.blobs),
                    seqno,
                );
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
        let used = tree.storage_stats()?.used_bytes;
        let limit = used + used * case.headroom_pct / 100;
        let tight = |c: &mut lsm_tree::runtime_config::RuntimeConfig| {
            c.storage_admission_check = true;
            c.storage_limit_bytes = Some(limit);
            c.tight_space_compaction = true;
        };
        match &tree {
            AnyTree::Standard(t) => t.update_runtime_config(tight)?,
            AnyTree::Blob(t) => t.update_runtime_config(tight)?,
        }

        let tables_before = tree.table_count();
        let start_allocated = allocated(dir.path());
        let peak = Arc::new(AtomicU64::new(start_allocated));
        let done = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (peak, done, root) = (Arc::clone(&peak), Arc::clone(&done), dir.path().to_owned());
            std::thread::spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    peak.fetch_max(allocated(&root), Ordering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        };
        injector.clear();
        let start = Instant::now();
        tree.major_compact(64 * 1024 * 1024, seqno)?;
        let took = start.elapsed();
        done.store(true, Ordering::Relaxed);
        sampler
            .join()
            .map_err(|_| std::io::Error::other("sampler panicked"))?;
        let end_allocated = allocated(dir.path());
        peak.fetch_max(end_allocated, Ordering::Relaxed);

        let syncs = injector.take_syncs();
        let file_syncs = syncs
            .iter()
            .filter(|(_, kind)| matches!(kind, SyncKind::File | SyncKind::FileData))
            .count();
        let dir_syncs = syncs
            .iter()
            .filter(|(_, kind)| matches!(kind, SyncKind::Directory | SyncKind::Device))
            .count();
        let sidecars = syncs
            .iter()
            .filter(|(path, _)| path.to_string_lossy().contains(".restrict-bound"))
            .count();
        let table_punches = injector.punches_for("tables");
        let blob_punches = injector.punches_for("blobs");
        let covered = |punches: &[(u64, u64)]| punches.iter().map(|&(_, len)| len).sum::<u64>();

        // The pass rewrote, it did not lose: the last input's value reads back.
        for i in (0..ROWS).step_by(997) {
            let got = tree.get(format!("key{i:08}").as_bytes(), lsm_tree::SeqNo::MAX)?;
            assert_eq!(
                got.as_deref(),
                Some(value(i, case.inputs - 1, case.blobs).as_slice()),
                "key {i} after the pass",
            );
        }
        println!(
            "case inputs={} headroom={}% blobs={} took={took:?} tables={tables_before}->{} \
             file_syncs={file_syncs} dir_syncs={dir_syncs} sidecar_syncs={sidecars} \
             table_punches={} table_punched={} blob_punches={} blob_punched={} \
             allocated_start={start_allocated} allocated_peak={} allocated_end={end_allocated}",
            case.inputs,
            case.headroom_pct,
            case.blobs,
            tree.table_count(),
            table_punches.len(),
            covered(&table_punches),
            blob_punches.len(),
            covered(&blob_punches),
            peak.load(Ordering::Relaxed),
        );
        Ok(())
    }

    pub fn run() -> lsm_tree::Result<()> {
        for inputs in [1, 4] {
            for headroom_pct in [5, 15, 40] {
                one(&Case {
                    inputs,
                    headroom_pct,
                    blobs: false,
                })?;
            }
        }
        for headroom_pct in [5, 15] {
            one(&Case {
                inputs: 2,
                headroom_pct,
                blobs: true,
            })?;
        }
        Ok(())
    }
}
