// Integration test: encryption under realistic mixed-workload conditions.
//
// Exercises the encryption-at-rest path across the full combination matrix
// of compression backends, write/update/delete operations, point + range
// reads at MAX_SEQNO, memtable flush, major compaction, and tree-reopen
// recovery. The single-threaded matrix sweeps every (compression ×
// encryption) cell; the concurrent variant adds writer / reader
// contention on top of the most-stressing cell. Snapshot-seqno (MVCC)
// reads are deliberately out of scope here — they depend on flush-GC
// thresholds and are covered separately.
//
// Each cell asserts: every inserted key reads back with the expected
// value at its expected seqno; deletes shadow earlier inserts; range
// scans return the expected count + content; after close+reopen via a
// fresh `Tree`, the same invariants hold without any in-memory state.
//
// Part 2 of #128. Part 1 (the tempfile helper) landed as #293.

#![cfg(feature = "encryption")]

use lsm_tree::{
    AbstractTree, Aes256GcmProvider, AnyTree, CompressionType, Config, Guard,
    SequenceNumberCounter, config::CompressionPolicy,
};
// `Guard` looks unused at first glance but is required: `tree.range()`
// yields `IterGuardImpl`, and `.into_inner()` is a method on the
// `IterGuard` trait (re-exported as `Guard`). Without the import,
// the trait isn't in scope and compilation fails with E0599.
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// Stable test key. The actual bytes don't matter — every cell uses the
/// same key so reopen-recovery tests can construct a fresh provider with
/// the same key bytes and decrypt the on-disk data.
fn test_key() -> [u8; 32] {
    [0x42; 32]
}

/// Raise this process's open-file soft limit toward its hard limit.
///
/// `setrlimit(RLIMIT_NOFILE)` raising the soft limit up to the hard limit is
/// unprivileged. On macOS the hard limit is reported as an unbounded sentinel
/// but the kernel rejects a soft limit above `kern.maxfilesperproc`, so the
/// target is capped to that value there. Best-effort: any failure leaves the
/// limit unchanged and the test simply runs closer to the original budget.
#[cfg(unix)]
fn raise_fd_limit() {
    // SAFETY: get/setrlimit operate on an `rlimit` we own; the macOS sysctl
    // reads one `c_int` into a stack buffer whose byte length we pass and read
    // back, so the kernel never writes past it.
    unsafe {
        let mut rlim: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) != 0 {
            return;
        }

        // On macOS an unbounded hard limit must be capped to the kernel's
        // per-process file cap, else setrlimit is rejected. Elsewhere the hard
        // limit is a usable finite value.
        #[cfg(target_os = "macos")]
        let target = if rlim.rlim_max == libc::RLIM_INFINITY {
            let mut per_proc: libc::c_int = 0;
            let mut len = std::mem::size_of::<libc::c_int>();
            let queried = libc::sysctlbyname(
                c"kern.maxfilesperproc".as_ptr(),
                std::ptr::addr_of_mut!(per_proc).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            ) == 0
                && per_proc > 0;
            if queried {
                libc::rlim_t::try_from(per_proc).unwrap_or(8192)
            } else {
                8192
            }
        } else {
            rlim.rlim_max
        };
        #[cfg(not(target_os = "macos"))]
        let target = rlim.rlim_max;

        if target > rlim.rlim_cur {
            rlim.rlim_cur = target;
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
        }
    }
}

#[cfg(not(unix))]
fn raise_fd_limit() {}

/// Default config + per-cell compression + optional encryption + optional
/// Page ECC. When `ecc` is set the SST blocks carry a Reed-Solomon parity
/// trailer OUTSIDE the encryption envelope, so this exercises the
/// ECC-trailer-strip → AEAD-decrypt → decompress read path end-to-end.
fn open_tree(
    dir: &std::path::Path,
    compression: CompressionType,
    encrypt: bool,
    ecc: bool,
) -> lsm_tree::Result<AnyTree> {
    let mut cfg = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(compression));

    if encrypt {
        let provider = Arc::new(Aes256GcmProvider::new(&test_key()));
        cfg = cfg.with_encryption(Some(provider));
    }

    // ECC needs an explicit shard scheme (there is no implicit RS default);
    // RS(4,2) matches the SST writer's per-block parity layout.
    #[cfg(feature = "page_ecc")]
    if ecc {
        cfg = cfg
            .page_ecc(true)
            .ecc_scheme(lsm_tree::runtime_config::EccScheme::ReedSolomon {
                data_shards: 4,
                parity_shards: 2,
            });
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = ecc;

    cfg.open()
}

/// Human-readable label for a (compression, encryption, ecc) cell. Used in
/// assertion messages so a failing cell is identifiable without inspecting
/// the stack trace.
fn cell_label(compression: CompressionType, encrypt: bool, ecc: bool) -> String {
    let suffix = format!(
        "{}{}",
        if encrypt { "+enc" } else { "" },
        if ecc { "+ecc" } else { "" }
    );
    let comp = match compression {
        CompressionType::None => "none",
        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => "lz4",
        #[cfg(zstd_any)]
        CompressionType::Zstd(level) => {
            return format!("zstd{level}{suffix}");
        }
        #[cfg(zstd_any)]
        CompressionType::ZstdDict { level, dict_id } => {
            return format!("zstdDict{level}@{dict_id}{suffix}");
        }
        // CompressionType is #[non_exhaustive] — future variants get a
        // generic label so a new compression type doesn't break this
        // helper at compile time. Tests still exercise specific cells
        // via explicit `cell_*` test functions below.
        _ => "other",
    };
    format!("{comp}{suffix}")
}

/// The single-cell stress sequence. Runs the same invariant chain across
/// every (compression × encryption) combination so a regression in any
/// specific cell is isolated to one test failure.
///
/// Sequence:
///   1. Insert 1000 keys (k00000..k00999) with v=k mirrored at seqno=i
///   2. Update first 100 (k00000..k00099) at seqno=2000+i with v="updated"
///   3. Delete next 50 (k00100..k00149) at seqno=3000+i
///   4. Flush active memtable → forces encrypted SST write
///   5. Insert 500 more keys (k01000..k01499) at seqno=4000+i — these
///      live in the NEW memtable, while the flushed SST holds the older
///      data. Read path now spans memtable + SST.
///   6. Point-read every key at MAX_SEQNO:
///      - k00000..k00099: returns "updated" (the update at 2000+i shadows
///        the original insert at i)
///      - k00100..k00149: returns None (tombstoned)
///      - k00150..k00999: returns original value
///      - k01000..k01499: returns original value
///   7. (Snapshot-seqno read intentionally omitted — MVCC is out of
///      scope for this suite; see module-level comment.)
///   8. Range scan over [k00500, k00700): exactly 200 entries, all
///      original values
///   9. Drop the tree, reopen from disk with the SAME encryption provider
///      bytes, repeat steps 6 + 8 against the recovered tree.
fn stress_cell(compression: CompressionType, encrypt: bool, ecc: bool) -> lsm_tree::Result<()> {
    let label = cell_label(compression, encrypt, ecc);
    let dir = tempfile::tempdir()?;

    // Phase 1: open + populate + flush + extra inserts
    {
        let tree = open_tree(dir.path(), compression, encrypt, ecc)?;

        // Step 1: initial inserts at seqno=1+i (1..=1000). seqno=0 is a
        // sentinel meaning "compacted final" in the engine, so writes
        // start at 1.
        for i in 0u32..1000 {
            let key = format!("k{i:05}");
            let val = format!("v{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), 1 + u64::from(i));
        }

        // Step 2: updates on first 100 at seqno=2001+i
        for i in 0u32..100 {
            let key = format!("k{i:05}");
            let val = format!("updated-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), 2001 + u64::from(i));
        }

        // Step 3: tombstones on k00100..k00149 at seqno=3001+i
        for i in 100u32..150 {
            let key = format!("k{i:05}");
            tree.remove(key.as_bytes(), 3001 + u64::from(i));
        }

        // Step 4: flush — produces an encrypted SST on disk
        tree.flush_active_memtable(3999)?;

        // Step 5: more inserts at seqno=4001+i — these live in the new
        // memtable, providing a read path that spans both layers
        // (memtable + first SST) for the pre-reopen check.
        for i in 0u32..500 {
            let key = format!("k{:05}", 1000 + i);
            let val = format!("late-{i:05}");
            tree.insert(key.as_bytes(), val.as_bytes(), 4001 + u64::from(i));
        }

        verify_invariants(&tree, &label, "pre-reopen")?;

        // Flush late inserts to disk before dropping the tree — without
        // this the second memtable is lost and the post-reopen check
        // can't see the late writes (they only existed in RAM).
        tree.flush_active_memtable(4999)?;
    }

    // Phase 2: reopen from disk → recovery + decrypt path exercised
    {
        let tree = open_tree(dir.path(), compression, encrypt, ecc)?;
        verify_invariants(&tree, &label, "post-reopen")?;
    }

    Ok(())
}

/// Shared invariant chain used both pre- and post-reopen. Asserts the
/// expected MAX_SEQNO read results for every key range and the
/// expected range-scan content. Snapshot-seqno (MVCC) reads are
/// intentionally out of scope — see module-level comment.
fn verify_invariants(tree: &AnyTree, label: &str, phase: &str) -> lsm_tree::Result<()> {
    // Step 6: MAX_SEQNO reads — sees all updates and tombstones
    for i in 0u32..100 {
        let key = format!("k{i:05}");
        let expected = format!("updated-{i:05}");
        let got = tree
            .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
            .unwrap_or_else(|| {
                panic!("{label} {phase}: expected updated value for {key}, got None")
            });
        assert_eq!(got.as_ref(), expected.as_bytes(), "{label} {phase}: {key}");
    }
    for i in 100u32..150 {
        let key = format!("k{i:05}");
        let got = tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?;
        assert!(
            got.is_none(),
            "{label} {phase}: {key} should be tombstoned, got {got:?}"
        );
    }
    for i in 150u32..1000 {
        let key = format!("k{i:05}");
        let expected = format!("v{i:05}");
        let got = tree
            .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
            .unwrap_or_else(|| {
                panic!("{label} {phase}: expected original value for {key}, got None")
            });
        assert_eq!(got.as_ref(), expected.as_bytes(), "{label} {phase}: {key}");
    }
    for i in 0u32..500 {
        let key = format!("k{:05}", 1000 + i);
        let expected = format!("late-{i:05}");
        let got = tree
            .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
            .unwrap_or_else(|| panic!("{label} {phase}: expected late value for {key}, got None"));
        assert_eq!(got.as_ref(), expected.as_bytes(), "{label} {phase}: {key}");
    }

    // Snapshot-seqno reads intentionally omitted — see module-level
    // comment. This suite covers encryption + decompression correctness
    // at MAX_SEQNO; MVCC interactions are tested elsewhere.

    // Step 8: range scan [k00500, k00700) at MAX_SEQNO — exactly 200 entries
    // with their original values (none of this range was updated or
    // tombstoned).
    let start = b"k00500".to_vec();
    let end = b"k00700".to_vec();
    let mut count = 0usize;
    for item in tree.range(start..end, lsm_tree::MAX_SEQNO, None) {
        let (k, v) = item.into_inner()?;
        count += 1;
        let key_str = std::str::from_utf8(&k).unwrap();
        let idx: u32 = key_str.strip_prefix('k').unwrap().parse().unwrap();
        let expected = format!("v{idx:05}");
        assert_eq!(
            v.as_ref(),
            expected.as_bytes(),
            "{label} {phase} range: {key_str}"
        );
    }
    assert_eq!(count, 200, "{label} {phase} range scan count");

    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Cell matrix — one test per (compression × encryption) combination.
// Splitting into separate tests so a regression in one cell doesn't mask
// failures in others, and so each cell shows up individually in the
// nextest progress feed.
// ────────────────────────────────────────────────────────────────────────

#[test]
fn cell_none_plaintext_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::None, false, false)
}

#[test]
fn cell_none_encrypted_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::None, true, false)
}

#[test]
#[cfg(feature = "lz4")]
fn cell_lz4_plaintext_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Lz4, false, false)
}

#[test]
#[cfg(feature = "lz4")]
fn cell_lz4_encrypted_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Lz4, true, false)
}

#[test]
#[cfg(zstd_any)]
fn cell_zstd1_plaintext_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(1), false, false)
}

#[test]
#[cfg(zstd_any)]
fn cell_zstd1_encrypted_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(1), true, false)
}

#[test]
#[cfg(zstd_any)]
fn cell_zstd3_plaintext_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(3), false, false)
}

#[test]
#[cfg(zstd_any)]
fn cell_zstd3_encrypted_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(3), true, false)
}

// ────────────────────────────────────────────────────────────────────────
// Encryption × Page-ECC cells — the third axis. With Page ECC on, every
// SST block carries a Reed-Solomon parity trailer that sits OUTSIDE the
// AAD-bound encryption envelope. The read path must strip + verify the
// parity trailer, THEN AEAD-decrypt the body, THEN decompress — so these
// cells pin that the two features coexist through flush + reopen and that
// the parity layer doesn't corrupt the AAD layer (or vice versa). Gated on
// `page_ecc`: the RS implementation only exists under that feature.
// ────────────────────────────────────────────────────────────────────────

#[test]
#[cfg(feature = "page_ecc")]
fn cell_none_encrypted_ecc_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::None, true, true)
}

#[test]
#[cfg(all(feature = "page_ecc", feature = "lz4"))]
fn cell_lz4_encrypted_ecc_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Lz4, true, true)
}

#[test]
#[cfg(all(feature = "page_ecc", zstd_any))]
fn cell_zstd1_encrypted_ecc_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(1), true, true)
}

#[test]
#[cfg(all(feature = "page_ecc", zstd_any))]
fn cell_zstd3_encrypted_ecc_round_trips() -> lsm_tree::Result<()> {
    stress_cell(CompressionType::Zstd(3), true, true)
}

// ────────────────────────────────────────────────────────────────────────
// Provider capability gate — on a zstd build the live block path seals
// encrypted blocks through the AAD-bound envelope, so a provider that only
// implements the opaque surface must be rejected at open time (fail-fast)
// rather than blowing up on the first encrypted read/write.
// ────────────────────────────────────────────────────────────────────────

#[cfg(zstd_any)]
#[test]
fn opaque_only_provider_is_rejected_at_open() {
    use lsm_tree::encryption::EncryptionProvider;

    // Minimal provider with NO `supports_aad_block_path` override (defaults to
    // false) and no AAD-bound block methods — exactly the downstream shape the
    // gate must catch.
    struct OpaqueOnly;
    impl std::panic::UnwindSafe for OpaqueOnly {}
    impl std::panic::RefUnwindSafe for OpaqueOnly {}
    impl EncryptionProvider for OpaqueOnly {
        fn encrypt(&self, p: &[u8]) -> lsm_tree::Result<Vec<u8>> {
            Ok(p.to_vec())
        }
        fn decrypt(&self, c: &[u8]) -> lsm_tree::Result<Vec<u8>> {
            Ok(c.to_vec())
        }
        fn max_overhead(&self) -> u32 {
            0
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_encryption(Some(Arc::new(OpaqueOnly)))
    .open();

    assert!(
        matches!(result, Err(lsm_tree::Error::Encrypt(_))),
        "opaque-only provider must be rejected at open, got {:?}",
        result.map(|_| "Ok(tree)")
    );
}

// ────────────────────────────────────────────────────────────────────────
// Major compaction cell — runs the same invariant chain BUT triggers a
// major compaction before reopen, so the recovery path validates that
// post-compaction SSTs (single merged file per level) decrypt + decode
// correctly. Done as a separate test rather than expanding stress_cell
// so a compaction-specific regression is isolated.
// ────────────────────────────────────────────────────────────────────────

#[test]
fn major_compaction_encrypted_round_trips() -> lsm_tree::Result<()> {
    let compression = CompressionType::None;
    let label = "none+enc+major-compact";
    let dir = tempfile::tempdir()?;

    {
        let tree = open_tree(dir.path(), compression, true, false)?;

        // Populate three flush groups so major compaction has multiple
        // SSTs to merge (otherwise it's a no-op).
        for batch in 0u32..3 {
            for i in 0u32..200 {
                let key = format!("b{batch}k{i:05}");
                let val = format!("b{batch}v{i:05}");
                // seqno=0 is the engine's compacted-final sentinel; start at 1
                // so the first write (batch=0, i=0) exercises the same MVCC
                // path as user writes.
                let seqno = 1 + u64::from(batch) * 1000 + u64::from(i);
                tree.insert(key.as_bytes(), val.as_bytes(), seqno);
            }
            tree.flush_active_memtable(1 + u64::from(batch) * 1000 + 999)?;
        }

        // Major compaction rewrites every SST into one merged file per
        // level — exercises encrypted-block read + re-encrypted-block
        // write on the compaction worker.
        tree.major_compact(u64::MAX, 99_999)?;

        // Verify everything still readable post-compaction
        for batch in 0u32..3 {
            for i in 0u32..200 {
                let key = format!("b{batch}k{i:05}");
                let expected = format!("b{batch}v{i:05}");
                let got = tree
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .unwrap_or_else(|| {
                        panic!("{label} pre-reopen: expected {key} present, got None")
                    });
                assert_eq!(
                    got.as_ref(),
                    expected.as_bytes(),
                    "{label} pre-reopen: {key}"
                );
            }
        }
    }

    // Reopen — recovers from post-compaction encrypted SSTs only
    {
        let tree = open_tree(dir.path(), compression, true, false)?;
        for batch in 0u32..3 {
            for i in 0u32..200 {
                let key = format!("b{batch}k{i:05}");
                let expected = format!("b{batch}v{i:05}");
                let got = tree
                    .get(key.as_bytes(), lsm_tree::MAX_SEQNO)?
                    .unwrap_or_else(|| {
                        panic!("{label} post-reopen: expected {key} present, got None")
                    });
                assert_eq!(
                    got.as_ref(),
                    expected.as_bytes(),
                    "{label} post-reopen: {key}"
                );
            }
        }
    }

    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Concurrent stress — 4 writers + 2 readers contend on the same encrypted
// tree for a fixed duration. Readers issue deterministic point reads only
// (no range scans). Asserts:
//   • no panics
//   • no data corruption — every observed key reads back as either its
//     committed value (`w{id}_v...` prefix) or `None`, never as garbage
//     bytes (which would indicate AAD mismatch surfacing as silent
//     wrong-data instead of a decrypt error)
//   • after the final flush + reopen, EVERY committed write survives
//     the round-trip through encrypted SST decoding (exact-equality
//     assertion against the writer-side tally). Workers only bump
//     the writes_committed counter AFTER a successful insert into
//     the active memtable, the stop signal happens-before the main
//     flush via the Release/Acquire pair on `stop`, and the final
//     flush_active_memtable seals every still-live memtable cell —
//     so any post-reopen shortfall indicates real flush/recovery
//     data loss, not a benign in-flight race.
// ────────────────────────────────────────────────────────────────────────

#[test]
fn concurrent_encrypted_no_corruption() -> lsm_tree::Result<()> {
    // This test flushes many SSTs while readers hold descriptors open via the
    // engine's descriptor cache, so its live fd count can exceed a low default
    // soft limit (e.g. macOS's 256) and surface as EMFILE — a resource limit,
    // not an encryption fault. Raise this process's soft limit to its hard
    // limit so the assertion measures encryption correctness, not fd budget.
    raise_fd_limit();

    let dir = tempfile::tempdir()?;
    let tree = Arc::new(open_tree(dir.path(), CompressionType::None, true, false)?);

    let stop = Arc::new(AtomicBool::new(false));
    // Start at 1 so the first concurrent write doesn't hit the engine's
    // seqno=0 compacted-final sentinel.
    let seqno_gen = Arc::new(AtomicU64::new(1));
    let writes_committed = Arc::new(AtomicU64::new(0));
    // Surface any I/O / encryption error from worker threads. Without this
    // a failed flush or get under contention would be silently swallowed.
    let worker_errors = Arc::new(AtomicU64::new(0));
    // Per-writer max index reached — drives post-reopen probing without
    // assuming the four writer threads made balanced progress. A scheduler
    // that gives one writer 10× more CPU than the others is fine; we
    // probe each writer's individual range exactly.
    let writer_max_idx: Arc<Vec<AtomicU32>> = Arc::new((0..4).map(|_| AtomicU32::new(0)).collect());

    let mut handles = Vec::new();

    // 4 writers — each writes unique keys in its own namespace (w{id}_kNNN)
    // so they don't conflict on the same logical key.
    for writer_id in 0u32..4 {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);
        let seqno_gen = Arc::clone(&seqno_gen);
        let writes = Arc::clone(&writes_committed);
        let errors = Arc::clone(&worker_errors);
        let max_idx = Arc::clone(&writer_max_idx);
        handles.push(std::thread::spawn(move || {
            let mut i = 0u32;
            // Acquire pairs with the main thread's Release store on
            // stop — guarantees the worker eventually observes the
            // termination signal. Relaxed would compile to the same
            // load on x86 but allows the formal memory model to delay
            // observation indefinitely, risking a join() hang on
            // weaker targets (aarch64 / riscv).
            while !stop.load(Ordering::Acquire) {
                let key = format!("w{writer_id}_k{i:06}");
                let val = format!("w{writer_id}_v{i:06}");
                let seqno = seqno_gen.fetch_add(1, Ordering::Relaxed);
                tree.insert(key.as_bytes(), val.as_bytes(), seqno);
                writes.fetch_add(1, Ordering::Relaxed);
                #[expect(
                    clippy::indexing_slicing,
                    reason = "writer_id < 4 by loop bound; max_idx has 4 slots"
                )]
                // Record the highest index this writer has committed so far
                // so the post-reopen probe can cover its exact range.
                max_idx[writer_id as usize].store(i + 1, Ordering::Relaxed);
                i += 1;
                if i.is_multiple_of(100) {
                    // Periodic flush to exercise memtable→SST transition
                    // under concurrent reads. Flush failure under contention
                    // would mask SST-write bugs, so propagate to the main
                    // thread instead of swallowing.
                    if tree.flush_active_memtable(seqno).is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            }
        }));
    }

    // 2 readers — deterministic point reads cycling through all four
    // writer namespaces (round-robin on `i % 4`). Range scans are
    // intentionally NOT exercised here: this test targets the
    // encrypt/decrypt + AAD-matching path on the point-get hot loop,
    // and adding scans would dilute the per-second iteration count
    // without finding additional decryption paths (scans go through
    // the same decrypt code as gets, one block at a time). Assert
    // every returned value is either a valid `w{id}_v` pattern OR
    // `None`; a garbage value would indicate decryption failure or
    // AAD mismatch surfacing as silent wrong-data instead of an
    // error.
    for _ in 0..2 {
        let tree = Arc::clone(&tree);
        let stop = Arc::clone(&stop);
        let errors = Arc::clone(&worker_errors);
        handles.push(std::thread::spawn(move || {
            let mut i = 0u32;
            // Acquire-load pairs with main's Release-store on stop
            // (same rationale as the writer threads above).
            while !stop.load(Ordering::Acquire) {
                let writer_id = i % 4;
                let key_idx = i / 4;
                let key = format!("w{writer_id}_k{key_idx:06}");
                match tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO) {
                    Ok(Some(got)) => {
                        let val_str = std::str::from_utf8(&got).expect("valid utf8 from decrypt");
                        let expected_prefix = format!("w{writer_id}_v");
                        assert!(
                            val_str.starts_with(&expected_prefix),
                            "concurrent read got garbage {val_str} for {key}"
                        );
                    }
                    Ok(None) => {
                        // key not yet written by its writer — fine
                    }
                    Err(_) => {
                        // Encrypted read failed under contention. Should
                        // never happen — surface to the main thread.
                        errors.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                i = i.wrapping_add(1);
            }
        }));
    }

    // Run for 2 seconds of mixed contention.
    std::thread::sleep(Duration::from_secs(2));
    // Release-store paired with Acquire-loads on every worker's stop
    // check — establishes the happens-before edge that guarantees
    // termination on weak-memory targets.
    stop.store(true, Ordering::Release);
    for h in handles {
        h.join().expect("worker panic");
    }
    let errors_seen = worker_errors.load(Ordering::Relaxed);
    assert_eq!(
        errors_seen, 0,
        "concurrent workers reported {errors_seen} flush/read errors — \
         encrypted I/O under contention must not return Err"
    );

    // Final verification — flush remaining memtable then sample-read
    // every 50th key written and confirm presence.
    let final_seqno = seqno_gen.load(Ordering::Relaxed);
    tree.flush_active_memtable(final_seqno)?;
    let total_written = writes_committed.load(Ordering::Relaxed);
    assert!(
        total_written > 0,
        "no writes committed in 2s contention window"
    );

    // Reopen and confirm a meaningful fraction of the writes survived
    // round-trip through encrypted SST decode. Probe each writer's
    // EXACT committed index range (recorded as the writers ran) and
    // count every Some — tolerates internal gaps (so a single missing
    // key doesn't truncate the count) but enforces a real recovery
    // fraction against the writer-side tally, not just `> 0`. Per-writer
    // ranges avoid the "balanced progress" assumption that an
    // average-based probe (total_written / 4) would impose — a scheduler
    // that gives one writer 10× more CPU than the others is still
    // covered exactly.
    drop(tree);
    let tree = open_tree(dir.path(), CompressionType::None, true, false)?;
    let mut found = 0u64;
    for writer_id in 0u32..4 {
        #[expect(
            clippy::indexing_slicing,
            reason = "writer_id < 4 by loop bound; writer_max_idx has 4 slots"
        )]
        let upper = writer_max_idx[writer_id as usize].load(Ordering::Relaxed);
        for i in 0u32..upper {
            let key = format!("w{writer_id}_k{i:06}");
            if tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO)?.is_some() {
                found += 1;
            }
        }
    }
    // After workers joined AND we called flush_active_memtable above,
    // every counted insert must be readable post-reopen: workers only
    // ever bump `writes_committed` AFTER a successful insert into the
    // active memtable, the stop signal happens-before the main flush
    // (via the Release/Acquire pair on `stop`), and the final flush
    // seals every still-live memtable cell into an SST. Anything less
    // than exact equality means encrypted SST flush + recovery dropped
    // committed bytes, which is a real bug.
    assert_eq!(
        found, total_written,
        "post-reopen found {found} of {total_written} committed writes \
         (expected exact match); encrypted concurrent writes failed to persist"
    );

    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Wrong-key recovery — open a tree with key A, write data, close. Reopen
// with key B (different bytes). Assert: opening fails OR reads fail with
// a recognisable decrypt error — but NEVER returns garbage that looks like
// valid plaintext. This is the AAD threat model's most important property:
// no key, no data, no silent corruption.
// ────────────────────────────────────────────────────────────────────────

#[test]
fn wrong_key_reopen_reads_error() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Write with key A
    {
        let provider_a = Arc::new(Aes256GcmProvider::new(&[0xAA; 32]));
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .with_encryption(Some(provider_a))
        .open()?;

        for i in 0u32..100 {
            let key = format!("k{i:05}");
            let val = format!("v{i:05}");
            // seqno=0 is the engine's compacted-final sentinel; start at 1.
            tree.insert(key.as_bytes(), val.as_bytes(), 1 + u64::from(i));
        }
        tree.flush_active_memtable(100)?;
    }

    // Reopen with key B — different bytes
    let provider_b = Arc::new(Aes256GcmProvider::new(&[0xBB; 32]));
    let reopen_result = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .with_encryption(Some(provider_b))
    .open();

    // Two acceptable outcomes:
    //   (a) Open itself fails — reader catches the key mismatch when
    //       loading meta or the first block.
    //   (b) Open succeeds but reads fail — reader tolerates the meta
    //       layer but the per-block AEAD check rejects.
    // FORBIDDEN: open succeeds AND reads return garbage masquerading
    // as valid plaintext.
    match reopen_result {
        Err(_) => {
            // (a) — open failed loudly, satisfies the property.
        }
        Ok(tree) => {
            // (b) — every read must fail with a block-validation error.
            // The documented contract (`src/config/mod.rs`, `src/table/block/mod.rs`)
            // is "AEAD rejects → Err, never silent None or garbage". Accepting
            // Ok(None) / Ok(Some(_)) here would let a regression that strips
            // AEAD checks pass undetected.
            for i in 0u32..100 {
                let key = format!("k{i:05}");
                assert!(
                    tree.get(key.as_bytes(), lsm_tree::MAX_SEQNO).is_err(),
                    "wrong-key read for {key} must error — encryption is \
                     not enforced on the read path"
                );
            }
        }
    }

    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Slow-test sentinel — a smoke check that the stress suite itself doesn't
// hang. Has a budget; if any individual cell exceeds it, nextest's
// per-test slow-timeout will surface it.
// ────────────────────────────────────────────────────────────────────────

#[test]
fn single_cell_smoke_under_30s() -> lsm_tree::Result<()> {
    let start = Instant::now();
    stress_cell(CompressionType::None, false, false)?;
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "single-cell stress took {elapsed:?}, expected < 30s — investigate before scaling concurrent matrix"
    );
    Ok(())
}
