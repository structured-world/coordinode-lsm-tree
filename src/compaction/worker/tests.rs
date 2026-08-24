use super::{create_compaction_stream, pick_run_indexes};
use crate::{
    AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, Table, TableId,
    compaction::{Choice, CompactionStrategy, Input, state::CompactionState},
    config::BlockSizePolicy,
    version::Version,
};
use std::sync::Arc;
use test_log::test;

/// Shared key count and formatter for the tight-space crash-recovery tests, so
/// the writer (in `tight_space_crash_and_reopen`) and the reopen assertion loop
/// always cover the identical key set.
const TIGHT_SPACE_KEYS: u64 = 2_000;
fn tight_space_key(i: u64) -> String {
    format!("key{i:08}")
}

/// Ranks keys by their first byte only, so byte-distinct keys that share a
/// first byte compare equal — exercises the comparator-aware dedup path that
/// raw `dedup()` would miss.
struct FirstByteComparator;
impl crate::comparator::UserComparator for FirstByteComparator {
    fn name(&self) -> &'static str {
        "test-first-byte"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> core::cmp::Ordering {
        a.first().cmp(&b.first())
    }
}

#[test]
fn boundary_candidates_dedups_comparator_equal_keys() {
    let cmp: crate::comparator::SharedComparator = Arc::new(FirstByteComparator);
    // "a1" and "a2" are byte-distinct but compare equal under the first-byte
    // comparator; "b1" is in a different group. Raw dedup() would keep both
    // a-keys (not byte-identical) and, after popping the global max, leave
    // two boundaries in the "a" group → overlapping sub-compaction ranges.
    let keys = vec![
        crate::UserKey::from("a1"),
        crate::UserKey::from("a2"),
        crate::UserKey::from("b1"),
    ];
    let out = super::boundary_candidates(keys, &cmp);
    assert_eq!(
        out.len(),
        1,
        "comparator-equal keys must collapse to a single boundary candidate",
    );
    assert_eq!(
        out.first().and_then(|k| k.first()),
        Some(&b'a'),
        "the surviving boundary should be from the deduped a-group",
    );
}

/// A failing sub-compaction range must abort the whole compaction, roll
/// back the finalized files of the ranges that DID succeed, and restore the
/// hidden input tables — leaving the tree fully readable with nothing
/// partially installed. Drives the parallel rollback path via the test
/// failpoint (one range errors, its siblings succeed and are rolled back).
#[cfg(feature = "parallel")]
#[test]
fn failed_subcompaction_rolls_back_and_restores_inputs() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 4_000;
    let key = |i: u64| format!("key_{i:08}");
    let val = |i: u64, generation: u64| format!("g{generation}-{i}-{}", "x".repeat(40));

    let dir = tempfile::tempdir()?;
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    // KV separation so the surviving sub-compactions also produce blob
    // files, exercising the blob-file arm of the rollback as well.
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ));
    // Share the failpoint handle before the config is consumed by open().
    let failpoint = config.fail_one_subcompaction.clone();
    let tree = config.open()?;

    // Populate the bottom level with several tables (the split boundaries).
    for i in 0..N {
        tree.insert(key(i), val(i, 0), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Overwrite the whole keyspace into L0; the next compaction merges it
    // into the populated bottom and splits into parallel sub-compactions.
    for i in 0..N {
        tree.insert(key(i), val(i, 1), N + i);
    }
    tree.flush_active_memtable(0)?;
    let tables_before = tree.table_count();

    // Arm: exactly one sub-compaction range will error.
    failpoint.store(true, Ordering::SeqCst);
    let result = tree.major_compact(u64::MAX, 0);

    assert!(
        result.is_err(),
        "a failing sub-compaction range must abort the compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed itself",
    );
    assert_eq!(
        tree.table_count(),
        tables_before,
        "rollback must leave nothing partially installed",
    );
    for i in 0..N {
        assert_eq!(
            tree.get(key(i), crate::MAX_SEQNO)?.as_deref(),
            Some(val(i, 1).as_bytes()),
            "value for {} must survive the rolled-back compaction",
            key(i),
        );
    }
    Ok(())
}

/// A tight-space compaction that crashes after durably installing and
/// punching its first slice must reopen consistently: the manifest carries
/// the input's persisted key-range restriction, and recovery rebuilds the
/// restricted view so every key (those in the installed slice output AND
/// those still in the punched input's intact suffix) reads back.
#[test]
fn tight_space_crash_after_first_slice_recovers_all_keys_on_reopen() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    // Force the single-table major compaction to be gated, opting in to
    // tight-space reclaim by leaving only a quarter of the footprint free.
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(mem.clone()),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            reopened
                .get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "key {i} lost after a crash mid tight-space compaction + reopen",
        );
    }
    Ok(())
}

/// The `.restrict-bound` sidecar is written AFTER the slice commits, so a
/// failure (or a crash) in that window leaves a committed restriction with no
/// sidecar. The manifest still describes it, but a later manifest-loss repair
/// would find an unrestricted input beside the slice output and publish BOTH
/// histories — and merge operands, which are deliberately never deduplicated
/// across sources, would then be applied twice. The window has to close by
/// itself: the manifest is the authority, so an open that finds a restricted
/// table without its sidecar writes it.
#[test]
fn open_rewrites_a_missing_restriction_sidecar() -> crate::Result<()> {
    use crate::fs::Fs;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let fs: Arc<dyn Fs> = Arc::new(mem.clone());
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::clone(&fs),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;

    // Locate the restricted table and delete its sidecar, reproducing the
    // post-commit window where the write never landed.
    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the crashed tight-space slice must leave a restricted table");
    };
    let sidecar = crate::restrict_bound::sidecar_path(&restricted.path);
    fs.remove_file(&sidecar)?;
    assert!(
        fs.metadata(&sidecar).is_err(),
        "the fixture must start from a MISSING sidecar",
    );
    drop(version);
    drop(reopened);

    let _tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?;

    assert!(
        fs.metadata(&sidecar).is_ok(),
        "the open must republish the sidecar from the manifest'\u{2019}s own \
         restriction, or a later manifest-loss repair republishes the whole \
         input beside the slice output",
    );
    Ok(())
}

/// A real-on-disk [`Fs`](crate::fs::Fs) wrapper (over
/// [`StdFs`](crate::fs::StdFs)) that simulates disk pressure for the tight-space
/// compaction test while keeping every byte in a real file under the test's
/// `tempdir`. It reports a fixed `available_space`, advertises hole-punch
/// support, and EMULATES `punch_hole` by zeroing the range in place (real
/// `StdFs::punch_hole` is Linux-only, so emulation keeps the test
/// cross-platform and locally runnable). `punched_bytes` counts the bytes
/// punched so the test can assert the first slice reclaimed its prefix.
mod capfs {
    use crate::fs::{Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, StdFs};
    use crate::io;
    use core::sync::atomic::{AtomicU64, Ordering};
    use std::io::{Read as _, Seek as _, SeekFrom};
    use std::path::Path;
    use std::sync::Arc;

    #[derive(Clone)]
    pub(super) struct CapacityFs {
        available: Arc<AtomicU64>,
        punched: Arc<AtomicU64>,
        /// Link count `hard_link_count` reports for every path. `1` (the
        /// default) is an exclusively-owned file; `2` models an inode a
        /// checkpoint has hard-linked, which in-place reclaim must not mutate.
        link_count: Arc<AtomicU64>,
    }

    impl CapacityFs {
        pub(super) fn new() -> Self {
            Self {
                available: Arc::new(AtomicU64::new(u64::MAX)),
                punched: Arc::new(AtomicU64::new(0)),
                link_count: Arc::new(AtomicU64::new(1)),
            }
        }

        /// Makes every file report `n` hard links (see [`Self::link_count`]).
        pub(super) fn set_link_count(&self, n: u64) {
            self.link_count.store(n, Ordering::Relaxed);
        }

        /// Sets the fixed free-space figure `available_space` reports (the
        /// simulated remaining disk).
        pub(super) fn set_available_space(&self, bytes: u64) {
            self.available.store(bytes, Ordering::Relaxed);
        }

        /// Total bytes passed to `punch_hole` so far.
        pub(super) fn punched_bytes(&self) -> u64 {
            self.punched.load(Ordering::Relaxed)
        }
    }

    impl Fs for CapacityFs {
        fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
            StdFs.open(path, opts)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.create_dir_all(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
            StdFs.read_dir(path)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_file(path)
        }
        fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
            StdFs.remove_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            StdFs.rename(from, to)
        }
        fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
            StdFs.metadata(path)
        }
        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            StdFs.sync_directory(path)
        }
        fn exists(&self, path: &Path) -> io::Result<bool> {
            StdFs.exists(path)
        }
        fn hard_link_count(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.link_count.load(Ordering::Relaxed))
        }
        fn backend_id(&self) -> Option<u64> {
            StdFs.backend_id()
        }
        fn volume_id(&self, path: &Path) -> Option<u64> {
            StdFs.volume_id(path)
        }

        // Simulated disk pressure: the fixed free-space figure the tight-space
        // admission check reads to decide a full rewrite will not fit.
        fn available_space(&self, _path: &Path) -> io::Result<u64> {
            Ok(self.available.load(Ordering::Relaxed))
        }

        // Advertise hole-punch so the compaction takes the punch-and-reclaim
        // path even on a platform whose real StdFs reports no support.
        fn capabilities(&self, path: &Path) -> FsCapabilities {
            FsCapabilities {
                punch_hole: true,
                ..StdFs.capabilities(path)
            }
        }

        // Emulate a hole-punch by zeroing the range in place: the prefix then
        // reads as zeros exactly as a real punch (`FALLOC_FL_KEEP_SIZE`) would,
        // so the restricted view is byte-faithful and the file keeps its length.
        // Count the bytes for the test's reclaim assertion.
        fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
            if len == 0 {
                return Ok(());
            }
            let mut f = StdFs.open(path, &FsOpenOptions::new().write(true))?;
            // Clamp to the bytes actually present from `offset` to EOF: a real
            // punch (`FALLOC_FL_KEEP_SIZE`) never extends the file, so neither
            // may this zero-fill emulation. The min-0 clamp IS the intended
            // semantics here (an out-of-range offset punches nothing), so a
            // saturating subtraction is correct rather than bug-masking.
            let file_len = f.metadata()?.len;
            let punch_len = len.min(file_len.saturating_sub(offset));
            if punch_len == 0 {
                return Ok(());
            }
            f.seek(SeekFrom::Start(offset))?;
            // Stream the zero bytes over the range (no manual chunk buffer, so
            // no indexing / cast / unwrap the crate lints forbid).
            std::io::copy(&mut std::io::repeat(0u8).take(punch_len), &mut f)?;
            f.sync_all()?;
            // Count the bytes only AFTER the punch durably lands, so a failed
            // open / seek / write / sync does not inflate the reclaim counter.
            self.punched.fetch_add(punch_len, Ordering::Relaxed);
            Ok(())
        }
    }
}

/// The shared tight-space crash-and-reopen flow. On `shared_fs`: writes 2000
/// keys, leaves the disk tight via `set_capacity(used)`, crashes the tight-space
/// compaction right after its first slice is installed and punched (asserting
/// the crash failpoint fired and `punched_bytes()` grew), then reopens so
/// recovery rebuilds the restricted input, returning the reopened tree. Callers
/// supply the filesystem and its capacity / reclaim accessors (a `MemFs` and a
/// real-file `CapacityFs` configure these differently) and then run their own
/// assertions on the returned tree.
fn tight_space_crash_and_reopen(
    dir: &std::path::Path,
    shared_fs: Arc<dyn crate::fs::Fs>,
    set_capacity: impl FnOnce(u64),
    punched_bytes: impl Fn() -> u64,
) -> crate::Result<crate::AnyTree> {
    use core::sync::atomic::Ordering;

    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared_fs));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    set_capacity(used);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, 0).is_err(),
        "the crash failpoint must abort the tight-space compaction",
    );
    // The failpoint disarms itself when it fires: confirm the error came from the
    // intended crash point, not an unrelated failure before the punch.
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the crash failpoint must have fired and disarmed",
    );
    assert!(
        punched_bytes() > 0,
        "the first slice must have punched before the crash",
    );

    drop(tree);
    Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(shared_fs)
    .open()
}

/// A legitimately RESTRICTED table (a tight-space compaction crashed after
/// punching its first slice, so its `[0, punch)` data-block prefix reads as
/// zeros) must pass every heal-reconcile security gate AND the whole-file
/// integrity verify. Each gate walks the data blocks and would, before
/// restriction-awareness, try to decode the punched prefix and FALSELY reject
/// the healthy restricted view, refusing to reconcile a legitimate heal and
/// stranding recovery. Cross-checks the suffix only; the punched prefix is
/// dead. This is the reopen state of
/// [`tight_space_crash_after_first_slice_recovers_all_keys_on_reopen`], run on
/// real on-disk files ([`capfs::CapacityFs`] over `StdFs`) so the scan exercises
/// real filesystem read / seek / EOF behavior.
#[test]
fn restricted_view_passes_every_reconcile_gate() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = capfs::CapacityFs::new();
    // Leave only a quarter of the flushed footprint free so a full rewrite cannot
    // fit and the compaction takes the tight-space slice-and-punch path.
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(fs.clone()),
        |used| fs.set_available_space(used / 4),
        || fs.punched_bytes(),
    )?;

    // The whole-file digest verify must pass on the restricted table: it streams
    // the SUFFIX digest for a restricted view (the punched prefix is excluded),
    // so a legitimately punched file is not flagged as corrupt. Now testable
    // because the table lives in a real on-disk file `verify_integrity` can read.
    let integrity = crate::verify::verify_integrity(&reopened);
    assert!(
        integrity.is_ok(),
        "verify_integrity must pass on a legitimately restricted table, got {:?}",
        integrity.errors,
    );

    // Locate the restricted table and drive every heal-reconcile gate directly:
    // each must accept the healthy suffix without decoding the punched prefix.
    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the punched input must reopen as a restricted table");
    };

    restricted.verify_kv_checksums()?;
    restricted.verify_blob_links()?;
    restricted.verify_tli_mirrors()?;
    restricted.verify_seqno_bounds()?;
    restricted.verify_block_entry_counts()?;
    restricted.verify_zone_map()?;
    restricted.verify_locator()?;
    restricted.verify_filter(None)?;
    restricted.verify_block_layout()?;
    restricted.verify_point_read_reachability()?;
    restricted.verify_metadata_bounds(false)?;
    Ok(())
}

/// Detaching a tight-space-restricted SST for an in-place heal (it was hard-linked
/// into a checkpoint) must reproduce the source BYTE-FOR-BYTE. The detach punches
/// only the reclaimed DATA-block extents below the frontier — the exact set
/// `Inner::drop` reclaims (the block index yields only data-block handles) — and
/// copies everything else, so any live index / filter block interleaved below the
/// frontier is preserved rather than zeroed by a wholesale `[0, punch)` punch. This
/// guards the scatter-copy against dropping or mis-placing a live block.
#[cfg(feature = "page_ecc")]
#[test]
fn unshare_for_heal_reproduces_the_source_faithfully() -> crate::Result<()> {
    use crate::fs::{Fs, FsOpenOptions, SyncMode};

    let dir = tempfile::tempdir()?;
    let fs = capfs::CapacityFs::new();
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(fs.clone()),
        |used| fs.set_available_space(used / 4),
        || fs.punched_bytes(),
    )?;

    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the punched input must reopen as a restricted table");
    };
    assert!(
        restricted.punch_offset()? > 0,
        "the restricted table has a punched prefix",
    );

    let shared: Arc<dyn Fs> = Arc::new(fs);
    let read_all = |path: &std::path::Path| -> crate::Result<alloc::vec::Vec<u8>> {
        let f = shared.open(path, &FsOpenOptions::new().read(true))?;
        let len = usize::try_from(f.metadata()?.len).unwrap_or(usize::MAX);
        let mut buf = alloc::vec![0u8; len];
        let mut off = 0usize;
        while off < len {
            let got = f.read_at(
                buf.get_mut(off..).unwrap_or(&mut []),
                u64::try_from(off).unwrap_or(u64::MAX),
            )?;
            if got == 0 {
                break;
            }
            off += got;
        }
        Ok(buf)
    };

    // Snapshot the source before the detach. It is a real punched restricted table:
    // its bytes hold BOTH punched data blocks (zeros) and live blocks (non-zero).
    let src = read_all(&restricted.path)?;
    assert!(src.contains(&0), "the source has punched data blocks");
    assert!(src.iter().any(|&b| b != 0), "the source has live blocks");

    // Detach into a fresh copy, exactly as the in-place heal does for a
    // checkpoint-shared inode.
    let source = shared.open(&restricted.path, &FsOpenOptions::new().read(true))?;
    let _copy = match restricted.unshare_for_heal(source.as_ref(), SyncMode::Normal) {
        Ok(copy) => copy,
        Err(e) => panic!("unshare_for_heal must succeed on a restricted table: {e}"),
    };

    // The copy the rename published must be byte-identical to the source.
    let copy = read_all(&restricted.path)?;
    assert_eq!(
        copy, src,
        "the detached heal copy must reproduce the source byte-for-byte",
    );
    Ok(())
}

/// A REAL tight-space compaction must record its punched input's exact bound in a
/// `.restrict-bound` sidecar before punching, WITHOUT touching the SST. That
/// sidecar bound must equal the manifest's restriction lower bound — the invariant
/// that lets manifest repair recover the same restriction from the on-disk files
/// alone. Reading it back also proves the sidecar survives the reopen.
#[test]
fn tight_space_writes_a_restrict_bound_sidecar_matching_the_manifest_bound() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let fs = capfs::CapacityFs::new();
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(fs.clone()),
        |used| fs.set_available_space(used / 4),
        || fs.punched_bytes(),
    )?;

    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the punched input must reopen as a restricted table");
    };
    let Some(manifest_bound) = restricted.restrict_lower_bound().cloned() else {
        panic!("restricted table has a manifest bound");
    };

    // The compaction published the exact bound to the SST's `.restrict-bound`
    // sidecar; it must read back as the manifest bound.
    match crate::restrict_bound::read(&fs, &restricted.path, None)? {
        crate::restrict_bound::SidecarRead::Present(_id, bound) => {
            assert_eq!(
                bound.as_slice(),
                manifest_bound.as_ref(),
                "the sidecar bound must equal the manifest restriction bound",
            );
        }
        _ => panic!("a punched table must carry a valid .restrict-bound sidecar"),
    }
    Ok(())
}

/// The `.restrict-bound` sidecar is written STRICTLY AFTER the slice's version
/// install commits, so a fault on that (post-commit) write is NOT fatal: the
/// restriction is already durable in the manifest. The slice logs the failure and
/// leaves that input UNPUNCHED (never punching an input whose sidecar did not
/// land, which would force a later repair to derive a conservative bound and drop
/// a live block), so the compaction still SUCCEEDS and a reopen reads every key at
/// its latest value — no data loss, no resurrection.
#[test]
fn tight_space_sidecar_write_fault_is_nonfatal_and_recovers() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let fault = FaultFs::new(capfs.clone());
    let injector = fault.injector();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(fault);

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared));
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    // Leave only a quarter of the footprint free so a full rewrite cannot fit and
    // the compaction takes the tight-space slice-and-punch path, then fail EVERY
    // `.restrict-bound` sidecar write (post-commit under commit-then-mark) so no
    // restricted input is punched.
    capfs.set_available_space(used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).on_path("restrict-bound"),
    );

    // The sidecar fault is post-commit, so the compaction SUCCEEDS (it does not
    // roll back a committed slice). Space is quartered, so a normal rewrite cannot
    // fit — a `Merged` action proves the tight-space slice-and-punch path engaged
    // (a plain merge would have been skipped for lack of headroom, never reaching
    // the faulted `.restrict-bound` write).
    let result = match tree.major_compact(64 * 1024 * 1024, 0) {
        Ok(r) => r,
        Err(e) => panic!("a post-commit sidecar-write fault must not fail the compaction: {e:?}"),
    };
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Merged,
        "tight-space must have engaged and merged, got {result:?}",
    );
    // Every restricted input whose sidecar write faulted must stay UNPUNCHED: a
    // punched prefix with no sidecar would force a later repair to derive a
    // conservative bound and drop up to one live block. With every sidecar write
    // faulted, no input is punched at all.
    assert_eq!(
        capfs.punched_bytes(),
        0,
        "an input whose restrict-bound sidecar failed to land must stay unpunched",
    );

    // Correctness: reopen and read every key at its latest value. The manifest
    // committed the restriction, so the unpunched-and-sidecarless input's redundant
    // prefix is routed to the installed output — nothing is lost or resurrected.
    drop(tree);
    let reopened = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..TIGHT_SPACE_KEYS {
        let got = reopened.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?;
        assert_eq!(
            got.as_deref(),
            Some(&[0xCDu8; 64][..]),
            "key {i} must read its latest value after the sidecar-fault reopen",
        );
    }
    Ok(())
}

/// A tight-space slice must apply NO removal semantics: its output must be a
/// SUPERSET that shadows any surviving input prefix in every crash window (a
/// sidecar write that fails post-commit deliberately leaves the input
/// unpunched, and a manifest-loss repair then republishes that prefix whole).
/// Bottommost GC is already deferred for this reason; the user compaction
/// filter must be deferred the same way — a record the filter removed from the
/// slice output survives nowhere else, so the crash window would resurrect it.
/// The filtering is not lost, only deferred: the next normal compaction over
/// the output applies it.
#[test]
fn tight_space_slice_defers_the_compaction_filter() -> crate::Result<()> {
    use crate::compaction::filter::{
        CompactionFilter, Context as FilterContext, Factory, ItemAccessor, Verdict,
    };

    let victim = tight_space_key(100);

    struct DropVictim(Vec<u8>);
    impl CompactionFilter for DropVictim {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &FilterContext,
        ) -> crate::Result<Verdict> {
            if item.key().as_ref() == self.0.as_slice() {
                Ok(Verdict::Destroy)
            } else {
                Ok(Verdict::Keep)
            }
        }
    }
    struct DropVictimFactory(Vec<u8>);
    impl Factory for DropVictimFactory {
        fn name(&self) -> &'static str {
            "drop-victim"
        }
        fn make_filter(&self, _ctx: &FilterContext) -> Box<dyn CompactionFilter> {
            Box::new(DropVictim(self.0.clone()))
        }
    }

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(capfs.clone());
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_compaction_filter_factory(Some(Arc::new(DropVictimFactory(
        victim.clone().into_bytes(),
    ))))
    .with_shared_fs(Arc::clone(&shared));
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    capfs.set_available_space(used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    let result = tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Merged,
        "tight-space must have engaged and merged, got {result:?}",
    );

    assert!(
        tree.get(victim.as_bytes(), crate::MAX_SEQNO)?.is_some(),
        "the tight-space slice must DEFER the compaction filter (its output \
         must be a superset shadowing any surviving input prefix); the filter \
         applies on the next normal compaction instead",
    );
    Ok(())
}

/// A last-level tight-space slice defers bottommost GC (tombstone drop, RT
/// application, seqno zeroing), so its output is a SUPERSET of every input's
/// consumed prefix. Pins the resurrection-safety consequence end to end: a
/// post-commit sidecar write failure leaves an input unpunched with no
/// sidecar, a manifest-loss repair then republishes that input WHOLE — and a
/// key whose tombstone lived in a fully-consumed (deleted) sibling input must
/// still read as deleted, because the deferred-GC output retains the
/// tombstone and shadows the resurrected prefix.
#[test]
fn tight_space_sidecar_fault_must_not_resurrect_evicted_deletes() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let fault = FaultFs::new(capfs.clone());
    let injector = fault.injector();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(fault);

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared));
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // One big table holding every key, then a tiny sibling holding ONLY the
    // tombstone of an early key: the slice fully consumes the sibling (its
    // sole key sits below the first boundary, so the file is deleted) while
    // the big input becomes a restricted survivor.
    let victim = tight_space_key(100);
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    tree.remove(victim.as_bytes(), TIGHT_SPACE_KEYS + 1);
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    // Tight space + every sidecar write faulted, exactly like the non-fatal
    // sidecar-fault test above — but here the last-level merge EVICTS the
    // tombstone, so the unpunched-prefix fallback is not available.
    capfs.set_available_space(used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).on_path("restrict-bound"),
    );
    let result = tree.major_compact(64 * 1024 * 1024, TIGHT_SPACE_KEYS + 2)?;
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Merged,
        "tight-space must have engaged and merged, got {result:?}",
    );
    injector.clear();

    // The delete must hold on the live tree.
    assert_eq!(
        tree.get(victim.as_bytes(), crate::MAX_SEQNO)?,
        None,
        "the tombstoned key is gone on the live tree",
    );

    // Lose the manifest, repair, reopen: the deleted key must STAY deleted.
    drop(tree);
    for e in shared.read_dir(dir.path())? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || e.file_name == "current" {
            shared.remove_file(&e.path)?;
        }
    }
    Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .repair()?;
    let reopened = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    assert_eq!(
        reopened.get(victim.as_bytes(), crate::MAX_SEQNO)?,
        None,
        "a tombstone evicted by the last-level slice must never resurrect \
         through an unpunched, sidecarless input after a manifest-loss repair",
    );
    Ok(())
}

/// The `.restrict-bound` sidecar is written STRICTLY AFTER the version install
/// commits, so an install FAILURE leaves NO sidecar (nothing to retract) and the
/// slice's finalized outputs must be rolled back at once (marked deleted, so their
/// blocks free on drop) rather than orphaned to pin the scarce space until the
/// next open's sweep. Faulting the edit-log append (the install's durable commit)
/// must abort with that fault, leave no full-size orphan output, and leave no
/// `.restrict-bound` sidecar behind.
#[test]
fn tight_space_install_failure_rolls_back_outputs_and_leaves_no_sidecar() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let fault = FaultFs::new(capfs.clone());
    let injector = fault.injector();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(fault);

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared));
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    let tables_dir = dir.path().join(crate::file::TABLES_FOLDER);
    let numeric_files = || -> Vec<(std::ffi::OsString, u64)> {
        std::fs::read_dir(&tables_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .bytes()
                    .all(|b| b.is_ascii_digit())
            })
            .filter_map(|e| e.metadata().ok().map(|m| (e.file_name(), m.len())))
            .collect()
    };
    let inputs: crate::HashSet<std::ffi::OsString> =
        numeric_files().into_iter().map(|(name, _)| name).collect();
    assert!(
        !inputs.is_empty(),
        "the flush produced at least one input SST"
    );
    let sidecar_count = || -> usize {
        std::fs::read_dir(&tables_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".restrict-bound"))
            .count()
    };

    capfs.set_available_space(used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    // Fault the slice's version install: the edit-log append is its durable commit.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("edits")
            .once(),
    );

    // Pin the abort to the INJECTED fault: a bare `is_err()` would also pass if the
    // tight-space path stopped engaging or the admission gate refused the compaction
    // before any output was finalized, silently not exercising the rollback.
    let result = tree.major_compact(64 * 1024 * 1024, 0);
    assert!(
        matches!(&result, Err(crate::Error::Io(e)) if e.kind() == ErrorKind::Other),
        "the tight-space compaction must abort with the injected install fault, got {result:?}",
    );

    // No NEW full-size table file may survive: the aborted slice's finalized outputs
    // were rolled back, not orphaned, so the scarce space is freed at once.
    for (name, len) in numeric_files() {
        assert!(
            len == 0 || inputs.contains(&name),
            "a finalized slice output ({name:?}, {len} bytes) was orphaned instead of \
             rolled back after the install failed",
        );
    }
    // The sidecar write is post-install, so an install failure leaves none behind:
    // there is no uncommitted boundary a later repair could honor.
    assert_eq!(
        sidecar_count(),
        0,
        "an install failure must leave no `.restrict-bound` sidecar (it is written \
         only after the install commits)",
    );
    Ok(())
}

/// A tight-space slice must NOT garbage-collect a tombstone whose deleted key also
/// lives in a SURVIVING (restricted) input's consumed prefix. If it did, a crash
/// window that leaves the survivor unrestricted (sidecar not written, prefix not
/// punched) would let manifest repair — which rebuilds from `tables/` and treats
/// the survivor as a full table — re-expose the deleted key: the tombstone-bearing
/// sibling was fully consumed and, with bottommost GC, the compacted output dropped
/// BOTH records. Tight-space slice outputs keep every record (GC is deferred to a
/// later normal compaction), so the output still shadows the survivor's prefix and
/// the deleted key stays deleted.
#[test]
fn tight_space_slice_retains_a_tombstone_for_an_unrestricted_repair_survivor() -> crate::Result<()>
{
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let fault = FaultFs::new(capfs.clone());
    let injector = fault.injector();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(fault);

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // Input B: every key as a live value — the broad input that survives the first
    // slice (restricted), with the deleted key in its consumed prefix.
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    // Input T: a tombstone for the FIRST key (smallest, so it lands in the first
    // slice's consumed prefix and T is fully consumed). Newer than B's value.
    let deleted = tight_space_key(0);
    tree.remove(deleted.as_bytes(), TIGHT_SPACE_KEYS);
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;

    capfs.set_available_space(used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    // Fault EVERY restrict-bound sidecar write so the surviving input is left with no
    // sidecar and unpunched, then crash right after the first slice installs so the
    // survivor is not consumed by the tail. Together: a committed restriction with no
    // recoverable bound over a still-full input — exactly #40's window.
    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::Other)).on_path("restrict-bound"),
    );
    failpoint.store(true, Ordering::SeqCst);

    // A watermark above every live seqno so bottommost GC would (without the fix)
    // drop the consumed tombstone.
    assert!(
        tree.major_compact(64 * 1024 * 1024, TIGHT_SPACE_KEYS + 1)
            .is_err(),
        "the crash failpoint must abort the tight-space compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint must have fired after the first slice",
    );
    assert_eq!(
        capfs.punched_bytes(),
        0,
        "the faulted-sidecar survivor must be left unpunched",
    );

    // Rebuild the manifest from `tables/`: the survivor has no sidecar and is
    // unpunched, so repair recovers it UNRESTRICTED. Clear the fault first so the
    // rebuild's own I/O is not faulted.
    injector.clear();
    drop(tree);
    Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .repair_with_salvage(true)?;

    let reopened = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(shared)
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    assert!(
        reopened
            .get(deleted.as_bytes(), crate::MAX_SEQNO)?
            .is_none(),
        "a tombstone consumed by a tight-space slice must not be GC'd away, or manifest \
         repair of the unrestricted survivor resurrects the deleted key",
    );
    Ok(())
}

/// A KV-separated tight-space compaction that RELOCATES a fragmented blob
/// file in slices and crashes after the first slice must reopen consistently:
/// the relocated entries (now in fresh compact files referenced by the
/// installed slice output) AND the not-yet-relocated entries (still in the
/// punched stale file's intact suffix) must all read their latest value.
#[test]
fn tight_space_blob_relocation_crash_after_first_slice_recovers_all_keys() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 4_000;
    let k = |i: u64| format!("key{i:08}");
    // High-entropy (xorshift) values so the blobs do NOT compress away: the
    // relocation transient must be real for the space gate to skip the full
    // merge and engage the slicing path. Deterministic per (key, generation)
    // so the post-reopen assertion can regenerate the expected bytes. Odd keys
    // keep their first-generation value (a relocated live blob); even keys are
    // overwritten (their first-gen blob is dead → in-file fragmentation).
    let val = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "xorshift byte extraction; the high bits are intentionally dropped"
                )]
                let byte = (s >> 24) as u8;
                byte
            })
            .collect()
    };

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            // Keep every stale file (default age_cutoff 0.25 would drain a
            // small candidate set to empty) and treat a lightly-dead file as
            // stale so the half-shadowed first generation is relocated.
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            // Small blob files → several per generation, so relocation has
            // multiple stale files and the merge slices across them.
            .file_target_size(48 * 1024),
    ));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // Generation 1: every key → a blob.
    for i in 0..N {
        tree.insert(k(i).as_bytes(), val(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Generation 2: overwrite EVEN keys only, interleaved so every gen-1 blob
    // file ends up ~half dead (stale, not fully dead → eligible to relocate).
    for i in (0..N).step_by(2) {
        tree.insert(k(i).as_bytes(), val(i, 2), N + i);
    }
    tree.flush_active_memtable(0)?;

    // Blob fragmentation is only LEARNED during a merge (the drop callback
    // records each shadowed gen-1 blob as dead). Run one ample-space merge
    // first so the even-key gen-1 blobs are counted dead, leaving every gen-1
    // file ~half stale — the precondition for the next merge to RELOCATE them.
    // It also collapses the index SSTs to the bottom level. The watermark sits
    // above every live seqno so the merge actually folds the shadowed entries
    // (seqno 0 keeps all MVCC versions and records no fragmentation).
    let gc_watermark = 4 * N;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;

    // Cap so the full relocation of the now-stale generation cannot fit,
    // forcing the gate to skip and the tight loop to relocate in slices.
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    // Crash right after the first relocated slice is durably installed +
    // punched.
    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, gc_watermark).is_err(),
        "the crash failpoint must abort the relocating tight-space compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed",
    );
    assert!(
        mem.punched_bytes() > 0,
        "the first relocated slice must have punched a stale blob prefix",
    );

    // Reopen on the same simulated disk and verify every key reads its latest
    // value: odd keys = relocated gen-1 blob, even keys = gen-2 blob.
    drop(tree);
    let reopened = match Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .with_shared_fs(Arc::new(mem))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..N {
        let expected = if i % 2 == 0 { val(i, 2) } else { val(i, 1) };
        assert_eq!(
            reopened.get(k(i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after a crash mid blob-relocation + reopen",
        );
    }
    Ok(())
}

/// A restricted-blob reopen failure mid-slice — after `run_subcompaction`
/// finalized the slice's output SSTs and blob files, before the install
/// references them — must ROLL BACK those outputs like every other
/// pre-install failure. Leaking them until the next open's orphan sweep pins
/// disk space under the exact condition that triggered tight-space in the
/// first place: scarce free space.
#[test]
fn tight_space_blob_reopen_failure_rolls_back_the_slice_outputs() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 4_000;
    let k = |i: u64| format!("key{i:08}");
    let val = |i: u64, generation: u8| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..200u32)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "xorshift byte extraction; the high bits are intentionally dropped"
                )]
                let byte = (s >> 24) as u8;
                byte
            })
            .collect()
    };

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(mem.clone());
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::clone(&shared))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(48 * 1024),
    ));
    let failpoint = config.fail_tight_blob_reopen.clone();
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // Same fragmented-generation setup as the crash-after-first-slice test:
    // gen 1 everywhere, gen 2 over the even keys, one ample-space merge to
    // learn the fragmentation.
    for i in 0..N {
        tree.insert(k(i).as_bytes(), val(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..N).step_by(2) {
        tree.insert(k(i).as_bytes(), val(i, 2), N + i);
    }
    tree.flush_active_memtable(0)?;
    let gc_watermark = 4 * N;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    // Snapshot the on-disk file sets, then fail the FIRST slice's blob reopen.
    let list_names = |folder: &std::path::Path| -> crate::Result<Vec<String>> {
        let mut names: Vec<String> = if shared.exists(folder)? {
            shared
                .read_dir(folder)?
                .into_iter()
                .map(|e| e.file_name)
                .collect()
        } else {
            Vec::new()
        };
        names.sort();
        Ok(names)
    };
    let tables_before = list_names(&dir.path().join("tables"))?;
    let blobs_before = list_names(&dir.path().join("blobs"))?;

    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, gc_watermark).is_err(),
        "the injected blob-reopen failure must abort the compaction",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired and disarmed",
    );

    // The failed slice committed nothing of its own, but EARLIER merges /
    // slices of the same compaction may have legitimately installed before the
    // failpoint fired (their outputs are referenced by the current version,
    // their consumed inputs deleted), so exact before/after file-set equality
    // is interleaving-dependent. The leak-free invariant is: every file that
    // APPEARED since the snapshot must be REFERENCED by the current version —
    // the aborted slice's retracted outputs are neither pre-existing nor
    // referenced, so a missed rollback still fails this check.
    let version = tree.current_version();
    let referenced_tables: Vec<String> =
        version.iter_tables().map(|t| t.id().to_string()).collect();
    let referenced_blobs: Vec<String> = version
        .blob_files
        .iter()
        .map(|bf| bf.id().to_string())
        .collect();
    for name in list_names(&dir.path().join("tables"))? {
        assert!(
            tables_before.contains(&name) || referenced_tables.contains(&name),
            "leaked table file {name}: neither pre-existing nor referenced by \
             the current version (rollback missed it)",
        );
    }
    for name in list_names(&dir.path().join("blobs"))? {
        assert!(
            blobs_before.contains(&name) || referenced_blobs.contains(&name),
            "leaked blob file {name}: neither pre-existing nor referenced by \
             the current version (rollback missed it)",
        );
    }
    Ok(())
}

/// A range tombstone fully below the GC watermark must be applied during a
/// last-level compaction: its covered keys are physically dropped AND the
/// tombstone itself is GC'd. If the keys were only suppressed (not dropped)
/// while the tombstone was GC'd, they would resurrect — so a `None` read
/// after GC proves both the physical drop (#1) and the tombstone GC (#2).
///
/// Routed through the atomic sub-compaction path (which is where GC runs):
/// `compaction_threads > 1` + `subcompaction_min_bytes = 0` + a populated
/// bottom level (split boundaries) make the final compaction split.
#[test]
fn last_level_applies_and_gcs_below_watermark_range_tombstone() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    let val = |i: u64| format!("v{i}-{}", "x".repeat(40));

    // Step 1: populate the bottom level with several tables (split
    // boundaries the final compaction can partition on).
    for i in 0..200u64 {
        tree.insert(key(i), val(i), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Delete [k0000, k0050) at seqno 1000 and overwrite the rest into L0, so
    // the next compaction merges L0 into the populated bottom and splits.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0050"),
        1000,
    );
    for i in 50..200u64 {
        tree.insert(key(i), val(i), 1001 + i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to the bottom with a watermark (5000) above the tombstone:
    // covered keys are physically dropped and the tombstone is GC'd.
    tree.major_compact(u64::MAX, 5000)?;

    for i in 0..50u64 {
        assert_eq!(
            tree.get(key(i), crate::MAX_SEQNO)?,
            None,
            "covered key {} must be physically gone after GC",
            key(i),
        );
    }
    for i in 50..200u64 {
        assert!(
            tree.get(key(i), crate::MAX_SEQNO)?.is_some(),
            "uncovered key {} must survive",
            key(i),
        );
    }
    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        remaining.is_empty(),
        "a fully-applied below-watermark tombstone must be GC'd, found {remaining:?}",
    );
    Ok(())
}

/// An above-watermark tombstone must be retained, not GC'd: read-time
/// application still needs it for snapshots that predate the tombstone.
#[test]
fn above_watermark_range_tombstone_is_retained() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    for i in 0..50u64 {
        tree.insert(key(i), "v", i);
    }
    tree.flush_active_memtable(0)?;

    // Tombstone at seqno 100; compact with a watermark (50) BELOW it, so the
    // tombstone is neither applied nor GC'd.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0025"),
        100,
    );
    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 50)?;

    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        !remaining.is_empty(),
        "an above-watermark tombstone must be retained, not GC'd",
    );
    Ok(())
}

/// A range tombstone whose seqno equals the GC watermark sits exactly on the
/// visibility boundary. RT visibility is strict (`visible_at` is `seqno <
/// read_seqno`), so the oldest live snapshot reading at `read_seqno ==
/// watermark` does NOT see `RT@watermark`. Compaction must therefore neither
/// apply it (physically dropping covered keys) nor GC it — doing either one
/// compaction too early makes a key that is still visible at the watermark
/// disappear. Reading the covered key at `read_seqno == watermark` (where the
/// tombstone is invisible but the key is committed) must still return it.
#[test]
fn range_tombstone_at_exact_watermark_is_not_applied_or_gced() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_threads(4)
    .subcompaction_min_bytes(0)
    .open()?;

    let key = |i: u64| format!("k{i:04}");
    let val = |i: u64| format!("v{i}-{}", "x".repeat(40));

    // Populate the bottom level (split boundaries for the final compaction).
    // Covered keys live here at low seqnos (< the watermark).
    for i in 0..200u64 {
        tree.insert(key(i), val(i), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(4_096, 0)?;

    // Delete [k0000, k0050) at seqno 1000 and overwrite the rest into L0.
    tree.remove_range(
        crate::UserKey::from("k0000"),
        crate::UserKey::from("k0050"),
        1000,
    );
    for i in 50..200u64 {
        tree.insert(key(i), val(i), 1001 + i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to the bottom with the watermark set EXACTLY to the tombstone's
    // seqno. At this boundary the tombstone is invisible to a read at the
    // watermark, so its covered keys must be preserved, not dropped.
    tree.major_compact(u64::MAX, 1000)?;

    // Read at read_seqno == watermark: RT@1000 is invisible here
    // (`1000 < 1000` is false), and each covered key was committed at
    // seqno < 1000, so it must still be visible.
    for i in 0..50u64 {
        assert_eq!(
            tree.get(key(i), 1000)?.as_deref(),
            Some(val(i).as_bytes()),
            "covered key {} must survive: RT@watermark is invisible at read==watermark",
            key(i),
        );
    }

    // The boundary tombstone must also be retained (not GC'd one compaction
    // early), since snapshots at the watermark still rely on it.
    let remaining = super::collect_version_tombstones(&tree.current_version());
    assert!(
        !remaining.is_empty(),
        "a tombstone at the exact watermark must be retained, not GC'd",
    );
    Ok(())
}

#[test]
fn compaction_stream_run_not_found() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    assert!(
        create_compaction_stream(
            &tree.current_version(),
            &[666],
            0,
            None,
            crate::comparator::default_comparator()
        )?
        .is_none()
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((0, 2)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[0, 1, 2],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_2() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((0, 0)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[0],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_3() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        Some((2, 2)),
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[2],
        )
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn compaction_stream_run_4() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        None,
        pick_run_indexes(
            tree.current_version()
                .level(0)
                .unwrap()
                .iter()
                .next()
                .unwrap(),
            &[4],
        )
    );

    Ok(())
}

#[test]
fn compaction_drop_tables() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.insert("b", "a", 1);
    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.insert("c", "a", 2);
    tree.flush_active_memtable(0)?;
    assert_eq!(3, tree.approximate_len());
    assert_eq!(0, tree.sealed_memtable_count());

    tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

    assert_eq!(0, tree.table_count());

    Ok(())
}

#[test]
fn blob_file_picking_simple() -> crate::Result<()> {
    struct InPlaceStrategy(Vec<TableId>);

    impl CompactionStrategy for InPlaceStrategy {
        fn get_name(&self) -> &'static str {
            "InPlaceCompaction"
        }

        fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Merge(Input {
                table_ids: self.0.iter().copied().collect(),
                dest_level: 6,
                target_size: 64_000_000,
                canonical_level: 6, // We don't really care - this compaction is only used for very specific unit tests
            })
        }
    }

    let folder = tempfile::tempdir()?;

    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1))
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(1)
            .age_cutoff(1.0)
            .staleness_threshold(0.01)
            .compression(crate::CompressionType::None),
    ))
    .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);
    tree.flush_active_memtable(1_000)?;
    assert_eq!(0, tree.sealed_memtable_count());
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    tree.major_compact(1, 1_000)?;
    assert_eq!(3, tree.table_count());
    assert_eq!(1, tree.blob_file_count());
    // We now have tables [1, 2, 3] pointing into blob file 0

    tree.drop_range("a"..="a")?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        assert_eq!(
            &{
                let mut map = crate::HashMap::default();
                map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                map
            },
            &**tree.current_version().gc_stats(),
        );
    }

    // Even though we are compacting table #2, blob file is not rewritten
    // because table #3 still points into it
    tree.compact(Arc::new(InPlaceStrategy(vec![2])), 1_000)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    {
        assert_eq!(
            &{
                let mut map = crate::HashMap::default();
                map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                map
            },
            &**tree.current_version().gc_stats(),
        );
    }

    // Because tables #3 & #4 both point into the blob file
    // Only selecting both for compaction will actually rewrite the file
    tree.compact(Arc::new(InPlaceStrategy(vec![3, 4])), 1_000)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    // Fragmentation is cleared up because blob file was relocated
    {
        assert_eq!(
            crate::HashMap::default(),
            **tree.current_version().gc_stats(),
        );
    }

    Ok(())
}

#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test asserts over known-good fixtures; failure surfaces via panic"
)]
#[test]
fn narrow_merge_candidates_for_full_run_are_adjacent_pairs_sorted_ascending() -> crate::Result<()> {
    // Build a single bottom-level run of several tables (small target size
    // forces table rotation), then enumerate the narrowing candidates of a
    // whole-run merge. The gate tries them in order, so the contract is:
    // every candidate is a run-adjacent pair, and they are sorted by combined
    // SST size ascending (smallest tried first).
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()?;
    for i in 0..3_000u64 {
        tree.insert(format!("k{i:08}"), "v".repeat(40), i);
    }
    tree.flush_active_memtable(0)?;
    // Small target size → the major compaction emits a run of several tables.
    tree.major_compact(16 * 1024, 0)?;

    let version = tree.current_version();
    let run = version
        .iter_levels()
        .flat_map(|level| level.iter())
        .find(|run| run.len() >= 3)
        .expect("a bottom-level run with >= 3 tables");
    let ordered: Vec<(TableId, u64)> = run.iter().map(|t| (t.id(), t.file_size())).collect();

    let payload = Input {
        table_ids: ordered.iter().map(|(id, _)| *id).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };

    let candidates = super::narrow_merge_candidates(&version, &payload);

    // One candidate per run-adjacent pair, each exactly two tables on the
    // payload's destination.
    assert_eq!(
        candidates.len(),
        ordered.len() - 1,
        "one candidate per run-adjacent pair"
    );
    for c in &candidates {
        assert_eq!(c.table_ids.len(), 2, "each candidate is an adjacent pair");
        assert_eq!(c.dest_level, 6, "destination preserved");
    }

    let combined = |c: &Input| -> u64 {
        c.table_ids
            .iter()
            .filter_map(|id| version.get_table(*id))
            .map(Table::file_size)
            .sum()
    };
    let sums: Vec<u64> = candidates.iter().map(combined).collect();

    // Sorted ascending: the gate tries the smallest-Σ pair first, then larger
    // ones (a larger pair with fewer blob rewrites can fit where the smallest
    // does not).
    let mut sorted = sums.clone();
    sorted.sort_unstable();
    assert_eq!(sums, sorted, "candidates sorted ascending by SST size");

    // The first candidate is the smallest-Σ run-adjacent pair.
    let smallest_pair = ordered
        .windows(2)
        .map(|w| w[0].1 + w[1].1)
        .min()
        .expect(">= 2 tables");
    assert_eq!(sums[0], smallest_pair, "smallest-Σ pair is tried first");

    Ok(())
}

#[test]
fn space_fits_two_layer_combines_shared_volume_outputs_and_separates_routed_ones()
-> crate::Result<()> {
    use crate::fs::MemFs;

    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir()?;

    // Single volume (no routes): the SST tables folder and the blobs folder
    // share the primary filesystem, so the transient peak is their SUM on one
    // volume. An empty MemFs reports `capacity` free.
    let cfg = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::with_capacity(100 * MIB)));

    // 60 + 60 = 120 MiB > 100 MiB free → rejected, even though each output
    // fits the volume alone. This is the single-volume over-admission the
    // two-layer model prevents (checking sst and blob independently would
    // wrongly admit it).
    assert!(
        !super::space_fits_two_layer(&cfg, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "shared-volume outputs must be summed, not checked independently"
    );
    // 60 + 30 = 90 MiB (+1 MiB reserve) ≤ 100 MiB → admitted.
    assert!(super::space_fits_two_layer(
        &cfg,
        u64::MAX,
        60 * MIB,
        6,
        30 * MIB
    ));

    // Layer 1 (logical quota) caps the total regardless of physical free:
    // 50 + 40 = 90 MiB exceeds an 80 MiB quota headroom.
    assert!(!super::space_fits_two_layer(
        &cfg,
        80 * MIB,
        50 * MIB,
        6,
        40 * MIB
    ));

    // Routed to a PROVEN-independent volume: level 6 lives on its own MemFs
    // (a distinct volume id), blobs on the primary MemFs. The two outputs are
    // checked independently — 60 MiB on each of two 100 MiB volumes fits, even
    // though the sum is 120 MiB (a full cold-tier route must not stall a hot
    // merge).
    let routed = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(MemFs::with_capacity(100 * MIB)))
    .level_routes(vec![crate::config::LevelRoute {
        levels: 6..7,
        path: crate::path::PathBuf::from("/cold-tier"),
        fs: Arc::new(MemFs::with_capacity(100 * MIB)),
    }]);
    assert!(
        super::space_fits_two_layer(&routed, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "proven-independent volumes are checked independently"
    );
    // A blob output that overflows the primary volume alone still fails.
    assert!(!super::space_fits_two_layer(
        &routed,
        u64::MAX,
        60 * MIB,
        6,
        130 * MIB
    ));

    // Routed but NOT proven independent: the route points at the SAME backend
    // as the primary (one shared MemFs → one volume id / one free-space pool),
    // as happens when level_routes maps a level to a directory on the same
    // mount. The SST and blob budgets must combine, so 60 + 60 = 120 MiB > 100
    // MiB free is rejected even though each fits alone — the routed
    // over-admission guard.
    // ONE `Arc<MemFs>` reused for both config slots so the primary and the
    // route are unambiguously the same backend (one volume id / one
    // free-space pool), the not-proven-independent case.
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(MemFs::with_capacity(100 * MIB));
    let routed_same_mount = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::clone(&shared))
    .level_routes(vec![crate::config::LevelRoute {
        levels: 6..7,
        path: crate::path::PathBuf::from("/same-mount-subdir"),
        fs: Arc::clone(&shared),
    }]);
    assert!(
        !super::space_fits_two_layer(&routed_same_mount, u64::MAX, 60 * MIB, 6, 60 * MIB),
        "a route on the same volume must combine budgets, not admit each independently"
    );

    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "test asserts over known-good fixtures; failure surfaces via panic"
)]
#[test]
fn space_gate_for_merge_narrows_a_full_run_that_exceeds_free() -> crate::Result<()> {
    use crate::fs::MemFs;

    // Build a multi-table bottom-level run on a capped simulated disk, then
    // ask the gate to admit a whole-run merge whose transient output does NOT
    // fit free space but a run-adjacent pair does. The gate must narrow rather
    // than skip — exercising the per-payload demand, the candidate loop, and
    // the `Narrowed` return that integration tests cannot reach (the public
    // major-compaction path picks a non-narrowable multi-level merge).
    let dir = tempfile::tempdir()?;
    let mem = MemFs::with_capacity(u64::MAX);
    let any = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()?;
    let crate::AnyTree::Standard(tree) = any else {
        panic!("expected Standard tree");
    };
    for i in 0..3_000u64 {
        tree.insert(format!("k{i:08}"), "v".repeat(40), i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(16 * 1024, 0)?;

    let version = tree.current_version();
    let run = version
        .iter_levels()
        .flat_map(|level| level.iter())
        .find(|run| run.len() >= 3)
        .expect("a bottom-level run with >= 3 tables");
    let run_sigma: u64 = run.iter().map(Table::file_size).sum();
    let payload = Input {
        table_ids: run.iter().map(Table::id).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };

    // Free space below the full run's Σ but above a single pair: the run does
    // not fit, a run-adjacent pair does. Calibrate against the SIMULATED
    // disk's real stored bytes (manifest / WAL count too, not just live SSTs),
    // since the gate probes `available_space`, not the version footprint.
    // `run_sigma >= 1` (real SST files), so `- 1` cannot underflow.
    let probe_capacity = 1u64 << 40;
    mem.set_capacity(probe_capacity);
    let stored =
        probe_capacity - crate::fs::Fs::available_space(&mem, dir.path()).unwrap_or(probe_capacity);
    mem.set_capacity(stored + run_sigma - 1);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;

    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    match super::space_gate_for_merge(&version, &opts, &payload)? {
        super::SpaceGate::Narrowed(narrowed) => {
            assert_eq!(narrowed.table_ids.len(), 2, "narrowed to an adjacent pair");
        }
        super::SpaceGate::Run => {
            panic!("expected Narrowed, got Run (full run wrongly admitted)")
        }
        super::SpaceGate::Skip => panic!("expected Narrowed, got Skip (no pair admitted)"),
    }

    Ok(())
}

/// Every compaction-produced table must inherit the tree's shared deletion pause
/// before it becomes visible. A flush registers it (via `register_tables`), but a
/// compaction installs its outputs directly in `install_merge` — so without an
/// explicit install, a Page-ECC compaction output's in-place heal would skip the
/// checkpoint mutation window (`deletion_pause.get()` is `None`), race a
/// checkpoint that hard-links the SST, and overwrite the shared inode.
#[test]
fn compaction_outputs_inherit_the_deletion_pause() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // Two flushes → two L0 tables, then a major compaction merges them into one
    // output installed via `install_merge`.
    for i in 0..100u64 {
        tree.insert(format!("k{i:05}").as_bytes(), vec![1u8; 16], i);
    }
    tree.flush_active_memtable(0)?;
    for i in 100..200u64 {
        tree.insert(format!("k{i:05}").as_bytes(), vec![1u8; 16], i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, 0)?;

    let version = tree.current_version();
    let tables: Vec<_> = version.iter_tables().collect();
    assert!(
        !tables.is_empty(),
        "the major compaction produced an output"
    );
    for t in &tables {
        assert!(
            t.deletion_pause.get().is_some(),
            "compaction output table {} must inherit the deletion pause",
            t.id(),
        );
    }
    Ok(())
}

/// Builds a one-flush blob tree on `capfs`, then arms the tight-space prefix
/// reclaim on its blob file and releases every reference so the drop path runs.
/// `pause_active` installs a checkpoint deletion pause and holds it across the
/// drop. Returns the bytes the reclaim punched, measured with the pause still
/// held and again after releasing it.
fn punch_blob_prefix_on_drop(
    capfs: &capfs::CapacityFs,
    link_count: u64,
    pause_active: bool,
) -> crate::Result<(u64, u64)> {
    use crate::{Config, KvSeparationOptions, SequenceNumberCounter};

    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(capfs.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    for i in 0..64u64 {
        tree.insert(format!("k{i:05}").as_bytes(), vec![b'v'; 256], i);
    }
    tree.flush_active_memtable(0)?;

    let blob = {
        let version = tree.index.current_version();
        let Some(bf) = version.blob_files.iter().next().cloned() else {
            panic!("the flush spilled at least one blob file");
        };
        bf
    };
    let physical = blob.physical_size()?;
    assert!(physical > 0, "the blob file has bytes to reclaim");

    // The link count the reclaim probes: 1 = exclusively owned, 2 = an inode a
    // checkpoint has hard-linked.
    capfs.set_link_count(link_count);

    // A checkpoint's deletion pause, when requested, is held ACTIVE across the
    // drop, modelling a checkpoint running concurrently with the reclaim. The
    // TREE'S pause is acquired (the open already installed it on every blob
    // handle; a fresh one would be a no-op install).
    let pause_guard = if pause_active {
        Some(tree.index.deletion_pause.acquire())
    } else {
        None
    };

    // Arm the reclaim, then release EVERY reference (the tree's version holds
    // one too) so the blob file's drop path — the code under test — runs.
    blob.mark_punch_on_drop(physical);
    drop(blob);
    drop(tree);

    // Measured on both sides of the pause release: a deferred reclaim must be
    // carried out when the checkpoint's window closes, not dropped with the
    // view that armed it.
    let during_pause = capfs.punched_bytes();
    drop(pause_guard);
    Ok((during_pause, capfs.punched_bytes()))
}

/// A blob file whose consumed prefix was reclaimed in place must still verify
/// clean: its recorded digest covers the LIVE SUFFIX from the frontier, and
/// integrity checks hash from there. Hashing the whole file would fold in the
/// punched (zeroed) prefix and report every successfully reclaimed blob file —
/// and every checkpoint taken from that tree — as corrupt.
#[test]
fn reclaimed_blob_file_verifies_against_its_live_suffix() -> crate::Result<()> {
    use crate::fs::Fs as _;

    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let tree = match Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(capfs.clone()))
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(64),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..64u64 {
        tree.insert(format!("k{i:05}").as_bytes(), vec![b'v'; 256], i);
    }
    tree.flush_active_memtable(0)?;

    let blob = {
        let version = tree.index.current_version();
        let Some(bf) = version.blob_files.iter().next().cloned() else {
            panic!("the flush spilled at least one blob file");
        };
        bf
    };

    // Install the restricted view the relocation slice would install, then
    // punch the prefix it declares dead — the exact on-disk shape a completed
    // reclaim leaves behind.
    let frontier = blob.physical_size()? / 2;
    let restricted = blob.reopen_restricted(frontier)?;
    capfs.punch_hole(blob.path(), 0, frontier)?;

    let got = crate::verify::stream_checksum_from(restricted.path(), restricted.live_data_start())?;
    assert_eq!(
        got,
        restricted.checksum(),
        "a reclaimed blob file must verify against its live suffix, not the \
         punched prefix",
    );
    Ok(())
}

/// A tight-space blob-prefix reclaim must NOT punch an inode a checkpoint has
/// hard-linked: the checkpoint's captured SSTs still reference values in that
/// prefix, so zeroing it destroys data inside the supposedly immutable
/// snapshot. The delete path already probes the link count before truncating;
/// the punch path must apply the same guard. The exclusively-owned case is the
/// positive control — it proves the reclaim really fires in this fixture.
#[test]
fn blob_prefix_reclaim_skips_a_hard_linked_inode() -> crate::Result<()> {
    let (owned, _) = punch_blob_prefix_on_drop(&capfs::CapacityFs::new(), 1, false)?;
    assert!(
        owned > 0,
        "control: an exclusively-owned blob file must have its prefix reclaimed",
    );

    let (shared, _) = punch_blob_prefix_on_drop(&capfs::CapacityFs::new(), 2, false)?;
    assert_eq!(
        shared, 0,
        "a blob file shared with a checkpoint must not be punched: the snapshot \
         still references values in the reclaimed prefix",
    );
    Ok(())
}

/// The blob-prefix reclaim must also stand down while a checkpoint's deletion
/// pause is ACTIVE, exactly like the table-prefix punch: the pause covers the
/// checkpoint's whole copy/link pass, so standing down removes the
/// probe-then-punch window in which the checkpoint could link the inode after
/// the link count read 1. It must then RUN on release — the intent lives in the
/// dropping view, so discarding it would strand the space permanently.
#[test]
fn blob_prefix_reclaim_defers_across_a_deletion_pause() -> crate::Result<()> {
    let (during, after) = punch_blob_prefix_on_drop(&capfs::CapacityFs::new(), 1, true)?;
    assert_eq!(
        during, 0,
        "an active checkpoint pause must defer the reclaim, closing the \
         probe-to-punch race against the checkpoint's link pass",
    );
    assert!(
        after > 0,
        "releasing the pause must carry out the deferred reclaim, not drop it",
    );
    Ok(())
}

/// A restricted re-open builds a NEW blob-file handle and inherits none of
/// the tree's shared machinery — which is exactly why the tight-space slice
/// loop must bind every handle it puts into the version, not only the ones it
/// wrote from scratch. An unbound handle's `Drop` can unlink the file while a
/// checkpoint is capturing, and its prefix punch can zero bytes the
/// checkpoint has already hard-linked, because the reclaim consults a pause
/// slot nobody filled.
#[test]
fn a_reopened_blob_view_needs_binding_to_carry_the_deletion_pause() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(16),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected a blob tree"),
    };
    for i in 0..32u64 {
        tree.insert(format!("key{i:06}").as_bytes(), vec![b'v'; 64], i);
    }
    tree.flush_active_memtable(0)?;

    let original = {
        let binding = tree.index.version_history.read().latest_version();
        binding
            .version
            .blob_files
            .iter()
            .next()
            .cloned()
            .ok_or(crate::Error::Unrecoverable)?
    };
    assert!(
        original.deletion_pause_for_test().is_some(),
        "a flush-published blob file is bound when it is registered",
    );

    let reopened = original.reopen_restricted(0)?;
    assert!(
        reopened.deletion_pause_for_test().is_none(),
        "the re-open starts from the file on disk, so it inherits nothing — \
         the path that publishes it is what has to bind it",
    );

    reopened.bind_to_tree(&crate::table::TableSinks {
        deletion_pause: &tree.index.deletion_pause,
        heal_hints: &tree.index.heal_hints,
        #[cfg(feature = "std")]
        background_deleter: None,
    });
    assert!(
        reopened.deletion_pause_for_test().is_some(),
        "binding is what makes a re-opened view safe to publish",
    );
    Ok(())
}
