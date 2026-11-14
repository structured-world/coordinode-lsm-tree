use super::{create_compaction_stream, may_break_chain, pick_run_indexes};
use crate::{
    AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, Table, TableId,
    compaction::{Choice, CompactionStrategy, Input, state::CompactionState},
    config::BlockSizePolicy,
    version::Version,
};
use std::sync::Arc;
use test_log::test;

mod outside_spans {
    use super::super::OutsideSpans;
    use crate::{UserKey, range_tombstone::RangeTombstone};
    use test_log::test;

    fn key(i: u8) -> UserKey {
        UserKey::from([i].as_slice())
    }

    /// Merged spans answer as a scan of every table range does: tables given
    /// out of order, overlapping, nested, touching at an inclusive end, and
    /// apart; tombstones before, between, across and past them, and ending on
    /// a table's first key, which they do not reach.
    #[test]
    fn merged_spans_find_the_tables_a_scan_finds() {
        let comparator = crate::comparator::default_comparator();
        let cmp = comparator.as_ref();
        let ranges = vec![
            (key(40), key(45)),
            (key(10), key(20)),
            (key(15), key(18)),
            (key(20), key(25)),
            (key(60), key(60)),
            (key(18), key(30)),
        ];
        let spans = OutsideSpans::from_ranges(ranges.clone(), cmp);
        assert_eq!(spans.0.len(), 3, "10..30, 40..45, 60");
        for start in 0..70 {
            for end in start + 1..=70 {
                let rt = RangeTombstone::new(key(start), key(end), 1);
                let scanned = ranges.iter().any(|(min, max)| {
                    cmp.compare(&rt.start, max).is_le() && cmp.compare(min, &rt.end).is_lt()
                });
                assert_eq!(spans.overlaps(&rt, cmp), scanned, "[{start}, {end})");
            }
        }
    }
}

mod chain_completeness {
    use super::may_break_chain;
    use crate::{KeyRange, UserKey};
    use test_log::test;

    fn range(min: &str, max: &str) -> KeyRange {
        KeyRange::new((UserKey::from(min.as_bytes()), UserKey::from(max.as_bytes())))
    }

    #[test]
    fn a_table_older_than_the_chain_cannot_break_it() {
        // Versions below the chain are the base it will meet later, not a
        // break inside it, so the filter is never even consulted.
        let cmp = crate::comparator::default_comparator();
        assert!(!may_break_chain(
            Some((1, 4)),
            &range("a", "z"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || panic!("the seqno range alone must settle this"),
        ));
    }

    #[test]
    fn a_table_newer_than_the_chain_cannot_break_it() {
        let cmp = crate::comparator::default_comparator();
        assert!(!may_break_chain(
            Some((30, 40)),
            &range("a", "z"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || panic!("the seqno range alone must settle this"),
        ));
    }

    #[test]
    fn a_table_beside_the_key_cannot_break_the_chain() {
        // Overlapping seqnos, but the key is outside its range entirely.
        let cmp = crate::comparator::default_comparator();
        assert!(!may_break_chain(
            Some((10, 20)),
            &range("a", "f"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || panic!("the key range alone must settle this"),
        ));
    }

    #[test]
    fn a_table_spanning_the_key_defers_to_its_filter() {
        // Both ranges overlap, so only the filter can rule the table out —
        // and it is asked exactly here, where reading a block may be needed.
        let cmp = crate::comparator::default_comparator();
        assert!(!may_break_chain(
            Some((10, 20)),
            &range("a", "z"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || false,
        ));
        assert!(may_break_chain(
            Some((10, 20)),
            &range("a", "z"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || true,
        ));
    }

    #[test]
    fn an_unknown_seqno_range_is_assumed_to_overlap() {
        let cmp = crate::comparator::default_comparator();
        assert!(may_break_chain(
            None,
            &range("a", "z"),
            cmp.as_ref(),
            b"m",
            (10, 20),
            || true,
        ));
    }
}

/// A serial merge stopped part-way must not commit what it wrote.
///
/// The install swaps EVERY input table named in the payload for the output, so
/// a merge that never read its input to the end would drop the unread tail out
/// of the tree. The parallel path already refuses for exactly this reason; this
/// pins the serial one.
#[test]
fn interrupted_serial_merge_keeps_every_key() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    const N: u64 = 64;
    let key = |i: u64| format!("key_{i:04}");

    let dir = tempfile::tempdir()?;
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // One table per flush below, and no parallel split, so the merge takes the
    // serial path this test is about.
    .compaction_threads(1);
    let failpoint = config.stop_serial_merge_after_first_item.clone();
    let tree = config.open()?;

    // Two tables, so the compaction has a real merge to perform and a tail to
    // leave unread once it stops.
    for i in 0..N {
        tree.insert(key(i), "v", i);
    }
    tree.flush_active_memtable(0)?;
    for i in N..(2 * N) {
        tree.insert(key(i), "v", i);
    }
    tree.flush_active_memtable(0)?;

    for i in 0..(2 * N) {
        assert!(
            tree.get(key(i), crate::SeqNo::MAX)?.is_some(),
            "precondition: {} is readable before the compaction",
            key(i),
        );
    }

    // Arm: the merge writes one item, then observes the stop signal.
    failpoint.store(true, Ordering::SeqCst);
    let result = tree.major_compact(u64::MAX, 0);

    assert!(
        result.is_err(),
        "an interrupted merge must refuse, not commit a truncated output",
    );

    // Whatever the compaction did, every key must still be there.
    for i in 0..(2 * N) {
        assert!(
            tree.get(key(i), crate::SeqNo::MAX)?.is_some(),
            "{} was lost by the interrupted compaction",
            key(i),
        );
    }

    // The abort leaves the merge's half-written output unfinalized on disk. A
    // reopen has to sweep it and come back with the same tree, so the refusal
    // does not trade data loss for an unopenable directory.
    drop(tree);
    let reopened = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for i in 0..(2 * N) {
        assert!(
            reopened.get(key(i), crate::SeqNo::MAX)?.is_some(),
            "{} was lost across the reopen after an interrupted compaction",
            key(i),
        );
    }

    Ok(())
}

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
fn open_rewrites_a_sidecar_that_disagrees_with_the_manifest() -> crate::Result<()> {
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

    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the crashed tight-space slice must leave a restricted table");
    };
    let path = restricted.path.clone();
    let table_id = restricted.id();
    let Some(bound) = restricted.restrict_lower_bound().cloned() else {
        panic!("the table was selected by having a restriction bound");
    };

    // A STALE sidecar: valid framing and the right table, but a LOWER bound
    // than the manifest holds — the shape a second slice leaves when its own
    // sidecar write fails. A manifest-loss repair honoring it would restrict
    // less than reality and resurrect consumed rows.
    let mut stale = bound.to_vec();
    stale.truncate(bound.len().saturating_sub(1));
    crate::restrict_bound::write(
        &*fs,
        &path,
        None,
        table_id,
        &stale,
        crate::fs::SyncMode::Normal,
    )?;
    drop(version);
    drop(reopened);

    let _tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?;

    let crate::restrict_bound::SidecarRead::Present(id, recorded) =
        crate::restrict_bound::read(&*fs, &path, None)?
    else {
        panic!("the sidecar must be present after the open");
    };
    assert_eq!(id, table_id, "the sidecar names its own table");
    assert_eq!(
        recorded,
        bound.to_vec(),
        "an existing sidecar that disagrees with the manifest is republished \
         from the manifest, which is the authority on the bound",
    );
    Ok(())
}

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

/// The sidecar an open derives from the manifest it read is only as good as
/// that manifest: an open whose install of the recovered state fails has not
/// made that manifest durable, so it must not leave a sidecar a manifest-loss
/// repair would then trust over an older generation's tables.
#[test]
fn an_open_whose_install_fails_leaves_no_sidecar_it_derived() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let fs: Arc<dyn Fs> = Arc::new(mem.clone());
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::clone(&fs),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;
    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the crashed tight-space slice must leave a restricted table");
    };
    let sidecar = crate::restrict_bound::sidecar_path(&restricted.path);
    fs.remove_file(&sidecar)?;
    drop(version);
    drop(reopened);

    // The install renames the fresh snapshot into place; that rename fails.
    let faulty = FaultFs::new(mem.clone());
    faulty.injector().arm(
        FaultRule::new(FaultOp::Rename, Fault::Error(crate::io::ErrorKind::Other))
            .on_path(dir.path().join("v").to_string_lossy())
            .once(),
    );
    let failed = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(faulty))
    .open();
    assert!(failed.is_err(), "self-check: the install failed");
    assert!(
        fs.metadata(&sidecar).is_err(),
        "no sidecar is published for a manifest the open did not install",
    );

    let _tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?;
    assert!(
        fs.metadata(&sidecar).is_ok(),
        "the open that installs the manifest republishes it",
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

/// A tight-space pass that fails after installing its first slice leaves that
/// slice installed: the bytes it wrote count in the tree's compaction work,
/// though the run as a whole returns the failure.
#[cfg(feature = "metrics")]
#[test]
fn a_failed_tight_space_pass_counts_the_work_of_the_slices_it_installed() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let crate::AnyTree::Standard(tree) = config.open()? else {
        panic!("expected Standard tree");
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    // Room for a slice, not for the whole merge.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 8);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    failpoint.store(true, Ordering::SeqCst);
    assert!(tree.major_compact(64 * 1024 * 1024, 0).is_err());
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failure came after the first slice"
    );
    assert!(
        tree.metrics().compaction_work_measured().written > 0,
        "the installed slice's tables count as written",
    );
    Ok(())
}

/// A tight-space pass stops after a slice whose consumed inputs a reader still
/// holds: the room the next slice was cut to comes back only once the reader
/// lets go, so the pass leaves the rest, its slice installed and every key
/// readable, and the next compaction finishes once the room is back.
#[test]
fn tight_space_pass_stops_while_a_reader_holds_what_a_slice_consumed() -> crate::Result<()> {
    // Room for a few slices, and room for one slice and the tail, which the
    // pass stops before too.
    for (part, of) in [(1, 4), (3, 5)] {
        reader_holds_a_slices_inputs((part, of))?;
    }
    Ok(())
}

/// [`tight_space_pass_stops_while_a_reader_holds_what_a_slice_consumed`] with
/// `part / of` of the tree's footprint free.
fn reader_holds_a_slices_inputs((part, of): (u64, u64)) -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .open()?
    else {
        panic!("expected Standard tree");
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    // Room for slices, not for the whole merge: each slice's outputs hold a
    // table's fixed bytes beside what it consumed, and too little room leaves
    // a pass that only grows.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used * part / of);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    let restricted = |tree: &crate::Tree| {
        tree.current_version()
            .iter_tables()
            .any(|table| table.restrict_lower_bound().is_some())
    };

    let reader = tree.current_version();
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(
        restricted(&tree),
        "the pass stopped after its first slice, {part}/{of} free"
    );
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "key {i}"
        );
    }

    drop(reader);
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(
        !restricted(&tree),
        "the next pass finishes the merge, {part}/{of} free"
    );
    Ok(())
}

/// A tight-space pass reserves each slice against what the pool holds when
/// the slice starts: room another tree took after the pass measured it is not
/// written over, and the pass stops before a slice that no longer fits.
#[test]
fn tight_space_slice_with_its_room_taken_meanwhile_does_not_run() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()));
    let squeeze = Arc::clone(&config.tight_slice_squeeze);
    let crate::AnyTree::Standard(tree) = config.open()? else {
        panic!("expected Standard tree");
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    // Some room, far below what a whole merge writes: the pass slices.
    mem.set_capacity(tree.storage_stats()?.used_bytes + 8 * 1_024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    // Another tree takes far more than the volume holds once the pass has
    // measured it.
    squeeze.store(u64::MAX / 4, Ordering::SeqCst);
    let result = tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Nothing,
        "{result:?}"
    );
    assert_eq!(mem.punched_bytes(), 0, "no slice ran");
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some()
        );
    }
    Ok(())
}

/// The post-commit sidecar-write window under MANIFEST LOSS: a committed
/// slice whose `.restrict-bound` write failed leaves an UNPUNCHED input
/// beside its slice outputs (the punch never arms without the sidecar).
/// A manifest-loss repair then faces both histories — and resolves them
/// through the outputs' recorded lineage: the input is the complete history,
/// so the derived slice outputs are excluded and no merge operand can be
/// applied twice. This is what makes the sidecar failure tolerable beyond
/// the normal-open republish path.
#[test]
fn manifest_loss_in_the_unwritten_sidecar_window_resolves_to_one_history() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    // Every sidecar write is refused, so no slice can arm its punch.
    let fault = FaultFs::new(mem.clone());
    fault.injector().arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::PermissionDenied))
            .on_path(".restrict-bound"),
    );

    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_fs(fault);
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
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, 0).is_err(),
        "the crash failpoint must abort the tight-space compaction",
    );
    assert_eq!(
        mem.punched_bytes(),
        0,
        "with the sidecar write refused, no punch may arm",
    );
    drop(tree);

    // Manifest loss inside the window.
    for e in mem.read_dir(dir.path())? {
        let is_version = e
            .file_name
            .strip_prefix('v')
            .or_else(|| e.file_name.strip_prefix("edits-"))
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version {
            mem.remove_file(&e.path)?;
        }
    }
    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem.clone()))
    .repair()?;
    assert!(
        report
            .excluded_files
            .iter()
            .any(|(_, reason)| reason.contains("derived output")),
        "the slice outputs must be excluded as derived — publishing both \
         histories would double-apply merge operands: {report:?}",
    );
    assert_eq!(
        report.unreadable, 0,
        "a healthy redundancy exclusion is not an unreadable file: {report:?}",
    );

    let reopened = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(Arc::new(mem))
    .open()?;
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            reopened
                .get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "key {i} lost across the sidecar window + manifest loss",
        );
    }
    Ok(())
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

    restricted.verify_blob_links()?;
    restricted.verify_tli_mirrors()?;
    restricted.verify_block_layout()?;
    if let Err((gate, e)) = restricted.verify_reconcile_gates(None, false) {
        panic!("the healthy restricted suffix must pass every gate, {gate:?} refused it: {e}");
    }
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
            .or_else(|| e.file_name.strip_prefix("edits-"))
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version {
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

/// While a tight-space slice installs, the filter budget holds its outputs'
/// filters on top of the published ones: the input it restricts keeps its
/// filter live, so it gives no credit, and the slice's own plan is not given
/// back before the install.
#[cfg(zstd_any)]
#[test]
fn tight_space_slice_holds_its_filter_budget_until_the_install() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let capfs = capfs::CapacityFs::new();
    let shared: Arc<dyn crate::fs::Fs> = Arc::new(capfs.clone());
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .filter_advisor(Some(crate::config::FilterAdvisor::new(u64::MAX)))
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
    let seen: Arc<parking_lot::Mutex<Option<(u64, u64)>>> = Arc::default();
    {
        let budget = Arc::clone(&tree.filter_budget);
        let seen = Arc::clone(&seen);
        tree.config.arm_before_output_install(move || {
            *seen.lock() = Some((budget.held(), budget.published()));
        });
    }
    tree.major_compact(64 * 1024 * 1024, 0)?;

    let Some((held, published)) = *seen.lock() else {
        panic!("the tight-space path installed a slice");
    };
    assert!(
        held > published,
        "held {held} bytes before the first slice's install, the published filters \
         alone take {published}"
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

/// Shared key count, key / value shapes and GC watermark for the blob
/// relocation tests, so the fixture and every assertion cover one dataset.
const BLOB_RELOC_KEYS: u64 = 4_000;
const BLOB_RELOC_WATERMARK: u64 = 4 * BLOB_RELOC_KEYS;

fn blob_reloc_key(i: u64) -> String {
    format!("key{i:08}")
}

/// High-entropy (xorshift) values so the blobs do NOT compress away: the
/// relocation transient must be real for the space gate to skip the full merge
/// and engage the slicing path. Deterministic per (key, generation) so an
/// assertion can regenerate the expected bytes.
fn blob_reloc_value(i: u64, generation: u8) -> Vec<u8> {
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
}

/// The KV-separation options both the original and the reopened tree use: a
/// reopen with different staleness knobs would not be the same deployment, and
/// a retry of the relocation has to see the same candidate set.
fn blob_reloc_kv_options() -> KvSeparationOptions {
    KvSeparationOptions::default()
        .separation_threshold(64)
        // Keep every stale file (default age_cutoff 0.25 would drain a small
        // candidate set to empty) and treat a lightly-dead file as stale so the
        // half-shadowed first generation is relocated.
        .age_cutoff(1.0)
        .staleness_threshold(0.1)
        // Small blob files → several per generation, so relocation has multiple
        // stale files and the merge slices across them.
        .file_target_size(48 * 1024)
}

/// Drives a KV-separated tight-space compaction that RELOCATES a fragmented
/// blob file in slices, crashes it right after the first slice is durably
/// installed and punched, and returns the reopened tree on the same simulated
/// disk. Callers assert over the resulting state.
fn blob_relocation_crash_and_reopen(
    dir: &std::path::Path,
    mem: &crate::fs::MemFs,
) -> crate::Result<crate::blob_tree::BlobTree> {
    use core::sync::atomic::Ordering;

    let config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(blob_reloc_kv_options()));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // Generation 1: every key → a blob.
    for i in 0..BLOB_RELOC_KEYS {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Generation 2: overwrite EVEN keys only, interleaved so every gen-1 blob
    // file ends up ~half dead (stale, not fully dead → eligible to relocate).
    for i in (0..BLOB_RELOC_KEYS).step_by(2) {
        tree.insert(
            blob_reloc_key(i).as_bytes(),
            blob_reloc_value(i, 2),
            BLOB_RELOC_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;

    // Blob fragmentation is only LEARNED during a merge (the drop callback
    // records each shadowed gen-1 blob as dead). Run one ample-space merge
    // first so the even-key gen-1 blobs are counted dead, leaving every gen-1
    // file ~half stale — the precondition for the next merge to RELOCATE them.
    // It also collapses the index SSTs to the bottom level. The watermark sits
    // above every live seqno so the merge actually folds the shadowed entries
    // (seqno 0 keeps all MVCC versions and records no fragmentation).
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;

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
        tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)
            .is_err(),
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

    drop(tree);
    match Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(blob_reloc_kv_options()))
    .with_shared_fs(Arc::new(mem.clone()))
    .open()?
    {
        crate::AnyTree::Blob(t) => Ok(t),
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    }
}

/// A KV-separated tight-space compaction that RELOCATES a fragmented blob
/// file in slices and crashes after the first slice must reopen consistently:
/// the relocated entries (now in fresh compact files referenced by the
/// installed slice output) AND the not-yet-relocated entries (still in the
/// punched stale file's intact suffix) must all read their latest value.
#[test]
fn tight_space_blob_relocation_crash_after_first_slice_recovers_all_keys() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let reopened = blob_relocation_crash_and_reopen(dir.path(), &mem)?;

    // Every key reads its latest value: odd keys = relocated gen-1 blob, even
    // keys = gen-2 blob.
    for i in 0..BLOB_RELOC_KEYS {
        let expected = blob_reloc_value(i, u8::from(i % 2 == 0) + 1);
        assert_eq!(
            reopened
                .get(blob_reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after a crash mid blob-relocation + reopen",
        );
    }
    Ok(())
}

/// A tight-space slice reserves room for the live values it relocates, so it
/// copies no block whole: a whole copy also writes the block's dead values,
/// which the reservation does not count and the volume may not have room for.
/// Mostly live blocks a normal pass would copy whole are written value by
/// value here, and no dead value reaches the new files.
#[cfg(feature = "lz4")]
#[test]
fn a_tight_space_slice_copies_no_block_whole() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // Values share blocks under a codec; with none they would be packed
    // records, which are not copied whole.
    .blob_compression(crate::CompressionType::Lz4)
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(
        blob_reloc_kv_options()
            .block_size(4_096)
            .file_target_size(64 * 1024 * 1024),
    ));
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    // One blob file per generation: the first is relocated whole, so the
    // files left after the pass hold only what it wrote and the second. At
    // seqno 0, the seqno the merge before the pass leaves its rows at, so its
    // blocks list their values at their rows' seqno and may be copied whole.
    for i in 0..BLOB_RELOC_KEYS {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), 0);
    }
    tree.flush_active_memtable(0)?;
    // A run overwritten whole makes the files stale; past it, one value in
    // sixteen leaves the blocks mostly live, each with a dead value in it.
    let run = BLOB_RELOC_KEYS * 3 / 10;
    for i in (0..BLOB_RELOC_KEYS).filter(|i| *i < run || i % 16 == 0) {
        tree.insert(
            blob_reloc_key(i).as_bytes(),
            blob_reloc_value(i, 2),
            BLOB_RELOC_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;

    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    assert!(
        mem.punched_bytes() > 0,
        "the fixture needs the pass to run in slices"
    );

    let values: u64 = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(|file| file.0.meta.item_count)
        .sum();
    assert_eq!(
        values, BLOB_RELOC_KEYS,
        "the blob files hold one value per key and no dead one"
    );
    for i in 0..BLOB_RELOC_KEYS {
        let generation = if i < run || i % 16 == 0 { 2 } else { 1 };
        assert_eq!(
            tree.get(blob_reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(blob_reloc_value(i, generation).as_slice()),
            "key {i}"
        );
    }
    Ok(())
}

/// A tight-space slice whose share of a stale blob file is all dead (the
/// pointers to those frames were folded away before the pass) still reclaims
/// it: every version of the slice's keys went through the slice, so a frame
/// no emitted pointer claimed is free. The first slice must advance the
/// file's frontier past those frames, not leave them to a later slice.
#[test]
#[expect(clippy::expect_used, reason = "test assertion")]
fn a_tight_space_slice_reclaims_stale_frames_no_pointer_claims() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    // One blob file per generation, so the first generation is a single
    // stale file whose head is dead and whose tail is live.
    let kv_options = || blob_reloc_kv_options().file_target_size(64 * 1024 * 1024);
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(kv_options()));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    for i in 0..BLOB_RELOC_KEYS {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    // Overwrite the first three quarters: their first-generation frames die,
    // the last quarter keeps the file live.
    let overwritten = BLOB_RELOC_KEYS * 3 / 4;
    for i in 0..overwritten {
        tree.insert(
            blob_reloc_key(i).as_bytes(),
            blob_reloc_value(i, 2),
            BLOB_RELOC_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;

    // Fold the shadowed pointers away and learn the file's dead share.
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    let stale = {
        let version = tree.index.current_version();
        let mut gen1: Vec<_> = version
            .blob_files
            .iter()
            .map(crate::vlog::BlobFile::id)
            .collect();
        gen1.sort_unstable();
        *gen1.first().expect("the first generation's blob file")
    };

    // Less room than the live quarter's frames, so the pass relocates them in
    // slices, the dead frames' keys first.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 16);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;
    failpoint.store(true, Ordering::SeqCst);
    assert!(
        tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)
            .is_err(),
        "the crash failpoint must stop the pass after its first slice",
    );
    assert!(
        !failpoint.load(Ordering::SeqCst),
        "the failpoint should have fired",
    );

    let version = tree.index.current_version();
    let file = version
        .blob_files
        .get(stale)
        .expect("the stale file still holds the live last quarter");
    assert!(
        file.live_data_start() > 0,
        "the first slice must advance the stale file's frontier past the dead \
         frames of its keys",
    );
    drop(version);

    for i in 0..BLOB_RELOC_KEYS {
        let expected = blob_reloc_value(i, u8::from(i < overwritten) + 1);
        assert_eq!(
            tree.get(blob_reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} after the first slice",
        );
    }
    Ok(())
}

/// A relocation that committed a slice and then aborted leaves the stale blob
/// file RESTRICTED: its consumed prefix is punched and its view carries the
/// committed frontier. A retry must resume the scan THERE. Starting at the data
/// section instead reads the punched zeros, byte-wise resynchronization taints
/// every surviving frame, and the relocation is rejected on every retry —
/// stranding a space-constrained blob tree with no way to reclaim.
#[test]
fn a_relocation_retry_resumes_at_the_committed_blob_frontier() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let tree = blob_relocation_crash_and_reopen(dir.path(), &mem)?;

    {
        let version = tree.index.current_version();
        assert!(
            version.blob_files.iter().any(|bf| bf.live_data_start() > 0),
            "the crashed relocation must leave a blob file with a committed frontier",
        );
    }

    let punched_before = mem.punched_bytes();
    // Less room than what is left to relocate, so the retry runs in slices
    // too rather than in one go.
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 16);
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    assert!(
        mem.punched_bytes() > punched_before,
        "the retry must relocate further and punch what it consumed",
    );

    for i in 0..BLOB_RELOC_KEYS {
        let expected = blob_reloc_value(i, u8::from(i % 2 == 0) + 1);
        assert_eq!(
            tree.get(blob_reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after the relocation retry",
        );
    }
    Ok(())
}

/// Once space frees up, the restricted blob file a crashed tight-space
/// relocation left behind is relocated by an ordinary merge. That merge must
/// also start the scan at the file's committed frontier: from the data section
/// it reads the punched zeros and rejects the file.
#[test]
fn an_ordinary_merge_resumes_a_restricted_blob_file_at_its_frontier() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let tree = blob_relocation_crash_and_reopen(dir.path(), &mem)?;

    let restricted: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .filter(|bf| bf.live_data_start() > 0)
        .map(crate::vlog::BlobFile::id)
        .collect();
    assert!(
        !restricted.is_empty(),
        "the crashed relocation must leave a blob file with a committed frontier",
    );

    mem.set_capacity(u64::MAX);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = false;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;

    let version = tree.index.current_version();
    assert!(
        version
            .blob_files
            .iter()
            .all(|bf| !restricted.contains(&bf.id())),
        "the ordinary merge must relocate the restricted blob file",
    );
    for i in 0..BLOB_RELOC_KEYS {
        let expected = blob_reloc_value(i, u8::from(i % 2 == 0) + 1);
        assert_eq!(
            tree.get(blob_reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after the ordinary merge",
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
            crate::comparator::default_comparator(),
            None,
            None,
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

/// A run that unlinks the tables a range tombstone spans and then runs its
/// strategy consumed the tables of both, and says so: the strategy's action,
/// with every table the run took in.
#[test]
fn compaction_reclaim_then_strategy_drop_reports_both_input_counts() -> crate::Result<()> {
    use crate::version::run::Ranged as _;

    struct DropZ;

    impl CompactionStrategy for DropZ {
        fn get_name(&self) -> &'static str {
            "DropZ"
        }

        fn choose(&self, version: &Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Drop(
                version
                    .iter_tables()
                    .filter(|t| t.key_range().min().as_ref() == b"z")
                    .map(crate::Table::id)
                    .collect(),
            )
        }
    }

    let folder = tempfile::tempdir()?;
    let tree = crate::Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    tree.insert("k", "v", 1);
    tree.flush_active_memtable(0)?;
    tree.remove_range("a", "m", 2);
    tree.flush_active_memtable(0)?;
    tree.insert("z", "v", 3);
    tree.flush_active_memtable(0)?;

    let result = tree.compact(Arc::new(DropZ), 3)?;

    assert_eq!(super::CompactionAction::Dropped, result.action);
    assert_eq!(2, result.tables_in, "the table of k and the table of z");
    Ok(())
}

#[test]
fn blob_file_picking_simple() -> crate::Result<()> {
    struct DropStrategy(Vec<TableId>);

    impl CompactionStrategy for DropStrategy {
        fn get_name(&self) -> &'static str {
            "DropCompaction"
        }

        fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Drop(self.0.iter().copied().collect())
        }
    }

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
            .staleness_threshold(0.01),
    ))
    .blob_compression(crate::CompressionType::None)
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

    // Table #1 holds `a`.
    tree.compact(Arc::new(DropStrategy(vec![1])), 1_000)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    // `a` is a record of a packed block shared with the others: it is
    // charged its record as block bytes, its key and one byte with the
    // record's fixed part, nothing of frames of its own.
    let charged = (crate::vlog::blob_file::packed::RECORD_HEADER_LEN + 2) as u64;
    let garbage = crate::blob_tree::FragmentationEntry::new(1, 1, 0).with_block_bytes(charged);
    {
        assert_eq!(
            &{
                let mut map = crate::HashMap::default();
                map.insert(0, garbage);
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
                map.insert(0, garbage);
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
    let ordered: Vec<(TableId, u64)> = run
        .iter()
        .map(|t| Ok((t.id(), t.live_file_size()?)))
        .collect::<crate::Result<_>>()?;

    let payload = Input {
        table_ids: ordered.iter().map(|(id, _)| *id).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };

    let candidates = super::narrow_merge_candidates(&version, &payload)?;

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

    let combined = |c: &Input| -> crate::Result<u64> {
        c.table_ids
            .iter()
            .filter_map(|id| version.get_table(*id))
            .try_fold(0u64, |acc, t| t.live_file_size().map(|size| acc + size))
    };
    let sums: Vec<u64> = candidates
        .iter()
        .map(combined)
        .collect::<crate::Result<_>>()?;

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
fn merge_would_fit_combines_shared_volume_outputs_and_separates_routed_ones() -> crate::Result<()> {
    use crate::fs::MemFs;

    const MIB: u64 = 1024 * 1024;
    let dir = tempfile::tempdir()?;
    let pool = crate::CapacityPool::default();
    let fits = |cfg: &Config, quota: u64, sst: u64, blob: u64| {
        super::merge_would_fit(&pool, 0, cfg, quota, (sst, blob), 6)
    };

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
        !fits(&cfg, u64::MAX, 60 * MIB, 60 * MIB),
        "shared-volume outputs must be summed, not checked independently"
    );
    // 60 + 30 = 90 MiB (+1 MiB reserve) ≤ 100 MiB → admitted.
    assert!(fits(&cfg, u64::MAX, 60 * MIB, 30 * MIB));

    // The logical quota caps the total regardless of physical free:
    // 50 + 40 = 90 MiB exceeds an 80 MiB quota headroom.
    assert!(!fits(&cfg, 80 * MIB, 50 * MIB, 40 * MIB));

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
        fits(&routed, u64::MAX, 60 * MIB, 60 * MIB),
        "proven-independent volumes are checked independently"
    );
    // A blob output that overflows the primary volume alone still fails.
    assert!(!fits(&routed, u64::MAX, 60 * MIB, 130 * MIB));

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
        !fits(&routed_same_mount, u64::MAX, 60 * MIB, 60 * MIB),
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
    let tables: Vec<&Table> = run.iter().collect();
    let input = |ids: crate::HashSet<TableId>| Input {
        table_ids: ids,
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };
    let payload = input(tables.iter().map(|t| t.id()).collect());
    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    // What the gate charges the whole run and an adjacent pair of it.
    let probe = crate::compaction::flavour::output_bound_probe(
        &version,
        &opts,
        &payload,
        &opts.runtime_config.load(),
    )?;
    let full = super::recorded_table_demand(&version, &opts, &payload, &probe, &[])?;
    let pair = super::recorded_table_demand(
        &version,
        &opts,
        &input(tables.iter().take(2).map(|t| t.id()).collect()),
        &probe,
        &[],
    )?;
    assert!(
        pair < full,
        "a pair takes less than the run: {pair} against {full}"
    );

    // Free space below what the full run takes but above a pair: the run does
    // not fit, a run-adjacent pair does. Calibrate against the SIMULATED
    // disk's real stored bytes (manifest / WAL count too, not just live SSTs),
    // since the gate probes `available_space`, not the version footprint.
    let probe_capacity = 1u64 << 40;
    mem.set_capacity(probe_capacity);
    let stored =
        probe_capacity - crate::fs::Fs::available_space(&mem, dir.path()).unwrap_or(probe_capacity);
    mem.set_capacity(stored + full - 1);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    match super::space_gate_for_merge(&version, &opts, &payload)? {
        super::SpaceGate::Narrowed(narrowed, ..) => {
            assert_eq!(narrowed.table_ids.len(), 2, "narrowed to an adjacent pair");
        }
        super::SpaceGate::Run(..) => {
            panic!("expected Narrowed, got Run (full run wrongly admitted)")
        }
        super::SpaceGate::Skip(_) => panic!("expected Narrowed, got Skip (no pair admitted)"),
    }

    // Run through a compaction, the narrowed merge is what runs: two of the
    // run's tables are rewritten, the others stay, and every key still reads.
    struct Fixed(Input);
    impl crate::compaction::CompactionStrategy for Fixed {
        fn get_name(&self) -> &'static str {
            "FixedTest"
        }
        fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Merge(self.0.clone())
        }
    }
    let ran = tree.compact(Arc::new(Fixed(payload.clone())), 0)?;
    assert!(ran.work.measured.read > 0, "a merge ran");
    let after = tree.current_version();
    let kept = payload
        .table_ids
        .iter()
        .filter(|id| after.iter_tables().any(|table| table.id() == **id))
        .count();
    assert_eq!(
        kept,
        payload.table_ids.len() - 2,
        "only the pair was merged"
    );
    for i in (0..3_000u64).step_by(97) {
        assert!(tree.get(format!("k{i:08}"), crate::SeqNo::MAX)?.is_some());
    }

    Ok(())
}

/// A retry over an ALREADY restricted input must never hand it a lower bound.
/// The boundary list is built from the whole block index, punched prefix
/// included, so an early boundary can sit below the restriction a previous run
/// committed. Reopening there would make the manifest claim the SST serves its
/// own punched prefix again: reads land in the zeros, and rows the earlier
/// slice output already owns are served twice.
#[test]
fn a_retry_never_lowers_an_existing_restriction() -> crate::Result<()> {
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(mem.clone()),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;
    let (restricted_id, committed_bound) = {
        let version = reopened.current_version();
        let Some(table) = version
            .iter_tables()
            .find(|t| t.restrict_lower_bound().is_some())
        else {
            panic!("the crashed tight-space slice must leave a restricted table");
        };
        let Some(bound) = table.restrict_lower_bound().cloned() else {
            panic!("the table was selected by having a restriction bound");
        };
        (table.id(), bound)
    };
    drop(reopened);

    // Re-run the pass over that restricted input, crashing after its own first
    // slice so the retry's restriction is what survives to be inspected.
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem));
    let failpoint = config.fail_tight_after_first_slice.clone();
    let tree = match config.open()? {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    failpoint.store(true, Ordering::SeqCst);
    let _ = tree.major_compact(64 * 1024 * 1024, 0);

    let version = tree.current_version();
    let Some(after) = version.get_table(restricted_id) else {
        panic!("the retry must still hold the restricted input, not consume it");
    };
    let Some(bound) = after.restrict_lower_bound() else {
        panic!("a table that was restricted can never come back unrestricted");
    };
    // Default comparator: the fixture's keys order lexicographically.
    assert!(
        bound.as_ref() >= committed_bound.as_ref(),
        "a retry must not serve a prefix an earlier slice already punched: \
         bound went from {committed_bound:?} to {bound:?}",
    );

    // The sidecar is what a manifest-loss repair trusts, so it must record the
    // bound the manifest committed. A lower one there republishes the SST as
    // serving an interval the earlier slice output already owns, whose blocks
    // are punched out.
    let crate::restrict_bound::SidecarRead::Present(_, recorded) =
        crate::restrict_bound::read(&*after.fs, &after.path, after.encryption.as_deref())?
    else {
        panic!("the committed restriction must have published its sidecar");
    };
    assert_eq!(
        recorded,
        bound.to_vec(),
        "the sidecar must record the bound the manifest committed",
    );
    Ok(())
}

/// A backend that cannot report free space reads as physically unbounded, so
/// on a quota-only deployment the LOGICAL limit is the only thing that rejects
/// a merge. Deriving the slice budget from the physical probe alone then leaves
/// it unbounded, `tight_slice_boundaries` emits no interior boundary, and the
/// pass that was engaged to reclaim space returns having reclaimed nothing —
/// leaving the tree read-only with no way out.
#[test]
fn tight_space_slices_when_only_the_quota_constrains() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let faulty = FaultFs::new(mem.clone());
    faulty.injector().arm(FaultRule::new(
        FaultOp::AvailableSpace,
        Fault::Error(crate::io::ErrorKind::Unsupported),
    ));

    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(faulty))
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;

    // A quota that leaves less headroom than the merge's output needs: the
    // only constraint in play, since the free-space probe answers "unknown".
    let used = crate::storage_stats::compute_used_bytes(&tree.current_version())?;
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
        c.storage_limit_bytes = Some(used + used / 4);
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(
        mem.punched_bytes() > 0,
        "the slices must reclaim a prefix; a quota-only tree has no other way \
         back from read-only",
    );
    Ok(())
}

/// A restricted view's metadata still counts the punched-out prefix. The
/// planner's numerator raises that table's lower bound to the restriction, so
/// a denominator taken from the whole file reports a FULL-keyspace query as
/// selecting less than everything.
#[test]
fn full_range_selectivity_is_one_over_a_restricted_view() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(mem.clone()),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;
    {
        let version = reopened.current_version();
        assert!(
            version
                .iter_tables()
                .any(|t| t.restrict_lower_bound().is_some()),
            "the crashed tight-space slice must leave a restricted table",
        );
    }

    let card = reopened.approximate_range_cardinality::<&[u8], _>(.., crate::SeqNo::MAX)?;
    assert!(
        (card.selectivity - 1.0).abs() < 1e-9,
        "a full-keyspace query selects everything the snapshot sees, got {}",
        card.selectivity,
    );
    Ok(())
}

/// A tight-space slice leaves a RESTRICTED input whose `file_size` still
/// describes the punched original, prefix included. Sizing an ordinary merge by
/// that obsolete figure stalls a compaction whose real output — the live suffix
/// — fits the headroom, and after a restart without tight-space mode there is
/// nothing left to unstick it.
#[test]
fn space_gate_sizes_a_restricted_input_by_its_live_suffix() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::new(mem.clone()),
        |used| mem.set_capacity(used + used / 4),
        || mem.punched_bytes(),
    )?;
    let crate::AnyTree::Standard(tree) = reopened else {
        panic!("expected Standard tree");
    };
    // Physical free space is put out of the way so the logical quota alone
    // governs the gate's verdict.
    mem.set_capacity(u64::MAX);

    let version = tree.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the crashed tight-space slice must leave a restricted table");
    };
    let live = restricted.live_file_size()?;
    assert!(
        live < restricted.file_len(),
        "the fixture's input must carry a punched, superseded prefix",
    );

    let payload = Input {
        table_ids: core::iter::once(restricted.id()).collect(),
        dest_level: 6,
        canonical_level: 6,
        target_size: 64 * 1024 * 1024,
    };
    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    // What the gate charges the live suffix, and what the whole original's
    // entries, as its metadata records them, would take.
    let probe = crate::compaction::flavour::output_bound_probe(
        &version,
        &opts,
        &payload,
        &opts.runtime_config.load(),
    )?;
    let suffix = super::recorded_table_demand(&version, &opts, &payload, &probe, &[])?;
    let meta = &restricted.metadata;
    let whole = probe.rewrite_bound(
        &crate::table::writer::RewriteLoad {
            entries: meta.item_count,
            key_bytes: meta.sum_user_key_bytes.unwrap_or_default(),
            value_bytes: meta.sum_value_bytes.unwrap_or_default(),
            filter_hashes: meta.item_count,
            longest_key: meta.longest_user_key.unwrap_or_default(),
            longest_value: meta.longest_value,
            ..crate::table::writer::RewriteLoad::default()
        },
        0,
    );
    assert!(
        suffix < whole,
        "the suffix is charged less than the whole original: {suffix} against {whole}"
    );
    // Headroom that fits the live suffix EXACTLY and not the original.
    let used = crate::storage_stats::compute_used_bytes(&version)?;
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = false;
        c.storage_limit_bytes = Some(used + suffix);
    })?;

    match super::space_gate_for_merge(&version, &opts, &payload)? {
        super::SpaceGate::Run(..) => Ok(()),
        super::SpaceGate::Narrowed(..) => {
            panic!("expected Run, got Narrowed (a single-table merge cannot narrow)")
        }
        super::SpaceGate::Skip(_) => {
            panic!("expected Run, got Skip (the punched prefix was charged to the output)")
        }
    }
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
        read_budget: tree.index.config.columnar_read_budget,
        #[cfg(feature = "std")]
        background_deleter: &tree.index.background_deleter,
        track_filter_probes: false,
    });
    assert!(
        reopened.deletion_pause_for_test().is_some(),
        "binding is what makes a re-opened view safe to publish",
    );
    Ok(())
}

/// Tight-space compaction reclaims by punching the CONSUMED PREFIX of each
/// input, through that input's own backend. Under level routing an input can
/// sit on a volume that refuses the punch while the output destination accepts
/// it, and the destination's capability says nothing about the source's.
/// Admitting the pass on the destination alone commits a restriction and writes
/// an output per slice while every consumed prefix stays allocated, so the pass
/// meant to relieve the volume ends up using more of it.
#[test]
fn tight_space_declines_when_an_input_backend_cannot_punch() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    let dir = tempfile::tempdir()?;
    let hot_dir = dir.path().join("hot");
    // The volume holding the inputs: ample, but it cannot punch.
    let hot = Arc::new(MemFs::with_capacity(u64::MAX));
    hot.set_punch_hole_supported(false);
    hot.create_dir_all(&hot_dir)?;
    // The volume holding the compaction output: punch-capable, and tight
    // enough that the space gate hands the merge to the tight-space path.
    let main = Arc::new(MemFs::with_capacity(u64::MAX));

    let hot_fs: Arc<dyn Fs> = hot.clone();
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(main.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..1,
        path: hot_dir.clone(),
        fs: hot_fs,
    }])
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    // The inputs live on the routed volume, so the destination's own pool is
    // nearly empty: squeeze it below what the merge would write.
    main.set_capacity(64 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    let sidecars = hot
        .read_dir(&hot_dir.join("tables"))?
        .into_iter()
        .filter(|e| e.file_name.contains("restrict-bound"))
        .count();
    assert_eq!(
        sidecars, 0,
        "no slice may commit a restriction against an input whose backend \
         cannot reclaim the prefix that restriction declares consumed",
    );
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "key {i} lost",
        );
    }
    Ok(())
}

/// On a relocating (KV-separated) tight-space merge the transient is the
/// RELOCATED BLOB PAYLOAD, which lands on the blob volume. Level routing can
/// put the SST destination on a different volume entirely, so sizing the slice
/// budget from the destination's free space scales by the wrong pool: an ample
/// destination beside a full `blobs/` yields a budget larger than every input
/// block, `tight_slice_boundaries` finds nothing to cut, and the pass that
/// exists to reclaim the constrained volume reclaims nothing at all.
#[test]
fn tight_space_budgets_blob_relocation_on_the_blob_volume() -> crate::Result<()> {
    let (_dir, tree, mem) = relocating_tree_on_a_tight_blob_volume()?;
    tree.major_compact(64 * 1024 * 1024, 4 * RELOC_KEYS)?;

    assert!(
        mem.punched_bytes() > 0,
        "the relocation must slice against the blob volume's free space and \
         reclaim stale blob prefixes there",
    );
    assert_relocated_values(&tree)
}

/// A relocating pass whose tables and blob files share one volume holds a
/// slice to the room both of its writes take there together; another tree
/// that took all but too little of it for the first key stops the pass before
/// a slice writes.
#[test]
fn tight_slice_on_a_shared_volume_counts_both_demands_against_its_room() -> crate::Result<()> {
    use crate::fs::Fs;
    use core::sync::atomic::Ordering;

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .with_kv_separation(Some(blob_reloc_kv_options()));
    let squeeze = Arc::clone(&config.tight_slice_squeeze);
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..BLOB_RELOC_KEYS {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..BLOB_RELOC_KEYS).step_by(2) {
        tree.insert(
            blob_reloc_key(i).as_bytes(),
            blob_reloc_value(i, 2),
            BLOB_RELOC_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    // The first slice as the pass cuts it.
    let version = tree.index.current_version();
    let inputs: Vec<Table> = version.iter_tables().cloned().collect();
    let ids: crate::HashSet<TableId> = inputs.iter().map(Table::id).collect();
    let stale_files = super::pick_blob_files_to_rewrite(&ids, &version, &blob_reloc_kv_options())?;
    let free = mem.available_space(dir.path())?;
    let relocated = super::Relocated::new(&stale_files, &blob_reloc_kv_options());
    drop(stale_files);
    let opts = super::Options::from_tree(
        &tree.index,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    let first = governed(
        &inputs,
        super::SliceCaps {
            tables: u64::MAX,
            blobs: u64::MAX,
            total: free,
        },
        &super::SliceOutputs {
            probe: &slice_probe(&opts, &inputs, 64 * 1024 * 1024)?,
            tombstones: crate::table::writer::RewriteLoad::default(),
            cut_keys: &[],
            domains: None,
        },
        Some(&relocated),
        &tree.index.config.comparator,
    )?;
    drop(opts);
    drop(inputs);
    drop(version);
    assert!(
        first.end.is_some() && first.tables.min(first.blobs) >= 2,
        "the fixture needs a pass in slices writing both"
    );
    assert!(
        first.tables + first.blobs <= free,
        "both writes of a slice are held to the one room: {} + {} past {free}",
        first.tables,
        first.blobs
    );

    // All of the room but a byte: not even the first key fits.
    squeeze.store(free - 1, Ordering::SeqCst);
    let punched = mem.punched_bytes();
    let result = tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Nothing,
        "{result:?}"
    );
    assert_eq!(mem.punched_bytes(), punched, "no slice ran");
    Ok(())
}

/// A strategy's merge naming a table the version does not hold is declined,
/// in a tree whose merges reckon the room of the blob files they rewrite as
/// in any other.
#[test]
fn merge_of_a_missing_table_in_a_blob_tree_is_declined() -> crate::Result<()> {
    use crate::compaction::{Choice, CompactionStrategy, Input, state::CompactionState};

    struct MergeMissing(Vec<TableId>);

    impl CompactionStrategy for MergeMissing {
        fn get_name(&self) -> &'static str {
            "MergeMissingTest"
        }

        fn choose(&self, _: &crate::version::Version, _: &Config, _: &CompactionState) -> Choice {
            Choice::Merge(Input {
                table_ids: self.0.iter().copied().collect(),
                dest_level: 1,
                canonical_level: 1,
                target_size: u64::MAX,
            })
        }
    }

    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(blob_reloc_kv_options()))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..10 {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    let mut ids = tree.index.current_version().l0().list_ids();
    ids.insert(TableId::MAX);

    let result = tree.compact(Arc::new(MergeMissing(ids.into_iter().collect())), 0)?;
    assert_eq!(result.action, crate::compaction::CompactionAction::Nothing);
    Ok(())
}

/// A merge that relocates blob values counts the blob files it wrote as they
/// lie on disk, their framing and metadata with the values, as it counts its
/// tables.
#[test]
fn relocating_merge_counts_its_blob_files_whole_as_written() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_kv_separation(Some(blob_reloc_kv_options()))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..BLOB_RELOC_KEYS {
        tree.insert(blob_reloc_key(i).as_bytes(), blob_reloc_value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..BLOB_RELOC_KEYS).step_by(2) {
        tree.insert(
            blob_reloc_key(i).as_bytes(),
            blob_reloc_value(i, 2),
            BLOB_RELOC_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;
    // The first merge learns which frames died; the second relocates.
    tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    let before: crate::HashSet<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();

    let result = tree.major_compact(64 * 1024 * 1024, BLOB_RELOC_WATERMARK)?;
    let version = tree.index.current_version();
    let mut created = 0;
    let mut relocated = false;
    for file in version.blob_files.iter() {
        if !before.contains(&file.id()) {
            relocated = true;
            created += file.physical_size()?;
        }
    }
    assert!(relocated, "the fixture needs the merge to relocate");
    let tables: u64 = version.iter_tables().map(Table::file_len).sum();
    assert_eq!(result.work.measured.written, tables + created);
    Ok(())
}

/// A relocation that copies the frames of ingested values as stored lists
/// each at the sequence number its table gives it: an ingestion writes its
/// blob entries before it is given one, so the copy carries the table's, as a
/// re-encoded copy does, and the new file's listings agree with its tables.
/// The keys are held at their real seqnos, not zeroed below the watermark,
/// so the two differ.
#[test]
fn relocating_ingested_frames_lists_them_at_their_table_seqno() -> crate::Result<()> {
    const KEYS: u64 = 400;
    let folder = crate::get_tmp_folder();
    let tree = match crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(blob_reloc_kv_options().block_size(1_024)))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    // Values above the block size: a frame each, copied as stored.
    let value = |i: u64, generation: u8| blob_reloc_value(i, generation).repeat(10);
    // A write first, so the ingestion is given a seqno other than zero, and a
    // range tombstone under it that deletes nothing written after it but
    // keeps the keys it covers at their real seqnos through the merges.
    tree.insert("a", value(0, 0), 0);
    tree.remove_range("key", "kez", 0);
    tree.flush_active_memtable(0)?;
    let mut ingestion = crate::blob_tree::ingest::BlobIngestion::new(&tree)?;
    for i in 0..KEYS {
        ingestion.write(blob_reloc_key(i).into_bytes().into(), value(i, 1).into())?;
    }
    ingestion.finish()?;
    let Some(ingested) = tree.get_highest_seqno() else {
        panic!("the ingestion is given a seqno");
    };
    assert!(ingested > 0);
    for i in (0..KEYS).step_by(2) {
        tree.insert(blob_reloc_key(i), value(i, 2), ingested + 1 + i);
    }
    tree.flush_active_memtable(0)?;
    let watermark = ingested + 2 * KEYS;
    // The first merge learns which frames died; the second relocates.
    tree.major_compact(64 * 1024 * 1024, watermark)?;
    let before: crate::HashSet<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    tree.major_compact(64 * 1024 * 1024, watermark)?;

    let version = tree.index.current_version();
    let mut relocated = 0;
    for file in version
        .blob_files
        .iter()
        .filter(|f| !before.contains(&f.id()))
    {
        let scanner = crate::vlog::blob_file::scanner::Scanner::new(
            &file.0.path,
            &crate::fs::StdFs,
            file.id(),
        )?;
        for entry in scanner {
            let entry = entry?;
            assert_eq!(
                entry.seqno, ingested,
                "{:?} listed at its table's seqno",
                entry.key
            );
            relocated += 1;
        }
    }
    assert!(relocated > 0, "the fixture needs the merge to relocate");
    Ok(())
}

/// A relocation never copies whole a block whose values a key domain set
/// after it was written splits: copied as stored, it would carry keys of two
/// domains into one file, which expiring either could then not drop. Its
/// values are written anew, each file held to one domain, as any written
/// value is. Only a file that compresses its values writes such blocks: one
/// that compresses nothing packs them in records.
#[test]
#[cfg(feature = "lz4")]
fn relocation_rewrites_a_block_a_key_domain_splits() -> crate::Result<()> {
    const KEYS: u64 = 400;
    const BOUNDARY: u64 = 202;

    /// Two domains, split at `BOUNDARY`'s key.
    struct Split;
    impl crate::KeyDomains for Split {
        fn domain_end(&self, key: &[u8]) -> Option<crate::UserKey> {
            let boundary = blob_reloc_key(BOUNDARY);
            (key < boundary.as_bytes()).then(|| crate::UserKey::from(boundary.as_bytes()))
        }
    }

    let folder = crate::get_tmp_folder();
    let open = |domains: bool| -> crate::Result<crate::BlobTree> {
        let mut config = crate::Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .blob_compression(crate::CompressionType::Lz4)
        .with_kv_separation(Some(blob_reloc_kv_options().block_size(1_024)));
        if domains {
            config = config.key_domains(alloc::sync::Arc::new(Split));
        }
        match config.open()? {
            crate::AnyTree::Blob(t) => Ok(t),
            crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
        }
    };
    // Values below the block size, several to a block, written before the
    // domains: a block holds keys on both sides of the boundary.
    {
        let tree = open(false)?;
        for i in 0..KEYS {
            tree.insert(blob_reloc_key(i), blob_reloc_value(i, 1), 0);
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = open(true)?;
    // The first quarter shadowed in a run: its blocks die whole and the file
    // turns stale, while the rest stay whole and live.
    for i in 0..KEYS / 4 {
        tree.insert(blob_reloc_key(i), blob_reloc_value(i, 2), 1);
    }
    tree.flush_active_memtable(0)?;
    let before: crate::HashSet<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    tree.major_compact(64 * 1024 * 1024, 2)?;
    tree.major_compact(64 * 1024 * 1024, 2)?;

    // The files the relocation wrote: the ones written before the domains
    // hold keys of both by construction.
    let boundary = blob_reloc_key(BOUNDARY);
    let (mut relocated, mut lower, mut upper) = (0, false, false);
    for file in tree
        .index
        .current_version()
        .blob_files
        .iter()
        .filter(|file| !before.contains(&file.id()))
    {
        let scanner = crate::vlog::blob_file::scanner::Scanner::new(
            &file.0.path,
            &crate::fs::StdFs,
            file.id(),
        )?;
        let keys = scanner
            .map(|entry| entry.map(|entry| entry.key))
            .collect::<crate::Result<Vec<_>>>()?;
        let below = keys
            .iter()
            .filter(|key| ***key < *boundary.as_bytes())
            .count();
        assert!(
            below == 0 || below == keys.len(),
            "file {} holds keys of both domains",
            file.id()
        );
        lower |= below > 0;
        upper |= below < keys.len();
        relocated += keys.len();
    }
    assert!(
        relocated > 0 && lower && upper,
        "the fixture needs the merge to relocate keys of both domains"
    );
    for i in 0..KEYS {
        let expected = blob_reloc_value(i, if i < KEYS / 4 { 2 } else { 1 });
        assert_eq!(
            tree.get(blob_reloc_key(i), crate::SeqNo::MAX)?.as_deref(),
            Some(&expected[..]),
            "key {i} reads back"
        );
    }
    Ok(())
}

/// A zstd blob file compresses its blocks with a dictionary of its own. A
/// relocation that copies such a block whole lands it in a file recording the
/// source's codec, which must then hold that dictionary: copied without it,
/// the output file names a dictionary the tree does not have, and the pass
/// fails before it installs.
#[cfg(zstd_any)]
#[test]
fn relocation_copies_whole_blocks_of_a_file_with_its_own_dictionary() -> crate::Result<()> {
    const KEYS: u64 = 2_000;

    /// Text-like values, so the file's dictionary trains on them and the
    /// blocks after it compress under it.
    fn value(i: u64, generation: u8) -> Vec<u8> {
        format!("generation {generation} record {i:08} status=ok region=eu-west payload ")
            .repeat(3)
            .into_bytes()
    }

    let folder = crate::get_tmp_folder();
    let open = || -> crate::Result<crate::BlobTree> {
        match crate::Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .blob_compression(crate::CompressionType::Zstd(3))
        .with_kv_separation(Some(blob_reloc_kv_options().block_size(1_024)))
        .open()?
        {
            crate::AnyTree::Blob(t) => Ok(t),
            crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
        }
    };
    let tree = open()?;
    for i in 0..KEYS {
        tree.insert(blob_reloc_key(i), value(i, 1), 0);
    }
    tree.flush_active_memtable(0)?;
    // Runs of keys shadowed: each file turns stale, the blocks the runs fall
    // in are written anew, and the blocks between them stay whole and live,
    // copied as they were stored.
    let shadowed = |i: u64| i % 64 < 10;
    for i in (0..KEYS).filter(|i| shadowed(*i)) {
        tree.insert(blob_reloc_key(i), value(i, 2), 1);
    }
    tree.flush_active_memtable(0)?;
    let first: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .filter(|file| matches!(file.compression(), crate::CompressionType::ZstdDict { .. }))
        .map(crate::vlog::BlobFile::id)
        .collect();
    assert!(
        !first.is_empty(),
        "the fixture needs files with their own dictionary"
    );
    tree.major_compact(64 * 1024 * 1024, 2)?;
    tree.major_compact(64 * 1024 * 1024, 2)?;
    assert!(
        tree.index
            .current_version()
            .blob_files
            .iter()
            .all(|file| !first.contains(&file.id())),
        "the files with their own dictionary are relocated"
    );

    // Read back as written, and again after a reopen, which loads each
    // file's dictionary from the file itself.
    let read_back = |tree: &crate::BlobTree| -> crate::Result<()> {
        for i in 0..KEYS {
            let expected = value(i, if shadowed(i) { 2 } else { 1 });
            assert_eq!(
                tree.get(blob_reloc_key(i), crate::SeqNo::MAX)?.as_deref(),
                Some(&expected[..]),
                "key {i} reads back"
            );
        }
        Ok(())
    };
    read_back(&tree)?;
    drop(tree);
    read_back(&open()?)?;
    Ok(())
}

/// Two blob files whose own dictionaries differ but share a 32-bit id are
/// relocated in one pass: each source's frames and blocks land in a file that
/// holds that source's dictionary, never the other's, so every value still
/// decodes. The id is a truncated hash of content the user writes, so such a
/// pair is constructible: 64 KiB of one repeated `u64` against another.
#[cfg(zstd_any)]
#[test]
fn relocation_keeps_apart_own_dictionaries_that_share_an_id() -> crate::Result<()> {
    const KEYS: u64 = 40;

    /// 8000 bytes of `word` repeated: eight and a bit of these fill a file's
    /// 64 KiB of samples with nothing but `word`.
    fn value(word: u64) -> Vec<u8> {
        word.to_le_bytes().repeat(1_000)
    }

    let folder = crate::get_tmp_folder();
    let tree = match crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Zstd(3))
    .with_kv_separation(Some(blob_reloc_kv_options()))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    let words = [31_195_u64, 33_067];
    for (prefix, word) in ["a", "b"].into_iter().zip(words) {
        for i in 0..KEYS {
            tree.insert(format!("{prefix}{i:04}"), value(word), 0);
        }
        tree.flush_active_memtable(0)?;
    }
    let ids: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::compression)
        .collect();
    assert!(
        matches!(
            ids.as_slice(),
            [
                crate::CompressionType::ZstdDict { dict_id: a, .. },
                crate::CompressionType::ZstdDict { dict_id: b, .. },
            ] if a == b
        ),
        "the fixture needs two own dictionaries under one id: {ids:?}"
    );
    // A run shadowed in each file: both turn stale and are relocated together.
    for prefix in ["a", "b"] {
        for i in 0..KEYS / 4 {
            tree.insert(format!("{prefix}{i:04}"), value(7), 1);
        }
    }
    tree.flush_active_memtable(0)?;
    let written: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    tree.major_compact(64 * 1024 * 1024, 2)?;
    tree.major_compact(64 * 1024 * 1024, 2)?;

    // Every file holding a source's values holds that source's dictionary.
    let version = tree.index.current_version();
    let relocated: Vec<_> = version
        .blob_files
        .iter()
        .filter(|file| !written.contains(&file.id()))
        .collect();
    assert!(!relocated.is_empty(), "the fixture needs a relocation");
    for file in relocated {
        let Some(dict) = &file.0.zstd_dictionary else {
            assert!(
                !matches!(file.compression(), crate::CompressionType::ZstdDict { .. }),
                "file {} records a dictionary codec but holds no dictionary",
                file.id()
            );
            continue;
        };
        let min = file.0.meta.key_range.min().first().copied();
        let max = file.0.meta.key_range.max().first().copied();
        for (prefix, word) in b"ab".iter().copied().zip(words) {
            if min == Some(prefix) || max == Some(prefix) {
                assert_eq!(
                    dict.raw().get(..8),
                    Some(&word.to_le_bytes()[..]),
                    "file {} holds {} values under another source's dictionary",
                    file.id(),
                    char::from(prefix)
                );
            }
        }
    }

    for (prefix, word) in ["a", "b"].into_iter().zip(words) {
        for i in 0..KEYS {
            let expected = value(if i < KEYS / 4 { 7 } else { word });
            assert_eq!(
                tree.get(format!("{prefix}{i:04}"), crate::SeqNo::MAX)?
                    .as_deref(),
                Some(&expected[..]),
                "{prefix}{i:04} reads back"
            );
        }
    }
    Ok(())
}

/// A relocation over more files with their own dictionaries than it holds
/// copies open writes the values of the rest anew: an output that copies a
/// file's frames holds that file's dictionary, so one per source would hold a
/// file open per source, however many there are.
#[cfg(zstd_any)]
#[test]
fn relocation_holds_no_more_dictionary_outputs_than_copy_sources() -> crate::Result<()> {
    const FILES: u64 = 20;
    const KEYS: u64 = 12;

    /// 8000 bytes of `word` repeated: twelve fill a file's dictionary samples.
    fn value(word: u64) -> Vec<u8> {
        word.to_le_bytes().repeat(1_000)
    }

    let folder = crate::get_tmp_folder();
    let tree = match crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Zstd(3))
    .with_kv_separation(Some(blob_reloc_kv_options()))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for file in 0..FILES {
        for i in 0..KEYS {
            tree.insert(format!("{file:02}-{i:04}"), value(1_000 + file), 0);
        }
        tree.flush_active_memtable(0)?;
    }
    assert!(
        tree.index
            .current_version()
            .blob_files
            .iter()
            .all(crate::vlog::BlobFile::has_own_dictionary),
        "the fixture needs files with their own dictionaries"
    );
    // Half of each file shadowed: all turn stale and relocate together.
    for file in 0..FILES {
        for i in 0..KEYS / 2 {
            tree.insert(format!("{file:02}-{i:04}"), value(7), 1);
        }
    }
    tree.flush_active_memtable(0)?;
    let written: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    tree.major_compact(64 * 1024 * 1024, 2)?;
    tree.major_compact(64 * 1024 * 1024, 2)?;

    let relocated = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .filter(|file| !written.contains(&file.id()))
        .count();
    // At most 16 outputs hold a source's dictionary, as many as the writer
    // copies blocks of; the rest share an output of their own codec.
    assert!(
        relocated > 0 && relocated <= 16 + 2,
        "{relocated} relocated files for {FILES} sources"
    );
    for file in 0..FILES {
        for i in 0..KEYS {
            let expected = value(if i < KEYS / 2 { 7 } else { 1_000 + file });
            assert_eq!(
                tree.get(format!("{file:02}-{i:04}"), crate::SeqNo::MAX)?
                    .as_deref(),
                Some(&expected[..]),
                "{file:02}-{i:04} reads back"
            );
        }
    }
    Ok(())
}

/// A relocation holds the rows after a pointer into a block until the block's
/// values are all seen, and no more than a bound of them: past it the blocks
/// still waiting are given up, their values written anew, and the rows go
/// out. Here more than the bound of inline rows lies between two values of
/// one block, and every row and value still reads back after the pass.
#[test]
fn relocation_gives_up_a_block_whose_values_lie_past_the_held_bound() -> crate::Result<()> {
    const KEYS: u64 = 40;
    const ROWS: u64 = 140_000;

    let folder = crate::get_tmp_folder();
    let tree = match crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(blob_reloc_kv_options().block_size(1_024)))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..KEYS {
        tree.insert(blob_reloc_key(i), blob_reloc_value(i, 1), 0);
    }
    tree.flush_active_memtable(0)?;
    let first: Vec<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    // Inline rows sorting between the first block's first two values, more
    // bytes of them than a relocation holds back.
    let row = |j: u64| format!("{}/{j:08}", blob_reloc_key(0));
    for j in 0..ROWS {
        tree.insert(row(j), [b'r'; 40], 1);
    }
    // The second half shadowed: the file turns stale and is relocated.
    for i in KEYS / 2..KEYS {
        tree.insert(blob_reloc_key(i), blob_reloc_value(i, 2), 1);
    }
    tree.flush_active_memtable(0)?;
    let given_up =
        || crate::compaction::flavour::BLOCKS_GIVEN_UP.load(core::sync::atomic::Ordering::Relaxed);
    let before = given_up();
    tree.major_compact(64 * 1024 * 1024, 2)?;
    tree.major_compact(64 * 1024 * 1024, 2)?;
    assert!(given_up() > before, "the pass gives up a block");

    assert!(
        tree.index
            .current_version()
            .blob_files
            .iter()
            .all(|file| !first.contains(&file.id())),
        "the first generation is relocated"
    );
    for i in 0..KEYS {
        let expected = blob_reloc_value(i, if i < KEYS / 2 { 1 } else { 2 });
        assert_eq!(
            tree.get(blob_reloc_key(i), crate::SeqNo::MAX)?.as_deref(),
            Some(&expected[..]),
            "key {i} reads back"
        );
    }
    for j in [0, ROWS / 2, ROWS - 1] {
        assert_eq!(
            tree.get(row(j), crate::SeqNo::MAX)?.as_deref(),
            Some(&[b'r'; 40][..]),
            "row {j} reads back"
        );
    }
    Ok(())
}

/// [`relocating_ingested_frames_lists_them_at_their_table_seqno`] for values
/// that share blocks: a block of ingested values, whole and live, would be
/// copied as stored, its entries listed at the zero the ingestion wrote them
/// under. A block whose claimed values are listed at another seqno than their
/// table's is written anew instead, each value at its table's seqno, as a
/// block below the copy share is. Only a file that compresses its values
/// writes such blocks: one that compresses nothing packs them in records.
#[test]
#[cfg(feature = "lz4")]
fn relocating_ingested_blocks_lists_them_at_their_table_seqno() -> crate::Result<()> {
    const KEYS: u64 = 400;
    let folder = crate::get_tmp_folder();
    let tree = match crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Lz4)
    .with_kv_separation(Some(blob_reloc_kv_options().block_size(1_024)))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    // Values below the block size: several to a block.
    let value = blob_reloc_value;
    // As in the frame test: a seqno other than zero for the ingestion, and a
    // range tombstone that keeps the covered keys at their real seqnos.
    tree.insert("a", value(0, 0), 0);
    tree.remove_range("key", "kez", 0);
    tree.flush_active_memtable(0)?;
    let mut ingestion = crate::blob_tree::ingest::BlobIngestion::new(&tree)?;
    for i in 0..KEYS {
        ingestion.write(blob_reloc_key(i).into_bytes().into(), value(i, 1).into())?;
    }
    ingestion.finish()?;
    let Some(ingested) = tree.get_highest_seqno() else {
        panic!("the ingestion is given a seqno");
    };
    assert!(ingested > 0);
    // The first half shadowed in a run: its blocks die whole and the file
    // turns stale, while the second half's blocks stay whole and live, the
    // share a block is copied whole at.
    for i in 0..KEYS / 2 {
        tree.insert(blob_reloc_key(i), value(i, 2), ingested + 1 + i);
    }
    tree.flush_active_memtable(0)?;
    let watermark = ingested + 2 * KEYS;
    // The first merge learns which values died; the second relocates.
    tree.major_compact(64 * 1024 * 1024, watermark)?;
    let before: crate::HashSet<_> = tree
        .index
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();
    tree.major_compact(64 * 1024 * 1024, watermark)?;

    let version = tree.index.current_version();
    let mut relocated = 0;
    for file in version
        .blob_files
        .iter()
        .filter(|f| !before.contains(&f.id()))
    {
        let scanner = crate::vlog::blob_file::scanner::Scanner::new(
            &file.0.path,
            &crate::fs::StdFs,
            file.id(),
        )?;
        for entry in scanner {
            let entry = entry?;
            assert_eq!(
                entry.seqno, ingested,
                "{:?} listed at its table's seqno",
                entry.key
            );
            assert!(entry.block.is_some(), "{:?} shares a block", entry.key);
            relocated += 1;
        }
    }
    assert!(relocated > 0, "the fixture needs the merge to relocate");
    for i in 0..KEYS {
        let expected = value(i, if i < KEYS / 2 { 2 } else { 1 });
        assert_eq!(
            tree.get(blob_reloc_key(i), crate::SeqNo::MAX)?.as_deref(),
            Some(&expected[..]),
            "key {i} reads back"
        );
    }
    Ok(())
}

/// A block alone past the room the pass found makes a slice no cut can bring
/// within it. The pass runs the slices before it and stops there, the rest
/// of its inputs left in place, rather than write past the room.
#[test]
fn tight_space_pass_stops_before_a_slice_past_its_room() -> crate::Result<()> {
    const ROOM: u64 = 48 * 1_024;
    let big = |i: u64| -> Vec<u8> {
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (0..4 * ROOM)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s.to_le_bytes()[3]
            })
            .collect()
    };

    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(Arc::new(mem.clone()))
    .open()?
    else {
        panic!("expected Standard tree");
    };
    // Two overlapping flushes, so the major compaction merges them.
    for round in 0..2u64 {
        for i in 0..TIGHT_SPACE_KEYS {
            let value = if i == TIGHT_SPACE_KEYS / 2 {
                big(round)
            } else {
                vec![0xCD; 64]
            };
            tree.insert(
                tight_space_key(i).as_bytes(),
                value,
                round * TIGHT_SPACE_KEYS + i,
            );
        }
        tree.flush_active_memtable(0)?;
    }
    mem.set_capacity(tree.storage_stats()?.used_bytes + ROOM);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert!(mem.punched_bytes() > 0, "the slices before it ran");
    assert!(
        tree.current_version()
            .level(0)
            .is_some_and(|level| !level.is_empty()),
        "the inputs from the slice past the room on stay in place",
    );
    for i in 0..TIGHT_SPACE_KEYS {
        let expected = if i == TIGHT_SPACE_KEYS / 2 {
            big(1)
        } else {
            vec![0xCD; 64]
        };
        assert_eq!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i}",
        );
    }
    Ok(())
}

/// Live blob bytes are not spread over the keys as the table bytes are: here
/// the first tenth of the keys holds most of them. A slice cut by its share
/// of the tables relocates far more than that share of the blob files, past
/// the room it holds on the blob volume; each slice is cut by the frames it
/// moves, and none writes there past its room.
#[test]
fn tight_space_relocation_of_values_gathered_on_few_keys_fits_each_slice() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    const N: u64 = 4_000;
    let value = |i: u64, generation: u8| {
        let len = if i < N / 10 { 4_000 } else { 70 };
        let mut s = (i + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (u64::from(generation) << 1);
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s.to_le_bytes()[3]
            })
            .collect::<Vec<u8>>()
    };

    let dir = tempfile::tempdir()?;
    let cold_dir = dir.path().join("cold");
    let cold = Arc::new(MemFs::with_capacity(u64::MAX));
    cold.create_dir_all(&cold_dir)?;
    let mem = Arc::new(MemFs::with_capacity(u64::MAX));
    let cold_fs: Arc<dyn Fs> = cold;
    let pool = Arc::new(crate::CapacityPool::default());
    let tree = match Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .compaction_capacity(Arc::clone(&pool))
    .with_shared_fs(mem.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: cold_dir,
        fs: cold_fs,
    }])
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(48 * 1024),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    for i in 0..N {
        tree.insert(reloc_key(i).as_bytes(), value(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..N).step_by(2) {
        tree.insert(reloc_key(i).as_bytes(), value(i, 2), N + i);
    }
    tree.flush_active_memtable(0)?;
    let gc_watermark = 4 * N;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    const PROBE: u64 = 1 << 40;
    mem.set_capacity(PROBE);
    let blob_stored = PROBE - mem.available_space(dir.path())?;
    mem.set_capacity(blob_stored + blob_stored / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;

    let blob_dir = dir.path().join(crate::file::BLOBS_FOLDER);
    let before = pool.excess_on(&*mem, &blob_dir);
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;
    assert!(mem.punched_bytes() > 0, "the pass reclaimed in slices");
    assert_eq!(
        pool.excess_on(&*mem, &blob_dir) - before,
        0,
        "a slice relocated past the room it held on the blob volume",
    );
    for i in 0..N {
        let generation = if i % 2 == 0 { 2 } else { 1 };
        assert_eq!(
            tree.get(reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(value(i, generation).as_slice()),
            "key {i}",
        );
    }
    Ok(())
}

/// A relocating tight-space pass punches each stale blob file through the
/// blob volume's own backend: one that cannot punch declines the pass, which
/// would otherwise leave every relocated prefix allocated beside its copy.
#[test]
fn tight_space_declines_when_the_blob_backend_cannot_punch() -> crate::Result<()> {
    let (_dir, tree, mem) = relocating_tree_on_a_tight_blob_volume()?;
    mem.set_punch_hole_supported(false);
    let result = tree.major_compact(64 * 1024 * 1024, 4 * RELOC_KEYS)?;

    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Nothing,
        "{result:?}"
    );
    assert_eq!(mem.punched_bytes(), 0);
    assert_relocated_values(&tree)
}

const RELOC_KEYS: u64 = 4_000;

fn reloc_key(i: u64) -> String {
    format!("key{i:08}")
}

/// High-entropy values so the blobs do not compress away and the relocation
/// transient is real.
fn reloc_value(i: u64, generation: u8) -> Vec<u8> {
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
}

/// Every key reads the generation written last.
fn assert_relocated_values(tree: &crate::BlobTree) -> crate::Result<()> {
    for i in 0..RELOC_KEYS {
        let expected = reloc_value(i, if i % 2 == 0 { 2 } else { 1 });
        assert_eq!(
            tree.get(reloc_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(expected.as_slice()),
            "key {i} wrong/lost after the relocating tight-space merge",
        );
    }
    Ok(())
}

/// A KV-separated tree whose tables live on an ample routed volume and whose
/// blob files, half stale, live on a volume squeezed so that relocating them
/// takes the tight-space path.
fn relocating_tree_on_a_tight_blob_volume()
-> crate::Result<(tempfile::TempDir, crate::BlobTree, Arc<crate::fs::MemFs>)> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    const N: u64 = RELOC_KEYS;
    let (k, val) = (reloc_key, reloc_value);

    let dir = tempfile::tempdir()?;
    let cold_dir = dir.path().join("cold");
    // The SST volume: ample, and a different volume from the blobs.
    let cold = Arc::new(MemFs::with_capacity(u64::MAX));
    cold.create_dir_all(&cold_dir)?;
    // The blob volume, which is the one that runs out of space.
    let mem = Arc::new(MemFs::with_capacity(u64::MAX));

    let cold_fs: Arc<dyn Fs> = cold;
    let config = Config::new(
        &dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(mem.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..7,
        path: cold_dir,
        fs: cold_fs,
    }])
    .with_kv_separation(Some(
        KvSeparationOptions::default()
            .separation_threshold(64)
            .age_cutoff(1.0)
            .staleness_threshold(0.1)
            .file_target_size(48 * 1024),
    ));
    let tree = match config.open()? {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    for i in 0..N {
        tree.insert(k(i).as_bytes(), val(i, 1), i);
    }
    tree.flush_active_memtable(0)?;
    for i in (0..N).step_by(2) {
        tree.insert(k(i).as_bytes(), val(i, 2), N + i);
    }
    tree.flush_active_memtable(0)?;

    // One ample merge so the shadowed generation is COUNTED dead, leaving each
    // gen-1 file half stale — the precondition for relocating it.
    let gc_watermark = 4 * N;
    tree.index.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.storage_limit_bytes = None;
    })?;
    tree.major_compact(64 * 1024 * 1024, gc_watermark)?;

    // Squeeze ONLY the blob volume. The SST destination stays unbounded, so a
    // budget taken from it is meaningless here.
    const PROBE: u64 = 1 << 40;
    mem.set_capacity(PROBE);
    let blob_stored = PROBE - mem.available_space(dir.path())?;
    mem.set_capacity(blob_stored + blob_stored / 4);
    tree.index.update_runtime_config(|c| {
        c.tight_space_compaction = true;
    })?;
    Ok((dir, tree, mem))
}

/// Punching an input reclaims space on the INPUT'S volume. Under level routing
/// that can be a different volume from the destination, and each slice adds its
/// whole output to the destination while freeing nothing there: a destination
/// under pressure grows across slices until it hits ENOSPC, having never
/// relieved the pool that engaged tight-space mode. Being punch-capable is not
/// enough — the punch has to land where the outputs do.
#[test]
fn tight_space_declines_when_punching_inputs_frees_the_wrong_volume() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    let dir = tempfile::tempdir()?;
    let hot_dir = dir.path().join("hot");
    // The volume holding the inputs: ample AND punch-capable, so only the
    // volume comparison can decline this pass.
    let hot = Arc::new(MemFs::with_capacity(u64::MAX));
    hot.create_dir_all(&hot_dir)?;
    // The volume the slice outputs land on, squeezed below the merge.
    let main = Arc::new(MemFs::with_capacity(u64::MAX));

    let hot_fs: Arc<dyn Fs> = hot.clone();
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(main.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..1,
        path: hot_dir.clone(),
        fs: hot_fs,
    }])
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    main.set_capacity(64 * 1024);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert_eq!(
        hot.punched_bytes(),
        0,
        "punching the routed inputs would free the wrong volume: the constrained \
         destination gains every slice output and loses nothing",
    );
    let sidecars = hot
        .read_dir(&hot_dir.join("tables"))?
        .into_iter()
        .filter(|e| e.file_name.contains("restrict-bound"))
        .count();
    assert_eq!(sidecars, 0, "no slice may commit against such an input");
    for i in 0..TIGHT_SPACE_KEYS {
        assert!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .is_some(),
            "key {i} lost",
        );
    }
    Ok(())
}

/// A standalone walk learns a table is RESTRICTED from its `.restrict-bound`
/// sidecar. When reading that sidecar fails for a reason that says nothing
/// about the SST — a refused mount, an exhausted allocator — answering "not
/// restricted" sends the walk over the intentionally punched prefix and
/// reports its zeros as corruption on a HEALTHY table. The condition is the
/// operator's to fix, so it must be reported as what it is.
#[test]
fn a_refused_sidecar_read_is_reported_not_walked_as_corruption() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule, Fs};
    use crate::io::ErrorKind;

    let dir = tempfile::tempdir()?;
    let capacity = capfs::CapacityFs::new();
    let fault = FaultFs::new(capacity.clone());
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);
    let reopened = tight_space_crash_and_reopen(
        dir.path(),
        Arc::clone(&fs),
        |used| capacity.set_available_space(used / 4),
        || capacity.punched_bytes(),
    )?;
    let version = reopened.current_version();
    let Some(restricted) = version
        .iter_tables()
        .find(|t| t.restrict_lower_bound().is_some())
    else {
        panic!("the punched input must reopen as a restricted table");
    };
    let path = (*restricted.path).clone();

    // Baseline: with the sidecar readable the walk skips the punched prefix,
    // so the healthy table verifies clean. That is what the faulted run must
    // not diverge from by inventing corruption.
    let clean = crate::verify::verify_sst_file_with_fs(&fs, &path);
    assert!(
        clean.errors.is_empty(),
        "a legitimately punched table verifies clean: {:?}",
        clean.errors,
    );

    injector.arm(
        FaultRule::new(FaultOp::Open, Fault::Error(ErrorKind::PermissionDenied))
            .on_path(".restrict-bound"),
    );
    let report = crate::verify::verify_sst_file_with_fs(&fs, &path);
    injector.clear();

    assert!(
        !report.errors.is_empty(),
        "the refused read must be reported at all: {report:?}",
    );
    assert!(
        report
            .errors
            .iter()
            .all(|e| matches!(e, crate::verify::BlockVerifyError::SstFileUnreadable { .. })),
        "a refused sidecar read must be reported, not turned into corruption \
         findings over the punched prefix: {:?}",
        report.errors,
    );
    Ok(())
}

/// A routed merge whose inputs straddle two volumes is not automatically
/// hopeless: the slices reclaim the DESTINATION-local inputs as they go, so the
/// pass only has to absorb the remote share. Declining whenever a single remote
/// input exists leaves a space-constrained tree read-only with reclaimable
/// bytes sitting right there.
#[test]
fn tight_space_engages_when_the_local_share_covers_the_output() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    let dir = tempfile::tempdir()?;
    let hot_dir = dir.path().join("hot");
    let hot = Arc::new(MemFs::with_capacity(u64::MAX));
    hot.create_dir_all(&hot_dir)?;
    let main = Arc::new(MemFs::with_capacity(u64::MAX));

    // Moved, not cloned: only the destination volume is inspected afterwards.
    let hot_fs: Arc<dyn Fs> = hot;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(512))
    .with_shared_fs(main.clone())
    .level_routes(vec![LevelRoute {
        levels: 0..1,
        path: hot_dir.clone(),
        fs: hot_fs,
    }])
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    // First generation, compacted down so it lands on the DESTINATION volume.
    for i in 0..TIGHT_SPACE_KEYS {
        tree.insert(tight_space_key(i).as_bytes(), vec![0xCDu8; 64], i);
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, 0)?;
    let local_bytes: u64 = tree
        .current_version()
        .iter_tables()
        .filter(|t| t.path.starts_with(dir.path().join("tables")))
        .map(crate::table::Table::file_len)
        .sum();
    assert!(
        local_bytes > 0,
        "the first generation is on the destination"
    );

    // A small second generation on the ROUTED volume.
    for i in 0..64u64 {
        tree.insert(
            tight_space_key(i).as_bytes(),
            vec![0xEFu8; 64],
            TIGHT_SPACE_KEYS + i,
        );
    }
    tree.flush_active_memtable(0)?;
    let remote_bytes: u64 = tree
        .current_version()
        .iter_tables()
        .filter(|t| t.path.starts_with(&hot_dir))
        .map(crate::table::Table::file_len)
        .sum();
    assert!(
        remote_bytes > 0 && remote_bytes < local_bytes,
        "the routed share must be the smaller one ({remote_bytes} vs {local_bytes})",
    );

    // Free space covers the REMOTE share but not the whole merge: reclaiming
    // the local inputs slice by slice is exactly what makes this fit.
    let used = tree.storage_stats()?.used_bytes;
    main.set_capacity(used + remote_bytes * 2);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;

    tree.major_compact(64 * 1024 * 1024, 0)?;

    assert!(
        main.punched_bytes() > 0,
        "the destination-local inputs must be reclaimed rather than the pass \
         declining over the routed remainder",
    );
    for i in 0..64u64 {
        assert_eq!(
            tree.get(tight_space_key(i).as_bytes(), crate::MAX_SEQNO)?
                .as_deref(),
            Some(&[0xEFu8; 64][..]),
            "the newest generation survives the rewrite",
        );
    }
    Ok(())
}

/// A set that carries removals, so folding onto an assumed-empty base is
/// distinguishable from folding onto the real one. `0x02` removes, anything
/// else is a whole set to union in.
struct SetMerge;

impl crate::MergeOperator for SetMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> crate::Result<crate::UserValue> {
        let mut set: Vec<u8> = base_value.map_or_default(<[u8]>::to_vec);
        for operand in operands {
            match operand.split_first() {
                Some((0x02, rest)) => set.retain(|m| !rest.contains(m)),
                _ => {
                    for m in *operand {
                        if !set.contains(m) {
                            set.push(*m);
                        }
                    }
                }
            }
        }
        set.sort_unstable();
        Ok(set.into())
    }
}

/// Compacts exactly the named table back into the level it already sits in,
/// which is how a targeted rewrite (ECC self-heal) reaches the merge path.
struct RewriteOneTable(TableId, u8);

impl CompactionStrategy for RewriteOneTable {
    fn get_name(&self) -> &'static str {
        "RewriteOneTableTest"
    }

    fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
        Choice::Merge(Input {
            table_ids: core::iter::once(self.0).collect(),
            dest_level: self.1,
            canonical_level: self.1,
            target_size: u64::MAX,
        })
    }
}

/// Moves exactly the named tables to `dest`, which is how a strategy can hand
/// over a subset of a level (the built-in `MoveDown` always takes a whole one).
struct MoveTables(Vec<TableId>, u8);

impl CompactionStrategy for MoveTables {
    fn get_name(&self) -> &'static str {
        "MoveTablesTest"
    }

    fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
        Choice::Move(Input {
            table_ids: self.0.iter().copied().collect(),
            dest_level: self.1,
            canonical_level: self.1,
            target_size: u64::MAX,
        })
    }
}

/// Returns the ids of the tables currently on `level`, newest id first.
fn table_ids_on_level(tree: &crate::Tree, level: usize) -> Vec<TableId> {
    let version = tree.version_history.read().latest_version();
    let mut ids: Vec<TableId> = version.version.level(level).map_or_default(|level| {
        level
            .iter()
            .flat_map(|run| run.iter())
            .map(Table::id)
            .collect()
    });
    ids.sort_unstable_by(|a, b| b.cmp(a));
    ids
}

/// Moving only PART of L0 leaves the rest of L0 above the moved tables, so the
/// same reasoning applies there as to any other level: the read path takes the
/// best L0 answer before it ever looks lower, and an older leftover would mask
/// the newer version that was moved away.
#[test]
fn a_move_of_part_of_l0_does_not_leave_an_older_peer_above_it() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // Two L0 tables holding the same key, older flushed first.
    tree.insert("k", "old", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("k", "new", seqno.next());
    tree.flush_active_memtable(0)?;

    let ids = table_ids_on_level(&tree, 0);
    assert_eq!(2, ids.len(), "the recipe needs two L0 tables");
    #[expect(clippy::indexing_slicing, reason = "length asserted right above")]
    let newer = ids[0];

    // Move only the NEWER one down, leaving the older behind in L0.
    tree.compact(Arc::new(MoveTables(vec![newer], last_level)), seqno.get())?;

    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "the moved newer version must not be masked by the peer left in L0",
    );

    Ok(())
}

/// Overlapping tables moved together into an empty level stay in runs of
/// their own, newest first, rather than one run: a run below L0 is read as
/// disjoint, the lookup taking the first table whose range covers the key, so
/// one run of both would lose a key held by the other table. Kept apart, the
/// level reads as L0 does and finds every key.
#[test]
fn a_move_of_overlapping_tables_keeps_them_in_separate_runs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // One table spanning "a".."z", another holding only "m": their ranges
    // overlap while the keys do not, so one run cannot serve both.
    tree.insert("a", "1", seqno.next());
    tree.insert("z", "1", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("m", "1", seqno.next());
    tree.flush_active_memtable(0)?;

    let ids = table_ids_on_level(&tree, 0);
    assert_eq!(2, ids.len(), "the recipe needs two overlapping L0 tables");

    let result = tree.compact(Arc::new(MoveTables(ids, last_level)), seqno.get())?;

    assert_eq!(crate::compaction::CompactionAction::Moved, result.action);
    assert_eq!(
        2,
        tree.current_version()
            .level(usize::from(last_level))
            .map_or(0, |level| level.run_count()),
        "the two overlapping tables are kept in runs of their own",
    );

    for key in ["a", "m", "z"] {
        assert_eq!(
            Some(&b"1"[..]),
            tree.get(key, crate::MAX_SEQNO)?.as_deref(),
            "key {key} must still be readable after the move",
        );
    }

    Ok(())
}

/// Disjoint tables can still be handed over in an order that is not the
/// comparator's: the move collects them by walking levels, so a table from a
/// higher level comes first whatever its keys are. The destination run is read
/// by binary search over the tables' minimum keys, so an unsorted run loses the
/// keys of whichever table sits on the wrong side of the split.
#[test]
fn a_move_from_two_levels_sorts_the_run_it_lands_in() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // The LATER key goes to the higher level, so walking levels yields "z"
    // before "a" while the comparator orders them the other way round.
    tree.insert("a", "1", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), seqno.get())?;
    tree.insert("z", "1", seqno.next());
    tree.flush_active_memtable(0)?;

    let mut ids = table_ids_on_level(&tree, 0);
    ids.extend(table_ids_on_level(&tree, 1));
    assert_eq!(2, ids.len(), "the recipe needs one table on each level");

    let result = tree.compact(Arc::new(MoveTables(ids, last_level)), seqno.get())?;
    assert_eq!(
        crate::compaction::CompactionAction::Moved,
        result.action,
        "the tables are disjoint, so the move itself is sound",
    );

    for key in ["a", "z"] {
        assert_eq!(
            Some(&b"1"[..]),
            tree.get(key, crate::MAX_SEQNO)?.as_deref(),
            "key {key} must survive landing in a run built from two levels",
        );
    }

    Ok(())
}

/// Level routing turns a move into a rewrite, and a rewrite is exactly what
/// repairs inputs that cannot share a run: it reads all of them and emits one
/// sorted output. Refusing it would leave a strategy handing over an
/// overlapping L0 with nothing to do, forever.
#[test]
fn a_cross_folder_move_of_overlapping_tables_is_rewritten() -> crate::Result<()> {
    use crate::config::LevelRoute;
    use crate::fs::{Fs, MemFs};

    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let hot_dir = crate::path::PathBuf::from("/hot-tier");
    let hot = Arc::new(MemFs::with_capacity(u64::MAX));
    hot.create_dir_all(&hot_dir)?;
    let hot_fs: Arc<dyn Fs> = hot;

    let config = Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default())
        .with_shared_fs(Arc::new(MemFs::with_capacity(u64::MAX)))
        .level_routes(vec![LevelRoute {
            levels: 0..1,
            path: hot_dir,
            fs: hot_fs,
        }]);
    let last_level = config.level_count - 1;
    let crate::AnyTree::Standard(tree) = config.open()? else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // The same shape a trivial move must refuse: ranges overlap, keys do not.
    tree.insert("a", "1", seqno.next());
    tree.insert("z", "1", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("m", "1", seqno.next());
    tree.flush_active_memtable(0)?;

    let ids = table_ids_on_level(&tree, 0);
    assert_eq!(2, ids.len(), "the recipe needs two overlapping L0 tables");

    let result = tree.compact(Arc::new(MoveTables(ids, last_level)), seqno.get())?;
    assert_eq!(
        crate::compaction::CompactionAction::Merged,
        result.action,
        "routing rewrites the inputs, which consolidates them instead of \
         leaving them in one unreadable run",
    );

    for key in ["a", "m", "z"] {
        assert_eq!(
            Some(&b"1"[..]),
            tree.get(key, crate::MAX_SEQNO)?.as_deref(),
            "key {key} must survive the rewrite",
        );
    }

    Ok(())
}

/// The L0 exemption is decided per overlapping pair, so finding one pair that
/// qualifies says nothing about the next: a mover that is older than the first
/// peer it meets can still be newer than the second, and that second pair is
/// the one that hides a version. A check that stops at the first overlap it
/// finds would admit this move.
#[test]
fn a_move_out_of_l0_weighs_every_overlapping_peer_not_just_the_first() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // Oldest: the table that will be moved. It spans both peers below.
    tree.insert("a", "1", seqno.next());
    tree.insert("m", "mover", seqno.next());
    tree.flush_active_memtable(0)?;
    let mover = {
        let ids = table_ids_on_level(&tree, 0);
        assert_eq!(1, ids.len(), "the mover is the only table so far");
        #[expect(clippy::indexing_slicing, reason = "length asserted right above")]
        let id = ids[0];
        id
    };

    // Newer, and sorts FIRST by key: this is the pair the exemption clears.
    tree.insert("a", "newer", seqno.next());
    tree.flush_active_memtable(0)?;

    // The second peer: it overlaps the mover too, but holds a version OLDER
    // than the mover's, so the mover would end up below something it outranks.
    // Written with an explicit seqno under the mover's, since the tree's own
    // counter only moves forward.
    tree.insert("m", "older-peer", 0);
    tree.flush_active_memtable(0)?;

    let result = tree.compact(Arc::new(MoveTables(vec![mover], last_level)), seqno.get())?;
    assert_eq!(
        crate::compaction::CompactionAction::Nothing,
        result.action,
        "the second overlapping peer is not strictly newer, so the move hides a version",
    );

    assert_eq!(
        Some(&b"newer"[..]),
        tree.get("a", crate::MAX_SEQNO)?.as_deref(),
        "key a still reads the newest version",
    );
    assert_eq!(
        Some(&b"mover"[..]),
        tree.get("m", crate::MAX_SEQNO)?.as_deref(),
        "and key m still reads the version the move would have hidden",
    );

    Ok(())
}

/// An L0 table that holds only versions older than the peer it passes under
/// stays readable in both directions, because L0 is the one level read by
/// taking the best answer across every run before looking lower: the newer peer
/// wins while it is visible, and a snapshot that predates it falls through to
/// the level the older table moved to. Refusing this would stall a strategy
/// that hands over part of L0 forever.
#[test]
fn a_move_of_an_older_l0_table_under_a_newer_peer_still_runs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // Two L0 tables holding the same key, the older flushed first. Its seqno
    // range ends below where the newer one's begins, which is what makes the
    // move provably safe rather than merely plausible.
    let old_seqno = seqno.next();
    tree.insert("k", "old", old_seqno);
    tree.flush_active_memtable(0)?;
    let new_seqno = seqno.next();
    tree.insert("k", "new", new_seqno);
    tree.flush_active_memtable(0)?;

    let ids = table_ids_on_level(&tree, 0);
    assert_eq!(2, ids.len(), "the recipe needs two L0 tables");
    #[expect(clippy::indexing_slicing, reason = "length asserted right above")]
    let older = ids[1];

    // Move the OLDER one down, leaving the newer behind in L0 above it.
    let result = tree.compact(Arc::new(MoveTables(vec![older], last_level)), seqno.get())?;
    assert_eq!(
        crate::compaction::CompactionAction::Moved,
        result.action,
        "an overlap with a strictly newer L0 peer does not hide anything",
    );

    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "the newer version still answers the current read",
    );
    assert_eq!(
        Some(&b"old"[..]),
        tree.get("k", new_seqno)?.as_deref(),
        "and a snapshot predating it still reaches the moved older version",
    );

    Ok(())
}

/// A move upward inverts the precedence the move downward relies on, so the
/// levels it crosses are the ones BETWEEN the destination and the source. Read
/// in the downward direction that span is empty, which checks nothing at all.
#[test]
fn a_move_up_under_a_newer_table_is_refused() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // The older copy settles on L2, the newer one above it on L1.
    tree.insert("k", "old", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 2)), seqno.get())?;
    tree.insert("k", "new", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), seqno.get())?;

    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "the recipe needs the newer copy to win before the move under test",
    );

    // Lifting the older table up to L1 puts its run ahead of the newer one on
    // the same level, where the first covering run answers the read.
    let ids = table_ids_on_level(&tree, 2);
    assert_eq!(1, ids.len(), "the recipe needs exactly one table on L2");
    let result = tree.compact(Arc::new(MoveTables(ids, 1)), seqno.get())?;

    assert_eq!(
        crate::compaction::CompactionAction::Nothing,
        result.action,
        "a move that lifts an older table over a newer one must be declined",
    );
    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "and the newer version still answers the read",
    );

    Ok(())
}

/// The gate must only refuse what the move actually changes. An overlap on a
/// level ALREADY above the source keeps winning before and after, so declining
/// over it would stall a safe move forever.
#[test]
fn a_move_under_a_level_that_already_outranked_it_still_runs() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // The older copy goes to L2 first, then the newer one lands above it in L1.
    tree.insert("k", "old", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 2)), seqno.get())?;
    tree.insert("k", "new", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), seqno.get())?;

    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "the recipe needs the newer copy to win before the move under test",
    );

    // L2 -> L3 passes under nothing new: L1 outranked L2 before and still does.
    let result = tree.compact(Arc::new(crate::compaction::MoveDown(2, 3)), seqno.get())?;

    assert_eq!(
        crate::compaction::CompactionAction::Moved,
        result.action,
        "a move that changes no precedence must not be declined",
    );
    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "and the answer is unchanged by it",
    );

    Ok(())
}

/// Being at the last level does not by itself prove the compaction holds every
/// version of a key: a targeted rewrite can take one table out of a last level
/// that holds several overlapping runs, leaving the base behind in a run it
/// never read. Absence has to be proven from the inputs, not from the
/// destination.
#[test]
fn a_rewrite_of_one_last_level_table_does_not_prove_absence() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default())
        .with_merge_operator(Some(Arc::new(SetMerge)));
    let last_level = config.level_count - 1;
    let tree = config.open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };

    // The base, parked at the last level on its own run.
    tree.insert("k", [1u8, 2, 3], seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(
        Arc::new(crate::compaction::MoveDown(0, last_level)),
        seqno.get(),
    )?;

    // A second run at the SAME last level, overlapping the first and holding
    // only the operand. Merged rather than moved: a move under an overlapping
    // table is refused (it would hide what it moved), while a merge reads its
    // input and is free to land beside the run already there.
    tree.merge("k", [0x02u8, 2], seqno.next());
    tree.flush_active_memtable(0)?;
    let fresh = {
        let version = tree.version_history.read().latest_version();
        version
            .version
            .level(0)
            .and_then(|level| level.iter().flat_map(|run| run.iter()).map(Table::id).max())
            .ok_or_else(|| crate::Error::from(crate::io::Error::other("no L0 table")))?
    };
    tree.compact(Arc::new(RewriteOneTable(fresh, last_level)), seqno.get())?;

    let version = tree.version_history.read().latest_version();
    let level = version
        .version
        .level(last_level.into())
        .ok_or_else(|| crate::Error::from(crate::io::Error::other("last level is missing")))?;
    assert!(
        level.iter().count() >= 2,
        "the recipe needs two overlapping runs at the last level, else the \
         rewrite below would hold the base after all",
    );
    // The operand's table is the later one, so it carries the higher id.
    let operand_table = version
        .version
        .iter_tables()
        .map(Table::id)
        .max()
        .ok_or_else(|| crate::Error::from(crate::io::Error::other("no tables")))?;
    drop(version);

    tree.compact(
        Arc::new(RewriteOneTable(operand_table, last_level)),
        seqno.get(),
    )?;

    assert_eq!(
        Some(vec![1u8, 3]),
        tree.get("k", crate::MAX_SEQNO)?.map(|v| v.to_vec()),
        "the removal must still apply to the base the rewrite never read",
    );

    Ok(())
}

/// A trivial move relocates tables without reading them, which is only sound
/// while it keeps the ordering the point read depends on: a level walk stops at
/// the first level holding the key, so a table placed BELOW an overlapping one
/// must not hold the newer version. Moving newer tables under older ones turns
/// every read of those keys into the stale answer, with both tables intact on
/// disk. The move has to become a merge instead.
#[test]
fn a_move_beneath_an_older_overlapping_table_does_not_stale_the_read() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let config = Config::new(&dir, seqno.clone(), SequenceNumberCounter::default());
    let last_level = config.level_count - 1;
    let tree = config.open()?;

    // The older version, parked on an upper level.
    tree.insert("k", "old", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(Arc::new(crate::compaction::MoveDown(0, 1)), seqno.get())?;

    // The newer version, aimed at the last level: below the one above, which
    // still holds the same key.
    tree.insert("k", "new", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.compact(
        Arc::new(crate::compaction::MoveDown(0, last_level)),
        seqno.get(),
    )?;

    assert_eq!(
        Some(&b"new"[..]),
        tree.get("k", crate::MAX_SEQNO)?.as_deref(),
        "the newer version must win wherever the compaction put it",
    );

    Ok(())
}

/// `compaction_rate_limit` is bytes per second for the TREE. Every compaction
/// draws on one bucket, so what an earlier one spent is still spent when the
/// next one asks: a fresh bucket per invocation would multiply the configured
/// rate by however many compactions run, and a regular compaction takes only a
/// read lock, so several do run at once.
#[test]
fn compactions_of_one_tree_draw_on_one_budget() -> crate::Result<()> {
    use core::time::Duration;

    let dir = tempfile::tempdir()?;
    let rate = 1_024 * 1_024; // 1 MiB/s, so the burst ceiling is 1 MiB
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_rate_limit(rate)
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));

    // One compaction spends the whole bucket and then some: the debit lands
    // immediately and drives it into debt.
    let first = super::Options::from_tree(&tree, Arc::clone(&strategy));
    let owed = first.rate_limiter.acquire_wait(rate * 4, Duration::ZERO);
    assert!(
        !owed.is_zero(),
        "spending four seconds of budget at once must leave a debt to wait out",
    );

    // A second compaction, at the same instant, must meet that debt.
    let second = super::Options::from_tree(&tree, strategy);
    let next = second.rate_limiter.acquire_wait(1, Duration::ZERO);
    assert!(
        !next.is_zero(),
        "the budget the first compaction spent is still spent: a second \
         compaction starting at the same instant must wait, not find a full \
         bucket (waited {next:?})",
    );

    Ok(())
}

/// The one-second burst the constructor seeds is meant to be earned by idling,
/// not handed out afresh to every compaction. A tree that runs many short
/// compactions back to back must not collect a new burst each time.
#[test]
fn a_second_compaction_does_not_re_earn_the_burst() -> crate::Result<()> {
    use core::time::Duration;

    let dir = tempfile::tempdir()?;
    let rate = 1_024 * 1_024;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_rate_limit(rate)
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));

    // Drain exactly the burst, at t = 0. Nothing has to be waited out yet.
    let first = super::Options::from_tree(&tree, Arc::clone(&strategy));
    assert!(
        first
            .rate_limiter
            .acquire_wait(rate, Duration::ZERO)
            .is_zero(),
        "the seeded burst covers the first second of work",
    );

    // Still t = 0, so nothing has refilled. A fresh bucket here would hand out
    // the same burst again and let the tree issue 2 MiB in an instant.
    let second = super::Options::from_tree(&tree, strategy);
    let after = second.rate_limiter.acquire_wait(rate, Duration::ZERO);
    assert!(
        !after.is_zero(),
        "the burst is earned by idling, not granted per compaction: a second \
         compaction at the same instant must wait for the refill (waited \
         {after:?})",
    );

    Ok(())
}

/// What the budget bounds is work ADMITTED after waiting, not bytes debited —
/// a debit lands immediately and can drive the bucket into debt, so counting
/// debits would report work the limiter is in fact still holding back. Over an
/// interval the admitted total stays within `burst + rate × elapsed`, the
/// burst being the head start the constructor deliberately grants.
#[test]
fn admitted_work_stays_within_the_burst_plus_the_rate() -> crate::Result<()> {
    use core::time::Duration;

    let dir = tempfile::tempdir()?;
    let rate: u64 = 1_024 * 1_024;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_rate_limit(rate)
    .open()?
    {
        crate::AnyTree::Standard(t) => t,
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    };

    let chunk = 64 * 1_024u64;
    let mut now = Duration::ZERO;
    let mut admitted = 0u64;

    // Ten compactions in sequence, each asking for a few chunks. The clock
    // only advances by what the limiter made the caller wait, so nothing but
    // the limiter's own pacing bounds the total.
    for _ in 0..10 {
        let opts = super::Options::from_tree(
            &tree,
            Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
        );
        for _ in 0..4 {
            let wait = opts.rate_limiter.acquire_wait(chunk, now);
            now += wait;
            admitted += chunk;
        }
    }

    let ceiling =
        rate + rate * now.as_secs() + rate * u64::from(now.subsec_nanos()) / 1_000_000_000;
    assert!(
        admitted <= ceiling,
        "admitted {admitted} B over {now:?} exceeds burst + rate × elapsed = \
         {ceiling} B",
    );
    // And the pacing is real: 2.5 MiB at 1 MiB/s cannot have been instant.
    assert!(
        !now.is_zero(),
        "asking for more than the burst must have forced a wait",
    );

    Ok(())
}

/// A blob tree runs its compactions through its index tree, so it draws on
/// that tree's budget — not on one of its own, and not on a fresh one each
/// time.
#[test]
fn a_blob_tree_shares_the_index_trees_budget() -> crate::Result<()> {
    use core::time::Duration;

    let dir = tempfile::tempdir()?;
    let rate = 1_024 * 1_024;
    let tree = match Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .compaction_rate_limit(rate)
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };

    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));
    let first = super::Options::from_tree(&tree.index, Arc::clone(&strategy));
    assert!(
        !first
            .rate_limiter
            .acquire_wait(rate * 4, Duration::ZERO)
            .is_zero(),
    );

    let second = super::Options::from_tree(&tree.index, strategy);
    assert!(
        !second
            .rate_limiter
            .acquire_wait(1, Duration::ZERO)
            .is_zero(),
        "the blob tree's compactions run through the index tree, so they meet \
         the debt an earlier one left",
    );

    Ok(())
}

/// The unthrottled default must stay free: at a limit of `0` every request is
/// admitted immediately, however much is asked for and whenever it is asked.
#[test]
fn an_unlimited_tree_never_makes_a_compaction_wait() -> crate::Result<()> {
    use core::time::Duration;

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
    assert_eq!(
        tree.config.compaction_rate_limit, 0,
        "0 is the default, and this test is about that default",
    );

    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    for bytes in [1u64, 1 << 20, u64::MAX] {
        assert!(
            opts.rate_limiter
                .acquire_wait(bytes, Duration::ZERO)
                .is_zero(),
            "an unlimited tree must admit {bytes} B immediately",
        );
    }
    // Still immediate after the bucket would otherwise be deep in debt.
    assert!(
        opts.rate_limiter
            .acquire_wait(u64::MAX, Duration::from_secs(1))
            .is_zero(),
    );

    Ok(())
}

/// The budget is a live-process figure, not durable state: a reopened tree
/// starts fresh. Pinning it keeps a later change from quietly persisting debt
/// across restarts without saying so.
#[test]
fn a_reopened_tree_starts_with_a_fresh_budget() -> crate::Result<()> {
    use core::time::Duration;

    let dir = tempfile::tempdir()?;
    let rate = 1_024 * 1_024;
    let open = || -> crate::Result<crate::Tree> {
        match Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .compaction_rate_limit(rate)
        .open()?
        {
            crate::AnyTree::Standard(t) => Ok(t),
            crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
        }
    };
    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));

    {
        let tree = open()?;
        let opts = super::Options::from_tree(&tree, Arc::clone(&strategy));
        assert!(
            !opts
                .rate_limiter
                .acquire_wait(rate * 4, Duration::ZERO)
                .is_zero(),
        );
    }

    let tree = open()?;
    let opts = super::Options::from_tree(&tree, strategy);
    assert!(
        opts.rate_limiter.acquire_wait(1, Duration::ZERO).is_zero(),
        "a new process gets a new bucket; the debt does not outlive the tree",
    );

    Ok(())
}

/// Opens a standard tree at `dir` with the given configuration tail.
fn open_standard(
    dir: &std::path::Path,
    configure: impl FnOnce(Config) -> Config,
) -> crate::Result<crate::Tree> {
    match configure(Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    ))
    .open()?
    {
        crate::AnyTree::Standard(t) => Ok(t),
        crate::AnyTree::Blob(_) => panic!("expected Standard tree"),
    }
}

/// A row written as cells relocates the frames of the cells it references in
/// a stale file, and only those: a cell kept inline and a reference into a
/// live file move nothing, and a plain value moves nothing either. Its table
/// links every file it references and lists every object it owns.
#[test]
fn a_cell_row_relocates_its_references_into_stale_files() -> crate::Result<()> {
    use crate::blob_tree::field_row::{RowCell, RowField, encode_row};
    use crate::blob_tree::handle::BlobIndirection;
    use crate::vlog::ValueHandle;
    use crate::{InternalValue, vlog::BlobFileId};

    let indirection = |blob_file_id: u64, frame: u32, stored: u32| BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id,
            frame,
            index: 0,
        },
        size: stored,
        stored: crate::blob_tree::handle::Stored::Frames(stored),
    };
    let reference = |indirection| RowCell::Ref {
        indirection,
        owner: true,
    };
    let (first, other, second) = (
        indirection(7, 0, 100),
        indirection(8, 0, 300),
        indirection(7, 1, 40),
    );
    let row = encode_row(&[
        RowField::bytes(3, RowCell::Value(b"inline")),
        RowField::bytes(4, reference(first)),
        RowField::bytes(5, reference(other)),
        RowField::bytes(6, reference(second)),
    ])?;
    let relocated = super::Relocated {
        stale: core::iter::once::<BlobFileId>(7).collect(),
        rewritten: crate::HashSet::default(),
        target_size: u64::MAX,
        chunk_size: u32::MAX,
        file_groups: 1,
        groups: 1,
        own_dictionaries: core::iter::once::<(BlobFileId, u64)>((7, 4_096)).collect(),
        fresh_threshold: u64::MAX,
        fresh_dictionary: 0,
        largest_dictionary: 4_096,
    };
    let key = b"row-key";

    // Takes `item` into `load` on top of what `known` holds.
    let take = |load: &mut super::SliceLoad,
                known: &super::SliceLoad,
                item: &InternalValue,
                dictionaries: &mut Vec<BlobFileId>| {
        super::take_entry(
            &mut load.rows,
            super::NewShapes {
                known_links: &known.links,
                known_cells: &known.cells,
                known_fresh: known.fresh,
                links: &mut load.links,
                cells: &mut load.cells,
                fresh: &mut load.fresh,
            },
            (
                item.key.value_type,
                item.key.seqno,
                item.key.user_key.len(),
                &item.value,
            ),
            1,
            false,
            Some(&relocated),
            dictionaries,
        )
    };

    let item = InternalValue::from_components(*key, row, 1, crate::ValueType::CellRow);
    let mut dictionaries = Vec::new();
    let mut load = super::SliceLoad::default();
    let nothing = super::SliceLoad::default();
    assert_eq!(
        take(&mut load, &nothing, &item, &mut dictionaries)?,
        (
            super::frame_bytes(&first, u32::MAX) + super::frame_bytes(&second, u32::MAX),
            true
        ),
        "the row links blob files the slice does not link yet",
    );
    assert_eq!(
        dictionaries,
        vec![7, 7],
        "each reference into the stale file names the dictionary its copy may hold",
    );
    assert_eq!(
        load.links.len(),
        2,
        "the row links both files it references"
    );
    assert_eq!(
        load.rows.owned_blob_objects, 3,
        "the row owns its three objects"
    );
    assert_eq!(load.rows.entries, 1);
    let plain = InternalValue::from_components(*key, b"v".to_vec(), 2, crate::ValueType::Value);
    assert_eq!(
        take(&mut load, &nothing, &plain, &mut Vec::new())?,
        (0, false)
    );

    // On top of a slice that links both files already, the row links
    // nothing new: the slice's bound counts each file once.
    let mut more = super::SliceLoad::default();
    assert_eq!(
        take(&mut more, &load, &item, &mut Vec::new())?,
        (
            super::frame_bytes(&first, u32::MAX) + super::frame_bytes(&second, u32::MAX),
            false
        ),
    );
    assert!(more.links.is_empty(), "a known link is not taken again");
    Ok(())
}

/// A relocating pass writes a new pointer for a value it moves, and the new
/// one may be longer than the one read: another file id, another frame,
/// another stored length. The slice charges its table with the longest a
/// pointer can be for every reference into a file it relocates, alone or in
/// a cell row, and with the bytes read for one it leaves where it is.
#[test]
fn a_relocated_reference_is_charged_at_the_longest_its_new_pointer_can_be() -> crate::Result<()> {
    use crate::blob_tree::field_row::{RowCell, RowField, encode_row};
    use crate::blob_tree::handle::BlobIndirection;
    use crate::coding::Encode;
    use crate::vlog::ValueHandle;
    use crate::{InternalValue, vlog::BlobFileId};

    let indirection = |blob_file_id: BlobFileId| BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id,
            frame: 0,
            index: 0,
        },
        size: 100,
        stored: crate::blob_tree::handle::Stored::Frames(100),
    };
    let relocated = super::Relocated {
        stale: core::iter::once::<BlobFileId>(7).collect(),
        rewritten: crate::HashSet::default(),
        target_size: u64::MAX,
        chunk_size: u32::MAX,
        file_groups: 1,
        groups: 1,
        own_dictionaries: crate::HashMap::default(),
        fresh_threshold: u64::MAX,
        fresh_dictionary: 0,
        largest_dictionary: 0,
    };
    // The table's value bytes after taking `item` alone.
    let charged = |item: &InternalValue| {
        let mut load = super::SliceLoad::default();
        let known = super::SliceLoad::default();
        super::take_entry(
            &mut load.rows,
            super::NewShapes {
                known_links: &known.links,
                known_cells: &known.cells,
                known_fresh: known.fresh,
                links: &mut load.links,
                cells: &mut load.cells,
                fresh: &mut load.fresh,
            },
            (
                item.key.value_type,
                item.key.seqno,
                item.key.user_key.len(),
                &item.value,
            ),
            1,
            false,
            Some(&relocated),
            &mut Vec::new(),
        )?;
        crate::Result::Ok(load.rows.value_bytes)
    };
    let pointer = |blob_file_id| {
        let mut bytes = Vec::new();
        indirection(blob_file_id).encode_into(&mut bytes)?;
        crate::Result::Ok(bytes)
    };

    let (moved, kept) = (pointer(7)?, pointer(8)?);
    let read = moved.len() as u64;
    assert!(read < BlobIndirection::MAX_ENCODED_LEN);
    let item =
        |value: Vec<u8>, value_type| InternalValue::from_components(*b"key", value, 1, value_type);
    assert_eq!(
        charged(&item(moved, crate::ValueType::Indirection))?,
        BlobIndirection::MAX_ENCODED_LEN,
        "a pointer the pass writes anew",
    );
    assert_eq!(
        charged(&item(kept, crate::ValueType::Indirection))?,
        read,
        "a pointer the pass copies as it is",
    );

    let reference = |blob_file_id| RowCell::Ref {
        indirection: indirection(blob_file_id),
        owner: true,
    };
    let row = encode_row(&[
        RowField::bytes(3, reference(7)),
        RowField::bytes(4, reference(8)),
        RowField::bytes(5, reference(7)),
    ])?;
    let row_len = row.len() as u64;
    assert_eq!(
        charged(&item(row, crate::ValueType::CellRow))?,
        row_len + 2 * (BlobIndirection::MAX_ENCODED_LEN - read),
        "each of the two references the pass writes anew may grow to the longest",
    );
    Ok(())
}

/// An inline value the separation threshold reaches is written anew into a
/// blob file by a relocating pass: the slice charges the blob files with it
/// as written raw, and its table with the pointer left in its place. A value
/// below the threshold, and any value of a pass that relocates nothing, stays
/// in the table.
#[test]
fn an_inline_value_the_threshold_reaches_is_charged_to_the_blob_files() -> crate::Result<()> {
    use crate::{InternalValue, vlog::BlobFileId};

    let relocated = super::Relocated {
        stale: core::iter::once::<BlobFileId>(7).collect(),
        rewritten: crate::HashSet::default(),
        target_size: u64::MAX,
        chunk_size: u32::MAX,
        file_groups: 1,
        groups: 1,
        own_dictionaries: crate::HashMap::default(),
        fresh_threshold: 1_000,
        fresh_dictionary: 0,
        largest_dictionary: 0,
    };
    // Takes `item` into a slice that wrote a value anew already or not
    // (`known_fresh`): the blob bytes, whether the slice grew, its table's
    // value bytes and whether it now writes a value anew.
    let take = |item: &InternalValue, relocated: Option<&super::Relocated>, known_fresh: bool| {
        let mut load = super::SliceLoad::default();
        let known = super::SliceLoad::default();
        let blobs = super::take_entry(
            &mut load.rows,
            super::NewShapes {
                known_links: &known.links,
                known_cells: &known.cells,
                known_fresh,
                links: &mut load.links,
                cells: &mut load.cells,
                fresh: &mut load.fresh,
            },
            (
                item.key.value_type,
                item.key.seqno,
                item.key.user_key.len(),
                &item.value,
            ),
            1,
            false,
            relocated,
            &mut Vec::new(),
        )?;
        crate::Result::Ok((blobs, load.rows.value_bytes, load.fresh))
    };

    let key = b"value-key";
    let large = InternalValue::from_components(*key, vec![b'v'; 1_000], 1, crate::ValueType::Value);
    let anew = super::anew_bytes(1_000, key.len(), u32::MAX);
    assert!(anew > 1_000, "its frame takes at least its bytes");
    assert_eq!(
        take(&large, Some(&relocated), false)?,
        (
            (anew, true),
            crate::blob_tree::handle::BlobIndirection::MAX_ENCODED_LEN,
            true
        ),
        "the value goes to a blob file as written raw, its table keeps a \
         pointer, and the slice's first such value opens the file of them",
    );
    assert_eq!(
        take(&large, Some(&relocated), true)?.0,
        (anew, false),
        "past the slice's first, a value written anew opens nothing",
    );

    let small = InternalValue::from_components(*key, vec![b'v'; 999], 1, crate::ValueType::Value);
    assert_eq!(
        take(&small, Some(&relocated), false)?,
        ((0, false), 999, false)
    );
    assert_eq!(take(&large, None, false)?, ((0, false), 1_000, false));
    Ok(())
}

/// What a relocated copy takes is bounded tight: a value with frames of its
/// own is copied as those frames, headers included, so it takes its stored
/// bytes and their index entries; a value sharing a block is re-encoded, and
/// its copy may open a block, so its bound is its entry, one frame header and
/// its index entry. A header too many on every value would refuse a
/// tight-space slice that fits its budget.
#[test]
fn frame_bytes_add_a_header_only_to_a_value_that_shares_a_block() {
    use crate::blob_tree::handle::{BlobIndirection, Stored};
    use crate::vlog::ValueHandle;

    let indirection = |stored| BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 7,
            frame: 0,
            index: 0,
        },
        size: 100,
        stored,
    };
    let header = crate::vlog::blob_file::frame::FRAME_HEADER_LEN as u64;
    let index_entry = 8;
    // A 100-byte value is one frame; the bound allows one chunk more.
    assert_eq!(
        super::frame_bytes(&indirection(Stored::Frames(140)), u32::MAX),
        140 + 2 * index_entry
    );
    assert_eq!(
        super::frame_bytes(&indirection(Stored::InBlock(114)), u32::MAX),
        114 + header + index_entry
    );
}

/// A copied value adds its frames to the output file's frame index too, eight
/// bytes a frame: a value split into many chunks adds hundreds. The bound a
/// tight-space pass charges covers them, or a slice sized to its room fails
/// to land.
#[test]
fn frame_bytes_cover_the_frame_index_a_chunked_copy_adds() {
    use crate::blob_tree::handle::{BlobIndirection, Stored};
    use crate::vlog::ValueHandle;

    let size = 1024 * 1024;
    let chunk_size = crate::vlog::blob_file::writer::MIN_CHUNK_SIZE;
    let Ok(header) = u32::try_from(crate::vlog::blob_file::frame::FRAME_HEADER_LEN) else {
        panic!("a frame header fits u32");
    };
    let frames = 1 + size / chunk_size;
    let stored = size + header * frames;
    let chunked = BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 7,
            frame: 0,
            index: 0,
        },
        size,
        stored: Stored::Frames(stored),
    };
    let index = 8 * u64::from(frames);
    assert!(
        super::frame_bytes(&chunked, chunk_size) >= u64::from(stored) + index,
        "the bound covers the value's frames and their {index} index bytes"
    );
}

/// A value that shares a block was written below the block size of its day;
/// reopened with a smaller chunk size, its relocated copy is split into a head
/// and chunks, a header each. Its bound covers what the writer then writes:
/// one header more alone would let a tight-space slice outgrow its room.
#[test]
fn frame_bytes_cover_a_block_value_the_copy_splits_into_chunks() -> crate::Result<()> {
    use crate::blob_tree::handle::{BlobIndirection, Stored};
    use crate::vlog::{ValueHandle, blob_file::writer::FrameSizes};

    let dir = tempfile::tempdir()?;
    let key = b"key";
    let value = vec![b'v'; 16 * 1_024];
    let chunk_size = 4 * 1_024;
    let mut writer =
        crate::vlog::blob_file::writer::Writer::new(dir.path().join("0"), 0, 0, &crate::fs::StdFs)?
            .use_frame_sizes(FrameSizes {
                block_target: chunk_size,
                chunk_size,
            });
    let written = writer.write(key, 0, &value)?.stored.bytes();
    let (_, _, frames) = writer.finish()?;
    // A head and four chunks, then the data end: the copy is chunked.
    assert_eq!(frames.len(), 6, "the value is written as a head and chunks");

    let Ok(entry) =
        u32::try_from(crate::vlog::blob_file::frame::ENTRY_HEADER_LEN + key.len() + value.len())
    else {
        panic!("a block entry fits u32");
    };
    let Ok(size) = u32::try_from(value.len()) else {
        panic!("the value fits u32");
    };
    let in_block = BlobIndirection {
        vhandle: ValueHandle {
            blob_file_id: 7,
            frame: 0,
            index: 0,
        },
        size,
        stored: Stored::InBlock(entry),
    };
    let bound = super::frame_bytes(&in_block, chunk_size);
    assert!(
        bound >= u64::from(written),
        "bound {bound} under the {written} bytes the copy writes"
    );
    Ok(())
}

/// A relocated value of a file that holds a dictionary of its own is copied
/// into a file that holds that dictionary too. The room a tight-space pass
/// charges for the relocation covers it, or a slice of a few surviving values
/// is admitted to a room the dictionary alone outgrows.
#[test]
#[cfg(feature = "zstd")]
fn tight_slices_cover_the_dictionary_a_relocated_file_holds() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tree, kv) = few_survivors_of_a_dictionary_file(dir.path(), None)?;
    assert_tight_slice_covers_relocation(dir.path(), &tree, &kv, 1)
}

/// A source written before the tree took a key-domain policy holds values of
/// several domains, and a relocation closes its outputs at each boundary: a
/// file per domain, each holding the source's dictionary again. The room a
/// tight-space pass charges covers every one of them.
#[test]
#[cfg(feature = "zstd")]
fn tight_slices_cover_the_files_each_key_domain_opens() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    // Each surviving value its own domain.
    let (tree, kv) = few_survivors_of_a_dictionary_file(dir.path(), Some(Arc::new(PerKeys(500))))?;
    assert_tight_slice_covers_relocation(dir.path(), &tree, &kv, 1)
}

/// The writer settings a merge of `inputs` into the last level of `opts`'s
/// tree at `target_size` writes its outputs with, to bound a slice's tables.
pub(super) fn slice_probe(
    opts: &super::Options,
    inputs: &[Table],
    target_size: u64,
) -> crate::Result<crate::table::multi_writer::BoundProbe> {
    let version = opts.version_history.read().latest_version().version;
    let Some(last) = version.level_count().checked_sub(1) else {
        panic!("a tree has levels");
    };
    let Ok(last) = u8::try_from(last) else {
        panic!("a level index fits u8");
    };
    let payload = crate::compaction::Input {
        table_ids: inputs.iter().map(Table::id).collect(),
        dest_level: last,
        canonical_level: last,
        target_size,
    };
    crate::compaction::flavour::output_bound_probe(
        &version,
        opts,
        &payload,
        &opts.runtime_config.load_full(),
    )
}

/// What a tight-space slice's governor lets a slice over `inputs` write
/// within `caps`, the rows read in key order as the merge keeps them all: its
/// tables and its relocated values as the governor bounds them, and the key
/// it ends before when the caps cut it.
pub(super) struct Governed {
    pub tables: u64,
    pub blobs: u64,
    pub end: Option<crate::UserKey>,
}

/// Runs a slice's governor over every row of `inputs`, a level's tables (see
/// [`Governed`]).
pub(super) fn governed(
    inputs: &[Table],
    caps: super::SliceCaps,
    outputs: &super::SliceOutputs<'_>,
    relocated: Option<&super::Relocated>,
    comparator: &crate::comparator::SharedComparator,
) -> crate::Result<Governed> {
    governed_from(inputs, caps, outputs, relocated, comparator, None)
}

/// [`governed`] over the rows of `inputs` from `from` on: the slice a pass
/// runs after one that ended before `from`.
pub(super) fn governed_from(
    inputs: &[Table],
    caps: super::SliceCaps,
    outputs: &super::SliceOutputs<'_>,
    relocated: Option<&super::Relocated>,
    comparator: &crate::comparator::SharedComparator,
    from: Option<&crate::UserKey>,
) -> crate::Result<Governed> {
    let mut governor =
        super::governor::SliceGovernor::new(caps, outputs, relocated, comparator, from);
    let dropped = core::cell::RefCell::new(Vec::new());
    let mut ordered: Vec<&Table> = inputs.iter().collect();
    ordered
        .sort_by(|a, b| comparator.compare(a.metadata.key_range.min(), b.metadata.key_range.min()));
    'feed: for table in ordered {
        let mut rows = table.scan()?.peekable();
        while let Some(item) = rows.next() {
            let item = item?;
            if from.is_some_and(|from| {
                comparator.compare(&item.key.user_key, from) == core::cmp::Ordering::Less
            }) {
                continue;
            }
            // A row known to end its key, as the merge knows it: the next
            // row of its table carries another. The last row of a table is
            // not known to, and waits for the next.
            let ends_key = matches!(
                rows.peek(),
                Some(Ok(next)) if !crate::comparator::same_user_key(&next.key.user_key, &item.key.user_key)
            );
            match governor.decide(&item, ends_key, &dropped)? {
                super::governor::Decided::Pass => {}
                super::governor::Decided::End => break 'feed,
                super::governor::Decided::Hold => {
                    if !governor.hold(item, false, &dropped)? {
                        break 'feed;
                    }
                }
            }
            while governor.pop_ready().is_some() {}
        }
    }
    governor.finish(&dropped);
    let (tables, blobs) = governor.committed_bound();
    Ok(Governed {
        tables,
        blobs,
        end: governor.end().cloned(),
    })
}

/// A domain per this many keys of the form `k{n:06}`.
struct PerKeys(u32);

impl crate::KeyDomains for PerKeys {
    fn domain_end(&self, key: &[u8]) -> Option<crate::UserKey> {
        let n: u32 = core::str::from_utf8(key.get(1..)?).ok()?.parse().ok()?;
        Some(
            format!("k{:06}", (n / self.0 + 1) * self.0)
                .as_bytes()
                .into(),
        )
    }
}

/// A table written before the tree took a key-domain policy holds keys of
/// several domains, and a merge of it ends a table at each boundary, each
/// with the bytes every table takes. The room a tight-space pass charges for
/// its tables covers every one of them.
#[test]
fn tight_slices_cover_the_tables_each_key_domain_opens() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempfile::tempdir()?;
    {
        let tree = open_standard(dir.path(), |c| c)?;
        for i in 0..2_000u32 {
            tree.insert(format!("k{i:06}"), vec![b'v'; 16], u64::from(i));
        }
        tree.flush_active_memtable(0)?;
    }
    let tree = open_standard(dir.path(), |c| c.key_domains(Arc::new(PerKeys(50))))?;
    let version = tree.current_version();
    let inputs: Vec<Table> = version.iter_tables().cloned().collect();
    let opts = super::Options::from_tree(
        &tree,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    let only = governed(
        &inputs,
        super::SliceCaps {
            tables: u64::MAX,
            blobs: u64::MAX,
            total: u64::MAX,
        },
        &super::SliceOutputs {
            probe: &slice_probe(&opts, &inputs, 64 * 1024 * 1024)?,
            tombstones: crate::table::writer::RewriteLoad::default(),
            cut_keys: &[],
            domains: tree.config.key_domains.as_deref(),
        },
        None,
        &tree.config.comparator,
    )?;
    drop(inputs);
    drop(version);
    assert!(only.end.is_none(), "unbounded caps take every key");

    tree.major_compact(64 * 1024 * 1024, crate::MAX_SEQNO)?;
    let version = tree.current_version();
    assert_eq!(version.table_count(), 40, "a table per domain");
    let written: u64 = version.iter_tables().map(Table::file_len).sum();
    assert!(
        only.tables >= written,
        "the pass charges {} bytes for tables that take {written}",
        only.tables
    );
    Ok(())
}

/// A tree holding one zstd blob file with a dictionary of its own, of which
/// every 500th of 4000 values survives, reopened under `domains` when given.
#[cfg(feature = "zstd")]
fn few_survivors_of_a_dictionary_file(
    dir: &std::path::Path,
    domains: Option<Arc<dyn crate::KeyDomains>>,
) -> crate::Result<(crate::BlobTree, KvSeparationOptions)> {
    let kv = KvSeparationOptions::default()
        .separation_threshold(1)
        .block_size(0)
        .age_cutoff(1.0)
        .staleness_threshold(0.1);
    let config = || {
        Config::new(
            dir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .blob_compression(crate::CompressionType::Zstd(3))
        .with_kv_separation(Some(kv.clone()))
    };
    let open = |config: Config| match config.open() {
        Ok(crate::AnyTree::Blob(t)) => Ok(t),
        Ok(crate::AnyTree::Standard(_)) => panic!("expected Blob tree"),
        Err(e) => Err(e),
    };
    let tree = open(config())?;
    let keys = 4_000u32;
    for i in 0..keys {
        let value = format!(
            "{{\"id\":{i},\"tenant\":\"acme\",\"kind\":\"order\",\"region\":\"eu-west-{}\",\
             \"sku\":\"SKU-{}\",\"note\":\"customer asked for delivery before noon\"}}",
            i % 3,
            i % 97
        );
        tree.insert(format!("k{i:06}"), value, u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    // A few survivors: the stale file is worth relocating, and what it
    // relocates is small beside its dictionary.
    for i in 0..keys {
        if i % 500 != 0 {
            tree.remove(format!("k{i:06}"), u64::from(keys + i));
        }
    }
    tree.flush_active_memtable(0)?;
    // Drops the removed values and records how much of the file they were.
    tree.major_compact(64 * 1024 * 1024, crate::MAX_SEQNO)?;
    let Some(domains) = domains else {
        return Ok((tree, kv));
    };
    drop(tree);
    Ok((open(config().key_domains(domains))?, kv))
}

/// Asserts the room a tight-space pass over all of `tree`'s tables charges
/// for the blob frames it relocates covers what a relocation of them writes;
/// the fixture needs `own` stale files holding a dictionary of their own.
#[cfg(feature = "zstd")]
fn assert_tight_slice_covers_relocation(
    dir: &std::path::Path,
    tree: &crate::BlobTree,
    kv: &KvSeparationOptions,
    own: usize,
) -> crate::Result<()> {
    use std::collections::HashSet;

    let version = tree.index.current_version();
    let inputs: Vec<Table> = version.iter_tables().cloned().collect();
    let ids: crate::HashSet<TableId> = inputs.iter().map(Table::id).collect();
    let stale_files = super::pick_blob_files_to_rewrite(&ids, &version, kv)?;
    let with_own = stale_files
        .iter()
        .filter(|file| file.has_own_dictionary())
        .count();
    assert!(
        with_own >= own,
        "the fixture needs {own} stale files holding a dictionary of their own, has {with_own}"
    );
    let relocated = super::Relocated::new(&stale_files, kv);
    drop(stale_files);
    let opts = super::Options::from_tree(
        &tree.index,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    let only = governed(
        &inputs,
        super::SliceCaps {
            tables: u64::MAX,
            blobs: u64::MAX,
            total: u64::MAX,
        },
        &super::SliceOutputs {
            probe: &slice_probe(&opts, &inputs, 64 * 1024 * 1024)?,
            tombstones: crate::table::writer::RewriteLoad::default(),
            cut_keys: &[],
            domains: tree.index.config.key_domains.as_deref(),
        },
        Some(&relocated),
        &tree.index.config.comparator,
    )?;
    drop(inputs);
    drop(version);
    assert!(only.end.is_none(), "unbounded caps take every key");

    let blobs = dir.join(crate::file::BLOBS_FOLDER);
    let names = |dir: &std::path::Path| -> crate::Result<HashSet<std::ffi::OsString>> {
        Ok(std::fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<_>>()?)
    };
    let before = names(&blobs)?;
    tree.major_compact(64 * 1024 * 1024, crate::MAX_SEQNO)?;
    let mut written = 0;
    for name in names(&blobs)?.difference(&before) {
        written += std::fs::metadata(blobs.join(name))?.len();
    }
    assert!(written > 0, "the relocation writes a blob file");
    assert!(
        only.blobs >= written,
        "the pass charges {} bytes for a relocation that writes {written}",
        only.blobs
    );
    Ok(())
}

/// Past the sources whose dictionaries a relocation keeps with their frames,
/// a source's values are written anew without its dictionary. A value that
/// compresses only against that dictionary then takes what it is raw, and
/// the room a tight-space pass charges covers that, not the frames it had.
#[test]
#[cfg(feature = "zstd")]
fn tight_slices_cover_values_written_anew_without_their_dictionary() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tree, kv) = many_dictionary_files(dir.path())?;
    assert_tight_slice_covers_relocation(
        dir.path(),
        &tree,
        &kv,
        crate::vlog::blob_file::multi_writer::MAX_COPY_SOURCES + 1,
    )
}

/// A slice charges the dictionaries of the sources it relocates values out
/// of, not those of every stale file: in a room that holds a few of them, a
/// pass over many files with dictionaries of their own, each slice reaching
/// a few, cuts slices that fit rather than none.
#[test]
#[cfg(feature = "zstd")]
fn a_tight_slice_charges_only_the_dictionaries_it_relocates_out_of() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let (tree, kv) = many_dictionary_files(dir.path())?;
    let version = tree.index.current_version();
    let inputs: Vec<Table> = version.iter_tables().cloned().collect();
    let ids: crate::HashSet<TableId> = inputs.iter().map(Table::id).collect();
    let stale_files = super::pick_blob_files_to_rewrite(&ids, &version, &kv)?;
    let relocated = super::Relocated::new(&stale_files, &kv);
    drop(stale_files);
    // The room for every stale file's dictionary at once, and no more.
    let every_dictionary: u64 = relocated.own_dictionaries.values().sum::<u64>() * relocated.groups;
    assert!(
        relocated.own_dictionaries.len() > 4,
        "the fixture needs many files with dictionaries of their own"
    );
    let caps = super::SliceCaps {
        tables: u64::MAX,
        blobs: every_dictionary,
        total: u64::MAX,
    };
    let opts = super::Options::from_tree(
        &tree.index,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    let first = governed(
        &inputs,
        caps,
        &super::SliceOutputs {
            probe: &slice_probe(&opts, &inputs, 64 * 1024 * 1024)?,
            tombstones: crate::table::writer::RewriteLoad::default(),
            cut_keys: &[],
            domains: None,
        },
        Some(&relocated),
        &tree.index.config.comparator,
    )?;
    assert!(
        first.blobs > 0 && first.blobs <= caps.blobs,
        "the first slice fits its room: {} against {}",
        first.blobs,
        caps.blobs
    );
    assert!(first.end.is_some(), "the pass runs in slices");
    Ok(())
}

/// A tree holding more zstd blob files with a dictionary of their own than a
/// relocation keeps the dictionaries of, half of every file's values
/// surviving.
#[cfg(feature = "zstd")]
fn many_dictionary_files(
    dir: &std::path::Path,
) -> crate::Result<(crate::BlobTree, KvSeparationOptions)> {
    let kv = KvSeparationOptions::default()
        .separation_threshold(1)
        // Every value a frame of its own.
        .block_size(64)
        .file_dictionary(4 * 1024)
        // Room for each file to build its dictionary and hold many values
        // under it: more such files than a pass keeps the dictionaries of.
        .file_target_size(16 * 1024)
        .age_cutoff(1.0)
        .staleness_threshold(0.1);
    let tree = match Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .blob_compression(crate::CompressionType::Zstd(3))
    .with_kv_separation(Some(kv.clone()))
    // Table blocks of a few keys: a slice can end every few values.
    .data_block_size_policy(BlockSizePolicy::all(512))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected Blob tree"),
    };
    // One run of bytes with nothing to repeat within it, which every value
    // carries: a file's dictionary, its first values, matches all of a value,
    // and without it a value does not compress.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let common: Vec<u8> = (0..3_000)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state.to_le_bytes()[7]
        })
        .collect();
    let keys = 3_000u32;
    for i in 0..keys {
        let mut value = common.clone();
        if let Some(head) = value.get_mut(..4) {
            head.copy_from_slice(&i.to_le_bytes());
        }
        tree.insert(format!("k{i:06}"), value, u64::from(i));
    }
    tree.flush_active_memtable(0)?;
    // Half survives in every file: what the values written anew grow by
    // outweighs the dictionaries the pass leaves behind.
    for i in 0..keys {
        if i % 2 != 0 {
            tree.remove(format!("k{i:06}"), u64::from(keys + i));
        }
    }
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, crate::MAX_SEQNO)?;
    Ok((tree, kv))
}

/// Two trees handed one limiter are bounded by it together: what one tree's
/// compaction spent is still spent when the other's asks, whatever rate each
/// tree was configured with.
#[test]
fn trees_sharing_a_limiter_draw_on_one_budget() -> crate::Result<()> {
    use core::time::Duration;

    let rate = 1_024 * 1_024;
    let shared = Arc::new(crate::rate_limiter::RateLimiter::new(rate));
    let (a_dir, b_dir) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let a = open_standard(a_dir.path(), |c| {
        c.compaction_rate_limit(rate * 100)
            .compaction_rate_limiter(Arc::clone(&shared))
    })?;
    let b = open_standard(b_dir.path(), |c| {
        c.compaction_rate_limiter(Arc::clone(&shared))
    })?;
    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));

    let on_a = super::Options::from_tree(&a, Arc::clone(&strategy));
    assert!(
        !on_a
            .rate_limiter
            .acquire_wait(rate * 4, Duration::ZERO)
            .is_zero(),
        "four seconds of budget at once leaves a debt",
    );
    let on_b = super::Options::from_tree(&b, strategy);
    assert!(
        !on_b.rate_limiter.acquire_wait(1, Duration::ZERO).is_zero(),
        "the other tree's compaction meets the debt the first one left",
    );

    Ok(())
}

/// Without a shared limiter each tree keeps its own budget: one tree's debt
/// does not hold back another's compaction.
#[test]
fn trees_without_a_shared_limiter_keep_their_own_budget() -> crate::Result<()> {
    use core::time::Duration;

    let rate = 1_024 * 1_024;
    let (a_dir, b_dir) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let a = open_standard(a_dir.path(), |c| c.compaction_rate_limit(rate))?;
    let b = open_standard(b_dir.path(), |c| c.compaction_rate_limit(rate))?;
    let strategy: Arc<dyn CompactionStrategy> =
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));

    let on_a = super::Options::from_tree(&a, Arc::clone(&strategy));
    assert!(
        !on_a
            .rate_limiter
            .acquire_wait(rate * 4, Duration::ZERO)
            .is_zero()
    );
    let on_b = super::Options::from_tree(&b, strategy);
    assert!(
        on_b.rate_limiter.acquire_wait(1, Duration::ZERO).is_zero(),
        "a tree's own budget is untouched by another tree's debt",
    );

    Ok(())
}

/// A retune through one holder is seen by every tree sharing the limiter,
/// with one value, and the compactions each runs are measured against it,
/// not against either tree's configured rate.
#[test]
fn a_retune_through_one_tree_binds_every_tree_sharing_the_limiter() -> crate::Result<()> {
    use crate::AbstractTree;
    use core::time::Duration;

    let shared = Arc::new(crate::rate_limiter::RateLimiter::new(1_000));
    let (a_dir, b_dir) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let a = open_standard(a_dir.path(), |c| {
        c.compaction_rate_limit(1_000_000)
            .compaction_rate_limiter(Arc::clone(&shared))
    })?;
    let b = open_standard(b_dir.path(), |c| {
        c.compaction_rate_limit(5)
            .compaction_rate_limiter(Arc::clone(&shared))
    })?;
    assert_eq!(a.compaction_rate_limiter().rate(), 1_000);
    assert_eq!(b.compaction_rate_limiter().rate(), 1_000);

    a.compaction_rate_limiter()
        .set_rate_at(2_000, Duration::ZERO);
    assert_eq!(b.compaction_rate_limiter().rate(), 2_000);

    // b's compaction owes at the retuned rate: the bucket still holds the
    // 1000 B it had (a raise grants no new credit), so 2000 B of debt is one
    // second at 2000 B/s, where the old rate would have made it two.
    let on_b = super::Options::from_tree(
        &b,
        Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024)),
    );
    assert_eq!(
        on_b.rate_limiter.acquire_wait(3_000, Duration::ZERO),
        Duration::from_secs(1),
    );

    Ok(())
}

/// A tree opened without a shared limiter keeps its own: a copy of its
/// configuration opened as another tree, at a rate of its own, gets a limiter
/// of its own at that rate, and a retune of either leaves the other as it was.
#[test]
fn a_copy_of_a_trees_config_opens_with_a_limiter_of_its_own() -> crate::Result<()> {
    use crate::AbstractTree;
    use core::time::Duration;

    let (a_dir, b_dir) = (tempfile::tempdir()?, tempfile::tempdir()?);
    let a = open_standard(a_dir.path(), |c| c.compaction_rate_limit(1_000))?;
    let mut copy = a.tree_config().clone();
    copy.path = b_dir.path().to_path_buf();
    let b = copy.compaction_rate_limit(5_000).open()?;

    assert_eq!(a.compaction_rate_limiter().rate(), 1_000);
    assert_eq!(
        b.compaction_rate_limiter().rate(),
        5_000,
        "the copy's own rate"
    );
    a.compaction_rate_limiter()
        .set_rate_at(2_000, Duration::ZERO);
    assert_eq!(b.compaction_rate_limiter().rate(), 5_000, "not shared");
    Ok(())
}

/// A verification scan that ends while the device is congested leaves the
/// latency backoff of a limiter it shares with compaction low, and only read
/// latencies move it: a compaction on that limiter reports how long its input
/// reads take, so the rate climbs back once the device answers fast again
/// instead of staying at the floor until another scan comes.
#[test]
fn compaction_reads_lift_a_backoff_a_scan_left_low() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::rate_limiter::{LatencyBackoff, RateLimiter};
    use core::time::Duration;

    let shared = Arc::new(RateLimiter::new(1 << 30));
    shared.set_latency_backoff(Some(
        LatencyBackoff::new(Duration::from_secs(10)).with_period(Duration::ZERO),
    ));
    // The scan's last reads, far over the ceiling.
    for _ in 0..20 {
        shared.record_read_latency(Duration::from_secs(100));
    }
    let left_by_the_scan = shared.effective_rate();
    assert!(
        left_by_the_scan < shared.rate(),
        "the scan lowered the rate"
    );

    let dir = tempfile::tempdir()?;
    let tree = open_standard(dir.path(), |c| {
        c.compaction_rate_limiter(Arc::clone(&shared))
    })?;
    // Values the block codec cannot shrink, so the inputs span many reads.
    let mut noise = 0x9E37_79B9_7F4A_7C15u64;
    for flush in 0..4u64 {
        for key in 0..256u64 {
            let value: Vec<u8> = (0..512)
                .flat_map(|_| {
                    noise = noise
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    noise.to_le_bytes()
                })
                .collect();
            tree.insert(key.to_be_bytes(), value, flush * 256 + key);
        }
        tree.flush_active_memtable(0)?;
    }
    tree.major_compact(u64::MAX, 0)?;

    assert!(
        shared.effective_rate() > left_by_the_scan,
        "the compaction's fast reads did not lift the backoff: {} of {}",
        shared.effective_rate(),
        shared.rate(),
    );
    Ok(())
}

/// A merge split into ranges reads the row groups of columnar inputs it
/// rewrites a region at a time while the limiter steers by latency, each
/// region timed on its own, and those reads lift a backoff a verification
/// scan left low.
#[cfg(all(feature = "columnar", feature = "lz4"))]
#[test]
fn columnar_range_merge_reads_lift_a_backoff_a_scan_left_low() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::rate_limiter::{LatencyBackoff, RateLimiter};
    use core::time::Duration;

    let shared = Arc::new(RateLimiter::new(1 << 30));
    shared.set_latency_backoff(Some(
        LatencyBackoff::new(Duration::from_secs(10)).with_period(Duration::ZERO),
    ));
    let dir = tempfile::tempdir()?;
    let tree = open_standard(dir.path(), |c| {
        c.compaction_rate_limiter(Arc::clone(&shared))
            .compaction_threads(4)
            .subcompaction_min_bytes(0)
    })?;
    tree.update_runtime_config(|c| {
        c.columnar = true;
        c.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::None);
    })?;
    let write = |flush: u64| -> crate::Result<()> {
        for i in 0..4_000u64 {
            tree.insert(
                format!("k{i:06}"),
                format!("value-{flush}-{i}-payload"),
                flush * 10_000 + i,
            );
        }
        tree.flush_active_memtable(0)
    };
    // Small outputs, so the bottom level holds several tables to cut the
    // next merge's ranges at.
    write(0)?;
    tree.major_compact(32 * 1024, 0)?;
    assert!(tree.current_version().iter_tables().count() > 2);

    // Every row changes and the output codec differs, so no group is copied:
    // each range reads them, page by page.
    write(1)?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy =
            crate::config::CompressionPolicy::all(crate::CompressionType::Lz4);
    })?;
    for _ in 0..20 {
        shared.record_read_latency(Duration::from_secs(100));
    }
    let left_by_the_scan = shared.effective_rate();
    tree.major_compact(32 * 1024, 0)?;

    assert!(
        shared.effective_rate() > left_by_the_scan,
        "the merge's reads did not lift the backoff: {} of {}",
        shared.effective_rate(),
        shared.rate(),
    );
    for i in (0..2_000u64).step_by(101) {
        assert_eq!(
            tree.get(format!("k{i:06}"), crate::SeqNo::MAX)?.as_deref(),
            Some(format!("value-1-{i}-payload").as_bytes()),
        );
    }
    Ok(())
}

/// A lone columnar segment whose own range tombstone deletes rows takes the
/// merge-on-read fast path: its planning scan and its relocation each read
/// the whole segment, and those reads report their latency as a merge's do,
/// so they lift a backoff a verification scan left low.
#[cfg(feature = "columnar")]
#[test]
fn merge_on_read_reads_lift_a_backoff_a_scan_left_low() -> crate::Result<()> {
    use crate::AbstractTree;
    use crate::config::{DeleteStrategy, DeleteStrategyPolicy};
    use crate::rate_limiter::{LatencyBackoff, RateLimiter};
    use core::time::Duration;

    let shared = Arc::new(RateLimiter::new(1 << 30));
    shared.set_latency_backoff(Some(
        LatencyBackoff::new(Duration::from_secs(10)).with_period(Duration::ZERO),
    ));
    for _ in 0..20 {
        shared.record_read_latency(Duration::from_secs(100));
    }
    let left_by_the_scan = shared.effective_rate();
    assert!(
        left_by_the_scan < shared.rate(),
        "the scan lowered the rate"
    );

    let dir = tempfile::tempdir()?;
    let tree = open_standard(dir.path(), |c| {
        c.compaction_rate_limiter(Arc::clone(&shared))
    })?;
    tree.update_runtime_config(|c| {
        c.columnar = true;
        c.zone_map = true;
        c.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
    })?;
    for i in 0..2_000u64 {
        tree.insert(format!("k{i:06}"), format!("value-{i}-payload"), i);
    }
    tree.remove_range("k000000", "k000050", 10_000);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64 * 1024 * 1024, 20_000)?;

    assert!(
        tree.current_version()
            .iter_tables()
            .any(|table| !table.delete_bitmap().is_empty()),
        "the segment was relocated with a delete bitmap",
    );
    assert!(
        shared.effective_rate() > left_by_the_scan,
        "the fast path's reads did not lift the backoff: {} of {}",
        shared.effective_rate(),
        shared.rate(),
    );
    Ok(())
}

mod budget_gate {
    use super::open_standard;
    use crate::fs::{CountingFs, MemFs, WriteIoStats};
    use crate::write_budget::{Allowance, BudgetCheckpoint, WriteBudget};
    use crate::{AbstractTree, compaction::CompactionStrategy};
    use core::time::Duration;
    use std::sync::Arc;
    use test_log::test;

    const DAY: Duration = Duration::from_hours(24);

    /// A clock that stays at the epoch, so the budget refills nothing.
    struct Epoch;

    impl crate::Clock for Epoch {
        fn unix_time(&self) -> Duration {
            Duration::ZERO
        }
    }

    /// A tree on a medium whose budget holds `credit`, and its options.
    fn on_budget(
        dir: &std::path::Path,
        credit: i64,
    ) -> crate::Result<(Arc<WriteBudget>, crate::Tree, super::super::Options)> {
        let budget = Arc::new(
            WriteBudget::resume_at(
                Arc::new(WriteIoStats::default()),
                Allowance::new(1 << 30, DAY),
                BudgetCheckpoint {
                    credit,
                    as_of: Duration::ZERO,
                },
                0,
                Duration::ZERO,
            )
            .with_clock(Arc::new(Epoch)),
        );
        let fs = Arc::new(CountingFs::with_budget(
            Arc::new(MemFs::new()),
            Arc::clone(&budget),
        ));
        let tree = open_standard(dir, |c| c.with_shared_fs(fs))?;
        let strategy: Arc<dyn CompactionStrategy> =
            Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));
        let opts = super::super::Options::from_tree(&tree, strategy);
        Ok((budget, tree, opts))
    }

    /// Without credit, a merge the space gate narrowed to reclaim room runs as
    /// a space override; the merge as chosen waits. A forced budget names the
    /// caller's reason first.
    #[test]
    fn a_space_narrowed_merge_runs_past_an_empty_budget() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (budget, tree, opts) = on_budget(dir.path(), 0)?;
        let version = tree.current_version();

        let gate = super::super::budget_gate(
            &version,
            &opts,
            6,
            (1_000, 0),
            super::super::Narrowing::ForSpace,
        );
        let super::super::BudgetGate::Run(holds) = gate else {
            panic!("a reclaiming merge must run");
        };
        assert_eq!(holds.len(), 1);
        drop(holds);
        let report = budget.report();
        assert_eq!(report.space_overrides.reservations, 1);
        assert_eq!(report.refused, 0);

        let gate = super::super::budget_gate(
            &version,
            &opts,
            6,
            (1_000, 0),
            super::super::Narrowing::None,
        );
        assert!(matches!(gate, super::super::BudgetGate::Wait));
        assert_eq!(budget.report().refused, 1);

        budget.set_forced(true);
        let gate = super::super::budget_gate(
            &version,
            &opts,
            6,
            (1_000, 0),
            super::super::Narrowing::ForSpace,
        );
        assert!(matches!(gate, super::super::BudgetGate::Run(_)));
        let report = budget.report();
        assert_eq!(report.forced_overrides.reservations, 1);
        assert_eq!(report.space_overrides.reservations, 1, "not counted twice");
        Ok(())
    }

    /// Merges exactly the named tables into `dest`.
    struct MergeRun(Vec<crate::TableId>, u8);

    impl CompactionStrategy for MergeRun {
        fn get_name(&self) -> &'static str {
            "MergeRunTest"
        }

        fn choose(
            &self,
            _: &crate::version::Version,
            _: &crate::Config,
            _: &crate::compaction::state::CompactionState,
        ) -> crate::compaction::Choice {
            crate::compaction::Choice::Merge(crate::compaction::Input {
                table_ids: self.0.iter().copied().collect(),
                dest_level: self.1,
                canonical_level: self.1,
                target_size: 64 * 1024 * 1024,
            })
        }
    }

    /// On a volume with no room for the merge chosen, the space gate runs the
    /// smaller merge that reclaims room, and an empty budget lets it through
    /// as a space override, end to end through a compaction. With room for
    /// the merge as chosen, the same empty budget makes it wait.
    #[test]
    fn a_full_volume_runs_the_reclaiming_merge_past_an_empty_budget() -> crate::Result<()> {
        use crate::Table;
        use crate::config::BlockSizePolicy;

        let dir = tempfile::tempdir()?;
        let mem = MemFs::with_capacity(u64::MAX);
        let budget = Arc::new(
            WriteBudget::resume_at(
                Arc::new(WriteIoStats::default()),
                Allowance::new(1 << 30, DAY),
                BudgetCheckpoint {
                    credit: 0,
                    as_of: Duration::ZERO,
                },
                0,
                Duration::ZERO,
            )
            .with_clock(Arc::new(Epoch)),
        );
        let fs = Arc::new(CountingFs::with_budget(
            Arc::new(mem.clone()),
            Arc::clone(&budget),
        ));
        let tree = open_standard(dir.path(), |c| {
            c.with_shared_fs(fs)
                .data_block_size_policy(BlockSizePolicy::all(512))
        })?;
        for i in 0..3_000u64 {
            tree.insert(format!("k{i:08}"), "v".repeat(40), i);
        }
        tree.flush_active_memtable(0)?;
        // The setup's own merge runs on the caller's word.
        budget.set_forced(true);
        tree.major_compact(16 * 1024, 0)?;
        budget.set_forced(false);

        let version = tree.current_version();
        let Some(run) = version
            .iter_levels()
            .flat_map(|level| level.iter())
            .find(|run| run.len() >= 3)
        else {
            panic!("a bottom-level run with >= 3 tables");
        };
        let ids: Vec<_> = run.iter().map(Table::id).collect();
        // What the gate charges the whole run.
        let payload = crate::compaction::Input {
            table_ids: ids.iter().copied().collect(),
            dest_level: 6,
            canonical_level: 6,
            target_size: 64 * 1024 * 1024,
        };
        let opts = crate::compaction::worker::Options::from_tree(
            &tree,
            Arc::new(MergeRun(ids.clone(), 6)),
        );
        let probe = crate::compaction::flavour::output_bound_probe(
            &version,
            &opts,
            &payload,
            &opts.runtime_config.load(),
        )?;
        let run_bound = crate::compaction::worker::recorded_table_demand(
            &version,
            &opts,
            &payload,
            &probe,
            &[],
        )?;
        drop(opts);
        drop(version);

        // Room enough for the merge as chosen: the empty budget holds it.
        tree.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.storage_limit_bytes = None;
        })?;
        let result = tree.compact(Arc::new(MergeRun(ids.clone(), 6)), 0)?;
        assert_eq!(result.action, crate::compaction::CompactionAction::Nothing);
        assert_eq!(budget.report().refused, 1);

        // Room below the run but above a pair, as the space gate's own test
        // sizes it: the gate narrows, and the budget lets the reclaiming
        // merge through.
        let probe_capacity = 1u64 << 40;
        mem.set_capacity(probe_capacity);
        let stored = probe_capacity
            - crate::fs::Fs::available_space(&mem, dir.path()).unwrap_or(probe_capacity);
        mem.set_capacity(stored + run_bound - 1);
        tree.update_runtime_config(|_| {})?;
        let tables = tree.table_count();

        let result = tree.compact(Arc::new(MergeRun(ids, 6)), 0)?;

        assert_ne!(result.action, crate::compaction::CompactionAction::Nothing);
        assert_eq!(result.tables_in, 2, "the narrowed pair");
        assert!(tree.table_count() < tables);
        let report = budget.report();
        assert_eq!(report.space_overrides.reservations, 1);
        assert_eq!(report.refused, 1, "only the earlier wait");
        assert_eq!(report.reserved, 0);
        for i in (0..3_000u64).step_by(97) {
            assert_eq!(
                tree.get(format!("k{i:08}"), crate::SeqNo::MAX)?.as_deref(),
                Some("v".repeat(40).as_bytes()),
            );
        }
        Ok(())
    }

    /// On a volume with room for no merge at all, the tight-space pass
    /// reclaims it slice by slice. It asks the budget as the narrowed merge
    /// does: an empty one lets it through as a space override, counted in the
    /// report like any other byte past the budget.
    #[test]
    fn the_tight_space_pass_runs_past_an_empty_budget_as_a_space_override() -> crate::Result<()> {
        use crate::config::BlockSizePolicy;

        let dir = tempfile::tempdir()?;
        let mem = MemFs::with_capacity(u64::MAX);
        let budget = Arc::new(
            WriteBudget::resume_at(
                Arc::new(WriteIoStats::default()),
                Allowance::new(1 << 30, DAY),
                BudgetCheckpoint {
                    credit: 0,
                    as_of: Duration::ZERO,
                },
                0,
                Duration::ZERO,
            )
            .with_clock(Arc::new(Epoch)),
        );
        let fs = Arc::new(CountingFs::with_budget(
            Arc::new(mem.clone()),
            Arc::clone(&budget),
        ));
        let tree = open_standard(dir.path(), |c| {
            c.with_shared_fs(fs)
                .data_block_size_policy(BlockSizePolicy::all(512))
        })?;
        for i in 0..2_000u64 {
            tree.insert(format!("k{i:08}"), vec![0xCDu8; 64], i);
        }
        tree.flush_active_memtable(0)?;
        // Room for a slice, not for the whole merge, and a lone table leaves
        // no smaller merge to narrow to.
        let used = tree.storage_stats()?.used_bytes;
        mem.set_capacity(used + used / 8);
        tree.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.tight_space_compaction = true;
        })?;

        tree.major_compact(64 * 1024 * 1024, 0)?;

        assert!(mem.punched_bytes() > 0, "the tight-space pass ran");
        let report = budget.report();
        assert_eq!(report.space_overrides.reservations, 1);
        assert!(report.space_overrides.bytes > 0);
        assert_eq!(report.refused, 0);
        assert_eq!(report.reserved, 0, "given back once the pass returns");
        for i in (0..2_000u64).step_by(97) {
            assert_eq!(
                tree.get(format!("k{i:08}"), crate::SeqNo::MAX)?.as_deref(),
                Some(vec![0xCDu8; 64].as_slice()),
            );
        }
        Ok(())
    }

    /// A merge of a tree that separates values may write new blob files
    /// though it relocates none: a merge operator's result, a compaction
    /// filter's replacement or a locality relocation past the separation
    /// threshold all land in the blob folder. Its medium is asked for every
    /// such merge, for nothing reserved, and an overrun budget there makes the
    /// merge wait. A tree that separates nothing writes no blob file, and that
    /// budget is not asked.
    #[test]
    fn a_merge_of_a_separating_tree_asks_the_blob_medium() -> crate::Result<()> {
        let gate =
            |separates: bool| -> crate::Result<(super::super::BudgetGate, Arc<WriteBudget>)> {
                let dir = tempfile::tempdir()?;
                let blobs = Arc::new(
                    WriteBudget::resume_at(
                        Arc::new(WriteIoStats::default()),
                        Allowance::new(1 << 30, DAY),
                        BudgetCheckpoint {
                            credit: -1,
                            as_of: Duration::ZERO,
                        },
                        0,
                        Duration::ZERO,
                    )
                    .with_clock(Arc::new(Epoch)),
                );
                let primary = Arc::new(CountingFs::with_budget(
                    Arc::new(MemFs::new()),
                    Arc::clone(&blobs),
                ));
                let mut config = crate::Config::new(
                    dir.path(),
                    crate::SequenceNumberCounter::default(),
                    crate::SequenceNumberCounter::default(),
                )
                .with_shared_fs(primary)
                .level_routes(vec![crate::config::LevelRoute {
                    levels: 1..7,
                    path: dir.path().join("cold"),
                    fs: Arc::new(MemFs::new()),
                }]);
                if separates {
                    config = config.with_kv_separation(Some(crate::KvSeparationOptions::default()));
                }
                let tree = match config.open()? {
                    crate::AnyTree::Blob(tree) => tree.index,
                    crate::AnyTree::Standard(tree) => tree,
                };
                let strategy: Arc<dyn CompactionStrategy> =
                    Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));
                let opts = super::super::Options::from_tree(&tree, strategy);
                let gate = super::super::budget_gate(
                    &tree.current_version(),
                    &opts,
                    6,
                    (1_000, 0),
                    super::super::Narrowing::None,
                );
                Ok((gate, blobs))
            };

        let (separating, blobs) = gate(true)?;
        assert!(
            matches!(separating, super::super::BudgetGate::Wait),
            "the overrun blob medium holds back a merge that may write there"
        );
        assert_eq!(blobs.report().refused, 1);

        let (plain, blobs) = gate(false)?;
        assert!(matches!(plain, super::super::BudgetGate::Run(_)));
        assert_eq!(blobs.report().refused, 0, "nothing can land there");
        Ok(())
    }

    /// The tables and the blob files of a merge whose folders are on one
    /// medium are reserved together, as one hold of their sum.
    #[test]
    fn two_folders_on_one_medium_reserve_one_hold() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (budget, tree, opts) = on_budget(dir.path(), 1 << 20)?;
        let version = tree.current_version();

        let gate = super::super::budget_gate(
            &version,
            &opts,
            6,
            (300, 200),
            super::super::Narrowing::None,
        );
        let super::super::BudgetGate::Run(holds) = gate else {
            panic!("covered by the credit");
        };
        assert_eq!(
            holds
                .iter()
                .map(crate::write_budget::BudgetHold::held)
                .collect::<Vec<_>>(),
            [500]
        );
        assert_eq!(budget.report().reserved, 500);
        drop(holds);
        assert_eq!(budget.report().reserved, 0);
        Ok(())
    }

    /// A merge writing nothing, or onto a medium with no budget, reserves
    /// nothing and is never held back.
    #[test]
    fn nothing_to_reserve_holds_nothing() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let (budget, tree, opts) = on_budget(dir.path(), 0)?;
        let gate = super::super::budget_gate(
            &tree.current_version(),
            &opts,
            6,
            (0, 0),
            super::super::Narrowing::None,
        );
        let super::super::BudgetGate::Run(holds) = gate else {
            panic!("nothing written, nothing to wait for");
        };
        assert!(holds.is_empty());
        assert_eq!(budget.report().refused, 0);

        let plain_dir = tempfile::tempdir()?;
        let plain = open_standard(plain_dir.path(), |c| c)?;
        let strategy: Arc<dyn CompactionStrategy> =
            Arc::new(crate::compaction::major::Strategy::new(64 * 1024 * 1024));
        let plain_opts = super::super::Options::from_tree(&plain, strategy);
        let gate = super::super::budget_gate(
            &plain.current_version(),
            &plain_opts,
            6,
            (1 << 30, 1 << 30),
            super::super::Narrowing::None,
        );
        let super::super::BudgetGate::Run(holds) = gate else {
            panic!("no budget, nothing to wait for");
        };
        assert!(holds.is_empty());
        Ok(())
    }
}

mod locality_relocation {
    use super::RewriteOneTable;
    use crate::{AbstractTree, AnyTree, Config, KvSeparationOptions, SequenceNumberCounter};
    use core::num::NonZeroU64;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use test_log::test;

    const FILES: u64 = 16;
    const KEYS_PER_FILE: u64 = 16;

    fn open(path: &std::path::Path, blob_opts: KvSeparationOptions) -> crate::Result<AnyTree> {
        Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        // Tables close at a block, so small blocks let a small target split
        // the keys into several tables whatever a reference's encoded width.
        .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
        .blob_compression(crate::CompressionType::None)
        .with_kv_separation(Some(blob_opts))
        .open()
    }

    fn relocating() -> KvSeparationOptions {
        let Some(max_depth) = NonZeroU64::new(2) else {
            panic!("two is not zero");
        };
        KvSeparationOptions::default().relocate_for_locality(max_depth, f32::MAX)
    }

    /// Flushes that each write every `FILES`-th key, one blob file per flush.
    fn fill_interleaved(tree: &AnyTree) -> crate::Result<()> {
        let mut seqno = 0;
        for file in 0..FILES {
            for i in 0..KEYS_PER_FILE {
                let k = i * FILES + file;
                tree.insert(format!("key{k:06}"), vec![b'v'; 2_048], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }
        Ok(())
    }

    fn blob_ids(tree: &AnyTree) -> BTreeSet<u64> {
        tree.current_version()
            .blob_files
            .iter()
            .map(crate::vlog::BlobFile::id)
            .collect()
    }

    /// A blob file another table still points into cannot move, however deep
    /// the interleaving: rewriting it would leave that table's references
    /// dangling. The merged table's files that only it references may move;
    /// the ones it shares with its neighbours stay.
    #[test]
    fn a_blob_file_a_table_outside_the_merge_references_stays() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            // Split into several tables, each covering a run of keys that
            // reaches into many of the files. Relocation is off here.
            let tree = open(folder.path(), KvSeparationOptions::default())?;
            fill_interleaved(&tree)?;
            tree.major_compact(1_024, u64::MAX)?;
            assert!(tree.table_count() > 1, "the layout needs several tables");
        }

        let tree = open(folder.path(), relocating())?;
        let version = tree.current_version();
        let mut tables = version.iter_tables();
        let Some(table) = tables.next().cloned() else {
            panic!("the split left no table");
        };
        let mut shared = BTreeSet::new();
        for other in tables {
            shared.extend(other.blob_links()?.iter().map(|link| link.blob_file_id));
        }
        let own: BTreeSet<u64> = table
            .blob_links()?
            .iter()
            .map(|link| link.blob_file_id)
            .collect();
        let shared: BTreeSet<u64> = own.intersection(&shared).copied().collect();
        assert!(!shared.is_empty(), "the merged table must share some files");
        let table_count = tree.table_count();
        drop(version);

        tree.compact(Arc::new(RewriteOneTable(table.id(), 6)), u64::MAX)?;
        assert_eq!(
            tree.table_count(),
            table_count,
            "the one table was rewritten"
        );
        let after = blob_ids(&tree);
        assert!(
            shared.is_subset(&after),
            "shared files {shared:?} must all survive, left {after:?}",
        );
        Ok(())
    }

    /// Merges exactly the named tables into `dest`.
    struct MergeInto(Vec<crate::TableId>, u8);

    impl crate::compaction::CompactionStrategy for MergeInto {
        fn get_name(&self) -> &'static str {
            "MergeIntoTest"
        }

        fn choose(
            &self,
            _: &crate::version::Version,
            _: &Config,
            _: &crate::compaction::state::CompactionState,
        ) -> crate::compaction::Choice {
            crate::compaction::Choice::Merge(crate::compaction::Input {
                table_ids: self.0.iter().copied().collect(),
                dest_level: self.1,
                canonical_level: self.1,
                target_size: u64::MAX,
            })
        }
    }

    /// A deeply interleaved table that a strategy only moves down is rewritten
    /// on the way instead: a move would carry the interleaving to the bottom
    /// level, where nothing may ever merge it again.
    #[test]
    fn a_move_of_a_deeply_interleaved_table_relocates_it() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        {
            // One interleaved table on level 1, merged with relocation off.
            let tree = open(folder.path(), KvSeparationOptions::default())?;
            fill_interleaved(&tree)?;
            let ids: Vec<_> = tree
                .current_version()
                .iter_tables()
                .map(crate::Table::id)
                .collect();
            tree.compact(Arc::new(MergeInto(ids, 1)), u64::MAX)?;
            assert_eq!(tree.table_count(), 1);
            assert_eq!(
                tree.storage_stats()?.blob_references.depth,
                FILES,
                "the merge left the files interleaved",
            );
        }

        let tree = open(folder.path(), relocating())?;
        let Some(table) = tree
            .current_version()
            .iter_tables()
            .next()
            .map(crate::Table::id)
        else {
            panic!("the merge left one table");
        };
        let before = blob_ids(&tree);
        tree.compact(Arc::new(super::MoveTables(vec![table], 6)), u64::MAX)?;
        assert!(
            before.is_disjoint(&blob_ids(&tree)),
            "the interleaved files were rewritten on the way down",
        );
        assert_eq!(tree.storage_stats()?.blob_references.depth, 1);
        Ok(())
    }

    /// A lone columnar segment whose own range tombstone deletes rows would be
    /// relocated verbatim with a delete bitmap, keeping its blob links as they
    /// are. When locality relocation applies to it, the segment is rewritten
    /// instead, so its interleaved files are relocated too.
    #[cfg(feature = "columnar")]
    #[test]
    fn locality_relocation_takes_precedence_over_merge_on_read() -> crate::Result<()> {
        use crate::config::{DeleteStrategy, DeleteStrategyPolicy};

        let merge_on_read = |tree: &AnyTree| -> crate::Result<()> {
            let AnyTree::Blob(blob) = tree else {
                panic!("a tree that separates values");
            };
            blob.update_runtime_config(|c| {
                c.columnar = true;
                c.zone_map = true;
                c.delete_strategy = DeleteStrategyPolicy::all(DeleteStrategy::MergeOnRead);
            })
        };

        let folder = tempfile::tempdir()?;
        {
            // One interleaved columnar segment on level 1 that carries its own
            // range tombstone, merged with relocation off and the tombstone
            // kept.
            let tree = open(folder.path(), KvSeparationOptions::default())?;
            merge_on_read(&tree)?;
            fill_interleaved(&tree)?;
            tree.remove_range("key000000", "key000010", 10_000);
            tree.flush_active_memtable(0)?;
            let ids: Vec<_> = tree
                .current_version()
                .iter_tables()
                .map(crate::Table::id)
                .collect();
            tree.compact(Arc::new(MergeInto(ids, 1)), 0)?;
            assert_eq!(tree.table_count(), 1);
            assert_eq!(tree.storage_stats()?.blob_references.depth, FILES);
        }

        let tree = open(folder.path(), relocating())?;
        merge_on_read(&tree)?;
        let Some(table) = tree
            .current_version()
            .iter_tables()
            .next()
            .map(crate::Table::id)
        else {
            panic!("the merge left one table");
        };
        let before = blob_ids(&tree);
        tree.compact(Arc::new(RewriteOneTable(table, 1)), u64::MAX)?;
        let after = blob_ids(&tree);
        assert!(
            before.intersection(&after).count() < before.len(),
            "some interleaved files were rewritten: before {before:?}, after {after:?}",
        );
        assert_eq!(tree.storage_stats()?.blob_references.depth, 1);
        Ok(())
    }

    /// A deeply interleaved file is relocated only together with another file
    /// it overlaps. Here the budget covers one interleaved file and a stale
    /// file in an unrelated key range is relocated anyway: moving the lone
    /// interleaved file with it would keep its interleaving and stretch the new
    /// file's span across the gap, so it stays.
    #[test]
    fn a_locality_pick_without_an_overlapping_partner_stays() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let staleness = || {
            KvSeparationOptions::default()
                .staleness_threshold(0.01)
                .age_cutoff(1.0)
        };
        let interleaved: BTreeSet<u64>;
        {
            let tree = open(folder.path(), staleness())?;
            fill_interleaved(&tree)?;
            interleaved = blob_ids(&tree);
            // A file of its own, far from the interleaved keys, most of whose
            // values are then overwritten inline so it turns stale.
            for i in 0..64u64 {
                tree.insert(
                    format!("key{:06}", 100_000 + i),
                    vec![b's'; 2_048],
                    1_000 + i,
                );
            }
            tree.flush_active_memtable(0)?;
            for i in 0..48u64 {
                tree.insert(format!("key{:06}", 100_000 + i), "inline", 2_000 + i);
            }
            tree.flush_active_memtable(0)?;
            tree.major_compact(u64::MAX, u64::MAX)?;
            assert!(tree.stale_blob_bytes() > 0, "the separate file is stale");
        }

        // A budget that covers exactly one interleaved file, read and written,
        // on top of what the merge writes anyway.
        let budget = {
            let tree = open(folder.path(), staleness())?;
            let version = tree.current_version();
            let Some(table) = version.iter_tables().next() else {
                panic!("the merge left one table");
            };
            let table_len = table.fs.metadata(&table.path)?.len;
            let mut stale_written = 0;
            let mut smallest = u64::MAX;
            for link in table.blob_links()? {
                let Some(bf) = version.blob_files.get(link.blob_file_id) else {
                    panic!("a linked file exists");
                };
                if interleaved.contains(&link.blob_file_id) {
                    smallest = smallest.min(bf.physical_size()?);
                } else {
                    stale_written += link.on_disk(bf.meta());
                }
            }
            #[expect(clippy::cast_precision_loss, reason = "test budget from byte counts")]
            let ratio = (3 * smallest) as f32 / (table_len + stale_written) as f32;
            ratio
        };

        let Some(max_depth) = NonZeroU64::new(2) else {
            panic!("two is not zero");
        };
        let tree = open(
            folder.path(),
            staleness().relocate_for_locality(max_depth, budget),
        )?;
        let before = blob_ids(&tree);
        tree.major_compact(u64::MAX, u64::MAX)?;
        let after = blob_ids(&tree);
        assert!(
            interleaved.is_subset(&after),
            "no interleaved file moved without a partner: before {before:?}, after {after:?}",
        );
        assert_eq!(tree.stale_blob_bytes(), 0, "the stale file was rewritten");
        Ok(())
    }

    /// The budget is a share of what the merge writes anyway, which for a
    /// stale file is its values the tables still reference, not its whole
    /// length. A mostly dead stale file therefore adds little to the allowance.
    #[test]
    fn a_stale_file_adds_only_its_live_values_to_the_budget() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let staleness = || {
            KvSeparationOptions::default()
                .staleness_threshold(0.01)
                .age_cutoff(1.0)
        };
        let interleaved: BTreeSet<u64>;
        {
            let tree = open(folder.path(), staleness())?;
            fill_interleaved(&tree)?;
            interleaved = blob_ids(&tree);
            // A large separate file whose values are nearly all overwritten.
            for i in 0..256u64 {
                tree.insert(
                    format!("key{:06}", 100_000 + i),
                    vec![b's'; 2_048],
                    1_000 + i,
                );
            }
            tree.flush_active_memtable(0)?;
            for i in 0..250u64 {
                tree.insert(format!("key{:06}", 100_000 + i), "inline", 2_000 + i);
            }
            tree.flush_active_memtable(0)?;
            tree.major_compact(u64::MAX, u64::MAX)?;
            assert!(tree.stale_blob_bytes() > 0, "the separate file is stale");
        }

        // A budget that the stale file's whole length would stretch to every
        // interleaved file, and its live values would not.
        let (budget, allowance, sizes) = {
            let tree = open(folder.path(), staleness())?;
            let version = tree.current_version();
            let Some(table) = version.iter_tables().next() else {
                panic!("the merge left one table");
            };
            let table_len = table.fs.metadata(&table.path)?.len;
            let mut stale_written = 0;
            let mut stale_len = 0;
            let mut sizes = std::collections::BTreeMap::new();
            for link in table.blob_links()? {
                let Some(bf) = version.blob_files.get(link.blob_file_id) else {
                    panic!("a linked file exists");
                };
                if interleaved.contains(&link.blob_file_id) {
                    sizes.insert(link.blob_file_id, bf.physical_size()?);
                } else {
                    stale_written += link.on_disk(bf.meta());
                    stale_len += bf.physical_size()?;
                }
            }
            let all: u64 = sizes.values().sum();
            #[expect(clippy::cast_precision_loss, reason = "test budget from byte counts")]
            let ratio = (2 * all) as f32 / (table_len + stale_len) as f32;
            #[expect(clippy::cast_precision_loss, reason = "test budget from byte counts")]
            let allowance = f64::from(ratio) * (table_len + stale_written) as f64;
            (ratio, allowance, sizes)
        };

        let Some(max_depth) = NonZeroU64::new(2) else {
            panic!("two is not zero");
        };
        let tree = open(
            folder.path(),
            staleness().relocate_for_locality(max_depth, budget),
        )?;
        tree.major_compact(u64::MAX, u64::MAX)?;
        let after = blob_ids(&tree);
        let relocated: u64 = sizes
            .iter()
            .filter(|(id, _)| !after.contains(id))
            .map(|(_, size)| size)
            .sum();
        #[expect(clippy::cast_precision_loss, reason = "byte count to float")]
        let spent = (2 * relocated) as f64;
        assert!(
            spent <= allowance,
            "locality spent {spent} B against an allowance of {allowance} B",
        );
        Ok(())
    }

    /// A pick that ends up without a partner gives its budget back. Here the
    /// best-ranked file interleaves with two large files the budget cannot
    /// take along, and ranking first it would crowd out half of a cheaper
    /// overlapping pair; the pair is relocated instead of nothing.
    #[test]
    fn a_partnerless_pick_leaves_its_budget_to_an_overlapping_pair() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let flush_file = |tree: &AnyTree, keys: &[String], seqno: &mut u64| -> crate::Result<()> {
            for key in keys {
                tree.insert(key.as_str(), vec![b'v'; 2_048], *seqno);
                *seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            Ok(())
        };
        let (small, pair) = {
            let tree = open(folder.path(), KvSeparationOptions::default())?;
            let mut seqno = 0;
            // Region x: one file of 16 values interleaved with two of 32.
            let x = |slots: &[u64]| -> Vec<String> {
                (0..80u64)
                    .filter(|k| slots.contains(&(k % 5)))
                    .map(|k| format!("x{k:06}"))
                    .collect()
            };
            flush_file(&tree, &x(&[0]), &mut seqno)?;
            let small = blob_ids(&tree);
            flush_file(&tree, &x(&[1, 2]), &mut seqno)?;
            flush_file(&tree, &x(&[3, 4]), &mut seqno)?;
            // Region y: two interleaved files of 16 values each.
            let y = |parity: u64| -> Vec<String> {
                (0..32u64)
                    .filter(|k| k % 2 == parity)
                    .map(|k| format!("y{k:06}"))
                    .collect()
            };
            let before_pair = blob_ids(&tree);
            flush_file(&tree, &y(0), &mut seqno)?;
            flush_file(&tree, &y(1), &mut seqno)?;
            let pair: BTreeSet<u64> = blob_ids(&tree).difference(&before_pair).copied().collect();
            (small, pair)
        };
        assert_eq!(small.len(), 1);
        assert_eq!(pair.len(), 2);

        // Room for the small file and one of the pair, or for the whole pair,
        // but never for the small file together with a large one.
        let budget = {
            let tree = open(folder.path(), KvSeparationOptions::default())?;
            let version = tree.current_version();
            let size = |id: u64| -> crate::Result<u64> {
                let Some(bf) = version.blob_files.get(id) else {
                    panic!("blob file {id} exists");
                };
                bf.physical_size()
            };
            let a = small
                .iter()
                .map(|&id| size(id))
                .sum::<crate::Result<u64>>()?;
            let mut pair_sizes = pair.iter().map(|&id| size(id));
            let (Some(b), Some(c)) = (pair_sizes.next(), pair_sizes.next()) else {
                panic!("two files in the pair");
            };
            let (b, c) = (b?, c?);
            let allowance = 2 * (a + b) + c / 2;
            assert!(2 * (b + c) <= allowance, "the pair alone must fit");
            let mut table_bytes = 0;
            for table in version.iter_tables() {
                table_bytes += table.fs.metadata(&table.path)?.len;
            }
            #[expect(clippy::cast_precision_loss, reason = "test budget from byte counts")]
            let ratio = allowance as f32 / table_bytes as f32;
            ratio
        };

        let Some(max_depth) = NonZeroU64::new(1) else {
            panic!("one is not zero");
        };
        let tree = open(
            folder.path(),
            KvSeparationOptions::default().relocate_for_locality(max_depth, budget),
        )?;
        let before = blob_ids(&tree);
        tree.major_compact(u64::MAX, u64::MAX)?;
        let after = blob_ids(&tree);
        assert!(
            pair.is_disjoint(&after),
            "the pair was relocated: before {before:?}, after {after:?}",
        );
        assert!(
            small.is_subset(&after),
            "the small file stays: before {before:?}, after {after:?}",
        );
        Ok(())
    }

    /// Relocation copies frames verbatim, one output file per source codec, so
    /// interleaved files of two codecs collapse to one file per codec and no
    /// further. A compaction after that has nothing to gain and rewrites
    /// nothing, rather than rewriting the same two files over and over.
    #[cfg(feature = "lz4")]
    #[test]
    fn locality_relocation_stops_at_one_file_per_codec() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let Some(max_depth) = NonZeroU64::new(1) else {
            panic!("one is not zero");
        };
        let AnyTree::Blob(blob) = open(
            folder.path(),
            KvSeparationOptions::default().relocate_for_locality(max_depth, f32::MAX),
        )?
        else {
            panic!("a tree that separates values");
        };
        let tree = AnyTree::Blob(blob.clone());
        let mut seqno = 0;
        for file in 0..FILES {
            // Every other flush writes its blob file under lz4.
            let codec = if file % 2 == 0 {
                crate::CompressionType::None
            } else {
                crate::CompressionType::Lz4
            };
            blob.update_runtime_config(|c| c.blob_compression = codec)?;
            for i in 0..KEYS_PER_FILE {
                let k = i * FILES + file;
                tree.insert(format!("key{k:06}"), vec![b'v'; 2_048], seqno);
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
        }

        tree.major_compact(u64::MAX, u64::MAX)?;
        let collapsed = blob_ids(&tree);
        assert_eq!(collapsed.len(), 2, "one file per codec: {collapsed:?}");
        assert_eq!(tree.storage_stats()?.blob_references.depth, 2);

        tree.major_compact(u64::MAX, u64::MAX)?;
        assert_eq!(blob_ids(&tree), collapsed, "nothing left to gain");
        Ok(())
    }

    /// Relocation for locality is optional work: a quota that leaves room for
    /// the merged table but not for the rewritten values skips it, while the
    /// merge itself still runs.
    #[test]
    fn locality_relocation_is_skipped_when_the_quota_leaves_no_room() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let AnyTree::Blob(tree) = open(folder.path(), relocating())? else {
            panic!("a tree that separates values");
        };
        let tree = AnyTree::Blob(tree);
        fill_interleaved(&tree)?;
        let before = blob_ids(&tree);

        let used = tree.storage_stats()?.used_bytes;
        let AnyTree::Blob(blob_tree) = &tree else {
            panic!("a tree that separates values");
        };
        blob_tree.update_runtime_config(|c| c.storage_limit_bytes = Some(used + 128 * 1_024))?;

        tree.major_compact(u64::MAX, u64::MAX)?;
        assert_eq!(tree.table_count(), 1, "the merge itself must still run");
        assert_eq!(blob_ids(&tree), before);
        Ok(())
    }
}

/// A compaction finishes and reopens its outputs without the version lock:
/// while they finish, a reader can still take the version. Guards against
/// finishing the outputs under the write lock, which held every read and every
/// write of the tree for that table I/O.
#[test]
fn produced_compaction_outputs_leave_the_version_lock_free() -> crate::Result<()> {
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
    for seqno in 0..2 {
        for i in 0..100u64 {
            tree.insert(i.to_be_bytes(), b"value", seqno * 100 + i);
        }
        tree.flush_active_memtable(0)?;
    }

    let readable: Arc<parking_lot::Mutex<Option<bool>>> = Arc::default();
    {
        let probe = tree.clone();
        let readable = Arc::clone(&readable);
        tree.config.arm_while_outputs_finish(move || {
            *readable.lock() = Some(probe.version_history.try_read().is_some());
        });
    }
    tree.major_compact(64 * 1024 * 1024, 200)?;

    assert_eq!(
        *readable.lock(),
        Some(true),
        "the version is readable while the outputs finish"
    );
    Ok(())
}

/// A row-major table of cell rows, written before columnar tables were
/// switched on, is split into the columns of its fields when compacted after:
/// the bound a full compaction is admitted by covers those columns although
/// the table records none, so the merge writes no more than it was admitted
/// for.
#[cfg(feature = "columnar")]
#[test]
fn a_row_table_of_cell_rows_is_bounded_by_the_columns_it_splits_into() -> crate::Result<()> {
    use crate::SeqNo;
    use crate::blob_tree::field_row::{FIRST_FIELD_COLUMN, Field};

    let folder = tempfile::tempdir()?;
    let tree = match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    // No cell is separated: every field stays in the row, as a column.
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1 << 20),
    ))
    .open()?
    {
        crate::AnyTree::Blob(t) => t,
        crate::AnyTree::Standard(_) => panic!("expected a blob tree"),
    };
    // Many field columns of small cells, so the columns' own costs outweigh
    // the bytes of the rows.
    let fields: Vec<Field<'_>> = (0..200u16)
        .map(|column| Field::bytes(FIRST_FIELD_COLUMN + column, b"v"))
        .collect();
    for row in 0..64u64 {
        tree.insert_cells(format!("row-{row:04}"), &fields, row)?;
    }
    tree.flush_active_memtable(0)?;
    assert!(
        tree.current_version()
            .iter_tables()
            .all(|t| !t.metadata.columnar),
        "the rows were written row-major"
    );

    tree.index.update_runtime_config(|rc| rc.columnar = true)?;
    let (bound, _) = super::full_compaction_demand(&tree.index, &tree.current_version())?;
    tree.major_compact(u64::MAX, SeqNo::MAX)?;
    let version = tree.current_version();
    assert!(
        version.iter_tables().all(|t| t.metadata.columnar),
        "the compaction wrote columnar tables"
    );
    let written = version
        .iter_tables()
        .try_fold(0u64, |sum, t| t.live_file_size().map(|size| sum + size))?;
    assert!(
        written <= bound,
        "the merge wrote {written} bytes past the {bound} it was bounded by"
    );
    Ok(())
}

/// A tight-space pass over a table on a disk with a quarter of its size free
/// rewrites the whole table: each slice is sized to the room the volume has
/// when it starts, so the pass's outputs taking more than their punched
/// inputs freed shrink the next slice instead of ending the pass, and no
/// input is left restricted.
#[test]
fn a_tight_space_pass_rewrites_the_whole_table() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let mem = crate::fs::MemFs::with_capacity(u64::MAX);
    let tree = open_standard(dir.path(), |c| c.with_shared_fs(Arc::new(mem.clone())))?;
    for i in 0..20_000u64 {
        tree.insert(format!("key{i:08}").as_bytes(), [0xCD; 100], i);
    }
    tree.flush_active_memtable(0)?;
    let used = tree.storage_stats()?.used_bytes;
    mem.set_capacity(used + used / 4);
    tree.update_runtime_config(|c| {
        c.storage_admission_check = true;
        c.tight_space_compaction = true;
    })?;
    tree.major_compact(64 * 1024 * 1024, 0)?;

    let version = tree.current_version();
    assert!(mem.punched_bytes() > 0, "the pass ran in slices");
    assert!(
        version
            .iter_tables()
            .all(|t| t.restrict_lower_bound().is_none()),
        "a table is left restricted: the pass ended before its input did"
    );
    let last = tree.config.level_count - 1;
    assert!(
        version
            .iter_levels()
            .take(usize::from(last))
            .all(|level| level.is_empty()),
        "every key reached the last level"
    );
    Ok(())
}
