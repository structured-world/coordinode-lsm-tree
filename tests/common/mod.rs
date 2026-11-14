// Shared helpers for integration/property tests. Each test binary compiles
// this module independently, so not every binary uses every item.
// `#[expect(dead_code)]` becomes noisy here because binaries that happen to use
// every helper trigger `unfulfilled_lint_expectations`, while others need the
// suppression. Keep this as `allow` until the test helper layout changes.
#![allow(
    dead_code,
    reason = "each test binary compiles this module independently; not every binary uses every helper"
)]

use lsm_tree::Guard;
use proptest::test_runner::Config as ProptestConfig;

/// Default compaction target size for property tests (64 MiB).
pub(crate) const COMPACTION_TARGET: u64 = 64 * 1024 * 1024;

/// Default `cases` budget when neither `PROPTEST_CASES` env var nor a
/// per-suite override is set. 32 keeps the CI run quick; local runs
/// that want thorough coverage can crank `PROPTEST_CASES=512` or so.
const DEFAULT_PROPTEST_CASES: u32 = 32;

/// Default `max_shrink_iters` budget. 1000 is generous for thorough
/// shrinking; CI overrides via `PROPTEST_MAX_SHRINK=100` because at
/// `cases: 32`, shrinking rarely exceeds 20–50 iterations and the
/// extra budget just slows CI when a property does fail.
const DEFAULT_PROPTEST_MAX_SHRINK: u32 = 1000;

/// Shared property-test config used by every proptest suite in
/// `tests/`. Both knobs can be overridden via env var at run time:
///
/// - `PROPTEST_CASES=<N>` — how many random cases each property
///   runs. `proptest` itself already honours this env var for the
///   `cases` field; we set the field explicitly so the per-suite
///   default is `DEFAULT_PROPTEST_CASES` when the env var is
///   missing (instead of `proptest`'s much larger built-in
///   default of 256).
/// - `PROPTEST_MAX_SHRINK=<N>` — how many shrink iterations to run
///   when a property fails. `proptest` does NOT honour an env var
///   for this field, so the lookup happens here.
///
/// `fork: false` matches every existing suite's prior config — these
/// tests don't need process isolation.
pub(crate) fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PROPTEST_CASES);
    let max_shrink_iters = std::env::var("PROPTEST_MAX_SHRINK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PROPTEST_MAX_SHRINK);
    ProptestConfig {
        cases,
        fork: false,
        max_shrink_iters,
        ..ProptestConfig::default()
    }
}

/// Removes every `v{N}` manifest snapshot and `edits-{N}` log from a tree
/// directory, simulating a manifest loss while leaving the SSTs intact.
///
/// Keep in sync with the copy in `tools/sst-dump/tests/repair_smoke.rs` (a
/// separate crate, so the helper cannot be shared directly): both encode the
/// manifest file-naming convention (`v{N}` + `edits-{N}`).
pub(crate) fn nuke_manifest(dir: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_manifest = name
            .strip_prefix('v')
            .or_else(|| name.strip_prefix("edits-"))
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_manifest {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// The id of the tree's installed manifest snapshot: the highest `N` of a
/// `v{N}` (a snapshot not yet installed carries a temporary name). Only the
/// canonical decimal spelling counts, as in recovery: `v007`, `v00` and
/// `v+5` name nothing.
pub(crate) fn installed_snapshot_id(dir: &std::path::Path) -> std::io::Result<Option<u64>> {
    let mut installed = None;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        let canonical = name.strip_prefix('v').filter(|r| {
            !r.is_empty()
                && r.bytes().all(|b| b.is_ascii_digit())
                && (*r == "0" || !r.starts_with('0'))
        });
        if let Some(id) = canonical.and_then(|r| r.parse::<u64>().ok()) {
            installed = installed.max(Some(id));
        }
    }
    Ok(installed)
}

/// Returns the SST file paths under `<dir>/tables/`, sorted by id. The empty
/// files the folder keeps for tables to come are no SSTs and are left out.
pub(crate) fn sorted_sst_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("tables"))
        .expect("tables dir exists")
        .filter_map(Result::ok)
        .filter(|e| e.metadata().is_ok_and(|m| m.len() > 0))
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().parse::<u64>().is_ok())
        })
        .collect();
    v.sort();
    v
}

/// Flips a byte a fixed depth into the SST's `data` section, so the SST still
/// opens but one data block fails its checksum.
pub(crate) fn corrupt_data_region(path: &std::path::Path) -> std::io::Result<()> {
    corrupt_data_region_with(path, 0xFF)
}

/// [`corrupt_data_region`], flipping the bits of `mask` in that byte: one bit
/// is a fault Page-ECC corrects.
pub(crate) fn corrupt_data_region_with(path: &std::path::Path, mask: u8) -> std::io::Result<()> {
    // Locate the `data` section through the SFA trailer TOC rather than assuming
    // it begins at offset 0: a byte a fixed depth into the section's payload is
    // stable against tail growth (the index / filter / meta / trailer that follow
    // can change size, e.g. a new meta key) AND correct even if the data section
    // ever stops being written first.
    const DEPTH: u64 = 512;
    let pos = {
        let mut f = std::fs::File::open(path)?;
        let reader = lsm_tree::sfa::Reader::from_reader(&mut f)
            .map_err(|e| std::io::Error::other(format!("read SFA TOC: {e}")))?;
        let entry = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"data")
            .expect("the SST carries a data section");
        assert!(
            entry.len() > DEPTH,
            "data section (len {}) is too small to corrupt a block at depth {DEPTH}",
            entry.len(),
        );
        usize::try_from(entry.pos() + DEPTH).expect("position fits usize")
    };
    let mut bytes = std::fs::read(path)?;
    *bytes
        .get_mut(pos)
        .expect("corruption offset within the SST") ^= mask;
    std::fs::write(path, &bytes)
}

/// A backend delegating to [`lsm_tree::fs::StdFs`] that records, per batched
/// read, how many requests it carried.
#[derive(Clone, Default)]
pub(crate) struct BatchCountFs(std::sync::Arc<std::sync::Mutex<Vec<usize>>>);

impl BatchCountFs {
    /// The request counts recorded since the last take, forgotten.
    pub(crate) fn take(&self) -> Vec<usize> {
        core::mem::take(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl lsm_tree::fs::Fs for BatchCountFs {
    fn read_blocks_batched(
        &self,
        reqs: &mut [lsm_tree::fs::BlockRead<'_>],
    ) -> lsm_tree::io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(reqs.len());
        lsm_tree::fs::StdFs.read_blocks_batched(reqs)
    }

    fn open(
        &self,
        path: &std::path::Path,
        opts: &lsm_tree::fs::FsOpenOptions,
    ) -> lsm_tree::io::Result<Box<dyn lsm_tree::fs::FsFile>> {
        lsm_tree::fs::StdFs.open(path, opts)
    }

    fn create_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        lsm_tree::fs::StdFs.create_dir_all(path)
    }

    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> lsm_tree::io::Result<Vec<lsm_tree::fs::FsDirEntry>> {
        lsm_tree::fs::StdFs.read_dir(path)
    }

    fn remove_file(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        lsm_tree::fs::StdFs.remove_file(path)
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        lsm_tree::fs::StdFs.remove_dir_all(path)
    }

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> lsm_tree::io::Result<()> {
        lsm_tree::fs::StdFs.rename(from, to)
    }

    fn metadata(&self, path: &std::path::Path) -> lsm_tree::io::Result<lsm_tree::fs::FsMetadata> {
        lsm_tree::fs::StdFs.metadata(path)
    }

    fn sync_directory(&self, path: &std::path::Path) -> lsm_tree::io::Result<()> {
        lsm_tree::fs::StdFs.sync_directory(path)
    }

    fn exists(&self, path: &std::path::Path) -> lsm_tree::io::Result<bool> {
        lsm_tree::fs::StdFs.exists(path)
    }
}

/// The bytes of the frames an uncompressed separated value of `value_len`
/// bytes under `key` has of its own under the default frame sizes, what
/// garbage accounting charges once it is dropped.
///
/// A frame is a 40-byte header, an entry per value (14 bytes and the key)
/// and its payload. A value up to the 256 KiB chunk size is a frame of its
/// own; a larger one is a head (its entry and a chunk table of 8 bytes and
/// 21 per 256 KiB chunk) followed by the chunks, which carry no header. A
/// value below the 8 KiB block size shares a block instead, see
/// [`block_entry_len`].
pub(crate) fn stored_len(key: &[u8], value_len: u64) -> u64 {
    const HEADER: u64 = 40;
    const CHUNK: u64 = 256 * 1024;
    const CHUNK_ROW: u64 = 21;
    assert!(
        value_len >= BLOCK,
        "a value of {value_len} bytes shares a block"
    );
    let entry = ENTRY + key.len() as u64;
    if value_len <= CHUNK {
        HEADER + entry + value_len
    } else {
        HEADER + entry + 8 + value_len.div_ceil(CHUNK) * CHUNK_ROW + value_len
    }
}

/// The entry a value below the default 8 KiB block size takes in the block
/// it shares, uncompressed: what it is charged as block bytes once dropped.
pub(crate) fn block_entry_len(key: &[u8], value_len: u64) -> u64 {
    assert!(
        value_len < BLOCK,
        "a value of {value_len} bytes has frames of its own"
    );
    ENTRY + key.len() as u64 + value_len
}

/// The bytes of a blob entry's fixed fields, before its key.
const ENTRY: u64 = 14;

/// The default block size: smaller values share a block.
const BLOCK: u64 = 8 * 1024;

/// Convert an iterator guard into owned `(key, value)` byte vectors.
///
/// Returns `Err` on I/O failure (e.g. BlobTree indirection read) instead
/// of panicking, so property tests get a clear error message.
pub(crate) fn guard_to_kv(guard: impl Guard) -> lsm_tree::Result<(Vec<u8>, Vec<u8>)> {
    let (k, v) = guard.into_inner()?;
    Ok((k.to_vec(), v.to_vec()))
}
