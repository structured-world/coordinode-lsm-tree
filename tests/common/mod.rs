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
pub const COMPACTION_TARGET: u64 = 64 * 1024 * 1024;

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
pub fn proptest_config() -> ProptestConfig {
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

/// Removes every `v{N}` manifest file and the `current` pointer from a tree
/// directory, simulating a manifest loss while leaving the SSTs intact.
///
/// Keep in sync with the copy in `tools/sst-dump/tests/repair_smoke.rs` (a
/// separate crate, so the helper cannot be shared directly): both encode the
/// manifest file-naming convention (`v{N}` + `current`).
pub fn nuke_manifest(dir: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_version = name
            .strip_prefix('v')
            .is_some_and(|rest| rest.parse::<u64>().is_ok());
        if is_version || name == "current" {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Returns the SST file paths under `<dir>/tables/`, sorted by id.
pub fn sorted_sst_paths(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("tables"))
        .expect("tables dir exists")
        .filter_map(Result::ok)
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
pub fn corrupt_data_region(path: &std::path::Path) -> std::io::Result<()> {
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
        .expect("corruption offset within the SST") ^= 0xFF;
    std::fs::write(path, &bytes)
}

/// Convert an iterator guard into owned `(key, value)` byte vectors.
///
/// Returns `Err` on I/O failure (e.g. BlobTree indirection read) instead
/// of panicking, so property tests get a clear error message.
pub fn guard_to_kv(guard: impl Guard) -> lsm_tree::Result<(Vec<u8>, Vec<u8>)> {
    let (k, v) = guard.into_inner()?;
    Ok((k.to_vec(), v.to_vec()))
}
