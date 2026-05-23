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

/// Convert an iterator guard into owned `(key, value)` byte vectors.
///
/// Returns `Err` on I/O failure (e.g. BlobTree indirection read) instead
/// of panicking, so property tests get a clear error message.
pub fn guard_to_kv(guard: impl Guard) -> lsm_tree::Result<(Vec<u8>, Vec<u8>)> {
    let (k, v) = guard.into_inner()?;
    Ok((k.to_vec(), v.to_vec()))
}
