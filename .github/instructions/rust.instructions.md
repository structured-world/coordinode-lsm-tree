---
applyTo: "**/*.rs"
---

# Rust Code Review Instructions

## Review Priority (HIGH → LOW)

Focus review effort on real bugs, not cosmetics. Stop after finding issues in higher tiers — do not pad reviews with low-priority nitpicks.

### Tier 1 — Logic Bugs and Correctness (MUST flag)
- Data corruption: wrong compaction merge logic, incorrect key ordering, dropped or duplicated entries during merge
- Off-by-one in block/segment boundaries, fence pointer lookups, or range scans
- CRC/checksum mismatches: computing checksum over wrong byte range, verifying against stale value
- TOCTOU on file operations: checking file existence then opening, or reading metadata then acting on it without holding a lock
- Incorrect merge semantics: tombstones not propagated to lower levels, point deletes applied out of order
- Missing validation: unchecked block offset, unvalidated segment metadata from disk
- Resource leaks: unclosed file handles, temporary files not cleaned up on error paths
- Concurrency: data races on shared segment lists, lock ordering violations, missing synchronization on manifest updates
- Error swallowing: `let _ = fallible_call()` silently dropping I/O errors that affect data integrity
- Integer overflow/truncation on sizes, offsets, or block counts

### Tier 2 — Safety and Crash Recovery (MUST flag)
- `unsafe` without `// SAFETY:` invariant explanation
- `unwrap()`/`expect()` on disk I/O or deserialization (must use `Result` propagation)
- Crash safety: write ordering that leaves data unrecoverable after power loss (e.g., updating index before data is fsynced, deleting old segments before new manifest is durable)
- Partial write exposure: readers seeing a segment file that is still being written
- fsync ordering: metadata (manifest, WAL) must be durable before the operation it describes is considered committed
- Hardcoded secrets, credentials, or private URLs

### Tier 3 — API Design and Robustness (flag if clear improvement)
- Public API missing `#[must_use]` on builder-style methods returning `Self` or other non-`Result` types that callers might accidentally discard
- `pub` visibility where `pub(crate)` suffices
- Missing `Send + Sync` bounds on types used across threads
- `Clone` on large types (segment readers, block caches) where a reference would work
- Fallible operations returning `()` instead of `Result`

### Tier 4 — Style (ONLY flag if misleading or confusing)
- Variable/function names that actively mislead about behavior
- Dead code (unused functions, unreachable branches)

## DO NOT Flag (Explicit Exclusions)

These are not actionable review findings. Do not raise them:

- **Caller-handled edge cases**: Before flagging a function for not handling an edge case (empty collection, `None` vs `Some(empty)`, missing guard), check call sites visible in the PR diff. If all visible callers already handle the case, the function's behavior is part of a deliberate contract — not a bug. Only flag if the edge case is truly unhandled end-to-end within the scope of the PR.
- **Type-system-prevented issues**: Before flagging a potential collision, overlap, or misuse, check whether distinct enum variants, wrapper types, or visibility modifiers make the issue structurally impossible. A `WeakTombstone` variant that never appears in user-facing merge paths cannot collide with user data regardless of key/seqno overlap.
- **Documented design decisions** (Tier 3-4 only): When code has a comment explaining WHY a specific approach was chosen, trust the documented reasoning for style and API design choices. Flag only if the comment contradicts the actual code behavior — not if you would have chosen a different approach. This exclusion does NOT apply to Tier 1 (logic bugs, data corruption) or Tier 2 (safety, crash recovery) — always flag those regardless of documentation.

- **Comment wording vs code behavior**: If a comment says "flush when full" but the threshold is checked with `>=` not `>`, the intent is clear — the boundary condition is a design choice. Do not suggest rewording comments to match exact comparison operators.
- **Comment precision**: "returns the block" when it technically returns `Result<Block>` — the comment conveys meaning, not type signature.
- **Magic numbers with context**: `4` in `assert_eq!(header.len(), 4, "expected u32 checksum")` — the assertion message provides the context. Do not suggest a named constant when the value is used once in a test with an explanatory message.
- **Block sizes and compression levels**: Specific numeric values for block sizes (e.g., `4096`), compression levels, or bloom filter parameters are domain constants, not magic numbers, when used in configuration or tests with surrounding context.
- **Segment ID and sequence number formats**: Internal naming conventions for segment files and sequence counters are implementation choices, not review findings.
- **Minor naming preferences**: `lvl` vs `level`, `blk` vs `block`, `seg` vs `segment` — these are team style, not bugs.
- **Import ordering**: Import grouping or ordering style (e.g., std vs crate vs external order). Unused imports are NOT cosmetic — they cause `clippy -D warnings` failures and must be removed.
- **Test code style**: Tests prioritize readability and explicitness over DRY. Repeated setup code in tests is acceptable.
- **`#[allow(clippy::...)]` in untouched legacy code**: Do not flag `#[allow]` on lines outside the PR diff. For new or modified code within the diff, flag `#[allow]` and request migration to `#[expect(..., reason = "...")]`.
- **Temporary directory strategies**: Using `tempfile::tempdir()` vs manual temp paths — both are valid in test code.

## Scope Rules

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues **outside the diff**, suggest opening a separate issue.
- **Read the PR description.** If it lists known limitations or deferred items, do not re-flag them.
- This fork has **multiple feature branches in parallel**. A hardening that seems missing in one PR may already exist in another. Check the PR description for cross-references.

## Rust-Specific Standards

- `#[expect(lint, reason = "...")]` is the standard for lint suppressions **when the crate’s MSRV is at least Rust 1.79** (the stabilization release for `#[expect]`). `#[expect]` warns when suppression becomes unnecessary, catching stale allowances.
  - For repos with **MSRV ≥ 1.79**: Flag any new `#[allow(lint)]` in the PR diff and request migration to `#[expect(..., reason = "...")]`. `#[allow]` is accepted only for legacy code on lines outside the diff, with a migration note recommended.
  - For repos with **MSRV < 1.79** (where `#[expect]` is unavailable): `#[allow(lint)]` is acceptable, but must be accompanied by a brief `// reason: ...` or `// SAFETY: ...` style comment explaining the justification. Do **not** request migration to `#[expect]` unless/until the repo explicitly raises its MSRV to ≥ 1.79.
- `TryFrom`/`TryInto` for fallible conversions; `as` casts need justification
- No `unwrap()` / `expect()` on I/O paths — use `?` propagation
- `expect()` is acceptable for programmer invariants (e.g., lock poisoning, `const` construction) with reason
- Code must pass `cargo clippy --all-features --all-targets -- -D warnings`

## Testing Standards

- Test naming: `fn <what>_<condition>_<expected>()` (sole exception: `src/compaction/leveled/test.rs` may use `fn test_<scenario>()`)
- Corruption tests: tamper the relevant on-disk field (checksum, block header, segment metadata) and assert the expected error
- Use the same serialization/compression APIs as production; avoid test-only helpers that change framing or length-prefixing.
- Use `tempfile::tempdir()` for test directories — ensures cleanup even on panic
- Integration tests that require specific disk layout or large data use `#[ignore = "reason"]`
- Prefer `assert_eq!` with message over bare `assert!` for better failure output
- Hardcoded values in tests are fine when accompanied by explanatory comments or assertion messages
## no_std + alloc Compatibility (Direction, Not Hard Mandate)

This crate is moving toward a `no_std + alloc` build. The CI `no-std-check` job builds against `thumbv7em-none-eabihf` with `--no-default-features --features alloc` and is **the direction we're heading**, not a hard mandate yet. Per-crate tier tables are NOT a required artifact — the rules below describe what reviewers should SUGGEST (not block) for changes that move in the right direction, and what they should ACTIVELY FLAG when changes regress no-std readiness.

When in doubt, suggest — don't gatekeep.

### Direction rules (suggest)

1. **No-std capability is the target.** Every library crate SHOULD in principle support `no_std + alloc` builds. Existing code that uses `std` is grandfathered until someone migrates it.
2. **Primitive selection order for NEW code**: `core::*` → `alloc::*` → external `no_std + alloc` crate → `std::*` behind `#[cfg(feature = "std")]` → unconditional `std::*` (last resort). Reviewers SHOULD suggest higher-tier alternatives.
3. **Cargo.toml shape**: a crate that already declares `default = ["std"]`, `std = []`, `alloc = []` should keep them coherent. `src/lib.rs` should open with `#![cfg_attr(not(feature = "std"), no_std)]` + `extern crate alloc;` once the migration is underway.
5. **Public API surface** SHOULD prefer `core` / `alloc` types where the crate's tier permits. Suggest, don't block.
13. **Prefer `core::*` over `std::*` re-exports** whenever the type is available there (e.g. `core::sync::atomic::AtomicU64`, `core::cmp::Ordering`). One-line suggestion — `std::*` re-exports of `core` types break the build under `no_std` even when binary-identical for `std`.
14. **The std-vs-no_std choice is per-primitive, NOT per-crate.** A crate tiered `std-only` is still encouraged to prefer the faster no_std-ready primitive when one exists (e.g. `hashbrown`, `parking_lot`, `smallvec`, `rustc_hash`, `bytes`) — these are normally faster than their `std::*` counterparts on hot paths.
18. **Tier reclassification suggestions** (e.g. `alloc` → `std-bound, leaf-isolated`) are welcome in the PR description. Reviewers MUST NOT block on a missing tier table — soft expectation only.

### Active flags (reject if introduced)

4. **CI gate**: the `no-std-check` job MUST run against a no-std-only target (e.g. `thumbv7em-none-eabihf`) with `--no-default-features --features alloc`. Host targets with available `std` MUST NOT be used — they silently pull `std` in via transitive features and hide real failures. PRs disabling or weakening this job's target/flags are rejected.
6. **NEW `std::collections::HashMap` / `HashSet` uses in alloc-tier modules** — prefer `hashbrown::HashMap` / `HashSet` (no_std + alloc), or `rustc_hash::FxHashMap` for internal-ID keys. Suggest in std-tier modules; reject in alloc-tier.
7. **`std::sync::Mutex` / `RwLock` in NEW code on hot paths.** Prefer `parking_lot::Mutex` / `RwLock`. `spin::Mutex` only in genuinely no-std contexts and only for very short critical sections. Existing usage is grandfathered.
8. **NEW `std::sync::OnceLock` for fallible init.** Use `once_cell::sync::OnceCell::get_or_try_init` or `once_cell::race::OnceBox`.
9. **NEW `thread_local!` in alloc-tier modules.** Replace with caller-managed scratch parameters or atomic-pointer patterns.
10. **NEW `std::io::Error` in public APIs of alloc-tier modules.** Define a crate-local error enum; `From<std::io::Error>` impls live behind `#[cfg(feature = "std")]`. Tolerate in `std-only` tier.
11. **NEW `std::time::Instant` / `std::time::SystemTime` in public APIs of alloc-tier modules.** Use a caller-provided clock trait or a `#[cfg(feature = "std")]`-gated convenience wrapper. Tolerate in `std-only` tier.
12. **NEW `std::thread::*` in alloc-tier modules.** Threading must be hoisted to a higher-tier crate.
15. **Adding `use std::*` to an alloc-tier module that was previously no-std-clean — without justification — is a regression.** Suggest a no_std alternative first; only reject if the PR's stated direction is no-std cleanup and this addition undoes that progress. No per-crate tier table is required to make this judgement.
16. **`no-std-check` compile-error count MUST NOT increase per PR.** While a crate is in transition, the job MAY run `continue-on-error: true` and the count tracked as a metric — but it MUST decrease or stay equal, never increase.
20. **Adding a transitive dependency that pulls `std` into an otherwise no-std-clean module — without justification — regresses no-std readiness.** Suggest alternatives first; reject only if the PR's stated direction is no-std cleanup and the addition undoes it.

### Always-applies

17. **Test code (`#[cfg(test)]`), benches (`benches/`), and binaries (`src/bin/`) are NOT subject to no-std rules** — they MAY use `std::*` freely. Only library code in `src/lib.rs` and its submodules is governed.
19. **Doc comments and rustdoc `# Examples` blocks** on no-std-capable APIs SHOULD NOT depend on `std::*` types if the API itself does not. Doctest examples requiring `std` should be gated `#[cfg(feature = "std")]`. Reject only when the API is documented as alloc-only and the doctest contradicts that.
