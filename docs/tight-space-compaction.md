# Tight-space compaction

Compaction normally needs transient headroom: while it rewrites a set of input
tables into a merged output, both the inputs and the not-yet-installed output
exist on disk at once, so the peak footprint is roughly the inputs plus the
output. On a near-full disk that headroom may not exist, and the engine's
space-admission gate then *skips* the merge rather than run it into `ENOSPC`.
A skipped merge is safe but unproductive: the disk stays full and the data it
would have reclaimed (shadowed values, dead blob files) is never freed.

Tight-space compaction is the opt-in escape hatch for that situation. Instead of
writing the whole output before freeing any input, it rewrites the merge one
key-range slice at a time and reclaims each consumed input prefix *in place* with
a hole punch, so the peak transient footprint is one slice rather than the whole
rewrite. A compaction that could never fit as a single rewrite then completes on
a disk far smaller than the data it is rewriting.

## When it engages

Tight-space compaction is a fallback on the space-admission path; it never
changes a compaction that already fits. It engages only when **all** of these
hold:

| Condition | Why |
|-----------|-----|
| `RuntimeConfig::tight_space_compaction == true` | Opt-in. Default `false` — no behaviour change. |
| `RuntimeConfig::storage_admission_check == true` | The admission gate is what detects the no-fit condition that triggers the fallback. |
| The chosen merge does not fit, and no smaller fitting subset exists | This is the gate's `Skip` decision. A merge that fits (or narrows to a fitting subset) runs normally. |
| The destination volume advertises `FsCapabilities::punch_hole` | In-place reclaim requires `fallocate(PUNCH_HOLE)` (Linux ext4/xfs/btrfs/zfs/tmpfs). Without it the merge is skipped as before. |
| The combined input data spans more than one data block | A single indivisible block cannot be reclaimed incrementally. |

When any condition is unmet the merge falls back to the ordinary skip, so
enabling the flag can only ever turn a skip into a completed reclaim.

## How a slice works

The inputs are split into key-range slices sized to the free space. For each
slice `[lower, boundary)`:

1. **Merge** the slice over the surviving inputs into a finished output SST and
   fsync it.
2. **Install** one atomic, durable version edit that adds the output and either
   restricts each input that extends past the boundary to `[boundary, hi)` or
   drops an input the slice fully consumed. This edit is the slice's commit
   point: nothing before it is durable, everything after it is.
3. **Mark** each restricted input's exact bound in a small sibling
   `.restrict-bound` sidecar, written *strictly after* the install commits. The
   sidecar is the only record of the exact bound that survives a lost manifest,
   so a later rebuild from `tables/` can reconstruct the restricted view. Writing
   it after the commit makes its existence prove the restriction is committed
   (see *Crash safety*).
4. **Punch** the consumed data blocks of each restricted input (the bytes below
   the boundary), reclaiming their space while leaving the file in place for the
   restricted view that still serves the suffix.

A restricted view is a distinct handle over the same physical SST: reads of keys
below the boundary are served by the freshly installed output, reads at or above
it by the punched file's intact suffix (its index and the suffix blocks are never
touched). The punch fires only once every reader holding the prior, unrestricted
view has released it, so a concurrent snapshot keeps its own view intact until it
finishes — the reclaim is deferred, never unsafe.

The final slice merges the remainder and removes the inputs outright.

## Crash safety

Each slice is one durable version edit, so a crash mid-rewrite is always
recoverable, and recovery works from either of two sources.

**Manifest intact.** The version edit carries each input's persisted key-range
restriction, so normal recovery rebuilds the restricted view directly. A crash
before a slice's edit leaves an orphan output (swept on recovery) and an intact
input; a crash after it leaves the restriction recorded, so the consumed prefix
is already routed to the installed output.

**Manifest lost.** When the manifest is gone, recovery rebuilds it from the SST
files under `tables/` and cannot consult any commit state, so it needs an on-disk
record of each restriction. That is the `.restrict-bound` sidecar, and the order
in step 3 is what makes it trustworthy: because the sidecar is written *strictly
after* the version install commits, **a sidecar on disk proves its restriction is
committed.** An aborted or crashed slice can never leave an uncommitted sidecar
behind, so there is no rollback that must chase one down. The two remaining crash
windows are both safe:

- *Committed, sidecar written, not yet punched.* Recovery honors the bound (the
  installed output covers the dropped prefix); the punch is pure reclaim and its
  absence loses nothing.
- *Committed, crash before the sidecar is written.* No sidecar and an unpunched
  input, so recovery keeps the whole input unrestricted. This is safe because a
  slice output is a **superset** of its consumed inputs: tight-space slices run
  their merges *without bottommost GC* (no tombstone drop, no seqno zeroing, no
  range-tombstone application), even when the destination is the last level. So
  the committed output shadows the unrestricted input's prefix by sequence number
  for every key — including keys whose tombstone lived in a *different*,
  fully-consumed input, which ordinary last-level GC would have dropped along with
  the rows it covered. Nothing is resurrected and nothing is lost; only the
  redundant prefix waits for a later compaction to reclaim it. Space is still
  reclaimed here via the hole punch (GC is deferred to a later normal compaction).

Either way, reopening a partially-rewritten tree yields a consistent state with
every key readable, and a later compaction continues the work. The recovery rules
are specified in `docs/manifest-recovery.md`.

## KV-separated trees

KV-separated trees store large values in blob files and keep only handles in the
index SSTs, so their reclaimable space is mostly in blobs. Tight-space compaction
handles two cases:

**Dead blob files.** A gated KV-separated merge is *completed* rather than
skipped, and completing it lets the final removal drop the shadowed generation's
now-dead blob files — the real reclaim a skip would forgo. Each slice's blob
fragmentation is folded into the running GC stats; dead blob files are dropped
once every key is processed (a blob a later slice still references must not be
dropped early).

**Fragmented (live-but-stale) blob files — defragmentation.** A blob file that
holds a mix of live and dead entries keeps its dead regions until its live
entries are relocated into a fresh compact file. Under tight space that
relocation is done *in slices*, exactly like the SST path: each slice relocates
the live entries whose keys fall in `[lower, boundary)` into a new compact blob
file, rewrites the index handles to point at it, and — once the slice's edit is
durable — punches the consumed prefix of each stale blob file. Blob files are
immutable and key-sorted, so a slice consumes a *prefix* of each stale file; the
next slice resumes its scan exactly where the previous one stopped, never
re-reading a punched prefix. The final slice drops the fully-consumed stale
files. Peak transient stays near one compact slice instead of all stale files
plus the whole compacted file.

Because the index SSTs of a KV-separated tree are tiny (just handles) while the
payload lives in the blobs, slice boundaries are weighted by blob payload rather
than SST size, so each slice's relocated bytes stay within the free space the
gate flagged as tight.

Every value — relocated or not-yet-relocated — reads its latest version
throughout, and the result survives a reopen, including after a crash
mid-relocation (the partially-punched stale file's archive trailer and unpunched
suffix stay intact, and the installed slices route their keys to the compact
file).

## Cost and guarantees

- **Reads**: zero steady-state overhead. A restricted view costs one branch on
  the point-read path, taken only on the rare restricted table.
- **Writes**: the rewrite re-opens each input once per slice (re-reading its
  footer and block index) — heavier than a single-pass merge, which is the
  intended trade-off for an emergency, opt-in path.
- **Correctness**: identical to an ordinary merge. Every key is preserved, the
  latest version wins, and the result round-trips through a reopen.
- **GC is deferred, not performed here.** Slice merges run without bottommost GC
  (tombstones and superseded versions are retained) so every output is a superset
  that stays crash-safe under manifest repair (see *Crash safety*). Tight-space
  reclaims space through the hole punch; a later normal last-level compaction does
  the tombstone / seqno GC.

## Configuration

```rust
tree.update_runtime_config(|c| {
    c.storage_admission_check = true;   // enable the space gate
    c.tight_space_compaction = true;    // opt in to the sliced reclaim
})?;
```

Both toggles are live: turning them on takes effect on the next compaction
cycle, with no restart.
