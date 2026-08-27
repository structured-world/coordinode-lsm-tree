# External Write-Ahead Log Integration

This engine has **no internal write-ahead log**: a write lands in the active
memtable and becomes durable only when that memtable is flushed to an SST. A
crash between a write and the next flush loses every unflushed write. Durability
is therefore the caller's responsibility: if you need it, you log each write to
your own WAL before applying it and replay the tail on restart.

This document specifies the contract for building that external WAL on top of the
existing public API. No engine callbacks are required (see
[Why no hook API](#why-no-hook-api)); the contract is expressed entirely through
the write methods (`insert`, `remove`, `remove_weak`, `remove_range`, `merge`, and
`WriteBatch`), `flush_active_memtable`, `get_highest_persisted_seqno`, and
recover-on-open.

## The sequence number is the durability cursor

Every write carries a caller-supplied sequence number:

```rust,ignore
fn insert<K: Into<UserKey>, V: Into<UserValue>>(&self, key: K, value: V, seqno: SeqNo) -> (u64, u64);
```

The engine does not assign seqnos; the caller does, typically by drawing
monotonically increasing values from a [`SequenceNumberCounter`]. Because the
seqno is an input, it is the single cursor that ties your WAL records to engine
state: a WAL record and the write it produces share one seqno, and recovery is
expressed as "replay every WAL record with a seqno above your trim watermark `W`"
(the gap-free applied-and-persisted prefix defined in section 3, not the raw
persisted maximum).

MVCC visibility follows the seqno: a read at read-seqno `R` sees the newest version
of each key with `seqno < R`: the read seqno is an *exclusive* upper bound. The
visible watermark is published as the last applied seqno + 1, so a write at seqno
`s` becomes visible once the watermark reaches `s + 1` (see
[INVARIANTS.md](INVARIANTS.md), Snapshot / seqno). Re-applying a put or delete at its original seqno reproduces the same
version (an overwrite); a merge operand is the exception (re-applying folds it
twice), so replay must apply each record exactly once (see
[Recovery replay](#3-recovery-replay)).

## 1. Log before apply

For each write (or batch):

1. Draw the seqno(s) the write will carry (`SequenceNumberCounter::next`, or your
   own monotonic source). A `WriteBatch` shares a single seqno across all its
   entries.
2. Append the record (keys, values, and the seqno) to your WAL and make it
   durable (`fsync`, or your log's equivalent).
3. Only then call the matching write API at that seqno: `insert` for a put,
   `remove` for a point delete, `remove_weak` for a weak/single delete,
   `remove_range` for a range tombstone, `merge` for a merge operand, or a
   `WriteBatch`. Use the admission-gated `try_*` variant (`try_insert`,
   `try_merge`, ...) if you want over-budget writes refused up front. Apply the
   same operation that was logged, never collapsing to `insert`.
4. If the write API returns an error (an admission rejection, or `apply_batch`
   rejecting a malformed batch), the record was NOT applied: keep it in the WAL,
   do not advance your applied-and-persisted watermark, and retry or surface the
   failure. On success, publish your visible watermark so reads observe the write,
   e.g. `visible_seqno.fetch_max(seqno + 1)` (the exclusive read bound from the
   durability-cursor section).

The ordering is what guarantees recoverability: if the process dies after step 2
but before or during step 3, the record is in your WAL and replay re-applies it.
If it dies before step 2, the write never happened from the caller's point of
view, so there is nothing to lose. Never apply before logging: a write that
reaches the memtable but not the WAL is unrecoverable after a crash that drops the
memtable.

## 2. Durability points: when a seqno is safe to trim

A write is durable once the memtable holding it has been flushed:

```rust,ignore
fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<()>;
```

When this returns `Ok`, the active memtable has been written and synced as an SST,
so every seqno it contained is now on disk and survives a crash. To learn the
watermark, query:

```rust,ignore
fn get_highest_persisted_seqno(&self) -> Option<SeqNo>;
```

This returns the highest seqno present in the persisted SSTs (`None` for an empty
tree): the *maximum*, not a contiguity guarantee. A record is trimmable only once
it has both been **applied** (its `insert` / `remove` / `merge` / ... returned)
AND **persisted**. A record fsynced to your WAL but not yet applied (a crash
between the log write and the apply) is absent from every SST and must stay in the
WAL for replay, even if a later seqno was applied and flushed past it. So trim only
a gap-free prefix of applied-and-persisted records: when you apply in strict seqno
order and every record up to some seqno has been applied,
`get_highest_persisted_seqno()` is that contiguous watermark and records with
`seqno <= it` may be trimmed. If applies can be reordered or skipped (concurrent
appliers, or a failed apply that leaves a gap), the maximum is NOT contiguous, so
track the applied-and-persisted prefix yourself and never trim against the raw
maximum.

`create_checkpoint` gives the same guarantee for a point-in-time copy: it flushes
the active memtable first, then hard-links every resulting SST into the checkpoint
directory, so the checkpoint contains every write that had reached the active
memtable at the call (the persisted watermark advances to cover the flushed
writes).

Note `get_highest_persisted_seqno` is the *persisted* watermark, distinct from
`get_highest_seqno` (the max over memtable + SSTs, including not-yet-durable
writes). Trim against the persisted one only.

## 3. Recovery replay

On `Config::open` the engine recovers its state from the persisted SSTs alone (it
has no log of its own to replay). After open:

1. Recover from your **trim watermark `W`**, not the raw persisted maximum. `W` is
   the gap-free applied-and-persisted prefix you trimmed to (section 2); replay
   every WAL record that survived the trim, i.e. `seqno > W`. With strict gap-free
   in-order apply `W == get_highest_persisted_seqno()`, but if you retained a lower
   record across a gap (a logged-but-unapplied seqno below a flushed higher one) it
   is still in the WAL and MUST be replayed, so never use the raw maximum as the
   boundary, which would skip it. (Phrase the bound as `> W`, not a literal
   `W + 1`, which would overflow at the top of the seqno range.)
2. Replay each surviving record with its **original operation** and seqno: the
   same call it was logged for (`insert` for a put, `remove` for a point delete,
   `remove_weak` for a weak/single delete, `remove_range` for a range tombstone,
   `merge` for a merge operand, or the original `WriteBatch` for a batch). Never
   collapse every record to `insert`, which loses deletes, range tombstones, and
   merge semantics.
3. Do NOT re-apply records at or below `W`. For put / delete that would be harmless
   (re-applying at the original seqno reproduces the same MVCC version, an
   overwrite), but a **merge operand** re-applied on top of its already-persisted
   self is folded twice by merge resolution, so a counter would double-count. The
   strict `> W` boundary is correct for every record type, so use it
   unconditionally rather than relying on over-replay being idempotent. For
   merge-bearing workloads, apply gap-free so that `W` equals the persisted maximum
   and no already-persisted operand can sit above `W` to be replayed.

The strict boundary still covers the crash window in step 1 of
[Log before apply](#1-log-before-apply): a record that was logged and applied but
not yet flushed is, by definition, absent from the SSTs, so its seqno is above
your trim watermark `W` and the replay step covers it exactly once.

## 4. Replay after repair

A manifest repair (`Config::repair*`, or the one-call
`Config::open_or_repair`) can REGRESS persisted state below your trim
watermark `W`: a table the repair had to exclude loses every version it held,
including versions at or below `W` — while `get_highest_persisted_seqno()`
stays high because neighbouring tables survived. The standard `seqno > W` tail
replay from section 3 does not cover such a mid-history hole, and blindly
widening the replay would double-fold merge operands that DID survive. After
any repair, derive the obligation from the report instead:

1. **Ask the report.** `RepairReport::wal_replay_scope()` aggregates
   `lost_coverage` (which covers excluded tables AND kept lossy salvaged
   copies — a copy that dropped corrupt blocks lost data too, scoped by the
   damaged SOURCE's coverage, not the shrunken copy's) and
   `unknowable_losses`:
   - `TailOnly` — nothing was lost; run the section 3 replay unchanged.
   - `LostUpTo(b)` — in addition to the tail, replay every RETAINED record
     whose key falls inside a `lost_coverage` range and whose seqno is at or
     below `b`.
   - `FullHistory` — same, with no seqno filter: a loss whose sequence base
     died with the manifest, or an excluded table whose coverage never
     parsed (`unknowable_losses`), leaves no bound that scopes the damage.
     When the trigger is an UNSCOPABLE loss (`unknowable_losses` non-empty),
     there is no key range either: reconcile retained records across the
     ENTIRE keyspace in ONE pass — the survivor subtraction of step 3 runs
     over the whole tree, and iterating `lost_coverage` ranges on top of
     that pass would subtract the same survivors twice.
2. **Puts and deletes replay blindly.** Re-applying one at its original seqno
   reproduces the same MVCC version, and a surviving NEWER version still wins
   by seqno — over-replay inside the lost ranges is harmless for them. This
   covers EVERY logged write kind: a range deletion is selected by SPAN
   OVERLAP with a lost range (its half-open `[start, end)` against the
   inclusive coverage bounds) and replays blindly too, and a batch replays
   per entry — each entry the lost range covers gets the same treatment its
   standalone form gets (merge entries go through step 3's presence check).
3. **Merge operands need a presence check.** An operand that survived the
   repair and is replayed again is folded twice. Collect the surviving
   operands inside each lost range with
   `scan_since_seqno_in_range(0, range)` (it skips every SST outside the
   range, so this is proportional to the damage, not the store), build the
   multiset of `(key, seqno, operand bytes)` it delivers, and re-apply only
   the WAL records NOT covered by that multiset (decrement on match). The
   scan's event stream mirrors the read path — an operand appears once per
   application the tree will make — which is exactly the set to subtract.
4. **Replay order**: tail and lost-range records alike in increasing seqno
   order, each with its original operation, exactly as in section 3.
5. **Compaction folding needs a superseded-record floor.** A merge chain a
   compaction FOLDED leaves a plain surviving value at the chain head's
   seqno and no operand events, so absence from step 3's multiset does not
   prove an operand was lost. Track, per key, the highest surviving value /
   point-tombstone seqno (and the surviving range tombstones) from the same
   scan, and skip ANY retained record — an operand, but also a put or
   delete — at or below that floor: it is already incorporated or
   superseded. A range tombstone covers only records STRICTLY below its
   seqno (the engine's suppression rule): a record tied with the
   tombstone's caller-assigned seqno survives a read and must replay. A
   surviving WEAK (single-delete) tombstone contributes nothing to the
   floor: it does not incorporate older history — it annihilates exactly
   its matching put during compaction and can then expose an older value,
   so the paired put must replay from a lost SST or the weak delete later
   consumes a different, older value than the source's pair. Two coordination rules make the floor decidable:
   - **WAL seqnos start at 1.** Bottommost compaction ZEROES the seqno of
     entries below its GC watermark, so a surviving value at seqno `0` is a
     zeroed survivor embodying the key's whole folded history — treat its
     floor as unbounded. A deployment that starts its log at seqno 0 cannot
     tell that survivor from a genuine first write.
   - **Do not fold or zero history a lost RANGE DELETION may target.** A
     zeroed survivor's true age is unknowable, so a replayed range deletion
     from a lost SST cannot decide whether the survivor pre- or post-dates
     it. Keep the compaction GC watermark (`seqno_threshold`) at or below
     the seqno up to which the tree is already authoritative — reconciled,
     or provably not in need of replay — before letting it fold history.

**Retention is what makes this recoverable.** Records at or below `W` were
trimmable under section 2, and a trimmed record inside a lost range is gone
from both the engine and the log — that is precisely the loss
`lost_coverage` reports. A deployment that wants repairs to be lossless must
ARCHIVE trimmed segments (a retention window) instead of deleting them at the
watermark, and `wal_replay_scope()` tells it how deep the archive must reach:
up to `b` for `LostUpTo(b)`, unbounded for `FullHistory`.

Run the replay BEFORE publishing your visible watermark, so readers never
observe the repaired-but-not-yet-reconciled state.

## Executable companion

This recipe is not only specified here; it is executed and self-verified in the
repository, so a future engine change that violated the contract would break a
test rather than silently diverge from the prose:

- **Reference WAL:**
  [`tests/external_wal/reference_wal.rs`](../tests/external_wal/reference_wal.rs)
  is a minimal append-only WAL: append + `fsync`, trim through a watermark, and
  replay the survivors in order. It is illustrative (a `std`-only test/dev
  surface, not the `no_std` production path), and is the worked example the spec
  refers to.
- **Worked example:**
  [`examples/external_wal.rs`](../examples/external_wal.rs) runs the full recipe
  (log-before-apply, flush, trim to `W`, crash, reopen, replay above `W`) and
  asserts the recovered state. Run it with `cargo run --example external_wal`.
- **Integration test:** [`tests/external_wal.rs`](../tests/external_wal.rs)
  drives the recipe across every write kind through a crash and asserts the
  recovered state is byte-for-byte a non-crashed run's. Its contract guards
  prove a wrong recovery is *detectably* wrong: collapsing ops to `insert`,
  re-applying a merge at or below `W`, or replaying from the raw persisted
  maximum instead of `W` each make the recovery diverge. Section 4 has its own
  pair there: `repair_and_reconciled_replay_recover_identical_state` runs a
  crash that also destroys a flushed SST through `open_or_repair` plus the
  documented reconciliation and reproduces the non-crashed state, and
  `blindly_replaying_lost_range_merges_double_counts` proves that skipping the
  survivor subtraction double-folds a surviving merge operand.

Keep this spec and that proof in sync: a change to the contract should update
both.

## Why no hook API

A thin observability hook surface (`before_write_batch`, `after_flush`,
`after_checkpoint`) was considered and is **not** provided: the existing API
already expresses the full contract. The seqno is a caller input, so the caller
already knows every seqno it applied without a callback; `flush_active_memtable` /
`create_checkpoint` return `Ok` exactly when their durability guarantee holds; and
`get_highest_persisted_seqno` reports the watermark to trim against. Adding
callbacks would duplicate information the caller already has and couple the engine
to a notification lifecycle it does not need. If a future requirement cannot be
expressed through this surface, a hook trait can be added then; document-first
until proven necessary.

[`SequenceNumberCounter`]: https://docs.rs/coordinode-lsm-tree/latest/lsm_tree/struct.SequenceNumberCounter.html
