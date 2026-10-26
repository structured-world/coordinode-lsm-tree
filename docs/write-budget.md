# Write budget

A device with a rated endurance, flash above all, is rated in bytes written
per day over its life. A [`RateLimiter`](../src/rate_limiter.rs) paces how
fast compaction writes; it does not bound how much a medium takes over a day.
A [`WriteBudget`](../src/write_budget.rs) does: a byte allowance per period of
wall time, which every byte written to the medium is charged against and
which compaction reserves its output from before it runs.

It needs no cargo feature. A tree whose backends carry no budget reserves
nothing and pays nothing.

## Binding

A budget belongs to one physical medium. It charges what that medium's
[write ledger](write-ledger.md) counts, so it is given to the medium's
counting backend, and every tree and level route on the medium gets that
backend:

```rust
use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CountingFs, StdFs, WriteIoStats};
use lsm_tree::write_budget::{Allowance, WriteBudget};
use std::sync::Arc;
use std::time::Duration;

let day = Duration::from_secs(86_400);
// 8 GiB a day for the card, at most 2 GiB of it saved up for one burst.
let card = Arc::new(WriteBudget::new(
    Arc::new(WriteIoStats::default()),
    Allowance::new(8 << 30, day).with_burst(2 << 30),
));
let on_card = Arc::new(CountingFs::with_budget(Arc::new(StdFs), Arc::clone(&card)));
// The SSD only counts.
let on_ssd = Arc::new(CountingFs::new(Arc::new(StdFs)));

let tree = Config::new(path, seqno, visible_seqno)
    .with_shared_fs(on_ssd)
    .level_routes(vec![LevelRoute { levels: 3..7, path: cold_path, fs: on_card }])
    .open()?;
```

A merge into L3 and below reserves from the card's budget; one into L1 or L2
reserves nothing, since the SSD has no budget.

## What is charged

Everything the ledger counts on the medium: the bytes the files' writes
accepted, of every origin, and the length of every clone. Flushes, ingestion,
the manifest, repair and checkpoints are charged as they write, and the budget
never holds them back. Compaction is the work it holds back: a merge reserves
the bytes it expects to write (the inputs' file sizes and the blob files it
relocates) on each medium before it runs, gives the reservation back as each
output file lands in the ledger, and gives the rest back when it returns. A
merge of a tree that separates values may write new blob files though it
relocates none (a merge operator's result, a compaction filter's replacement,
a locality relocation), so it asks the blob folder's budget too: it reserves
nothing there, but an overrun budget holds it back, and what it writes is
charged as it lands. A
move and a drop write no table and need no credit. The tight-space pass, which
rewrites a merge no volume has room for slice by slice, reserves at most what
that merge would write, and is let through when credit is short (see below).

These are host writes. What the device then programs into its flash, with its
own garbage collection, lies below them and is not known here.

## Refill

The allowance refills continuously with wall time, `bytes_per_period` every
`period`, and holds at most `burst` bytes of credit, so over any span `T` it
credits at most `burst + bytes_per_period * T / period`. A reservation is
granted only while the credit left past what is already reserved covers it.
Writes the budget does not hold back, and output past what a merge reserved,
can take the charge past the credit: the budget is then overrun and grants
nothing until the refill has repaid it.

The budget reads the ledger whenever it is asked something and dates what it
finds then: the refill first, the new bytes after. A full budget gains nothing
for the time between a write and the next reading, which makes it stricter
than its bound, never looser.

## When compaction runs anyway

Holding compaction back keeps runs in L0 and debt in the levels; past a point
the tree stops its writers. A merge without credit therefore runs anyway, as
an override charged like any other work and counted in the report, when:

- the tree is at a stop threshold of its
  [backpressure](../src/backpressure.rs) on an axis compaction drains: its L0
  runs or its pending compaction bytes. The memtable axis is drained by
  flushes, which no budget holds back, and does not override it.
- the volume has no room for the merge chosen, and the merge is the smaller,
  reclaiming one the space gate ran instead, or the tight-space pass where no
  smaller merge fits: holding it back would leave the volume as full as it is.
- the caller forces the budget with `WriteBudget::set_forced`: a read latency
  it cannot keep, or space it must reclaim that the engine does not see. It
  clears the flag to let the budget hold work back again.

With backpressure thresholds set, an exhausted budget first slows the writers
(the slowdown tier) and only then, at the stop tier, lets compaction overrun.
An override lets through a merge the strategy chose; it does not make a
strategy merge what it would not.

## Reading it

`WriteBudget::report` returns:

| Field | Meaning |
|---|---|
| `credit` | Bytes of credit; negative while overrun. |
| `reserved` | Bytes held by merges in progress that their output has not yet charged. |
| `discretionary_bytes`, `mandatory_bytes` | Bytes charged since the budget began, compaction's and everything else's. |
| `refused` | Merges that waited for want of credit. |
| `waiting_since` | The earliest refusal since a merge last got credit: how long work has waited. |
| `waiting_bytes` | The largest merge refused since one last got credit: the work that waits. Zero for a merge refused only on a medium it reserves nothing on, such as the blob folder's, which waits for that budget's overrun to be repaid. |
| `credit_in` | How long until the refill covers that work beside what is reserved, if nothing else is charged meanwhile; `None` when it never will (the work is more than the burst holds, or nothing refills), so it runs only as an override. |
| `backpressure_overrides`, `space_overrides`, `forced_overrides` | Merges that ran past the budget, and their bytes, by reason. |
| `unsustainable` | The writes the budget does not hold back took more than the allowance refilled over the last period, or over as much of it as has passed since the budget began or its allowance changed: no throttle of compaction keeps this load within the budget. A span shorter than the burst takes to refill is not judged, since the burst absorbs it by design. |

`unsustainable` is the signal that the allowance is too small for the offered
load. The budget never answers it by weakening durability: the writes it
cannot hold back go on, and the overrun is reported.

## Restarts

The budget lives in memory, and a new process starts a new ledger. Keeping a
budget across restarts is the caller's: `WriteBudget::checkpoint` reads the
credit and the wall time it was refilled to, and `WriteBudget::resume` starts
a budget from them. It refills only the time since the checkpoint, and takes
off a bound the caller states for what was written after the checkpoint and
never charged, so a restart does not quietly hand back a full period.
`WriteBudget::new` starts with a full burst, which is a fresh medium's state,
not a restarted one's.

The clock is the process's wall clock, or one given with
`WriteBudget::with_clock`. One that steps back refills nothing until it
passes the time already counted.

## Choosing a strategy

Every strategy is held to the budget by the gate: a merge it chooses waits
for credit. Only planned compaction also shapes the tree to the budget, as
below; leveled compaction keeps its shape and waits, which under a tight
budget holds L0 at its stop threshold, where merges run as overrides. For a
medium with a budget, use planned compaction.

## Planned compaction

[`Planned`](../src/compaction/planned/mod.rs) sizes its rounds to the
compaction rate a tree can afford. With a budget on the medium of the last
level, that rate is also capped at the allowance's rate less the model's write
rate, which the flushes spend from the same allowance, so it plans a tree the
budget pays for instead of one whose merges the budget would keep waiting.
