# Host-write ledger

A [`CountingFs`](../src/fs/counting_fs.rs) wrapped around a tree's backend
counts everything the tree hands it: the bytes of every write call, the calls,
the syncs and their failures, the files it creates, renames and removes, each
charged to what it was written for. A device with a write budget, such as flash
with a rated endurance, needs bytes per day; a compaction rate limit sets
pacing, and misses flushes, metadata and ingestion. The ledger gives the bytes.
It needs no cargo feature, and a tree given a plain backend pays nothing for
it.

Counting is bound per physical medium. Give every tree and level route on one
medium a `CountingFs` counting into one shared `WriteIoStats`, and those on
another medium one counting into another set:

```rust
use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CountingFs, StdFs, WriteIoStats, WriteOrigin};
use std::sync::Arc;

let ssd = Arc::new(WriteIoStats::default());
let sd_card = Arc::new(WriteIoStats::default());
let on = |stats: &Arc<WriteIoStats>| {
    Arc::new(CountingFs::with_stats(Arc::new(StdFs), Arc::clone(stats)))
};

let tree = Config::new(path, seqno, visible_seqno)
    .with_shared_fs(on(&ssd))
    .level_routes(vec![LevelRoute { levels: 3..7, path: cold_path, fs: on(&sd_card) }])
    .open()?;

let start = sd_card.snapshot();
// ... a period of work, by every tree on the card ...
let period = sd_card.snapshot().since(&start).expect("one counter set");
let compacted = period.origin(WriteOrigin::CompactionTable);
println!("compaction wrote {} bytes to the card in {} calls", compacted.bytes, compacted.writes);
for (origin, files) in period.iter() {
    let name = origin.map_or("unattributed", WriteOrigin::name);
    println!("{name},{},{},{}", files.bytes, files.syncs, files.installed);
}
```

## What is counted

Every file the engine opens for writing names its `WriteOrigin` in its open
options, and the counters of that origin take:

| Counter | Meaning |
|---|---|
| `bytes` | Bytes the file's `write` calls accepted. A write the backend takes only part of counts the part; the retry of the rest counts the rest. Each byte once. |
| `writes`, `short_writes`, `write_errors` | Write calls that accepted bytes, those that accepted fewer than they were handed, and those that failed. |
| `opened`, `created` | Files opened for writing, and those opens allowed to create the file. |
| `syncs`, `sync_errors` | File syncs, data-only ones included, and those that failed. |
| `writebacks` | Ranges whose writeback a file started ahead of its sync without waiting (`sync_file_range` on Linux with the standard backend): no durability point, and no byte beyond `bytes`. |
| `truncated` | Lengths set on the files. |
| `installed` | Bytes of the tables and blob files an install put in the version, recorded into the set of the backend each file was written through. What a tree keeps, not what it wrote: it never fills in for `bytes`. |

A file opened without an origin is counted as `unattributed`; the engine opens
none, so a tree's `unattributed` stays zero. A file opened only for reading is
the backend's own handle: reads cost nothing more and are not counted.

Directory and device operations name no file handle, so they are counted
apart, in `directory`: directory and device syncs and their failures,
directories created and removed, files removed, entries renamed, hard and
symbolic links, clones and the bytes of the files cloned, punched holes and
files truncated by path.

What a tree took in from its callers, the denominator of host write
amplification, is a profiling figure of the `metrics` feature:
`tree.metrics().accepted_writes()`, the keys and values of inserts and merge
operands, the keys of point deletions, the bounds of range deletions, and what
ingestions took in (a columnar batch counts its rows' bytes across every
column). Every write is counted as it arrives, overwrites and retries
included, so the bytes a flush writes do not stand in for it. Divided into the
bytes of a set that only this tree writes through, over the same interval, it
is the tree's host write amplification.

## Origins

| Origin | Files |
|---|---|
| `flush_table`, `flush_blob` | The tables a flush writes from sealed memtables, and the blob files of their separated values. |
| `ingest_table`, `ingest_blob` | The tables and blob files an ingestion writes. |
| `compaction_table`, `compaction_blob` | The tables a compaction writes, and the blob files it relocates values into or writes them anew. |
| `manifest` | Manifest snapshots and the edit log appended to them. A checkpoint's manifest is one too. |
| `metadata` | Dictionaries, pins, restriction sidecars, the empty files the table file pool makes ahead of its tables, the lock file. |
| `repair` | Salvage, and the healing of damaged tables in place. |
| `checkpoint` | The copies a checkpoint makes of the tree's files where it cannot link them. |

A move, which gives a table another level, writes no table: only the manifest
records it.

## What it does not see

- **Below the filesystem.** These are host writes: the bytes handed to the
  backend. The page cache's writeback, the filesystem's metadata and journal,
  and the device's garbage collection and erase amplification are all below
  them. They are never NAND writes.
- **A caller's write-ahead log.** The tree does not own one; a log the caller
  keeps is written through its own files and is counted apart.
- **Clones done by copying.** A backend that cannot share a file's blocks
  copies it inside the clone call, below the wrapper. The clone's length is
  counted in `cloned_bytes`, apart from `bytes`: what the clones wrote lies
  between zero and it, and the wrapper cannot tell which.
- **Writes through another backend.** A repair run, a restore of a pinned
  version or the offline converter given a plain backend is not counted; given
  the medium's `CountingFs`, it is, like any tree.

## Scope and resets

A set lives as long as the caller keeps it: trees open and close on it, and a
tree reopened on the same backend counts on into the same set. Each set is
numbered when it is made and its readings carry the number: `since` of a
reading of another set is `None` whatever the values, so a reading of one set
is never subtracted from another's. The number tells sets of one process
apart; it names no medium. Nothing is persisted: a new process starts new
sets from zero, and keeping a budget across restarts, with whatever went
uncounted across one, is the job of whoever keeps the budget.

A reading is an observation of what was written, not a reservation of what
may be: admission that must stay within a budget reserves before the work and
reconciles against the ledger after it.

## Reconciling with the other counters

- For a flush or an ingestion that succeeds, `bytes` equals `installed`: a
  table or blob file is written once and installed whole. Written beyond
  installed is the output of work that failed or was given up.
- For a set only one tree writes through, with the `metrics` feature,
  `compaction_table.installed + compaction_blob.installed` equals
  `Metrics::compaction_work_measured().written`: both read the installed
  outputs' lengths. `compaction_table.bytes` beyond it is compaction output
  that was never installed.
- With the standard backend, the ledger's `bytes` and `writes` are what the
  process hands to `write` syscalls: on Linux, `wchar` and `syscw` of
  `/proc/self/io` move by exactly them when the tree is the only writer, and
  `strace` counts the same write, sync and writeback calls. An `io_uring`
  backend's writes do not pass those syscalls.

## A low-rate device workload

`benches/write_ledger.rs`, a tree on a `CountingFs` over the standard backend:
64 sensors reporting once a second for six hours,
24-byte readings, flushed every ten minutes and compacted after each flush,
three hours retained and the oldest hour deleted every hour with range
tombstones. Keyed by time first, every flush lies past the last; keyed by
sensor first, every flush overlaps the tables before it. Byte counts are the
same on every run on one filesystem; across ext4 and btrfs the time order
matches, and the sensor order differs by a hundred-odd bytes of 269 MiB (the
table below).

| | time order | sensor order |
|---|---|---|
| Payload inserted | 47.46 MiB, 1 382 400 values, 3 range deletions | 47.47 MiB, 1 382 400 values, 192 range deletions |
| `flush_table` | 46.70 MiB, 1 116 writes, 39 syncs | 47.53 MiB, 1 152 writes, 39 syncs |
| `compaction_table` | 0 | 221.71 MiB, 3 880 writes, 40 syncs |
| `manifest` | 0.03 MiB, 237 writes, 74 syncs | 0.02 MiB, 222 writes, 69 syncs |
| `metadata` | 49 empty files made | 97 empty files made |
| Directory | 5 syncs, 17 files removed | 8 syncs, 75 files removed |
| Caller's log | external | external |
| Written | 46.74 MiB in 1 353 calls, 118 syncs | 269.26 MiB in 5 254 calls, 156 syncs |
| Host write amplification | 0.98 | 5.67 |

Keyed by time, compaction gives tables another level without rewriting them
and an expired hour goes as whole files; keyed by sensor, it merges every flush
into the levels below, which is where the bytes go.

Against `/proc/self/io` on Linux, with the standard backend:

| Filesystem, order | Ledger bytes | `wchar` | Ledger calls | `syscw` | `write_bytes` |
|---|---|---|---|---|---|
| ext4, time | 49 006 327 | 49 006 327 | 1 353 | 1 353 | 49 471 488 (+0.9%) |
| ext4, sensor | 282 343 874 | 282 343 874 | 5 254 | 5 254 | 283 688 960 (+0.5%) |
| btrfs, time | 49 006 327 | 49 006 327 | 1 353 | 1 353 | 55 681 024 (+13.6%) |
| btrfs, sensor | 282 343 998 | 282 343 998 | 5 254 | 5 254 | 292 814 848 (+3.7%) |

`wchar` and `syscw` match the ledger exactly. `write_bytes`, what the process
sent to the block layer, adds the filesystem's own writes: little on ext4, more
on copy-on-write btrfs, most where the writes are smallest. What the device
then writes to its medium is beyond both.

Syscalls counted by `strace -f -c` over the same runs on btrfs, the bench's own
output to stdout and stderr taken out of `write`:

| Order | `write` | ledger `writes` | `fdatasync` + `fsync` | ledger file + directory syncs | `sync_file_range` | ledger `writebacks` |
|---|---|---|---|---|---|---|
| time | 1 353 | 1 353 | 113 + 5 | 113 + 5 | 36 | 36 |
| sensor | 5 254 | 5 254 | 148 + 8 | 148 + 8 | 237 | 237 |

Every write and sync call the process made is one the ledger counted, under
one origin each, and a directory sync is an `fsync` on the directory.
