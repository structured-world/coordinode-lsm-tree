# Data integrity & durability

Every record this engine stores is checksummed end to end: from the in-memory
memtable, through each on-disk data block, to the manifest that ties a version
together. On top of that detection layer the engine offers optional Reed-Solomon
error correction, a self-healing scrub, and AAD-bound encryption at rest. The
default configuration detects silent corruption everywhere and refuses to serve
or commit data it cannot vouch for; opt-in layers turn detection into recovery.

This document is the full map of that surface: what each layer protects against,
how to configure it, and how the engine behaves when it meets corruption it can
and cannot recover from.

## End-to-end integrity at a glance

| Stage | Mechanism | Detects | Corrects | Default |
|-------|-----------|---------|----------|---------|
| Memtable (RAM) | Per-KV digest at insert (`AtInsert`) | RAM bit-flip during a record's memtable residence | n/a | off (opt-in) |
| Data block (disk) | Block XXH3-128 | Bit-rot of the block as written | n/a | always on |
| Data block (disk) | Per-KV digest footer (`AtBlockCompile`) | Which record in a block diverged | n/a | off (opt-in) |
| Data block (disk) | Page ECC parity trailer | Bit-rot of the block | yes (SEC-DED / XOR / Reed-Solomon) | off (opt-in) |
| Data block (disk) | AAD-bound AEAD | Tampering, block-swap, codec/epoch relabel + confidentiality | n/a | off (opt-in) |
| Manifest | 5-layer hardening (XXH3-128 + ECC + AEAD + footer mirror) | Bit-rot, partial write, file substitution | partial (ECC / mirror) | mirror on, rest opt-in |
| On open | Manifest recovery modes | Malformed manifest records | salvage prefix (mode-dependent) | `AbsoluteConsistency` |
| Out of band | `verify_integrity`, `patrol_scrub`, `sst-dump verify`, `Config::repair` | Latent corruption anywhere | scrub heals; repair rebuilds manifest | n/a |

The rest of this document details each layer.

## Block checksums (always on)

Every on-disk `Block` (data, index, filter, manifest section, footer) carries an
XXH3-128 over its on-disk bytes in its header. The read path verifies it before
decoding, so a single flipped bit on disk surfaces as a typed
`Error::ChecksumMismatch` rather than a silently wrong value. This baseline is
part of the on-disk format and cannot be turned off: it is the floor every other
layer builds on.

## Per-KV checksums

Block-level XXH3 covers the bytes as written to disk, but only as one digest over
the whole block. A per-KV checksum gives finer protection: it pinpoints the
corrupt **entry** rather than just the block, and (at insert) also covers the
record's lifecycle in RAM.

Configured through `RuntimeConfig` (live-toggleable via
`Tree::update_runtime_config`; a change takes effect on the next flush /
compaction, so compaction migrates the choice without downtime):

- **`kv_checksums: KvChecksumPolicy`** selects which data blocks get a per-entry
  checksum footer:
  - `Off` (default): no per-KV footer, zero overhead.
  - `AllLevels`: every data block on every level.
  - `PerLevel(LevelMask)`: only the selected LSM levels (e.g. the hot tier), so
    cold archival levels skip the per-entry cost.
  - `PerTable(TableIdRange)`: only an inclusive table-id span (e.g. a
    compliance-sensitive table) opts in.
- **`kv_checksum_algo: ChecksumAlgorithm`** selects the digest: `Xxh3_64` (8-byte,
  fastest on SIMD hardware, default), `Xxh3Low32` (the same digest truncated to 4
  bytes), or `Crc32c` (4-byte, `crc32c` cargo feature).
- **`kv_checksum_compute_point: KvChecksumComputePoint`** selects *when* the
  digest is computed:
  - `AtBlockCompile` (default): at flush / compaction. No memtable overhead;
    covers the on-disk lifecycle but not the memtable-residence window.
  - `AtInsert`: at `tree.insert` / `WriteBatch` insert, then re-checked at flush.
    Closes the memtable-residence window, so a RAM bit-flip that corrupts a
    record while it sits in the memtable (the seconds-to-minutes before flush) is
    caught and surfaced as `Error::MemtableKvChecksumMismatch`. Requires a 4-byte
    algorithm so the digest fits the skiplist node's reserved slot with zero size
    growth; the algorithm is stored per node, so a mid-memtable algorithm change
    cannot misverify earlier records.

### Footer-wrapped layout

A per-KV-checked data block is a standard `Data` block payload, byte-for-byte
identical to a plain block, followed by a trailing footer:

```text
[ standard Data-block payload ]
[ kv_checksums_array: count × digest_size ]   little-endian digests
[ kv_checksum_algorithm: u8 ]                 ChecksumAlgorithm wire tag
[ kv_count: u32 ]                             entry count, little-endian
```

Wrapping rather than prefixing keeps the inner payload identical to a plain data
block, so the standard decoder / point-read / seek paths run on the inner slice
unchanged. The reader splits the footer off the end and verifies per-entry
digests only on the scrub / paranoid path; the block-level XXH3 already covers
the on-disk bytes on the hot read path. Footer presence is learned out of band
from the per-SST meta descriptor, so the hot paths never test a per-block flag.

### Digest domain

Each digest covers the entry's **logical content**, not its on-disk encoding:
`value_type ‖ seqno (LE u64) ‖ len(user_key) (LE u32) ‖ user_key ‖ value`. The
explicit `len(user_key)` frame keeps the domain injective across the key/value
boundary. Because the domain is logical, it is invariant to restart-interval
re-encoding, so a record's digest is reproduced every time a compaction re-packs
it: recompute and carry always agree, which is what lets `AtInsert` verify a
record at flush against the digest fixed at insert.

## Page ECC (correction at rest)

Where per-KV checksums and block XXH3 *detect* corruption, Page ECC *corrects*
it. It adds a parity trailer outside the block payload (so the inner block stays
decodable as-is) and is verified by the block's own XXH3 before decompress. ECC
is off by default; enable it with the `page_ecc` cargo feature plus
`Config::page_ecc` (SST data blocks, tree-static at open) and/or
`RuntimeConfig::page_ecc` (manifest blocks, live). `RuntimeConfig::ecc_scheme`
selects the algorithm, ordered cheapest-first:

- **`Secded`**: Hsiao SEC-DED per word: single-bit correct + double-bit detect.
  The cheap fast path that heals the overwhelmingly common single-bit flip before
  any heavier scheme runs.
- **`Xor`**: one XOR parity shard over `data_shards` data shards. Recovers a
  single lost or garbled shard.
- **`ReedSolomon { data_shards, parity_shards }`**: `parity_shards` Reed-Solomon
  parity shards (needs `>= 2`; single parity is expressed as `Xor`). Recovers up
  to `parity_shards` simultaneously corrupt shards.

Turning ECC on upgrades the detection layers from "detect and refuse" to "detect
and recover" with no change to call sites: the block layer transparently emits
and consumes the parity trailer.

## Self-healing

ECC corrects on read, but a corrected-on-read block is still corrupt on disk
until it is rewritten. Two mechanisms close that loop:

- **Auto-heal** (`RuntimeConfig::auto_heal`): after a read repairs a block via
  ECC, schedule a compaction rewrite so the healed bytes land on disk.
  Correction-on-read happens whether or not auto-heal is on; auto-heal only adds
  the durable rewrite.
- **Patrol scrub** (`scrub::patrol_scrub(tree, &options) -> PatrolScrubReport`):
  a proactive sweep that reads every block to surface latent errors before a
  reader hits them, healing along the way when auto-heal is enabled. Run it on a
  schedule (leader-only in a clustered deployment) to keep cold data from
  accumulating undetected bit-rot.

## Tamper-evident encryption at rest

Block encryption is AAD-bound: the AEAD additional-authenticated-data binds each
block's identity and transform context, so confidentiality comes with tamper
evidence. Beyond plain content tampering (any AEAD catches that), the AAD binding
detects block-swap (intra/inter-table), dictionary substitution, key-epoch
downgrade, cipher-suite downgrade, and compression-type relabel; each surfaces as
a typed decrypt error instead of a silently wrong plaintext. Enable it with
`Config::with_encryption(provider)` (AES-256-GCM or ChaCha20-Poly1305); off by
default, byte-identical to the non-encrypted format when disabled.

The exact wire format, AAD field layout, and the full threat-model matrix are
documented in **[aad-block-format.md](aad-block-format.md)**.

## Manifest hardening

The per-version manifest file (`v{N}`) is stored as a sequence of standard
lsm-tree `Block`s: one `BlockType::Manifest` Block per section, plus a
`BlockType::ManifestFooter` Block at the tail carrying the table of contents and
the manifest layout version. Every Block goes through the same XXH3-128 /
optional ECC / optional AEAD pipeline data Blocks use, so every protection that
applies to a data Block automatically applies to the manifest.

Five layers compose the manifest's integrity surface. L1 and L4 are always on as
part of the on-disk format; L2, L3, and L5 are independently configurable
(compile-time feature, encryption provider, or runtime config):

| Layer | Defends against | Config knob | Default |
|-------|-----------------|-------------|---------|
| L1, Block XXH3-128 | Bit-rot detection per section / footer Block | Always on (`Block` invariant) | always on |
| L2, Page ECC (Reed-Solomon (4, 2)) | Bit-rot recovery per Block | `RuntimeConfig::page_ecc` for **manifest Blocks** (current release) + `Config::page_ecc` for **SST data Blocks** (compile-time `page_ecc` feature gates both) | off (opt-in) |
| L3, AEAD encryption | Tampering detection + confidentiality | `Config::with_encryption(provider)` | off |
| L4, Footer Block tail hint | Reader locates footer without scanning | Always on (trailing `u32` size hint) | always on |
| L5, Head footer mirror | Partial-write / tail-bit-rot recovery via mirrored copy at offset 0 | `RuntimeConfig::manifest_footer_mirror` | on |

Manifest-side ECC and the head mirror are reachable through
`Tree::update_runtime_config` (for L2 *on manifest Blocks* and L5); SST data-block
ECC is tree-static via `Config::page_ecc` at open time and not affected by runtime
updates. AEAD is supplied at open via `Config::with_encryption` (L3). Runtime
toggles take effect on the next manifest write, and existing on-disk manifests
stay readable in their original format because each Block self-describes via its
header.

Failure-mode coverage with the defaults (mirror on, ECC off, AEAD off):

- **Bit-flip inside one section Block**: XXH3 surfaces it, other sections + the footer Block still read correctly (per-section isolation; see [`manifest_blocks::reader::tests::reader_isolates_corruption_to_one_section_other_sections_readable`](../src/manifest_blocks/reader.rs)).
- **Tail footer Block corruption**: reader falls back to the head mirror at offset 0 and continues.
- **Partial write mid-update on a fresh `v{N+1}`**: the prior version's manifest stays intact in its own `v{N}` file, and the atomic `CURRENT` rewrite either lands fully (pointing at `v{N+1}`) or stays at `v{N}`. A torn or truncated `v{N+1}` file is detected at open time: the reader's tail-footer + head-mirror probes both fail and `Tree::open` surfaces `ManifestFooterInvalid`. The head mirror inside each `v{N}` is a copy of THAT file's own tail footer Block (for tail-bit-rot recovery within one version), not a snapshot of a prior version's TOC.
- **Accidental file substitution / mislinking**: the `CURRENT` pointer carries an XXH3-128 of the referenced `v{N}` manifest's canonical footer content (version_id + TOC entries + per-section XXH3-128 each section Block already carries in its own header); `Tree::open` recomputes this from the parsed footer on read and refuses to follow a mismatched pointer. The digest binds logical content, not raw bytes: a section bit-flip that per-Block Page ECC heals on read does NOT trip this check (the section's TOC checksum is unchanged), preserving recovery. XXH3-128 is **not** a cryptographic MAC: an adversary with write access can craft matching content. Enable `Config::with_encryption(...)` (AEAD per Block) for adversarial tamper resistance.

Turning ECC on (`page_ecc = true`) upgrades the first three rows from "detect and
refuse" to "detect and recover" without any change to call sites: the Block layer
transparently emits / consumes the parity trailer per Block.

## Manifest recovery modes

`Config::manifest_recovery_mode` controls how the engine reacts to a malformed
MANIFEST record at `Tree::open` time. Each mode trades a different point on the
**strictness vs availability** axis; pick the one whose contract matches the
deployment.

| Mode | Behaviour on corruption | When to use |
|------|-------------------------|-------------|
| `AbsoluteConsistency` (default) | Any per-record decode mismatch (bad XXH3, invalid tag, truncated TOC entry, declared-count overrun) aborts the open with the original error. No data is silently dropped. | **Production default.** Surfaces every byte of corruption before the tree comes back online; matches what most workloads actually want. |
| `TolerateCorruptedTailRecords` | If the iteration over `tables` / `blob_files` runs out of bytes before the declared count is reached (truncated tail), keep everything that decoded cleanly before the cut and emit a `warn!` listing the dropped count. Any mid-record error that is NOT a clean tail truncation (bad checksum, etc.) still aborts. | **Power-loss-at-write-tail salvage.** Use when a crash mid-fsync left the MANIFEST tail incomplete and you'd rather come up with the last consistent prefix than refuse to open. Not a general bit-rot tolerance, only "the writer never finished". |
| `PointInTimeRecovery` | On the first record-decode mismatch inside the `tables` section, keeps the consistent prefix collected so far (records that decoded cleanly BEFORE the corrupt one in the current run, plus complete earlier runs in the same level, plus complete earlier levels) and drops everything after. "Record-decode mismatch" covers all three failure shapes the per-record loop produces: (a) framing-layer XXH3 mismatch, (b) framing-header structural failure (`len > MAX_FRAME_PAYLOAD`), and (c) payload-decode failure inside an otherwise-framed-OK record (e.g. `InvalidTag` from a corrupt `checksum_type` byte: the framing XXH3 happens to cover the corrupt byte, so the bytes pass the framing layer; the corruption only surfaces at per-entry decode). Analogous treatment on `blob_files`. Tail-truncation is still tolerated like `TolerateCorruptedTailRecords`. | **Post-corruption salvage with LSM invariants intact.** Use when a manifest has acquired real bit-rot (not just a truncated write) and you want the largest internally-consistent prefix the engine can still vouch for; matches RocksDB's `kPointInTimeRecovery` accept-the-prefix rule adapted to the level/run/table nesting. |
| `SkipAnyCorruptedRecords` | On any per-record decode mismatch (framing-layer XXH3 mismatch, payload-decode failure inside an otherwise-framed-OK record, or framing-header `BadHeader`), logs the skip and advances past the bad record using the framing-supplied length field. If the framing header itself is corrupt (length field outside the legal range, so the next-record boundary cannot be located), the rest of that section is dropped: there's no safe way to find the next record boundary in that case. Symmetric on `tables` and `blob_files`. | **Maximum-availability forensic mode.** Use when you'd rather open the tree with whatever survives than refuse to open at all; pairs with the `repair_db` tooling tracked in [#303](https://github.com/structured-world/coordinode-lsm-tree/issues/303) for the cases where even the surviving records aren't enough. |

When a non-default mode drops records, the recovery path logs `warn!` lines
describing what was tolerated. Individual table-IDs / blob-file-IDs are NOT
enumerated because they were never decoded. Warnings fall into two categories:

**Per-condition warns** (one warn for each malformed shape encountered, at the point of detection):

- `tables` section truncated before the `level_count` byte: tail-tolerant mode produces 0 levels.
- `tables` declared `table_count` exceeds remaining section payload (count header forged or entries truncated): loop walks bytes-actually-present and stops at the first EOF.
- `blob_files` section truncated before its count header: 0 blob files.
- `blob_files` declared count exceeds remaining section capacity: same forged-or-truncated shape, same walk-and-stop fallback.
- `blob_gc_stats` payload truncated (power-loss between the `blob_files` commit and the `blob_gc_stats` payload landing): tail-tolerant mode produces an empty `FragmentationMap`. GC stats are advisory (fragmentation re-accrues on the next compaction pass), so this is a "rebuild on next pass" outcome, not data loss. This is a single in-place warn with no later summary.

**Per-section summary warns** (at end of section processing, only if the section actually lost records):

- `tables` section, emitted only when `tables_dropped_to_tail > 0` OR `tables_truncated_headers > 0`. Reports two counters in one line: declared-but-missing table records (count header said N, only K < N records read before EOF, so N-K dropped) and the separate counter for level / run / `table_count` headers cut mid-byte (no records were supposed to be present yet for those levels / runs, so the headers contribute zero to record loss but the levels / runs themselves are absent).
- `blob_files` section, emitted only when `blob_dropped_to_tail > 0`. Reports the declared-but-missing blob-file records count, analogous to the tables-section record-drop counter. A `blob_files` section whose only damage was a missing count header surfaces the per-condition warn above but does NOT add a summary line.

Operators wanting a per-record audit trail should pair a tail-tolerant open with
an out-of-band integrity scan (see `verify::verify_integrity` /
`tools/sst-dump verify`).

For workflows where the MANIFEST is unrecoverable even under the lossy modes, the
`repair_db` tool ([#303](https://github.com/structured-world/coordinode-lsm-tree/issues/303))
rebuilds the MANIFEST from the SST files themselves.

## Point-in-time recovery

`Tree::create_checkpoint(target_path) -> CheckpointInfo` captures a consistent
snapshot by hard-linking every live SST + blob file into a fresh directory in
O(1) per file, with zero extra disk until the source files compact away.
Compaction continues during the checkpoint (deletions are deferred), and the
resulting directory opens as an independent tree, usable for backup, forensic
inspection, or rollback to a known-good point.

## Out-of-band verification & repair

- **`verify::verify_integrity(tree) -> IntegrityReport`**: walk the whole tree and
  verify every block's checksum (and ECC where present), returning a report
  rather than failing a read. Use it as a periodic health check.
- **`tools/sst-dump verify <file>`**: verify a single SST out of band: walk every
  block, check per-block XXH3, exit non-zero on corruption. Pair with
  `tools/sst-dump hex <offset>` to inspect a flagged region.
- **`Config::repair() -> RepairReport`**: rebuild a missing or corrupt manifest
  (standard and KV-separated / blob trees) from the on-disk SST files when even
  the lossy recovery modes cannot open the tree.
- **`salvage::salvage_sst(src, dest, &fs) -> SalvageReport`** (also
  `tools/sst-dump salvage <file> <dest>`): block-granular salvage of a single
  SST. Re-emit every block that passes its checksum (and ECC) into a fresh,
  fully-valid file, quarantine the corrupt ones, and report the key range each
  dropped, so one bad block costs only its own keys instead of the whole file. A
  columnar segment with a damaged sidecar degrades conservatively: a torn
  sub-column drops just its block, and a corrupt delete-bitmap reads as "all rows
  live, pending recompaction" rather than failing the open.
- **`Config::repair_with_salvage(true)`** (also `tools/sst-dump repair
  --salvage`): the manifest rebuild above, but an SST that fails verification is
  block-salvaged in place instead of being left out, and
  `RepairReport::salvaged` counts how many were recovered that way. A
  last-resort mode: a salvaged table may be missing the key ranges of its corrupt
  blocks.

## Regulated-data integrity

The detection-and-correction layers above are precisely the **integrity
controls** that medical, clinical, and other regulated environments are required
to implement: the assurance that stored records have not been silently altered or
destroyed.

- **HIPAA Security Rule, §164.312(c)**, "Integrity": implement mechanisms to
  protect ePHI from improper alteration or destruction, and electronic mechanisms
  to corroborate that ePHI has not been altered or destroyed. The end-to-end
  checksums (RAM through disk) and ECC are exactly such mechanisms.
- **FDA 21 CFR Part 11, §11.10**: electronic records must be "accurate, reliable"
  and protected throughout the retention period; tamper-evident. Block XXH3,
  per-KV checksums, and AAD-bound encryption provide the accuracy-and-tamper-
  evidence half of that requirement.
- **ALCOA+ (Accurate / Original / Enduring)**: the data-integrity principles used
  across GxP and clinical data: a record must remain an accurate, unaltered
  representation of the original for its full lifetime. End-to-end checksums plus
  self-healing ECC keep records bit-for-bit enduring.

This engine supplies the storage-layer **integrity controls** those frameworks
require; it is not, by itself, a certification. Full compliance is a property of
the whole system: it also needs the controls this engine does not provide (audit
trail of *who* changed a record, electronic signatures, access control, and a
Business Associate Agreement where applicable). What this engine guarantees is the
part it owns: a record you wrote is the record you read back, end to end, or you
get a typed error instead of silent corruption.
