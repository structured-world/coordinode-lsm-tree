# Columnar page format

How a columnar row group is laid out so that a read can fetch the parts it
needs and verify them without reading the rest.

Addressing — what identifies a row, and which identifiers this layout may
change — is settled in `columnar-addressing.md` and assumed here. The short
form: a row group is the unit of logical addressing, a page is a physical
subdivision of a row group, and a page is invisible to addressing.

## A page is a block that the index does not name

A page is an ordinary block in every respect the block layer already
defines: the same header (magic, type, checksum, on-disk and uncompressed
lengths, transform flags), the same compression, encryption and Page-ECC
path, the same 256 MiB payload cap. Nothing in the block layer needed a new
concept, and deliberately so: a page that were a bespoke byte range would
need its own framing, its own checksum discipline and its own ECC placement,
all of which exist and are tested.

What a page does **not** have is an entry in the table's index. The index
keeps exactly one entry per row group, as it keeps one per data block today,
so `block_id` stays the row group's ordinal in key order and the retrieval
locator's `(block_id, slot)` keeps its meaning unchanged.

Pages are found through the row group's own directory instead.

## Layout of a row group

```text
row group, contiguous in the file:

  [ page directory ]              <- the block the index points at
  [ keys,  row page 0 ]           <- keys and MVCC metadata first
  [ keys,  row page 1 ]
  ...
  [ seqno, row page 0 ]
  ...
  [ value, row page r-1 ]
```

The group's rows are cut into **row pages**: consecutive row ranges shared by
every column. Each column part has exactly one page per row page, and a
column's pages are written together, in row order, before the next column's.

The directory comes **first**, immediately followed by the key pages. That
order is not cosmetic: those two are what every read of the group needs
before it knows anything else, and placing them adjacent lets one coalesced
read cover both. A point read is then one read for `directory + keys` and one
per column for the row pages holding the key, rather than a read of the
group.

Column-major order is what makes both kinds of partial read cheap. A
projection reads a column as one run of adjacent pages, whatever the number
of row pages; a point read takes one page per column, the row page its key is
on. A row-major order (all columns of row page 0, then of row page 1) would
make the point read one run and the projection one request per row page. The
projection is the read pages are for, so it gets the contiguous run.

### Row pages

A row page closes once its rows reach `columnar_page_size_policy` bytes, a
row's bytes being what it adds across every column (a fixed column's width, a
bytes cell's length plus its offset). A row page therefore holds at least one
row, and a row wider than the page size is a page of its own. A page size at
or above the row group size writes one row page per group, which is the
default: groups and pages are both 4 KiB, so a table that does not set either
writes what it wrote before row pages existed.

The row ranges are the same for every column, which is what lets a reader
decode row page `i` of the columns it wants and hand them back as one batch
of those rows, without joining anything. Reads produce one batch per row
page; a consumer that needs the group as one batch (salvage, verification)
joins them, framing each column once.

A key's versions are one run of rows, so they can cross a row page boundary.
A point read therefore takes every row page from the first whose keys reach
the needle through the one where its run ends, not only the first.

### The page directory

A block of type `ColumnPageDirectory`. Its header carries the row group's
row count, because a reader needs it before any page is read: it bounds every
slot, it is what the global row position sums, and a projection must not have
to decode the key page just to count rows.

| Header field | Width | Meaning |
|---|---|---|
| `version` | `u8` | directory wire version (2); an unknown one is refused |
| `page_count` | `u16` | page entries that follow the row pages |
| `row_count` | `u32` | rows in the group; the row pages sum to exactly this |
| `group_tag` | `u64` | names the group; every page repeats it in its stamp |
| `row_page_count` | `u16` | row pages that follow |

Then, for each row page in row order, its row count as a `u32`: none is zero,
and together they are the group's rows. Then, for each page:

| Field | Width | Meaning |
|---|---|---|
| `offset` | `u32` | start of the page, **relative to the end of the directory** |
| `length` | `u32` | on-disk length of the page, header included |
| `column_id` | `u16` | the column the page belongs to |
| `part` | `u8` | which part of that column's encoding the page holds |
| `flags` | `u8` | reserved; a reader rejects unknown bits rather than ignoring them |
| `row_page` | `u16` | the row page whose rows the page holds |

A page is named by `(column_id, part, row_page)`. A column id alone is 16 bits
wide already, so the three cannot share one field; keeping them separate is
also what lets a column's parts be addressed individually once encodings name
more than one.

A directory is refused unless it is a complete grid: every column part has a
page for every row page, and no `(column_id, part, row_page)` appears twice. A
part missing a row page would hand back a batch short of a column for those
rows, and a reader cannot tell a page that was never written from one that
was lost.

Offsets are relative to the group, not to the file. A relative offset is what
makes a row group relocatable by a compaction that copies it whole, and it is
the same reason the block layer refuses to bind a file offset into a block's
identity.

They are measured from the **end of the directory**, where the first page
starts, rather than from the directory's start. The directory's own on-disk
length depends on the transforms applied to it, so offsets measured from its
start would have to be known before the directory they are written into is
sealed. Measured from its end they are a plain running sum of page lengths,
and the reader already has the directory's length from its block header.

**The directory is never compressed.** It is a few dozen to a few hundred
bytes of offsets and ids that no codec shrinks usefully, and leaving it plain
means a reader never needs the table's data codec, or its zstd dictionary, to
find out where the pages are. It is still checksummed, and still encrypted and
ECC-protected when the table is.

**The directory is per row group, not per table.** The reference formats put
theirs in the file footer, which is affordable at their row-group sizes and
is not at ours: at a 32 KiB row group a 64 MiB SST reaches 256–640 KiB of
footer, read whole every time the table opens. The reasoning, with the
figures, is in `columnar-addressing.md`.

### How many pages

Not a constant. A page holds one part of one column's encoding for one row
page's rows, so a group has as many pages as its column parts times its row
pages. How many parts a column has is a property of the encoding expression
chosen for it — a dictionary-plus-bit-packed string column has five
(dictionary bytes, dictionary offsets, codes, bases, bit widths), a constant
column has none of its own, a dictionary-only column has one. The directory
is therefore variable-length and the reader must not assume a fixed page
count or a fixed part-to-page mapping.

## Identity: what the page layer must add

The block layer binds `table_id`, the block type and the codec context into
the AEAD AAD, and deliberately binds **neither the byte offset nor the tree
id**. The offset is excluded so a writer can encrypt every block in parallel
before placement decides where it lands; the tree id because it is a
process-ephemeral counter that would fail verification after a restart. The
stated consequence is that two blocks of the same table are interchangeable
at the AEAD layer, and that **position integrity is supplied one layer up, by
the authenticated index**.

For a row-major block that trade costs little: a swapped block is a lookup
miss, because a value never leaves the block that holds its key. A page
breaks that premise. Its values are separated from their keys by
construction, and it is not in the index, so two row groups' value pages of
equal length and row count swap undetected, and a key reads another key's
value. Under encryption that is a forged read, not a miss.

The page layer therefore makes every page **name itself**. Each page payload
opens with a stamp, and a reader refuses a page whose stamp is not the one its
directory entry implies:

```text
page payload = group_tag : u64 || column_id : u16 || part : u8
               || row_page : u16 || encoding
```

Inside an encrypted page the stamp is authenticated with the rest of the
payload, so it binds exactly as a field in the AAD would, without changing
the block layer's identity or its AAD layout. In a plain table, which has no
authentication at all, it still turns a misplaced page into a refused read
rather than a wrong value.

The tag is **carried in the directory, not derived from the group's
position**. A position-derived binding such as the group's ordinal would break
the byte-for-byte copy that salvage makes of intact groups: once an earlier
group is lost, every later group's ordinal in the copy shifts, and each copied
page would fail verification under its new one. A carried tag survives the
copy. What the binding needs is uniqueness within a table, and the writer
supplies it by issuing tags in strictly increasing order: a group it encodes
takes the next tag, and a copied group is accepted only above the last tag
already written. A salvage copying one table in key order always satisfies
that, because the source's tags increase and a re-encoded group takes a tag
no higher than the source tag it replaces.

Moving a page between slots of one group is refused by
`(column_id, part, row_page)`, which the stamp repeats. The row page is what
stops the swap row pages invite: two row pages of one fixed-width column hold
the same number of bytes whenever they hold the same number of rows, and
without it they would trade places and hand each row another row's value.
Moving a whole group, directory and pages together, is the block swap the
index already governs: the group's keys travel with its values.

## Framing overhead

The acceptance this format is held to asks for the overhead as a measured
figure rather than a promise of zero. Per row group, each page adds a block
header (33 bytes: SST blocks carry no flags byte, their transform comes from
the table descriptor), a 13-byte stamp and a 14-byte directory entry, 60
bytes in all; each row page adds its 4-byte row count to the directory; and
the group adds the directory's own 17-byte header and block header once. With
one row page per group:

| Row group | 8 pages | 20 pages |
|---|---|---|
| 32 KiB | 534 B (1.63%) | 1254 B (3.83%) |
| 128 KiB | 534 B (0.41%) | 1254 B (0.96%) |
| 256 KiB | 534 B (0.20%) | 1254 B (0.48%) |

Row pages multiply the pages: every column part pays its 60 bytes once per
row page. Four parts in 4 KiB row pages cost 244 bytes per row page, about 6%
of the data; in 16 KiB row pages, 1.5%. That is what the page size trades
against the rows a point read decodes, and why its default is chosen by
measurement together with the group size.

Encryption adds its per-page tag and frame on top, roughly doubling those.
Page-ECC does **not** scale with the page count in any meaningful way: its
parity is proportional to the data, so the ~12.5% a SECDED trailer costs is
paid with or without pages, and splitting only adds the rounding at each page
boundary.

These figures are a second reason the row group grows. At 32 KiB with a
richly-encoded schema the framing is already 3.6%; at 128 KiB and above it is
under 1% and stops being a term in the decision.

## What the index entry covers

The index entry for a row group spans the **whole group**: directory and
every page, as one extent starting at the directory. That keeps the index's
contract intact — an entry is still one contiguous extent at one offset — and
it is why every section keyed by a data block's file offset (zone map, seqno
bounds, the delete-position lookup) keeps working unchanged: the group starts
where its directory starts, which is where the block it replaces started.

A reader that wants only part of the group needs the directory's own length
first, and the index does not record it. It does not have to: a block header
is fixed-size and carries its on-disk length, so the reader fetches a
speculative prefix of the group, reads the directory's length out of its
header, and extends the read only if the directory did not fit. With the key
page adjacent, a prefix sized for the directory plus the key page usually
satisfies a point read in one request.

The prefix is 4 KiB, or the whole group when it is smaller. A directory is
17 bytes plus 4 per row page and 14 per page, so 4 KiB holds the directory of
some 280 pages, and the rest of it goes to the start of the key pages. A
longer directory, which many row pages of a wide schema make, is completed by
a second request. A
page the prefix covers, wholly or in part, is served from it rather than
asked for again; the reader then requests each run of consecutive wanted
pages that are not cached as one range.

## What a read does

**Point read.** Index gives the row group's extent. One read covers
the directory and the key pages (adjacent by construction). A key the key
pages do not hold ends the read there. Otherwise they yield the row pages
holding the key's versions, and the directory the pages of those row pages;
one further read per column fetches them.

**Projection.** As above, minus the slot resolution: the directory names the
pages for the projected columns' parts, and only those are read.

**Full scan.** Every page of the group is wanted, so the directory read is
followed by one coalesced read of the whole remainder — the same single
sequential read the group is today, plus the directory.

The extra dependent step (directory before pages) is real and is the cost of
the indirection. It is bounded by the directory being small and adjacent to
the first page anyone needs, and by the staged read path treating it as one
more stage to batch across tables rather than as a per-table stall.

## Row group size

A columnar table cuts its row groups at `columnar_row_group_size_policy`,
separately from the data block size a row-major table uses. The default is
still 4 KiB, the size row groups had before pages existed, because the
measurement below says a larger group is not yet a win on every read.

`db_bench --benchmark mixed-layout --num 70000`, one pass per size, on the
scenarios whose fixture is columnar:

| Row group | near-full scan, B/row | its time | sparse scan, B/row | point reads over a columnar base, B/row | their time |
|---|---|---|---|---|---|
| 4 KiB | 363 | 33.5 ms | 4574 | 330 | 190 ms |
| 16 KiB | 340 | 19.2 ms | 16196 | 225 | 213 ms |
| 64 KiB | 324 | 11.7 ms | 28281 | 206 | 295 ms |
| 128 KiB | 322 | 10.5 ms | 28088 | 205 | 405 ms |
| 256 KiB | 322 | 10.2 ms | 28053 | 205 | 624 ms |

Two costs grow with the group, and neither is the page layout's:

- **Pruning granularity.** A zone-map entry covers a whole group, so a ~1%
  predicate that prunes most 4 KiB groups prunes few 64 KiB ones, and the
  sparse scan reads six times the bytes. Statistics zones finer than the
  group are what the format separates them for.
- **Work per point read.** A point hit decoded and validated its pages
  whole, which was linear in the group's rows, so reads that the cache
  served still slowed down as the group grew. Row pages bound it by the page
  size instead: a hit decodes the key pages and the row pages that hold the
  key.

The dense scan is three times faster from 64 KiB up, which is what a larger
default is for once pruning also scales with the zone rather than with the
group.

## Relationship to the existing partial-decode section

The table-level `block_layout` section records, for a data block that
compressed into several inner zstd frames, the cumulative decompressed end
offset of each, so a range query can decode part of a frame. It solves
partial **decoding** and keyed by a block's file offset.

Pages solve partial **reading**, and make the decode question narrower rather
than replacing it: a page is its own block with its own frame, so decoding a
page is an ordinary whole-block decode. Whether `block_layout` still earns
its keep for data blocks is a separate question from this format, and is not
decided here.
