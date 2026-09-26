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
  [ zone block, column a ]        <- statistics of each non-key column,
  [ zone block, column b ]           one block per column, when the group
                                     has several row pages
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

### Column types and their order

Each column's header names its physical type as a `(tag, width)` pair. The
type decides the framing and, for the statistics and a predicate, the order
the column's values have. The engine attaches no logical meaning to a column;
a type is enough to order the values and nothing more.

| Tag | Width | Type | Order |
|---|---|---|---|
| 0 | 1..=255 | opaque fixed-width | none |
| 1 | 0 | variable-width bytes | byte-wise over the value |
| 2 / 3 | 1, 2, 4, 8, 16 | unsigned integer, little / big endian | numeric |
| 4 / 5 | 1, 2, 4, 8, 16 | two's-complement signed integer, little / big endian | numeric |
| 6 / 7 | 4, 8 | IEEE 754 binary float, little / big endian | `totalOrder` (IEEE 754-2019 5.10) |

An ordered column is compared through its **comparable encoding**: a bytes
column's value itself, a number's `width` bytes most significant first, with
the sign bit flipped for a signed integer and, for a float, every bit
inverted when negative and the sign bit set when not. Byte-wise order of that
encoding is the numbers' order: `-NaN < -inf < ... < -0 < +0 < ... < +inf <
+NaN` for floats, so every value, NaN and each zero included, has one place.
Statistics record it and a predicate's bounds are given in it, so a range
over a number column returns the rows the same range returns over the same
values stored as bytes in that encoding. A null has no value: it is left out
of the statistics and matches no range.

An opaque column has no order. It gets no statistics, no block or row page is
pruned on it, and a predicate over it does not run; the scan reports that
rather than return an all-matching mask a caller cannot tell from a match.
The engine never infers an order from a width: a fixed-width column whose
writer declared no number type stays opaque. The engine's seqno column is an
unsigned little-endian 8-byte number, so a seqno range prunes and filters.

A table stores a row's seqno relative to its `global_seqno`, a bulk-ingested
one every row at `0`, while a scan speaks effective seqnos. A seqno range is
therefore moved into the table's coordinates, down by its `global_seqno`,
before its statistics or its row filter see it, and a range wholly below a
table's base rules the table out.

### The page directory

A block of type `ColumnPageDirectory`. It gives a reader the row group's rows
before any page is read, because they bound every slot, they are what the
global row position sums, and a projection must not have to decode the key
page just to count rows. Counts, row counts and lengths are LEB128 varints
(`var`).

| Header field | Width | Meaning |
|---|---|---|
| `version` | `u8` | directory wire version (3); an unknown one is refused |
| `group_tag` | `u64` | names the group; every page repeats it in its stamp |
| `row_page_count` | `var` | row pages, at most `u16::MAX` |
| `part_count` | `var` | column parts; parts times row pages is at most `u16::MAX` |
| `zone_block_count` | `var` | zone blocks after the pages, at most `u16::MAX` |
| `head_zones_len` | `var` | on-disk length of the zone block between the directory and the pages, zero for none |

When `head_zones_len` is not zero, the column whose zones that block holds
follows (`u16`). Then each column part once, in the order its pages lie: its `column_id`
(`u16`) and `part` (`u8`). Then, for each row page in row order, its row count
(`var`): none is zero, and the group's rows are their sum. Then one on-disk
length (`var`, header included) per page, part by part and, within a part,
row page by row page.

A page is named by `(column_id, part, row_page)`. A column id alone is 16 bits
wide already, so the three cannot share one field; keeping them separate is
also what lets a column's parts be addressed individually once encodings name
more than one.

The directory records none of the three per page, nor where a page starts:
the pages lie back to back, from the end of the directory and of the head
zone block, in exactly the order of their lengths, so a page's offset is the
sum of the lengths before it, and its
place in the list is its column part and row page. That makes the grid
complete by construction, every column part holding one page per row page,
and leaves a writer one layout, the one that makes a run of one column's
pages one contiguous read. A directory that named each page's placement
itself (an earlier layout spent 14 bytes a page on an offset, a length, the
three names and a flags byte) could describe gaps, overlaps and missing
pages, all of which a reader had to refuse; at 64 KiB groups of 4 KiB row
pages over the mixed-layout records, the directory averaged 1892 bytes that
way, the key column's zones included, and averages 262 this way, with the key
zones in a block of their own (below).

Then, for each zone block after the pages, in the order they follow them, the
column whose zones it holds (`u16`) and its on-disk length (`var`). The
directory, its head zone block, its pages and its other zone blocks fill the
group exactly; a reader refuses a group whose index entry and directory
disagree about where it ends.

**A read caches the directory decoded**, not as the block it was read as, and
keeps the head zone block's zones on it once a read has needed them. A
directory of many row pages is dozens to hundreds of pages whose decode
rebuilds each one's placement, and every read of the group starts from it
(a point read twice: once for the key pages, once for the row pages holding
the key). On the mixed-layout point reads over a columnar base, at 64 KiB
groups of 4 KiB row pages, re-decoding it per read took 1.43 s where 4 KiB
groups take 232 ms; cached decoded it takes 295 ms, reading 222 B per row
where 4 KiB groups read 330. A directory cached by a read that did not select
by the key lacks the key zones; the first point read reads them once and
caches the directory with them, so later ones decode no zone block.

### Statistics zones

A group of more than one row page carries a **statistics zone** per row page
and ordered column (see [Column types](#column-types-and-their-order)): the
row page's null count for the column, and a byte range of the column's
comparable encoding holding every non-null value of the column in the row
page. A read prunes the
row pages whose zones cannot hold what it looks for, before reading them, so
pruning is as fine as the pages it can skip rather than as coarse as the
group. This is the statistics-zone granularity the issue separates from the
compression unit and the read page, set equal to the read page because
nothing finer can be left unread. A group of one row page has no zones: its
zone is the group's zone-map entry, which prunes the whole group already.

Every ordered column's zones are a `ColumnZones` block of their own, read
only by a read that prunes on that column. A full scan and a projection that
does not prune never pay for any; a read that prunes on a narrow column does
not pay for a wide column's zones, whose 64-byte bounds make them the widest.
The blocks differ only in where they lie:

- **The key column's lies right after the directory**, before the pages, and
  the index entry records its length beside the directory's. A point read
  takes the directory and the key zones in one request, and the key zones
  take it from there to the one key page that can hold its key, instead of
  reading every key page of the group.
- **Every other column's lies after the pages.**

This is the separation Parquet makes with its page index: an `OffsetIndex`
(where the pages lie) apart from a `ColumnIndex` (their statistics) per
column chunk, the sort column's included, so a reader reads the statistics of
only the columns it filters on. ORC keeps one `ROW_INDEX` stream per column
and Vortex a zone map per column for the same reason. Any zones inside the
directory make every read of it pay for them. With every zone there, a
key-only projection over 4 KiB values in 4 KiB row pages read 1/15 of what a
scan of keys and values reads, where the pages alone give it well under 1/20.
With only the key's there, a scan pruning on another column paid for the key
zones at every group it admitted: on the mixed-layout sparse scan at 64 KiB
groups of 4 KiB row pages, 4439 B per returned row, and 4367 B with the key
zones in their own block, point reads reading the same bytes as before. One
zone block for every non-key column together would make a pruning read pay
for the columns it does not prune on: a predicate on an 8-byte field read
7296 B per returned row with one block, and 6157 B with one per column.

A zone's bounds are cut to 64 bytes, Parquet's default for its page
statistics. A prefix of the minimum is still a lower bound, and a prefix of
the maximum with its last byte below `0xFF` raised is still an upper bound; a
maximum whose 64-byte prefix is all `0xFF` has no such bound and is recorded
as unbounded. A cut bound prunes less, never wrongly. The comparison is
byte-wise, as the zone map's is, and the comparator contract (equal keys are
equal bytes) is what makes byte-wise key zones sound for a point read under
any comparator: the row page holding a key holds its bytes, and so its
byte-wise zone contains them.

| Zone field | Width | Meaning |
|---|---|---|
| `null_count` | `var` | null rows of the column in the row page |
| `flags` | `u8` | bit 0: no upper bound; other bits reserved and refused |
| `min` | bound | lower bound, at most 64 bytes |
| `max` | bound | upper bound, at most 64 bytes; absent without one |

A bound is written as what it adds to a reference: the length of the prefix
it shares with it (`u8`), then the length of the rest (`u8`) and the rest. A
lower bound's reference is the same column's zone on the row page before, its
upper bound when it has one and its lower bound otherwise, and nothing on the
first row page; an upper bound's reference is its own lower bound. A sorted
column's neighbouring zones, and the two bounds of a narrow number, share
most of their bytes: over the mixed-layout records at 64 KiB groups of 4 KiB
row pages, the zone block of an 8-byte field averaged 390 bytes a group with
whole bounds and a `u32` null count, and averages 171 this way.

Both places hold the same form: a count of the columns they describe
(`var`), the column ids (`u16`), then the zones row page by row page, each
row page's in column order. A zone block opens with its group's `group_tag`
(`u64`) before that form, as every page opens with its stamp. A zone set is
refused unless it names distinct columns the
group has, one zone per row page and column, no more nulls than rows, the
empty range for an all-null row page, and a lower bound no greater than the
upper one: each of those, read as written, would prune a row page holding a
match. A zone block holds exactly the column the directory lists it for, and
names the group it describes, so one moved into another column's place or
another group's is refused rather than read as theirs: two groups' zone
blocks of one length would otherwise trade places, still verify as blocks,
and prune each group's row pages by the other's bounds, dropping rows that
match. The verification gates re-derive every zone from the decoded rows
and refuse a group whose zones differ, a zone block it should not have, or
one it lacks, as they do for the zone map.

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

The directory is bound to its group by the **index entry**, which carries the
group's tag. A directory verifies as a block wherever it is read, so another
group's of the same shape, read in this one's place, would otherwise be taken
for this group's: the page stamps catch it as soon as a page is read, but a
point read whose key zones prune every row page reads none, and would answer
from the foreign directory that the key is absent. A reader therefore refuses
a directory whose tag is not the one the index entry names. The tag is a
varint of at most 9 bytes per group in the index, the directory's and head
zone block's lengths beside it one of at most 5 each, and they cost a
row-major table nothing: its entries keep the markers they had, and only an
entry that names a row group takes the tagged one. A group salvage finds by
its frame rather than through the index has no entry to name it and is taken
under its directory's own tag and lengths, its pages' stamps still checked
against it.

A block of another role in a page slot (a zone block, a directory) is refused
by the type its block header names, before its stamp is consulted. Moving a page **between
tables** is refused too, encrypted or not. A row-major block moved into
another table brings its own keys, so a read of it is a miss; a page does not,
so the key page of one table beside the value page of another would serve the
other table's value under this table's key. Every table therefore starts its
tags at its own base, a hash of its path, id and creation time in the lower
half of `u64`: tables' tags collide only by chance, and the stamp names the
table as well as the group. The base is carried, not derived, so a group
copied verbatim into another table keeps the tags its pages were stamped
with. Under encryption the block layer's AAD binds the table id on top.

## Framing overhead

The acceptance this format is held to asks for the overhead as a measured
figure rather than a promise of zero. Per row group, each page adds a block
header (33 bytes: SST blocks carry no flags byte, their transform comes from
the table descriptor), a 13-byte stamp and its length in the directory (2
bytes for a page under 16 KiB), 48 bytes in all; each column part adds 3
bytes to the directory and each row page its row count (1 or 2 bytes); and
the group adds the directory's own header (12 bytes for a few row pages), its
zone count and its block header once. With one row page per group, which has
no zones:

| Row group | 8 pages | 20 pages |
|---|---|---|
| 32 KiB | 456 B (1.39%) | 1068 B (3.26%) |
| 128 KiB | 456 B (0.35%) | 1068 B (0.81%) |
| 256 KiB | 456 B (0.17%) | 1068 B (0.41%) |

Row pages multiply the pages: every column part pays its 48 bytes once per
row page. Four parts in 4 KiB row pages cost 194 bytes per row page, about
4.7% of the data; in 16 KiB row pages, 1.2%. Their zones add 2 bytes per zone
and 2 per bound, plus the bytes each bound does not share with its reference:
some 20 bytes of key zone per row page for sequential 16-byte keys and some 8
per zone of a narrow number. Each column's zones are a block of their own,
which adds a 33-byte block header, its 8-byte group tag and a 3 or 4-byte
directory listing per column and group, and which only a read pruning on that
column fetches. That is what the page size trades against the rows a
point read decodes and the pages a predicate skips, and why its default is
chosen by measurement together with the group size.

Encryption adds its per-page tag and frame on top, roughly doubling those.
Page-ECC does **not** scale with the page count in any meaningful way: its
parity is proportional to the data, so the ~12.5% a SECDED trailer costs is
paid with or without pages, and splitting only adds the rounding at each page
boundary.

These figures are a second reason the row group grows. At 32 KiB with a
richly-encoded schema the framing is already 3.26%; at 128 KiB and above it is
under 1% and stops being a term in the decision.

### Measured against the unpaged layout

A cold scan of every column of one table (20,000 rows, 16-byte keys,
100-byte values, the four engine columns), read once as rows and once as a
projection of all four columns, against the same scan of the layout before
pages, where a row group was one block. Bytes and requests are exact; times
are medians of 21 cold scans on an x86 Linux host with the file in the page
cache, where a request costs a system call and nothing else. They were taken
with the directory that spent 14 bytes a page and whole bounds on its zones,
so the byte figures are an upper bound for the current one.

| Row group, row page | bytes read | requests | projection time |
|---|---|---|---|
| 4 KiB, one row page | +5.3% | +1 (557 vs 556) | 5.08 ms vs 3.07 ms |
| 32 KiB, one row page | +0.68% | +1 (72 vs 71) | 1.57 ms vs 1.16 ms |
| 128 KiB, one row page | +0.17% | +1 (19 vs 18) | 1.09 ms vs 1.04 ms |
| 32 KiB, 4 KiB row pages | +12.3% | +1 | 3.77 ms |
| 128 KiB, 4 KiB row pages | +11.3% | +1 | 3.33 ms |

The bytes are the framing above, and with row pages the zone blocks, which
a read of the whole group takes along. The requests are the unpaged count
plus one: the first group of a scan is read directory first, and every later
one in one request (see [What a read does](#what-a-read-does)). The time
the bytes do not explain is work done per block rather than per byte: each
page is its own block, with its own header, checksum, verification and
allocation, so a group of four columns is five blocks where it was one. It
is a constant per group, which is why it weighs 65% at 4 KiB groups and 4%
at 128 KiB, and it is what the page layout buys independent verification
with. Row scans pay the same framing: 11.7 ms against 9.3 ms at 4 KiB, and
within noise of the unpaged scan from 32 KiB up.

## What the index entry covers

The index entry for a row group spans the **whole group**: directory and
every page, as one extent starting at the directory. That keeps the index's
contract intact — an entry is still one contiguous extent at one offset — and
it is why every section keyed by a data block's file offset (zone map, seqno
bounds, the delete-position lookup) keeps working unchanged: the group starts
where its directory starts, which is where the block it replaces started.

The entry also carries the group's tag, its directory's on-disk length and
its head zone block's, after its seqno, under markers of its own (4 for a
full entry, 5 for a truncated one) beside the row-major entries' 0 and 1. A
tagged entry whose tag is zero, whose directory is empty, or whose directory
and head zone block run past the group, is refused: no group is written with
one.

A reader that wants only part of the group reads exactly the directory first,
the length the entry records, with the head zone block in the same request
when it selects by the key, and then each run of consecutive wanted pages
that are not cached as one range. The directory's own header repeats its
length and the directory the head zone block's, and a directory that
disagrees with the entry on either is refused, as one carrying another
group's tag is: a reader that took the entry's lengths would otherwise cut a
block short or read into the next.

The length costs the entry two bytes for a directory under 16 KiB. Without it
a reader has to guess: an earlier layout read a fixed 4 KiB prefix and took
the directory's length from its header, which cost a second request for a
longer directory and, for a shorter one, read the rest of the prefix whether
the read wanted those bytes or not. On the mixed-layout sparse scan at 64 KiB
groups of 4 KiB row pages the directory averaged 1892 bytes, so more than half
of every prefix was bytes the scan did not use: 6162 bytes read per returned
row with the prefix, 5256 with the recorded length, the rows and every other
byte the same.

## What a read does

**Point read.** Index gives the row group's extent and the lengths of its
directory and of the key zones after it. One read takes both. The key zones
name the row pages whose key pages can hold the key, and only
those key pages are read; a key none of them holds ends the read there.
Otherwise they yield the row pages holding the key's versions, and the
directory the pages of those row pages; one further read per column fetches
them.

**Projection.** As above, minus the slot resolution: the directory names the
pages for the projected columns' parts, and only those are read.

**Predicate scan.** The zone map admits or skips the group; for a group it
admits, the zones of the predicate's column, from that column's zone block,
admit or skip each row page. Only the admitted row pages' pages of the
projected columns are read.

**Full scan.** Every page of the group is wanted, so a group that fits the
I/O buffer is read in one request, directory and pages together, the single
sequential read the group was before pages. A projection is read directory
first, since it does not know which pages it wants until the directory
says; a scan whose projection took every page of one group expects the same
of the next and reads that one whole too, so a projection of every column
pays the directory-first read once per table, not once per group.

### The read budget

How a read fetches the pages it wants is the reader's choice, not the file's:
`Config::columnar_read_budget` sets it per tree, and the same tables read
under any budget return the same rows.

- **The I/O buffer** is what one request may ask for. A run of adjacent wanted
  pages is one request up to it and is cut between pages past it; a single page
  larger than it is a request of its own. A read of every page takes a group
  that fits it in one request.
- **The in-flight count** is how many requests go out together, through the
  filesystem's batched read. A backend with batched I/O keeps them in flight at
  once; one without reads them in turn.

The default is 1 MiB and 16 requests: the I/O buffer Vortex reads a column
with, under which every group up to 1 MiB still reads in one request. A
smaller buffer spends more requests on the same bytes; that is its whole
effect, since the pages of a column lie next to each other and a larger
buffer only covers more of them per request.

The extra dependent step (directory before pages) is real and is the cost of
the indirection. It is bounded by the directory being small and adjacent to
the first page anyone needs, and by the staged read path treating it as one
more stage to batch across tables rather than as a per-table stall.

## Row group size

A columnar table cuts its row groups at `columnar_row_group_size_policy`,
separately from the data block size a row-major table uses, and a group's
rows into row pages at `columnar_page_size_policy`. The defaults are 16 KiB
groups of 4 KiB row pages, four row pages to a group.

`db_bench --benchmark mixed-layout --num 70000 --iterations 3`, every cell on
the same host and build, median of the three, with the default cache and with
none (`--cache-mb 0`), bytes read per emitted row:

| Row group / row page | near-full scan | sparse scan | point reads over a columnar base | near-full scan, no cache | point reads, no cache |
|---|---|---|---|---|---|
| 4 KiB / 4 KiB | 341.4 | 4302 | 216.5 | 344.4 | 6388 |
| 8 KiB / 4 KiB | 342.9 | 4257 | 217.0 | 344.4 | 6497 |
| **16 KiB / 4 KiB** | **340.6** | **4255** | **215.8** | **341.4** | **6548** |
| 32 KiB / 4 KiB | 339.2 | 4359 | 215.2 | 339.6 | 6652 |
| 64 KiB / 4 KiB | 338.6 | 4398 | 215.0 | 338.8 | 6845 |
| 128 KiB / 4 KiB | 337.7 | 4376 | 214.8 | 337.8 | 7253 |
| 32 KiB / 8 KiB | 330.9 | 8037 | 210.4 | 331.2 | 11099 |
| 64 KiB / 8 KiB | 330.2 | 7988 | 210.1 | 330.4 | 11180 |
| 128 KiB / 8 KiB | 329.9 | 7972 | 210.0 | 330.0 | 11389 |

The point reads with no cache are before a point read took its directory once
for both of its steps; since, they read 5820 B per row at 4 KiB / 4 KiB and
5947 at 16 KiB / 4 KiB.

16 KiB / 4 KiB is the only layout that reads fewer bytes than 4 KiB / 4 KiB
on every one of the near-full scan, the sparse scan, point reads and the
near-full scan with no cache. Larger groups of 4 KiB row pages read less on
the first and third but more on the sparse scan, whose group metadata (a
longer directory, and a zone block with its own header and group tag) is
spread over one or two matches a group; 8 KiB row pages read the least on
scans and point reads and nearly twice as much on the sparse scan, where a
match reads a whole 8 KiB page. In time, over two interleaved runs of five
iterations each, the near-full scan at 16 KiB / 4 KiB takes 31.5-32.0 ms where
4 KiB / 4 KiB takes 40.2-42.1 ms, and 24.0-24.6 ms where it takes 33.0-33.9 ms
with no cache; point reads take 211-230 ms against 214-216 ms, the same within
the run-to-run noise.

Two costs of the larger group remain, both of its geometry:

- **A sparse scan's first touch of a group** reads the directory, then the
  zone block of the predicate's column, then the matching pages: one more
  dependent read than a group of one row page, which has no zone block. Over
  a scan whose blocks are all uncached this is about a fifth of its time
  (6.6-7.4 ms against 5.5-5.7 ms here). A zone block is cached like a page,
  so a scan of a group already read does not pay it again.
- **A point read with nothing cached** reads and decodes the group's
  directory and key zones, which grow with the group's row pages: 2.2% more
  bytes than one-page groups, and a third more time (665-672 ms against
  500 ms), most of it decoding the larger directory on every read. A cached
  directory keeps its decoded key zones, so a read of a group already read
  pays neither.

The same trade shapes the defaults elsewhere. Row-oriented engines keep the
unit a point read decodes small: RocksDB's data block is 4 KiB by default
([tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)),
Cassandra lowered its compression chunk from 64 KiB to 16 KiB in 4.0 because
a large chunk makes reading a small record expensive
([production recommendations](https://cassandra.apache.org/doc/latest/cassandra/getting-started/production.html)),
and TiKV uses 16-32 KiB blocks
([configuration](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/)).
Formats written once per dataset for scanning separate a coarse unit from
fine statistics, as row groups and row pages do here, at far larger sizes:
Parquet recommends 512 MB - 1 GB row groups of 8 KB pages
([configurations](https://parquet.apache.org/docs/file-format/configurations/)),
and ORC 64 MB stripes indexed every 10,000 rows
([ORC](https://orc.apache.org/docs/)). An LSM table is written per flush and
serves point reads, so its sizes are chosen by measurement over its own
scenarios rather than adopted. Both policies are per level, so a deployment
whose lower levels sit on slower media can give them larger groups.

Neither the sparse scan nor a point read grows with the group any more:

- **Pruning granularity.** A zone-map entry covers a whole group, so a ~1%
  predicate that prunes most 4 KiB groups admits most 64 KiB ones. The
  statistics zones of an admitted group's row pages then prune it down to
  the row pages that can match, so what the sparse scan reads follows the
  row page size, not the group size.
- **Work per point read.** A read resolves the row pages it selects, finds
  the pages it wants from the directory's grid (a page's index is its
  part's times the row page count plus its row page) and decodes those
  alone, so its work follows the pages it reads, not the pages the group
  has. Before, it walked every directory entry twice, and a point read at
  64 KiB / 4 KiB took 262-271 ms where it now takes 190-195 ms.

What a smaller row page still costs is its own framing: the near-full scan
reads a little more per row, and a point read reads and decodes smaller pages
that miss the cache more often.

### What a sparse scan pays per admitted group

Row pages and their zones took the sparse scan at 64 KiB groups to 6162 bytes
read per returned row, against 4308 at 4 KiB groups: each admitted group cost
more before its first matching row than the whole 4 KiB group it replaces.
The same run (`--num 70000`, a predicate on an 8-byte field matching 722
rows), each group's reads by role, in bytes per returned row:

| Layout | 64 KiB groups, 4 KiB row pages | 4 KiB groups |
|---|---|---|
| **Before:** directory prefix read | 1969, of which the directory 910 | 4096 |
| zone block of the predicate's column | 187 | none |
| pages past the prefix | 4006 | 212 |
| total | 6162 | 4308 |
| **After:** total | 4398 | 4302 |

Four changes removed the difference between the two columns that the scan
does not need: the index entry records the directory's length, so no fixed
prefix is read past it; the directory records each page as its length alone;
the zones write each bound as what it adds to the one before; and the key
column's zones moved out of the directory into a block of their own, which
only a read selecting by the key reads. Every byte left is the directory, the
predicate column's zone block and the pages of the projected columns on the
row pages holding a match; the zones prune every other row page.

A group and a row page are also cut by one count of a row's size, so a
4 KiB group and a 4 KiB row page hold the same rows (14 of these records)
and the two columns compare the same pages.

What remains between them, 96 bytes a returned row, is geometry, not waste:
a 64 KiB group's directory, which lists sixteen row pages, and the zone
block of the predicate's column, a block with its own header and group tag,
spread over the 2.08 matches such a group holds. Both are what the group and
page sizes decide, and those are chosen by measurement over the whole grid
of scenarios above.

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
