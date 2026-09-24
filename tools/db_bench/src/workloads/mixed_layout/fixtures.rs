//! The eight record shapes the mixed-layout work is measured over, each with
//! the expectation a correct read must satisfy.
//!
//! # The oracle is the write history, not a second read
//!
//! Every fixture returns an [`Oracle`] built AS IT WRITES, from the operations
//! it performed: which key got which value last, and which keys a later delete
//! or range tombstone covered. It is never derived by reading the tree back.
//!
//! That distinction is the whole point. Comparing a new read path against the
//! ordinary one only proves the two AGREE — and two paths sharing one faulty
//! version-resolution routine agree with each other while both are wrong. The
//! write history is independent of every read path, so it can disagree with
//! all of them at once.
//!
//! # Why values are generated rather than stored
//!
//! An oracle holding a copy of every value would hold as much memory as the
//! tree it describes (the wide-record fixture alone writes ~200 MB). Values
//! are a pure function of a `seed` and a length, so a row records those two
//! numbers and [`value_bytes`] reproduces the value on demand.

use crate::config::BenchConfig;
use lsm_tree::runtime_config::RuntimeConfig;
use lsm_tree::table::columnar::{
    COL_VALUE, Column, TypeTag, entries_to_column_batch, frame_value_cells,
};
use lsm_tree::{AbstractTree, AnyTree, InternalValue, ValueType};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;

/// Offset of the small fields a projection would read, and where the payload
/// starts. The shape is the point: a few narrow fields a predicate can filter
/// on, then a large payload the filter does not need.
pub const HEADER_LEN: usize = 32;

/// A value's identity, from which its bytes are reproduced.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Value {
    pub seed: u64,
    pub len: usize,
}

impl Value {
    pub fn bytes(self) -> Vec<u8> {
        value_bytes(self.seed, self.len)
    }
}

/// The bytes a `seed` stands for, at `len` bytes.
///
/// The first [`HEADER_LEN`] bytes are the small fields: the seed itself, then
/// the two derived fields the selectivity scenarios filter on. The rest is
/// payload, filled with a seed-dependent byte so it compresses like real data
/// rather than like a run of zeroes.
pub fn value_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len.max(HEADER_LEN));
    v.extend_from_slice(&seed.to_be_bytes());
    v.extend_from_slice(&group_of(seed).to_be_bytes());
    v.extend_from_slice(&bucket_of(seed).to_be_bytes());
    v.extend_from_slice(&(seed ^ 0x5bd1_e995).to_be_bytes());
    debug_assert_eq!(v.len(), HEADER_LEN);
    // The remainder by 251 is a byte by construction, so the conversion is
    // total and needs no fallback.
    let fill = u8::try_from(seed % 251).unwrap_or(0);
    v.resize(len.max(HEADER_LEN), fill);
    v
}

/// The sparse field: one value in 97, so a predicate on it selects ~1%.
pub fn group_of(seed: u64) -> u64 {
    seed % 97
}

/// The near-full field: nine values in ten, so a predicate on it selects ~90%.
pub fn bucket_of(seed: u64) -> u64 {
    seed % 10
}

/// Reads the sparse field back out of a value's header.
///
/// The scenarios filter on what they READ, not on the key they asked for: a
/// predicate evaluated against the oracle would test the oracle, and a
/// projection that returned the wrong row's header would pass.
pub fn field_group(value: &[u8]) -> Option<u64> {
    value.get(8..16)?.try_into().ok().map(u64::from_be_bytes)
}

/// Reads the near-full field back out of a value's header.
pub fn field_bucket(value: &[u8]) -> Option<u64> {
    value.get(16..24)?.try_into().ok().map(u64::from_be_bytes)
}

/// What a correct read must return for one key.
#[derive(Clone, Debug)]
pub struct Row {
    pub key: Vec<u8>,
    /// `None` where the last operation covering this key was a delete or a
    /// range tombstone.
    pub expect: Option<Value>,
    /// Whether the scenario's predicate selects this row. Rows the predicate
    /// rejects, and rows that are not visible at all, are both `false`.
    pub selected: bool,
}

/// The expected content of a fixture, in key order.
#[derive(Clone, Debug, Default)]
pub struct Oracle {
    pub rows: Vec<Row>,
}

impl Oracle {
    pub fn visible(&self) -> u64 {
        self.rows.iter().filter(|r| r.expect.is_some()).count() as u64
    }

    pub fn selected(&self) -> u64 {
        self.rows.iter().filter(|r| r.selected).count() as u64
    }
}

/// Builds a scenario's tree beneath the given directory, and the expectation
/// that goes with it.
pub type FixtureFn = fn(&BenchConfig, &AtomicU64, &Path) -> lsm_tree::Result<Fixture>;

/// A fresh directory for one fixture's tree beneath `base`, removed with the
/// fixture.
fn fixture_dir(base: &Path) -> std::io::Result<TempDir> {
    tempfile::Builder::new()
        .prefix("mixed-layout-")
        .tempdir_in(base)
}

/// A built tree together with what it must contain.
///
/// The temp directory is held for the fixture's lifetime: dropping it would
/// remove the files under the open tree, and every read the scenario performs
/// is about those files.
pub struct Fixture {
    pub tree: AnyTree,
    pub oracle: Oracle,
    pub shape: Shape,
    _dir: TempDir,
}

/// How a fixture stores its values, which decides what a row read returns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    /// One opaque value per key: a row read returns [`Value::bytes`].
    Opaque,
    /// The value in [`COL_ROW`], with its two filter fields split out into
    /// [`COL_GROUP`] and [`COL_BUCKET`] so a columnar scan can filter on them.
    /// A row read returns the three cells framed together.
    Split,
}

/// The sub-column holding a split value's whole row.
pub const COL_ROW: u16 = COL_VALUE;
/// The sub-column holding the sparse field, as its 8 big-endian header bytes:
/// big-endian so the bytewise order a predicate compares in is numeric order.
pub const COL_GROUP: u16 = COL_VALUE + 1;
/// The sub-column holding the near-full field, encoded like [`COL_GROUP`].
pub const COL_BUCKET: u16 = COL_VALUE + 2;

impl Fixture {
    /// What a row read of a key holding `value` returns.
    pub fn read_bytes(&self, value: Value) -> lsm_tree::Result<Vec<u8>> {
        let row = value.bytes();
        match self.shape {
            Shape::Opaque => Ok(row),
            Shape::Split => {
                let (group, bucket) = header_fields(&row);
                frame_value_cells(&[
                    (TypeTag::Bytes, &row),
                    (TypeTag::Bytes, &group),
                    (TypeTag::Bytes, &bucket),
                ])
            }
        }
    }
}

/// The two filter fields of a value, as the bytes their sub-columns hold.
fn header_fields(row: &[u8]) -> ([u8; 8], [u8; 8]) {
    let group = field_group(row).expect("every value carries a full header");
    let bucket = field_bucket(row).expect("every value carries a full header");
    (group.to_be_bytes(), bucket.to_be_bytes())
}

/// Builds a bytes column: a `rows + 1` little-endian `u32` offset table, then
/// the cells back to back, which is the framing a columnar block stores.
fn bytes_column<'a>(column_id: u16, cells: impl Iterator<Item = &'a [u8]>) -> Column {
    let cells: Vec<&[u8]> = cells.collect();
    let payload: usize = cells.iter().map(|c| c.len()).sum();
    let mut data = Vec::with_capacity((cells.len() + 1) * 4 + payload);
    let mut end = 0_u32;
    data.extend_from_slice(&end.to_le_bytes());
    for cell in &cells {
        end += u32::try_from(cell.len()).expect("a fixture cell is far below 4 GiB");
        data.extend_from_slice(&end.to_le_bytes());
    }
    for cell in &cells {
        data.extend_from_slice(cell);
    }
    Column {
        column_id,
        type_tag: TypeTag::Bytes,
        validity: None,
        data: data.into(),
    }
}

/// Reads row `row` of a bytes column built with the framing [`bytes_column`]
/// writes, or `None` if the column is shorter than its offsets claim.
pub fn bytes_cell(column: &Column, rows: u32, row: u32) -> Option<&[u8]> {
    let offset = |i: u32| -> Option<usize> {
        let at = usize::try_from(i).ok()? * 4;
        let raw: [u8; 4] = column.data.get(at..at + 4)?.try_into().ok()?;
        usize::try_from(u32::from_le_bytes(raw)).ok()
    };
    let table = (usize::try_from(rows).ok()? + 1) * 4;
    column
        .data
        .get(table + offset(row)?..table + offset(row + 1)?)
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:012}").into_bytes()
}

/// How a fixture's tree is opened. Only the knobs the shapes actually differ
/// in: everything else, the cache included, follows the benchmark
/// configuration, so a cold-read run (`--cache-mb 0`) is cold here too.
#[derive(Clone, Copy, Default)]
struct Opening {
    columnar: bool,
    kv_separation: bool,
    /// Per-block column min / max, which lets a predicate skip a block
    /// without loading it.
    zone_map: bool,
}

fn open(dir: &TempDir, config: &BenchConfig, opening: Opening) -> lsm_tree::Result<AnyTree> {
    let mut rc = RuntimeConfig::default();
    rc.columnar = opening.columnar;
    rc.zone_map = opening.zone_map;

    let mut builder = crate::config::tree_builder(dir.path(), config)?.with_runtime_config(rc);

    if opening.kv_separation {
        builder = builder.with_kv_separation(Some(Default::default()));
    }

    builder.open()
}

/// Switch the tree's layout mid-fixture, so later writes land in a different
/// representation than the ones already flushed.
///
/// Only the standard tree can: the blob tree's own layout is the separation,
/// and no fixture asks it to change.
fn set_columnar(tree: &AnyTree, columnar: bool) -> lsm_tree::Result<()> {
    match tree {
        AnyTree::Standard(t) => t.update_runtime_config(|rc| rc.columnar = columnar),
        AnyTree::Blob(_) => Ok(()),
    }
}

/// Narrow records: a handful of small fields and nothing else.
///
/// The control every wide-record figure is read against. Whatever a projection
/// costs per row, it cannot legitimately cost more than reading a narrow row
/// whole.
pub fn narrow(config: &BenchConfig, seqno: &AtomicU64, base: &Path) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(&dir, config, Opening::default())?;
    let n = config.num.min(200_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let value = Value {
            seed: i,
            len: HEADER_LEN,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: true,
        });
    }
    tree.flush_active_memtable(0)?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

/// Wide records: the small filterable fields plus a 4 KiB payload.
///
/// Serves both the full read (the baseline a projection is compared against)
/// and the projection scenario, which reads the same tree and must return the
/// header fields without paying for the payload.
pub fn wide(config: &BenchConfig, seqno: &AtomicU64, base: &Path) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(&dir, config, Opening::default())?;
    let n = config.num.min(50_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let value = Value {
            seed: i,
            len: HEADER_LEN + 4_096,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: true,
        });
    }
    tree.flush_active_memtable(0)?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

/// Short and long values in one key space.
///
/// This is what makes a single block geometry a compromise: the block size
/// that fits many short rows holds a fraction of a long one, so whichever way
/// it is set, one of the two shapes pays.
pub fn mixed_sizes(
    config: &BenchConfig,
    seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(&dir, config, Opening::default())?;
    let n = config.num.min(100_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let len = if i.is_multiple_of(10) {
            HEADER_LEN + 8_192
        } else {
            HEADER_LEN
        };
        let value = Value { seed: i, len };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: true,
        });
    }
    tree.flush_active_memtable(0)?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

/// Fresh row-format updates layered over a columnar base.
///
/// The base is flushed with the columnar layout on, then the layout is
/// switched off and a third of the keys are rewritten, so the newest version
/// of those keys lives in a row-major run above a columnar one. A read has to
/// resolve across both representations, which is the shape a projected scan
/// has to handle and today refuses.
pub fn columnar_base_row_updates(
    config: &BenchConfig,
    seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(
        &dir,
        config,
        Opening {
            columnar: true,
            ..Opening::default()
        },
    )?;
    let n = config.num.min(100_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let value = Value {
            seed: i,
            len: HEADER_LEN + 96,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: true,
        });
    }
    tree.flush_active_memtable(0)?;

    // Row-major from here on, so the updates land in a differently-shaped run.
    set_columnar(&tree, false)?;
    for i in (0..n).step_by(3) {
        let value = Value {
            seed: i + 1_000_000,
            len: HEADER_LEN + 96,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows[i as usize].expect = Some(value);
    }
    tree.flush_active_memtable(0)?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

/// Historical versions, point deletes and a range tombstone.
///
/// Read at the newest sequence number, so every version has to be resolved
/// rather than skipped. The oracle applies the same three operations in the
/// same order to its own rows, which is why it can catch a build that stopped
/// resolving versions and started returning whichever one it found first.
pub fn versions_deletes_tombstones(
    config: &BenchConfig,
    seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(&dir, config, Opening::default())?;
    let n = config.num.min(100_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let value = Value {
            seed: i,
            len: HEADER_LEN + 32,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: true,
        });
    }
    tree.flush_active_memtable(0)?;

    // Second version for every third key.
    for i in (0..n).step_by(3) {
        let value = Value {
            seed: i + 2_000_000,
            len: HEADER_LEN + 32,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows[i as usize].expect = Some(value);
    }
    // Point deletes for every fifth key, some of which have two versions.
    for i in (0..n).step_by(5) {
        tree.remove(key(i), seqno.fetch_add(1, Ordering::Relaxed));
        rows[i as usize].expect = None;
    }
    // One range tombstone over a contiguous slice in the middle, which covers
    // keys in all three states above.
    let (lo, hi) = (n / 2, n / 2 + n / 20);
    if lo < hi {
        tree.remove_range(key(lo), key(hi), seqno.fetch_add(1, Ordering::Relaxed));
        for row in &mut rows[lo as usize..hi as usize] {
            row.expect = None;
        }
    }
    tree.flush_active_memtable(0)?;

    for row in &mut rows {
        row.selected = row.expect.is_some();
    }

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

/// One key space read twice under predicates of very different selectivity.
///
/// `selected` here marks the sparse predicate (~1%); the near-full scenario
/// derives its own set from [`bucket_of`] over the same oracle. The right
/// point to materialize a row differs between the two, so a change that helps
/// one can hurt the other and both have to be visible.
///
/// Stored columnar, with the two filter fields split out of the header into
/// sub-columns of their own ([`Shape::Split`]), because that is the only shape
/// in which the engine evaluates a predicate itself: the scenarios hand it the
/// predicate, and a filter the harness ran over returned rows would make both
/// cost exactly the same engine work. Zone maps are on, so a block holding no
/// matching row can be skipped unread.
pub fn selectivity(
    config: &BenchConfig,
    _seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    /// Rows per ingested batch. The ingestion accumulates batches and cuts a
    /// block once the pending rows reach the block size target, so a batch is
    /// the granularity of that cut: one row per call lets the target decide
    /// block geometry exactly as it does for a row-major flush. A large batch
    /// would land as one block of its own size, holding a matching row for
    /// any predicate and leaving the zone map nothing to skip.
    const BATCH: u64 = 1;

    let dir = fixture_dir(base)?;
    let tree = open(
        &dir,
        config,
        Opening {
            columnar: true,
            zone_map: true,
            ..Opening::default()
        },
    )?;
    let n = config.num.min(100_000);

    let mut rows = Vec::with_capacity(n as usize);
    let mut ingestion = tree.ingestion()?;
    for start in (0..n).step_by(BATCH as usize) {
        let end = (start + BATCH).min(n);
        let values: Vec<Vec<u8>> = (start..end)
            .map(|i| value_bytes(i, HEADER_LEN + 224))
            .collect();
        // Transposed at local seqno 0: an ingested run is ordered by the one
        // sequence number the ingestion assigns it, not by per-row ones.
        let entries: Vec<InternalValue> = (start..end)
            .zip(&values)
            .map(|(i, v)| InternalValue::from_components(key(i), v.as_slice(), 0, ValueType::Value))
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        let fields: Vec<([u8; 8], [u8; 8])> = values.iter().map(|v| header_fields(v)).collect();
        batch
            .columns
            .push(bytes_column(COL_GROUP, fields.iter().map(|(g, _)| &g[..])));
        batch
            .columns
            .push(bytes_column(COL_BUCKET, fields.iter().map(|(_, b)| &b[..])));
        ingestion.write_columnar_batch(&batch)?;

        for i in start..end {
            rows.push(Row {
                key: key(i),
                expect: Some(Value {
                    seed: i,
                    len: HEADER_LEN + 224,
                }),
                selected: group_of(i) == 0,
            });
        }
    }
    ingestion.finish()?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Split,
        _dir: dir,
    })
}

/// Blobs written in key order, in one pass.
///
/// Values are far above the separation threshold, so each lives in a blob
/// file, and a sequential write leaves consecutive keys' blobs adjacent. The
/// favourable case, against which the scattered profile below is read.
pub fn blobs_well_placed(
    config: &BenchConfig,
    seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(
        &dir,
        config,
        Opening {
            kv_separation: true,
            ..Opening::default()
        },
    )?;
    let n = config.num.min(10_000);

    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        let value = Value {
            seed: i,
            len: HEADER_LEN + 8_192,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows.push(Row {
            key: key(i),
            expect: Some(value),
            selected: group_of(i) == 0,
        });
    }
    tree.flush_active_memtable(0)?;

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// The same blobs, deliberately scattered.
///
/// Written in a strided order and then rewritten in several rounds with a
/// flush between them, so a key's live blob sits in whichever file its last
/// round landed in and neighbouring keys' blobs are far apart. This is the
/// profile where a scan that fetches a blob before deciding it wants the row
/// pays the most, which is what makes it the fixture the late-materialization
/// scenario waits for.
pub fn blobs_scattered(
    config: &BenchConfig,
    seqno: &AtomicU64,
    base: &Path,
) -> lsm_tree::Result<Fixture> {
    let dir = fixture_dir(base)?;
    let tree = open(
        &dir,
        config,
        Opening {
            kv_separation: true,
            ..Opening::default()
        },
    )?;
    let n = config.num.min(10_000);

    let mut rows: Vec<Row> = (0..n)
        .map(|i| Row {
            key: key(i),
            expect: None,
            selected: group_of(i) == 0,
        })
        .collect();

    // A stride coprime with n visits the key space out of order without
    // repeating, so the first pass already writes neighbours far apart in time.
    // No fixed stride is coprime with every n (a stride divides its own
    // multiples), so the first one at or above the preferred value is taken.
    // With no keys there is nothing to visit, and nothing is coprime with 0
    // (gcd(s, 0) == s), so the search is skipped rather than run forever.
    let stride = if n == 0 {
        1
    } else {
        (7_919..)
            .find(|&s| gcd(s, n) == 1)
            .expect("a prime above both 7919 and n is coprime with n")
    };
    for step in 0..n {
        let i = step.wrapping_mul(stride) % n;
        let value = Value {
            seed: i,
            len: HEADER_LEN + 8_192,
        };
        tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
        rows[i as usize].expect = Some(value);
    }
    tree.flush_active_memtable(0)?;

    // Three rewrite rounds, each touching a different residue class and each
    // flushed on its own, so the live blobs end up spread across four files.
    for round in 1..=3_u64 {
        for i in (round..n).step_by(4) {
            let value = Value {
                seed: i + round * 1_000_000,
                len: HEADER_LEN + 8_192,
            };
            tree.insert(key(i), value.bytes(), seqno.fetch_add(1, Ordering::Relaxed));
            rows[i as usize].expect = Some(value);
        }
        tree.flush_active_memtable(0)?;
    }

    Ok(Fixture {
        tree,
        oracle: Oracle { rows },
        shape: Shape::Opaque,
        _dir: dir,
    })
}
