//! Cross-feature compatibility matrix.
//!
//! The on-disk format has several orthogonal axes that can combine: block
//! compression, ECC, encryption, KV separation (blob tree), and the columnar
//! PAX track (with optional per-block zone maps). Individual combinations are
//! tested in pockets elsewhere; this is the single authoritative grid proving
//! that **every feasible combination round-trips**.
//!
//! For the cross product of {compression} x {KV-sep} x {columnar} x {zone-map}
//! x {encryption} x {ECC}, each cell writes a known dataset, reopens the tree
//! from disk, and asserts an exact round-trip (point read of every key + a full
//! range scan). A cell whose feature is not compiled into this build is skipped
//! with a logged reason — no silent gaps. The covered/skipped grid is printed,
//! and a coverage assertion fails if a new axis value is added without updating
//! the expected cell count.
//!
//! Run under `--all-features` (or `--features lz4,zstd,columnar,encryption,page_ecc`)
//! to exercise every cell; a narrower feature set covers the in-build subset and
//! skips the rest.

use lsm_tree::{
    AbstractTree, AnyTree, CompressionType, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter, config::CompressionPolicy, get_tmp_folder,
};

const KEYS: u32 = 200;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

fn val(i: u32) -> Vec<u8> {
    format!("v{i:06}-payload-bytes").into_bytes()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Comp {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ecc {
    None,
    Xor,
    ReedSolomon,
}

#[derive(Clone, Copy)]
struct Cell {
    comp: Comp,
    blob: bool,
    columnar: bool,
    zone_map: bool,
    encrypt: bool,
    ecc: Ecc,
}

/// Every cell in the cross product. Keep `EXPECTED_CELLS` in sync.
const EXPECTED_CELLS: usize = 3 * 2 * 2 * 2 * 2 * 3;

fn all_cells() -> Vec<Cell> {
    let mut cells = Vec::with_capacity(EXPECTED_CELLS);
    for comp in [Comp::None, Comp::Lz4, Comp::Zstd] {
        for blob in [false, true] {
            for columnar in [false, true] {
                for zone_map in [false, true] {
                    for encrypt in [false, true] {
                        for ecc in [Ecc::None, Ecc::Xor, Ecc::ReedSolomon] {
                            cells.push(Cell {
                                comp,
                                blob,
                                columnar,
                                zone_map,
                                encrypt,
                                ecc,
                            });
                        }
                    }
                }
            }
        }
    }
    cells
}

impl Cell {
    fn label(self) -> String {
        let comp = match self.comp {
            Comp::None => "none",
            Comp::Lz4 => "lz4",
            Comp::Zstd => "zstd",
        };
        let ecc = match self.ecc {
            Ecc::None => "",
            Ecc::Xor => "+xor",
            Ecc::ReedSolomon => "+rs",
        };
        format!(
            "{comp}{}{}{}{}{ecc}",
            if self.blob { "+blob" } else { "" },
            if self.columnar { "+col" } else { "" },
            if self.zone_map { "+zm" } else { "" },
            if self.encrypt { "+enc" } else { "" },
        )
    }

    /// Why this cell cannot run in the current build, or `None` if runnable.
    fn skip_reason(self) -> Option<&'static str> {
        if self.comp == Comp::Lz4 && !cfg!(feature = "lz4") {
            return Some("lz4 feature off");
        }
        if self.comp == Comp::Zstd && !cfg!(feature = "zstd") {
            return Some("zstd feature off");
        }
        if self.columnar && !cfg!(feature = "columnar") {
            return Some("columnar feature off");
        }
        if self.encrypt && !cfg!(feature = "encryption") {
            return Some("encryption feature off");
        }
        if self.ecc != Ecc::None && !cfg!(feature = "page_ecc") {
            return Some("page_ecc feature off");
        }
        None
    }
}

fn compression_type(comp: Comp) -> CompressionType {
    match comp {
        Comp::None => CompressionType::None,
        #[cfg(feature = "lz4")]
        Comp::Lz4 => CompressionType::Lz4,
        #[cfg(feature = "zstd")]
        Comp::Zstd => CompressionType::Zstd(3),
        // These arms only exist when the feature is off; the cell is skipped
        // before this is reached (see `skip_reason`), so they are unreachable.
        #[cfg(not(feature = "lz4"))]
        Comp::Lz4 => unreachable!("lz4 cell is skipped when the feature is off"),
        #[cfg(not(feature = "zstd"))]
        Comp::Zstd => unreachable!("zstd cell is skipped when the feature is off"),
    }
}

/// Builds the per-cell `Config`. Feature-gated axes only apply when compiled in;
/// a cell needing a disabled feature never reaches here (it is skipped).
fn build_config(dir: &std::path::Path, cell: Cell) -> Config {
    #[allow(unused_mut)]
    let mut cfg = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(compression_type(cell.comp)));

    let mut cfg = if cell.blob {
        // threshold 1 forces every value down the blob path so the KV-sep axis
        // is actually exercised (the values here are small).
        cfg.with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    } else {
        cfg
    };

    #[cfg(feature = "encryption")]
    if cell.encrypt {
        cfg = cfg.with_encryption(Some(std::sync::Arc::new(lsm_tree::Aes256GcmProvider::new(
            &[0x42; 32],
        ))));
    }

    #[cfg(feature = "page_ecc")]
    {
        use lsm_tree::runtime_config::EccScheme;
        cfg = match cell.ecc {
            Ecc::None => cfg,
            Ecc::Xor => cfg
                .page_ecc(true)
                .ecc_scheme(EccScheme::Xor { data_shards: 4 }),
            Ecc::ReedSolomon => cfg.page_ecc(true).ecc_scheme(EccScheme::ReedSolomon {
                data_shards: 4,
                parity_shards: 2,
            }),
        };
    }

    cfg
}

/// Applies the runtime-config axes (columnar / zone-map) before any write.
fn apply_runtime(tree: &AnyTree, cell: Cell) -> lsm_tree::Result<()> {
    let set = |rc: &mut lsm_tree::runtime_config::RuntimeConfig| {
        rc.columnar = cell.columnar;
        rc.zone_map = cell.zone_map;
    };
    match tree {
        AnyTree::Standard(t) => t.update_runtime_config(set)?,
        AnyTree::Blob(t) => t.index.update_runtime_config(set)?,
    };
    Ok(())
}

fn verify(tree: &AnyTree, cell: Cell) {
    for i in 0..KEYS {
        let got = tree
            .get(key(i), SeqNo::MAX)
            .unwrap_or_else(|e| panic!("[{}] get key {i} errored: {e}", cell.label()))
            .unwrap_or_else(|| panic!("[{}] key {i} missing", cell.label()));
        assert_eq!(
            &*got,
            val(i).as_slice(),
            "[{}] value mismatch for key {i}",
            cell.label()
        );
    }
    let scanned = tree.range(key(0)..key(1_000_000), SeqNo::MAX, None).count();
    assert_eq!(
        scanned,
        KEYS as usize,
        "[{}] range scan must see every row",
        cell.label()
    );
}

fn run_cell(cell: Cell) {
    let folder = get_tmp_folder();

    {
        let tree = build_config(folder.path(), cell)
            .open()
            .unwrap_or_else(|e| panic!("[{}] open failed: {e}", cell.label()));
        apply_runtime(&tree, cell)
            .unwrap_or_else(|e| panic!("[{}] runtime config failed: {e}", cell.label()));

        for i in 0..KEYS {
            tree.insert(key(i), val(i), u64::from(i));
        }
        tree.flush_active_memtable(0)
            .unwrap_or_else(|e| panic!("[{}] flush failed: {e}", cell.label()));

        // A columnar cell must actually have produced columnar SSTs, else the
        // axis is vacuous (the reader is transparent to row vs columnar). The
        // columnar SSTs live in the tree itself (Standard) or in a blob tree's
        // index tree, so verify whichever holds them — including blob+columnar.
        #[cfg(feature = "columnar")]
        {
            let columnar_version = match &tree {
                AnyTree::Standard(t) if cell.columnar => Some(t.current_version()),
                AnyTree::Blob(t) if cell.columnar => Some(t.index.current_version()),
                _ => None,
            };
            if let Some(version) = columnar_version {
                assert!(
                    version.iter_tables().all(|x| x.metadata.columnar),
                    "[{}] expected columnar SSTs, got row-major",
                    cell.label()
                );
            }
        }

        verify(&tree, cell);
    }

    // Reopen from disk with the same config and re-verify (the real round-trip).
    let tree = build_config(folder.path(), cell)
        .open()
        .unwrap_or_else(|e| panic!("[{}] reopen failed: {e}", cell.label()));
    verify(&tree, cell);
}

#[test]
fn compatibility_matrix_round_trips_every_feasible_cell() {
    let cells = all_cells();
    assert_eq!(
        cells.len(),
        EXPECTED_CELLS,
        "matrix cell count drifted from EXPECTED_CELLS — a new axis value was \
         added without updating the matrix"
    );

    let mut covered = Vec::new();
    let mut skipped = Vec::new();
    for cell in cells {
        match cell.skip_reason() {
            Some(reason) => skipped.push((cell.label(), reason)),
            None => {
                run_cell(cell);
                covered.push(cell.label());
            }
        }
    }

    eprintln!(
        "=== compat matrix: {} covered / {} skipped / {} total ===",
        covered.len(),
        skipped.len(),
        EXPECTED_CELLS
    );
    for (label, reason) in &skipped {
        eprintln!("  SKIP {label}: {reason}");
    }

    // "No silent gaps" is guaranteed structurally: the `cells.len()` assertion
    // above pins the total, and `skip_reason` returns a `&'static str` for every
    // skip (the type, not a runtime count, is the contract), so each cell is
    // either covered or skipped-with-reason.
    assert!(
        !covered.is_empty(),
        "at least the baseline (no-feature) cells must run in every build"
    );
}
