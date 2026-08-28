//! Reproducible single-byte-bitrot fuzzer for the SST read / heal path.
//!
//! Builds a deterministic corpus of SSTs across option variations (block size,
//! per-KV checksums, columnar layout, compression, encryption, Page-ECC), then —
//! from a FIXED seed — repeatedly picks a corpus SST, flips one random bit, and
//! reads every entry back through [`Table::recover`] + a full scan. Invariant on
//! every mutation: the read path NEVER panics and NEVER returns a wrong value —
//! a flipped block either heals (Page-ECC corrects the single-symbol error) or
//! fails its block checksum and is surfaced as an error, but a corrupt block can
//! never silently yield altered data.
//!
//! `#[ignore]`d so it stays out of the normal suite (which has a 30s per-test
//! slow-timeout); a dedicated CI step runs it with `--run-ignored=only`. Bounded
//! to ~45s of wall-clock; on failure it prints the seed + iteration + corpus
//! label + byte offset + bit so the exact case reproduces.

#![expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "fuzz test")]
// `allow`, not `expect`: whether a truncating cast fires depends on the target
// pointer width (a `u64 as usize` truncates on 32-bit but not 64-bit), so an
// `expect` would go unfulfilled on some entries of the cross-compile matrix.
#![allow(
    clippy::cast_possible_truncation,
    reason = "fuzz test: intentional narrowing of RNG output"
)]

use crate::table::{Table, Writer};
use crate::{InternalValue, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Fixed seed: the whole fuzz sequence (corpus pick, byte offset, bit) is
/// deterministic, so a failure reproduces exactly from the printed iteration.
const SEED: u64 = 0x5eed_1234_abcd_ef01;
/// Wall-clock budget. Kept under a minute for CI; the fixed seed makes the
/// covered cases deterministic regardless of how many iterations fit.
const BUDGET: Duration = Duration::from_secs(45);
/// Entries per corpus SST.
const KEYS: u32 = 400;

/// `SplitMix64` — a tiny deterministic PRNG (no external `rand` crate, so the
/// sequence is stable across toolchains).
struct SplitMix64(u64);
impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}
fn val(i: u32) -> Vec<u8> {
    // A payload long enough to span the small-block boundary and vary per key.
    format!(
        "value-{i:06}-{:016x}",
        u64::from(i).wrapping_mul(0x9e37_79b9)
    )
    .into_bytes()
}

struct CorpusEntry {
    label: String,
    bytes: Vec<u8>,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    /// Byte range of the TAIL meta block, and of its MID mirror. A flip inside
    /// exactly ONE of them must still recover COMPLETELY — that is what the
    /// mirror is for — so those iterations hold a stricter invariant than the
    /// rest (see `meta_mirror_span`).
    meta_tail: core::ops::Range<usize>,
    meta_mid: Option<core::ops::Range<usize>>,
}

impl CorpusEntry {
    /// Whether `off` lands in exactly one meta mirror, with the other intact.
    fn hits_one_meta_mirror(&self, off: usize) -> bool {
        let Some(mid) = self.meta_mid.clone() else {
            return false;
        };
        let in_tail = self.meta_tail.contains(&off);
        let in_mid = mid.contains(&off);
        in_tail ^ in_mid
    }
}

/// One writer configuration: a label plus the option closure applied to a fresh
/// [`Writer`], and the provider the reader must supply to recover it.
struct Variant {
    label: &'static str,
    configure: fn(Writer) -> Writer,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
}

fn variants() -> Vec<Variant> {
    let mut v: Vec<Variant> = Vec::new();
    for &bs in &[128u32, 4096] {
        v.push(Variant {
            label: if bs == 128 { "small" } else { "big" },
            configure: if bs == 128 {
                |w| w.use_data_block_size(128)
            } else {
                |w| w.use_data_block_size(4096)
            },
            encryption: None,
        });
    }
    // Per-KV checksum footers.
    v.push(Variant {
        label: "kvcheck",
        configure: |w| {
            w.use_data_block_size(256).use_kv_checksums(
                crate::runtime_config::KvChecksumPolicy::AllLevels,
                crate::runtime_config::ChecksumAlgorithm::Xxh3_64,
            )
        },
        encryption: None,
    });
    #[cfg(feature = "lz4")]
    v.push(Variant {
        label: "lz4",
        configure: |w| {
            w.use_data_block_size(256)
                .use_data_block_compression(crate::CompressionType::Lz4)
        },
        encryption: None,
    });
    #[cfg(feature = "columnar")]
    v.push(Variant {
        label: "columnar",
        configure: |w| w.use_data_block_size(256).use_columnar(true),
        encryption: None,
    });
    #[cfg(feature = "page_ecc")]
    {
        v.push(Variant {
            label: "ecc-xor",
            configure: |w| {
                w.use_data_block_size(256).use_page_ecc(
                    true,
                    crate::runtime_config::EccScheme::Xor { data_shards: 4 },
                )
            },
            encryption: None,
        });
        v.push(Variant {
            label: "ecc-rs",
            configure: |w| {
                w.use_data_block_size(256).use_page_ecc(
                    true,
                    crate::runtime_config::EccScheme::ReedSolomon {
                        data_shards: 4,
                        parity_shards: 2,
                    },
                )
            },
            encryption: None,
        });
    }
    #[cfg(feature = "encryption")]
    v.push(Variant {
        label: "encrypted",
        configure: |w| w.use_data_block_size(256),
        encryption: Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
            &[5u8; 32],
        ))),
    });
    v
}

fn build_corpus(dir: &std::path::Path, fs: &Arc<dyn crate::fs::Fs>) -> Vec<CorpusEntry> {
    let mut out = Vec::new();
    for (n, variant) in variants().into_iter().enumerate() {
        let sst = dir.join(format!("corpus-{n}"));
        let base = Writer::new(sst.clone(), 0, 0, Arc::clone(fs))
            .unwrap()
            .use_encryption(variant.encryption.clone());
        let mut w = (variant.configure)(base);
        for i in 0..KEYS {
            w.write(InternalValue::from_components(
                key(i),
                val(i),
                u64::from(i) + 1,
                ValueType::Value,
            ))
            .unwrap();
        }
        assert!(w.finish().unwrap().is_some(), "corpus SST is non-empty");
        let bytes = std::fs::read(&sst).unwrap();
        let (meta_tail, meta_mid) = meta_mirror_spans(&sst);
        out.push(CorpusEntry {
            label: variant.label.to_string(),
            bytes,
            encryption: variant.encryption,
            meta_tail,
            meta_mid,
        });
    }
    out
}

/// Reads the byte spans of the TAIL meta block and its MID mirror from a
/// freshly-written (undamaged) corpus SST.
fn meta_mirror_spans(
    path: &std::path::Path,
) -> (core::ops::Range<usize>, Option<core::ops::Range<usize>>) {
    let mut file = std::fs::File::open(path).unwrap();
    let reader = crate::sfa::Reader::from_reader(&mut file).unwrap();
    let span = |name: &[u8]| {
        reader.toc().section(name).map(|e| {
            let pos = e.pos() as usize;
            pos..pos + e.len() as usize
        })
    };
    (span(b"meta").unwrap(), span(b"meta_mid"))
}

/// Recovers the (possibly bit-flipped) SST at `path`, passing its FRESHLY
/// recomputed whole-file checksum so recovery does not reject it on the
/// whole-file digest — the per-block checksums / ECC are what this fuzzer
/// exercises.
fn recover(
    path: &std::path::Path,
    fs: &Arc<dyn crate::fs::Fs>,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
) -> crate::Result<Table> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, path)?);
    let mut params = crate::table::RecoverParams::new(
        path.to_path_buf(),
        checksum,
        0,
        Arc::clone(fs),
        crate::comparator::default_comparator(),
        Arc::new(crate::Cache::with_capacity_bytes(1 << 20)),
    );
    params.encryption = encryption;
    Table::recover(params)
}

/// Recovers and fully scans the SST, checking every yielded entry against the
/// expected map. Returns `Err` on any read failure (detected corruption — an
/// acceptable outcome); PANICS only on the fuzzer invariant violation (a wrong
/// value), which the caller surfaces with the reproducing case.
fn recover_and_scan(
    path: &std::path::Path,
    fs: &Arc<dyn crate::fs::Fs>,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    ctx: &str,
) -> crate::Result<()> {
    use crate::table::block_index::BlockIndex as _;
    let table = recover(path, fs, encryption)?;
    // Full scan: a corrupt block surfaces as `Err`, never a wrong value.
    let mut seen = 0u32;
    for item in table.range_iter(..) {
        let iv = item?;
        let k = iv.key.user_key.as_ref();
        // Every yielded key belongs to a block that passed its checksum (or was
        // ECC-healed), so it must be an original key with its original value.
        let expected = k
            .strip_prefix(b"k")
            .and_then(|d| std::str::from_utf8(d).ok())
            .and_then(|d| d.parse::<u32>().ok())
            .map(val);
        assert_eq!(
            expected.as_deref(),
            Some(iv.value.as_ref()),
            "{ctx}: scan yielded a WRONG value for key {k:?} (silent corruption)",
        );
        seen += 1;
    }
    // Touch the block index too, so a corrupt index surfaces (as Err) rather than
    // being skipped by an early scan short-circuit.
    for handle in table.block_index.iter() {
        handle?;
    }
    // Silent OMISSION is its own corruption class: a bit flip that truncates the
    // scan or skips a block while every yielded value stays correct, with no error
    // surfaced, would slip past the per-value check above. A clean scan means every
    // block was checksum-clean or ECC-healed, so the FULL key set must be present.
    assert_eq!(
        seen, KEYS,
        "{ctx}: the scan completed cleanly but yielded {seen}/{KEYS} keys (silent omission)",
    );
    Ok(())
}

/// How many fuzz iterations actually reached a SUCCESSFUL salvage, and how
/// many of those dropped a block. Reported at the end so the minimality check
/// cannot pass vacuously: a run where salvage always refused would satisfy the
/// assertion while proving nothing about how much it loses.
static SALVAGE_OK: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);
static SALVAGE_DROPPED: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);
/// Iterations whose flip landed in exactly one meta mirror — the cases that
/// must recover COMPLETELY from the other copy.
static METAFLIP_SEEN: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);

/// Block-salvages the flipped SST and checks that the loss is MINIMAL.
///
/// The read path's contract is only "never serve altered data"; a table that
/// refuses everything satisfies it. Salvage carries the opposite obligation —
/// keep every block the damage did not touch — and that is what this checks:
///
/// - every entry the salvaged copy DOES hold is the original one (a salvage
///   that re-emits altered payloads is silent corruption with extra steps);
/// - a single flipped bit costs AT MOST ONE block. The flip lands inside one
///   block, so at most that block can fail its checksum; dropping a second
///   means the walk lost a boundary it could have re-derived and surrendered
///   healthy data with the damaged block.
///
/// A flip outside the data section (trailer, index, meta) is not covered by
/// the one-block bound — it can legitimately cost the enumeration itself — so
/// those iterations only check the no-altered-data half. `Err` is always an
/// acceptable outcome: refusing is never a loss the caller cannot see.
#[cfg(feature = "std")]
fn salvage_and_check_minimality(
    source: &std::path::Path,
    dest: &std::path::Path,
    fs: &Arc<dyn crate::fs::Fs>,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    ctx: &str,
) -> crate::Result<()> {
    let options = crate::salvage::SalvageOptions {
        encryption,
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
        expected_stored_id: None,
        output_id: None,
        allow_delete_resurrection: false,
        sync_mode: crate::fs::SyncMode::Normal,
        prefix_extractor: None,
        blob_rewrite: None,
        progress: None,
    };
    let report = crate::salvage::salvage_with_context(
        source,
        dest.to_path_buf(),
        fs,
        &crate::comparator::default_comparator(),
        &options,
    )?;
    SALVAGE_OK.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    if !report.dropped.is_empty() {
        SALVAGE_DROPPED.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }
    assert!(
        report.dropped.len() <= 1,
        "{ctx}: a single flipped bit cost {} blocks — the walk surrendered \
         healthy blocks along with the damaged one: {report:?}",
        report.dropped.len(),
    );
    let Some(path) = report.salvaged_path else {
        return Ok(());
    };
    // Every surviving entry must still be its original value.
    let table = recover(&path, fs, options.encryption.clone())?;
    for item in table.range_iter(..) {
        let iv = item?;
        let k = iv.key.user_key.as_ref();
        let expected = k
            .strip_prefix(b"k")
            .and_then(|d| std::str::from_utf8(d).ok())
            .and_then(|d| d.parse::<u32>().ok())
            .map(val);
        assert_eq!(
            expected.as_deref(),
            Some(iv.value.as_ref()),
            "{ctx}: the SALVAGED copy holds a wrong value for key {k:?}",
        );
    }
    Ok(())
}

/// Persists the exact bytes that failed (plus a `.txt` describing the case) to a
/// stable path, and returns it. This is the ground-truth reproducer for a
/// non-byte-deterministic corpus (encrypted / timestamped SSTs): re-running the
/// read path over this file re-hits the failure directly, no seed replay needed.
fn repro_dump(bytes: &[u8], ctx: &str, entry: &CorpusEntry) -> std::path::PathBuf {
    // Resolve the dump directory EXPLICITLY, not from the process working
    // directory (which need not be the crate root, e.g. once the crate lives in a
    // workspace subdirectory): `FUZZ_HEAL_REPRO_DIR` overrides at runtime,
    // otherwise the compile-time `CARGO_MANIFEST_DIR` (the crate root), otherwise
    // the current directory as a last resort. The CI dump step reads the same
    // resolved location, and the returned ABSOLUTE path goes into the panic
    // message so the artifact is findable.
    let base = std::env::var_os("FUZZ_HEAL_REPRO_DIR")
        .map(std::path::PathBuf::from)
        .or_else(|| option_env!("CARGO_MANIFEST_DIR").map(std::path::PathBuf::from))
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let sst = base.join("fuzz_heal_repro.sst");
    let _ = std::fs::write(&sst, bytes);
    let _ = std::fs::write(
        base.join("fuzz_heal_repro.txt"),
        format!(
            "{ctx}\nvariant={}\nencryption={}\nsst_len={}\n",
            entry.label,
            if entry.encryption.is_some() {
                "Aes256Gcm key=[5u8;32]"
            } else {
                "none"
            },
            bytes.len(),
        ),
    );
    sst
}

#[test]
#[ignore = "long-running bitrot fuzzer; run explicitly in CI via --run-ignored=only"]
fn fuzz_heal_bitrot() {
    let dir = tempfile::tempdir().unwrap();
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::StdFs);
    let corpus = build_corpus(dir.path(), &fs);
    assert!(!corpus.is_empty(), "the corpus has at least one variant");

    let scratch = dir.path().join("scratch");
    let mut rng = SplitMix64(SEED);
    let start = Instant::now();
    let mut iters: u64 = 0;

    while start.elapsed() < BUDGET {
        iters += 1;
        let entry = &corpus[(rng.next_u64() as usize) % corpus.len()];
        let mut bytes = entry.bytes.clone();
        let off = (rng.next_u64() as usize) % bytes.len();
        let bit = (rng.next_u64() % 8) as u8;
        bytes[off] ^= 1u8 << bit;
        std::fs::write(&scratch, &bytes).unwrap();

        let ctx = format!(
            "seed={SEED:#x} iter={iters} variant={} off={off} bit={bit}",
            entry.label
        );
        // Any error is an acceptable (detected) outcome; a PANIC or a wrong value
        // is the bug this fuzzer hunts. Catch panics so the reproducing case is
        // reported rather than an opaque unwind.
        let enc = entry.encryption.clone();
        let fs2 = Arc::clone(&fs);
        let scratch2 = scratch.clone();
        let ctx2 = ctx.clone();
        let salvage_dest = scratch.with_extension("salvaged");
        let _ = std::fs::remove_file(&salvage_dest);
        let enc_for_salvage = entry.encryption.clone();
        let fs3 = Arc::clone(&fs);
        let scratch3 = scratch.clone();
        let ctx3 = ctx.clone();
        let dest3 = salvage_dest.clone();
        // A flip inside exactly ONE meta mirror must recover COMPLETELY: the
        // other copy is byte-identical and intact, which is the entire reason
        // the mirror exists. Accepting an error here would let a regression in
        // the fallback hide behind "an error is a valid outcome".
        let one_mirror_damaged = entry.hits_one_meta_mirror(off);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let scan = recover_and_scan(&scratch2, &fs2, enc, &ctx2);
            if one_mirror_damaged {
                METAFLIP_SEEN.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                assert!(
                    scan.is_ok(),
                    "{ctx2}: a flip in ONE meta mirror must fall back to the \
                     intact copy and recover completely, got {scan:?}",
                );
            }
            // The read path only has to REFUSE damaged data. Salvage has to
            // lose as little as possible, which is a separate claim and the
            // one this branch checks: a single flipped bit must never cost
            // more than the one block that carries it.
            let _ = salvage_and_check_minimality(&scratch3, &dest3, &fs3, enc_for_salvage, &ctx3);
        }));
        let _ = std::fs::remove_file(&salvage_dest);
        if outcome.is_err() {
            // Encrypted / timestamped corpus SSTs are NOT byte-deterministic (a
            // fresh AEAD nonce + `created_at` per build), so the seed alone cannot
            // reproduce a failure in those. Persist the EXACT flipped bytes that
            // failed — plus the context — so the case reproduces regardless. The
            // repro path is stable across runs; a follow-up run overwrites it, but
            // fail-fast stops at the first failure so the artifact survives.
            let repro = repro_dump(&bytes, &ctx, entry);
            panic!(
                "read path PANICKED / returned a wrong value on a single-bit flip.\n  {ctx}\n  \
                 reproduce with the exact failing SST at: {}\n  (variant \"{}\", encryption {})",
                repro.display(),
                entry.label,
                if entry.encryption.is_some() {
                    "Aes256Gcm key=[5u8;32]"
                } else {
                    "none"
                },
            );
        }
    }

    let salvage_ok = SALVAGE_OK.load(core::sync::atomic::Ordering::Relaxed);
    let salvage_dropped = SALVAGE_DROPPED.load(core::sync::atomic::Ordering::Relaxed);
    let metaflips = METAFLIP_SEEN.load(core::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "fuzz_heal_bitrot: {iters} iterations across {} variants in {:?} (seed {SEED:#x}); \
         salvage succeeded {salvage_ok}× ({salvage_dropped} of them dropped a block); \
         {metaflips} flips hit one meta mirror and recovered from the other",
        corpus.len(),
        start.elapsed(),
    );
    // The minimality bound is only meaningful if salvage actually ran and
    // actually lost something: a run where every attempt refused, or where
    // nothing was ever dropped, would satisfy `dropped <= 1` vacuously.
    assert!(
        salvage_dropped > 0,
        "no fuzz iteration produced a salvage that dropped a block, so the \
         one-block bound was never exercised",
    );
    // Likewise for the mirror: if no flip ever landed in exactly one meta copy,
    // the "must recover completely" assertion never ran.
    assert!(
        metaflips > 0,
        "no fuzz iteration flipped a byte inside exactly one meta mirror, so the \
         mirror-fallback requirement was never exercised",
    );
    // A per-iteration recover + full scan is fast, but a loaded / emulated runner
    // can still fit few iterations into the fixed wall-clock budget. Require only
    // enough to cover every corpus variant at least once, so the check catches a
    // broken loop without turning host slowness into a spurious failure.
    assert!(
        iters >= corpus.len() as u64,
        "the fuzzer must cover every corpus variant at least once",
    );
}
