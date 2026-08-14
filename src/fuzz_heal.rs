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

#![expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    reason = "fuzz test"
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
        out.push(CorpusEntry {
            label: variant.label.to_string(),
            bytes,
            encryption: variant.encryption,
        });
    }
    out
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
    Table::recover(
        path.to_path_buf(),
        checksum,
        0,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(1 << 20)),
        None,
        Arc::clone(fs),
        false,
        false,
        encryption,
        #[cfg(zstd_any)]
        None,
        crate::comparator::default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )
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
    }
    // Touch the block index too, so a corrupt index surfaces (as Err) rather than
    // being skipped by an early scan short-circuit.
    for handle in table.block_index.iter() {
        handle?;
    }
    Ok(())
}

/// Persists the exact bytes that failed (plus a `.txt` describing the case) to a
/// stable path, and returns it. This is the ground-truth reproducer for a
/// non-byte-deterministic corpus (encrypted / timestamped SSTs): re-running the
/// read path over this file re-hits the failure directly, no seed replay needed.
fn repro_dump(bytes: &[u8], ctx: &str, entry: &CorpusEntry) -> std::path::PathBuf {
    // CWD during `cargo`/`nextest` is the crate root; a CI failure step can upload
    // it as an artifact. Fixed name so the location is predictable.
    let sst = std::path::PathBuf::from("fuzz_heal_repro.sst");
    let _ = std::fs::write(&sst, bytes);
    let _ = std::fs::write(
        "fuzz_heal_repro.txt",
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
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _ = recover_and_scan(&scratch2, &fs2, enc, &ctx2);
        }));
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

    eprintln!(
        "fuzz_heal_bitrot: {iters} iterations across {} variants in {:?} (seed {SEED:#x})",
        corpus.len(),
        start.elapsed(),
    );
    assert!(
        iters > 100,
        "the fuzzer must run a meaningful number of iterations"
    );
}
