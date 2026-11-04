//! Runs the blob file sweep over the corpora in `--data` and writes one JSON
//! object per run to `--out` (`sweep.json` by default), printing a summary.
//!
//! ```text
//! blob-sweep --data DIR [--work DIR] [--out FILE] [--only SUBSTRING]
//! ```
//!
//! `--work` is where the trees are written, the system's temporary directory
//! by default; it should be the disk the products run on, not a RAM disk.

use blob_sweep::corpus::{self, Corpus};
use blob_sweep::run::{Codec, Layout, measure, train};
use blob_sweep::{Error, HeapProbe};
use lsm_tree::DictionaryClasses;
use std::alloc::{GlobalAlloc, Layout as AllocLayout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// The system allocator, counting the bytes in use and the most since the
/// last reset.
struct Counting {
    in_use: AtomicU64,
    peak: AtomicU64,
    base: AtomicU64,
}

// SAFETY: every call goes to `System` unchanged; the counters only observe it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: AllocLayout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`, passed on.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = self
                .in_use
                .fetch_add(layout.size() as u64, Ordering::Relaxed)
                + layout.size() as u64;
            self.peak.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: AllocLayout) {
        // SAFETY: the caller's contract for `dealloc`, passed on.
        unsafe { System.dealloc(ptr, layout) };
        self.in_use
            .fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }
}

#[global_allocator]
static HEAP: Counting = Counting {
    in_use: AtomicU64::new(0),
    peak: AtomicU64::new(0),
    base: AtomicU64::new(0),
};

impl HeapProbe for Counting {
    fn reset(&self) {
        let now = self.in_use.load(Ordering::Relaxed);
        self.base.store(now, Ordering::Relaxed);
        self.peak.store(now, Ordering::Relaxed);
    }

    fn peak(&self) -> Option<u64> {
        let (peak, base) = (
            self.peak.load(Ordering::Relaxed),
            self.base.load(Ordering::Relaxed),
        );
        Some(peak - base.min(peak))
    }
}

struct Args {
    data: PathBuf,
    work: PathBuf,
    out: PathBuf,
    only: Option<String>,
}

fn args() -> Result<Args, Error> {
    let mut parsed = Args {
        data: PathBuf::new(),
        work: std::env::temp_dir(),
        out: PathBuf::from("sweep.json"),
        only: None,
    };
    let mut given = std::env::args().skip(1);
    while let Some(arg) = given.next() {
        let mut value = || given.next().ok_or_else(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--data" => parsed.data = PathBuf::from(value()?),
            "--work" => parsed.work = PathBuf::from(value()?),
            "--out" => parsed.out = PathBuf::from(value()?),
            "--only" => parsed.only = Some(value()?),
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    if parsed.data.as_os_str().is_empty() {
        return Err("--data DIR is required".into());
    }
    Ok(parsed)
}

/// The runs of `corpus`: the per-value baseline and the block targets under
/// both codecs, the chunk sizes, and the dictionary classes under zstd with a
/// dictionary trained on the corpus.
fn runs(corpus: &Corpus) -> Result<Vec<(Layout, Codec)>, Error> {
    let codecs = [Codec::Lz4, Codec::Zstd(3)];
    let mut runs = Vec::new();
    for codec in &codecs {
        runs.push((Layout::per_value(), codec.clone()));
        for block in [4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10] {
            runs.push((Layout::sized(block, 256 << 10), codec.clone()));
        }
        for chunk in [64 << 10, 1 << 20, 4 << 20] {
            runs.push((Layout::sized(16 << 10, chunk), codec.clone()));
        }
    }
    let dictionary = Codec::ZstdDict(3, train(corpus, 8 << 20, 64 << 10)?);
    for (name, classes) in [
        ("blocks", (true, false, false)),
        ("blocks singles", (true, true, false)),
        ("all", (true, true, true)),
    ] {
        let mut layout = Layout::sized(16 << 10, 256 << 10);
        layout.name = format!("{} dict {name}", layout.name);
        layout.dictionary = DictionaryClasses {
            blocks: classes.0,
            singles: classes.1,
            chunks: classes.2,
        };
        runs.push((layout, dictionary.clone()));
    }
    Ok(runs)
}

fn main() -> Result<(), Error> {
    let args = args()?;
    let corpora = corpus::load(&args.data)?;
    if corpora.is_empty() {
        return Err(format!("no corpus under {}", args.data.display()).into());
    }
    let mut results = Vec::new();
    println!(
        "{:<10} {:<34} {:<14} {:>7} {:>9} {:>9} {:>10} {:>10} {:>9} {:>6}",
        "corpus",
        "layout",
        "codec",
        "ratio",
        "write ms",
        "scan ms",
        "get ns/s",
        "get ns/l",
        "range ns",
        "gc wa"
    );
    for corpus in &corpora {
        println!(
            "# {}: {} values, {} bytes",
            corpus.name,
            corpus.values.len(),
            corpus.bytes()
        );
        for (layout, codec) in runs(corpus)? {
            let label = format!("{} {} {}", corpus.name, layout.name, codec.name());
            if args.only.as_ref().is_some_and(|only| !label.contains(only)) {
                continue;
            }
            let measured = measure(corpus, &layout, &codec, &args.work, &HEAP)?;
            let json = measured.to_json(&corpus.name, &layout, &codec);
            let get = |field: &str| json[field].as_f64().unwrap_or(f64::NAN);
            println!(
                "{:<10} {:<34} {:<14} {:>7.4} {:>9.0} {:>9.0} {:>10.0} {:>10.0} {:>9.0} {:>6.2}",
                corpus.name,
                layout.name,
                codec.name(),
                measured.ratio(),
                get("write_ms"),
                get("scan_ms"),
                get("get_ns_small_cache"),
                get("get_ns_large_cache"),
                get("range_ns"),
                get("gc_write_amplification"),
            );
            results.push(json);
        }
    }
    std::fs::write(&args.out, serde_json::to_string_pretty(&results)?)?;
    println!("{} runs written to {}", results.len(), args.out.display());
    Ok(())
}
