mod config;
mod db;
mod reporter;
mod workloads;

use crate::config::{BenchConfig, Compression};
use crate::reporter::{JsonConfig, Reporter};
use crate::workloads::{available_benchmarks, create_workload};
use clap::Parser;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;

#[derive(Parser, Debug)]
#[command(
    name = "db_bench",
    about = "LSM-tree benchmark suite (RocksDB db_bench compatible)"
)]
struct Cli {
    /// Benchmark workload to run.
    #[arg(long, value_parser = parse_benchmark)]
    benchmark: String,

    /// Number of operations.
    #[arg(long, default_value = "1000000")]
    num: u64,

    /// Key size in bytes.
    #[arg(long, default_value = "16")]
    key_size: usize,

    /// Value size in bytes.
    #[arg(long, default_value = "100")]
    value_size: usize,

    /// Number of concurrent threads.
    #[arg(long, default_value = "1")]
    threads: usize,

    /// Block cache size in MB.
    #[arg(long, default_value = "64")]
    cache_mb: u64,

    /// Compression type: none, lz4, zstd.
    #[arg(long, default_value = "none")]
    compression: Compression,

    /// Data block size in bytes.
    #[arg(long, default_value = "4096")]
    block_size: u32,

    /// Use BlobTree (key-value separation) instead of standard Tree.
    #[arg(long, default_value = "false")]
    use_blob_tree: bool,

    /// Output results as JSON.
    #[arg(long, default_value = "false")]
    json: bool,

    /// Database directory path. If not set, a temporary directory is used.
    #[arg(long)]
    db: Option<PathBuf>,
}

fn parse_benchmark(s: &str) -> Result<String, String> {
    let available = available_benchmarks();
    if available.contains(&s) {
        Ok(s.to_string())
    } else {
        Err(format!(
            "unknown benchmark '{}'. Available: {}",
            s,
            available.join(", ")
        ))
    }
}

fn main() {
    let cli = Cli::parse();

    let bench_config = BenchConfig {
        num: cli.num,
        key_size: cli.key_size,
        value_size: cli.value_size,
        threads: cli.threads,
        cache_mb: cli.cache_mb,
        compression: cli.compression,
        block_size: cli.block_size,
        use_blob_tree: cli.use_blob_tree,
    };

    // Use provided path or create a temp directory.
    let _tmpdir;
    let db_path = match &cli.db {
        Some(p) => p.clone(),
        None => {
            _tmpdir = tempfile::tempdir().unwrap_or_else(|e| {
                eprintln!("Error: failed to create temp directory: {e}");
                std::process::exit(1);
            });
            _tmpdir.path().to_path_buf()
        }
    };

    eprintln!("=== db_bench ===");
    eprintln!("Benchmark:   {}", cli.benchmark);
    eprintln!("Num ops:     {}", cli.num);
    eprintln!("Key size:    {} bytes", cli.key_size);
    eprintln!("Value size:  {} bytes", cli.value_size);
    eprintln!("Threads:     {}", cli.threads);
    eprintln!("Cache:       {} MB", cli.cache_mb);
    eprintln!("Compression: {:?}", cli.compression);
    eprintln!("Block size:  {} bytes", cli.block_size);
    eprintln!("BlobTree:    {}", cli.use_blob_tree);
    eprintln!("DB path:     {}", db_path.display());
    eprintln!();

    let tree = config::create_tree(&db_path, &bench_config).unwrap_or_else(|e| {
        eprintln!("Error: failed to open tree: {e}");
        std::process::exit(1);
    });
    let seqno = AtomicU64::new(1);
    let mut reporter = Reporter::new();

    let workload = create_workload(&cli.benchmark).unwrap_or_else(|| {
        eprintln!("Error: unknown benchmark '{}'", cli.benchmark);
        std::process::exit(1);
    });

    if let Err(e) = workload.run(&tree, &bench_config, &seqno, &mut reporter) {
        eprintln!("Error: benchmark failed: {e}");
        std::process::exit(1);
    }

    if cli.json {
        let json_config = JsonConfig {
            num: cli.num,
            key_size: cli.key_size,
            value_size: cli.value_size,
            entry_size: bench_config.entry_size(),
            threads: cli.threads,
            compression: format!("{:?}", cli.compression),
        };
        println!("{}", reporter.to_json(&cli.benchmark, &json_config));
    } else {
        reporter.print_human(&cli.benchmark, bench_config.entry_size());
    }
}
