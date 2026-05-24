// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! `sst-dump` — operational diagnostic CLI for a single SST file.
//!
//! Mirrors RocksDB's `sst_dump` utility. Used to inspect / verify an
//! SST out-of-band, without spinning up a `Tree` or recovering the
//! manifest. This is the tool reached for when a production read
//! starts failing and the question becomes "is this file corrupt? what
//! does it actually contain?".

use clap::{Parser, Subcommand};
use lsm_tree::verify::{BlockVerifyError, verify_sst_file};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser, Debug)]
#[command(
    name = "sst-dump",
    about = "Inspect / verify a single coordinode-lsm-tree SST file",
    version
)]
struct Cli {
    /// Path to the SST file (typically `<dir>/tables/<id>`).
    file: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Walk every block in the file, verify per-block XXH3 checksums,
    /// print a summary. Exits non-zero if any block or section is
    /// corrupt.
    Verify {
        /// Print every per-block error individually instead of just a
        /// count + the first three.
        #[arg(long)]
        verbose: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Verify { verbose } => run_verify(&cli.file, verbose),
    }
}

fn run_verify(path: &std::path::Path, verbose: bool) -> ExitCode {
    let report = verify_sst_file(path);

    println!("file:           {}", path.display());
    println!("blocks scanned: {}", report.blocks_scanned);
    println!("errors:         {}", report.errors.len());

    if report.is_ok() {
        println!("status:         OK");
        return ExitCode::SUCCESS;
    }

    println!("status:         CORRUPT");
    println!();

    let to_show: usize = if verbose { report.errors.len() } else { 3 };
    for (idx, err) in report.errors.iter().take(to_show).enumerate() {
        // Show each error with its variant tag so consumers grep'ing
        // for a specific failure mode (HeaderCorrupted, DataCorrupted,
        // DataReadError, TocCorrupted, SstFileUnreadable) get a stable
        // anchor. The Display impl includes file path + offset + a
        // human reason.
        let kind = match err {
            BlockVerifyError::SstFileUnreadable { .. } => "SstFileUnreadable",
            BlockVerifyError::HeaderCorrupted { .. } => "HeaderCorrupted",
            BlockVerifyError::DataCorrupted { .. } => "DataCorrupted",
            BlockVerifyError::DataReadError { .. } => "DataReadError",
            BlockVerifyError::TocCorrupted { .. } => "TocCorrupted",
            // `BlockVerifyError` is `#[non_exhaustive]` upstream — a
            // future lib release can add new variants without bumping
            // the tool's major version. Unknown variants fall back to
            // their `Display` impl with a generic tag.
            _ => "Unknown",
        };
        println!("  [{idx}] {kind}: {err}");
    }

    if !verbose && report.errors.len() > to_show {
        println!(
            "  ... {} more (use --verbose to see all)",
            report.errors.len() - to_show
        );
    }

    ExitCode::FAILURE
}
