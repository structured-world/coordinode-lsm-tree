//! `v5-to-v6 <store>`: converts a store written by the 5.x crate to the 6.0
//! on-disk format, offline and in place.

use clap::Parser;
use std::sync::Arc;

/// Converts a 5.x store to the 6.0 format, offline and in place.
#[derive(Parser)]
struct Args {
    /// The store's folder.
    store: std::path::PathBuf,

    /// A file holding the 32-byte AES-256-GCM key the store is encrypted
    /// under, for a store opened with the crate's AES-256-GCM provider.
    #[arg(long)]
    aes256_key_file: Option<std::path::PathBuf>,

    /// A compression dictionary the store was opened with but does not keep
    /// in its dictionary folder, as a file of its raw bytes. Repeatable.
    #[arg(long = "dictionary")]
    dictionaries: Vec<std::path::PathBuf>,

    /// A level route the store was opened with, as `<first>..<end>=<folder>`:
    /// the levels from `first` up to, not including, `end` keep their tables
    /// in `<folder>/tables`. Repeatable.
    #[arg(long = "level-route", value_parser = level_route)]
    level_routes: Vec<v5_to_v6::LevelRoute>,
}

fn level_route(arg: &str) -> Result<v5_to_v6::LevelRoute, String> {
    let malformed = || format!("{arg}: expected <first>..<end>=<folder>");
    let (levels, path) = arg.split_once('=').ok_or_else(malformed)?;
    let (first, end) = levels.split_once("..").ok_or_else(malformed)?;
    Ok(v5_to_v6::LevelRoute {
        levels: first.parse().map_err(|_| malformed())?..end.parse().map_err(|_| malformed())?,
        path: path.into(),
    })
}

fn options(args: &Args) -> Result<v5_to_v6::Options, String> {
    let encryption = match &args.aes256_key_file {
        Some(path) => {
            let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let key: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                format!(
                    "{}: holds {} bytes, an AES-256 key is 32",
                    path.display(),
                    bytes.len()
                )
            })?;
            Some(v5_to_v6::EncryptionPair {
                v5: Arc::new(lsm5::Aes256GcmProvider::new(&key)),
                v6: Arc::new(lsm6::Aes256GcmProvider::new(&key)),
            })
        }
        None => None,
    };
    let dictionaries = args
        .dictionaries
        .iter()
        .map(|path| std::fs::read(path).map_err(|e| format!("{}: {e}", path.display())))
        .collect::<Result<_, _>>()?;
    Ok(v5_to_v6::Options {
        encryption,
        dictionaries,
        level_routes: args.level_routes.clone(),
    })
}

fn main() -> std::process::ExitCode {
    let args = Args::parse();
    let result = options(&args)
        .and_then(|options| v5_to_v6::convert(&args.store, &options).map_err(|e| e.to_string()));
    match result {
        Ok(report) if report.resumed => {
            println!("finished the switch an earlier run started");
            std::process::ExitCode::SUCCESS
        }
        Ok(report) => {
            println!(
                "converted {} tables, {} data blocks, {} blob files: {} bytes, {} before",
                report.tables,
                report.data_blocks,
                report.blob_files,
                report.converted_bytes,
                report.source_bytes,
            );
            for (from, to) in &report.renumbered_fields {
                println!("field {from} is a column id 6.0 keeps for itself: it is now field {to}");
            }
            println!(
                "the source is kept in {}",
                args.store.join(v5_to_v6::BACKUP).display()
            );
            for route in &args.level_routes {
                println!(
                    "the source's tables of levels {}..{} are kept in {}",
                    route.levels.start,
                    route.levels.end,
                    route.path.join(v5_to_v6::BACKUP).display()
                );
            }
            std::process::ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::ExitCode::FAILURE
        }
    }
}
