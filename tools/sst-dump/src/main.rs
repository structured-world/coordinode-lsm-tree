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
use lsm_tree::coding::Decode;
use lsm_tree::inspect::{read_filter_stats, read_table_properties, read_top_level_index_entries};
use lsm_tree::table::block::Header;
use lsm_tree::verify::{BlockVerifyError, verify_sst_file};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::ExitCode;

/// Default number of bytes the `hex` subcommand prints when the
/// caller does not supply `--len`. Chosen to cover one `Header`
/// (currently 33 bytes: 4 B magic + 1 B block_type + 16 B
/// XXH3-128 checksum + 4 B data_length + 4 B uncompressed_length +
/// 4 B trailing checksum tag — see `Header::serialized_len()`)
/// plus a few payload lines worth of context.
const HEX_DEFAULT_LEN: u64 = 256;
/// Hard ceiling on the user-requestable `hex` length so a typo
/// (`--len 4294967295`) can't allocate a 4 GiB buffer. 1 MiB
/// already overshoots any plausible single-block diagnostic dump.
const HEX_MAX_LEN: u64 = 1024 * 1024;

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

    /// Raw hex dump of a region of the file. If the region starts on
    /// a `Header`-prefixed block boundary (as reported by `verify` or
    /// derived from a TOC offset), the header is decoded and printed
    /// before the hex bytes for context. Useful for inspecting a
    /// specific block flagged as `HeaderCorrupted` / `DataCorrupted`
    /// without spinning up a `Tree`.
    Hex {
        /// Byte offset into the file where the dump starts. Typically
        /// a value reported by `verify --verbose` or a TOC section
        /// start position.
        offset: u64,

        /// How many bytes to dump (max 1 MiB). Defaults to 256, which
        /// covers a `Header` plus a few payload lines worth of
        /// context.
        #[arg(long, default_value_t = HEX_DEFAULT_LEN)]
        len: u64,

        /// Skip the `Header` decode attempt and print only the raw
        /// hex. Use when the offset is known not to be a block
        /// boundary (e.g. mid-payload, raw-format section).
        #[arg(long)]
        no_header: bool,
    },

    /// Print the SST's stored metadata: table id, key range, KV /
    /// tombstone counts, data / index block counts, compression
    /// codecs, and creation timestamp. Reads the on-disk meta block
    /// directly (with MID-mirror fallback per #295); does not open a
    /// live `Tree` or touch the manifest.
    Properties,

    /// Print the SST's top-level index (TLI) entries: one row per
    /// pointed-at block with its `end_key`, file `offset`, on-disk
    /// `size`, and the highest `seqno` it covers. For full-index
    /// tables each row corresponds to a data block; for
    /// partitioned-index tables each row corresponds to a sub-index
    /// leaf block (one further indirection from data blocks). Useful
    /// for diagnosing range-read fan-out and verifying the TLI
    /// matches what `verify` walked. Reads the TLI directly (with
    /// tail-mirror fallback per #296); does not open a live `Tree`.
    IndexDump,

    /// Print sizing stats for the SST's BuRR filter: on-disk filter
    /// section bytes, BuRR layer count, item count from meta, and
    /// approximate bits-per-key. Only single-block (full) filters
    /// are supported by this subcommand; partitioned-filter tables
    /// (`filter_tli` SFA section present) exit non-zero with a
    /// "not supported" error. Filter-less tables (no `filter`
    /// section at all) exit 0 with a "no filter installed" notice.
    FilterStats,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Verify { verbose } => run_verify(&cli.file, verbose),
        Command::Hex {
            offset,
            len,
            no_header,
        } => run_hex(&cli.file, offset, len, no_header),
        Command::Properties => run_properties(&cli.file),
        Command::IndexDump => run_index_dump(&cli.file),
        Command::FilterStats => run_filter_stats(&cli.file),
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

fn run_hex(path: &std::path::Path, offset: u64, len: u64, no_header: bool) -> ExitCode {
    if len == 0 {
        eprintln!("error: --len must be > 0");
        return ExitCode::FAILURE;
    }
    if len > HEX_MAX_LEN {
        eprintln!("error: --len {len} exceeds maximum of {HEX_MAX_LEN} bytes (1 MiB)");
        return ExitCode::FAILURE;
    }

    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: open {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    let file_size = match file.metadata().map(|m| m.len()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: stat {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    if offset >= file_size {
        eprintln!(
            "error: offset {offset} is past end of file ({file_size} bytes); nothing to dump"
        );
        return ExitCode::FAILURE;
    }

    // Clamp the requested length to bytes-actually-available so a
    // `--len 256` at the file tail dumps the partial tail rather
    // than EOF-erroring out. The caller sees the actual byte count
    // in the header line below.
    let available = file_size - offset;
    let read_len = len.min(available);

    if let Err(e) = file.seek(SeekFrom::Start(offset)) {
        eprintln!("error: seek to {offset}: {e}");
        return ExitCode::FAILURE;
    }

    // `read_len` is bounded by `HEX_MAX_LEN` (1 MiB) above, so the
    // `usize` cast is safe on any platform we target. `read_exact`
    // is strict: a short read fails the call, and we exit non-zero
    // without dumping anything. Short reads are not possible on the
    // happy path because `read_len = min(len, file_size - offset)`
    // is already bounded by bytes-actually-on-disk above, so the
    // only way `read_exact` can short-read is mid-call truncation
    // of the file by another process or a real I/O error — both
    // are reportable failures, not partial-dump scenarios.
    let mut buf = vec![0u8; read_len as usize];
    if let Err(e) = file.read_exact(&mut buf) {
        eprintln!("error: read {read_len} bytes at {offset}: {e}");
        return ExitCode::FAILURE;
    }

    println!("file:           {}", path.display());
    println!("file size:      {file_size} bytes");
    println!("offset:         {offset} (0x{offset:08x})");
    println!("dumped:         {read_len} bytes (requested {len})");

    // Attempt header decode unless the caller asked us to skip it
    // (offset known not to be a block boundary) or the buffer is
    // shorter than a serialized header.
    if !no_header && read_len as usize >= Header::serialized_len() {
        let mut header_reader = &buf[..];
        match Header::decode_from(&mut header_reader) {
            Ok(header) => {
                println!("header:");
                println!("  block_type:          {:?}", header.block_type);
                println!("  data_length:         {} bytes", header.data_length,);
                println!(
                    "  uncompressed_length: {} bytes",
                    header.uncompressed_length,
                );
                println!("  checksum (XXH3):     {:?}", header.checksum);
            }
            Err(e) => {
                // Decode failure is informational — the caller may be
                // dumping a non-header byte range deliberately. Print
                // the error so the operator knows the structural
                // shape didn't match, then continue with the raw hex.
                println!("header:         decode failed ({e}); printing raw bytes only");
            }
        }
    } else if no_header {
        println!("header:         skipped (--no-header)");
    } else {
        println!(
            "header:         skipped (only {read_len} bytes available, header is {} bytes)",
            Header::serialized_len(),
        );
    }

    println!();
    hex_dump(offset, &buf);

    ExitCode::SUCCESS
}

/// Prints a classic xxd-style hex+ASCII dump of `buf`, with each
/// line annotated with the absolute file offset (= `base_offset`
/// plus the line's index within `buf`).
fn hex_dump(base_offset: u64, buf: &[u8]) {
    const BYTES_PER_LINE: usize = 16;

    for (i, chunk) in buf.chunks(BYTES_PER_LINE).enumerate() {
        // Per-line absolute file offset. `i * BYTES_PER_LINE` cannot
        // overflow because `buf.len()` is bounded by `HEX_MAX_LEN`
        // (1 MiB) by the caller's clamp above.
        let line_offset = base_offset + (i * BYTES_PER_LINE) as u64;
        print!("{line_offset:08x}  ");

        // Hex bytes, padded to keep the ASCII column aligned even on
        // the final short line.
        for j in 0..BYTES_PER_LINE {
            if let Some(b) = chunk.get(j) {
                print!("{b:02x} ");
            } else {
                print!("   ");
            }
            if j == 7 {
                // Extra space at the half-line to match `xxd`'s
                // convention and make 8-byte alignment visible.
                print!(" ");
            }
        }

        // ASCII gutter: printable ASCII or `.` for everything else
        // (including the high-bit-set range, which is rarely useful
        // and clutters output).
        print!(" |");
        for b in chunk {
            let c = if (0x20..0x7f).contains(b) {
                *b as char
            } else {
                '.'
            };
            print!("{c}");
        }
        println!("|");
    }
}

fn run_properties(path: &std::path::Path) -> ExitCode {
    let props = match read_table_properties(path) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: read properties of {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    println!("file:                {}", path.display());
    println!("table_id:            {}", props.id);
    println!("file_size:           {} bytes", props.file_size);
    println!(
        "created_at:          {} ns since Unix epoch",
        props.created_at_nanos
    );
    println!(
        "key_range:           min={} max={}",
        format_key(&props.min_key),
        format_key(&props.max_key),
    );
    println!("item_count:          {}", props.item_count);
    println!("tombstone_count:     {}", props.tombstone_count);
    println!("weak_tombstones:     {}", props.weak_tombstone_count);
    println!("weak_reclaimable:    {}", props.weak_tombstone_reclaimable,);
    println!("data_block_count:    {}", props.data_block_count);
    println!("index_block_count:   {}", props.index_block_count);
    println!("data_compression:    {:?}", props.data_block_compression,);
    println!("index_compression:   {:?}", props.index_block_compression,);

    ExitCode::SUCCESS
}

fn run_index_dump(path: &std::path::Path) -> ExitCode {
    let entries = match read_top_level_index_entries(path) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("error: read TLI of {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    println!("file:                {}", path.display());
    println!("tli_entry_count:     {}", entries.len());
    println!();

    if entries.is_empty() {
        return ExitCode::SUCCESS;
    }

    // Header row. Numeric columns are right-aligned (`{:>...}`) so
    // u64 / u32 values stack cleanly under their headers; widths
    // chosen so a 64-bit offset (up to 20 digits) and u32 size (up
    // to 10) line up against a typical key prefix without crowding.
    // Keys can be arbitrarily long; place `end_key` last so
    // wrap-around doesn't mis-align the numeric columns.
    println!(
        "{:>5}  {:>20}  {:>10}  {:>20}  end_key",
        "#", "offset", "size", "seqno",
    );
    for (i, e) in entries.iter().enumerate() {
        println!(
            "{:>5}  {:>20}  {:>10}  {:>20}  {}",
            i,
            e.offset,
            e.size,
            e.seqno,
            format_key(&e.end_key),
        );
    }

    ExitCode::SUCCESS
}

fn run_filter_stats(path: &std::path::Path) -> ExitCode {
    let stats = match read_filter_stats(path) {
        Ok(s) => s,
        Err(lsm_tree::Error::Io(io_err)) if io_err.kind() == std::io::ErrorKind::Unsupported => {
            // Partitioned-filter SSTs surface here. Match the inner
            // `io::ErrorKind::Unsupported` so the CLI prints a
            // user-facing "not supported" line instead of the
            // bare-Display of the inner error; mention the
            // distinguishing on-disk signal (`filter_tli` SFA section)
            // so an operator can confirm the diagnosis with `verify
            // --verbose` or a hex dump of the SFA TOC.
            eprintln!(
                "error: filter-stats not supported for {}: {io_err} \
                 (look for a `filter_tli` section in the SFA TOC to confirm)",
                path.display(),
            );
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("error: read filter stats of {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    println!("file:                {}", path.display());
    let Some(stats) = stats else {
        println!("filter:              no filter section installed");
        return ExitCode::SUCCESS;
    };

    println!("filter_section_size: {} bytes", stats.filter_section_bytes);
    println!("layer_count:         {}", stats.layer_count);
    println!("item_count:          {}", stats.item_count);
    // Three decimals is enough resolution for diagnostic use without
    // implying false precision; bits-per-key for production filters
    // is typically in the 5-15 range, occasionally up to ~30 with
    // tight FPR targets, so the integer part is always small.
    println!("bits_per_key:        {:.3}", stats.bits_per_key);

    ExitCode::SUCCESS
}

/// Renders a user key as a single line, escaping non-printable bytes.
/// Output is always wrapped in double quotes. Printable ASCII
/// bytes (0x20..=0x7e) other than `"` and `\` are passed through
/// verbatim so the common "user-set alphanumeric prefix" stays
/// readable. `"` and `\` are escaped as `\"` and `\\` so the quoted
/// form round-trips unambiguously. Every other byte (control chars,
/// high-bit-set bytes) is rendered as a `\xNN` hex escape.
fn format_key(key: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut out = String::with_capacity(key.len() + 2);
    out.push('"');
    for &b in key {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            0x20..=0x7e => out.push(b as char),
            _ => {
                // Manual two-hex-char emit avoids `write!`'s `Result`
                // entirely. `String` writes are infallible but the
                // `write!` macro still returns `fmt::Result`, which
                // we previously discarded with `let _ = ...`; pushing
                // the chars directly is both clearer and slightly
                // faster (no formatter machinery).
                out.push('\\');
                out.push('x');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out.push('"');
    out
}
