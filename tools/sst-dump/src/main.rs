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
use lsm_tree::inspect::{
    iter_data_block_entries, read_filter_stats, read_table_properties, read_top_level_index_entries,
};
use lsm_tree::table::block::Header;
use lsm_tree::verify::{BlockVerifyError, verify_sst_file};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::process::ExitCode;

/// Default number of bytes the `hex` subcommand prints when the
/// caller does not supply `--len`. Chosen to cover one `Header`
/// (at least 33 bytes: 4 B magic + 1 B block_type + 16 B
/// XXH3-128 checksum + 4 B data_length + 4 B uncompressed_length +
/// 4 B trailing checksum tag — see `Header::MIN_LEN`; meta / manifest
/// blocks add one `block_flags` byte) plus a few payload lines worth
/// of context.
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

    /// Stream every KV entry from the SST to stdout, one per line.
    /// Honours `--from` / `--to` key bounds (inclusive lower,
    /// exclusive upper, matching standard Rust range semantics) and
    /// caps output at `--max=N` entries when set. With `--keys-only`,
    /// skips the value column entirely. Only full-index SSTs are
    /// supported (the index is a single block; the data section
    /// itself can have any number of data blocks); partitioned-index
    /// SSTs exit non-zero (see `index-dump` for the layout signal).
    /// Reads blocks streamingly: memory cost stays at one data block
    /// regardless of SST size.
    ///
    /// **Comparator caveat (only when `--from` / `--to` is set).**
    /// A bounds-free `dump` walks every entry sequentially in
    /// on-disk order and works for any comparator — the iteration
    /// itself is positional, not comparator-driven. The
    /// `--from` / `--to` filters, however, compare keys bytewise
    /// against the supplied bounds and the upper bound break-out
    /// assumes on-disk order matches bytewise order. That holds for
    /// the default lexicographic comparator but not for arbitrary
    /// custom user comparators: with a custom comparator and
    /// `--from` / `--to` set, the early break can stop before all
    /// qualifying entries are emitted, and the bounds themselves
    /// won't filter to the semantic range the caller had in mind.
    /// Use the owning tree's regular read APIs for range-bounded
    /// reads on custom-comparator SSTs.
    Dump {
        /// Lower key bound (inclusive). Entries with `key >= --from`
        /// are emitted. Without this flag, the walk starts from the
        /// smallest key in the SST.
        #[arg(long)]
        from: Option<String>,

        /// Upper key bound (exclusive). Entries with `key < --to` are
        /// emitted. Without this flag, the walk ends at the largest
        /// key in the SST.
        #[arg(long)]
        to: Option<String>,

        /// Cap on entries emitted. The walk stops after `--max=N`
        /// entries pass the `--from` / `--to` filters even if more
        /// would otherwise have qualified.
        #[arg(long)]
        max: Option<u64>,

        /// Omit the value column from the output, printing only the
        /// keys (one per line, still wrapped in `format_key`'s
        /// escape rules). Useful when only key enumeration is
        /// needed and values are large.
        #[arg(long)]
        keys_only: bool,
    },

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
        Command::Dump {
            from,
            to,
            max,
            keys_only,
        } => run_dump(&cli.file, from.as_deref(), to.as_deref(), max, keys_only),
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
    if !no_header && read_len as usize >= Header::MIN_LEN {
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
            "header:         skipped (only {read_len} bytes available, \
             header is at least {} bytes)",
            Header::MIN_LEN,
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

fn run_dump(
    path: &std::path::Path,
    from: Option<&str>,
    to: Option<&str>,
    max: Option<u64>,
    keys_only: bool,
) -> ExitCode {
    let iter = match iter_data_block_entries(path) {
        // Propagate `--keys-only` into the iterator itself: it then
        // yields entries with `value: Vec::new()` and skips the
        // per-entry `Slice::to_vec()` allocation entirely. Net
        // saving on a values-heavy SST is the full data-section
        // value byte count.
        Ok(it) if keys_only => it.keys_only(),
        Ok(it) => it,
        Err(lsm_tree::Error::Io(io_err)) if io_err.kind() == std::io::ErrorKind::Unsupported => {
            // Partitioned-index SSTs surface here. Same pattern as
            // `filter-stats` for partitioned filters: print a
            // user-facing "not supported" message that names the
            // distinguishing on-disk signal so an operator can
            // confirm via `index-dump` or a hex dump of the SFA TOC.
            eprintln!(
                "error: dump not supported for {}: {io_err} \
                 (look for an `index` section in the SFA TOC to confirm)",
                path.display(),
            );
            return ExitCode::FAILURE;
        }
        Err(e) => {
            // `iter_data_block_entries` covers more than just the
            // initial file-open: it also walks the SFA trailer, the
            // meta block, the TLI, and validates the data-block
            // layout. Any of those can fail here, so the generic
            // bucket message uses "iter entries" instead of "open"
            // to avoid suggesting the failure was at File::open
            // when it might have been an SFA trailer mismatch or a
            // meta-block decode error.
            eprintln!("error: iter entries from {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    // Bounds as byte slices for direct comparison against entry
    // keys (which are `Vec<u8>`). The CLI takes them as &str so the
    // common alphanumeric case is ergonomic; non-UTF-8 keys can't be
    // expressed on the command line without an escape mechanism,
    // which we deliberately don't ship in this first pass — point
    // users at a follow-up if they need it.
    let from_bytes = from.map(str::as_bytes);
    let to_bytes = to.map(str::as_bytes);

    // If the bounds collapse to an empty range bytewise (`--from >=
    // --to` with both set), the walk would scan up to `from` only to
    // immediately break on the upper bound: a forged-empty range
    // could trigger a full-SST scan with zero output. Short-circuit
    // up front and exit 0 with no output. The match condition stays
    // a bytewise comparison for parity with the in-loop checks; the
    // comparator caveat in the subcommand docstring covers
    // custom-comparator SSTs.
    if let (Some(lo), Some(hi)) = (from_bytes, to_bytes)
        && lo >= hi
    {
        return ExitCode::SUCCESS;
    }

    let mut emitted: u64 = 0;
    let cap = max.unwrap_or(u64::MAX);

    // `--max 0` is a "no output ever" request. The iterator
    // construction above already paid for the SFA trailer / meta /
    // TLI reads (those are needed regardless to know whether the
    // layout is even supported), but the in-loop `emitted >= cap`
    // check would still pull the FIRST data block off disk before
    // breaking. Short-circuit here so a `--max 0` invocation costs
    // zero data-block I/O on top of the unavoidable index reads.
    if cap == 0 {
        return ExitCode::SUCCESS;
    }

    // Pull the iterator manually instead of `for item in iter` so the
    // `emitted < cap` check can gate the `iter.next()` call itself.
    // With the `for` form, the cap check ran AFTER pulling and decoding
    // the next entry; in the worst case (cap falls on a block boundary)
    // that triggered an extra data-block read + decompression after the
    // cap was already reached. The while-let form short-circuits before
    // pulling.
    //
    // Output is buffered through a single locked stdout BufWriter and
    // each entry emits exactly one `writeln!` so a million-entry SST
    // dump doesn't pay one stdout lock + one syscall per print call.
    // Write errors can surface in two places: per-line `writeln!`
    // calls (handled below via `line_result`) and the explicit
    // `out.flush()` at the end (which surfaces errors buffered
    // inside the BufWriter that would otherwise be swallowed by
    // BufWriter's Drop implementation).
    use std::io::Write as _;
    let stdout = std::io::stdout();
    let stdout_lock = stdout.lock();
    let mut out = std::io::BufWriter::new(stdout_lock);

    let mut iter = iter;
    while emitted < cap {
        let Some(item) = iter.next() else {
            break;
        };
        let entry = match item {
            Ok(e) => e,
            Err(e) => {
                eprintln!("error: read entry from {}: {e}", path.display());
                return ExitCode::FAILURE;
            }
        };

        // Lower bound check first because it's the more common
        // trim direction (operators usually want "from key X
        // onwards" rather than "everything below key Y").
        if let Some(lo) = from_bytes
            && entry.key.as_slice() < lo
        {
            continue;
        }
        if let Some(hi) = to_bytes
            && entry.key.as_slice() >= hi
        {
            // Keys are sorted in the SST; once we pass the upper
            // bound we're done. Break instead of continuing to
            // avoid walking the rest of the (potentially huge)
            // data section.
            break;
        }

        // One writeln! per emitted entry: BufWriter accumulates and
        // releases the bytes in larger flushes, and stdout is locked
        // once (above) for the whole walk instead of per-line.
        let line_result = if keys_only {
            writeln!(out, "{}", format_key(&entry.key))
        } else {
            // `key=value` separator-style — matches what most
            // RocksDB / LevelDB sst-dump variants emit and is the
            // easiest format for downstream `awk -F=` consumers.
            // Non-`Value` entries are annotated with a per-variant
            // suffix tag so operators can tell a real value from a
            // tombstone / merge operand / blob-pointer indirection
            // without separately inspecting the value bytes;
            // regular Value entries get the bare `=value` line so
            // the happy path stays grep-friendly.
            let suffix = match entry.value_type {
                lsm_tree::ValueType::Value => "",
                lsm_tree::ValueType::Tombstone => "\t# tombstone",
                lsm_tree::ValueType::WeakTombstone => "\t# weak-tombstone",
                lsm_tree::ValueType::MergeOperand => "\t# merge-operand",
                lsm_tree::ValueType::Indirection => "\t# indirection",
            };
            writeln!(
                out,
                "{}={}{}",
                format_key(&entry.key),
                format_key(&entry.value),
                suffix,
            )
        };
        if let Err(e) = line_result {
            // Broken pipe (e.g. consumer piped through `head -n N`)
            // is the common case; report and exit cleanly rather
            // than panic-on-flush at function tail.
            eprintln!("error: write to stdout for {}: {e}", path.display());
            return ExitCode::FAILURE;
        }

        emitted = emitted.saturating_add(1);
    }

    // Surface a buffered-flush error to the operator instead of
    // letting BufWriter's Drop swallow it on the way out.
    if let Err(e) = out.flush() {
        eprintln!("error: flush stdout for {}: {e}", path.display());
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

fn run_filter_stats(path: &std::path::Path) -> ExitCode {
    let stats = match read_filter_stats(path) {
        Ok(s) => s,
        Err(lsm_tree::Error::FeatureUnsupported(marker)) => {
            // Partitioned-filter SSTs surface here. Match the typed
            // `FeatureUnsupported` variant directly so control flow
            // doesn't depend on a message-string substring; the
            // `marker` payload is the SFA section name the operator
            // can confirm via the TOC.
            eprintln!(
                "error: filter-stats not supported for {}: \
                 valid-but-unsupported layout (look for a `{marker}` \
                 section in the SFA TOC to confirm)",
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
