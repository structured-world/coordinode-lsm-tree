// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The host-write ledger of a low-rate device workload, on a real filesystem
//! (the one under `TMPDIR`): where every byte a tree writes goes, by origin.
//!
//! `SENSORS` sensors report one reading a second each. The tree is flushed
//! every ten simulated minutes, as a device bounds what a power loss takes,
//! and compacted after each flush; once an hour the readings older than the
//! retention are deleted with range tombstones. Two key orders:
//!
//! - `time`: keyed by time first, so each flush's table lies past the last
//!   and an hour goes under one range tombstone;
//! - `sensor`: keyed by sensor first, so every flush overlaps the tables
//!   before it and compaction merges them, and an hour goes under one range
//!   tombstone per sensor.
//!
//! Per order it prints the tree's [`write_io`](lsm_tree::AbstractTree::write_io)
//! by origin, with the payload and the host write amplification, and one
//! `ledger,<order>,<origin>,<field>,<value>` line per counter for a replay
//! harness to read. On Linux it also reads `/proc/self/io` around the run:
//! `wchar` and `syscw`, the bytes and calls the process handed to `write`
//! syscalls, set against the ledger's bytes and write calls, and
//! `write_bytes`, what the process sent to the block layer. `LEDGER_ORDER`
//! runs one order only.

use lsm_tree::compaction::{CompactionAction, Leveled};
use lsm_tree::fs::{WriteIoSnapshot, WriteOrigin};
use lsm_tree::{AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;

const SENSORS: u32 = 64;
const HOURS: u64 = 6;
const FLUSH_EVERY_SECONDS: u64 = 600;
const RETENTION_HOURS: u64 = 3;
const VALUE_BYTES: usize = 24;

#[derive(Clone, Copy)]
enum Order {
    Time,
    Sensor,
}

impl Order {
    fn name(self) -> &'static str {
        match self {
            Self::Time => "time",
            Self::Sensor => "sensor",
        }
    }

    fn key(self, second: u64, sensor: u32) -> Vec<u8> {
        let (second, sensor) = (second.to_be_bytes(), sensor.to_be_bytes());
        match self {
            Self::Time => [&second[..], &sensor[..]].concat(),
            Self::Sensor => [&sensor[..], &second[..]].concat(),
        }
    }

    /// Deletes the readings of `hour` with range tombstones.
    fn expire(self, tree: &AnyTree, hour: u64, seqno: &SequenceNumberCounter) {
        let (start, end) = (
            (hour * 3_600).to_be_bytes(),
            ((hour + 1) * 3_600).to_be_bytes(),
        );
        match self {
            Self::Time => {
                tree.remove_range(start.to_vec(), end.to_vec(), seqno.next());
            }
            Self::Sensor => {
                let seqno = seqno.next();
                for sensor in 0..SENSORS {
                    let sensor = sensor.to_be_bytes();
                    tree.remove_range(
                        [&sensor[..], &start[..]].concat(),
                        [&sensor[..], &end[..]].concat(),
                        seqno,
                    );
                }
            }
        }
    }
}

/// A reading: the same for the same sensor and second, and not compressible
/// past what a sensor's raw samples are.
fn reading(second: u64, sensor: u32) -> [u8; VALUE_BYTES] {
    let mut s = (second << 20 ^ u64::from(sensor)).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = [0; VALUE_BYTES];
    for byte in &mut out {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        *byte = (s >> 56) as u8;
    }
    out
}

/// `/proc/self/io` fields, by label.
#[cfg(target_os = "linux")]
fn proc_io() -> std::collections::BTreeMap<String, u64> {
    let text = std::fs::read_to_string("/proc/self/io")
        .unwrap_or_else(|e| panic!("/proc/self/io unreadable (task I/O accounting off?): {e}"));
    text.lines()
        .filter_map(|line| {
            let (label, value) = line.split_once(':')?;
            Some((label.to_owned(), value.trim().parse().ok()?))
        })
        .collect()
}

fn print(order: Order, reading: &WriteIoSnapshot) {
    let mib = |b: u64| b as f64 / (1_024.0 * 1_024.0);
    println!(
        "{:<18} {:>11} {:>9} {:>8} {:>8} {:>13}",
        "origin", "written MiB", "writes", "syncs", "created", "installed MiB"
    );
    for (origin, files) in reading.iter() {
        if files == Default::default() {
            continue;
        }
        println!(
            "{:<18} {:>11.2} {:>9} {:>8} {:>8} {:>13.2}",
            origin.map_or("unattributed", WriteOrigin::name),
            mib(files.bytes),
            files.writes,
            files.syncs,
            files.created,
            mib(files.installed),
        );
    }
    let directory = reading.directory;
    println!(
        "{:<18} {:>11} {:>9} {:>8} {:>8}   renamed {}, removed {}, linked {}",
        "directory",
        "-",
        "-",
        directory.syncs,
        directory.dirs_created,
        directory.renamed,
        directory.removed,
        directory.linked,
    );
    println!(
        "{:<18} {:>11}   (a write-ahead log the caller keeps is not written through the \
         tree; count it apart and add it)",
        "caller WAL", "external",
    );
    let files = reading.files();
    println!(
        "\npayload {:.2} MiB in {} values and {} range deletions; written {:.2} MiB in {} calls, \
         {} syncs; host write amplification {:.2}",
        mib(reading.accepted.payload_bytes),
        reading.accepted.values,
        reading.accepted.range_deletions,
        mib(files.bytes),
        files.writes,
        reading.syncs(),
        reading.host_write_amplification().unwrap_or(f64::NAN),
    );

    let order = order.name();
    for (origin, files) in reading.iter() {
        let name = origin.map_or("unattributed", WriteOrigin::name);
        for (field, value) in [
            ("bytes", files.bytes),
            ("writes", files.writes),
            ("short_writes", files.short_writes),
            ("write_errors", files.write_errors),
            ("opened", files.opened),
            ("created", files.created),
            ("syncs", files.syncs),
            ("sync_errors", files.sync_errors),
            ("truncated", files.truncated),
            ("installed", files.installed),
        ] {
            println!("ledger,{order},{name},{field},{value}");
        }
    }
    for (field, value) in [
        ("syncs", directory.syncs),
        ("sync_errors", directory.sync_errors),
        ("device_syncs", directory.device_syncs),
        ("dirs_created", directory.dirs_created),
        ("renamed", directory.renamed),
        ("removed", directory.removed),
        ("linked", directory.linked),
        ("cloned", directory.cloned),
        ("holes_punched", directory.holes_punched),
    ] {
        println!("ledger,{order},directory,{field},{value}");
    }
    println!(
        "ledger,{order},accepted,payload_bytes,{}",
        reading.accepted.payload_bytes
    );
}

fn run(order: Order) -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    #[cfg(target_os = "linux")]
    let before = proc_io();

    let tree = Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default()).open()?;
    let strategy = Arc::new(Leveled::default());
    let settle = |tree: &AnyTree| -> lsm_tree::Result<()> {
        tree.flush_active_memtable(seqno.get())?;
        while tree.compact(strategy.clone(), seqno.get())?.action != CompactionAction::Nothing {}
        Ok(())
    };
    for second in 0..HOURS * 3_600 {
        for sensor in 0..SENSORS {
            tree.insert(
                order.key(second, sensor),
                reading(second, sensor),
                seqno.next(),
            );
        }
        if (second + 1) % FLUSH_EVERY_SECONDS == 0 {
            settle(&tree)?;
        }
        // Once an hour completes, the hour that fell out of the retention
        // goes.
        if (second + 1) % 3_600 == 0
            && let Some(expired) = ((second + 1) / 3_600).checked_sub(RETENTION_HOURS + 1)
        {
            order.expire(&tree, expired, &seqno);
            settle(&tree)?;
        }
    }
    let reading = tree.write_io();
    // Read before anything is printed: the process's own output is a write
    // too.
    #[cfg(target_os = "linux")]
    let after = proc_io();
    let rows = tree.iter(SeqNo::MAX, None).count();
    drop(tree);

    println!(
        "\n{}: {SENSORS} sensors x {HOURS} h at 1 Hz, flushed every {FLUSH_EVERY_SECONDS} s, \
         {RETENTION_HOURS} h retained ({rows} rows left)\n",
        order.name(),
    );
    print(order, &reading);

    #[cfg(target_os = "linux")]
    {
        let delta = |field: &str| {
            after.get(field).copied().unwrap_or(0) - before.get(field).copied().unwrap_or(0)
        };
        let files = reading.files();
        println!(
            "\n/proc/self/io over the run: wchar {} against {} ledger bytes, syscw {} against \
             {} ledger write calls, write_bytes {}",
            delta("wchar"),
            files.bytes,
            delta("syscw"),
            files.writes,
            delta("write_bytes"),
        );
    }
    Ok(())
}

fn main() -> lsm_tree::Result<()> {
    let only = std::env::var("LEDGER_ORDER").ok();
    for order in [Order::Time, Order::Sensor] {
        if only.as_deref().is_some_and(|name| name != order.name()) {
            continue;
        }
        run(order)?;
    }
    Ok(())
}
