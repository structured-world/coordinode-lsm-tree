// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Rolling retention of time-bucketed keys on a real filesystem, the one
//! under `TMPDIR`, in three layouts of the same data:
//!
//! - `one-tree`: one tree, outputs cut by size only;
//! - `one-tree-domains`: one tree whose outputs end at each epoch's end
//!   ([`lsm_tree::KeyDomains`]);
//! - `tree-per-epoch`: a tree of its own for each epoch.
//!
//! Every epoch writes its rows in time order across the series, and a share
//! of them arrives late, during the next epoch. Once an epoch is older than
//! the retention, it expires: in one tree, a range tombstone over it that the
//! next compaction acts on; per epoch, its tree is dropped and removed.
//!
//! Per layout it prints the bytes the process wrote (`/proc/self/io`, the
//! filesystem's view, not the device's), the allocated bytes at their peak
//! and at the end, the tables and open descriptors at the end, the time to
//! reopen, and the wall time. It checks that every retained
//! row reads back and no expired one does.
//!
//! Linux only: the counters come from `/proc`.

#[cfg(target_os = "linux")]
fn main() -> lsm_tree::Result<()> {
    cases::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("key_domains_retention: the counters need Linux /proc");
}

#[cfg(target_os = "linux")]
mod cases {
    use lsm_tree::compaction::{CompactionAction, Leveled};
    use lsm_tree::{
        AbstractTree, AnyTree, Config, Guard as _, KeyDomains, SeqNo, SequenceNumberCounter,
        UserKey,
    };
    use std::collections::BTreeMap;
    use std::os::unix::fs::MetadataExt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Instant;

    /// One shape of the workload.
    struct Case {
        name: &'static str,
        epochs: u32,
        rows_per_epoch: u32,
        retention: u32,
        series: u32,
        /// Rows per thousand that arrive during the next epoch.
        late_permille: u32,
    }

    const CASES: &[Case] = &[
        Case {
            name: "many-small",
            epochs: 48,
            rows_per_epoch: 20_000,
            retention: 12,
            series: 64,
            late_permille: 10,
        },
        Case {
            name: "few-large",
            epochs: 8,
            rows_per_epoch: 120_000,
            retention: 2,
            series: 64,
            late_permille: 10,
        },
    ];

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Layout {
        OneTree,
        OneTreeDomains,
        TreePerEpoch,
    }

    impl Layout {
        const ALL: [Self; 3] = [Self::OneTree, Self::OneTreeDomains, Self::TreePerEpoch];

        fn name(self) -> &'static str {
            match self {
                Self::OneTree => "one-tree",
                Self::OneTreeDomains => "one-tree-domains",
                Self::TreePerEpoch => "tree-per-epoch",
            }
        }
    }

    /// The memtable size a tree flushes at.
    const FLUSH_AT: u64 = 4 * 1_024 * 1_024;

    /// One domain per epoch, the key's first four bytes.
    struct Epochs;

    impl KeyDomains for Epochs {
        fn domain_end(&self, key: &[u8]) -> Option<UserKey> {
            let epoch = u32::from_be_bytes(key.get(..4)?.try_into().ok()?);
            Some(UserKey::from(
                epoch.checked_add(1)?.to_be_bytes().as_slice(),
            ))
        }
    }

    fn key(epoch: u32, series: u32, seq: u32) -> [u8; 12] {
        let mut key = [0; 12];
        key[..4].copy_from_slice(&epoch.to_be_bytes());
        key[4..8].copy_from_slice(&series.to_be_bytes());
        key[8..].copy_from_slice(&seq.to_be_bytes());
        key
    }

    fn epoch_start(epoch: u32) -> Vec<u8> {
        epoch.to_be_bytes().to_vec()
    }

    /// 100 bytes no codec shrinks much, the same for the same row.
    fn value(epoch: u32, series: u32, seq: u32) -> [u8; 100] {
        let mut s = (u64::from(epoch) << 40 ^ u64::from(series) << 20 ^ u64::from(seq))
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            | 1;
        let mut out = [0; 100];
        for byte in &mut out {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            *byte = (s >> 56) as u8;
        }
        out
    }

    /// A `/proc/self/<file>` field, by its label.
    fn proc_field(file: &str, label: &str) -> u64 {
        std::fs::read_to_string(format!("/proc/self/{file}"))
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    let rest = line.strip_prefix(label)?;
                    rest.split_whitespace().next()?.parse().ok()
                })
            })
            .unwrap_or(0)
    }

    fn written_bytes() -> u64 {
        proc_field("io", "write_bytes:")
    }

    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count)
    }

    /// The bytes allocated to every file under `dir`.
    fn allocated(dir: &Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .map(|entry| match entry.metadata() {
                Ok(meta) if meta.is_dir() => allocated(&entry.path()),
                // `st_blocks` is in 512-byte units.
                Ok(meta) => meta.blocks() * 512,
                Err(_) => 0,
            })
            .sum()
    }

    /// The trees of one layout and how the rows route to them.
    struct Store {
        layout: Layout,
        root: PathBuf,
        seqno: SequenceNumberCounter,
        visible: SequenceNumberCounter,
        /// Every open tree, by the epoch it holds; `u32::MAX` for the one tree
        /// of a single-tree layout.
        trees: BTreeMap<u32, AnyTree>,
    }

    impl Store {
        fn open_tree(&self, epoch: u32) -> lsm_tree::Result<AnyTree> {
            let path = if self.layout == Layout::TreePerEpoch {
                self.root.join(format!("epoch-{epoch}"))
            } else {
                self.root.join("tree")
            };
            let mut config = Config::new(path, self.seqno.clone(), self.visible.clone());
            if self.layout == Layout::OneTreeDomains {
                config = config.key_domains(Arc::new(Epochs));
            }
            config.open()
        }

        fn new(layout: Layout, root: PathBuf) -> lsm_tree::Result<Self> {
            let mut store = Self {
                layout,
                root,
                seqno: SequenceNumberCounter::default(),
                visible: SequenceNumberCounter::default(),
                trees: BTreeMap::new(),
            };
            if layout != Layout::TreePerEpoch {
                let tree = store.open_tree(u32::MAX)?;
                store.trees.insert(u32::MAX, tree);
            }
            Ok(store)
        }

        fn tree_for(&mut self, epoch: u32) -> lsm_tree::Result<&AnyTree> {
            let slot = if self.layout == Layout::TreePerEpoch {
                epoch
            } else {
                u32::MAX
            };
            if !self.trees.contains_key(&slot) {
                let tree = self.open_tree(epoch)?;
                self.trees.insert(slot, tree);
            }
            Ok(&self.trees[&slot])
        }

        fn insert(&mut self, epoch: u32, row: u32, series: u32) -> lsm_tree::Result<()> {
            let (series_id, seq) = (row % series, row / series);
            let seqno = self.seqno.next();
            let tree = self.tree_for(epoch)?.clone();
            tree.insert(
                key(epoch, series_id, seq),
                value(epoch, series_id, seq),
                seqno,
            );
            self.visible.fetch_max(seqno + 1);
            if tree.active_memtable().size() >= FLUSH_AT {
                self.flush_and_compact(&tree)?;
            }
            Ok(())
        }

        fn flush_and_compact(&self, tree: &AnyTree) -> lsm_tree::Result<()> {
            let watermark = self.seqno.get();
            tree.flush_active_memtable(watermark)?;
            let strategy = Arc::new(Leveled::default());
            while tree.compact(strategy.clone(), watermark)?.action != CompactionAction::Nothing {}
            Ok(())
        }

        /// Writes the rest of what an epoch's tree holds in memory.
        fn settle(&self, epoch: u32) -> lsm_tree::Result<()> {
            let slot = if self.layout == Layout::TreePerEpoch {
                epoch
            } else {
                u32::MAX
            };
            if let Some(tree) = self.trees.get(&slot) {
                self.flush_and_compact(tree)?;
            }
            Ok(())
        }

        fn expire(&mut self, epoch: u32) -> lsm_tree::Result<()> {
            if self.layout == Layout::TreePerEpoch {
                if let Some(tree) = self.trees.remove(&epoch) {
                    drop(tree);
                    std::fs::remove_dir_all(self.root.join(format!("epoch-{epoch}")))?;
                }
                return Ok(());
            }
            let seqno = self.seqno.next();
            let tree = self.trees[&u32::MAX].clone();
            tree.remove_range(epoch_start(epoch), epoch_start(epoch + 1), seqno);
            self.visible.fetch_max(seqno + 1);
            self.flush_and_compact(&tree)
        }

        fn tables(&self) -> usize {
            self.trees.values().map(AbstractTree::table_count).sum()
        }

        /// The rows that read back, by epoch.
        fn rows(&self) -> lsm_tree::Result<BTreeMap<u32, u64>> {
            let mut rows = BTreeMap::new();
            for tree in self.trees.values() {
                for item in tree.iter(SeqNo::MAX, None) {
                    let key = item.key()?;
                    let epoch = u32::from_be_bytes(key[..4].try_into().expect("epoch"));
                    *rows.entry(epoch).or_insert(0) += 1;
                }
            }
            Ok(rows)
        }

        fn reads(&mut self, epoch: u32, series: u32, seq: u32) -> lsm_tree::Result<bool> {
            let tree = self.tree_for(epoch)?;
            Ok(tree.get(key(epoch, series, seq), SeqNo::MAX)?.is_some())
        }
    }

    struct Outcome {
        written: u64,
        peak: u64,
        end: u64,
        tables: usize,
        fds: usize,
        reopen_ms: f64,
        wall_s: f64,
    }

    fn run_case(case: &Case, layout: Layout) -> lsm_tree::Result<Outcome> {
        let dir = tempfile::tempdir()?;
        let fds_before = open_fds();
        let written_before = written_bytes();
        let started = Instant::now();
        let mut store = Store::new(layout, dir.path().to_path_buf())?;
        // Every `late_every`-th row of an epoch, the last of each run, arrives
        // during the next epoch instead, one with each run there.
        let late_every = 1_000 / case.late_permille.max(1);
        let mut peak = 0;
        for epoch in 0..case.epochs {
            if let Some(expired) = epoch.checked_sub(case.retention + 1) {
                store.expire(expired)?;
            }
            let delays = epoch + 1 < case.epochs;
            for t in 0..case.rows_per_epoch {
                if t % late_every == 0
                    && let Some(previous) = epoch.checked_sub(1)
                {
                    let late = t + late_every - 1;
                    if late < case.rows_per_epoch {
                        store.insert(previous, late, case.series)?;
                    }
                }
                if delays && t % late_every == late_every - 1 {
                    continue;
                }
                store.insert(epoch, t, case.series)?;
            }
            if let Some(previous) = epoch.checked_sub(1) {
                store.settle(previous)?;
            }
            peak = peak.max(allocated(dir.path()));
        }
        store.settle(case.epochs - 1)?;
        peak = peak.max(allocated(dir.path()));
        let wall_s = started.elapsed().as_secs_f64();
        let written = written_bytes() - written_before;
        let end = allocated(dir.path());
        let tables = store.tables();
        let fds = open_fds() - fds_before;

        // Every retained row, and none of an expired epoch.
        let first_kept = case.epochs.saturating_sub(case.retention + 1);
        let expected: BTreeMap<u32, u64> = (first_kept..case.epochs)
            .map(|epoch| (epoch, u64::from(case.rows_per_epoch)))
            .collect();
        let rows = store.rows()?;
        assert_eq!(
            rows,
            expected,
            "{} {}: retained rows by epoch",
            case.name,
            layout.name(),
        );
        // A dropped epoch tree has nothing to read: the single-tree layouts
        // are the ones a range tombstone has to hide.
        if let Some(expired) = first_kept.checked_sub(1)
            && layout != Layout::TreePerEpoch
        {
            assert!(
                !store.reads(expired, 0, 0)?,
                "{} {}: an expired row reads",
                case.name,
                layout.name(),
            );
        }

        let epochs: Vec<u32> = store.trees.keys().copied().collect();
        drop(store);
        let reopen = Instant::now();
        let mut reopened = Store::new(layout, dir.path().to_path_buf())?;
        if layout == Layout::TreePerEpoch {
            for epoch in epochs {
                reopened.tree_for(epoch)?;
            }
        }
        let reopen_ms = reopen.elapsed().as_secs_f64() * 1e3;
        drop(reopened);

        Ok(Outcome {
            written,
            peak,
            end,
            tables,
            fds,
            reopen_ms,
            wall_s,
        })
    }

    pub fn run() -> lsm_tree::Result<()> {
        let mib = |b: u64| b as f64 / (1_024.0 * 1_024.0);
        let only = std::env::var("KD_CASE").ok();
        for case in CASES {
            if only.as_deref().is_some_and(|name| name != case.name) {
                continue;
            }
            let payload = u64::from(case.epochs) * u64::from(case.rows_per_epoch) * 112;
            println!(
                "\n{}: {} epochs x {} rows, retention {}, {} late per mille, payload {:.1} MiB",
                case.name,
                case.epochs,
                case.rows_per_epoch,
                case.retention,
                case.late_permille,
                mib(payload),
            );
            println!(
                "{:<18} {:>11} {:>8} {:>10} {:>9} {:>7} {:>5} {:>10} {:>7}",
                "layout",
                "written MiB",
                "WA",
                "peak MiB",
                "end MiB",
                "tables",
                "fds",
                "reopen ms",
                "wall s",
            );
            let only_layout = std::env::var("KD_LAYOUT").ok();
            for layout in Layout::ALL {
                if only_layout
                    .as_deref()
                    .is_some_and(|name| name != layout.name())
                {
                    continue;
                }
                let o = run_case(case, layout)?;
                println!(
                    "{:<18} {:>11.1} {:>8.2} {:>10.1} {:>9.1} {:>7} {:>5} {:>10.1} {:>7.2}",
                    layout.name(),
                    mib(o.written),
                    o.written as f64 / payload as f64,
                    mib(o.peak),
                    mib(o.end),
                    o.tables,
                    o.fds,
                    o.reopen_ms,
                    o.wall_s,
                );
            }
        }
        Ok(())
    }
}
