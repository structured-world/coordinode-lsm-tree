//! What opening a consumer's set of trees costs at startup: twenty trees in
//! one data directory, made fresh (cold) and opened again (warm), one by one
//! and as one batch, on the real file system, under the default sync mode and
//! under full syncs.
//!
//! Each line reports the P50 and the slowest of the wall times over the
//! rounds, and the directory syncs a round issued, those of the data
//! directory apart. The data directory is made under `OPEN_MANY_DIR` when it
//! is set, so the file system under test can be chosen (ext4, XFS, APFS);
//! otherwise under the system's temporary directory. Run it on a quiet host.

use lsm_tree::fs::{FaultFs, Fs, StdFs, SyncKind, SyncMode};
use lsm_tree::{AnyTree, Config, SequenceNumberCounter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Trees a consumer opens at startup, about as many as `CoordiNode` does.
const TREES: usize = 20;
const ROUNDS: usize = 15;

/// The configs of the trees in `data`, over `fs`, syncing as `mode` says.
fn configs(data: &Path, fs: &Arc<FaultFs<StdFs>>, mode: SyncMode) -> Vec<Config> {
    (0..TREES)
        .map(|i| {
            Config::new(
                data.join(format!("tree{i:02}")),
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .with_shared_fs(fs.clone())
            .sync_mode(mode)
        })
        .collect()
}

fn open(
    data: &Path,
    fs: &Arc<FaultFs<StdFs>>,
    mode: SyncMode,
    batched: bool,
) -> lsm_tree::Result<Vec<AnyTree>> {
    if batched {
        Config::open_many(configs(data, fs, mode))
    } else {
        configs(data, fs, mode)
            .into_iter()
            .map(Config::open)
            .collect()
    }
}

/// Prints the P50 and the slowest of `times`, and the directory syncs per
/// round, of `data` and of all others.
fn report(label: &str, mut times: Vec<Duration>, data_syncs: usize, other_syncs: usize) {
    times.sort();
    let p50 = times[times.len() / 2];
    let max = times[times.len() - 1];
    eprintln!(
        "  [{label}] n={} P50={p50:?} max={max:?} data_dir_syncs={data_syncs} other_dir_syncs={other_syncs}",
        times.len()
    );
}

/// The directory syncs `fs` saw since the last call: of `data`, and of every
/// other directory.
fn directory_syncs(fs: &FaultFs<StdFs>, data: &Path) -> (usize, usize) {
    let syncs = fs.injector().take_syncs();
    let of_data = syncs
        .iter()
        .filter(|(path, kind)| *kind == SyncKind::Directory && path == data)
        .count();
    let others = syncs
        .iter()
        .filter(|(path, kind)| *kind == SyncKind::Directory && path != data)
        .count();
    (of_data, others)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root: PathBuf =
        std::env::var_os("OPEN_MANY_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
    let base = std::path::absolute(root.join(format!("open-many-{}", std::process::id())))?;
    let fs = Arc::new(FaultFs::new(StdFs));
    eprintln!("[open_many] {} trees under {}", TREES, base.display());

    for mode in [SyncMode::Normal, SyncMode::Full] {
        // Per arm, single then batch: the cold and warm times, and the last
        // round's syncs of each.
        let mut times = [[Vec::new(), Vec::new()], [Vec::new(), Vec::new()]];
        let mut census = [[(0, 0); 2]; 2];
        // The arms take turns within a round, so a drift of the host lands
        // on both.
        for round in 0..ROUNDS {
            for (arm, batched) in [false, true].into_iter().enumerate() {
                let data = base.join(format!("{mode:?}-{round}-{arm}"));
                fs.create_dir_all(&data)?;
                drop(fs.injector().take_syncs());
                for phase in 0..2 {
                    let start = Instant::now();
                    drop(open(&data, &fs, mode, batched)?);
                    times[arm][phase].push(start.elapsed());
                    census[arm][phase] = directory_syncs(&fs, &data);
                }
                fs.remove_dir_all(&data)?;
            }
        }
        for (arm, name) in ["single", "batch"].into_iter().enumerate() {
            for (phase, when) in ["cold", "warm"].into_iter().enumerate() {
                let (data_syncs, other_syncs) = census[arm][phase];
                report(
                    &format!("{mode:?}/{name}/{when}"),
                    core::mem::take(&mut times[arm][phase]),
                    data_syncs,
                    other_syncs,
                );
            }
        }
    }
    fs.remove_dir_all(&base)?;
    Ok(())
}
