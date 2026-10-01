//! Under `SyncMode::Barrier` a flush keeps its writes in order and makes them
//! durable once the devices are flushed: a power loss before that keeps an
//! ordered prefix of the flushes on each device, never a manifest naming a
//! table that did not survive, and loses nothing synced before a device flush.

use lsm_tree::config::LevelRoute;
use lsm_tree::fs::{CrashFs, Fs, MemFs, SyncMode};
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
use std::path::PathBuf;
use std::sync::Arc;

const KEYS: usize = 4;

fn key(i: usize) -> String {
    format!("k{i:02}")
}

/// The tree folder, absolute as the engine makes every configured path.
fn base() -> PathBuf {
    std::path::absolute("/db_barrier").expect("absolute base")
}

/// A tree under `SyncMode::Barrier` on `main`, its levels on `hot` when given.
fn open(main: Arc<dyn Fs>, hot: Option<Arc<dyn Fs>>) -> lsm_tree::Result<AnyTree> {
    let base = base();
    let config = Config::new(
        &base,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(main)
    .sync_mode(SyncMode::Barrier);
    match hot {
        Some(fs) => config.level_routes(vec![LevelRoute {
            levels: 0..7,
            path: base.join("hot"),
            fs,
        }]),
        None => config,
    }
    .open()
}

/// Creates the tree and makes it durable, then flushes `KEYS` keys one by
/// one, each flush ordered but not flushed to the devices.
fn flushes(main: &CrashFs, hot: Option<&CrashFs>) -> lsm_tree::Result<AnyTree> {
    let hot = hot.map(|hot| Arc::new(hot.clone()) as Arc<dyn Fs>);
    let tree = open(Arc::new(main.clone()), hot)?;
    tree.sync_devices()?;
    for i in 0..KEYS {
        tree.insert(key(i), "v", i as u64);
        tree.flush_active_memtable(0)?;
    }
    Ok(tree)
}

/// Reopens the tree on what survived and returns how many of the keys, from
/// the first, it holds, asserting it holds no other.
fn surviving_prefix(main: &CrashFs, hot: Option<&CrashFs>, what: &str) -> lsm_tree::Result<usize> {
    let tree = open(main.inner(), hot.map(CrashFs::inner))
        .unwrap_or_else(|e| panic!("{what}: the tree does not reopen: {e}"));
    let mut prefix = 0;
    while prefix < KEYS && tree.contains_key(key(prefix), u64::MAX)? {
        prefix += 1;
    }
    for i in prefix..KEYS {
        assert!(
            !tree.contains_key(key(i), u64::MAX)?,
            "{what}: key {i} survived without key {prefix}"
        );
    }
    Ok(prefix)
}

/// Every prefix of the ordered syncs on one device is a tree that reopens
/// with a prefix of the flushes, and a device flush keeps them all.
#[test]
fn a_power_loss_keeps_an_ordered_prefix_of_barrier_flushes() -> lsm_tree::Result<()> {
    let probe = CrashFs::new(MemFs::new());
    drop(flushes(&probe, None)?);
    let ordered = probe.ordered_syncs();
    assert!(ordered > 0, "the flushes are ordered, not yet durable");

    for kept in 0..=ordered {
        let main = CrashFs::new(MemFs::new());
        drop(flushes(&main, None)?);
        main.crash_keeping(kept);
        surviving_prefix(&main, None, &format!("{kept} of {ordered} kept"))?;
    }

    let main = CrashFs::new(MemFs::new());
    let tree = flushes(&main, None)?;
    tree.sync_devices()?;
    drop(tree);
    main.crash();
    assert_eq!(
        surviving_prefix(&main, None, "after a device flush")?,
        KEYS,
        "nothing synced before a device flush is lost"
    );
    Ok(())
}

/// With the tables on one device and the manifest on another, every pair of
/// prefixes the two devices can keep is a tree that reopens: the manifest
/// never names a table its device lost.
#[test]
fn a_flush_across_two_devices_never_names_a_lost_table() -> lsm_tree::Result<()> {
    let (probe_main, probe_hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
    drop(flushes(&probe_main, Some(&probe_hot))?);
    let (ordered_main, ordered_hot) = (probe_main.ordered_syncs(), probe_hot.ordered_syncs());

    for kept_main in 0..=ordered_main {
        for kept_hot in 0..=ordered_hot {
            let (main, hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
            drop(flushes(&main, Some(&hot))?);
            main.crash_keeping(kept_main);
            hot.crash_keeping(kept_hot);
            surviving_prefix(
                &main,
                Some(&hot),
                &format!(
                    "manifest device {kept_main} of {ordered_main} kept, \
                     table device {kept_hot} of {ordered_hot} kept"
                ),
            )?;
        }
    }

    let (main, hot) = (CrashFs::new(MemFs::new()), CrashFs::new(MemFs::new()));
    let tree = flushes(&main, Some(&hot))?;
    tree.sync_devices()?;
    drop(tree);
    main.crash();
    hot.crash();
    assert_eq!(
        surviving_prefix(&main, Some(&hot), "after a device flush")?,
        KEYS,
        "nothing synced before a device flush is lost on either device"
    );
    Ok(())
}
