use super::*;

/// Keys and values in key order.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// The routes as the 5.x crate takes them.
fn v5_routes(routes: &[LevelRoute]) -> Vec<lsm5::config::LevelRoute> {
    routes
        .iter()
        .map(|route| lsm5::config::LevelRoute {
            levels: route.levels.clone(),
            path: route.path.clone(),
            fs: Arc::new(lsm5::fs::StdFs),
        })
        .collect()
}

/// Every key and value a 5.x store at `folder` holds, newest versions, its
/// levels placed by `routes`.
fn read_v5(folder: &Path, routes: &[LevelRoute]) -> lsm5::Result<Entries> {
    use lsm5::{AbstractTree as _, Guard as _};
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .level_routes(v5_routes(routes))
    .open()?;
    tree.range::<&[u8], _>(.., u64::MAX, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect()
}

/// [`read_v5`] for a 6.0 store whose levels `routes` place elsewhere.
fn read_v6(folder: &Path, routes: &[LevelRoute]) -> lsm6::Result<Entries> {
    use lsm6::{AbstractTree as _, Guard as _};
    let tree = lsm6::Config::new(
        folder,
        lsm6::SequenceNumberCounter::default(),
        lsm6::SequenceNumberCounter::default(),
    )
    .level_routes(
        routes
            .iter()
            .map(|route| lsm6::config::LevelRoute {
                levels: route.levels.clone(),
                path: route.path.clone(),
                fs: Arc::new(lsm6::fs::StdFs),
            })
            .collect(),
    )
    .open()?;
    tree.range::<&[u8], _>(.., u64::MAX, None)
        .map(|guard| {
            let (key, value) = guard.into_inner()?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect()
}

/// A small 5.x store over two flushes, and what it holds. Under `routes` a
/// compaction moves it below L0 first, into the route's folder.
fn small_store(
    folder: &Path,
    routes: &[LevelRoute],
) -> Result<Entries, Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .level_routes(v5_routes(routes))
    .open()?;
    for round in 0..2u64 {
        for i in 0..200u64 {
            tree.insert(format!("k{i:04}"), format!("v{round}"), round * 200 + i + 1);
        }
        tree.flush_active_memtable(0)?;
        if !routes.is_empty() && round == 0 {
            tree.major_compact(64 * 1024 * 1024, 0)?;
        }
    }
    drop(tree);
    Ok(read_v5(folder, routes)?)
}

/// A switch stopped before each of its changes in turn is finished by the
/// next run, whichever change it stopped at: the folder ends up the converted
/// store holding the source's data, the backup the source, and nothing of the
/// conversion is left behind. Stops before the converted store is complete
/// start over from the source; the rest roll forward. A store whose lower
/// levels a route places in another folder switches that folder's tables
/// with it.
#[test]
fn an_interrupted_switch_is_finished_by_the_next_run() -> Result<(), Box<dyn std::error::Error>> {
    for routed in [false, true] {
        let mut stop_at = 0;
        loop {
            let folder = tempfile::tempdir()?;
            let cold = tempfile::tempdir()?;
            let routes = if routed {
                vec![LevelRoute {
                    levels: 1..7,
                    path: cold.path().to_path_buf(),
                }]
            } else {
                Vec::new()
            };
            let options = Options {
                level_routes: routes.clone(),
                ..Options::default()
            };
            let expected = small_store(folder.path(), &routes)?;
            if routed {
                assert!(
                    std::fs::read_dir(cold.path().join(lsm5::file::TABLES_FOLDER))?
                        .next()
                        .is_some(),
                    "the route holds tables"
                );
            }
            prepare(folder.path(), &options)?;

            let mut steps = 0;
            let sites = route_sites(folder.path(), &routes);
            let interrupted = switch(folder.path(), &sites, &mut || {
                steps += 1;
                if steps > stop_at {
                    Err(std::io::Error::other("interrupted"))
                } else {
                    Ok(())
                }
            });
            let finished_unbroken = interrupted.is_ok();

            if !finished_unbroken {
                let report = convert(folder.path(), &options)?;
                assert_eq!(
                    report.resumed,
                    stop_at > 0,
                    "a run after the ready marker resumes the switch (stopped at {stop_at})"
                );
            }
            let at = format!("stopped at {stop_at}, routed {routed}");
            assert_eq!(read_v6(folder.path(), &routes)?, expected, "{at}");
            // The backup opens as the source, its routed tables in the route
            // folder's backup.
            let backup_routes: Vec<LevelRoute> = routes
                .iter()
                .map(|route| LevelRoute {
                    levels: route.levels.clone(),
                    path: route.path.join(BACKUP),
                })
                .collect();
            assert_eq!(
                read_v5(&folder.path().join(BACKUP), &backup_routes)?,
                expected,
                "{at}"
            );
            for base in [folder.path(), cold.path()] {
                for leftover in [STAGING, READY, SWAPPING] {
                    assert!(!base.join(leftover).exists(), "{leftover} is gone ({at})");
                }
            }

            if finished_unbroken {
                assert!(stop_at > 3, "the switch takes several steps");
                break;
            }
            stop_at += 1;
        }
    }
    Ok(())
}

/// A store whose folder holds the backup of an earlier conversion is not
/// converted over it: the switch would mix two sources in one backup.
#[test]
fn a_store_holding_an_earlier_backup_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    small_store(folder.path(), &[])?;
    std::fs::create_dir(folder.path().join(BACKUP))?;
    std::fs::write(folder.path().join(BACKUP).join("current"), b"x")?;
    assert!(matches!(
        convert(folder.path(), &Options::default()),
        Err(Error::Unsupported(_))
    ));
    assert!(!folder.path().join(STAGING).exists(), "nothing was built");
    Ok(())
}

/// The store's own entries are recognized by name, and nothing else in the
/// folder is: the lock, the conversion's own entries and foreign files stay.
#[test]
fn store_entries_are_the_pointer_the_manifest_and_the_file_folders() {
    for name in [
        "current", "v0", "v17", "edits-0", "edits-17", "tables", "blobs", "dicts",
    ] {
        assert!(is_store_entry(name), "{name}");
    }
    for name in [
        "LOCK",
        "v",
        "edits-",
        "v6-convert",
        "v5-backup",
        "v6-convert.ready",
        "v6-convert.swapping",
        "v1.tmp",
        "notes.txt",
    ] {
        assert!(!is_store_entry(name), "{name}");
    }
}

/// A reserved field id takes the highest field id no field uses, never an
/// intrinsic column's: a store whose fields take every field id but the
/// reserved ones is refused rather than given the value-type column.
#[test]
fn renumbering_takes_only_free_field_ids() {
    use lsm6::blob_tree::field_row::{FIRST_FIELD_COLUMN, RESERVED_COLUMNS};
    let used: std::collections::BTreeSet<u16> = [5, RESERVED_COLUMNS - 1, u16::MAX].into();
    assert_eq!(
        renumber_into_free(&used).ok(),
        Some(vec![(u16::MAX, RESERVED_COLUMNS - 2)])
    );

    let full: std::collections::BTreeSet<u16> = (FIRST_FIELD_COLUMN..RESERVED_COLUMNS)
        .chain([u16::MAX])
        .collect();
    assert!(matches!(
        renumber_into_free(&full),
        Err(Error::Unsupported(_))
    ));
}
