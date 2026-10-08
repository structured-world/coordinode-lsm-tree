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
            // A renumbering the switch stands for, which a run that only
            // finishes it must still report.
            let renumbered = [(u16::MAX, 65_000)];
            let interrupted = switch(folder.path(), &sites, &renumbered, &mut || {
                steps += 1;
                if steps > stop_at {
                    Err(std::io::Error::other("interrupted"))
                } else {
                    Ok(())
                }
            });
            let finished_unbroken = interrupted.is_ok();

            // A switch stopped with neither pointer in the folder: a tree
            // opened there is refused rather than started fresh over it.
            if !folder
                .path()
                .join(lsm5::file::CURRENT_VERSION_FILE)
                .exists()
            {
                let v6 = lsm6::Config::new(
                    folder.path(),
                    lsm6::SequenceNumberCounter::default(),
                    lsm6::SequenceNumberCounter::default(),
                )
                .open();
                let v5 = lsm5::Config::new(
                    folder.path(),
                    lsm5::SequenceNumberCounter::default(),
                    lsm5::SequenceNumberCounter::default(),
                )
                .open();
                assert!(
                    v6.is_err() && v5.is_err(),
                    "no tree is created mid-switch (stopped at {stop_at})"
                );
            }

            if !finished_unbroken {
                let report = convert(folder.path(), &options)?;
                assert_eq!(
                    report.resumed,
                    stop_at > 0,
                    "a run after the ready marker resumes the switch (stopped at {stop_at})"
                );
                if report.resumed {
                    assert_eq!(
                        report.renumbered_fields, renumbered,
                        "a resumed switch reports the renumbering (stopped at {stop_at})"
                    );
                }
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
                for leftover in [STAGING, READY, SWAPPING, GUARD] {
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

/// Runs `prepare` and the switch up to its ready marker, as a run stopped
/// right after it does.
fn stop_after_ready(folder: &Path, options: &Options) -> Result<(), Box<dyn std::error::Error>> {
    let report = prepare(folder, options)?;
    let sites = route_sites(folder, &options.level_routes);
    let mut steps = 0;
    let stopped = switch(folder, &sites, &report.renumbered_fields, &mut || {
        steps += 1;
        if steps > 1 {
            Err(std::io::Error::other("interrupted"))
        } else {
            Ok(())
        }
    });
    assert!(stopped.is_err() && folder.join(READY).exists());
    Ok(())
}

/// A tree that writes to the source between a stopped switch and the run
/// that would resume it: the converted store is built again from the source
/// as it now is, instead of switching in one that lacks the write.
#[test]
fn a_source_written_after_the_ready_marker_is_converted_again()
-> Result<(), Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    let folder = tempfile::tempdir()?;
    small_store(folder.path(), &[])?;
    stop_after_ready(folder.path(), &Options::default())?;
    {
        let tree = lsm5::Config::new(
            folder.path(),
            lsm5::SequenceNumberCounter::default(),
            lsm5::SequenceNumberCounter::default(),
        )
        .open()?;
        tree.insert("late", "write", 1_000);
        tree.flush_active_memtable(0)?;
    }
    let expected = read_v5(folder.path(), &[])?;
    assert!(expected.iter().any(|(k, _)| k == b"late"));
    assert!(
        folder.path().join(READY).exists(),
        "the 5.x tree kept the stopped switch's marker"
    );

    let report = convert(folder.path(), &Options::default())?;
    assert!(!report.resumed, "the stale switch is not resumed");
    assert_eq!(read_v6(folder.path(), &[])?, expected);
    assert_eq!(read_v5(&folder.path().join(BACKUP), &[])?, expected);
    Ok(())
}

/// A switch stopped with a level route resumes only with that route: a run
/// that omits it is refused before it moves anything, and the run that names
/// it again finishes the switch, the routed tables in place.
#[test]
fn a_switch_resumes_only_with_the_routes_it_started_with() -> Result<(), Box<dyn std::error::Error>>
{
    let folder = tempfile::tempdir()?;
    let cold = tempfile::tempdir()?;
    let routes = vec![LevelRoute {
        levels: 1..7,
        path: cold.path().to_path_buf(),
    }];
    let options = Options {
        level_routes: routes.clone(),
        ..Options::default()
    };
    let expected = small_store(folder.path(), &routes)?;
    stop_after_ready(folder.path(), &options)?;

    assert!(matches!(
        convert(folder.path(), &Options::default()),
        Err(Error::Unsupported(_))
    ));
    assert!(folder.path().join(READY).exists(), "nothing was moved");

    assert!(convert(folder.path(), &options)?.resumed);
    assert_eq!(read_v6(folder.path(), &routes)?, expected);
    Ok(())
}

/// A route folder inside a folder the switch moves whole is refused before
/// anything is built: it would move with its ancestor, and the converted
/// store would miss its tables.
#[test]
fn a_route_inside_a_folder_the_switch_moves_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    small_store(folder.path(), &[])?;
    for nested in [lsm5::file::TABLES_FOLDER, BACKUP] {
        let options = Options {
            level_routes: vec![LevelRoute {
                levels: 1..7,
                path: folder.path().join(nested).join("cold"),
            }],
            ..Options::default()
        };
        assert!(
            matches!(convert(folder.path(), &options), Err(Error::Unsupported(_))),
            "a route in {nested}"
        );
        assert!(
            !folder.path().join(STAGING).exists() && !folder.path().join(READY).exists(),
            "nothing was built for a route in {nested}"
        );
    }
    Ok(())
}

/// A route naming the store's own folder, spelled otherwise, is the store's
/// folder: its levels convert in place like the rest, not as a route folder
/// whose staging is the store's own.
#[test]
fn a_route_naming_the_store_folder_converts_in_place() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    // Through a folder the switch does not move, so the alias resolves to the
    // store's folder all along.
    std::fs::create_dir(folder.path().join("alias"))?;
    let routes = vec![LevelRoute {
        levels: 1..7,
        path: folder.path().join("alias").join(".."),
    }];
    // Written under the route, so a compaction places tables at a routed level.
    let expected = small_store(folder.path(), &routes)?;
    let options = Options {
        level_routes: routes,
        ..Options::default()
    };
    convert(folder.path(), &options)?;
    assert_eq!(read_v6(folder.path(), &[])?, expected);
    assert_eq!(read_v5(&folder.path().join(BACKUP), &[])?, expected);
    Ok(())
}

/// A relative route names its folder from the working directory: a stopped
/// switch resumes with the same folder only, however the option is spelled,
/// and a rerun from elsewhere with the same text is refused before it moves
/// anything. The working directory is the process's: each test runs alone in
/// its own process under nextest.
#[test]
fn a_switch_resumes_with_a_relative_route_only_from_its_folder()
-> Result<(), Box<dyn std::error::Error>> {
    /// Puts the working directory back when the test ends, a panic included,
    /// and before the temporary folder it was moved into is removed.
    struct Restore(PathBuf);
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Err(e) = std::env::set_current_dir(&self.0) {
                eprintln!("restoring {}: {e}", self.0.display());
            }
        }
    }
    let base = tempfile::tempdir()?;
    let _restore = Restore(std::env::current_dir()?);
    let folder = base.path().join("store");
    for cwd in ["one", "two"] {
        std::fs::create_dir_all(base.path().join(cwd).join("cold"))?;
    }
    std::env::set_current_dir(base.path().join("one"))?;
    let routes = vec![LevelRoute {
        levels: 1..7,
        path: PathBuf::from("cold"),
    }];
    let options = Options {
        level_routes: routes.clone(),
        ..Options::default()
    };
    let expected = small_store(&folder, &routes)?;
    stop_after_ready(&folder, &options)?;

    std::env::set_current_dir(base.path().join("two"))?;
    assert!(matches!(
        convert(&folder, &options),
        Err(Error::Unsupported(_))
    ));
    assert!(folder.join(READY).exists(), "nothing was moved");

    std::env::set_current_dir(base.path().join("one"))?;
    assert!(convert(&folder, &options)?.resumed);
    assert_eq!(read_v6(&folder, &routes)?, expected);
    Ok(())
}

/// A route folder whose path the line-oriented markers cannot hold is refused
/// before anything is built, rather than once a marker no run can read again
/// is durable.
#[test]
fn a_route_path_with_a_line_break_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    let cold = tempfile::tempdir()?;
    small_store(folder.path(), &[])?;
    let options = Options {
        level_routes: vec![LevelRoute {
            levels: 1..7,
            path: cold.path().join("a\nb"),
        }],
        ..Options::default()
    };
    assert!(matches!(
        convert(folder.path(), &options),
        Err(Error::Unsupported(_))
    ));
    assert!(!folder.path().join(STAGING).exists() && !folder.path().join(READY).exists());
    Ok(())
}

/// A converted table may record an inner-block layout its source lacked, read
/// from its frames as the source's writer did not yet; a layout the source
/// had must arrive. A restricted table's carried suffix may hold no split
/// frame of the source's, so its layout is not compared.
#[test]
fn records_compare_the_inner_layout_one_way() {
    let table = |block_layout| lsm6::import::RecordedTable {
        id: 1,
        columnar: false,
        split_fields: false,
        created_at: 1,
        kv_checksum: None,
        ecc: None,
        partitioned_index: false,
        seqno_bounds: false,
        zone_map: false,
        bulk_ingested: None,
        lineage: lsm6::import::TableLineage::default(),
        blob_links: Vec::new(),
        restriction: None,
        seqnos: (1, 2),
        highest_kv_seqno: 2,
        block_layout,
    };
    assert!(records_match(&table(false), &table(true), false), "derived");
    assert!(!records_match(&table(true), &table(false), false), "lost");
    assert!(records_match(&table(false), &table(true), true));
    assert!(records_match(&table(true), &table(false), true));
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
        STAGING,
        BACKUP,
        READY,
        SWAPPING,
        "v1.tmp",
        "notes.txt",
    ] {
        assert!(!is_store_entry(name), "{name}");
    }
    // The markers are files an open of the store keeps: it removes each file
    // whose name starts with `v` other than its snapshot.
    for marker in [READY, SWAPPING] {
        assert!(!marker.starts_with('v'), "{marker}");
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
