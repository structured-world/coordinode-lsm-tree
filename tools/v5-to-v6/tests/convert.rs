//! End to end: a store written by the 5.x crate, converted, answers every
//! read the 6.0 engine is asked exactly as the 5.x engine answered it.

use std::path::Path;

/// What a tree answers at one snapshot: every live key with its value in key
/// order, and the point read of every probed key, present or not.
#[derive(Debug, PartialEq, Eq)]
struct Answers {
    scan: Vec<(Vec<u8>, Vec<u8>)>,
    gets: Vec<Option<Vec<u8>>>,
}

/// What one snapshot read returns: its answers, or the oldest snapshot the
/// tree still serves when this one is below its retention floor, which the
/// converted store must keep refusing the same way.
#[derive(Debug, PartialEq, Eq)]
enum Snapshot {
    Read(Answers),
    BelowRetention(u64),
}

impl Snapshot {
    /// [`Answers`] for the reads `read` makes at one snapshot, as a 5.x tree
    /// answers them.
    fn v5(read: impl FnOnce() -> lsm5::Result<Answers>) -> lsm5::Result<Self> {
        match read() {
            Ok(answers) => Ok(Self::Read(answers)),
            Err(lsm5::Error::SnapshotBelowRetention {
                oldest_retained, ..
            }) => Ok(Self::BelowRetention(oldest_retained)),
            Err(e) => Err(e),
        }
    }

    /// [`Self::v5`] for a 6.0 tree.
    fn v6(read: impl FnOnce() -> lsm6::Result<Answers>) -> lsm6::Result<Self> {
        match read() {
            Ok(answers) => Ok(Self::Read(answers)),
            Err(lsm6::Error::SnapshotBelowRetention {
                oldest_retained, ..
            }) => Ok(Self::BelowRetention(oldest_retained)),
            Err(e) => Err(e),
        }
    }

    /// Whether this snapshot read data.
    fn holds_data(&self) -> bool {
        matches!(self, Self::Read(answers) if !answers.scan.is_empty())
    }
}

/// Seqnos the comparison reads at: every snapshot the fixture's history
/// distinguishes, the newest included.
const READ_AT: [u64; 6] = [1, 40, 80, 120, 160, u64::MAX];

/// Keys every comparison reads: the `keys` the fixture wrote and as many
/// again it never wrote, which a filter or a locator must answer as absent.
fn probes(keys: u32) -> impl Iterator<Item = String> {
    (0..keys * 2).map(|i| format!("k{i:05}"))
}

/// What a store is opened with that its files do not say.
#[derive(Clone, Default)]
struct OpenWith {
    /// Whether the tree separates values.
    separated: bool,
    /// The key it is encrypted under, for both crates.
    encryption: Option<v5_to_v6::EncryptionPair>,
}

fn read_v5(folder: &Path, keys: u32, open: &OpenWith) -> lsm5::Result<Vec<Snapshot>> {
    use lsm5::{AbstractTree as _, Guard as _};
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .with_kv_separation(open.separated.then(lsm5::KvSeparationOptions::default))
    .with_encryption(open.encryption.as_ref().map(|e| e.v5.clone()))
    .open()?;
    READ_AT
        .iter()
        .map(|&seqno| {
            Snapshot::v5(|| {
                Ok(Answers {
                    scan: tree
                        .range::<&[u8], _>(.., seqno, None)
                        .map(|guard| {
                            let (key, value) = guard.into_inner()?;
                            Ok((key.to_vec(), value.to_vec()))
                        })
                        .collect::<lsm5::Result<_>>()?,
                    gets: probes(keys)
                        .map(|key| Ok(tree.get(key, seqno)?.map(|v| v.to_vec())))
                        .collect::<lsm5::Result<_>>()?,
                })
            })
        })
        .collect()
}

fn read_v6(folder: &Path, keys: u32, open: &OpenWith) -> lsm6::Result<Vec<Snapshot>> {
    use lsm6::{AbstractTree as _, Guard as _};
    let tree = lsm6::Config::new(
        folder,
        lsm6::SequenceNumberCounter::default(),
        lsm6::SequenceNumberCounter::default(),
    )
    .with_kv_separation(open.separated.then(lsm6::KvSeparationOptions::default))
    .with_encryption(open.encryption.as_ref().map(|e| e.v6.clone()))
    .open()?;
    READ_AT
        .iter()
        .map(|&seqno| {
            Snapshot::v6(|| {
                Ok(Answers {
                    scan: tree
                        .range::<&[u8], _>(.., seqno, None)
                        .map(|guard| {
                            let (key, value) = guard.into_inner()?;
                            Ok((key.to_vec(), value.to_vec()))
                        })
                        .collect::<lsm6::Result<_>>()?,
                    gets: probes(keys)
                        .map(|key| Ok(tree.get(key, seqno)?.map(|v| v.to_vec())))
                        .collect::<lsm6::Result<_>>()?,
                })
            })
        })
        .collect()
}

/// A row-table store with several levels and runs: overwrites, point
/// tombstones and a range tombstone across flushes, a compaction that moved
/// part of it down, and newer flushes on top, over `keys` keys, written
/// under `configure` and the runtime settings `tune` sets.
fn write_row_store(
    folder: &Path,
    keys: u32,
    configure: impl FnOnce(lsm5::Config) -> lsm5::Config,
    tune: impl FnOnce(&mut lsm5::runtime_config::RuntimeConfig),
) -> lsm5::Result<()> {
    use lsm5::AbstractTree as _;
    let tree = configure(
        lsm5::Config::new(
            folder,
            lsm5::SequenceNumberCounter::default(),
            lsm5::SequenceNumberCounter::default(),
        )
        .data_block_size_policy(lsm5::config::BlockSizePolicy::all(1_024)),
    )
    .open()?;
    match &tree {
        lsm5::AnyTree::Standard(standard) => standard.update_runtime_config(tune)?,
        lsm5::AnyTree::Blob(blob) => blob.update_runtime_config(tune)?,
    }
    let mut seqno = 0u64;
    let mut next = || {
        seqno += 1;
        seqno
    };
    for round in 0..4u32 {
        for i in 0..keys {
            if (i + round) % 3 == 0 {
                tree.insert(format!("k{i:05}"), format!("v{round}-{i}"), next());
            }
            if round == 2 && i % 17 == 0 {
                tree.remove(format!("k{i:05}"), next());
            }
        }
        if round == 1 {
            tree.remove_range(
                lsm5::UserKey::from(format!("k{:05}", keys / 3)),
                lsm5::UserKey::from(format!("k{:05}", keys / 2)),
                next(),
            );
        }
        tree.flush_active_memtable(0)?;
        if round == 1 {
            tree.major_compact(u64::MAX, 0)?;
        }
    }
    Ok(())
}

/// Converts the store of `keys` keys written under `configure` and checks the
/// 6.0 engine answers every read the 5.x engine answered, at every snapshot.
fn round_trip(
    keys: u32,
    configure: impl FnOnce(lsm5::Config) -> lsm5::Config,
) -> Result<(), Box<dyn std::error::Error>> {
    round_trip_checked(keys, configure, |_| Ok(()))
}

/// [`round_trip`], with `check` run on the written source first, to prove
/// the fixture holds what the test is about.
fn round_trip_checked(
    keys: u32,
    configure: impl FnOnce(lsm5::Config) -> lsm5::Config,
    check: impl FnOnce(&Path) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    round_trip_tuned(keys, None, configure, |_| {}, check)
}

/// [`round_trip_checked`] over a store written under the runtime settings
/// `tune` sets, encrypted under `encryption` when given.
fn round_trip_tuned(
    keys: u32,
    encryption: Option<v5_to_v6::EncryptionPair>,
    configure: impl FnOnce(lsm5::Config) -> lsm5::Config,
    tune: impl FnOnce(&mut lsm5::runtime_config::RuntimeConfig),
    check: impl FnOnce(&Path) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    let v5_encryption = encryption.as_ref().map(|e| e.v5.clone());
    write_row_store(
        folder.path(),
        keys,
        |config| configure(config.with_encryption(v5_encryption.clone())),
        tune,
    )?;
    check(folder.path())?;
    let open = OpenWith {
        separated: lsm5::export::read_manifest(folder.path(), &lsm5::fs::StdFs, v5_encryption)?
            .tree_type
            == lsm5::TreeType::Blob,
        encryption: encryption.clone(),
    };
    let expected = read_v5(folder.path(), keys, &open)?;
    assert!(
        expected.iter().any(Snapshot::holds_data),
        "the fixture holds data at the snapshots read"
    );

    v5_to_v6::convert(
        folder.path(),
        &v5_to_v6::Options {
            encryption,
            ..v5_to_v6::Options::default()
        },
    )?;

    assert_eq!(read_v6(folder.path(), keys, &open)?, expected);
    Ok(())
}

#[test]
fn a_converted_row_store_answers_every_read_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    round_trip(300, |config| config)
}

/// Partitioned filters are carried over partition by partition. Enough keys
/// that the 5.x writer splits the filter into several partitions.
#[test]
fn a_converted_store_with_partitioned_filters_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    round_trip_checked(
        12_000,
        |config| config.filter_block_partitioning_policy(lsm5::config::PinningPolicy::all(true)),
        |folder| {
            let most = source_tables(folder)?
                .iter()
                .map(|table| match table.filter() {
                    Ok(Some(lsm5::export::Filter::Partitioned(parts))) => Ok(parts.len()),
                    Ok(_) => Ok(0),
                    Err(e) => Err(e),
                })
                .collect::<lsm5::Result<Vec<_>>>()?
                .into_iter()
                .max()
                .unwrap_or(0);
            assert!(most > 1, "a source table holds several filter partitions");
            Ok(())
        },
    )
}

/// A table's properties are carried over: per-KV checksum footers inside the
/// carried payloads, fresh parity under the source's scheme, a split index,
/// seqno bounds, a zone map, the age and the compaction lineage. The
/// conversion itself refuses a table that reads back other than its source
/// recorded, so a clean round trip proves each one arrived.
#[test]
fn a_converted_store_keeps_every_table_property() -> Result<(), Box<dyn std::error::Error>> {
    round_trip_tuned(
        2_000,
        None,
        |config| config.page_ecc(true),
        |runtime| {
            runtime.kv_checksums = lsm5::runtime_config::KvChecksumPolicy::AllLevels;
            runtime.kv_checksum_algo = lsm5::runtime_config::ChecksumAlgorithm::Xxh3Low32;
            runtime.seqno_in_index = true;
            runtime.zone_map = true;
            // The adaptive index stays whole below its spill threshold.
            runtime.index_partition_spill_threshold = 0;
        },
        |folder| {
            let properties = source_tables(folder)?
                .iter()
                .map(lsm5::export::TableExport::properties)
                .collect::<lsm5::Result<Vec<_>>>()?;
            assert!(properties.iter().all(|p| p.kv_checksum.is_some()
                && p.ecc.is_some()
                && p.partitioned_index
                && p.seqno_bounds
                && p.zone_map));
            assert!(
                properties.iter().any(|p| p.lineage.is_some()),
                "a source table is a compaction output"
            );
            Ok(())
        },
    )
}

/// A tree that separates values: its blob files are carried value by value at
/// their offsets, so every handle in the tables still resolves, and each
/// table's links to them arrive with the same counts.
#[test]
fn a_converted_blob_tree_answers_as_the_source_did() -> Result<(), Box<dyn std::error::Error>> {
    round_trip_checked(
        300,
        |config| {
            config.with_kv_separation(Some(
                lsm5::KvSeparationOptions::default().separation_threshold(4),
            ))
        },
        |folder| {
            let state = lsm5::export::read_manifest(folder, &lsm5::fs::StdFs, None)?;
            assert!(
                state.blob_files.len() > 1,
                "the fixture holds several blob files"
            );
            Ok(())
        },
    )
}

/// An encrypted store: every block is decrypted by the 5.x read path and
/// encrypted again under the same key, bound to its place in the 6.0 table.
#[test]
fn a_converted_encrypted_store_answers_as_the_source_did() -> Result<(), Box<dyn std::error::Error>>
{
    let key = [7u8; 32];
    round_trip_tuned(
        300,
        Some(v5_to_v6::EncryptionPair {
            v5: std::sync::Arc::new(lsm5::Aes256GcmProvider::new(&key)),
            v6: std::sync::Arc::new(lsm6::Aes256GcmProvider::new(&key)),
        }),
        |config| config,
        |_| {},
        |_| Ok(()),
    )
}

/// A bit flipped in a data block after the store was written is repaired by
/// the 5.x read path's parity, so the converted store holds the data as
/// written, not the damage.
#[test]
fn a_converted_store_carries_data_its_parity_repaired() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    write_row_store(folder.path(), 300, |config| config.page_ecc(true), |_| {})?;
    let open = OpenWith::default();
    let expected = read_v5(folder.path(), 300, &open)?;

    // The exports hold the table files open, and Windows refuses to move a
    // folder holding an open file: they are closed before the conversion.
    let (path, block) = {
        let tables = source_tables(folder.path())?;
        let table = tables.first().ok_or("the fixture holds a table")?;
        let block = table
            .data_blocks()?
            .into_iter()
            .nth(1)
            .ok_or("the table holds several data blocks")?;
        (
            folder.path().join("tables").join(table.id().to_string()),
            block,
        )
    };
    let mut bytes = std::fs::read(&path)?;
    let at = usize::try_from(block.offset + u64::from(block.size) / 4)?;
    bytes[at] ^= 0x10;
    std::fs::write(&path, &bytes)?;

    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    assert_eq!(read_v6(folder.path(), 300, &open)?, expected);
    Ok(())
}

/// A columnar store: the 6.0 columnar layout is not the 5.x one, so its rows
/// are encoded again, the filter carried and the locator built again over the
/// new blocks.
#[test]
fn a_converted_columnar_store_answers_as_the_source_did() -> Result<(), Box<dyn std::error::Error>>
{
    round_trip_tuned(
        2_000,
        None,
        |config| {
            config.locator_policy(lsm5::config::LocatorPolicy::all(
                lsm5::config::LocatorPolicyEntry::Enabled {
                    precision: lsm5::config::LocatorPrecision::Entry,
                    block_id_bits: None,
                    slot_bits: None,
                },
            ))
        },
        |runtime| runtime.columnar = true,
        |folder| {
            let tables = source_tables(folder)?;
            assert!(
                tables.iter().all(lsm5::export::TableExport::is_columnar),
                "every source table is columnar"
            );
            Ok(())
        },
    )
}

/// Copies the folder `from` of `fs` to `to` on disk, file by file. A punched
/// range reads back as zeros, as it does from a real file.
fn copy_out(
    fs: &dyn lsm5::fs::Fs,
    from: &Path,
    to: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(to)?;
    for entry in fs.read_dir(from)? {
        let target = to.join(&entry.file_name);
        if entry.is_dir {
            copy_out(fs, &entry.path, &target)?;
        } else {
            let mut file = fs.open(&entry.path, &lsm5::fs::FsOpenOptions::new().read(true))?;
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut bytes)?;
            std::fs::write(target, bytes)?;
        }
    }
    Ok(())
}

/// A 5.x store whose tight-space compaction stopped after its first slice:
/// the input it consumed a prefix of stays in the manifest, restricted. Built
/// on a capped in-memory disk, stopped by failing the sync of a later
/// manifest edit and then a power loss, which keeps only what was synced, and
/// copied out to `to`.
fn write_restricted_store(to: &Path, keys: u32) -> Result<(), Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    // The in-memory store is named by a real, empty folder, as the 5.x
    // crate's own capped-disk tests name theirs.
    let named = tempfile::tempdir()?;
    let root = named.path();
    // Which edit sync falls after the first slice is installed depends on the
    // compaction's own edits; take the first stop that leaves a restriction.
    let mut last = String::new();
    for skip in 0..16 {
        let mem = lsm5::fs::MemFs::with_capacity(u64::MAX);
        let crash = lsm5::fs::CrashFs::new(mem.clone());
        let fs = lsm5::fs::FaultFs::new(crash.clone());
        let injector = fs.injector();
        let shared: std::sync::Arc<dyn lsm5::fs::Fs> = std::sync::Arc::new(fs);
        let tree = match lsm5::Config::new(
            root,
            lsm5::SequenceNumberCounter::default(),
            lsm5::SequenceNumberCounter::default(),
        )
        .data_block_size_policy(lsm5::config::BlockSizePolicy::all(512))
        .with_shared_fs(shared.clone())
        .open()
        .map_err(|e| format!("opening the in-memory store: {e:?}"))?
        {
            lsm5::AnyTree::Standard(tree) => tree,
            lsm5::AnyTree::Blob(_) => return Err("a standard tree was opened".into()),
        };
        for i in 0..keys {
            tree.insert(format!("k{i:05}"), vec![0xCD; 64], u64::from(i) + 1);
        }
        tree.flush_active_memtable(0)?;
        // Room for slices of several blocks, so the first one reclaims whole
        // blocks below its bound; not for a merge of the whole table.
        let used = tree.storage_stats()?.used_bytes;
        mem.set_capacity(used + used / 4);
        tree.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.tight_space_compaction = true;
        })?;
        injector.arm(
            lsm5::fs::FaultRule::new(
                lsm5::fs::FaultOp::SyncAll,
                lsm5::fs::Fault::Error(lsm5::io::ErrorKind::Other),
            )
            .on_path("edits-")
            .skip(skip)
            .once(),
        );
        let stopped = tree.major_compact(64 * 1024 * 1024, 0).is_err();
        drop(tree);
        injector.clear();
        if !stopped {
            last = format!("skip {skip}: the compaction was not stopped");
            continue;
        }
        crash.crash();
        let durable = crash.inner();
        match lsm5::export::read_manifest(root, &*durable, None) {
            Ok(state) if !state.restrictions.is_empty() => {
                let tables = root.join("tables");
                let all_present = state.levels.iter().flatten().flatten().all(|t| {
                    durable
                        .exists(&tables.join(t.id.to_string()))
                        .unwrap_or(false)
                });
                if all_present {
                    copy_out(&*durable, root, to)?;
                    return Ok(());
                }
                last = format!("skip {skip}: a table the manifest names is gone");
            }
            Ok(_) => last = format!("skip {skip}: no table is restricted"),
            Err(e) => last = format!("skip {skip}: {e}"),
        }
    }
    Err(format!("no stop left a restricted table ({last})").into())
}

/// A 5.x blob tree whose tight-space defragmentation stopped after its first
/// relocation slice: the stale blob file it consumed a prefix of stays in the
/// manifest behind a frontier. Built, stopped and copied out as
/// [`write_restricted_store`] does.
fn write_restricted_blob_store(to: &Path, keys: u32) -> Result<(), Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    // Values that do not compress, so the relocation's transient space is real.
    let value = |i: u32, generation: u64| -> Vec<u8> {
        let mut s = (u64::from(i) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (generation << 1);
        (0..200)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                s.to_le_bytes()[3]
            })
            .collect()
    };
    let named = tempfile::tempdir()?;
    let root = named.path();
    let mut last = String::new();
    for skip in 0..16 {
        let mem = lsm5::fs::MemFs::with_capacity(u64::MAX);
        let crash = lsm5::fs::CrashFs::new(mem.clone());
        let fs = lsm5::fs::FaultFs::new(crash.clone());
        let injector = fs.injector();
        let shared: std::sync::Arc<dyn lsm5::fs::Fs> = std::sync::Arc::new(fs);
        let tree = match lsm5::Config::new(
            root,
            lsm5::SequenceNumberCounter::default(),
            lsm5::SequenceNumberCounter::default(),
        )
        .with_shared_fs(shared.clone())
        .with_kv_separation(Some(
            lsm5::KvSeparationOptions::default()
                .separation_threshold(64)
                // Every half-dead file is stale, and there are several of them.
                .age_cutoff(1.0)
                .staleness_threshold(0.1)
                .file_target_size(48 * 1024),
        ))
        .open()
        .map_err(|e| format!("opening the in-memory store: {e:?}"))?
        {
            lsm5::AnyTree::Blob(tree) => tree,
            lsm5::AnyTree::Standard(_) => return Err("a blob tree was opened".into()),
        };
        let keys64 = u64::from(keys);
        for i in 0..keys {
            tree.insert(format!("k{i:05}"), value(i, 1), u64::from(i) + 1);
        }
        tree.flush_active_memtable(0)?;
        // Every other key overwritten: each first-generation file is half dead.
        for i in (0..keys).step_by(2) {
            tree.insert(format!("k{i:05}"), value(i, 2), keys64 + u64::from(i) + 1);
        }
        tree.flush_active_memtable(0)?;
        let watermark = 4 * keys64;
        tree.index.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.storage_limit_bytes = None;
        })?;
        // A merge with room learns which blobs are dead.
        tree.major_compact(64 * 1024 * 1024, watermark)?;
        let used = tree.storage_stats()?.used_bytes;
        mem.set_capacity(used + used / 4);
        tree.index
            .update_runtime_config(|c| c.tight_space_compaction = true)?;
        injector.arm(
            lsm5::fs::FaultRule::new(
                lsm5::fs::FaultOp::SyncAll,
                lsm5::fs::Fault::Error(lsm5::io::ErrorKind::Other),
            )
            .on_path("edits-")
            .skip(skip)
            .once(),
        );
        let stopped = tree.major_compact(64 * 1024 * 1024, watermark).is_err();
        drop(tree);
        injector.clear();
        if !stopped {
            last = format!("skip {skip}: the compaction was not stopped");
            continue;
        }
        crash.crash();
        let durable = crash.inner();
        match lsm5::export::read_manifest(root, &*durable, None) {
            Ok(state) if !state.blob_restrictions.is_empty() => {
                let present = |folder: &str, id: u64| {
                    durable
                        .exists(&root.join(folder).join(id.to_string()))
                        .unwrap_or(false)
                };
                let all_present = state
                    .levels
                    .iter()
                    .flatten()
                    .flatten()
                    .all(|t| present("tables", t.id))
                    && state.blob_files.iter().all(|b| present("blobs", b.id));
                if all_present {
                    copy_out(&*durable, root, to)?;
                    return Ok(());
                }
                last = format!("skip {skip}: a file the manifest names is gone");
            }
            Ok(_) => last = format!("skip {skip}: no blob file is restricted"),
            Err(e) => last = format!("skip {skip}: {e}"),
        }
    }
    Err(format!("no stop left a restricted blob file ({last})").into())
}

/// A blob file a tight-space relocation reclaimed below a frontier: its live
/// values are carried at their offsets past it, and the converted file is
/// restricted to them, as every value handle of the tables expects.
#[test]
fn a_converted_store_with_a_restricted_blob_file_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    let keys = 2_000;
    let folder = tempfile::tempdir()?;
    write_restricted_blob_store(folder.path(), keys)?;
    let open = OpenWith {
        separated: true,
        ..OpenWith::default()
    };
    let expected = read_v5(folder.path(), keys, &open)
        .map_err(|e| format!("reading the copied store: {e:?}"))?;
    let state = lsm5::export::read_manifest(folder.path(), &lsm5::fs::StdFs, None)?;
    let mut live_past_frontier = 0;
    for (id, from) in &state.blob_restrictions {
        let record = state
            .blob_files
            .iter()
            .find(|b| b.id == *id)
            .ok_or("a restricted blob file is recorded")?;
        let blob = lsm5::export::BlobFileExport::open(
            &folder.path().join("blobs").join(id.to_string()),
            record,
            *from,
            std::sync::Arc::new(lsm5::fs::StdFs),
        )?;
        if *from > 0 && !blob.frames()?.is_empty() {
            live_past_frontier += 1;
        }
    }
    assert!(
        live_past_frontier > 0,
        "the copied store holds a blob file restricted past its start that still serves values"
    );

    assert!(
        expected.iter().any(Snapshot::holds_data),
        "the fixture holds data at the snapshots read"
    );

    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    assert_eq!(read_v6(folder.path(), keys, &open)?, expected);
    Ok(())
}

/// A restricted blob file keeps the totals its source counted over the whole
/// file, the reclaimed prefix included, which its garbage statistics are
/// charged against: it is exactly as stale as the source. The fixture's
/// files are about half stale, so under a 0.9 threshold none is relocated,
/// and a merge that drops no version leaves every charge where it was.
#[test]
fn a_converted_restricted_blob_file_is_as_stale_as_its_source()
-> Result<(), Box<dyn std::error::Error>> {
    use lsm6::AbstractTree as _;
    let keys = 2_000;
    let folder = tempfile::tempdir()?;
    write_restricted_blob_store(folder.path(), keys)?;
    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;

    let tree = lsm6::Config::new(
        folder.path(),
        lsm6::SequenceNumberCounter::default(),
        lsm6::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        lsm6::KvSeparationOptions::default()
            .age_cutoff(1.0)
            .staleness_threshold(0.9),
    ))
    .open()?;
    let stale = tree.stale_blob_bytes();
    assert!(stale > 0, "the converted store carries its garbage charges");
    tree.major_compact(64 * 1024 * 1024, 0)?;
    assert_eq!(
        tree.stale_blob_bytes(),
        stale,
        "no blob file under the threshold is relocated"
    );
    Ok(())
}

/// A table a tight-space compaction restricted: its live blocks are carried,
/// the reclaimed ones are not read, and the restriction keeps hiding the keys
/// below its bound that the first carried block still holds.
#[test]
fn a_converted_store_with_a_restricted_table_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    let keys = 2_000;
    let folder = tempfile::tempdir()?;
    write_restricted_store(folder.path(), keys)?;
    let open = OpenWith::default();
    let expected = read_v5(folder.path(), keys, &open)
        .map_err(|e| format!("reading the copied store: {e:?}"))?;
    let state = lsm5::export::read_manifest(folder.path(), &lsm5::fs::StdFs, None)?;
    let context = lsm5::export::TableContext::new(
        &state,
        std::sync::Arc::new(lsm5::fs::StdFs),
        None,
        lsm5::ZstdDictionaries::new(),
        std::sync::Arc::new(lsm5::DefaultUserComparator),
    )?;
    let mut live_from = Vec::new();
    for (id, bound) in &state.restrictions {
        let record = state
            .levels
            .iter()
            .flatten()
            .flatten()
            .find(|t| t.id == *id)
            .ok_or("a restricted table is placed")?;
        let table = lsm5::export::TableExport::open(
            &folder.path().join("tables").join(id.to_string()),
            record,
            Some(bound),
            &context,
        )?;
        live_from.push(table.live_from()?);
    }
    assert!(
        live_from.iter().any(|&at| at > 0),
        "the copied store holds a table whose blocks below its bound are not read"
    );

    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    assert_eq!(read_v6(folder.path(), keys, &open)?, expected);
    Ok(())
}

/// Raw content a test dictionary is made of: the shape of the fixture's values.
fn dictionary_bytes() -> Vec<u8> {
    (0..512u32)
        .flat_map(|i| format!("v{}-{i}", i % 4).into_bytes())
        .collect()
}

/// Data blocks compressed against a dictionary are carried as compressed, and
/// the dictionary goes with them into the converted store's folder, where the
/// 6.0 open finds it.
#[test]
fn a_converted_store_with_a_dictionary_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    let dict = std::sync::Arc::new(lsm5::ZstdDictionary::new(&dictionary_bytes()));
    let codec = lsm5::CompressionType::zstd_dict(3, dict.id())?;
    round_trip_checked(
        300,
        |config| {
            config
                .zstd_dictionary(Some(dict))
                .data_block_compression_policy(lsm5::config::CompressionPolicy::all(codec))
        },
        |folder| {
            let properties = source_tables(folder)?
                .iter()
                .map(lsm5::export::TableExport::properties)
                .collect::<lsm5::Result<Vec<_>>>()?;
            assert!(properties.iter().all(|p| p.data_compression == codec));
            Ok(())
        },
    )
}

/// Blob files whose values are compressed against a dictionary keep it too.
#[test]
fn a_converted_blob_tree_with_a_dictionary_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    let dict = std::sync::Arc::new(lsm5::ZstdDictionary::new(&dictionary_bytes()));
    let codec = lsm5::CompressionType::zstd_dict(3, dict.id())?;
    round_trip_tuned(
        300,
        None,
        |config| {
            config.zstd_dictionary(Some(dict)).with_kv_separation(Some(
                lsm5::KvSeparationOptions::default().separation_threshold(4),
            ))
        },
        |runtime| runtime.blob_compression = codec,
        |folder| {
            let state = lsm5::export::read_manifest(folder, &lsm5::fs::StdFs, None)?;
            for record in &state.blob_files {
                let blob = lsm5::export::BlobFileExport::open(
                    &folder.join("blobs").join(record.id.to_string()),
                    record,
                    0,
                    std::sync::Arc::new(lsm5::fs::StdFs),
                )?;
                assert_eq!(blob.compression(), codec);
            }
            assert!(!state.blob_files.is_empty(), "the fixture separates values");
            Ok(())
        },
    )
}

/// A dictionary the store does not keep in its folder is supplied by the
/// caller; without it the conversion stops before touching the source.
#[test]
fn a_dictionary_the_store_does_not_keep_is_supplied_by_the_caller()
-> Result<(), Box<dyn std::error::Error>> {
    let raw = dictionary_bytes();
    let dict = std::sync::Arc::new(lsm5::ZstdDictionary::new(&raw));
    let codec = lsm5::CompressionType::zstd_dict(3, dict.id())?;
    let folder = tempfile::tempdir()?;
    write_row_store(
        folder.path(),
        300,
        |config| {
            config
                .zstd_dictionary(Some(dict.clone()))
                .data_block_compression_policy(lsm5::config::CompressionPolicy::all(codec))
        },
        |_| {},
    )?;
    let open = OpenWith::default();
    let expected = read_v5(folder.path(), 300, &open)?;
    std::fs::remove_file(folder.path().join("dicts").join(dict.id().to_string()))?;

    assert!(
        v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default()).is_err(),
        "a table compressed against a dictionary nobody holds is not converted"
    );
    assert!(folder.path().join("tables").exists(), "the source stays");

    v5_to_v6::convert(
        folder.path(),
        &v5_to_v6::Options {
            dictionaries: vec![raw],
            ..v5_to_v6::Options::default()
        },
    )?;
    assert_eq!(read_v6(folder.path(), 300, &open)?, expected);
    Ok(())
}

/// A store a 5.x process still has open is not converted: the conversion
/// would miss what that process writes after it read the manifest, and its
/// switch would move the files out from under it.
#[test]
fn a_store_another_process_holds_open_is_not_converted() -> Result<(), Box<dyn std::error::Error>> {
    let folder = tempfile::tempdir()?;
    write_row_store(folder.path(), 100, |config| config, |_| {})?;
    let open = lsm5::Config::new(
        folder.path(),
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .open()?;

    assert!(
        v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default()).is_err(),
        "a store held open by a 5.x tree is refused"
    );
    assert!(
        !folder.path().join("v6-convert").exists() && !folder.path().join("v5-backup").exists(),
        "nothing was built or moved"
    );
    drop(open);
    Ok(())
}

/// One column of a columnar scan: its id and bytes.
type ScannedColumn = (u16, Vec<u8>);

/// Every column of a columnar scan projecting `fields`, batch by batch.
fn scan_v5(folder: &Path, fields: &[u16]) -> lsm5::Result<Vec<Vec<ScannedColumn>>> {
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .open()?;
    tree.columnar_scan(fields, None, u64::MAX, ..)?
        .map(|batch| {
            Ok(batch?
                .columns
                .into_iter()
                .map(|c| (c.column_id, c.data.to_vec()))
                .collect())
        })
        .collect()
}

/// [`scan_v5`] over a 6.0 store.
fn scan_v6(folder: &Path, fields: &[u16]) -> lsm6::Result<Vec<Vec<ScannedColumn>>> {
    let tree = lsm6::Config::new(
        folder,
        lsm6::SequenceNumberCounter::default(),
        lsm6::SequenceNumberCounter::default(),
    )
    .open()?;
    tree.columnar_scan(fields, None, u64::MAX, ..)?
        .map(|batch| {
            Ok(batch?
                .columns
                .into_iter()
                .map(|c| (c.column_id, c.data.to_vec()))
                .collect())
        })
        .collect()
}

/// Ingests into a 5.x store at `folder` one columnar batch of `rows` rows whose
/// value is split into two fields under `ids`: a fixed-width number and a
/// name per row.
fn ingest_field_batch(
    folder: &Path,
    rows: u32,
    ids: [u16; 2],
) -> Result<(), Box<dyn std::error::Error>> {
    use lsm5::table::columnar::{Column, TypeTag, entries_to_column_batch};
    let tree = lsm5::Config::new(
        folder,
        lsm5::SequenceNumberCounter::default(),
        lsm5::SequenceNumberCounter::default(),
    )
    .open()?;
    {
        let lsm5::AnyTree::Standard(standard) = &tree else {
            return Err("a standard tree was opened".into());
        };
        standard.update_runtime_config(|c| c.columnar = true)?;
        let entries: Vec<_> = (0..rows)
            .map(|i| {
                lsm5::InternalValue::from_components(
                    format!("k{i:05}"),
                    "x",
                    0,
                    lsm5::ValueType::Value,
                )
            })
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        batch.columns.pop();
        // Field 10: a fixed-width number per row; field 11: a name per row.
        let fixed: Vec<u8> = (0..rows).flat_map(u32::to_le_bytes).collect();
        let names: Vec<String> = (0..rows).map(|i| format!("name-{i}")).collect();
        let mut bytes = Vec::new();
        let mut at = 0u32;
        bytes.extend(at.to_le_bytes());
        for name in &names {
            at += u32::try_from(name.len())?;
            bytes.extend(at.to_le_bytes());
        }
        for name in &names {
            bytes.extend(name.as_bytes());
        }
        batch.columns.push(Column {
            column_id: ids[0],
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data: fixed.into(),
        });
        batch.columns.push(Column {
            column_id: ids[1],
            type_tag: TypeTag::Bytes,
            validity: None,
            data: bytes.into(),
        });
        let mut ingestion = tree.ingestion()?;
        ingestion.write_columnar_batch(&batch)?;
        ingestion.finish()?;
    }
    Ok(())
}

/// A columnar batch ingested with its value split into fields keeps them:
/// the converted table stores the same field columns, which a projected scan
/// reads as the source's did, and every row reads back the same value.
#[test]
fn a_converted_ingested_column_batch_keeps_its_fields() -> Result<(), Box<dyn std::error::Error>> {
    let rows = 50u32;
    let folder = tempfile::tempdir()?;
    ingest_field_batch(folder.path(), rows, [10, 11])?;
    let open = OpenWith::default();
    let expected_reads = read_v5(folder.path(), rows, &open)?;
    let expected_scan = scan_v5(folder.path(), &[10, 11])?;
    assert!(
        expected_scan.iter().flatten().any(|(id, _)| *id == 10),
        "the source scan reads the field"
    );

    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    assert_eq!(read_v6(folder.path(), rows, &open)?, expected_reads);
    assert_eq!(scan_v6(folder.path(), &[10, 11])?, expected_scan);
    Ok(())
}

/// A field id 6.0 keeps for itself is renumbered, once for the store, to an id
/// no field uses: the field reads back under its new id with the bytes it had,
/// every row reads back the same value, and the report names the change.
#[test]
fn a_field_id_6_0_keeps_for_itself_is_renumbered() -> Result<(), Box<dyn std::error::Error>> {
    let rows = 50u32;
    let folder = tempfile::tempdir()?;
    ingest_field_batch(folder.path(), rows, [10, u16::MAX])?;
    let open = OpenWith::default();
    let expected_reads = read_v5(folder.path(), rows, &open)?;
    let expected_scan = scan_v5(folder.path(), &[10, u16::MAX])?;

    let report = v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    let [(from, to)] = report.renumbered_fields.as_slice() else {
        return Err(format!("one field renumbered, got {:?}", report.renumbered_fields).into());
    };
    assert_eq!(*from, u16::MAX);
    assert!(
        *to < lsm6::blob_tree::field_row::RESERVED_COLUMNS && *to != 10,
        "the new id is free and not reserved"
    );
    assert_eq!(read_v6(folder.path(), rows, &open)?, expected_reads);
    let renamed: Vec<Vec<_>> = expected_scan
        .into_iter()
        .map(|batch| {
            batch
                .into_iter()
                .map(|(id, bytes)| (if id == u16::MAX { *to } else { id }, bytes))
                .collect()
        })
        .collect();
    assert_eq!(scan_v6(folder.path(), &[10, *to])?, renamed);
    Ok(())
}

/// A table holding only a range tombstone keeps deleting what it covers: its
/// key range must still cover the whole tombstone after conversion, though
/// the table carries an entry at the tombstone's start only.
#[test]
fn a_converted_tombstone_only_table_still_deletes_its_range()
-> Result<(), Box<dyn std::error::Error>> {
    use lsm5::AbstractTree as _;
    let keys = 200u32;
    let folder = tempfile::tempdir()?;
    {
        let tree = lsm5::Config::new(
            folder.path(),
            lsm5::SequenceNumberCounter::default(),
            lsm5::SequenceNumberCounter::default(),
        )
        .open()?;
        for i in 0..keys {
            tree.insert(format!("k{i:05}"), "v", u64::from(i) + 1);
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;
        // Nothing but the range tombstone in the next flush.
        tree.remove_range(
            lsm5::UserKey::from("k00020"),
            lsm5::UserKey::from("k00150"),
            u64::from(keys) + 1,
        );
        tree.flush_active_memtable(0)?;
    }
    let open = OpenWith::default();
    let expected = read_v5(folder.path(), keys, &open)?;

    v5_to_v6::convert(folder.path(), &v5_to_v6::Options::default())?;
    assert_eq!(read_v6(folder.path(), keys, &open)?, expected);
    Ok(())
}

/// Every table of the source store, opened through the 5.x export.
fn source_tables(folder: &Path) -> lsm5::Result<Vec<lsm5::export::TableExport>> {
    let state = lsm5::export::read_manifest(folder, &lsm5::fs::StdFs, None)?;
    let context = lsm5::export::TableContext::new(
        &state,
        std::sync::Arc::new(lsm5::fs::StdFs),
        None,
        lsm5::export::read_dictionaries(folder, &lsm5::fs::StdFs, None)?,
        std::sync::Arc::new(lsm5::DefaultUserComparator),
    )?;
    state
        .levels
        .iter()
        .flatten()
        .flatten()
        .map(|record| {
            lsm5::export::TableExport::open(
                &folder.join("tables").join(record.id.to_string()),
                record,
                None,
                &context,
            )
        })
        .collect()
}

/// A locator is carried over and still resolves every key to its block.
#[test]
fn a_converted_store_with_a_locator_answers_as_the_source_did()
-> Result<(), Box<dyn std::error::Error>> {
    round_trip_checked(
        300,
        |config| {
            config.locator_policy(lsm5::config::LocatorPolicy::all(
                lsm5::config::LocatorPolicyEntry::Enabled {
                    precision: lsm5::config::LocatorPrecision::Entry,
                    block_id_bits: None,
                    slot_bits: None,
                },
            ))
        },
        |folder| {
            let with_locator = source_tables(folder)?
                .iter()
                .map(|table| table.locator().map(|l| l.is_some()))
                .collect::<lsm5::Result<Vec<_>>>()?;
            assert!(
                with_locator.iter().any(|&has| has),
                "a source table holds a locator"
            );
            Ok(())
        },
    )
}
