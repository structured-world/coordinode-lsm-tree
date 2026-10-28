use crate::{
    AbstractTree,
    Config,
    MAX_SEQNO,
    SequenceNumberCounter,
    runtime_config::EccScheme,
    // `BlockIndex` is imported only for its `.iter()` method on
    // `table.block_index` (a trait method); `as _` keeps it in scope for
    // method resolution without binding the unused type name.
    table::{block::Header, block_index::BlockIndex as _},
};
use alloc::sync::Arc;

/// Full self-heal cycle: a read that recovers a corrupted data block via RS
/// parity records the SST; running [`super::Strategy`] rewrites that SST so
/// the re-read is clean and records no further hint.
#[test]
fn ecc_heal_strategy_rewrites_flagged_sst_so_reread_is_clean() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    // Write one SST whose data blocks carry RS(8,2) parity, then capture the
    // first data block's on-disk offset before dropping the tree.
    let (sst_path, corrupt_pos) = {
        let tree = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .open()?;
        let crate::AnyTree::Standard(tree) = tree else {
            unreachable!("standard tree configured (no kv separation)");
        };
        for i in 0u64..2_000 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(2_000)?;

        let binding = tree.version_history.read().latest_version();
        #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
        let table = binding.version.iter_tables().next().expect("one table");
        #[expect(clippy::expect_used, reason = "table has at least one data block")]
        let keyed = table.block_index.iter().next().expect("a data block")?;
        // Target-conditional truncation: `as usize` only narrows on 32-bit
        // pointer widths, so `allow` (not `expect`) keeps it clean on the
        // 64-bit host where clippy frames it as a portability note.
        #[allow(
            clippy::cast_possible_truncation,
            reason = "in-file block offset fits usize; only narrows on 32-bit targets"
        )]
        let off = keyed.offset().0 as usize;
        ((*table.path).clone(), off + Header::MIN_LEN + 3)
    };

    // Flip one payload byte of the first data block (RS-correctable).
    let mut bytes = std::fs::read(&sst_path)?;
    #[expect(
        clippy::indexing_slicing,
        reason = "corrupt_pos is an in-file block offset, in range for the SST bytes"
    )]
    let slot = &mut bytes[corrupt_pos];
    *slot ^= 0x80;
    std::fs::write(&sst_path, &bytes)?;

    // Reopen (fresh caches + fds) so the read hits the tampered bytes.
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        unreachable!("standard tree configured (no kv separation)");
    };
    assert!(tree.heal_hints().is_empty(), "fresh tree has no hints");

    // Opt into rewrite scheduling (default off). The gate tracks auto_heal.
    assert!(!tree.heal_hints().is_enabled(), "auto_heal defaults off");
    tree.update_runtime_config(|c| c.auto_heal = true)?;
    assert!(
        tree.heal_hints().is_enabled(),
        "auto_heal toggle syncs the gate"
    );

    // A read of a key in the corrupted block repairs it (correct value) and
    // records the SST for healing.
    #[expect(clippy::expect_used, reason = "key was inserted before flush")]
    let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
    assert_eq!(&*got, b"v000000", "ECC must repair the value on read");
    assert!(
        !tree.heal_hints().is_empty(),
        "a persistent ECC correction must record a heal hint",
    );
    #[cfg(feature = "metrics")]
    {
        assert_eq!(
            tree.metrics().ecc_auto_heal_scheduled_count(),
            1,
            "the scheduled SST is counted once",
        );
        // The recovery is attributed to the RS shard path (this SST uses an
        // RS scheme), not SEC-DED, and counted once on the primary read.
        assert_eq!(
            tree.metrics().ecc_shard_recovered_count(),
            1,
            "the RS recovery is counted once",
        );
        assert_eq!(
            tree.metrics().ecc_secded_corrected_count(),
            0,
            "an RS recovery must not increment the SEC-DED counter",
        );
        assert_eq!(
            tree.metrics().ecc_recovered_count(),
            1,
            "one total recovery"
        );
    }

    // Run the heal strategy: it claims the SST and rewrites it clean.
    let result = tree.compact(
        Arc::new(super::Strategy::new(tree.heal_hints(), u64::MAX)),
        0,
    )?;
    assert!(
        tree.heal_hints().is_empty(),
        "heal compaction must drain the hint queue, got {result:?}",
    );

    // The healed SST reads clean: correct value, no fresh correction hint.
    #[expect(clippy::expect_used, reason = "key survives the rewrite")]
    let got = tree
        .get(b"key-000000", MAX_SEQNO)?
        .expect("key present after heal");
    assert_eq!(&*got, b"v000000", "healed value must still be correct");
    assert!(
        tree.heal_hints().is_empty(),
        "the rewritten SST must read clean (no further correction)",
    );

    Ok(())
}

/// Where a [`FlipOnce`] flips its byte: a file whose path holds `name`, at
/// `offset`, for the first read covering it.
#[derive(Default)]
struct FlipTarget {
    target: std::sync::Mutex<Option<(String, u64)>>,
}

/// A file system flipping one byte of one read, as a transient fault on the
/// way from the medium does: the file itself stays intact.
struct FlipOnce {
    inner: crate::fs::StdFs,
    flip: Arc<FlipTarget>,
}

/// A file of a [`FlipOnce`], flipping its byte when a read covers it.
struct FlipFile {
    inner: Box<dyn crate::fs::FsFile>,
    path: String,
    flip: Arc<FlipTarget>,
}

impl std::io::Read for FlipFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl std::io::Write for FlipFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for FlipFile {
    fn seek(&mut self, to: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(to)
    }
}

impl crate::fs::FsFile for FlipFile {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.inner.sync_all()
    }
    fn sync_data(&self) -> crate::io::Result<()> {
        self.inner.sync_data()
    }
    fn metadata(&self) -> crate::io::Result<crate::fs::FsMetadata> {
        self.inner.metadata()
    }
    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.inner.set_len(size)
    }
    fn lock_exclusive(&self) -> crate::io::Result<()> {
        self.inner.lock_exclusive()
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        let n = self.inner.read_at(buf, offset)?;
        let hit = self
            .flip
            .target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_if(|(name, at)| {
                self.path.contains(name.as_str()) && (offset..offset + n as u64).contains(at)
            })
            .map(|(_, at)| at);
        if let Some(at) = hit {
            #[expect(clippy::cast_possible_truncation, reason = "within this read")]
            if let Some(byte) = buf.get_mut((at - offset) as usize) {
                *byte ^= 0x80;
            }
        }
        Ok(n)
    }
}

impl crate::fs::Fs for FlipOnce {
    fn open(
        &self,
        path: &std::path::Path,
        opts: &crate::fs::FsOpenOptions,
    ) -> crate::io::Result<Box<dyn crate::fs::FsFile>> {
        Ok(Box::new(FlipFile {
            inner: self.inner.open(path, opts)?,
            path: path.to_string_lossy().into_owned(),
            flip: Arc::clone(&self.flip),
        }))
    }
    fn create_dir_all(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn read_dir(&self, path: &std::path::Path) -> crate::io::Result<Vec<crate::fs::FsDirEntry>> {
        self.inner.read_dir(path)
    }
    fn remove_file(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn remove_dir_all(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.inner.remove_dir_all(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> crate::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn metadata(&self, path: &std::path::Path) -> crate::io::Result<crate::fs::FsMetadata> {
        self.inner.metadata(path)
    }
    fn sync_directory(&self, path: &std::path::Path) -> crate::io::Result<()> {
        self.inner.sync_directory(path)
    }
    fn exists(&self, path: &std::path::Path) -> crate::io::Result<bool> {
        self.inner.exists(path)
    }
}

/// A block whose parity corrects a fault that a second read no longer shows
/// was hurt on the way from the medium, not on it: the read returns the right
/// value and nothing is queued for healing. Guards against rewriting a table
/// for a fault its file does not hold.
#[test]
fn a_transient_ecc_correction_queues_no_heal() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let flip = Arc::new(FlipTarget::default());
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(FlipOnce {
        inner: crate::fs::StdFs,
        flip: Arc::clone(&flip),
    });
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_shared_fs(fs)
    // Every read of a block goes to the file.
    .use_cache(Arc::new(crate::Cache::with_capacity_bytes(0)))
    .page_ecc(true)
    .ecc_scheme(EccScheme::ReedSolomon {
        data_shards: 8,
        parity_shards: 2,
    })
    .open()?
    else {
        unreachable!("standard tree configured (no kv separation)");
    };
    for i in 0u64..2_000 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.flush_active_memtable(2_000)?;
    tree.update_runtime_config(|c| c.auto_heal = true)?;

    let binding = tree.version_history.read().latest_version();
    #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
    let table = binding.version.iter_tables().next().expect("one table");
    #[expect(clippy::expect_used, reason = "table has at least one data block")]
    let keyed = table.block_index.iter().next().expect("a data block")?;
    let name = table
        .path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let at = keyed.offset().0 + Header::MIN_LEN as u64 + 3;
    *flip
        .target
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((name, at));

    #[expect(clippy::expect_used, reason = "key was inserted before flush")]
    let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
    assert_eq!(&*got, b"v000000", "ECC must repair the value on read");
    assert!(
        flip.target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none(),
        "the case under test: the read met the flipped byte"
    );
    assert!(
        tree.heal_hints().is_empty(),
        "a fault the re-read does not show queues no heal"
    );
    #[cfg(feature = "metrics")]
    assert_eq!(tree.metrics().ecc_recovered_count(), 1, "one recovery");
    Ok(())
}

/// End-to-end SEC-DED recovery through a tree read: a single-bit flip in a
/// SEC-DED-protected SST is healed on read by the SEC-DED fast path and
/// attributed to the SEC-DED counter (NOT the RS shard counter), proving the
/// unified recovery metric distinguishes the two heal mechanisms. Gated on
/// `metrics`: the counter is the whole point.
#[cfg(feature = "metrics")]
#[test]
fn read_healing_single_bit_increments_secded_counter() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;

    // Write one SST whose data blocks carry SEC-DED parity, capture the
    // first data block's on-disk offset.
    let (sst_path, corrupt_pos) = {
        let crate::AnyTree::Standard(tree) = Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(EccScheme::Secded)
        .open()?
        else {
            unreachable!("standard tree configured (no kv separation)");
        };
        for i in 0u64..2_000 {
            tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
        }
        tree.flush_active_memtable(2_000)?;

        let binding = tree.version_history.read().latest_version();
        #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
        let table = binding.version.iter_tables().next().expect("one table");
        #[expect(clippy::expect_used, reason = "table has at least one data block")]
        let keyed = table.block_index.iter().next().expect("a data block")?;
        // Target-conditional truncation: `as usize` only narrows on 32-bit
        // pointer widths, so `allow` (not `expect`) keeps it clean on the
        // 64-bit host.
        #[allow(
            clippy::cast_possible_truncation,
            reason = "in-file block offset fits usize; only narrows on 32-bit targets"
        )]
        let off = keyed.offset().0 as usize;
        ((*table.path).clone(), off + Header::MIN_LEN + 3)
    };

    // Flip a SINGLE bit: the SEC-DED fast path corrects one bit per word.
    let mut bytes = std::fs::read(&sst_path)?;
    {
        #[expect(
            clippy::expect_used,
            reason = "corrupt_pos is an in-file block offset, in range for the SST bytes"
        )]
        let slot = bytes.get_mut(corrupt_pos).expect("corrupt_pos in range");
        *slot ^= 0x01;
    }
    std::fs::write(&sst_path, &bytes)?;

    // Reopen (fresh caches + fds) so the read hits the tampered bytes.
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::Secded)
    .open()?
    else {
        unreachable!("standard tree configured (no kv separation)");
    };

    #[expect(clippy::expect_used, reason = "key was inserted before flush")]
    let got = tree.get(b"key-000000", MAX_SEQNO)?.expect("key present");
    assert_eq!(&*got, b"v000000", "SEC-DED must heal the single-bit flip");

    assert_eq!(
        tree.metrics().ecc_secded_corrected_count(),
        1,
        "the SEC-DED heal is counted once",
    );
    assert_eq!(
        tree.metrics().ecc_shard_recovered_count(),
        0,
        "a SEC-DED heal must not increment the RS shard counter",
    );
    assert_eq!(
        tree.metrics().ecc_recovered_count(),
        1,
        "one total recovery"
    );
    Ok(())
}

/// The staged multi-get read must never gather an ECC table's blocks:
/// `from_reader` repairs an ECC-corrected payload silently (no `EccStatus`),
/// so caching a corrected block as clean would let later `load_block` cache
/// hits skip `from_file_with_recovery` / `maybe_record_persistent_heal`,
/// leaving the latent on-disk fault unscheduled for healing. The decode it
/// uses relies on this gate (and pins it with a `debug_assert!`), so guard the
/// invariant directly: an ECC table is read serially.
#[test]
fn a_staged_read_leaves_ecc_tables_to_the_serial_path() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let crate::AnyTree::Standard(tree) = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .page_ecc(true)
    .ecc_scheme(EccScheme::Secded)
    .open()?
    else {
        unreachable!("standard tree configured (no kv separation)");
    };
    for i in 0u64..2_000 {
        tree.insert(format!("key-{i:06}"), format!("v{i:06}"), i);
    }
    tree.flush_active_memtable(2_000)?;

    let binding = tree.version_history.read().latest_version();
    #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
    let table = binding.version.iter_tables().next().expect("one table");

    let sorted_keys: [(&[u8], u64); 2] = [
        (b"key-000000", crate::hash::hash64(b"key-000000")),
        (b"key-001000", crate::hash::hash64(b"key-001000")),
    ];
    assert!(
        matches!(
            crate::table::staged::StagedRead::start(table, &sorted_keys, MAX_SEQNO, true),
            crate::table::staged::StagedStart::Serial
        ),
        "a staged read must leave an ECC table to the serial path so a \
         silently-corrected block is never cached as clean",
    );
    Ok(())
}

/// The zstd partial-decode read path also schedules auto-heal: a bounded
/// range scan that takes the partial path over a corrupted large block
/// recovers via parity and flags the SST, just like the full load path.
#[cfg(feature = "zstd")]
#[test]
fn ecc_heal_scheduled_on_partial_decode_corrected_read() -> crate::Result<()> {
    use crate::{
        CompressionType,
        config::{BlockSizePolicy, CompressionPolicy},
    };

    // Engages the partial-decode path on this thread only: no environment
    // mutation, and no dependence on which test first read the cached switch.
    crate::table::iter::force_partial_decode_on_this_thread();

    let dir = tempfile::tempdir()?;
    let open = || {
        Config::new(
            dir.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .page_ecc(true)
        .ecc_scheme(EccScheme::ReedSolomon {
            data_shards: 8,
            parity_shards: 2,
        })
        .data_block_compression_policy(CompressionPolicy::all(
            #[expect(clippy::expect_used, reason = "19 is a valid zstd level")]
            CompressionType::zstd(19).expect("valid level"),
        ))
        .data_block_size_policy(BlockSizePolicy::all(512 * 1024))
        .open()
    };

    // Build a large multi-inner-block zstd+ECC SST; capture the first data
    // block's on-disk offset before dropping the tree.
    let (sst_path, corrupt_pos) = {
        let crate::AnyTree::Standard(tree) = open()? else {
            unreachable!("standard tree configured");
        };
        for i in 0u64..20_000 {
            tree.insert(
                format!("key-{i:08}"),
                format!("value-{i:08}-padding-padding"),
                0,
            );
        }
        tree.flush_active_memtable(0)?;

        let binding = tree.version_history.read().latest_version();
        #[expect(clippy::expect_used, reason = "flush produced exactly one table")]
        let table = binding.version.iter_tables().next().expect("one table");
        #[expect(clippy::expect_used, reason = "table has at least one data block")]
        let keyed = table.block_index.iter().next().expect("a data block")?;
        // Target-conditional truncation: `as usize` only narrows on 32-bit
        // pointer widths, so `allow` (not `expect`) keeps it clean on the
        // 64-bit host where clippy frames it as a portability note.
        #[allow(
            clippy::cast_possible_truncation,
            reason = "in-file block offset fits usize; only narrows on 32-bit targets"
        )]
        let off = keyed.offset().0 as usize;
        ((*table.path).clone(), off + Header::MIN_LEN + 8)
    };

    // Flip one byte of the first block's compressed frame (RS-correctable).
    let mut bytes = std::fs::read(&sst_path)?;
    #[expect(
        clippy::indexing_slicing,
        reason = "corrupt_pos is an in-file block offset, in range for the SST bytes"
    )]
    let slot = &mut bytes[corrupt_pos];
    *slot ^= 0x01;
    std::fs::write(&sst_path, &bytes)?;

    // Reopen (fresh caches/fds), opt into healing, then run a bounded range
    // scan whose upper bound falls inside the first block so the read takes
    // the partial-decode path.
    let crate::AnyTree::Standard(tree) = open()? else {
        unreachable!("standard tree configured");
    };
    tree.update_runtime_config(|c| c.auto_heal = true)?;

    let count = tree
        .range(
            b"key-00000000".to_vec()..b"key-00000050".to_vec(),
            MAX_SEQNO,
            None,
        )
        .count();
    assert!(count > 0, "bounded range returned rows");
    assert!(
        !tree.heal_hints().is_empty(),
        "a corrected read on the partial-decode path must schedule healing",
    );

    Ok(())
}
