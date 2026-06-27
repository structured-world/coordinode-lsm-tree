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

/// The prewarm path must never gather an ECC table's blocks: `from_reader`
/// repairs an ECC-corrected payload silently (no `EccStatus`), so caching a
/// corrected block as clean would let later `load_block` cache hits skip
/// `from_file_with_recovery` / `maybe_record_persistent_heal`, leaving the
/// latent on-disk fault unscheduled for healing. `decode_prewarmed_blocks`
/// relies on this gate (and pins it with a `debug_assert!(ecc.is_none())`), so
/// guard the invariant directly: `plan_prewarm` returns `None` for an ECC table.
#[test]
fn plan_prewarm_skips_ecc_tables() -> crate::Result<()> {
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
        table.plan_prewarm(&sorted_keys, MAX_SEQNO).is_none(),
        "plan_prewarm must skip an ECC table so a silently-corrected block is \
         never prewarm-cached as clean",
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

    // Opt into the partial-decode path for this test process (OnceLock-cached;
    // nextest isolates each test in its own process so this does not leak).
    // SAFETY: set before any tree in this process reads the env.
    unsafe { std::env::set_var("LSM_PARTIAL_DECODE", "1") };

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
