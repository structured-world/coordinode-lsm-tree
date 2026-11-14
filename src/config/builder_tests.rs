use super::*;
use crate::SequenceNumberCounter;

/// The frame sizes a blob file is written with keep every frame a reader can
/// decode: a chunk within the frame cap, and a block target within the chunk
/// size, so a value above the chunk size is always chunked, and within half
/// the cap, so a block, which takes values below its target until 90% full,
/// never passes the cap. Unbounded, a large block size failed every flush.
#[test]
fn blob_frame_sizes_keep_every_frame_within_the_cap() {
    use crate::vlog::blob_file::writer::MAX_DECOMPRESSION_SIZE;

    let Ok(cap) = u32::try_from(MAX_DECOMPRESSION_SIZE) else {
        panic!("the cap fits u32");
    };
    let Some(huge) = core::num::NonZeroU32::new(u32::MAX) else {
        panic!("non-zero");
    };

    let sizes = KvSeparationOptions::default()
        .block_size(u32::MAX)
        .frame_sizes();
    assert_eq!(
        sizes.block_target, sizes.chunk_size,
        "a block never takes a value the chunk size splits"
    );

    let sizes = KvSeparationOptions::default()
        .block_size(u32::MAX)
        .chunk_size(huge)
        .frame_sizes();
    assert_eq!(sizes.chunk_size, cap, "a chunk is one frame");
    assert_eq!(sizes.block_target, cap / 2, "a block stays below the cap");

    let sizes = KvSeparationOptions::default().frame_sizes();
    assert_eq!(
        (sizes.block_target, sizes.chunk_size),
        (8 * 1024, 256 * 1024),
        "the defaults are within every bound"
    );
}

/// The bytes one value's frames take are recorded in a u32: a chunk size of a
/// byte splits a value at the cap into 2^28 chunks, each with a row in its
/// head's table, past what the record holds, and garbage accounting and the
/// space a relocation reserves would undercount by gigabytes. The chunk size
/// is held to a floor under which every value's frames fit.
#[test]
fn blob_chunk_size_keeps_a_value_s_frames_within_their_record() {
    use crate::vlog::blob_file::{
        frame::{CHUNK_ROW_LEN, ENTRY_HEADER_LEN, FRAME_HEADER_LEN},
        writer::MAX_DECOMPRESSION_SIZE,
    };

    let Some(one) = core::num::NonZeroU32::new(1) else {
        panic!("non-zero");
    };
    let sizes = KvSeparationOptions::default().chunk_size(one).frame_sizes();
    let chunks = (MAX_DECOMPRESSION_SIZE as u64).div_ceil(u64::from(sizes.chunk_size));
    // Every chunk's row, the value stored raw at most, and a head with the
    // longest key and its table's fixed part.
    let most = chunks * CHUNK_ROW_LEN as u64
        + MAX_DECOMPRESSION_SIZE as u64
        + (FRAME_HEADER_LEN + ENTRY_HEADER_LEN + usize::from(u16::MAX) + 8) as u64;
    assert!(
        u32::try_from(most).is_ok(),
        "a value at the cap takes {most} bytes at chunk size {}",
        sizes.chunk_size
    );
}

#[test]
fn restart_interval_policies_can_be_overridden_independently() {
    let folder = match tempfile::tempdir() {
        Ok(folder) => folder,
        Err(err) => panic!("tempdir failed: {err}"),
    };
    let cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_restart_interval_policy(RestartIntervalPolicy::all(7))
    .index_block_restart_interval_policy(RestartIntervalPolicy::all(3));

    assert_eq!(cfg.data_block_restart_interval_policy.first(), Some(&7));
    assert_eq!(cfg.index_block_restart_interval_policy.first(), Some(&3));
}

#[test]
fn fs_aware_builders_thread_to_initial_runtime_config() -> crate::Result<()> {
    // The CoW-disable + reflink toggles default ON and flip via the builder
    // (AC: "controlled via builder"). Verifies the builder threads to the
    // initial RuntimeConfig the Tree opens with; a wiring regression would
    // silently ignore the user's setting. Lives in the ungated builder
    // tests (the behaviour is unrelated to zstd).
    let folder = tempfile::tempdir()?;
    let mk = || {
        Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
    };

    let dflt = mk();
    assert!(dflt.initial_runtime_config.disable_cow_on_sst_files);
    assert!(dflt.initial_runtime_config.use_reflink_for_checkpoint);

    let off = mk()
        .disable_cow_on_sst_files(false)
        .use_reflink_for_checkpoint(false);
    assert!(!off.initial_runtime_config.disable_cow_on_sst_files);
    assert!(!off.initial_runtime_config.use_reflink_for_checkpoint);
    Ok(())
}

#[test]
#[should_panic(expected = "index block restart interval must be greater than zero")]
fn index_restart_interval_policy_rejects_zero_values() {
    let folder = match tempfile::tempdir() {
        Ok(folder) => folder,
        Err(err) => panic!("tempdir failed: {err}"),
    };
    let _cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_restart_interval_policy(RestartIntervalPolicy::all(0));
}

#[test]
#[should_panic(expected = "data block restart interval must be greater than zero")]
fn data_restart_interval_policy_rejects_zero_values() {
    let folder = match tempfile::tempdir() {
        Ok(folder) => folder,
        Err(err) => panic!("tempdir failed: {err}"),
    };
    let _cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_restart_interval_policy(RestartIntervalPolicy::all(0));
}

#[test]
#[should_panic(expected = "restart interval policy may not be empty")]
fn index_restart_interval_policy_rejects_empty() {
    let folder = match tempfile::tempdir() {
        Ok(folder) => folder,
        Err(err) => panic!("tempdir failed: {err}"),
    };
    let _cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .index_block_restart_interval_policy(RestartIntervalPolicy::new([]));
}

#[test]
#[should_panic(expected = "restart interval policy may not be empty")]
fn data_restart_interval_policy_rejects_empty() {
    let folder = match tempfile::tempdir() {
        Ok(folder) => folder,
        Err(err) => panic!("tempdir failed: {err}"),
    };
    let _cfg = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_restart_interval_policy(RestartIntervalPolicy::new([]));
}
