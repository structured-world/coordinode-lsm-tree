use super::*;
use crate::SequenceNumberCounter;

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
