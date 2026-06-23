use super::*;
use test_log::test;

// These tests assert std wall-clock behaviour, so they are gated behind
// `feature = "std"`: under `--no-default-features` the `std` feature is off,
// `SystemClock` is absent, and `unix_timestamp()` takes the no_std (epoch)
// branch where these assertions would not hold.

#[cfg(feature = "std")]
#[test]
fn system_clock_unix_time_is_after_a_known_recent_epoch() {
    // Any wall-clock reading taken after this fixed past instant (2023-11-14)
    // proves SystemClock consults the real system clock rather than a stub.
    let known_past = Duration::from_secs(1_700_000_000);
    assert!(
        SystemClock.unix_time() > known_past,
        "SystemClock must read the real system clock"
    );
}

// The no-override and override cases share one test so they cannot observe
// each other's mutation of the process-global test override. (The suite runs
// under nextest, which isolates each test in its own process, but keeping the
// override roundtrip self-contained holds under any runner.)
#[cfg(feature = "std")]
#[test]
fn unix_timestamp_honours_the_test_override() {
    // With no override registered, the free `unix_timestamp` helper reads
    // through the same SystemClock the rest of the engine uses.
    set_unix_timestamp_for_test(None);
    assert!(
        unix_timestamp() > Duration::from_secs(1_700_000_000),
        "without an override, unix_timestamp must read the real system clock"
    );

    // An override pins the value regardless of the wall-clock.
    let fixed = Duration::from_secs(42);
    set_unix_timestamp_for_test(Some(fixed));
    assert_eq!(unix_timestamp(), fixed);

    // Restore so later tests in this binary see the real clock again.
    set_unix_timestamp_for_test(None);
}
