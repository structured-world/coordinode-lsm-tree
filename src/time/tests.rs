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

#[cfg(feature = "std")]
#[test]
fn unix_timestamp_honours_the_test_override() {
    // With no override registered, the free `unix_timestamp` helper reads
    // through the same SystemClock the rest of the engine uses.
    let clock = TestClock::take();
    assert!(
        unix_timestamp() > Duration::from_secs(1_700_000_000),
        "without an override, unix_timestamp must read the real system clock"
    );

    // An override pins the value regardless of the wall-clock.
    clock.set_secs(42);
    assert_eq!(unix_timestamp(), Duration::from_secs(42));
}

/// The override is process-wide: dropping the guard must clear it, so a test
/// that returns early cannot leave a pinned clock to the next one.
#[cfg(feature = "std")]
#[test]
fn dropping_the_test_clock_clears_the_override() {
    drop(TestClock::at_secs(42));
    let _clock = TestClock::take();
    assert!(
        unix_timestamp() > Duration::from_secs(1_700_000_000),
        "a dropped guard must not leave the clock pinned"
    );
}

/// Two tests that pin the clock in one process must not interleave: the
/// second takes the clock only after the first has dropped its guard.
#[cfg(feature = "std")]
#[test]
fn a_second_test_clock_waits_for_the_first() {
    let first = TestClock::at_secs(42);
    let handle = std::thread::spawn(|| {
        let _second = TestClock::at_secs(7);
        unix_timestamp()
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        unix_timestamp(),
        Duration::from_secs(42),
        "the held clock must not be overridden by a waiting test"
    );
    drop(first);
    let Ok(seen) = handle.join() else {
        panic!("the second clock user panicked");
    };
    assert_eq!(seen, Duration::from_secs(7));
}
