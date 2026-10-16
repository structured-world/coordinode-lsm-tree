use super::{Claim, InflightLoads, LOAD_SAMPLE_EVERY, LoadTimer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

type Loads = InflightLoads<u64, Arc<Vec<u8>>, u32, rustc_hash::FxBuildHasher>;

/// A registry that coordinates every miss, whatever its loads take.
fn loads(per_shard_limit: usize) -> Arc<Loads> {
    Arc::new(InflightLoads::new(
        4,
        per_shard_limit,
        Duration::ZERO,
        rustc_hash::FxBuildHasher,
    ))
}

/// Readers that miss the same key at once do the load once: one leads, the
/// others take its value, and the registry is empty afterwards.
#[test]
fn simultaneous_claims_of_one_key_load_it_once() {
    const READERS: usize = 16;
    let loads = loads(8);
    let barrier = Arc::new(Barrier::new(READERS));
    let loaded = Arc::new(AtomicUsize::new(0));
    // Holds the leader inside its load until every reader has claimed, so the
    // others find its ticket instead of the closed registry.
    let claimed = Arc::new(Barrier::new(READERS));

    let handles: Vec<_> = (0..READERS)
        .map(|_| {
            let (loads, barrier, loaded, claimed) = (
                Arc::clone(&loads),
                Arc::clone(&barrier),
                Arc::clone(&loaded),
                Arc::clone(&claimed),
            );
            std::thread::spawn(move || {
                barrier.wait();
                match loads.claim(7, 1) {
                    Claim::Lead(lead) => {
                        claimed.wait();
                        loaded.fetch_add(1, Ordering::SeqCst);
                        let value = Arc::new(vec![7u8; 4]);
                        lead.publish(Arc::clone(&value));
                        value
                    }
                    Claim::Join(join) => {
                        claimed.wait();
                        join.wait().expect("the leader published")
                    }
                    Claim::Alone => panic!("a compatible load was in flight"),
                }
            })
        })
        .collect();

    for handle in handles {
        assert_eq!(*handle.join().unwrap(), vec![7u8; 4]);
    }
    assert_eq!(loaded.load(Ordering::SeqCst), 1);
    assert_eq!(loads.len(), 0, "no ticket outlives its load");
}

/// A leader that returns without publishing (an error) releases its waiters
/// with nothing, so each loads on its own, and leaves no ticket behind.
#[test]
fn abandoned_load_releases_waiters_empty_handed() {
    let loads = loads(8);
    let Claim::Lead(lead) = loads.claim(1, 1) else {
        panic!("the first claim leads");
    };
    let Claim::Join(join) = loads.claim(1, 1) else {
        panic!("a second claim joins");
    };
    drop(lead);
    assert_eq!(join.wait(), None);
    assert_eq!(loads.len(), 0);
}

/// A leader that unwinds abandons its load like one that errors: a waiter on
/// another thread is woken with nothing instead of waiting forever.
#[test]
fn unwinding_leader_wakes_its_waiters() {
    let loads = loads(8);
    let Claim::Lead(lead) = loads.claim(3, 1) else {
        panic!("the first claim leads");
    };
    let Claim::Join(join) = loads.claim(3, 1) else {
        panic!("a second claim joins");
    };
    let waiter = std::thread::spawn(move || join.wait());
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _held = lead;
        panic!("the load panics");
    }));
    assert!(unwound.is_err());
    assert_eq!(waiter.join().unwrap(), None);
    assert_eq!(loads.len(), 0);
}

/// A request whose identity differs from the load in flight (another extent,
/// role or codec under the same key) never takes that load's result.
#[test]
fn incompatible_request_loads_alone() {
    let loads = loads(8);
    let Claim::Lead(lead) = loads.claim(5, 1) else {
        panic!("the first claim leads");
    };
    assert!(matches!(loads.claim(5, 2), Claim::Alone));
    lead.publish(Arc::new(vec![1]));
    assert_eq!(loads.len(), 0);
}

/// Distinct keys are independent: each leads its own load at the same time.
#[test]
fn distinct_keys_lead_independently() {
    let loads = loads(8);
    let Claim::Lead(first) = loads.claim(10, 1) else {
        panic!("leads");
    };
    let Claim::Lead(second) = loads.claim(11, 1) else {
        panic!("another key leads too");
    };
    assert_eq!(loads.len(), 2);
    first.publish(Arc::new(vec![1]));
    second.publish(Arc::new(vec![2]));
    assert_eq!(loads.len(), 0);
}

/// A full shard stops coordinating instead of growing: the next request of a
/// new key loads alone, and coordination resumes once a load closes.
#[test]
fn full_registry_degrades_to_loading_alone() {
    // One shard, so every key lands in it.
    let loads: Arc<Loads> = Arc::new(InflightLoads::new(
        1,
        1,
        Duration::ZERO,
        rustc_hash::FxBuildHasher,
    ));
    let Claim::Lead(lead) = loads.claim(20, 1) else {
        panic!("leads");
    };
    assert!(matches!(loads.claim(21, 1), Claim::Alone));
    lead.publish(Arc::new(vec![]));
    assert!(matches!(loads.claim(21, 1), Claim::Lead(_)));
}

/// A reader that must not wait sees a load's value only once it is published:
/// nothing while it is pending, the value after publish, and nothing after the
/// leader gave up.
#[test]
fn try_take_never_waits() {
    let loads = loads(8);
    let Claim::Lead(lead) = loads.claim(40, 1) else {
        panic!("leads");
    };
    let Claim::Join(pending) = loads.claim(40, 1) else {
        panic!("joins");
    };
    assert_eq!(pending.try_take(), None, "still loading");
    lead.publish(Arc::new(vec![4]));
    assert_eq!(pending.try_take(), Some(Arc::new(vec![4])));

    let Claim::Lead(lead) = loads.claim(41, 1) else {
        panic!("leads");
    };
    let Claim::Join(failed) = loads.claim(41, 1) else {
        panic!("joins");
    };
    drop(lead);
    assert_eq!(failed.try_take(), None, "a failed load hands nothing out");
}

/// Nothing is remembered once a load closes, success or failure: the next
/// miss of the key leads a load of its own.
#[test]
fn closed_load_is_not_remembered() {
    let loads = loads(8);
    let Claim::Lead(lead) = loads.claim(30, 1) else {
        panic!("leads");
    };
    drop(lead);
    let Claim::Lead(lead) = loads.claim(30, 1) else {
        panic!("a failed load is not cached");
    };
    lead.publish(Arc::new(vec![9]));
    assert!(
        matches!(loads.claim(30, 1), Claim::Lead(_)),
        "a finished load is not cached either"
    );
}

/// A registry that coordinates once loads take 10 µs, its loads not yet
/// timed. One shard, so every key's samples meet in one estimate.
fn timed_loads() -> Loads {
    InflightLoads::new(1, 8, Duration::from_micros(10), rustc_hash::FxBuildHasher)
}

/// While loads are quicker than the threshold, a miss loads alone and leaves
/// nothing in the registry: two concurrent misses of one key both load, as
/// they would with no registry at all, and a non-waiting reader finds
/// nothing to take.
#[test]
fn quick_loads_do_not_coordinate() {
    let loads = timed_loads();
    for _ in 0..64 {
        loads.fold_sample(&1, 2_000);
    }
    assert!(matches!(loads.claim(1, 1), Claim::Alone));
    assert!(matches!(loads.claim(1, 1), Claim::Alone));
    assert_eq!(loads.len(), 0, "no ticket is taken");
    assert_eq!(loads.take_ready(&1, &1), None);
}

/// Loads that turn slow switch coordination on, and turning quick again
/// switches it off: the decision follows the measured loads both ways.
#[test]
fn coordination_follows_the_measured_load_time() {
    let loads = timed_loads();
    assert!(
        matches!(loads.claim(2, 1), Claim::Alone),
        "nothing measured yet: loads alone"
    );
    for _ in 0..64 {
        loads.fold_sample(&2, 1_000_000);
    }
    let Claim::Lead(lead) = loads.claim(2, 1) else {
        panic!("slow loads coordinate");
    };
    assert!(matches!(loads.claim(2, 1), Claim::Join(_)));
    lead.publish(Arc::new(vec![2]));
    for _ in 0..64 {
        loads.fold_sample(&2, 1_000);
    }
    assert!(
        matches!(loads.claim(2, 1), Claim::Alone),
        "quick again: loads alone"
    );
}

/// The estimate moves an eighth of the way to each sample, and stays within
/// range for the longest sample there is.
#[test]
fn estimate_moves_an_eighth_toward_each_sample() {
    let loads = timed_loads();
    let estimate = || loads.estimate(&3);
    loads.fold_sample(&3, 800);
    assert_eq!(estimate(), 100);
    loads.fold_sample(&3, 800);
    assert_eq!(estimate(), 100 - 12 + 100);
    for _ in 0..512 {
        loads.fold_sample(&3, u32::MAX);
    }
    assert!(estimate() > u32::MAX / 8 * 7, "converges toward the sample");
}

/// A timed wait of a batched read counts as its length shared among the
/// reads it handed back, in every shard: a batch of slow reads switches
/// sharing on for keys of any shard, and a wait that handed back nothing
/// tells nothing.
#[test]
fn a_batched_wait_feeds_every_shard_its_share_per_read() {
    std::thread::spawn(|| {
        let loads: Loads =
            InflightLoads::new(8, 8, Duration::from_micros(100), rustc_hash::FxBuildHasher);
        let timed = std::iter::repeat_with(LoadTimer::start)
            .find(|timer| timer.started.is_some())
            .expect("one in the sampling period is timed");
        loads.record_batch_wait(timed, 0);
        assert!((0..64u64).all(|key| loads.estimate(&key) == 0));

        std::thread::sleep(Duration::from_millis(20));
        // Twenty milliseconds over four reads: five each, far past 100 µs.
        loads.record_batch_wait(timed, 4);
        for key in 0..64u64 {
            let estimate = loads.estimate(&key);
            assert!(
                (5_000_000 / 8..250_000_000 / 8).contains(&estimate),
                "key {key}: {estimate}"
            );
        }
    })
    .join()
    .expect("sampling thread");
}

/// One load in `LOAD_SAMPLE_EVERY` of a thread is timed, and a timed one
/// feeds the estimate when recorded.
#[test]
fn one_load_in_the_sampling_period_is_timed() {
    std::thread::spawn(|| {
        let timers: Vec<LoadTimer> = (0..LOAD_SAMPLE_EVERY * 4)
            .map(|_| LoadTimer::start())
            .collect();
        assert_eq!(
            timers
                .iter()
                .filter(|timer| timer.started.is_some())
                .count(),
            4
        );
        let loads = timed_loads();
        let timed = timers
            .into_iter()
            .find(|timer| timer.started.is_some())
            .expect("one is timed");
        std::thread::sleep(Duration::from_millis(1));
        loads.record_load(&4, timed);
        assert!(loads.estimate(&4) > 0);
    })
    .join()
    .expect("sampling thread");
}
