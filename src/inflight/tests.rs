use super::{Claim, InflightLoads};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

type Loads = InflightLoads<u64, Arc<Vec<u8>>, u32, rustc_hash::FxBuildHasher>;

fn loads(per_shard_limit: usize) -> Arc<Loads> {
    Arc::new(InflightLoads::new(
        4,
        per_shard_limit,
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
    let loads: Arc<Loads> = Arc::new(InflightLoads::new(1, 1, rustc_hash::FxBuildHasher));
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
