use super::InsertGate;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// A close waits for an insert already admitted, from any thread's shard.
#[test]
fn close_waits_for_an_admitted_insert() {
    let gate = InsertGate::new();
    let left = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let entered = gate.enter();
        let closer = scope.spawn(|| {
            let _closed = gate.close();
            left.load(Ordering::Acquire)
        });
        std::thread::sleep(Duration::from_millis(50));
        left.store(true, Ordering::Release);
        drop(entered);
        assert!(
            closer.join().expect("closer"),
            "the close returned before the admitted insert left"
        );
    });
}

/// An insert waits while the gate is closed.
#[test]
fn an_insert_waits_while_the_gate_is_closed() {
    let gate = InsertGate::new();
    let reopened = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let closed = gate.close();
        let inserter = scope.spawn(|| {
            let _entered = gate.enter();
            reopened.load(Ordering::Acquire)
        });
        std::thread::sleep(Duration::from_millis(50));
        reopened.store(true, Ordering::Release);
        drop(closed);
        assert!(
            inserter.join().expect("inserter"),
            "the insert got through a closed gate"
        );
    });
}
