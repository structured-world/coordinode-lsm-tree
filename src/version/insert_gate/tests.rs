use super::InsertGate;
use crate::comparator::default_comparator;
use crate::memtable::Memtable;
use alloc::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn memtable(id: u64) -> Arc<Memtable> {
    Arc::new(Memtable::new(id, default_comparator()))
}

/// A close waits for an insert already admitted, from any thread's shard.
#[test]
fn close_waits_for_an_admitted_insert() {
    let gate = InsertGate::new(memtable(0));
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
    let gate = InsertGate::new(memtable(0));
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

/// The memtable a closed gate is given is the one every insert admitted after
/// it lands in, and an insert admitted before keeps the memtable it was
/// handed until it leaves. Guards against an insert writing into a memtable a
/// version change has already replaced.
#[test]
fn an_insert_lands_in_the_memtable_the_gate_hands_out() {
    let (first, second) = (memtable(1), memtable(2));
    let gate = InsertGate::new(Arc::clone(&first));
    assert_eq!(gate.enter().memtable().id, 1);
    std::thread::scope(|scope| {
        let entered = gate.enter();
        let changer = scope.spawn(|| {
            let closed = gate.close();
            closed.set_active(Arc::clone(&second));
        });
        std::thread::sleep(Duration::from_millis(50));
        // The change waits for this insert, which still has the first one.
        assert_eq!(entered.memtable().id, 1);
        drop(entered);
        changer.join().expect("changer");
    });
    assert_eq!(gate.enter().memtable().id, 2);
}
