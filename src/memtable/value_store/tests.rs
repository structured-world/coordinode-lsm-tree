use super::*;

fn val(s: &[u8]) -> UserValue {
    UserValue::from(s)
}

#[test]
fn append_and_get() {
    let store = ValueStore::new();
    let i0 = store.append(val(b"hello"));
    let i1 = store.append(val(b"world"));

    assert_eq!(&*unsafe { store.get(i0) }, b"hello");
    assert_eq!(&*unsafe { store.get(i1) }, b"world");
}

#[test]
fn empty_value() {
    let store = ValueStore::new();
    let i = store.append(val(b""));
    assert!(unsafe { store.get(i) }.is_empty());
}

#[test]
fn crosses_segment_boundary() {
    let store = ValueStore::new();

    // Fill first segment + 1
    for i in 0..=SEGMENT_SIZE {
        store.append(val(format!("v{i}").as_bytes()));
    }

    // Last entry is in segment 1
    let last_idx = u32::try_from(SEGMENT_SIZE).unwrap();
    assert_eq!(
        &*unsafe { store.get(last_idx) },
        format!("v{SEGMENT_SIZE}").as_bytes()
    );
}

/// The reservation counter must fail loudly at the end of the index space
/// instead of wrapping to zero. A wrap would hand out slot 0 a second time,
/// so `ptr::write` would overwrite a live value that existing skiplist nodes
/// still point at: silent wrong-value reads, plus a data race against a
/// concurrent reader of that slot.
///
/// The exhaustion itself is unreachable in production (the arena addresses
/// 2^32 bytes and a node costs at least 28 of them, so it panics roughly 28x
/// earlier), which is exactly why this guard has to hold in release builds
/// too: it is what catches the invariant being broken by a future change.
#[test]
#[should_panic(expected = "index space exhausted")]
fn append_at_the_end_of_the_index_space_panics_instead_of_wrapping_to_slot_zero() {
    let store = ValueStore::new();
    store.set_next_idx_for_test(u32::MAX);
    let _ = store.append(val(b"wraps-onto-slot-zero"));
}

#[test]
fn concurrent_append_and_read() {
    use std::sync::Arc;

    let store = Arc::new(ValueStore::new());
    let n_threads = 8usize;
    let n_per_thread = 1000usize;

    // Concurrent appends.
    let all: Vec<(u32, String)> = (0..n_threads)
        .map(|t| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let mut indices = Vec::with_capacity(n_per_thread);
                for i in 0..n_per_thread {
                    let v = format!("t{t}_v{i}");
                    indices.push((store.append(val(v.as_bytes())), v));
                }
                indices
            })
        })
        .flat_map(|h| h.join().expect("thread ok"))
        .collect();

    // Verify all values are readable and correct.
    for (idx, expected) in &all {
        assert_eq!(&*unsafe { store.get(*idx) }, expected.as_bytes());
    }

    assert_eq!(all.len(), n_threads * n_per_thread);
}
