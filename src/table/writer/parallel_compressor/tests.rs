#![expect(clippy::expect_used, reason = "test code")]
use super::*;

/// Deterministic spawner that runs each task synchronously on submit.
/// Exercises the full reorder/box machinery without thread timing.
struct InlineSpawner;
impl CompactionSpawner for InlineSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

fn encode_plain(payload: &[u8]) -> Vec<u8> {
    payload.to_vec()
}

/// Defers tasks and runs them in REVERSE submission order on demand, so the
/// reorder buffer receives out-of-order completions (the inline spawner only
/// ever completes in order, leaving the reordering logic untested).
#[derive(Default)]
struct ReverseSpawner {
    tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
}
impl ReverseSpawner {
    fn run_all_reverse(&self) {
        let mut tasks =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        tasks.reverse();
        for task in tasks {
            task();
        }
    }
}
impl CompactionSpawner for ReverseSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
    }
}

#[test]
fn take_next_returns_blocks_in_submission_order() {
    let mut c = BlockCompressor::new(
        Arc::new(InlineSpawner),
        7,
        CompressionType::None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
    );
    assert_eq!(c.pending(), 0);
    assert!(c.take_next().is_none());

    c.submit(encode_plain(b"alpha"), 0);
    c.submit(encode_plain(b"beta"), 0);
    c.submit(encode_plain(b"gamma"), 0);
    assert_eq!(c.pending(), 3);

    let mut out = Vec::new();
    while c.pending() > 0 {
        let prepared = c
            .take_next()
            .expect("pending > 0 yields a block")
            .expect("plain block prepares without error");
        let mut buf = Vec::new();
        prepared.write_to(&mut buf).expect("write to vec");
        out.push(buf);
    }
    assert_eq!(out.len(), 3);
    assert!(c.take_next().is_none());
}

#[test]
fn take_next_reorders_out_of_order_completions() {
    let spawner = Arc::new(ReverseSpawner::default());
    let mut c = BlockCompressor::new(
        spawner.clone() as Arc<dyn CompactionSpawner>,
        7,
        CompressionType::None,
        None,
        #[cfg(zstd_any)]
        None,
        None,
    );

    // Distinct uncompressed lengths (1, 2, 3) tag each block by submission
    // order; with no compression `uncompressed_length` == input length.
    c.submit(vec![0u8; 1], 0);
    c.submit(vec![0u8; 2], 0);
    c.submit(vec![0u8; 3], 0);
    assert_eq!(c.pending(), 3);

    // Complete the tasks in REVERSE order — the reorder buffer is filled
    // last-seq-first before any drain.
    spawner.run_all_reverse();

    // Despite reverse completion, take_next must yield submission order.
    for expected_len in [1u32, 2, 3] {
        let prepared = c
            .take_next()
            .expect("pending > 0 yields a block")
            .expect("plain block prepares without error");
        let mut buf = Vec::new();
        let header = prepared.write_to(&mut buf).expect("write to vec");
        assert_eq!(
            header.uncompressed_length, expected_len,
            "blocks must drain in submission order regardless of completion order",
        );
    }
    assert!(c.take_next().is_none());
}
