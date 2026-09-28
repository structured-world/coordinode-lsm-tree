#![expect(clippy::expect_used, reason = "test code")]
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};

/// A job returning its own number, optionally waiting for a signal first.
struct Echo {
    value: u32,
    wait: Option<mpsc::Receiver<()>>,
    /// Signalled once the job has run.
    done: Option<mpsc::Sender<u32>>,
}

impl Echo {
    fn new(value: u32) -> Self {
        Self {
            value,
            wait: None,
            done: None,
        }
    }
}

impl OrderedJob for Echo {
    type Context = ();
    type Output = u32;

    fn run(self, (): &()) -> u32 {
        if let Some(wait) = self.wait {
            wait.recv().expect("released");
        }
        if let Some(done) = self.done {
            done.send(self.value).expect("observed");
        }
        self.value
    }
}

/// Runs each spawned task at once, on the submitting thread.
struct InlineSpawner;
impl CompactionSpawner for InlineSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        task();
    }
}

/// Keeps spawned tasks until asked to run them, counting how many were
/// spawned.
#[derive(Default)]
struct DeferredSpawner {
    tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    spawned: AtomicUsize,
}
impl DeferredSpawner {
    fn run_all(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("lock"));
        for task in tasks {
            task();
        }
    }
    fn spawned(&self) -> usize {
        self.spawned.load(Ordering::SeqCst)
    }
}
impl CompactionSpawner for DeferredSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.spawned.fetch_add(1, Ordering::SeqCst);
        self.tasks.lock().expect("lock").push(task);
    }
}

/// Runs each spawned task on a thread of its own.
struct ThreadSpawner;
impl CompactionSpawner for ThreadSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
}

fn drain(pipeline: &mut OrderedPipeline<Echo>) -> Vec<u32> {
    core::iter::from_fn(|| pipeline.take_next()).collect()
}

#[test]
fn results_come_back_in_submission_order() {
    let mut pipeline = OrderedPipeline::new(Arc::new(InlineSpawner), (), 2, 4);
    assert!(pipeline.take_next().is_none(), "nothing submitted");
    for value in 0..4 {
        pipeline.submit(Echo::new(value));
    }
    assert_eq!(pipeline.pending(), 4);
    assert_eq!(drain(&mut pipeline), [0, 1, 2, 3]);
    assert_eq!(pipeline.pending(), 0);
}

#[test]
fn results_finished_in_reverse_come_back_in_submission_order() {
    // Three workers each hold one job, released last-submitted first, so the
    // ring is filled newest first; the writer still takes them oldest first.
    let mut pipeline = OrderedPipeline::new(Arc::new(ThreadSpawner), (), 3, 3);
    let (done_tx, done_rx) = mpsc::channel();
    let mut release = Vec::new();
    for value in 0..3 {
        let (tx, rx) = mpsc::channel();
        release.push(tx);
        pipeline.submit(Echo {
            value,
            wait: Some(rx),
            done: Some(done_tx.clone()),
        });
    }
    let mut finished = Vec::new();
    for tx in release.iter().rev() {
        tx.send(()).expect("release");
        finished.push(done_rx.recv().expect("finished"));
    }
    assert_eq!(finished, [2, 1, 0], "the jobs finished newest first");
    assert_eq!(drain(&mut pipeline), [0, 1, 2]);
}

#[test]
fn a_stream_of_jobs_spawns_one_token_per_worker() {
    // A token takes jobs until the queue is empty, so ten jobs over two
    // workers cost two spawns, not ten.
    let spawner = Arc::new(DeferredSpawner::default());
    let mut pipeline = OrderedPipeline::new(spawner.clone(), (), 2, 10);
    for value in 0..10 {
        pipeline.submit(Echo::new(value));
    }
    assert_eq!(spawner.spawned(), 2);
    spawner.run_all();
    assert_eq!(drain(&mut pipeline), (0..10).collect::<Vec<_>>());
    assert_eq!(spawner.spawned(), 2);
}

#[test]
fn a_token_that_finds_the_queue_empty_leaves_room_for_the_next() {
    // The writer runs every job itself before the tokens start; each token
    // then finds nothing and exits, so the next job spawns a token again
    // rather than waiting on one that has gone.
    let spawner = Arc::new(DeferredSpawner::default());
    let mut pipeline = OrderedPipeline::new(spawner.clone(), (), 2, 4);
    for value in 0..3 {
        pipeline.submit(Echo::new(value));
    }
    assert_eq!(drain(&mut pipeline), [0, 1, 2], "the writer ran them all");
    spawner.run_all();
    assert_eq!(spawner.spawned(), 2);

    pipeline.submit(Echo::new(3));
    assert_eq!(
        spawner.spawned(),
        3,
        "a job after the tokens left spawns one"
    );
    spawner.run_all();
    assert_eq!(drain(&mut pipeline), [3]);
}

#[test]
fn jobs_run_inline_keep_their_place_among_the_rest() {
    let spawner = Arc::new(DeferredSpawner::default());
    let mut pipeline = OrderedPipeline::new(spawner.clone(), (), 2, 4);
    pipeline.submit(Echo::new(0));
    pipeline.submit_inline(Echo::new(1));
    pipeline.submit(Echo::new(2));
    pipeline.submit_inline(Echo::new(3));
    assert_eq!(spawner.spawned(), 2, "only the queued jobs spawn tokens");
    spawner.run_all();
    assert_eq!(drain(&mut pipeline), [0, 1, 2, 3]);
}

#[test]
fn the_ring_reuses_its_slots_across_many_rounds() {
    // Two slots serve a hundred results when the writer takes one before it
    // submits past two outstanding, as the table writer does.
    let mut pipeline = OrderedPipeline::new(Arc::new(InlineSpawner), (), 1, 2);
    let mut taken = Vec::new();
    for value in 0..100 {
        if pipeline.pending() == 2 {
            taken.extend(pipeline.take_next());
        }
        pipeline.submit(Echo::new(value));
    }
    taken.extend(drain(&mut pipeline));
    assert_eq!(taken, (0..100).collect::<Vec<_>>());
}

#[test]
fn a_writer_with_no_worker_ever_running_does_every_job_itself() {
    // A pool that never runs what it is given (saturated, or the writer's own
    // thread) must not stall the writer: it runs the queued jobs itself.
    let spawner = Arc::new(DeferredSpawner::default());
    let mut pipeline = OrderedPipeline::new(spawner, (), 4, 8);
    for value in 0..8 {
        pipeline.submit(Echo::new(value));
    }
    assert_eq!(drain(&mut pipeline), (0..8).collect::<Vec<_>>());
}
