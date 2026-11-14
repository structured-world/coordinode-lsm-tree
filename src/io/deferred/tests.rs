use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Runs each task on a thread of its own.
struct Threads;

impl CompactionSpawner for Threads {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
}

/// Runs each task on a thread of its own and returns once the task ran: a
/// worker that is always there first.
struct Prompt;

impl CompactionSpawner for Prompt {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        let worker = std::thread::spawn(task);
        assert!(worker.join().is_ok(), "the lane's task panicked");
    }
}

/// Takes tasks and never runs them: a pool that cannot reach the lane.
#[derive(Default)]
struct Never(Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>);

impl CompactionSpawner for Never {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
    }
}

/// A sink that records its bytes, the writes it took and the thread of each.
#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    writes: Vec<usize>,
    threads: Vec<std::thread::ThreadId>,
    /// Fails the write that would take the sink past this many bytes.
    fail_past: Option<usize>,
    /// Sleeps this long in every write.
    slow: Option<Duration>,
}

impl Write for Sink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if let Some(slow) = self.slow {
            std::thread::sleep(slow);
        }
        if self
            .fail_past
            .is_some_and(|limit| self.bytes.len() + data.len() > limit)
        {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "the sink is full",
            ));
        }
        self.bytes.extend_from_slice(data);
        self.writes.push(data.len());
        self.threads.push(std::thread::current().id());
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// `len` bytes that differ along their length.
fn pattern(len: usize) -> Vec<u8> {
    (0..=250_u8).cycle().take(len).collect()
}

/// Writes `data` in pieces of `piece` bytes.
fn write_in<W: Write>(out: &mut W, data: &[u8], piece: usize) -> io::Result<()> {
    for chunk in data.chunks(piece) {
        out.write_all(chunk)?;
    }
    Ok(())
}

/// Without workers the writer is a buffering writer: whole buffers, on the
/// caller's thread, in order.
#[test]
fn without_workers_buffers_go_down_on_the_callers_thread() -> io::Result<()> {
    let data = pattern(10_000);
    let mut out = DeferredWriter::with_capacity(1_024, Sink::default(), true);
    write_in(&mut out, &data, 100)?;
    let sink = out.finish()?;
    assert_eq!(sink.bytes, data);
    assert!(
        sink.writes.iter().rev().skip(1).all(|&len| len == 1_024),
        "{:?}",
        sink.writes
    );
    let here = std::thread::current().id();
    assert!(sink.threads.iter().all(|&thread| thread == here));
    Ok(())
}

/// A writer that does not fill its buffer puts a write that does not fit
/// into the next one whole, as std's buffering writer does.
#[test]
fn a_writer_that_does_not_fill_keeps_a_write_whole() -> io::Result<()> {
    let mut out = DeferredWriter::with_capacity(1_000, Sink::default(), false);
    out.write_all(&[1; 600])?;
    out.write_all(&[2; 600])?;
    let sink = out.finish()?;
    assert_eq!(sink.writes, [600, 600]);
    Ok(())
}

/// A buffer the last write left exactly full takes the next write: it goes
/// down first, where a writer that took nothing of the write would have it
/// fail as a write of zero bytes.
#[test]
fn a_write_after_an_exactly_full_buffer_is_taken() -> io::Result<()> {
    for (fill, workers) in [(true, false), (true, true), (false, false), (false, true)] {
        let mut out = DeferredWriter::with_capacity(1_000, Sink::default(), fill);
        if workers {
            out.use_spawner(Arc::new(Threads));
        }
        out.write_all(&[1; 400])?;
        out.write_all(&[2; 600])?;
        out.write_all(&[3; 10])?;
        let sink = out.finish()?;
        let mut expected = vec![1; 400];
        expected.extend_from_slice(&[2; 600]);
        expected.extend_from_slice(&[3; 10]);
        assert_eq!(sink.bytes, expected, "fill: {fill}, workers: {workers}");
    }
    Ok(())
}

/// Writes of every size around the buffer's, in any sequence, reach the file
/// whole and in order, whichever way the writer fills its buffer and whoever
/// writes it.
#[test]
fn writes_of_any_size_reach_the_file_in_order() -> io::Result<()> {
    const CAPACITY: usize = 1_000;
    for (fill, workers) in [(true, false), (true, true), (false, false), (false, true)] {
        let mut out = DeferredWriter::with_capacity(CAPACITY, Sink::default(), fill);
        if workers {
            out.use_spawner(Arc::new(Threads));
        }
        let mut expected = Vec::new();
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        for round in 0..3_000_u32 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Mostly small, with sizes at and around the buffer's and a few
            // past it.
            let len = match state % 10 {
                0 => CAPACITY,
                1 => CAPACITY - 1,
                2 => CAPACITY + 1,
                3 => 2 * CAPACITY + 7,
                4 => 0,
                _ => usize::try_from(state >> 32).unwrap_or(0) % CAPACITY,
            };
            let piece = vec![round.to_le_bytes()[0]; len];
            out.write_all(&piece)?;
            expected.extend_from_slice(&piece);
        }
        let sink = out.finish()?;
        assert!(
            sink.bytes == expected,
            "fill: {fill}, workers: {workers}: {} bytes against {}",
            sink.bytes.len(),
            expected.len()
        );
    }
    Ok(())
}

/// With workers the buffers are written off the caller's thread, and the
/// file takes its bytes in the order they were written.
#[test]
fn workers_write_the_buffers_in_order() -> io::Result<()> {
    let data = pattern(600_000);
    for prompt in [false, true] {
        let mut out = DeferredWriter::with_capacity(4_096, Sink::default(), true);
        if prompt {
            out.use_spawner(Arc::new(Prompt));
        } else {
            out.use_spawner(Arc::new(Threads));
        }
        write_in(&mut out, &data, 333)?;
        let sink = out.finish()?;
        assert_eq!(sink.bytes, data, "prompt: {prompt}");
        // A worker that is there first writes every buffer handed over.
        if prompt {
            let here = std::thread::current().id();
            let by_workers = sink.threads.iter().filter(|&&t| t != here).count();
            assert!(
                by_workers + 1 >= sink.threads.len(),
                "{by_workers} of {} writes were a worker's",
                sink.threads.len()
            );
        }
    }
    Ok(())
}

/// A write at least as large as the buffer goes down in order with what was
/// buffered before it.
#[test]
fn a_write_larger_than_the_buffer_keeps_its_place() -> io::Result<()> {
    for workers in [false, true] {
        let mut out = DeferredWriter::with_capacity(1_024, Sink::default(), true);
        if workers {
            out.use_spawner(Arc::new(Threads));
        }
        out.write_all(&[1; 10])?;
        out.write_all(&[2; 5_000])?;
        out.write_all(&[3; 10])?;
        let sink = out.finish()?;
        let mut expected = vec![1; 10];
        expected.extend_from_slice(&[2; 5_000]);
        expected.extend_from_slice(&[3; 10]);
        assert_eq!(sink.bytes, expected, "workers: {workers}");
    }
    Ok(())
}

/// A pool that never runs the lane's task does not stop the writer: past the
/// lane's bound the writer drains it itself, and its end waits on no worker.
#[test]
fn a_pool_that_never_comes_does_not_stop_the_writer() -> io::Result<()> {
    let data = pattern(4 * LANE_BYTES_MAX);
    let mut out = DeferredWriter::with_capacity(64 * 1_024, Sink::default(), true);
    out.use_spawner(Arc::new(Never::default()));
    write_in(&mut out, &data, 5_000)?;
    let sink = out.finish()?;
    assert_eq!(sink.bytes, data);
    Ok(())
}

/// A lane a slow worker drains holds no more than its bound: the writer
/// waits for the worker.
#[test]
fn a_full_lane_holds_the_writer_back() -> io::Result<()> {
    let data = pattern(3 * LANE_BYTES_MAX);
    let sink = Sink {
        slow: Some(Duration::from_millis(1)),
        ..Sink::default()
    };
    let mut out = DeferredWriter::with_capacity(64 * 1_024, sink, true);
    out.use_spawner(Arc::new(Threads));
    let mut most = 0;
    for chunk in data.chunks(64 * 1_024) {
        out.write_all(chunk)?;
        most = most.max(out.queued_bytes());
    }
    let sink = out.finish()?;
    assert_eq!(sink.bytes, data);
    assert!(most <= LANE_BYTES_MAX, "the lane held {most} bytes");
    Ok(())
}

/// A job queued behind writes runs once they are written, on the lane.
#[test]
fn a_job_behind_writes_finds_them_written() -> io::Result<()> {
    for workers in [false, true] {
        let seen = Arc::new(AtomicUsize::new(usize::MAX));
        let mut out = DeferredWriter::with_capacity(1_024, Sink::default(), true);
        if workers {
            out.use_spawner(Arc::new(Threads));
        }
        out.write_all(&pattern(5_000))?;
        let at = Arc::clone(&seen);
        out.after_writes(move |sink: &mut Sink| at.store(sink.bytes.len(), Ordering::SeqCst))?;
        out.write_all(&pattern(5_000))?;
        let sink = out.finish()?;
        assert_eq!(seen.load(Ordering::SeqCst), 5_000, "workers: {workers}");
        assert_eq!(sink.bytes.len(), 10_000);
    }
    Ok(())
}

/// The first failed write is returned by the calls after it and by the
/// writer's end, and nothing queued behind it is written.
#[test]
fn a_failed_write_is_returned_and_nothing_follows_it() {
    for workers in [false, true] {
        let sink = Sink {
            fail_past: Some(3_000),
            ..Sink::default()
        };
        let mut out = DeferredWriter::with_capacity(1_024, sink, true);
        if workers {
            out.use_spawner(Arc::new(Threads));
        }
        // Enough writes for the failure to come back through one of them.
        let mut failed = None;
        for _ in 0..64 {
            if let Err(e) = out.write_all(&[7; 1_024]) {
                failed = Some(e);
                break;
            }
            if workers {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        let Some(error) = failed else {
            panic!("a later write returns the failure");
        };
        assert_eq!(
            error.kind(),
            io::ErrorKind::StorageFull,
            "workers: {workers}"
        );
        let seen = Arc::new(AtomicBool::new(false));
        let ran = Arc::clone(&seen);
        assert!(
            out.after_writes(move |_| ran.store(true, Ordering::SeqCst))
                .is_err()
        );
        let Err(end) = out.finish() else {
            panic!("the writer's end returns the failure");
        };
        assert_eq!(end.kind(), io::ErrorKind::StorageFull);
        assert!(!seen.load(Ordering::SeqCst), "a job ran behind the failure");
    }
}

/// A sink that panics in its second write.
#[derive(Default)]
struct PanicsOnSecond {
    writes: usize,
    dropped: Arc<AtomicBool>,
}

impl Write for PanicsOnSecond {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.writes += 1;
        assert!(self.writes != 2, "the sink's second write");
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for PanicsOnSecond {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// A panic of the inner writer on a worker is a failure of the file, not of
/// the worker's thread: the writer returns it and calls the inner writer no
/// more.
#[test]
fn a_panic_on_a_worker_is_a_failure_of_the_file() {
    let dropped = Arc::new(AtomicBool::new(false));
    let sink = PanicsOnSecond {
        writes: 0,
        dropped: Arc::clone(&dropped),
    };
    let mut out = DeferredWriter::with_capacity(1_024, sink, true);
    out.use_spawner(Arc::new(Threads));
    let mut failed = false;
    for _ in 0..64 {
        if out.write_all(&[7; 1_024]).is_err() {
            failed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(failed, "a later write returns the panic as a failure");
    assert!(out.finish().is_err());
    assert!(
        dropped.load(Ordering::SeqCst),
        "the inner writer is dropped"
    );
}

/// A sink that notes when it is dropped.
struct Noted(Arc<AtomicBool>, Vec<u8>);

impl Write for Noted {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        std::thread::sleep(Duration::from_millis(1));
        self.1.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for Noted {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A writer dropped with buffers on its lane leaves it idle: the inner
/// writer is dropped by then, so the caller may remove the file.
#[test]
fn a_dropped_writer_leaves_no_work_behind() -> io::Result<()> {
    let dropped = Arc::new(AtomicBool::new(false));
    let mut out =
        DeferredWriter::with_capacity(1_024, Noted(Arc::clone(&dropped), Vec::new()), true);
    out.use_spawner(Arc::new(Threads));
    out.write_all(&pattern(64 * 1_024))?;
    drop(out);
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}

/// A sink that counts its writes and panics in the second.
struct CountsThenPanics(Arc<AtomicUsize>);

impl Write for CountsThenPanics {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let writes = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(writes != 2, "the sink's second write");
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A writer on the caller's thread whose inner writer panicked does not call
/// it again when dropped: the buffer the panic interrupted is not written a
/// second time.
#[test]
fn a_writer_that_panicked_on_the_callers_thread_is_not_called_again() {
    let writes = Arc::new(AtomicUsize::new(0));
    let mut out = DeferredWriter::with_capacity(8, CountsThenPanics(Arc::clone(&writes)), true);
    let panicked = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| {
        // Two full buffers go down; the second write panics, with bytes
        // buffered behind it.
        for _ in 0..3 {
            out.write_all(&[7; 6])?;
        }
        io::Result::Ok(())
    }));
    assert!(panicked.is_err());
    drop(out);
    assert_eq!(writes.load(Ordering::SeqCst), 2);
}

/// Workers taken on after writes on the caller's thread keep the file's
/// order: what was buffered goes to the lane behind what was written.
#[test]
fn workers_taken_on_midway_keep_the_order() -> io::Result<()> {
    let data = pattern(40_000);
    let mut out = DeferredWriter::with_capacity(1_024, Sink::default(), true);
    let (before, after) = data.split_at(10_500);
    out.write_all(before)?;
    out.use_spawner(Arc::new(Threads));
    out.write_all(after)?;
    assert_eq!(out.finish()?.bytes, data);
    Ok(())
}

/// Reading the inner writer waits for what was written to reach it.
#[test]
fn the_inner_writer_is_read_with_everything_written() -> io::Result<()> {
    let mut out = DeferredWriter::with_capacity(1_024, Sink::default(), true);
    out.use_spawner(Arc::new(Threads));
    out.write_all(&pattern(50_000))?;
    assert_eq!(out.with_inner(|sink| sink.bytes.len())?, 50_000);
    out.write_all(&pattern(7))?;
    assert_eq!(out.finish()?.bytes.len(), 50_007);
    Ok(())
}
