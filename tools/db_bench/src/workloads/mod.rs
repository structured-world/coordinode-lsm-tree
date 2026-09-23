pub mod fillrandom;
pub mod fillseq;
pub mod mergerandom;
pub mod mixed;
#[cfg(feature = "counters")]
pub mod mixed_layout;
pub mod overwrite;
pub mod prefixscan;
pub mod readrandom;
pub mod readseq;
pub mod readwhilewriting;
pub mod seekrandom;

use crate::config::BenchConfig;
use crate::reporter::Reporter;
use lsm_tree::AnyTree;
use std::sync::Barrier;
use std::sync::atomic::AtomicU64;

/// All benchmark workloads implement this trait.
pub trait Workload {
    /// Run the benchmark, recording latencies into the reporter.
    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()>;
}

/// Split `total` operations across `threads`, capping so no thread gets zero ops.
/// Returns `(actual_threads, base_ops, remainder)`.
///
/// Thread `t` gets `base_ops + if t < remainder { 1 } else { 0 }` ops.
/// Its global starting index is `t * base_ops + min(t, remainder)`.
pub(crate) fn distribute_ops(total: u64, threads: usize) -> (usize, u64, u64) {
    if total == 0 {
        return (1, 0, 0);
    }
    // Benchmark tool targets 64-bit; on 32-bit this caps at usize::MAX threads.
    let threads = std::cmp::min(threads.max(1), usize::try_from(total).unwrap_or(usize::MAX));
    let base = total / threads as u64;
    let rem = total % threads as u64;
    (threads, base, rem)
}

/// Run a multi-threaded benchmark. Each thread calls `thread_fn(thread_index, my_ops, start_op)`
/// and returns a local [`Reporter`]. Results are merged into the caller's `reporter`.
///
/// `start_op` is the global op index for thread `t` — useful for partitioned workloads
/// (e.g. fillseq where thread `t` writes keys `[start_op, start_op + my_ops)`).
/// Random-access workloads may ignore it.
///
/// The caller's reporter is started before threads launch but **not stopped** — the
/// caller must call `reporter.stop()` after any post-thread work (e.g. flush, compaction).
pub(crate) fn run_threaded<F>(
    config: &BenchConfig,
    reporter: &mut Reporter,
    thread_fn: F,
) -> lsm_tree::Result<()>
where
    F: Fn(usize, u64, u64) -> lsm_tree::Result<Reporter> + Sync,
{
    let (threads, base_ops, remainder) = distribute_ops(config.num, config.threads);

    reporter.start();

    // Fast-path: avoid thread::scope + Barrier overhead for the default
    // single-thread case so --threads 1 stays comparable to the prior
    // non-threaded implementation.
    if threads == 1 {
        #[cfg(feature = "flamegraph")]
        let _span = tracing::info_span!("thread", id = 0).entered();

        let local = thread_fn(0, config.num, 0)?;
        reporter.merge(&local);
        return Ok(());
    }

    let barrier = Barrier::new(threads);

    let scope_result: lsm_tree::Result<()> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let my_ops = base_ops + if (t as u64) < remainder { 1 } else { 0 };
                let start = t as u64 * base_ops + std::cmp::min(t as u64, remainder);
                let barrier = &barrier;
                let thread_fn = &thread_fn;
                s.spawn(move || -> lsm_tree::Result<Reporter> {
                    #[cfg(feature = "flamegraph")]
                    let _span = tracing::info_span!("thread", id = t).entered();

                    barrier.wait();
                    thread_fn(t, my_ops, start)
                })
            })
            .collect();

        for handle in handles {
            #[expect(clippy::expect_used, reason = "thread panic is unrecoverable")]
            let local = handle.join().expect("thread panicked")?;
            reporter.merge(&local);
        }

        Ok(())
    });

    scope_result
}

/// Single source of truth for workload name → type mapping. An entry may carry
/// attributes, so a workload compiled in only under a feature is listed only
/// when it exists.
macro_rules! define_workloads {
    ( $( $(#[$attr:meta])* $name:literal => $ty:path ),+ $(,)? ) => {
        /// Create a workload by name.
        pub fn create_workload(name: &str) -> Option<Box<dyn Workload>> {
            match name {
                $( $(#[$attr])* $name => Some(Box::new($ty)), )+
                _ => None,
            }
        }

        /// List all available benchmark names.
        #[expect(
            clippy::vec_init_then_push,
            reason = "each push can carry a cfg, which a vec! element cannot"
        )]
        pub fn available_benchmarks() -> Vec<&'static str> {
            let mut names = Vec::new();
            $( $(#[$attr])* names.push($name); )+
            names
        }
    };
}

// Order is the dashboard's order: the JSON entries are emitted in this
// sequence and github-action-benchmark renders the charts in the order it
// receives them. `mixed` leads because it is the only series that walks a
// whole write / compact / read cycle, so it is the one to read first when
// asking whether a change helped or hurt overall; the single-operation
// workloads below then say where.
define_workloads! {
    "mixed" => mixed::Mixed,
    // Directly after `mixed`, because it answers the other half of the same
    // question: `mixed` says whether the whole cycle got faster, this says
    // what the read path actually moved to get there. Only in a `counters`
    // build, whose instrumented engine the rate workloads must not run on;
    // the dashboard runs it as a second pass, so there its series follow the
    // rate series instead.
    #[cfg(feature = "counters")]
    "mixed-layout" => mixed_layout::MixedLayout,
    "fillseq" => fillseq::FillSeq,
    "fillrandom" => fillrandom::FillRandom,
    "readrandom" => readrandom::ReadRandom,
    "readseq" => readseq::ReadSeq,
    "seekrandom" => seekrandom::SeekRandom,
    "prefixscan" => prefixscan::PrefixScan,
    "overwrite" => overwrite::Overwrite,
    "mergerandom" => mergerandom::MergeRandom,
    "readwhilewriting" => readwhilewriting::ReadWhileWriting,
}
