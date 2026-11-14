use super::*;
use crate::table::writer::CompactionSpawner;
use crate::vlog::blob_file::writer::Writer;
use crate::{CompressionType, ValueType, fs::StdFs};
use test_log::test;

/// Runs every task on a thread of its own.
struct ThreadSpawner;

impl CompactionSpawner for ThreadSpawner {
    fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(task);
    }
}

/// A flush that writes a value only when one is at hand, as the flush does,
/// and hands over values that are each a batch by themselves fills every
/// place the workers have: the batch that finds none brings the oldest back
/// first, so none is handed over past what the workers hold, and every value
/// comes back in the order it went in.
#[test]
fn a_batch_that_finds_every_place_taken_brings_the_oldest_back() -> crate::Result<()> {
    #[cfg(feature = "lz4")]
    let compression = CompressionType::Lz4;
    #[cfg(not(feature = "lz4"))]
    let compression = CompressionType::Zstd(3);
    let folder = tempfile::tempdir()?;
    let writer =
        Writer::new(folder.path().join("blob"), 0, 0, &StdFs)?.use_compression(compression);

    let mut ahead = ValuesAhead::for_flush(
        Some(ParallelCompression {
            spawner: alloc::sync::Arc::new(ThreadSpawner),
            threads: 1,
            inline_below: None,
        }),
        false,
    )
    .expect("a pool to compress on");

    // Small values that share a batch, then values of a batch each, more of
    // them than the one worker holds.
    let lens = [100_usize; 40]
        .into_iter()
        .chain([64 * 1_024; 12])
        .chain([100; 5]);
    let mut taken = Vec::new();
    let take = |ahead: &mut ValuesAhead, taken: &mut Vec<u64>| -> crate::Result<bool> {
        let Some(next) = ahead.take() else {
            return Ok(false);
        };
        let (separated, _) = next?;
        taken.push(separated.key.seqno);
        Ok(true)
    };
    let mut count = 0_u64;
    for len in lens {
        let codec = writer
            .value_codec(64 * 1_024, 0)
            .expect("a value of a frame of its own is compressed ahead");
        if ahead.has_ready() {
            take(&mut ahead, &mut taken)?;
        }
        ahead.submit(
            Separated {
                key: InternalKey::new(b"k".to_vec(), count, ValueType::Value),
                value: UserValue::from(vec![7_u8; len]),
                class: 0,
            },
            codec,
        );
        count += 1;
    }
    while take(&mut ahead, &mut taken)? {}

    assert_eq!(taken, (0..count).collect::<Vec<_>>());
    assert_eq!(ahead.queued_bytes(), 0);
    Ok(())
}
