// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::PartitionedFilterWriter;
use crate::{config::BloomConstructionPolicy, table::writer::filter::FilterWriter};

type W = std::io::Cursor<Vec<u8>>;

/// `finish` spills the open partition, which adds a top-level entry under its
/// last key before the top-level index is encoded: the scratch counts that
/// entry, as the output does. The open partition itself does not depend on
/// the key's length, so the scratch grows with it by the entry alone.
#[test]
fn the_scratch_counts_the_top_level_entry_of_the_open_partition() -> crate::Result<()> {
    let scratch = |key_len: usize| -> crate::Result<u64> {
        let mut writer = PartitionedFilterWriter::new(BloomConstructionPolicy::default());
        let key: crate::UserKey = vec![b'k'; key_len].into();
        FilterWriter::<W>::register_key(&mut writer, &key, crate::hash::hash64(&key))?;
        Ok(FilterWriter::<W>::finish_scratch_bytes(&writer))
    };
    let short = scratch(10)?;
    let long = scratch(5_000)?;
    assert!(
        long >= short + 4_990,
        "scratch {long} for a 5 000-byte last key against {short} for a 10-byte one",
    );
    Ok(())
}

#[cfg(feature = "std")]
mod parallel {
    use super::{BloomConstructionPolicy, FilterWriter, PartitionedFilterWriter, W};
    use crate::table::writer::{CompactionSpawner, ParallelCompression};
    use std::io::Write;
    use std::sync::{Arc, Mutex, PoisonError};

    /// Runs each spawned task at once, on the submitting thread: the writer's
    /// thread is the pool's only one.
    struct InlineSpawner;
    impl CompactionSpawner for InlineSpawner {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            task();
        }
    }

    /// Never runs a task: a pool too busy to reach any of them.
    struct SaturatedSpawner;
    impl CompactionSpawner for SaturatedSpawner {
        fn spawn(&self, _task: Box<dyn FnOnce() + Send + 'static>) {}
    }

    /// Holds spawned tasks until released, then starts them newest first,
    /// each on a thread of its own.
    #[derive(Default)]
    struct ReleasedInReverse {
        tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    }
    impl ReleasedInReverse {
        fn release(&self) {
            let tasks =
                std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
            for task in tasks.into_iter().rev() {
                std::thread::spawn(task);
            }
        }
    }
    impl CompactionSpawner for ReleasedInReverse {
        fn spawn(&self, task: Box<dyn FnOnce() + Send + 'static>) {
            self.tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(task);
        }
    }

    fn workers(spawner: Arc<dyn CompactionSpawner>, threads: usize) -> ParallelCompression {
        ParallelCompression {
            spawner,
            threads,
            inline_below: None,
        }
    }

    /// How a table's filter is configured.
    #[derive(Clone)]
    struct Setup {
        policy: BloomConstructionPolicy,
        partition_size: u32,
        ecc: Option<crate::table::block::EccParams>,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        keys: u32,
        /// Bytes the filter is held whole up to, if at all.
        hold: Option<u64>,
    }

    impl Setup {
        fn new(keys: u32) -> Self {
            Self {
                policy: BloomConstructionPolicy::default(),
                partition_size: 256,
                ecc: None,
                encryption: None,
                keys,
                hold: None,
            }
        }

        /// The filter section this setup writes, and its partition count,
        /// with the partitions built on `parallel`'s workers or here.
        /// `before_finish` runs once every key is registered.
        fn write(
            &self,
            parallel: Option<ParallelCompression>,
            before_finish: &dyn Fn(),
        ) -> crate::Result<(Vec<u8>, usize)> {
            let filter = PartitionedFilterWriter::new(self.policy);
            let filter = match self.hold {
                Some(limit) => filter.holding_whole_up_to(limit),
                None => filter,
            };
            let mut writer: Box<dyn FilterWriter<W>> = Box::new(filter)
                .use_partition_size(self.partition_size)
                .use_table_id(7)
                .use_ecc(self.ecc)
                .use_encryption(self.encryption.clone())
                .use_parallel(parallel);
            let mut last_key = None;
            for i in 0..self.keys {
                let key: crate::UserKey = format!("key{i:08}").into_bytes().into();
                writer.register_key(&key, crate::hash::hash64(&key))?;
                last_key = Some(key);
            }
            before_finish();
            let mut file = crate::sfa::Writer::from_writer(
                crate::checksum::ChecksummedWriter::new(std::io::Cursor::new(Vec::new())),
            );
            file.start("data")?;
            file.write_all(&[0; 64])?;
            let output = writer.finish(&mut file, last_key.as_ref())?;
            assert_eq!(output.hashes, u64::from(self.keys), "one hash per key");
            Ok((file.get_mut().get_ref().get_ref().clone(), output.blocks))
        }
    }

    /// The partitions built on workers make the same filter section, byte for
    /// byte, as the ones built on the writer's thread, whatever pool they run
    /// on: several workers, one, the writer's own thread, or none at all.
    #[cfg(feature = "parallel")]
    #[test]
    fn partitions_built_on_workers_write_the_serial_bytes() -> crate::Result<()> {
        let pools: Vec<(&str, ParallelCompression)> = vec![
            (
                "four workers",
                workers(
                    Arc::new(crate::table::writer::RayonSpawner::with_threads(4)?),
                    4,
                ),
            ),
            (
                "one worker",
                workers(
                    Arc::new(crate::table::writer::RayonSpawner::with_threads(1)?),
                    1,
                ),
            ),
            ("the writer's thread", workers(Arc::new(InlineSpawner), 4)),
            ("a saturated pool", workers(Arc::new(SaturatedSpawner), 4)),
        ];
        let setups = vec![
            ("one partition", Setup::new(10)),
            ("many partitions", Setup::new(20_000)),
            (
                "a false-positive rate",
                Setup {
                    policy: BloomConstructionPolicy::FalsePositiveRate(0.001),
                    ..Setup::new(20_000)
                },
            ),
            (
                "large partitions",
                Setup {
                    partition_size: 64 * 1_024,
                    ..Setup::new(20_000)
                },
            ),
        ];
        #[cfg(feature = "page_ecc")]
        let setups = {
            let mut setups = setups;
            setups.push((
                "page ECC",
                Setup {
                    ecc: Some(crate::table::block::EccParams::RS_4_2),
                    ..Setup::new(20_000)
                },
            ));
            setups
        };
        for (setup_name, setup) in &setups {
            let (serial, serial_partitions) = setup.write(None, &|| {})?;
            assert!(serial_partitions >= 1);
            if setup.keys > 1_000 && setup.partition_size < 1_024 {
                assert!(
                    serial_partitions >= 100,
                    "{setup_name}: {serial_partitions} partitions"
                );
            }
            for (pool_name, pool) in &pools {
                let (parallel, parallel_partitions) = setup.write(Some(pool.clone()), &|| {})?;
                assert_eq!(
                    parallel_partitions, serial_partitions,
                    "{setup_name}, {pool_name}"
                );
                assert!(
                    parallel == serial,
                    "{setup_name}, {pool_name}: the filter section differs from the serial one",
                );
            }
        }
        Ok(())
    }

    /// A filter that outgrows its hold writes what a writer that never held
    /// writes, byte for byte, at any limit: on the writer's thread and on
    /// workers, the limit crossed anywhere in a partition or at once.
    #[test]
    fn a_released_hold_writes_what_partitioning_from_the_start_writes() -> crate::Result<()> {
        let setup = Setup::new(5_000);
        let (from_the_start, partitions) = setup.write(None, &|| {})?;
        assert!(partitions > 10, "{partitions} partitions");
        // Past 3 000 keys, from a limit of no block at all.
        let bytes = BloomConstructionPolicy::default().filter_size_bound(3_000) as u64;
        for limit in [0, 1, 256, 257, 1_000, bytes] {
            let held = Setup {
                hold: Some(limit),
                ..setup.clone()
            };
            let (serial, serial_partitions) = held.write(None, &|| {})?;
            assert_eq!(serial_partitions, partitions, "limit {limit}");
            assert!(serial == from_the_start, "limit {limit}: serial");
            let (parallel, _) = held.write(Some(workers(Arc::new(InlineSpawner), 4)), &|| {})?;
            assert!(parallel == from_the_start, "limit {limit}: on workers");
        }
        Ok(())
    }

    /// A partition is cut within the limit that split the filter, however large
    /// the configured partition: past it the cache would refuse the partition.
    #[test]
    fn a_partition_stays_within_the_limit_that_split_the_filter() -> crate::Result<()> {
        let keys = 200_000;
        let setup = Setup {
            partition_size: 64 * 1_024,
            ..Setup::new(keys)
        };
        let (_, configured) = setup.write(None, &|| {})?;
        let limit = 8 * 1_024;
        let filter = BloomConstructionPolicy::default().filter_size_bound(keys as usize) as u64;
        let (_, capped) = Setup {
            hold: Some(limit),
            ..setup
        }
        .write(None, &|| {})?;
        assert!(
            capped as u64 >= filter / limit,
            "{capped} partitions of a {filter} byte filter, {configured} as configured",
        );
        Ok(())
    }

    /// A filter that stays under its limit writes what a full filter writer
    /// writes, byte for byte, and estimates what it estimates.
    #[test]
    fn a_kept_hold_writes_what_a_full_filter_writes() -> crate::Result<()> {
        use crate::table::writer::filter::FullFilterWriter;
        let keys = 5_000_u32;
        let policy = BloomConstructionPolicy::default();
        let held = Setup {
            hold: Some(policy.filter_size_bound(keys as usize) as u64),
            ..Setup::new(keys)
        };
        let (held_bytes, blocks) = held.write(None, &|| {})?;
        assert_eq!(blocks, 1, "one block");

        let mut full: Box<dyn FilterWriter<W>> = Box::new(FullFilterWriter::new(policy));
        full = full.use_table_id(7);
        let mut estimated: Box<dyn FilterWriter<W>> =
            Box::new(PartitionedFilterWriter::new(policy).holding_whole_up_to(u64::MAX));
        let mut last_key = None;
        for i in 0..keys {
            let key: crate::UserKey = format!("key{i:08}").into_bytes().into();
            let hash = crate::hash::hash64(&key);
            full.register_key(&key, hash)?;
            estimated.register_key(&key, hash)?;
            last_key = Some(key);
        }
        assert_eq!(estimated.finish_output_bytes(), full.finish_output_bytes());
        assert_eq!(
            estimated.finish_scratch_bytes(),
            full.finish_scratch_bytes()
        );
        let mut file = crate::sfa::Writer::from_writer(crate::checksum::ChecksummedWriter::new(
            std::io::Cursor::new(Vec::new()),
        ));
        file.start("data")?;
        file.write_all(&[0; 64])?;
        full.finish(&mut file, last_key.as_ref())?;
        assert_eq!(held_bytes, *file.get_mut().get_ref().get_ref());
        Ok(())
    }

    /// Partitions that finish newest first are still published oldest first,
    /// so their bodies, offsets and top-level entries keep the spill order.
    #[test]
    fn partitions_finished_in_reverse_keep_their_order() -> crate::Result<()> {
        let setup = Setup::new(5_000);
        let (serial, _) = setup.write(None, &|| {})?;
        for _ in 0..20 {
            let spawner = Arc::new(ReleasedInReverse::default());
            let release = Arc::clone(&spawner);
            let (parallel, _) = setup.write(Some(workers(spawner, 8)), &|| release.release())?;
            assert!(
                parallel == serial,
                "the filter section differs from the serial one"
            );
        }
        Ok(())
    }

    /// Partitions built on workers under encryption seal each frame with a
    /// fresh nonce, so the bytes differ run to run as they do on the writer's
    /// thread; the partitions keep their number and their framed lengths.
    #[cfg(all(feature = "encryption", feature = "parallel"))]
    #[test]
    fn encrypted_partitions_built_on_workers_keep_their_layout() -> crate::Result<()> {
        let setup = Setup {
            encryption: Some(Arc::new(crate::encryption::Aes256GcmProvider::new(
                &[7; 32],
            ))),
            ..Setup::new(20_000)
        };
        let (serial, serial_partitions) = setup.write(None, &|| {})?;
        let pool = workers(
            Arc::new(crate::table::writer::RayonSpawner::with_threads(4)?),
            4,
        );
        let (parallel, parallel_partitions) = setup.write(Some(pool), &|| {})?;
        assert_eq!(parallel_partitions, serial_partitions);
        assert_eq!(parallel.len(), serial.len());
        Ok(())
    }

    /// A partition that builds to nothing aborts the table with the same error
    /// on workers as on the writer's thread: skipping it would leave its keys
    /// reported absent.
    #[test]
    fn an_empty_partition_aborts_the_table_on_workers_too() {
        let setup = Setup {
            policy: BloomConstructionPolicy::BitsPerKey(0.0),
            ..Setup::new(1_000)
        };
        let serial = setup.write(None, &|| {});
        assert!(
            matches!(serial, Err(crate::Error::Unrecoverable)),
            "{serial:?}"
        );
        let parallel = setup.write(Some(workers(Arc::new(InlineSpawner), 4)), &|| {});
        assert!(
            matches!(parallel, Err(crate::Error::Unrecoverable)),
            "{parallel:?}"
        );
    }

    /// A partition spills at the first hash count whose estimate reaches its
    /// size, the count an estimate per key would have stopped at.
    #[test]
    fn the_spill_threshold_is_the_first_count_the_estimate_reaches() {
        let policy = BloomConstructionPolicy::default();
        for size in [64_u32, 256, 4_096, 64 * 1_024] {
            let mut writer = PartitionedFilterWriter::new(policy);
            writer.partition_size = size;
            let first = (1..1_000_000).find(|&n| policy.estimated_filter_size(n) >= size as usize);
            assert_eq!(
                Some(writer.spill_threshold()),
                first,
                "partition size {size}"
            );
        }
    }

    /// Partitions on workers hold their hashes until they are published: the
    /// held bytes count them, and they are gone once `finish` publishes them.
    #[test]
    fn partitions_on_workers_count_toward_the_held_bytes() -> crate::Result<()> {
        let mut writer = PartitionedFilterWriter::new(BloomConstructionPolicy::default());
        writer.partition_size = 256;
        let mut writer: Box<dyn FilterWriter<W>> =
            Box::new(writer).use_parallel(Some(workers(Arc::new(SaturatedSpawner), 64)));
        let mut serial = PartitionedFilterWriter::new(BloomConstructionPolicy::default());
        serial.partition_size = 256;
        for i in 0..5_000u32 {
            let key: crate::UserKey = format!("key{i:08}").into_bytes().into();
            let hash = crate::hash::hash64(&key);
            writer.register_key(&key, hash)?;
            FilterWriter::<W>::register_key(&mut serial, &key, hash)?;
        }
        let on_workers = writer.held_bytes();
        let built_here = FilterWriter::<W>::held_bytes(&serial);
        assert!(
            on_workers > built_here,
            "{on_workers} held with partitions on workers, {built_here} built here",
        );
        Ok(())
    }
}
