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
        FilterWriter::<W>::register_key(&mut writer, &vec![b'k'; key_len].into())?;
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
    }

    impl Setup {
        fn new(keys: u32) -> Self {
            Self {
                policy: BloomConstructionPolicy::default(),
                partition_size: 256,
                ecc: None,
                encryption: None,
                keys,
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
            let mut writer: Box<dyn FilterWriter<W>> =
                Box::new(PartitionedFilterWriter::new(self.policy))
                    .use_partition_size(self.partition_size)
                    .use_table_id(7)
                    .use_ecc(self.ecc)
                    .use_encryption(self.encryption.clone())
                    .use_parallel(parallel);
            for i in 0..self.keys {
                writer.register_key(&format!("key{i:08}").into_bytes().into())?;
            }
            before_finish();
            let mut file = crate::sfa::Writer::from_writer(
                crate::checksum::ChecksummedWriter::new(std::io::Cursor::new(Vec::new())),
            );
            file.start("data")?;
            file.write_all(&[0; 64])?;
            let output = writer.finish(&mut file)?;
            assert_eq!(output.hashes, u64::from(self.keys), "one hash per key");
            Ok((file.get_mut().inner_mut().get_ref().clone(), output.blocks))
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
            let key = format!("key{i:08}").into_bytes().into();
            writer.register_key(&key)?;
            FilterWriter::<W>::register_key(&mut serial, &key)?;
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
