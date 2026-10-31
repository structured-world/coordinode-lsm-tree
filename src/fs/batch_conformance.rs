// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! What every backend that reads by descriptor owes its batched reads and its
//! read queue, as tests: [`batched_read_conformance`] generates them for a
//! backend, given an expression yielding `Some(backend)`, or `None` to skip
//! where the kernel cannot run it.

/// Generates the batched-read conformance tests in a module of their own.
/// `$make` is evaluated once per test (twice where a test wraps the backend),
/// inside that module, so it names the caller's items through `super::`.
macro_rules! batched_read_conformance {
    ($make:expr) => {
        mod batched_read_conformance {
            #![expect(
                clippy::indexing_slicing,
                reason = "test assertions over fixtures of known sizes"
            )]

            use std::io::Write as _;
            use std::sync::Arc;
            use $crate::fs::{BlockBuf, BlockRead, Fs, FsFile, FsOpenOptions, QueuedRead, StdFs};
            use $crate::path::Path;

            /// Two files of `len` bytes each, byte `i` of file `f` being
            /// `(i + f) % 251`.
            fn two_files(
                fs: &dyn Fs,
                dir: &Path,
                len: usize,
            ) -> $crate::io::Result<Vec<Box<dyn FsFile>>> {
                let opts = FsOpenOptions::new().write(true).create(true).read(true);
                let mut files = Vec::new();
                for f in 0..2usize {
                    let mut file = fs.open(&dir.join(format!("f{f}.bin")), &opts)?;
                    let bytes: Vec<u8> = (0..len)
                        .map(|i| u8::try_from((i + f) % 251).expect("below 251"))
                        .collect();
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    files.push(file);
                }
                Ok(files)
            }

            /// The bytes a [`two_files`] file `f` holds at `offset`, `len` of them.
            fn expected_bytes(f: usize, offset: usize, len: usize) -> Vec<u8> {
                (offset..offset + len)
                    .map(|i| u8::try_from((i + f) % 251).expect("below 251"))
                    .collect()
            }

            /// Wraps a file handle, passing its descriptor through so the
            /// backend still reads it by descriptor, and counts the serial
            /// `read_at` calls made against it. A count above zero says the
            /// request took the serial path.
            struct CountingFile {
                inner: Box<dyn FsFile>,
                read_at_calls: core::sync::atomic::AtomicUsize,
            }

            impl CountingFile {
                fn new(inner: Box<dyn FsFile>) -> Self {
                    Self {
                        inner,
                        read_at_calls: core::sync::atomic::AtomicUsize::new(0),
                    }
                }

                fn read_at_calls(&self) -> usize {
                    self.read_at_calls
                        .load(core::sync::atomic::Ordering::Relaxed)
                }
            }

            impl std::io::Read for CountingFile {
                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                    self.inner.read(buf)
                }
            }

            impl std::io::Write for CountingFile {
                fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                    self.inner.write(buf)
                }

                fn flush(&mut self) -> std::io::Result<()> {
                    self.inner.flush()
                }
            }

            impl std::io::Seek for CountingFile {
                fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
                    self.inner.seek(pos)
                }
            }

            impl FsFile for CountingFile {
                fn sync_all(&self) -> $crate::io::Result<()> {
                    self.inner.sync_all()
                }

                fn sync_data(&self) -> $crate::io::Result<()> {
                    self.inner.sync_data()
                }

                fn metadata(&self) -> $crate::io::Result<$crate::fs::FsMetadata> {
                    self.inner.metadata()
                }

                fn set_len(&self, size: u64) -> $crate::io::Result<()> {
                    self.inner.set_len(size)
                }

                fn read_at(&self, buf: &mut [u8], offset: u64) -> $crate::io::Result<usize> {
                    self.read_at_calls
                        .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                    self.inner.read_at(buf, offset)
                }

                fn backing_fd(&self) -> Option<i32> {
                    self.inner.backing_fd()
                }

                fn lock_exclusive(&self) -> $crate::io::Result<()> {
                    self.inner.lock_exclusive()
                }
            }

            #[test_log::test]
            fn read_many_fills_every_region() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let path = dir.path().join("read_many.bin");
                let opts = FsOpenOptions::new().write(true).create(true).read(true);
                let mut file = fs.open(&path, &opts)?;
                let data: Vec<u8> = (0..=255u8).collect();
                file.write_all(&data)?;
                file.sync_all()?;

                // Disjoint regions in non-file order plus an EMPTY region must
                // each be filled by the one batched call.
                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 8];
                let mut empty: [u8; 0] = [];
                let mut b2 = [0u8; 1];
                let mut regions: Vec<(u64, &mut [u8])> = vec![
                    (10, &mut b0[..]),
                    (200, &mut b1[..]),
                    (50, &mut empty[..]),
                    (0, &mut b2[..]),
                ];
                file.read_many(&mut regions)?;
                drop(regions);

                assert_eq!(&b0[..], &data[10..14]);
                assert_eq!(&b1[..], &data[200..208]);
                assert_eq!(b2[0], data[0]);
                Ok(())
            }

            #[test_log::test]
            fn read_many_short_read_at_eof_errors() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let path = dir.path().join("short_many.bin");
                let opts = FsOpenOptions::new().write(true).create(true).read(true);
                let mut file = fs.open(&path, &opts)?;
                file.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
                file.sync_all()?;

                // The first region runs past EOF (a fixed-size short read is
                // UnexpectedEof, not EOF); the second is in range and must still
                // be read to completion before the call returns.
                let mut short = [0u8; 32];
                let mut ok = [0xAAu8; 4];
                let mut regions: Vec<(u64, &mut [u8])> =
                    vec![(0, &mut short[..]), (4, &mut ok[..])];
                match file.read_many(&mut regions) {
                    Ok(()) => panic!("read past EOF must fail, not report a short read as success"),
                    Err(err) => assert_eq!(err.kind(), $crate::io::ErrorKind::UnexpectedEof),
                }
                assert_eq!(ok, [5, 6, 7, 8]);
                Ok(())
            }

            /// A backend reads into memory nobody wrote: one read and a batch
            /// bring back the file's bytes, an empty region included.
            #[test_log::test]
            fn reads_into_uninitialized_buffers_fill_them() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files = two_files(&fs, dir.path(), 4096)?;
                let file = files[0].as_ref();

                let one = $crate::file::read_exact(file, 16, 64)?;
                assert_eq!(&one[..], &expected_bytes(0, 16, 64)[..]);

                let many = $crate::file::read_exact_many(file, &[(8, 32), (0, 0), (1024, 16)])?;
                assert_eq!(&many[0][..], &expected_bytes(0, 8, 32)[..]);
                assert!(many[1].is_empty());
                assert_eq!(&many[2][..], &expected_bytes(0, 1024, 16)[..]);
                Ok(())
            }

            /// A batch into memory nobody wrote refuses a region past the end
            /// of the file instead of handing it back.
            #[test_log::test]
            fn reads_into_uninitialized_buffers_refuse_a_short_region() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files = two_files(&fs, dir.path(), 64)?;
                let Err(error) =
                    $crate::file::read_exact_many(files[0].as_ref(), &[(0, 32), (32, 64)])
                else {
                    panic!("a read past the end must fail");
                };
                assert_eq!(error.kind(), $crate::io::ErrorKind::UnexpectedEof);
                Ok(())
            }

            #[test_log::test]
            fn read_blocks_batched_across_files() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut f0 = fs.open(&dir.path().join("a.bin"), &opts)?;
                f0.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                f0.sync_all()?;
                let mut f1 = fs.open(&dir.path().join("b.bin"), &opts)?;
                let rev: Vec<u8> = (0..=255u8).rev().collect();
                f1.write_all(&rev)?;
                f1.sync_all()?;

                assert!(f0.backing_fd().is_some(), "the file exposes its descriptor");
                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: f0.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: f1.as_ref(),
                            offset: 20,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)?;
                }
                assert_eq!(b0, [10, 11, 12, 13]);
                assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
                Ok(())
            }

            /// A request whose destination arrives partly filled resumes at
            /// `offset + filled`: the read's pointer and length describe the
            /// unfilled suffix, so reading from the original offset would
            /// duplicate the block's first bytes into it.
            #[test_log::test]
            fn read_blocks_batched_partly_filled_request_resumes() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);
                let mut file = fs.open(&dir.path().join("resume.bin"), &opts)?;
                file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                file.sync_all()?;

                // Block = bytes 10..18; the first 3 are already in the
                // destination, so the read owes bytes 13..18 into the suffix.
                let mut buf = [0u8; 8];
                {
                    let mut dst = BlockBuf::new(&mut buf);
                    dst.append(&[10, 11, 12]);
                    let mut reqs = vec![BlockRead {
                        file: file.as_ref(),
                        offset: 10,
                        buf: dst,
                    }];
                    fs.read_blocks_batched(&mut reqs)?;
                    assert!(reqs[0].buf.is_full(), "the request is completed, not short");
                }
                assert_eq!(buf, [10, 11, 12, 13, 14, 15, 16, 17]);
                Ok(())
            }

            #[test_log::test]
            fn read_blocks_batched_short_read_at_eof_errors() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);
                let mut file = fs.open(&dir.path().join("short_block.bin"), &opts)?;
                file.write_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])?;
                file.sync_all()?;

                // The first block runs past EOF; the second is in range and is
                // read to completion before the call returns.
                let mut short = [0u8; 64];
                let mut ok = [0xAAu8; 4];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut short),
                        },
                        BlockRead {
                            file: file.as_ref(),
                            offset: 4,
                            buf: BlockBuf::new(&mut ok),
                        },
                    ];
                    match fs.read_blocks_batched(&mut reqs) {
                        Ok(()) => {
                            panic!("read past EOF must fail, not report a short read as success")
                        }
                        Err(err) => {
                            assert_eq!(err.kind(), $crate::io::ErrorKind::UnexpectedEof);
                        }
                    }
                }
                assert_eq!(ok, [5, 6, 7, 8]);
                Ok(())
            }

            /// A handle with no descriptor is read serially, and a block it
            /// cuts short is `UnexpectedEof`, not a silent partial fill.
            #[test_log::test]
            fn read_blocks_batched_serial_short_read_errors() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(1..=64u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&[0u8; 10])?;
                std_file.sync_all()?;

                let mut b0 = [0xAAu8; 8];
                let mut b1 = [0u8; 64];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    match fs.read_blocks_batched(&mut reqs) {
                        Ok(()) => panic!("a short serial read must fail"),
                        Err(err) => {
                            assert_eq!(err.kind(), $crate::io::ErrorKind::UnexpectedEof);
                        }
                    }
                }
                assert_eq!(b0, [1, 2, 3, 4, 5, 6, 7, 8]);
                Ok(())
            }

            #[test_log::test]
            fn read_blocks_batched_serves_a_mixed_batch_without_degrading_the_descriptor_group()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;
                // Wrapped so the test sees whether this request was read by
                // descriptor or through the serial `read_at` path.
                let counted = CountingFile::new(ring_file);

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                let rev: Vec<u8> = (0..=255u8).rev().collect();
                std_file.write_all(&rev)?;
                std_file.sync_all()?;
                assert_eq!(std_file.backing_fd(), None);

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: &counted,
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 20,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)?;
                }
                assert_eq!(b0, [10, 11, 12, 13]);
                assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
                assert_eq!(
                    counted.read_at_calls(),
                    0,
                    "the descriptor-bearing request was read by descriptor; one handle \
                     without a descriptor must not drag the rest onto the serial path",
                );
                Ok(())
            }

            /// Every destination holds the bytes ITS `(file, offset)` names,
            /// whichever group served it.
            #[test_log::test]
            fn read_blocks_batched_keeps_each_destination_with_its_own_request()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                let rev: Vec<u8> = (0..=255u8).rev().collect();
                std_file.write_all(&rev)?;
                std_file.sync_all()?;

                // Interleaved, so a split that preserved only group-internal
                // order would still be caught.
                let mut bufs = [[0u8; 2]; 6];
                let offsets: [u8; 6] = [0, 10, 20, 30, 40, 50];
                {
                    let mut reqs: Vec<BlockRead<'_>> = Vec::new();
                    for (i, (buf, offset)) in bufs.iter_mut().zip(offsets).enumerate() {
                        reqs.push(BlockRead {
                            file: if i % 2 == 0 {
                                ring_file.as_ref()
                            } else {
                                std_file.as_ref()
                            },
                            offset: u64::from(offset),
                            buf: BlockBuf::new(&mut buf[..]),
                        });
                    }
                    fs.read_blocks_batched(&mut reqs)?;
                }
                for (i, (buf, offset)) in bufs.iter().zip(offsets).enumerate() {
                    let at = usize::from(offset);
                    let expected: [u8; 2] = if i % 2 == 0 {
                        [offset, offset + 1]
                    } else {
                        [rev[at], rev[at + 1]]
                    };
                    assert_eq!(*buf, expected, "request {i} was served the wrong bytes");
                }
                Ok(())
            }

            /// A batch where nothing has a descriptor still reads correctly,
            /// every request on the serial path.
            #[test_log::test]
            fn read_blocks_batched_with_no_descriptor_reads_serially() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 100,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)?;
                }
                assert_eq!(b0, [0, 1, 2, 3]);
                assert_eq!(b1, [100, 101, 102, 103]);
                Ok(())
            }

            /// A short read in the serial half of a mixed batch is still
            /// `UnexpectedEof`, and the descriptor half is still filled.
            #[test_log::test]
            fn read_blocks_batched_reports_a_short_serial_read_in_a_mixed_batch()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&[1, 2, 3, 4])?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 8];
                let err = {
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("a short read on a fixed-size block is an error")
                };
                assert_eq!(err.kind(), $crate::io::ErrorKind::UnexpectedEof, "{err}");
                assert_eq!(b0, [10, 11, 12, 13], "the descriptor group was served");
                Ok(())
            }

            /// A short read in the descriptor half of a mixed batch fails the
            /// call.
            #[test_log::test]
            fn read_blocks_batched_reports_a_short_descriptor_read_in_a_mixed_batch()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&[1, 2, 3, 4])?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 8];
                let mut b1 = [0u8; 4];
                let err = {
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("a short read on a fixed-size block is an error")
                };
                assert_eq!(err.kind(), $crate::io::ErrorKind::UnexpectedEof, "{err}");
                Ok(())
            }

            /// A destination that arrives partly filled is read from
            /// `offset + filled` on both sides of the split.
            #[test_log::test]
            fn read_blocks_batched_resumes_partly_filled_destinations_in_a_mixed_batch()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                let rev: Vec<u8> = (0..=255u8).rev().collect();
                std_file.write_all(&rev)?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                {
                    let mut buf0 = BlockBuf::new(&mut b0);
                    let mut buf1 = BlockBuf::new(&mut b1);
                    assert_eq!(buf0.append(&[0xAA, 0xBB]), 2);
                    assert_eq!(buf1.append(&[0xCC, 0xDD]), 2);
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: buf0,
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 20,
                            buf: buf1,
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)?;
                    assert!(reqs.iter().all(|r| r.buf.is_full()));
                }
                assert_eq!(b0, [0xAA, 0xBB, 12, 13]);
                assert_eq!(b1, [0xCC, 0xDD, rev[22], rev[23]]);
                Ok(())
            }

            /// An empty request is a no-op wherever it lands in a split batch.
            #[test_log::test]
            fn read_blocks_batched_tolerates_an_empty_request_in_a_mixed_batch()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                let rev: Vec<u8> = (0..=255u8).rev().collect();
                std_file.write_all(&rev)?;
                std_file.sync_all()?;

                let mut empty_ring: [u8; 0] = [];
                let mut empty_std: [u8; 0] = [];
                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut empty_ring),
                        },
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut empty_std),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 20,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)?;
                    assert!(
                        reqs.iter().all(|r| r.buf.is_full()),
                        "an empty destination is trivially full, and the rest were filled",
                    );
                }
                assert_eq!(b0, [10, 11, 12, 13]);
                assert_eq!(b1, [rev[20], rev[21], rev[22], rev[23]]);
                Ok(())
            }

            /// When both halves of a split batch fail, the error returned is
            /// the one whose request came first in the caller's order; the
            /// serial request is first here.
            #[test_log::test]
            fn read_blocks_batched_reports_the_earliest_failure_when_both_halves_fail()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&[1, 2, 3, 4])?;
                ring_file.sync_all()?;
                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&[1, 2, 3, 4])?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 8];
                let mut b1 = [0u8; 8];
                let err = {
                    let mut serial_first = BlockBuf::new(&mut b0);
                    // An overflowing resume offset, `InvalidInput`, distinct
                    // from the descriptor half's `UnexpectedEof`.
                    assert_eq!(serial_first.append(&[0xAA, 0xBB]), 2);
                    let mut reqs = vec![
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: u64::MAX - 1,
                            buf: serial_first,
                        },
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("both halves fail, so the call fails")
                };
                assert_eq!(
                    err.kind(),
                    $crate::io::ErrorKind::InvalidInput,
                    "the request at index 0 is the serial one, so its error wins: {err}",
                );
                Ok(())
            }

            /// The mirror of the case above: when the descriptor half holds
            /// the earlier request, its error is the one reported.
            #[test_log::test]
            fn read_blocks_batched_prefers_the_descriptor_failure_when_it_came_first()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&[1, 2, 3, 4])?;
                ring_file.sync_all()?;
                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&[1, 2, 3, 4])?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 8];
                let mut b1 = [0u8; 8];
                let err = {
                    let mut serial_second = BlockBuf::new(&mut b1);
                    assert_eq!(serial_second.append(&[0xAA, 0xBB]), 2);
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: u64::MAX - 1,
                            buf: serial_second,
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("both halves fail, so the call fails")
                };
                assert_eq!(
                    err.kind(),
                    $crate::io::ErrorKind::UnexpectedEof,
                    "the request at index 0 is the descriptor one, so its short read wins: {err}",
                );
                Ok(())
            }

            /// A descriptor-backed request whose partly filled destination
            /// would push its offset past `u64` is refused before anything is
            /// read, not wrapped into a read elsewhere in the file.
            #[test_log::test]
            fn read_blocks_batched_rejects_an_overflowing_resume_offset_by_descriptor()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut buf = [0u8; 4];
                let err = {
                    let mut dst = BlockBuf::new(&mut buf);
                    assert_eq!(dst.append(&[0xAA, 0xBB]), 2);
                    let mut reqs = vec![BlockRead {
                        file: ring_file.as_ref(),
                        offset: u64::MAX - 1,
                        buf: dst,
                    }];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("an overflowing resume offset must be refused")
                };
                assert_eq!(err.kind(), $crate::io::ErrorKind::InvalidInput, "{err}");
                Ok(())
            }

            /// The same refusal on the serial side of a mixed batch.
            #[test_log::test]
            fn read_blocks_batched_rejects_an_overflowing_resume_offset_serially()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                std_file.sync_all()?;

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                let err = {
                    let mut buf1 = BlockBuf::new(&mut b1);
                    assert_eq!(buf1.append(&[0xAA, 0xBB]), 2);
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: u64::MAX - 1,
                            buf: buf1,
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("an overflowing resume offset must be refused")
                };
                assert_eq!(err.kind(), $crate::io::ErrorKind::InvalidInput, "{err}");
                Ok(())
            }

            /// A serial read that fails outright is reported as itself.
            #[test_log::test]
            fn read_blocks_batched_surfaces_a_serial_read_error_verbatim() -> $crate::io::Result<()>
            {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;

                // A descriptor-less handle whose reads are made to fail.
                let faulty = $crate::fs::FaultFs::new(StdFs);
                let mut std_file = faulty.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                std_file.sync_all()?;
                assert_eq!(std_file.backing_fd(), None);
                faulty.injector().arm($crate::fs::FaultRule::new(
                    $crate::fs::FaultOp::ReadAt,
                    $crate::fs::Fault::Error($crate::io::ErrorKind::PermissionDenied),
                ));

                let mut b0 = [0u8; 4];
                let mut b1 = [0u8; 4];
                let err = {
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut b0),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: 20,
                            buf: BlockBuf::new(&mut b1),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("the serial half's read fails")
                };
                assert_eq!(
                    err.kind(),
                    $crate::io::ErrorKind::PermissionDenied,
                    "the backend's own error is what the caller sees: {err}",
                );
                assert_eq!(b0, [10, 11, 12, 13], "the descriptor half still ran");
                Ok(())
            }

            /// A descriptor request that succeeds does not lend its index to a
            /// later one that fails: with a good read at 0, a failing serial
            /// read at 1 and a failing descriptor read at 2, index 1 wins.
            #[test_log::test]
            fn read_blocks_batched_blames_the_request_that_failed_not_its_group()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let opts = FsOpenOptions::new().write(true).create(true).read(true);

                let mut ring_file = fs.open(&dir.path().join("u.bin"), &opts)?;
                ring_file.write_all(&(0..=255u8).collect::<Vec<_>>())?;
                ring_file.sync_all()?;
                let mut std_file = StdFs.open(&dir.path().join("s.bin"), &opts)?;
                std_file.write_all(&[1, 2, 3, 4])?;
                std_file.sync_all()?;

                let mut good = [0u8; 4];
                let mut serial_bad = [0u8; 4];
                let mut ring_bad = [0u8; 8];
                let err = {
                    let mut serial_buf = BlockBuf::new(&mut serial_bad);
                    assert_eq!(serial_buf.append(&[0xAA, 0xBB]), 2);
                    let mut reqs = vec![
                        BlockRead {
                            file: ring_file.as_ref(),
                            offset: 10,
                            buf: BlockBuf::new(&mut good),
                        },
                        BlockRead {
                            file: std_file.as_ref(),
                            offset: u64::MAX - 1,
                            buf: serial_buf,
                        },
                        BlockRead {
                            // Reads past the 256-byte fixture's end.
                            file: ring_file.as_ref(),
                            offset: 252,
                            buf: BlockBuf::new(&mut ring_bad),
                        },
                    ];
                    fs.read_blocks_batched(&mut reqs)
                        .expect_err("two of the three requests fail")
                };
                assert_eq!(
                    err.kind(),
                    $crate::io::ErrorKind::InvalidInput,
                    "index 1 fails before index 2, so the serial error wins: {err}",
                );
                Ok(())
            }

            /// Handing requests over as they finish returns exactly what the
            /// ordered path returns.
            #[test_log::test]
            fn read_blocks_batched_each_returns_what_the_ordered_path_returns()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files = two_files(&fs, dir.path(), 32 * 64)?;
                let read = |each: bool| -> $crate::io::Result<(Vec<[u8; 64]>, Vec<usize>)> {
                    let mut buffers = vec![[0u8; 64]; 32];
                    let mut handed = Vec::new();
                    {
                        let mut reqs: Vec<_> = buffers
                            .iter_mut()
                            .enumerate()
                            .map(|(i, buf)| BlockRead {
                                file: files[i % 2].as_ref(),
                                offset: (i * 64) as u64,
                                buf: BlockBuf::new(buf),
                            })
                            .collect();
                        if each {
                            fs.read_blocks_batched_each(&mut reqs, &mut |i, req| {
                                assert!(req.buf.is_full(), "request {i} handed over unfilled");
                                handed.push(i);
                            })?;
                        } else {
                            fs.read_blocks_batched(&mut reqs)?;
                        }
                    }
                    handed.sort_unstable();
                    Ok((buffers, handed))
                };
                let (ordered, _) = read(false)?;
                let (each, handed) = read(true)?;
                assert_eq!(ordered, each);
                assert_eq!(handed, (0..32).collect::<Vec<_>>());
                Ok(())
            }

            /// A batch mixing reads that fail with reads that succeed: every
            /// successful read is filled and handed over, none that failed is,
            /// and the failure reported is always the lowest one.
            #[test_log::test]
            fn a_mixed_batch_drains_every_read_and_reports_the_lowest_failure()
            -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files = two_files(&fs, dir.path(), 16 * 16)?;
                for _ in 0..200 {
                    // Every third request reads past the end of its file.
                    let mut buffers = vec![[0xAAu8; 16]; 24];
                    let mut handed = Vec::new();
                    let verdict = {
                        let mut reqs: Vec<_> = buffers
                            .iter_mut()
                            .enumerate()
                            .map(|(i, buf)| BlockRead {
                                file: files[i % 2].as_ref(),
                                offset: if i % 3 == 1 {
                                    1 << 20
                                } else {
                                    (i % 16 * 16) as u64
                                },
                                buf: BlockBuf::new(buf),
                            })
                            .collect();
                        fs.read_blocks_batched_each(&mut reqs, &mut |i, _| handed.push(i))
                    };
                    let Err(error) = verdict else {
                        panic!("reads past the end must fail the batch");
                    };
                    assert_eq!(error.kind(), $crate::io::ErrorKind::UnexpectedEof);
                    handed.sort_unstable();
                    let succeeded: Vec<usize> = (0..24).filter(|i| i % 3 != 1).collect();
                    assert_eq!(handed, succeeded);
                    for &i in &succeeded {
                        assert_ne!(buffers[i], [0xAAu8; 16], "read {i} was drained and written");
                    }
                }
                Ok(())
            }

            /// An empty request is complete without a read, so it is handed
            /// over like every other finished one.
            #[test_log::test]
            fn an_empty_request_in_a_batch_is_handed_over() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files = two_files(&fs, dir.path(), 64)?;
                let mut first = [0u8; 16];
                let mut empty = [0u8; 0];
                let mut last = [0u8; 16];
                let mut handed = Vec::new();
                {
                    let mut reqs = vec![
                        BlockRead {
                            file: files[0].as_ref(),
                            offset: 0,
                            buf: BlockBuf::new(&mut first),
                        },
                        BlockRead {
                            file: files[1].as_ref(),
                            offset: 16,
                            buf: BlockBuf::new(&mut empty),
                        },
                        BlockRead {
                            file: files[1].as_ref(),
                            offset: 32,
                            buf: BlockBuf::new(&mut last),
                        },
                    ];
                    fs.read_blocks_batched_each(&mut reqs, &mut |i, _| handed.push(i))?;
                }
                handed.sort_unstable();
                assert_eq!(handed, [0, 1, 2]);
                Ok(())
            }

            /// Reads submitted to the queue across two files and several
            /// rounds come back with their tags and bytes.
            #[test_log::test]
            fn a_read_queue_hands_back_every_read_with_its_bytes() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 64 * 64)?
                    .into_iter()
                    .map(Arc::from)
                    .collect();
                let mut queue = fs.read_queue();
                let mut got: Vec<(usize, Vec<u8>)> = Vec::new();
                for round in 0..4usize {
                    for i in 0..16usize {
                        let tag = round * 16 + i;
                        queue.submit(QueuedRead {
                            tag,
                            file: Arc::clone(&files[tag % 2]),
                            offset: (tag * 64) as u64,
                            buf: vec![0; 64],
                        });
                    }
                    queue.wait(1, &mut |done| {
                        assert!(done.result.is_ok(), "read {} failed", done.tag);
                        got.push((done.tag, done.buf));
                    });
                }
                while queue.outstanding() > 0 {
                    queue.wait(1, &mut |done| {
                        assert!(done.result.is_ok(), "read {} failed", done.tag);
                        got.push((done.tag, done.buf));
                    });
                }
                got.sort_unstable_by_key(|(tag, _)| *tag);
                let expected: Vec<(usize, Vec<u8>)> = (0..64)
                    .map(|tag| (tag, expected_bytes(tag % 2, tag * 64, 64)))
                    .collect();
                assert_eq!(got, expected);
                Ok(())
            }

            /// A read past the end of its file comes back failed as a short
            /// read, and the reads beside it still come back read.
            #[test_log::test]
            fn a_read_queue_reports_a_short_read_as_failed() -> $crate::io::Result<()> {
                let Some(fs) = $make else {
                    return Ok(());
                };
                let dir = tempfile::tempdir()?;
                let files: Vec<Arc<dyn FsFile>> = two_files(&fs, dir.path(), 256)?
                    .into_iter()
                    .map(Arc::from)
                    .collect();
                let mut queue = fs.read_queue();
                for (tag, offset) in [(0usize, 0u64), (1, 1 << 20), (2, 64)] {
                    queue.submit(QueuedRead {
                        tag,
                        file: Arc::clone(&files[0]),
                        offset,
                        buf: vec![0; 64],
                    });
                }
                let mut got = Vec::new();
                while queue.outstanding() > 0 {
                    queue.wait(1, &mut |done| {
                        got.push((done.tag, done.result.map_err(|e| e.kind())));
                    });
                }
                got.sort_unstable_by_key(|(tag, _)| *tag);
                assert_eq!(
                    got,
                    [
                        (0, Ok(())),
                        (1, Err($crate::io::ErrorKind::UnexpectedEof)),
                        (2, Ok(()))
                    ]
                );
                Ok(())
            }

            /// A batched read through a `FaultFs` over the backend still fires
            /// a read rule on the block it names, failing the batch with that
            /// block's error after the blocks before it were read.
            #[test_log::test]
            fn a_fault_rule_fires_on_a_batched_read_through_the_wrapped_backend()
            -> $crate::io::Result<()> {
                use $crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
                let (Some(fs), Some(wrapped)) = ($make, $make) else {
                    return Ok(());
                };
                let fault = FaultFs::new(wrapped);
                let dir = tempfile::tempdir()?;
                let path = dir.path().join("batched");
                let bytes: Vec<u8> = (0..8 * 64)
                    .map(|i| u8::try_from(i % 251).expect("below 251"))
                    .collect();
                let mut writer = fs.open(&path, &FsOpenOptions::new().write(true).create(true))?;
                writer.write_all(&bytes)?;
                writer.sync_all()?;
                let file = fault.open(&path, &FsOpenOptions::new().read(true))?;
                /// A read of each 64-byte block of `file` into its buffer.
                fn block_reads<'a>(
                    file: &'a dyn FsFile,
                    buffers: &'a mut [[u8; 64]],
                ) -> Vec<BlockRead<'a>> {
                    buffers
                        .iter_mut()
                        .enumerate()
                        .map(|(i, buf)| BlockRead {
                            file,
                            offset: (i * 64) as u64,
                            buf: BlockBuf::new(buf),
                        })
                        .collect()
                }
                let mut buffers = vec![[0u8; 64]; 8];
                fault.read_blocks_batched(&mut block_reads(file.as_ref(), &mut buffers))?;
                for (i, buf) in buffers.iter().enumerate() {
                    assert_eq!(&buf[..], &bytes[i * 64..(i + 1) * 64], "block {i}");
                }

                fault.injector().arm(
                    FaultRule::new(FaultOp::ReadAt, Fault::Error($crate::io::ErrorKind::Other))
                        .on_path("batched")
                        .at_offset(5 * 64)
                        .once(),
                );
                let mut fresh = vec![[0u8; 64]; 8];
                let mut batch = block_reads(file.as_ref(), &mut fresh);
                let error = fault
                    .read_blocks_batched(&mut batch)
                    .expect_err("the rule fails the batch");
                assert_eq!(error.kind(), $crate::io::ErrorKind::Other);
                assert!(
                    batch.iter().take(5).all(|req| req.buf.is_full()),
                    "the blocks before the failing one were read"
                );
                Ok(())
            }
        }
    };
}

pub(crate) use batched_read_conformance;
