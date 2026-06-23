// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

#[cfg(zstd_any)]
use crate::compression::CompressionProvider as _;

#[cfg(not(feature = "std"))]
use crate::io::{Cursor, Read};
use crate::io::{LittleEndian, ReadBytesExt};
use crate::{
    BlobFile, Checksum, CompressionType, UserValue,
    fs::FsFile,
    vlog::{
        ValueHandle,
        blob_file::writer::{
            BLOB_HEADER_LEN_V4, BLOB_HEADER_MAGIC_V3, BLOB_HEADER_MAGIC_V4, validate_header_crc,
        },
    },
};
#[cfg(feature = "std")]
use std::io::{Cursor, Read};

/// Safety cap on blob value size (256 MiB).
///
/// Enforced on this reader and on the write path to prevent producing
/// or accepting blobs that are unreasonably large. Other internal
/// readers (e.g., scanner used by compaction/GC) may impose different
/// constraints.
///
/// NOTE: Intentionally duplicated in `vlog::blob_file::writer` and
/// `table::block` rather than shared, because blocks and blobs are
/// independent storage formats that may diverge in the future.
const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;

/// Reads a single blob from a blob file
pub struct Reader<'a> {
    blob_file: &'a BlobFile,
    file: &'a dyn FsFile,

    /// Dictionary for `ZstdDict` decompression.  Must be supplied when the
    /// blob file's compression type is [`CompressionType::ZstdDict`].
    #[cfg(zstd_any)]
    zstd_dictionary: Option<&'a crate::compression::ZstdDictionary>,
}

impl<'a> Reader<'a> {
    pub fn new(blob_file: &'a BlobFile, file: &'a dyn FsFile) -> Self {
        Self {
            blob_file,
            file,
            #[cfg(zstd_any)]
            zstd_dictionary: None,
        }
    }

    /// Provides the zstd dictionary for [`CompressionType::ZstdDict`] blobs.
    ///
    /// Must be called when the blob file's metadata reports `ZstdDict`
    /// compression.  Passing `None` clears a previously set dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_dict(mut self, dict: Option<&'a crate::compression::ZstdDictionary>) -> Self {
        self.zstd_dictionary = dict;
        self
    }

    #[expect(
        clippy::too_many_lines,
        reason = "blob read/validation path is kept in one function so error handling and size checks stay co-located"
    )]
    pub fn get(&self, key: &'a [u8], vhandle: &'a ValueHandle) -> crate::Result<UserValue> {
        debug_assert_eq!(vhandle.blob_file_id, self.blob_file.id());

        // Enforce the same key-length constraint as the writer (u16::MAX)
        // so that a caller cannot inflate the computed read size.
        if key.len() > u16::MAX as usize {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Always read with V4 (max) header size so that version detection
        // is self-describing from the frame magic — no dependency on
        // metadata version which could be corrupted independently.
        // For V3 frames, the extra 4 bytes read are harmless: they come
        // from the next frame or metadata section (which always follows),
        // and raw_data is sliced to exact on_disk_val_len before use.
        let add_size = (BLOB_HEADER_LEN_V4 as u64) + (key.len() as u64);

        // Validate the full on-disk read size (header + key + value) against the limit.
        // Allow header+key overhead on top of the data cap.
        // NOTE: A separate `on_disk_size > MAX` check is mathematically redundant here
        // because `total > MAX + overhead` already implies `on_disk_size > MAX`.
        // 256 MiB cap plus a small (≤ header + u16 key) overhead — bounded well
        // within u64 (see the `add_size < u32::MAX` note below), so a plain add
        // cannot overflow.
        let max_total_read_size = (MAX_DECOMPRESSION_SIZE as u64) + add_size;

        // on_disk_size is u32 and add_size < u32::MAX, so this cannot overflow u64.
        let total_read_size = u64::from(vhandle.on_disk_size) + add_size;

        if total_read_size > max_total_read_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: total_read_size,
                limit: max_total_read_size,
            });
        }

        // After the cap check, total_read_size <= ~256 MiB + overhead, which fits
        // in usize on all supported platforms (>= 32-bit).
        #[expect(
            clippy::cast_possible_truncation,
            reason = "bounded to MAX_DECOMPRESSION_SIZE + overhead by the check above"
        )]
        let read_len = total_read_size as usize;

        let value = crate::file::read_exact(self.file, vhandle.offset, read_len)?;

        let mut reader = Cursor::new(&value[..]);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        // Determine format from frame magic — self-describing, no metadata dependency.
        let frame_is_v4 = magic == BLOB_HEADER_MAGIC_V4;
        if !frame_is_v4 && magic != BLOB_HEADER_MAGIC_V3 {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        let expected_checksum = reader.read_u128::<LittleEndian>()?;

        let seqno = reader.read_u64::<LittleEndian>()?;
        let key_len = reader.read_u16::<LittleEndian>()?;

        let real_val_len = reader.read_u32::<LittleEndian>()? as usize;

        let on_disk_val_len = reader.read_u32::<LittleEndian>()?;

        // V4: read and validate header CRC before cross-checks.
        // Uses the on-disk CRC value (not recomputed) in data checksum
        // verification so that recomputing header_crc after tampering
        // header fields is still caught by the data checksum.
        let stored_header_crc = if frame_is_v4 {
            let crc = reader.read_u32::<LittleEndian>()?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "real_val_len originates as u32, round-tripped through usize; lossless on supported targets"
            )]
            validate_header_crc(seqno, key_len, real_val_len as u32, on_disk_val_len, crc)?;
            Some(crc)
        } else {
            // V3: seqno is unused (not covered by any checksum).
            let _ = seqno;
            None
        };

        // Cross-check header fields against caller-provided inputs to catch
        // corruption or mismatched handles early, before checksum/decompression.
        if key_len as usize != key.len() || on_disk_val_len != vhandle.on_disk_size {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Validate real_val_len before checksum/decompression to fail fast
        // on malformed headers and avoid unnecessary hashing work.
        if real_val_len > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: real_val_len as u64,
                limit: MAX_DECOMPRESSION_SIZE as u64,
            });
        }

        // Actual header length determined from frame magic, not metadata.
        let header_len = if frame_is_v4 {
            BLOB_HEADER_LEN_V4
        } else {
            crate::vlog::blob_file::writer::BLOB_HEADER_LEN_V3
        };

        // Zero-copy view of the on-disk key bytes for checksum and cross-check.
        // The full blob record is already in `value`, so slicing avoids an extra
        // allocation vs UserKey::from_reader (upstream #277).
        let on_disk_key = value.slice(header_len..header_len + key_len as usize);

        // Ensure the stored key bytes exactly match the caller-provided key.
        // This protects against handles that point at a different key with the
        // same length (e.g., due to corruption or misuse).
        if on_disk_key != key {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Slice exactly on_disk_val_len bytes — important for V3 backward
        // compat where the read buffer is 4 bytes larger than the actual frame
        // (over-read from using V4 max header size).
        // No usize overflow: on_disk_val_len is u32, data_offset is ~42+key_len,
        // and total is bounded by MAX_DECOMPRESSION_SIZE (256 MiB) cap check above.
        let data_offset = header_len + key.len();
        let raw_data = value.slice(data_offset..data_offset + on_disk_val_len as usize);

        {
            // Checksum covers on-disk key + raw value data (upstream #277).
            // V4 additionally includes header_crc bytes so that recomputing
            // header_crc after tampering header fields is still detected.
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&on_disk_key);
                hasher.update(&raw_data);
                if let Some(hcrc) = stored_header_crc {
                    hasher.update(&hcrc.to_le_bytes());
                }
                hasher.digest128()
            };

            if expected_checksum != checksum {
                log::error!(
                    "Checksum mismatch for blob {vhandle:?}, got={checksum}, expected={expected_checksum}",
                );

                return Err(crate::Error::ChecksumMismatch {
                    got: Checksum::from_raw(checksum),
                    expected: Checksum::from_raw(expected_checksum),
                });
            }
        }

        #[warn(clippy::match_single_binding)]
        let value = match &self.blob_file.0.meta.compression {
            CompressionType::None => {
                if real_val_len != raw_data.len() {
                    return Err(crate::Error::InvalidHeader("Blob"));
                }
                raw_data
            }

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let mut buf = vec![0u8; real_val_len];

                let bytes_written = lz4_flex::decompress_into(&raw_data, &mut buf)
                    .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

                UserValue::from(buf)
            }

            #[cfg(zstd_any)]
            CompressionType::Zstd(_) => {
                let decompressed =
                    crate::compression::ZstdBackend::decompress(&raw_data, real_val_len)
                        .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                if decompressed.len() != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

                UserValue::from(decompressed)
            }

            #[cfg(zstd_any)]
            CompressionType::ZstdDict { dict_id, .. } => {
                let dict = self.zstd_dictionary.ok_or(crate::Error::ZstdDictMismatch {
                    expected: *dict_id,
                    got: None,
                })?;

                if dict.id() != *dict_id {
                    return Err(crate::Error::ZstdDictMismatch {
                        expected: *dict_id,
                        got: Some(dict.id()),
                    });
                }

                let decompressed = crate::compression::ZstdBackend::decompress_with_dict(
                    &raw_data,
                    dict,
                    real_val_len,
                )
                .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                if decompressed.len() != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

                UserValue::from(decompressed)
            }
        };

        debug_assert_eq!(real_val_len, value.len());

        Ok(value)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing, reason = "test code")]
mod tests;
