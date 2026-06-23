use super::*;

/// Hand-built minimal zstd frame: magic + single-segment header (declared
/// content size 5) + one RLE last-block regenerating 5 bytes of 0x41.
/// Built without the compression feature so the structural-walk tests run
/// in the default build profile.
const RLE_FRAME: [u8; 10] = [0x28, 0xB5, 0x2F, 0xFD, 0x20, 0x05, 0x2B, 0x00, 0x00, 0x41];

#[test]
fn is_zstd_frame_matches_only_on_magic() {
    assert!(is_zstd_frame(&RLE_FRAME));
    assert!(!is_zstd_frame(b"not a frame"));
    assert!(!is_zstd_frame(&[0x28, 0xB5, 0x2F])); // too short
}

#[test]
fn census_zstd_frame_decodes_single_rle_block() {
    let census = census_zstd_frame(&RLE_FRAME).unwrap();
    assert_eq!(census.frame_header_len, 6);
    // Single-segment frame: window size falls back to the declared
    // content size (5).
    assert_eq!(census.window_size, Some(5));
    assert_eq!(census.frame_content_size, Some(5));
    assert!(!census.content_checksum);
    assert_eq!(census.blocks.len(), 1);
    let b = &census.blocks[0];
    assert_eq!(b.index, 0);
    assert_eq!(b.block_type, ZstdBlockType::Rle);
    assert!(b.last);
    assert_eq!(b.header_offset, 6);
    // RLE stores a single repeated byte on disk; regenerated size is 5.
    assert_eq!(b.content_len, 1);
    assert_eq!(b.decompressed_len, Some(5));
}

#[test]
fn census_zstd_frame_rejects_non_zstd_payload() {
    let err = census_zstd_frame(b"definitely not zstd").unwrap_err();
    assert!(matches!(err, crate::Error::InvalidHeader(_)));
}

#[test]
fn census_zstd_frame_rejects_truncated_frame() {
    // Drop the RLE block's single content byte: the block content now runs
    // past EOF.
    let err = census_zstd_frame(&RLE_FRAME[..RLE_FRAME.len() - 1]).unwrap_err();
    assert!(matches!(err, crate::Error::InvalidHeader(_)));
}

#[test]
fn census_zstd_frame_rejects_reserved_block_type() {
    // Same header shape as RLE_FRAME but block type bits = 3 (Reserved):
    // block-header raw = (5 << 3) | (3 << 1) | 1 = 0x2F.
    let mut frame = RLE_FRAME;
    frame[6] = 0x2F;
    let err = census_zstd_frame(&frame).unwrap_err();
    assert!(matches!(err, crate::Error::InvalidHeader(_)));
}

#[test]
fn ecc_scheme_parity_trailer_len() {
    // RS(4,2): four data shards + two parity shards → non-zero trailer.
    let rs = EccSchemeInfo::Shard {
        data_shards: 4,
        parity_shards: 2,
    };
    assert!(rs.parity_trailer_len(4096) > 0);
    // SEC-DED: one check byte per 8-byte word.
    assert_eq!(EccSchemeInfo::Secded.parity_trailer_len(64), 8);
    assert_eq!(EccSchemeInfo::Secded.parity_trailer_len(65), 9);
}

// Real-compression coverage: walk a frame produced by the actual zstd
// backend (multi-byte, multi-block-capable). Gated on a zstd build since
// the backend is only present then.
#[cfg(zstd_any)]
mod with_zstd {
    use super::*;
    use crate::compression::CompressionProvider;

    #[test]
    fn census_real_frame_walks_to_last_block() {
        let frame = crate::compression::ZstdBackend::compress(&vec![0x5Au8; 8192], 3).unwrap();
        assert!(is_zstd_frame(&frame));
        let census = census_zstd_frame(&frame).unwrap();
        assert!(!census.blocks.is_empty());
        assert!(census.frame_header_len >= 5);
        // Exactly one last block, and it is the final entry.
        assert_eq!(census.blocks.iter().filter(|b| b.last).count(), 1);
        assert!(census.blocks.last().unwrap().last);
        // Indices contiguous from zero; headers in-bounds and non-overlapping.
        let mut prev = u64::from(census.frame_header_len);
        for (i, b) in census.blocks.iter().enumerate() {
            assert_eq!(b.index as usize, i);
            assert!(b.header_offset >= prev);
            let end = b.header_offset + 3 + u64::from(b.content_len);
            assert!(end <= frame.len() as u64);
            prev = end;
        }
    }

    #[test]
    fn census_real_frame_rejects_truncation() {
        let frame = crate::compression::ZstdBackend::compress(&vec![0x7Eu8; 4096], 3).unwrap();
        let truncated = &frame[..frame.len() - 8];
        let err = census_zstd_frame(truncated).unwrap_err();
        assert!(matches!(err, crate::Error::InvalidHeader(_)));
    }
}
