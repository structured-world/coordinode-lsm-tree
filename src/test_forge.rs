//! Shared on-disk forgery helpers for corruption tests.
//!
//! These deliberately construct byte patterns a healthy writer never emits
//! (stale footers behind re-stamped checksums), so integrity tests can prove
//! the read/repair paths fail closed. Test-only: never compiled into the
//! production library.

#![expect(
    clippy::expect_used,
    reason = "test helpers assert on known-present values; a panic is the failure signal"
)]

/// Forges a STALE per-KV footer behind a RE-STAMPED block checksum in the
/// first data block of the SST at `path`: flips one digest byte inside the
/// footer's checksum array, then recomputes the block header checksum over
/// the altered payload. Block-level verification then reads clean while
/// per-KV verification still detects the mismatch.
///
/// The SST must be uncompressed (the payload is patched in place) and
/// footer-bearing (written under `KvChecksumPolicy::AllLevels`).
// `pub` (not `pub(crate)`): the module itself is `pub(crate)`, so the item
// stays crate-internal either way and clippy::redundant_pub_crate fires on
// the doubled restriction.
pub fn forge_stale_kv_footer(path: &std::path::Path) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::block::kv_checksum::FOOTER_TAIL_LEN;

    let mut bytes = std::fs::read(path)?;
    // The data section is the first SFA section, so the first data block
    // starts at its position.
    let block_off = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"data") else {
            panic!("the SST must carry a data section");
        };
        usize::try_from(entry.pos()).expect("data offset fits usize")
    };
    let Some(block) = bytes.get(block_off..) else {
        panic!("data block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;

    // Flip the LAST byte of the digest array (just before the fixed
    // algo+count tail), leaving the footer structurally intact.
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("data payload within the file");
        };
        let digest_end = payload.len() - FOOTER_TAIL_LEN;
        let Some(slot) = payload.get_mut(digest_end - 1) else {
            panic!("digest array within the payload");
        };
        *slot ^= 0xFF;
    }

    // Re-stamp the block header checksum over the altered payload so the
    // block-level walk reads clean. Fail loudly on a bad range: silently
    // hashing an empty slice would leave the block failing the ORDINARY
    // checksum walk, proving nothing about the stale-footer path.
    let Some(payload) = bytes.get(payload_range) else {
        panic!("data payload within the file");
    };
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("data header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(path, &bytes)?;
    Ok(())
}
