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
    use crate::table::block::kv_checksum::FOOTER_TAIL_LEN;
    // The LAST byte of the digest array, just before the fixed algo+count
    // tail — the footer stays structurally intact.
    flip_and_restamp_first_data_block(path, FOOTER_TAIL_LEN + 1)
}

/// Flips the LAST payload byte of the first data block and re-stamps the
/// block header checksum: the frame reads internally valid while its bytes
/// no longer match what the manifest digest was computed over. For an
/// uncompressed, footer-less SST this models an in-band alteration only the
/// manifest-level digest can catch.
pub fn forge_restamped_data_block(path: &std::path::Path) -> crate::Result<()> {
    flip_and_restamp_first_data_block(path, 1)
}

/// RENAMES an SFA TOC section (same-length name) and re-stamps the trailer's
/// TOC checksum: the archive stays internally consistent while a section the
/// verifier knows disappears behind an unrecognized name — the shape only a
/// fail-closed unknown-section check can catch (every block inside still
/// passes its own byte-level checks).
pub fn forge_section_name(path: &std::path::Path, from: &[u8], to: &[u8]) -> crate::Result<()> {
    assert_eq!(from.len(), to.len(), "the rename must keep the name length");

    let mut bytes = std::fs::read(path)?;
    // SFA trailer layout (fixed size, at the very end of the file):
    // magic "SFA!" | version u8 | checksum_type u8 | toc_checksum u128 LE |
    // toc_pos u64 LE | toc_len u64 LE.
    const TRAILER_SIZE: usize = 4 + 1 + 1 + 16 + 8 + 8;
    let trailer_start = bytes.len() - TRAILER_SIZE;
    let read_u64 = |bytes: &[u8], at: usize| {
        let Some(b) = bytes.get(at..at + 8) else {
            panic!("u64 field within the trailer");
        };
        u64::from_le_bytes(b.try_into().expect("8 bytes"))
    };
    let toc_pos = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16))
        .expect("toc_pos fits usize");
    let toc_len = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16 + 8))
        .expect("toc_len fits usize");

    {
        let Some(toc) = bytes.get_mut(toc_pos..toc_pos + toc_len) else {
            panic!("TOC region within the file");
        };
        // Walk the parsed TOC entries instead of searching raw bytes: a raw
        // window search on `b"filter"` would also match inside the
        // `filter_tli` entry's name. Layout: "TOC!" | count u32 LE | entries
        // (pos u64 | len u64 | name_len u16 | name).
        let count = u32::from_le_bytes(
            toc.get(4..8)
                .expect("count prefix")
                .try_into()
                .expect("4 bytes"),
        );
        let mut at = 8usize;
        let mut name_at = None;
        for _ in 0..count {
            let name_len = usize::from(u16::from_le_bytes(
                toc.get(at + 16..at + 18)
                    .expect("name_len")
                    .try_into()
                    .expect("2 bytes"),
            ));
            let entry_name = toc.get(at + 18..at + 18 + name_len).expect("name");
            if entry_name == from {
                name_at = Some(at + 18);
                break;
            }
            at += 18 + name_len;
        }
        let Some(name_at) = name_at else {
            panic!("section name present in the TOC");
        };
        let Some(dst) = toc.get_mut(name_at..name_at + to.len()) else {
            panic!("section name within the TOC");
        };
        dst.copy_from_slice(to);
    }

    let Some(toc) = bytes.get(toc_pos..toc_pos + toc_len) else {
        panic!("TOC region within the file");
    };
    let fresh = {
        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        hasher.update(toc);
        hasher.digest128()
    };
    let Some(dst) = bytes.get_mut(trailer_start + 4 + 1 + 1..trailer_start + 4 + 1 + 1 + 16) else {
        panic!("toc_checksum within the trailer");
    };
    dst.copy_from_slice(&fresh.to_le_bytes());
    std::fs::write(path, &bytes)?;
    Ok(())
}

/// OMITS an SFA TOC entry entirely and re-stamps the trailer's TOC checksum
/// and length: the archive stays internally consistent while a whole section
/// vanishes from every reader's sight (its bytes are still in the file, but
/// nothing references them) — the shape only a TOC coverage check can catch,
/// since every remaining block still passes its own byte-level checks.
pub fn forge_section_omitted(path: &std::path::Path, name: &[u8]) -> crate::Result<()> {
    let bytes = std::fs::read(path)?;
    // SFA trailer layout (fixed size, at the very end of the file):
    // magic "SFA!" | version u8 | checksum_type u8 | toc_checksum u128 LE |
    // toc_pos u64 LE | toc_len u64 LE.
    const TRAILER_SIZE: usize = 4 + 1 + 1 + 16 + 8 + 8;
    let trailer_start = bytes.len() - TRAILER_SIZE;
    let read_u64 = |bytes: &[u8], at: usize| {
        let Some(b) = bytes.get(at..at + 8) else {
            panic!("u64 field within the trailer");
        };
        u64::from_le_bytes(b.try_into().expect("8 bytes"))
    };
    let toc_pos = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16))
        .expect("toc_pos fits usize");
    let toc_len = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16 + 8))
        .expect("toc_len fits usize");

    // Parse the TOC: "TOC!" magic | count u32 LE | entries
    // (pos u64 | len u64 | name_len u16 | name).
    let toc = bytes.get(toc_pos..toc_pos + toc_len).expect("TOC region");
    assert_eq!(toc.get(..4), Some(&b"TOC!"[..]), "TOC magic");
    let count = u32::from_le_bytes(toc.get(4..8).expect("count").try_into().expect("4 bytes"));
    let mut new_toc = Vec::with_capacity(toc.len());
    new_toc.extend_from_slice(b"TOC!");
    new_toc.extend_from_slice(&count.checked_sub(1).expect("TOC has entries").to_le_bytes());
    let mut at = 8usize;
    let mut omitted = false;
    for _ in 0..count {
        let entry_start = at;
        at += 16;
        let name_len = usize::from(u16::from_le_bytes(
            toc.get(at..at + 2)
                .expect("name_len")
                .try_into()
                .expect("2 bytes"),
        ));
        at += 2;
        let entry_name = toc.get(at..at + name_len).expect("name");
        at += name_len;
        if entry_name == name {
            omitted = true;
        } else {
            new_toc.extend_from_slice(toc.get(entry_start..at).expect("entry"));
        }
    }
    assert!(omitted, "section name present in the TOC");

    let fresh = {
        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        hasher.update(&new_toc);
        hasher.digest128()
    };
    let mut out = Vec::with_capacity(toc_pos + new_toc.len() + TRAILER_SIZE);
    out.extend_from_slice(bytes.get(..toc_pos).expect("pre-TOC prefix"));
    out.extend_from_slice(&new_toc);
    out.extend_from_slice(
        bytes
            .get(trailer_start..trailer_start + 4 + 1 + 1)
            .expect("trailer head"),
    );
    out.extend_from_slice(&fresh.to_le_bytes());
    out.extend_from_slice(
        &u64::try_from(toc_pos)
            .expect("toc_pos fits u64")
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u64::try_from(new_toc.len())
            .expect("TOC length fits u64")
            .to_le_bytes(),
    );
    std::fs::write(path, &out)?;
    Ok(())
}

/// REPLACES the value of `key` inside the TAIL `meta` block's payload with
/// `forged_value` (same length), then re-stamps the block checksum and (on a
/// parity-bearing build) recomputes the RS(4,2) trailer: the tail mirror
/// stays internally consistent in every byte-level check while its DECODED
/// metadata now disagrees with the intact `meta_mid` mirror — the shape only
/// a full mirror comparison can catch. The key must be present verbatim
/// (meta blocks use restart interval 1) with a one-byte length prefix
/// matching `forged_value.len()`.
pub fn forge_tail_meta_value(
    path: &std::path::Path,
    key: &[u8],
    forged_value: &[u8],
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"meta") else {
            panic!("the SST must carry a meta section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).expect("meta offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("meta block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("meta payload within the file");
        };
        let Some(key_pos) = payload.windows(key.len()).position(|w| w == key) else {
            panic!("meta key present verbatim (restart interval 1)");
        };
        // Entry layout after the key bytes: value length (LEB128, one byte
        // for these small values), then the value itself.
        let val_at = key_pos + key.len();
        assert_eq!(
            payload.get(val_at).copied(),
            u8::try_from(forged_value.len()).ok(),
            "the forged value must keep the original length",
        );
        let Some(value) = payload.get_mut(val_at + 1..val_at + 1 + forged_value.len()) else {
            panic!("meta value within the payload");
        };
        value.copy_from_slice(forged_value);
    }
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("meta payload within the file");
    };
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("meta header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    // A parity-bearing meta frame (self-describing blocks always use the
    // fixed RS(4,2) layout) must have its trailer recomputed over the forged
    // payload, or the walk would flag the forge ITSELF as parity rot.
    #[cfg(feature = "page_ecc")]
    {
        let payload_end = payload_range.end;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("meta section length fits usize");
        };
        let frame_end = block_off + section_len;
        if frame_end > payload_end {
            let Some(payload) = bytes.get(payload_range) else {
                panic!("meta payload within the file");
            };
            let parity = crate::ecc::encode_parity(payload, 4, 2)?;
            assert_eq!(
                frame_end - payload_end,
                parity.len(),
                "the meta frame's trailer length matches the fixed RS(4,2) layout",
            );
            let Some(dst) = bytes.get_mut(payload_end..frame_end) else {
                panic!("meta parity trailer within the file");
            };
            dst.copy_from_slice(&parity);
        }
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = section_len;

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// INFLATES the trailer `item_count` of the first data block by one and
/// re-stamps the block header checksum: the block stays checksum-clean but
/// iterating it yields FEWER entries than the trailer declares, modelling a
/// truncated / partially-decodable entry region that a count cross-check
/// must catch (the entry decoder turns a mid-stream parse failure into an
/// ordinary end of iteration). The SST must be uncompressed, and must carry
/// NO parity trailer: this helper re-stamps only the header checksum (unlike
/// [`forge_tail_meta_value`]), so a parity-bearing block would additionally
/// read as parity rot rather than as a clean-but-under-decoding block.
pub fn forge_inflated_item_count(path: &std::path::Path) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
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

    // The item count is the LAST u32 of the block payload (the trailer's
    // final field).
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("data payload within the file");
        };
        let count_at = payload.len() - core::mem::size_of::<u32>();
        let Some(count_le) = payload.get_mut(count_at..) else {
            panic!("item count within the payload");
        };
        let count = u32::from_le_bytes(count_le.try_into().expect("4 bytes"));
        count_le.copy_from_slice(&(count + 1).to_le_bytes());
    }

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

/// RELABELS the first block of the named SFA section to `forged` and
/// re-encodes its header (fresh header CRC, payload and its checksum
/// untouched): the block stays checksum-clean while its ROLE no longer
/// matches the section that holds it, modelling a re-stamped `block_type`
/// forge that only a section-vs-role cross-check can catch. The forged
/// type must have the same header length as the original (all SST block
/// types without `block_flags` do).
pub fn forge_section_block_role(
    path: &std::path::Path,
    section: &[u8],
    forged: crate::table::block::BlockType,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let block_off = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == section) else {
            panic!("the SST must carry the targeted section");
        };
        usize::try_from(entry.pos()).expect("section offset fits usize")
    };
    let Some(block) = bytes.get(block_off..) else {
        panic!("section block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    assert_eq!(
        header_len,
        Header::header_len(forged),
        "the forged role must keep the header length so the relabel is in place",
    );

    let new_header = Header {
        block_type: forged,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("section header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Flips the LAST byte of a named section's PAYLOAD and re-stamps the block
/// checksum (plus, for a parity-bearing SST, the descriptor-scheme parity
/// trailer). The section stays structurally valid and every byte-level check
/// reads clean while its decoded content now disagrees with the blocks it
/// summarizes — the shape only a content cross-check can catch. For the
/// `zone_map` section the last payload byte is the last byte of the final
/// block's `max` value, so this narrows/changes a recorded key range.
/// `shards` is the SST's descriptor scheme (`None` for a parity-less table).
pub fn forge_flip_section_last_payload_byte(
    path: &std::path::Path,
    section: &[u8],
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == section) else {
            panic!("the SST must carry the targeted section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).expect("section offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("section block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("section payload within the file");
        };
        let last = payload.len() - 1;
        let Some(slot) = payload.get_mut(last) else {
            panic!("payload is non-empty");
        };
        *slot ^= 0xFF;
    }
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("section payload within the file");
    };
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("section header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("section length fits usize");
        };
        let frame_end = block_off + section_len;
        if frame_end > payload_end {
            let Some(payload) = bytes.get(payload_range) else {
                panic!("section payload within the file");
            };
            let parity =
                crate::ecc::encode_parity(payload, data_shards.into(), parity_shards.into())?;
            assert_eq!(
                frame_end - payload_end,
                parity.len(),
                "the frame's trailer length matches the descriptor scheme",
            );
            let Some(dst) = bytes.get_mut(payload_end..frame_end) else {
                panic!("parity trailer within the file");
            };
            dst.copy_from_slice(&parity);
        }
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = (section_len, shards);

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Overwrites a named block-format section's PAYLOAD with `new_payload` (which
/// MUST be the same length as the existing payload, so the frame geometry is
/// unchanged) and re-stamps the block checksum plus, for a parity-bearing SST,
/// the descriptor-scheme parity trailer. Models a re-stamped section whose
/// content was swapped for another structurally valid payload — every
/// byte-level check reads clean while the decoded content disagrees with the
/// blocks it describes. `shards` is the SST's descriptor scheme (`None` for a
/// parity-less table).
pub fn forge_replace_section_payload(
    path: &std::path::Path,
    section: &[u8],
    new_payload: &[u8],
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == section) else {
            panic!("the SST must carry the targeted section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).expect("section offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("section block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    assert_eq!(
        payload_range.len(),
        new_payload.len(),
        "the replacement payload must keep the frame geometry",
    );
    {
        let Some(dst) = bytes.get_mut(payload_range.clone()) else {
            panic!("section payload within the file");
        };
        dst.copy_from_slice(new_payload);
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(new_payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("section header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("section length fits usize");
        };
        let frame_end = block_off + section_len;
        if frame_end > payload_end {
            let parity =
                crate::ecc::encode_parity(new_payload, data_shards.into(), parity_shards.into())?;
            assert_eq!(
                frame_end - payload_end,
                parity.len(),
                "the frame's trailer length matches the descriptor scheme",
            );
            let Some(dst) = bytes.get_mut(payload_end..frame_end) else {
                panic!("parity trailer within the file");
            };
            dst.copy_from_slice(&parity);
        }
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = (section_len, shards);

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Forges the `filter` section so that the key hashing to `target_hash`
/// becomes a FALSE NEGATIVE: searches for a single payload byte whose flip
/// makes the (still parseable) BuRR filter report the hash as definitely
/// absent, then re-stamps the block checksum plus, for a parity-bearing SST,
/// the block's parity trailer. Every byte-level and framing check reads
/// clean while a point read for that key is silently skipped — the shape
/// only a probe of the filter against the blocks' decoded keys can catch.
///
/// Operates on the FIRST filter block of the section (in partitioned mode
/// that is the partition covering the lowest keys, so pass the hash of the
/// table's first key). The table must be unencrypted (the payload is probed
/// as plaintext). `shards` is the SST's descriptor scheme (`None` for a
/// parity-less table).
pub fn forge_filter_false_negative(
    path: &std::path::Path,
    target_hash: u64,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::filter::ribbon::burr::contains_hash_from_bytes;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"filter") else {
            panic!("the SST must carry a filter section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).expect("section offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("section block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("section payload within the file");
    };
    assert!(
        matches!(contains_hash_from_bytes(payload, target_hash), Ok(true)),
        "the target key must be present in the healthy filter",
    );
    // Search for one byte whose flip turns the target hash into a false
    // negative while the filter still PARSES (a parse failure would be an
    // unreadable filter, not the silent-skip shape this forge models).
    let mut candidate = payload.to_vec();
    let flipped_at = (0..candidate.len()).find(|&i| {
        let Some(slot) = candidate.get_mut(i) else {
            return false;
        };
        *slot ^= 0xFF;
        let miss = matches!(contains_hash_from_bytes(&candidate, target_hash), Ok(false));
        if !miss {
            let Some(slot) = candidate.get_mut(i) else {
                return false;
            };
            *slot ^= 0xFF;
        }
        miss
    });
    assert!(
        flipped_at.is_some(),
        "some payload byte flip must produce a parseable false-negative filter",
    );
    {
        let Some(dst) = bytes.get_mut(payload_range.clone()) else {
            panic!("section payload within the file");
        };
        dst.copy_from_slice(&candidate);
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(&candidate));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("section header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    // Re-stamp THIS block's parity trailer only (the section may hold more
    // partition blocks after it, each with its own frame).
    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let parity =
            crate::ecc::encode_parity(&candidate, data_shards.into(), parity_shards.into())?;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("section length fits usize");
        };
        assert!(
            payload_end + parity.len() <= block_off + section_len,
            "the parity trailer stays within the section",
        );
        let Some(dst) = bytes.get_mut(payload_end..payload_end + parity.len()) else {
            panic!("parity trailer within the file");
        };
        dst.copy_from_slice(&parity);
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = (section_len, shards);

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// ZEROES the LAST entry's `[seqno_min, seqno_max]` inside the
/// `seqno_bounds` section's payload and re-stamps the block checksum (plus,
/// for a parity-bearing SST, the descriptor-scheme parity trailer): the map
/// stays structurally valid (`min <= max`, offsets untouched) and every
/// byte-level check reads clean, while `scan_since_seqno` now SKIPS the
/// block for any window above zero — the shape only a cross-check against
/// the block's actually-decoded entries can catch. `shards` is the SST's
/// descriptor scheme (`None` for a parity-less table).
pub fn forge_seqno_bounds_zeroed_entry(
    path: &std::path::Path,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
    let (pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"seqno_bounds") else {
            panic!("the SST must carry a seqno_bounds section");
        };
        (entry.pos(), entry.len())
    };
    let block_off = usize::try_from(pos).expect("section offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("seqno_bounds block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("seqno_bounds payload within the file");
        };
        // Wire layout: [count u32 LE] then count x [offset u64 | min u64 | max u64].
        let count = u32::from_le_bytes(
            payload
                .get(..4)
                .expect("count prefix")
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        assert!(count >= 1, "the map records at least one block");
        let min_at = 4 + (count - 1) * 24 + 8;
        let Some(minmax) = payload.get_mut(min_at..min_at + 16) else {
            panic!("last entry's bounds within the payload");
        };
        minmax.fill(0);
    }
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("seqno_bounds payload within the file");
    };
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(payload));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("seqno_bounds header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    // Recompute the descriptor-scheme parity trailer over the forged payload
    // so a parity-bearing SST reads clean rather than as parity rot.
    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let Ok(section_len) = usize::try_from(section_len) else {
            panic!("section length fits usize");
        };
        let frame_end = block_off + section_len;
        if frame_end > payload_end {
            let Some(payload) = bytes.get(payload_range) else {
                panic!("seqno_bounds payload within the file");
            };
            let parity =
                crate::ecc::encode_parity(payload, data_shards.into(), parity_shards.into())?;
            assert_eq!(
                frame_end - payload_end,
                parity.len(),
                "the frame's trailer length matches the descriptor scheme",
            );
            let Some(dst) = bytes.get_mut(payload_end..frame_end) else {
                panic!("parity trailer within the file");
            };
            dst.copy_from_slice(&parity);
        }
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = (section_len, shards);

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// REPLACES the `tli_tail` mirror with a re-encoded index block whose LAST
/// handle was dropped, shifting the following `meta` section and re-stamping
/// the TOC + trailer: every byte-level check (checksum, parity, role) stays
/// clean while the tail mirror now DECODES to a different handle list than
/// the intact head `tli` — the shape only a decoded mirror comparison can
/// catch. `read_tli` prefers the tail on the next recovery, so the forged
/// mirror silently hides the last data block's keys. The SST must be
/// unencrypted and its index uncompressed; `ecc` is the table's descriptor
/// scheme (`None` for a parity-less SST) so the forged block carries valid
/// parity where the original did.
pub fn forge_tli_tail_truncated(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    use crate::table::block::{Block, BlockIdentity, BlockType};
    use crate::table::{BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle};

    let identity = BlockIdentity {
        table_id,
        block_type: BlockType::Index,
        dict_id: 0,
        window_log: 0,
    };
    let transform = {
        let t = crate::table::block::BlockTransform::from_parts(
            crate::CompressionType::None,
            None,
            #[cfg(zstd_any)]
            None,
        )?;
        if let Some(ecc) = ecc {
            t.with_ecc(ecc)
        } else {
            t
        }
    };

    // Locate the sections and decode the tail mirror's handle list.
    let bytes = std::fs::read(path)?;
    const TRAILER_SIZE: usize = 4 + 1 + 1 + 16 + 8 + 8;
    let trailer_start = bytes.len() - TRAILER_SIZE;
    let read_u64 = |bytes: &[u8], at: usize| {
        let Some(b) = bytes.get(at..at + 8) else {
            panic!("u64 field within the trailer");
        };
        u64::from_le_bytes(b.try_into().expect("8 bytes"))
    };
    let toc_pos = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16))
        .expect("toc_pos fits usize");
    let (tail_pos, tail_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"tli_tail") else {
            panic!("the SST must carry a tli_tail mirror");
        };
        (
            usize::try_from(entry.pos()).expect("pos fits usize"),
            usize::try_from(entry.len()).expect("len fits usize"),
        )
    };
    let handles: Vec<KeyedBlockHandle> = {
        let file = crate::fs::Fs::open(
            &crate::fs::StdFs,
            path,
            &crate::fs::FsOpenOptions::new().read(true),
        )?;
        let block = Block::from_file(
            &*file,
            BlockHandle::new(
                BlockOffset(u64::try_from(tail_pos).expect("pos fits u64")),
                u32::try_from(tail_len).expect("tail section fits u32"),
            ),
            identity,
            &transform,
        )?;
        use crate::table::block::ParsedItem as _;
        let index = IndexBlock::new(block);
        let mut out = Vec::new();
        for item in index.iter(crate::comparator::default_comparator()) {
            out.push(item.materialize(index.as_slice()));
        }
        out
    };
    assert!(
        handles.len() >= 2,
        "the forge needs at least two handles so dropping one leaves a valid index",
    );

    // Re-encode without the LAST handle and frame it as a fresh Index block.
    let truncated = handles.get(..handles.len() - 1).expect("non-empty prefix");
    let payload = IndexBlock::encode_into_vec(truncated)?;
    let mut forged = Vec::new();
    Block::write_into(&mut forged, &payload, identity, &transform)?;

    // Rebuild the file: shift the trailing sections (`meta`), fix the TOC's
    // `tli_tail` length + shifted positions, re-stamp the trailer.
    let delta = i64::try_from(forged.len()).expect("forged block fits i64")
        - i64::try_from(tail_len).expect("tail section fits i64");
    let mut out = Vec::with_capacity(bytes.len());
    out.extend_from_slice(bytes.get(..tail_pos).expect("pre-tail prefix"));
    out.extend_from_slice(&forged);
    out.extend_from_slice(
        bytes
            .get(tail_pos + tail_len..toc_pos)
            .expect("post-tail sections"),
    );
    let new_toc_pos = out.len();

    // Rebuild the TOC from the old one, patching lengths/positions.
    let toc_len = usize::try_from(read_u64(&bytes, trailer_start + 4 + 1 + 1 + 16 + 8))
        .expect("toc_len fits usize");
    let toc = bytes.get(toc_pos..toc_pos + toc_len).expect("TOC region");
    assert_eq!(toc.get(..4), Some(&b"TOC!"[..]), "TOC magic");
    let count = u32::from_le_bytes(toc.get(4..8).expect("count").try_into().expect("4 bytes"));
    let mut new_toc = Vec::with_capacity(toc.len());
    new_toc.extend_from_slice(toc.get(..8).expect("TOC header"));
    let mut at = 8usize;
    for _ in 0..count {
        let pos = read_u64(toc, at);
        let len = read_u64(toc, at + 8);
        let name_len = usize::from(u16::from_le_bytes(
            toc.get(at + 16..at + 18)
                .expect("name_len")
                .try_into()
                .expect("2 bytes"),
        ));
        let name = toc.get(at + 18..at + 18 + name_len).expect("name");
        let (new_pos, new_len) = if name == b"tli_tail" {
            (pos, forged.len() as u64)
        } else if pos > tail_pos as u64 {
            (
                pos.checked_add_signed(delta).expect("shifted pos fits u64"),
                len,
            )
        } else {
            (pos, len)
        };
        new_toc.extend_from_slice(&new_pos.to_le_bytes());
        new_toc.extend_from_slice(&new_len.to_le_bytes());
        new_toc.extend_from_slice(toc.get(at + 16..at + 18 + name_len).expect("name field"));
        at += 18 + name_len;
    }
    let fresh = {
        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        hasher.update(&new_toc);
        hasher.digest128()
    };
    out.extend_from_slice(&new_toc);
    out.extend_from_slice(
        bytes
            .get(trailer_start..trailer_start + 4 + 1 + 1)
            .expect("trailer head"),
    );
    out.extend_from_slice(&fresh.to_le_bytes());
    out.extend_from_slice(
        &u64::try_from(new_toc_pos)
            .expect("toc_pos fits u64")
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u64::try_from(new_toc.len())
            .expect("TOC length fits u64")
            .to_le_bytes(),
    );
    std::fs::write(path, &out)?;
    Ok(())
}

/// Shared core: flips `payload[len - flip_from_end]` of the FIRST data
/// block of the SST at `path`, then recomputes the block header checksum
/// over the altered payload so block-level verification reads clean.
fn flip_and_restamp_first_data_block(
    path: &std::path::Path,
    flip_from_end: usize,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

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

    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("data payload within the file");
        };
        let flip_at = payload.len() - flip_from_end;
        let Some(slot) = payload.get_mut(flip_at) else {
            panic!("flip offset within the payload");
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
