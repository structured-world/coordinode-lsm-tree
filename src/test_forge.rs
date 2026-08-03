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

/// RENAMES a TOC section to a DUPLICATE recognized name and re-stamps the
/// renamed section's block header ROLE plus the trailer's TOC checksum: the
/// section entries still tile perfectly and both names pass the
/// recognized-role walk, yet the reader's name lookup (`Toc::section`)
/// returns the FIRST match, so the renamed section is hidden — e.g.
/// `range_tombstones` renamed to a second `data` vanishes and its deleted
/// range resurrects. The block header at the section's offset is re-encoded
/// under `to_role` (a fresh header checksum; both must be SST block types so
/// the header length is unchanged and the payload / parity stay valid), so
/// the only remaining trace is the duplicate name. `from` and `to` may
/// differ in length (the TOC is rebuilt). The payload is untouched, so its
/// parity trailer still verifies.
pub fn forge_duplicate_section_name(
    path: &std::path::Path,
    from: &[u8],
    to: &[u8],
    to_role: crate::table::block::BlockType,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;

    let mut bytes = std::fs::read(path)?;
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

    // Locate the section's block offset so its header role can be re-stamped.
    let section_off = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == from) else {
            panic!("the SST must carry the section to rename");
        };
        usize::try_from(entry.pos()).expect("section offset fits usize")
    };
    // Re-encode the block header under the new role (SST block types have no
    // block_flags byte and equal header length, so the frame geometry and
    // the payload / parity trailer are untouched).
    {
        let Some(block) = bytes.get(section_off..) else {
            panic!("section block within the file");
        };
        let mut cursor = block;
        let header = Header::decode_from(&mut cursor)?;
        let header_len = Header::header_len(header.block_type);
        assert_eq!(
            header_len,
            Header::header_len(to_role),
            "the rerole must keep the header length so the geometry holds",
        );
        let new_header = Header {
            block_type: to_role,
            ..header
        };
        let mut hdr_bytes = Vec::with_capacity(header_len);
        new_header.encode_into(&mut hdr_bytes)?;
        let Some(dst) = bytes.get_mut(section_off..section_off + header_len) else {
            panic!("section header within the file");
        };
        dst.copy_from_slice(&hdr_bytes);
    }

    // Rebuild the TOC with the renamed entry (name length may change).
    let toc = bytes.get(toc_pos..toc_pos + toc_len).expect("TOC region");
    assert_eq!(toc.get(..4), Some(&b"TOC!"[..]), "TOC magic");
    // The splice below rebuilds the file as prefix + new TOC + trailer, so
    // any bytes between the TOC's end and the trailer would be silently
    // dropped; the writer emits them adjacent today — fail loudly if that
    // layout ever changes instead of producing a truncated fixture.
    assert_eq!(
        toc_pos + toc_len,
        trailer_start,
        "the TOC must sit directly before the trailer",
    );
    let count = u32::from_le_bytes(toc.get(4..8).expect("count").try_into().expect("4 bytes"));
    let mut new_toc = Vec::with_capacity(toc.len());
    new_toc.extend_from_slice(toc.get(..8).expect("TOC header"));
    let mut at = 8usize;
    let mut renamed = false;
    for _ in 0..count {
        let pos = toc.get(at..at + 16).expect("pos+len");
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
        new_toc.extend_from_slice(pos);
        if entry_name == from {
            renamed = true;
            new_toc.extend_from_slice(
                &u16::try_from(to.len())
                    .expect("name fits u16")
                    .to_le_bytes(),
            );
            new_toc.extend_from_slice(to);
        } else {
            new_toc.extend_from_slice(&u16::try_from(name_len).expect("fits u16").to_le_bytes());
            new_toc.extend_from_slice(entry_name);
        }
    }
    assert!(renamed, "section name present in the TOC");

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
    forge_meta_value_in_section(path, b"meta", key, forged_value)
}

/// As [`forge_tail_meta_value`], but applied to BOTH meta mirrors (`meta`
/// and `meta_mid`) so the copies stay CONSISTENT with each other: the mirror
/// comparison passes and only a cross-check of the decoded field against the
/// table's actual data can catch the forge.
pub fn forge_meta_value_both_mirrors(
    path: &std::path::Path,
    key: &[u8],
    forged_value: &[u8],
) -> crate::Result<()> {
    forge_meta_value_in_section(path, b"meta", key, forged_value)?;
    forge_meta_value_in_section(path, b"meta_mid", key, forged_value)
}

/// Shared body of the meta-value forges: patches `key`'s value inside the
/// named meta section's payload and re-stamps the block checksum plus, on a
/// parity-bearing build, the fixed RS(4,2) trailer.
fn forge_meta_value_in_section(
    path: &std::path::Path,
    section: &[u8],
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
        let Some(entry) = reader.toc().iter().find(|e| e.name() == section) else {
            panic!("the SST must carry the requested meta section");
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

/// Forges a VALUE inside the first data block of a footer-less, uncompressed
/// SST: searches for a payload byte whose flip leaves the block fully
/// decodable with the SAME keys, seqnos, and entry count while at least one
/// VALUE differs, then re-stamps the block checksum plus, for a
/// parity-bearing SST, the block's parity trailer. Every byte-level check
/// and every derived-metadata cross-check (keys, counts, layout) reads clean
/// — the manifest digest is the only remaining record of the original value
/// bytes. `shards` is the SST's descriptor scheme (`None` for a parity-less
/// table).
pub fn forge_value_byte_in_first_data_block(
    path: &std::path::Path,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::{Header, ParsedItem as _};

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
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("data payload within the file");
    };

    // Decode entries from a candidate payload; None when it fails to decode.
    let decode = |payload: &[u8]| -> Option<alloc::vec::Vec<crate::InternalValue>> {
        let block = crate::table::Block {
            header: Header {
                checksum: crate::Checksum::from_raw(crate::hash::hash128(payload)),
                ..header
            },
            data: crate::Slice::from(payload.to_vec()),
        };
        let data_block = crate::table::DataBlock::from_loaded(block, false).ok()?;
        let data = data_block.inner.data.clone();
        let iter = data_block
            .try_iter(crate::comparator::default_comparator())
            .ok()?;
        Some(iter.map(|p| p.materialize(&data)).collect())
    };
    let Some(baseline) = decode(payload) else {
        panic!("the healthy first data block decodes");
    };

    // Search for a flip that changes ONLY a value: same count, same keys,
    // same seqnos and value types, at least one differing value.
    let mut candidate = payload.to_vec();
    let flipped_at = (0..candidate.len()).find(|&i| {
        let Some(slot) = candidate.get_mut(i) else {
            return false;
        };
        *slot ^= 0xFF;
        let ok = decode(&candidate).is_some_and(|entries| {
            entries.len() == baseline.len()
                && entries
                    .iter()
                    .zip(&baseline)
                    .all(|(a, b)| a.key == b.key && a.value.len() == b.value.len())
                && entries
                    .iter()
                    .zip(&baseline)
                    .any(|(a, b)| a.value != b.value)
        });
        if !ok {
            let Some(slot) = candidate.get_mut(i) else {
                return false;
            };
            *slot ^= 0xFF;
        }
        ok
    });
    assert!(
        flipped_at.is_some(),
        "some payload byte flip must alter only a value while the block stays decodable",
    );
    {
        let Some(dst) = bytes.get_mut(payload_range.clone()) else {
            panic!("data payload within the file");
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
        panic!("data header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);

    // Re-stamp THIS block's parity trailer (the data section holds more
    // blocks after it, each with its own frame).
    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let parity =
            crate::ecc::encode_parity(&candidate, data_shards.into(), parity_shards.into())?;
        let Some(dst) = bytes.get_mut(payload_end..payload_end + parity.len()) else {
            panic!("parity trailer within the file");
        };
        dst.copy_from_slice(&parity);
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = shards;

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Forges the `block_layout` section by SHIFTING a MIDDLE cumulative end of
/// the first recorded entry down to the midpoint of its neighbors, then
/// re-stamps the block checksum plus, for a parity-bearing SST, the parity
/// trailer. The map stays structurally valid (strictly ascending offsets,
/// inner count >= 2, final end untouched) and every byte-level check reads
/// clean, while the recorded boundary now disagrees with the zstd frame's
/// real inner-block layout — the shape only a decode-derived cross-check
/// can catch. `shards` is the SST's descriptor scheme (`None` for a
/// parity-less table).
pub fn forge_block_layout_shift_middle_end(
    path: &std::path::Path,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::table::block::Header;

    let bytes = std::fs::read(path)?;
    let pos = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"block_layout") else {
            panic!("the SST must carry a block_layout section");
        };
        entry.pos()
    };
    let block_off = usize::try_from(pos).expect("section offset fits usize");
    let Some(block) = bytes.get(block_off..) else {
        panic!("section block within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) =
        bytes.get(block_off + header_len..block_off + header_len + header.data_length as usize)
    else {
        panic!("section payload within the file");
    };

    // Wire layout: [count u32] then per entry [offset u64 | inner u32 |
    // inner x end u32]. Patch the FIRST entry's second-to-last end.
    let read_u32 = |at: usize| {
        u32::from_le_bytes(
            payload
                .get(at..at + 4)
                .expect("u32 within the payload")
                .try_into()
                .expect("4 bytes"),
        )
    };
    assert!(read_u32(0) >= 1, "the map records at least one block");
    let inner = read_u32(12) as usize;
    assert!(inner >= 2, "a recorded block has at least two inner blocks");
    let target_at = 16 + (inner - 2) * 4;
    let prev = if inner >= 3 {
        read_u32(target_at - 4)
    } else {
        0
    };
    let current = read_u32(target_at);
    assert!(
        current > prev + 1,
        "the target boundary must leave room for a shifted midpoint",
    );
    let shifted = prev + (current - prev) / 2;

    let mut candidate = payload.to_vec();
    let Some(dst) = candidate.get_mut(target_at..target_at + 4) else {
        panic!("target end within the payload");
    };
    dst.copy_from_slice(&shifted.to_le_bytes());
    forge_replace_section_payload(path, b"block_layout", &candidate, shards)
}

/// Fills the first data block's embedded HASH INDEX with `MARKER_FREE` and
/// re-stamps the block header checksum plus, for a parity-bearing SST, the
/// block's parity trailer. Every logical entry, per-KV footer, and the outer
/// block checksum stay valid, so a sequential decode and the count / key /
/// seqno gates all pass — yet `point_read` trusts the hash index and returns
/// `None` for every existing key. The SST must be uncompressed and
/// unencrypted (the hash index is patched in place through the on-disk
/// payload), its blocks must carry a hash index (a non-zero
/// `data_block_hash_ratio`) AND per-KV checksum footers — the block is
/// re-parsed with `has_kv_footer = true` unconditionally, so a footer-less
/// block would misread its trailer. `shards` is the SST's descriptor
/// scheme (`None` for a parity-less table).
pub fn forge_hash_index_all_free(
    path: &std::path::Path,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::table::block::hash_index::MARKER_FREE;
    patch_first_data_block_hash_index(path, shards, |region| region.fill(MARKER_FREE))
}

/// Re-stamps the FIRST data block's hash-index bucket for `key` (a
/// CONFLICT marker for a key spanning restart intervals) to `binary_index_pos`,
/// so `point_read` follows the forged bucket straight to that restart head
/// instead of the sequential scan — returning an OLDER version of a
/// multi-version key while the sequential decode still sees the newest.
/// Every logical entry and the per-KV footer stay valid; only a
/// newest-version cross-check of the point-read result can catch it. Same
/// preconditions as [`forge_hash_index_all_free`].
pub fn forge_hash_index_bucket(
    path: &std::path::Path,
    key: &[u8],
    binary_index_pos: u8,
    shards: Option<(u8, u8)>,
) -> crate::Result<()> {
    use crate::table::block::hash_index::MARKER_CONFLICT;
    patch_first_data_block_hash_index(path, shards, |region| {
        // One byte per bucket, so the region length is the bucket count; the
        // modulo keeps the result below it, hence within usize.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "hash % region.len() < region.len() <= usize::MAX"
        )]
        let bucket = (crate::hash::hash64(key) % region.len() as u64) as usize;
        let Some(slot) = region.get_mut(bucket) else {
            panic!("bucket within the hash index");
        };
        assert_eq!(
            *slot, MARKER_CONFLICT,
            "the target key's bucket must be a conflict marker (spans restart intervals)",
        );
        *slot = binary_index_pos;
    })
}

/// Shared machinery for the hash-index forges: locates the FIRST data
/// block's embedded hash index, hands its on-disk bytes to `patch`, and
/// re-stamps the block header checksum plus (for a parity-bearing SST) the
/// block's parity trailer. The SST must be uncompressed, unencrypted, carry
/// a hash index, and carry per-KV footers (the block is re-parsed with
/// `has_kv_footer = true`). `shards` is the descriptor scheme (`None` when
/// parity-less).
fn patch_first_data_block_hash_index(
    path: &std::path::Path,
    shards: Option<(u8, u8)>,
    patch: impl FnOnce(&mut [u8]),
) -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::{Block, Header};
    use crate::table::{DataBlock, block::BlockType};

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
    let Some(payload) = bytes.get(payload_range.clone()) else {
        panic!("data payload within the file");
    };

    // Locate the hash index within the footer-stripped inner block. For an
    // uncompressed block the inner data is a prefix of the on-disk payload,
    // so the inner offset maps straight through.
    let (hi_offset, hi_len) = {
        let loaded = Block {
            header: Header {
                block_type: BlockType::Data,
                ..header
            },
            data: crate::Slice::from(payload.to_vec()),
        };
        let data_block = DataBlock::from_loaded(loaded, true)?;
        data_block
            .hash_index_span()
            .expect("the block must carry a hash index")
    };
    {
        let start = payload_range.start + hi_offset;
        let Some(region) = bytes.get_mut(start..start + hi_len) else {
            panic!("hash index within the payload");
        };
        patch(region);
    }

    // Re-stamp the block header checksum over the altered payload.
    let Some(payload) = bytes.get(payload_range.clone()) else {
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

    #[cfg(feature = "page_ecc")]
    if let Some((data_shards, parity_shards)) = shards {
        let payload_end = payload_range.end;
        let parity = crate::ecc::encode_parity(
            bytes.get(payload_range).expect("payload"),
            data_shards.into(),
            parity_shards.into(),
        )?;
        let Some(dst) = bytes.get_mut(payload_end..payload_end + parity.len()) else {
            panic!("parity trailer within the file");
        };
        dst.copy_from_slice(&parity);
    }
    #[cfg(not(feature = "page_ecc"))]
    let _ = shards;

    std::fs::write(path, &bytes)?;
    Ok(())
}

/// Forges the `filter` section so that the key hashing to `target_hash`
/// becomes a FALSE NEGATIVE: searches for a single payload byte whose flip
/// makes the (still parseable) `BuRR` filter report the hash as definitely
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
    let forged = truncated_tli_frame(path, table_id, ecc)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// As [`forge_tli_tail_truncated`], but applied to BOTH mirrors (`tli` and
/// `tli_tail`) so the copies stay CONSISTENT with each other: the decoded
/// mirror comparison passes and only a structural check of the handle list
/// against the physical data section can catch the dropped handle.
pub fn forge_tli_mirrors_truncated(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    let forged = truncated_tli_frame(path, table_id, ecc)?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Re-encodes BOTH TLI mirrors (`tli`, `tli_tail`) after LOWERING the first
/// block's separator (end) key to a truncated prefix of itself: still
/// strictly below the next block's separator, so the handle list stays
/// sorted, the mirrors stay equal, and the section tiling holds — but the
/// separator no longer matches the addressed block's real last key. After
/// reopen the index binary search routes keys in `(forged_separator,
/// real_last_key]` to the WRONG block, so `point_read` returns `None` for
/// existing keys. Only a cross-check of each separator against the
/// addressed block's decoded final key can catch it. The SST must be
/// unencrypted, its index uncompressed, and carry >= 2 data blocks.
pub fn forge_tli_mirrors_lower_first_separator(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    let forged = rebuilt_tli_frame(path, table_id, ecc, |handles| {
        use crate::table::KeyedBlockHandle;
        let first = handles.first().expect("at least two handles");
        let key = first.end_key().as_ref();
        assert!(key.len() >= 2, "separator key long enough to truncate");
        // A one-byte-shorter prefix is lexicographically smaller than the
        // original and still smaller than the next block's separator.
        let lowered = crate::UserKey::from(key.get(..key.len() - 1).expect("prefix"));
        let rebuilt = KeyedBlockHandle::new(lowered, first.seqno(), *first.as_ref());
        if let Some(slot) = handles.get_mut(0) {
            *slot = rebuilt;
        }
    })?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Re-stamps the LAST binary-index pointer of BOTH TLI mirrors (`tli`,
/// `tli_tail`) to the FIRST pointer's value, then re-encodes each frame
/// (fresh checksum, role, and, under `ecc`, parity). The entry stream is
/// untouched, so a sequential decode still yields every correct separator
/// and handle — mirror equality, section tiling, and the separator
/// cross-checks all pass — yet the index binary search trusts the forged
/// pointer and can land on the wrong restart head, silently missing keys
/// on seeks after reopen. Only a comparison of each pointer against the
/// sequentially derived restart heads can catch it. The SST must be
/// unencrypted, its index uncompressed, and carry >= 2 data blocks.
pub fn forge_tli_binary_index_pointer(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    use crate::table::block::Block;

    let (identity, transform, index) = tli_forge_frame(path, table_id, ecc)?;
    let (mut payload, bi_offset, bi_len, step) = {
        // Locate the binary index from the trailer metadata.
        let meta = index.decoder_meta().expect("tli trailer parses");
        (
            index.as_slice().to_vec(),
            usize::try_from(meta.binary_index_offset()).expect("offset fits usize"),
            usize::try_from(meta.binary_index_len()).expect("len fits usize"),
            usize::from(meta.binary_index_step_size()),
        )
    };
    assert!(
        bi_len >= 2,
        "the forge needs at least two pointers so first != last",
    );
    let first: Vec<u8> = payload
        .get(bi_offset..bi_offset + step)
        .expect("first pointer within the payload")
        .to_vec();
    let last_at = bi_offset + (bi_len - 1) * step;
    payload
        .get_mut(last_at..last_at + step)
        .expect("last pointer within the payload")
        .copy_from_slice(&first);

    let mut forged = Vec::new();
    Block::write_into(&mut forged, &payload, identity, &transform)?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Rebuilds the `locator` section from `entries` (`(key_hash, block_id,
/// slot)` triples under `Restart` precision) and re-frames it (fresh
/// checksum, `Locator` role, no ECC / encryption). The caller passes the
/// source's HONEST triples with one key's slot redirected to a later
/// restart interval: the block id stays correct so the block-id gate
/// passes, yet `point_read_at_slot` starts at the wrong interval and
/// returns an older version. The SST must be unencrypted, non-ECC,
/// already carry a `locator` section, AND have been written with
/// `Restart` precision — the helper hardcodes `Restart` and does not read
/// the source's precision byte, so forging an `Entry` / `Block`-precision
/// SST would silently change its slot semantics. `table_id` is the SST's
/// id (0 for a standalone Writer fixture).
pub fn forge_locator_slots(
    path: &std::path::Path,
    table_id: crate::TableId,
    entries: &[(u64, u64, u64)],
) -> crate::Result<()> {
    use crate::table::block::{Block, BlockIdentity, BlockType};

    let spec = crate::table::locator::LocatorSpec {
        precision: crate::config::LocatorPrecision::Restart,
        block_id_bits: None,
        slot_bits: None,
    };
    let Some(section) = crate::table::locator::build_locator_section(entries, spec) else {
        panic!("the forged locator entries must build a section");
    };
    let identity = BlockIdentity {
        table_id,
        block_type: BlockType::Locator,
        dict_id: 0,
        window_log: 0,
    };
    let transform = crate::table::block::BlockTransform::PLAIN;
    let mut forged = Vec::new();
    Block::write_into(&mut forged, &section, identity, &transform)?;
    replace_section_frame(path, b"locator", &forged)
}

/// Re-encodes the `zone_map` section WITHOUT its LAST block entry (fresh
/// checksum, `ZoneMap` role): paired with a TLI forge hiding the same
/// trailing block, the positioning chain over the remaining indexed
/// blocks stays self-consistent — the omitted block is invisible to every
/// index-driven check and only a physical data-section walk can find it.
/// The SST must be unencrypted, non-ECC, and carry a zone map with >= 2
/// entries.
pub fn forge_zone_map_drop_last_entry(
    path: &std::path::Path,
    table_id: crate::TableId,
) -> crate::Result<()> {
    use crate::table::block::{Block, BlockIdentity, BlockType};
    use crate::table::{BlockHandle, BlockOffset};

    let identity = BlockIdentity {
        table_id,
        block_type: BlockType::ZoneMap,
        dict_id: 0,
        window_log: 0,
    };
    let transform = crate::table::block::BlockTransform::from_parts(
        crate::CompressionType::None,
        None,
        #[cfg(zstd_any)]
        None,
    )?;

    let (pos, len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("the SST must carry a zone_map section");
        };
        (
            usize::try_from(entry.pos()).expect("pos fits usize"),
            usize::try_from(entry.len()).expect("len fits usize"),
        )
    };
    let blocks: Vec<(BlockOffset, Vec<crate::table::zone_map::ColumnStats>)> = {
        let file = crate::fs::Fs::open(
            &crate::fs::StdFs,
            path,
            &crate::fs::FsOpenOptions::new().read(true),
        )?;
        let block = Block::from_file(
            &*file,
            BlockHandle::new(
                BlockOffset(u64::try_from(pos).expect("pos fits u64")),
                u32::try_from(len).expect("section fits u32"),
            ),
            identity,
            &transform,
        )?;
        let map = crate::table::zone_map::ZoneMap::decode(&block.data)?;
        let entries = map.entries();
        assert!(
            entries.len() >= 2,
            "dropping the last entry must leave a non-empty map",
        );
        entries
            .get(..entries.len() - 1)
            .expect("all but the last entry")
            .iter()
            .map(|(off, cols)| (BlockOffset(*off), cols.clone()))
            .collect()
    };

    let mut payload = Vec::new();
    crate::table::zone_map::encode_zone_map(&mut payload, &blocks)?;
    let mut forged = Vec::new();
    Block::write_into(&mut forged, &payload, identity, &transform)?;
    replace_section_frame(path, b"zone_map", &forged)
}

/// Shared preamble of the TLI forges: the Index identity, the uncompressed
/// (optionally ECC) transform, and the DECODED `tli_tail` mirror. Every TLI
/// forge must agree on these — a drift between copies would silently write
/// an unreadable frame instead of the intended corruption. The SST must be
/// unencrypted and its index uncompressed.
fn tli_forge_frame(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<(
    crate::table::block::BlockIdentity,
    crate::table::block::BlockTransform<'static>,
    crate::table::IndexBlock,
)> {
    use crate::table::block::{Block, BlockIdentity, BlockType};
    use crate::table::{BlockHandle, BlockOffset, IndexBlock};

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
    Ok((identity, transform, IndexBlock::new(block)))
}

/// Re-encodes BOTH TLI mirrors (`tli`, `tli_tail`) with an INTERIOR handle
/// removed (the middle of the list), so the hidden block sits between two
/// indexed neighbours rather than at the section tail. The mirrors stay
/// equal and every remaining handle is intact — only a physical tiling
/// cross-check can notice the interior gap. The SST must be unencrypted,
/// its index uncompressed, and carry >= 3 data blocks.
pub fn forge_tli_mirrors_drop_interior(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    let forged = rebuilt_tli_frame(path, table_id, ecc, |handles| {
        assert!(
            handles.len() >= 3,
            "an interior drop needs a handle strictly between two neighbours",
        );
        handles.remove(handles.len() / 2);
    })?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Re-encodes BOTH TLI mirrors (`tli`, `tli_tail`) as a SINGLE handle that
/// starts at the first block and spans the ENTIRE summed size, keeping the
/// first block's separator. Cumulative tiling accepts it (one span covers
/// the section), the mirrors stay equal, and the separator matches the
/// spanned frame's decoded content (only the FIRST payload decodes; the
/// rest reads as an unrecognized trailer on a non-ECC block) — yet every
/// later physical block is unreachable through the index. Only a
/// per-handle comparison against the physical block frame can catch it.
/// The SST must be unencrypted, non-ECC, its index uncompressed, and
/// carry >= 2 data blocks.
pub fn forge_tli_mirrors_span_single_handle(
    path: &std::path::Path,
    table_id: crate::TableId,
) -> crate::Result<()> {
    let forged = rebuilt_tli_frame(path, table_id, None, |handles| {
        use crate::table::{BlockHandle, KeyedBlockHandle};
        // With a single handle the "spanning" replacement would be
        // byte-identical to the original — a silent no-op fixture instead
        // of the intended corruption.
        assert!(
            handles.len() >= 2,
            "spanning a single handle needs at least two handles to hide",
        );
        let total: u32 = handles.iter().map(|h| h.as_ref().size()).sum();
        let Some(first) = handles.first() else {
            panic!("the source carries data blocks");
        };
        let spanning = KeyedBlockHandle::new(
            first.end_key().clone(),
            first.seqno(),
            BlockHandle::new(first.as_ref().offset(), total),
        );
        *handles = vec![spanning];
    })?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Re-encodes BOTH TLI mirrors (`tli`, `tli_tail`) with the FIRST TWO
/// handles SWAPPED: every handle is still present and intact, the mirrors
/// stay equal, and the section is still fully covered — but the list is no
/// longer in offset (key) order. A physical tiling pass that trusts the
/// stored order double-covers the out-of-place block (once via the gap
/// probe, once via the handle) unless it re-sorts and skips covered spans.
/// The SST must be unencrypted, its index uncompressed, and carry >= 2
/// data blocks.
pub fn forge_tli_mirrors_swap_first_two(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<()> {
    let forged = rebuilt_tli_frame(path, table_id, ecc, |handles| {
        handles.swap(0, 1);
    })?;
    replace_section_frame(path, b"tli", &forged)?;
    replace_section_frame(path, b"tli_tail", &forged)
}

/// Decodes the `tli_tail` mirror's handle list, drops the LAST handle, and
/// returns the re-encoded Index frame (checksum-, role-, and, under `ecc`,
/// parity-consistent). The SST must be unencrypted and its index
/// uncompressed.
fn truncated_tli_frame(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
) -> crate::Result<Vec<u8>> {
    rebuilt_tli_frame(path, table_id, ecc, |handles| {
        handles.pop();
    })
}

/// Decodes the `tli_tail` mirror's handle list, applies `mutate`, and returns
/// the re-encoded Index frame (checksum-, role-, and, under `ecc`,
/// parity-consistent). The SST must be unencrypted and its index
/// uncompressed, and the mutated list must stay DECODABLE (the delta
/// encoding does not require sorted input — the reorder forge relies on
/// that to model an out-of-order forged index).
fn rebuilt_tli_frame(
    path: &std::path::Path,
    table_id: crate::TableId,
    ecc: Option<crate::table::block::EccParams>,
    mutate: impl FnOnce(&mut Vec<crate::table::KeyedBlockHandle>),
) -> crate::Result<Vec<u8>> {
    use crate::table::block::Block;
    use crate::table::{IndexBlock, KeyedBlockHandle};

    let (identity, transform, index) = tli_forge_frame(path, table_id, ecc)?;
    let mut handles: Vec<KeyedBlockHandle> = {
        use crate::table::block::ParsedItem as _;
        let mut out = Vec::new();
        for item in index.iter(crate::comparator::default_comparator()) {
            out.push(item.materialize(index.as_slice()));
        }
        out
    };
    assert!(
        handles.len() >= 2,
        "the forge needs at least two handles so the mutation leaves a valid index",
    );

    mutate(&mut handles);

    let payload = IndexBlock::encode_into_vec(&handles)?;
    let mut forged = Vec::new();
    Block::write_into(&mut forged, &payload, identity, &transform)?;
    Ok(forged)
}

/// Replaces the named single-block section's bytes with `forged`, shifting
/// every later section, patching the TOC's length + positions, and
/// re-stamping the trailer. The rebuilt archive stays internally consistent
/// in every byte-level check.
fn replace_section_frame(
    path: &std::path::Path,
    section: &[u8],
    forged: &[u8],
) -> crate::Result<()> {
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
    let (section_pos, section_len) = {
        let mut f = std::fs::File::open(path)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == section) else {
            panic!("the SST must carry the section to replace");
        };
        (
            usize::try_from(entry.pos()).expect("pos fits usize"),
            usize::try_from(entry.len()).expect("len fits usize"),
        )
    };

    // Rebuild the file: splice the forged frame in, shift the trailing
    // sections, fix the TOC's length + shifted positions, re-stamp the
    // trailer.
    let delta = i64::try_from(forged.len()).expect("forged block fits i64")
        - i64::try_from(section_len).expect("section fits i64");
    let mut out = Vec::with_capacity(bytes.len());
    out.extend_from_slice(bytes.get(..section_pos).expect("pre-section prefix"));
    out.extend_from_slice(forged);
    out.extend_from_slice(
        bytes
            .get(section_pos + section_len..toc_pos)
            .expect("post-section sections"),
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
        let (new_pos, new_len) = if name == section {
            (pos, forged.len() as u64)
        } else if pos > section_pos as u64 {
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
