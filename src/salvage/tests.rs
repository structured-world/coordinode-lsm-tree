use super::{BlobDropReason, DropReason, salvage_blob_file, salvage_sst};
// The options-bearing entry is exercised on every feature set: the
// prefix-extractor round-trip below is ungated (extractors are core
// configuration), so the import must not hide behind the encrypted /
// dictionary / delete-resurrection gates its other consumers carry.
use super::{SalvageOptions, salvage_sst_with_options};
use crate::comparator::default_comparator;
use crate::fs::{Fs, StdFs};
use crate::table::{Table, Writer};
use crate::{InternalValue, ValueType};
use alloc::sync::Arc;
use tempfile::tempdir;
use test_log::test;

/// Rot in a frame's length field is caught by the header CRC, after which
/// the consumed lengths cannot locate the next frame: the salvage walk must
/// resynchronize at the next real frame instead of stopping — without that,
/// one rotted header would cost every otherwise-readable record in the tail.
#[test]
fn salvage_blob_file_resyncs_a_frame_after_a_length_rot() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_rot");
    let dest = dir.path().join("blob_rot_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(
        &source,
        &fs,
        &[
            (b"aaaa", b"AAAAAAAA"),
            (b"bbbb", b"BBBBBBBB"),
            (b"cccc", b"CCCCCCCC"),
        ],
    )?;

    // Rot the SECOND frame's key_len (4 -> 6). Frame layout: magic 4 |
    // checksum 16 | seqno 8 | key_len 2 | real_val_len 4 | on_disk_val_len 4
    // | header_crc 4 | key | value = 42 + 4 + 8 bytes; the data section
    // starts at file offset 0.
    let frame_len = 42 + 4 + 8;
    let kl_off = frame_len + 4 + 16 + 8;
    let mut bytes = std::fs::read(&source)?;
    let Some(slot) = bytes.get_mut(kl_off..kl_off + 2) else {
        panic!("second frame's key_len within the file");
    };
    slot.copy_from_slice(&6u16.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest, &fs, 0)?;
    assert_eq!(
        report.records_salvaged, 2,
        "the frames before AND after the rotted one are recovered: {report:?}",
    );
    assert!(
        report
            .dropped
            .iter()
            .any(|d| matches!(&d.reason, BlobDropReason::Corrupt(msg) if msg.contains("HeaderCrcMismatch"))),
        "the rotted frame drops as a header CRC mismatch: {report:?}",
    );

    // The recovered copy carries exactly the two intact records.
    let Some(salvaged) = report.salvaged_path else {
        panic!("two records were salvaged");
    };
    let keys: Vec<Vec<u8>> = BlobScanner::new(&salvaged, &*fs, 0)?
        .map(|r| r.map(|e| e.key.to_vec()))
        .collect::<crate::Result<_>>()?;
    assert_eq!(keys, vec![b"aaaa".to_vec(), b"cccc".to_vec()]);
    Ok(())
}

/// A forged `tli_tail` that DROPS a handle passes every byte-level check
/// (fresh checksum, valid framing, Index role): a walk following the tail
/// would silently skip the hidden block and "recover" a copy missing its
/// keys — no drop, no error, just vanished rows. Pins that the salvage open
/// walks the HEAD `tli` copy, so a forged tail cannot steer which blocks
/// are recovered.
#[test]
fn salvage_recovers_all_blocks_under_a_forged_tli_tail() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Tiny block budget so the SST spills several data blocks (several TLI
    // handles — the forge needs at least two).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    crate::test_forge::forge_tli_tail_truncated(&source, 0, None)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.dropped.is_empty(),
        "every block is intact, nothing may drop: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 64,
        "the block the forged tail hides must still be recovered: {report:?}",
    );
    Ok(())
}

/// BOTH TLI mirrors forged to the SAME truncated handle list pass every
/// byte-level check AND the mirror comparison (two forged copies prove
/// nothing), so the salvage open's block index simply omits the hidden
/// block. A walk trusting that index neither recovers the block nor
/// reports it dropped — repair then installs an apparently complete copy
/// with the block's keys silently missing. The physical data-section
/// tiling is the only ground truth: salvage must frame and recover the
/// bytes the index does not cover.
#[test]
fn salvage_recovers_a_block_hidden_by_forged_tli_mirrors() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Tiny block budget so the SST spills several data blocks (several TLI
    // handles — the forge needs at least two).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    crate::test_forge::forge_tli_mirrors_truncated(&source, 0, None)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.dropped.is_empty(),
        "every block is intact, nothing may drop: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 64,
        "the block both forged mirrors hide must still be recovered: {report:?}",
    );
    Ok(())
}

/// BOTH TLI mirrors re-encoded with the first two handles SWAPPED keep
/// every block present and the section fully covered — but the stored
/// order is no longer the offset order the physical tiling assumes. A
/// tiling pass trusting that order frames the out-of-place block through
/// the gap probe AND pushes its handle again: the duplicate emit is
/// rejected by the writer's ordering validation, so an intact block is
/// reported dropped and the block totals are inflated. The tiling must
/// re-sort by offset and skip spans it already covered.
#[test]
fn salvage_recovers_all_blocks_under_a_reordered_tli() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    crate::test_forge::forge_tli_mirrors_swap_first_two(&source, 0, None)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.dropped.is_empty(),
        "every block is intact and covered once, nothing may drop: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 64,
        "a reordered index must not double-walk or lose blocks: {report:?}",
    );
    Ok(())
}

/// A partitioned index whose MIDDLE leaf partition is corrupt yields only the
/// earlier partitions' handles before erroring, setting `index_broken`. The
/// physical data section is still intact, writer-ordered, and self-framing, so
/// salvage must walk it independently and recover EVERY data block instead of
/// re-emitting only the enumerated prefix and silently dropping the failed and
/// later partitions' blocks.
#[test]
fn salvage_recovers_physical_blocks_past_a_broken_index_partition() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Tiny data blocks + a partitioned index spread the 256 blocks across
    // several leaf partitions (no range tombstone, so salvage re-emits rather
    // than failing closed).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(128)
        .use_partitioned_index();
    for i in 0u64..256 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Corrupt the middle of the `index` section (a leaf partition), desyncing
    // the handle enumeration partway while leaving the data section intact.
    let (index_pos, index_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let Some((pos, len)) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"index")
            .map(|e| (e.pos(), e.len()))
        else {
            panic!("a partitioned-index SST must carry an index section");
        };
        (pos, len)
    };
    let Ok(flip) = usize::try_from(index_pos + index_len / 2) else {
        panic!("the index-section offset fits usize");
    };
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert_eq!(
        report.entries_salvaged, 256,
        "every data block must be recovered via the physical walk when the \
         index enumeration breaks: {report:?}",
    );
    Ok(())
}

/// A range tombstone hidden by RENAMING its section to a recognized name whose
/// block decodes cleanly (here an empty `filter`, since filtering is disabled)
/// must fail salvage closed. The rename keeps the catalogue uniquely named and
/// tiled and the block loads without degrading, so neither the TOC-concealment
/// check nor the rebuildable-section degradation flag fires — but the persisted
/// `range_tombstone_count` still records the tombstone, so salvage cross-checks
/// it and refuses rather than re-emitting the covered keys as live.
#[test]
fn salvage_refuses_a_range_tombstone_hidden_as_a_recognized_section() -> crate::Result<()> {
    use crate::UserKey;
    use crate::config::BloomConstructionPolicy;
    use crate::range_tombstone::RangeTombstone;
    use crate::table::block::BlockType;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Filtering disabled, so renaming range_tombstones to `filter` yields a
    // UNIQUE recognized name (no existing filter to duplicate).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0.0));
    for i in 0u64..8 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    writer.write_range_tombstone(RangeTombstone::new(
        UserKey::from(b"key-002".as_slice()),
        UserKey::from(b"key-005".as_slice()),
        9,
    ));
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Rename range_tombstones -> filter and re-role its block to Filter: the
    // block loads as a (garbage) filter WITHOUT degrading, so the
    // rebuildable-section flag stays clear and the parsed table reports no range
    // tombstones — but the persisted count still records them.
    crate::test_forge::forge_duplicate_section_name(
        &source,
        b"range_tombstones",
        b"filter",
        BlockType::Filter,
    )?;

    let Err(err) = salvage_sst(&source, dest, &fs) else {
        panic!("a hidden range tombstone must fail salvage");
    };
    let crate::Error::FeatureUnsupported(reason) = &err else {
        panic!("the refusal must be FeatureUnsupported, got {err:?}");
    };
    assert!(
        reason.contains("range tombstones"),
        "the refusal must name range tombstones specifically, got {reason:?}",
    );
    Ok(())
}

/// When the broken-index physical walk hits an UNFRAMEABLE block header, it
/// must RESYNCHRONIZE forward to the next parseable frame instead of abandoning
/// the rest of the gap. Otherwise the intact, self-framed blocks after the
/// smashed one are neither recovered nor individually reported — the salvage
/// silently finishes with only the prefix.
#[test]
fn salvage_resyncs_past_an_unframeable_block_in_the_physical_walk() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(128)
        .use_partitioned_index();
    for i in 0u64..256 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Smash the header of a block in the FIRST THIRD of the data section (its
    // first byte, so `probe_block_handle_at` cannot frame it). A LATE key lives
    // in a block AFTER it, so the physical walk must resync past the smash to
    // recover that block.
    let smash_offset = {
        let table = open(source.clone(), &fs)?;
        let offsets: Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|h| *h.as_ref().offset())
            .collect();
        let Some(&off) = offsets.get(offsets.len() / 3) else {
            panic!("need several data blocks, got {}", offsets.len());
        };
        off
    };
    {
        let mut bytes = std::fs::read(&source)?;
        let Ok(at) = usize::try_from(smash_offset) else {
            panic!("data block offset {smash_offset} fits usize");
        };
        let Some(b) = bytes.get_mut(at) else {
            panic!("the block header at {at} lies within the file");
        };
        *b ^= 0xFF;
        std::fs::write(&source, &bytes)?;
    }

    // Corrupt the `index` section so enumeration breaks and the physical walk
    // tiles the whole data section from the start.
    let (index_pos, index_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = crate::sfa::Reader::from_reader(&mut f)?;
        let Some((pos, len)) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"index")
            .map(|e| (e.pos(), e.len()))
        else {
            panic!("a partitioned-index SST must carry an index section");
        };
        (pos, len)
    };
    let Ok(flip) = usize::try_from(index_pos + index_len / 2) else {
        panic!("the index-section offset fits usize");
    };
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    // A late key (in a block after the smashed one) must still be recovered:
    // the walk drops the unframeable block, resyncs forward, and frames the
    // rest of the section.
    assert!(
        reopen_get(dest, &fs, b"key-250")?.is_some(),
        "a block after the unframeable one must recover via resync: {report:?}",
    );
    Ok(())
}

/// An INTERIOR handle omitted from both forged mirrors leaves the hidden
/// block between two indexed neighbours, exercising the mid-list gap
/// probe (the truncated-mirror sibling only covers the section-tail
/// probe). The block must be framed from the physical tiling and
/// recovered like any indexed block.
#[test]
fn salvage_recovers_an_interior_block_hidden_by_forged_tli_mirrors() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    crate::test_forge::forge_tli_mirrors_drop_interior(&source, 0, None)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.dropped.is_empty(),
        "every block is intact, nothing may drop: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 64,
        "the interior block both forged mirrors hide must still be recovered: {report:?}",
    );
    Ok(())
}

/// BOTH TLI mirrors re-encoded as a SINGLE handle spanning the whole data
/// section pass the cumulative tiling (one span covers the section), the
/// mirror comparison, and the separator cross-check (only the FIRST
/// payload decodes; the tail reads as an unrecognized trailer on a
/// non-ECC block) — yet every later physical block is unreachable through
/// the index, so reads silently miss its keys after the table is
/// accepted. Each handle must frame EXACTLY one physical block.
#[test]
fn verify_tli_mirrors_rejects_a_section_spanning_handle() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Sanity: the intact table passes the mirror gate.
    open(source.clone(), &fs)?.verify_tli_mirrors()?;

    crate::test_forge::forge_tli_mirrors_span_single_handle(&source, 0)?;

    let table = open(source, &fs)?;
    // Match the frame-check reason specifically: the gate returns several
    // distinct InvalidHeader reasons, and an unrelated one would keep this
    // green without proving the per-handle physical-frame check ran.
    let Err(err) = table.verify_tli_mirrors() else {
        panic!("a handle spanning several physical blocks must fail the mirror gate");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader(
                "an index handle's size disagrees with its block's physical frame"
            )
        ),
        "the spanning handle must be rejected by the physical-frame check, got {err:?}",
    );
    Ok(())
}

/// BOTH TLI mirrors re-encoded as a SINGLE handle spanning the whole data
/// section must not blind the SALVAGE walk: advancing the physical cursor
/// by the untrusted indexed size skips every later block, and the oversized
/// non-ECC handle still decodes its first payload — one block "salvaged",
/// zero drops, and repair installs a copy with the rest silently lost. The
/// tiler must trust each handle's span only after the block's own header
/// confirms it, falling back to the physically framed span otherwise.
#[test]
fn salvage_recovers_all_blocks_under_a_section_spanning_handle() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    crate::test_forge::forge_tli_mirrors_span_single_handle(&source, 0)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.dropped.is_empty(),
        "every block is intact, nothing may drop: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 64,
        "the blocks a spanning handle hides must still be recovered: {report:?}",
    );
    Ok(())
}

/// An index handle whose offset sits BEYOND the data section (a checksum-
/// repatched / forged entry) must not set the gap-probe's upper bound: probing
/// the range from the cursor up to that out-of-section offset would scan past
/// the section, potentially to an attacker-controlled `u64` (an unbounded hang,
/// and later SST sections read as candidate data frames). The tiler must skip
/// the out-of-section handle and bound the walk by the section end.
#[test]
fn salvage_ignores_an_index_handle_beyond_the_data_section() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    let n = 64u32;
    for i in 0..u64::from(n) {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Append an index handle at u64::MAX/2 (far past the data section). The real
    // handles stay intact, so a walk that bounds the gap probe to the section
    // recovers every block and finishes; the pre-fix walk scans up to the bogus
    // offset instead. The nextest slow-timeout terminates a hang, so completing
    // at all — with the full key range recovered — is the proof.
    crate::test_forge::forge_tli_mirrors_offset_beyond_section(&source, 0)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every real block is recovered once the out-of-section handle is skipped: {report:?}",
    );
    assert!(
        reopen_get(dest, &fs, b"key-060")?.is_some(),
        "a late key must survive the bounded walk: {report:?}",
    );
    Ok(())
}

/// A cleanly enumerated index handle whose block header does NOT frame and
/// whose stored span is oversized (a spanning forge with a smashed header)
/// must not advance the physical cursor by that UNVERIFIED size: trusting it
/// covers the whole remaining data section, so the gap walk never discovers
/// the later intact blocks and every one but the unframeable block is lost.
/// The tiler must leave the cursor where it is and let the physical resync
/// frame the blocks after the smashed one.
#[test]
fn salvage_recovers_blocks_after_an_unframeable_oversized_handle() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Resolve the FIRST data block's offset (the block the spanning handle
    // keeps) BEFORE the forge — the data section is first, so the tli rewrite
    // that follows only shifts later sections and leaves this offset valid.
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(off) = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|h| *h.as_ref().offset())
            .next()
        else {
            panic!("the source carries data blocks");
        };
        off
    };

    // Collapse both TLI mirrors into ONE handle spanning the whole data section
    // (size = sum of every block size), then smash that block's header first
    // byte so it cannot frame: the handle enumerates cleanly through the
    // mirror gate, but its oversized span is now UNVERIFIABLE.
    crate::test_forge::forge_tli_mirrors_span_single_handle(&source, 0)?;
    {
        let mut bytes = std::fs::read(&source)?;
        let Ok(at) = usize::try_from(first_off) else {
            panic!("data block offset {first_off} fits usize");
        };
        let Some(b) = bytes.get_mut(at) else {
            panic!("the block header lies within the file");
        };
        *b ^= 0xFF;
        std::fs::write(&source, &bytes)?;
    }

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    // A late key (in a block after the smashed spanning one) must survive: the
    // walk drops the unframeable block, then resyncs and frames the rest.
    assert!(
        reopen_get(dest, &fs, b"key-060")?.is_some(),
        "blocks after the unframeable oversized handle must recover via resync: {report:?}",
    );
    assert!(
        report.entries_salvaged > 1,
        "more than the single unframeable block must be recovered: {report:?}",
    );
    Ok(())
}

/// A forged `data` SFA-section length whose `pos + len` OVERFLOWS `u64` must
/// not be trusted as the physical-walk upper bound: saturating the bound to
/// `u64::MAX` makes the byte-at-a-time resync probe every nonexistent offset
/// up to that bound, effectively hanging standalone salvage. The walk must
/// reject a section that ends past the SFA `toc_pos` (or overflows) and fall
/// back to the intact index enumeration, completing promptly.
#[test]
fn salvage_rejects_overflowing_data_section_and_completes() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    let n = 64u32;
    for i in 0..u64::from(n) {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Forge the `data` section's advertised length to `u64::MAX` so its end
    // overflows: the index stays intact, so a walk that rejects the bogus
    // section recovers every block through the index enumeration instead of
    // scanning to the overflowed bound.
    crate::test_forge::forge_section_len(&source, b"data", u64::MAX)?;

    // The nextest slow-timeout terminates a hang; completing at all — with the
    // full key range recovered from the index — is the proof the bound was
    // rejected rather than tiled to.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "the intact index must recover every entry once the bogus section is rejected: {report:?}",
    );
    assert!(
        reopen_get(dest, &fs, b"key-060")?.is_some(),
        "a late key must survive the index-only recovery: {report:?}",
    );
    Ok(())
}

/// A source whose `seqno_bounds` section is PRESENT but does not decode must
/// fail SALVAGE closed on a table that exposes NO deletion metadata: a
/// re-stamped TOC can rename a `range_tombstones` / `delete_bitmap` section to
/// `seqno_bounds` and re-role its block, leaving a uniquely named, tiled
/// catalogue whose parsed table reports no deletion. Salvage re-derives the
/// seqno bounds from the recovered entries, so it would discard that section
/// and re-emit the suppressed rows as live. A genuinely rotted seqno-bounds
/// section is indistinguishable from the relabel, so both fail closed; the
/// operator recovers the quarantined original by hand.
#[test]
fn salvage_refuses_a_corrupt_seqno_bounds_that_may_hide_a_deletion() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_seqno_in_index(true);
    for i in 0u64..50 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Rot one payload byte of the seqno_bounds block WITHOUT re-stamping its
    // checksum: the recover-time load fails and degrades the map to empty.
    {
        let pos = {
            let mut f = std::fs::File::open(&source)?;
            let reader = match crate::sfa::Reader::from_reader(&mut f) {
                Ok(r) => r,
                Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
            };
            let Some(entry) = reader.toc().iter().find(|e| e.name() == b"seqno_bounds") else {
                panic!("the source carries a seqno_bounds section");
            };
            let Ok(pos) = usize::try_from(entry.pos()) else {
                panic!("pos fits usize");
            };
            pos
        };
        let mut bytes = std::fs::read(&source)?;
        // Past the block header, inside the payload.
        let at = pos + 40;
        let Some(slot) = bytes.get_mut(at) else {
            panic!("payload byte within the file");
        };
        *slot ^= 0xFF;
        std::fs::write(&source, bytes)?;
    }

    // Salvage fails closed: the seqno-bounds section did not decode and the
    // table exposes no deletion, so it may be a relabeled deletion salvage
    // would discard — quarantine for manual recovery instead of resurrecting
    // rows.
    let Err(err) = salvage_sst(&source, dest, &fs) else {
        panic!("a corrupt seqno-bounds section with no visible deletion must fail salvage");
    };
    assert!(
        matches!(err, crate::Error::FeatureUnsupported(_)),
        "the refusal names the unsupported salvage, got {err:?}",
    );
    Ok(())
}

/// When BOTH meta mirrors decode under the expected id but DIVERGE (a forged,
/// internally-consistent tail: `compression#data` re-stamped None -> Lz4,
/// `meta_mid` untouched), the tail-first open decodes every data block under
/// the wrong codec, drops them all, and repair would quarantine a table whose
/// intact MID mirror recovers everything. Salvage must arbitrate the mirrors
/// and keep the attempt that recovers more.
#[cfg(feature = "lz4")]
#[test]
fn salvage_arbitrates_divergent_meta_mirrors() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Forge the TAIL meta's data-block compression from the written None
    // (tag 0) to Lz4 (tag 1) — fresh block checksum, `meta_mid` untouched.
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(report.blocks_total > 0, "the walk saw the data blocks");
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block is recoverable through the intact MID mirror: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 100,
        "all rows recovered: {report:?}"
    );
    assert!(
        report.dropped.is_empty(),
        "nothing should drop under the intact mirror: {report:?}",
    );
    assert!(report.salvaged_path.is_some(), "a copy was written");

    // The MID attempt's sibling temp path must not survive the arbitration:
    // the winner is renamed over dest, the loser is discarded.
    for entry in std::fs::read_dir(dir.path())? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(
            !name.contains(".healtmp-"),
            "the arbitration temp file must be renamed or removed, found {name:?}",
        );
    }
    Ok(())
}

/// A MID-arbitration publish whose post-rename directory sync FAILS must not
/// leave the owned destination behind: the rename already populated `dest`,
/// so returning the durability error without cleanup would leave a partial
/// SST a standalone retry's `create_new` open trips over (and the repair
/// caller's rebuilt manifest omits, leaving an orphan). Every other salvage
/// failure path removes its owned destination; this one must too.
///
/// `lz4`-gated: the divergent-mirror arbitration is driven by a
/// `compression#data` forge to Lz4, so the tail attempt only mis-decodes
/// (and the MID attempt wins, taking the publish path) when lz4 is built.
#[cfg(feature = "lz4")]
#[test]
fn salvage_removes_the_mid_copy_when_the_publish_dir_sync_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Divergent mirrors so the MID attempt wins and publishes via rename.
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    // Fail the publish's post-rename directory sync. The tail attempt
    // mis-decodes under the forged codec and drops its only block, so it
    // never finishes (no sync); the MID writer's finish syncs the parent
    // once, and the publish after the MID rename syncs it again. Skip the
    // MID writer's sync and fire on the publish.
    injector.arm(FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).skip(1));

    // The error must be the INJECTED directory-sync fault: only the publish
    // path both consumes the fault and propagates it. If a future change
    // adds an earlier directory sync, `skip(1)` fires on the MID writer's
    // finish instead — `mid` fails, the arbitration returns the tail
    // attempt's (different) mis-decode error, and this assertion flags that
    // the test no longer covers the publish-sync cleanup path.
    let Err(err) = salvage_sst(&source, dest.clone(), &fs) else {
        panic!("a failed publish directory sync must fail the salvage");
    };
    assert!(
        err.to_string().contains("injected fault on SyncDirectory"),
        "the salvage error must be the injected publish-sync fault, got {err:?}",
    );
    assert!(
        !std::path::Path::new(&dest).exists(),
        "the owned MID copy must be removed when the publish sync fails",
    );
    for entry in std::fs::read_dir(dir.path())? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(
            !name.contains(".healtmp-"),
            "the MID temp copy must not survive either, found {name:?}",
        );
    }
    Ok(())
}

/// The MID publish must fall back to a best-effort rename when the backend
/// leaves `Fs::hard_link` unsupported. Such a backend can still create and
/// rename ordinary files, so a divergent-meta salvage must still publish the
/// recovered copy rather than drop a recoverable table.
#[cfg(feature = "lz4")]
#[test]
fn salvage_publishes_the_mid_copy_when_hard_link_is_unsupported() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Divergent mirrors so the MID attempt wins and reaches the publish path.
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    // The backend cannot hard-link: the publish must fall back to a rename.
    injector.arm(FaultRule::new(
        FaultOp::HardLink,
        Fault::Error(ErrorKind::Unsupported),
    ));

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.salvaged_path.as_deref(),
        Some(dest.as_path()),
        "the MID copy must be published via the rename fallback: {report:?}",
    );
    assert!(
        report.entries_salvaged > 0,
        "the recovered table must carry entries: {report:?}",
    );
    assert!(
        std::path::Path::new(&dest).exists(),
        "the recovered table must land at the destination",
    );
    Ok(())
}

/// The MID publish must claim `dest` with an atomic no-replace operation, never
/// a blind rename that clobbers an unowned file. When `dest` is already
/// occupied, the tail attempt's `create_new` open fails against it (so the MID
/// attempt wins and reaches the publish path), and the publish must surface an
/// error while leaving the occupant's bytes untouched and leaking no temp copy.
///
/// The narrower check-then-rename TOCTOU — `dest` appearing only AFTER an
/// existence probe reports it free — is not deterministically reachable through
/// the current fault surface (it cannot force `Fs::exists` to report a present
/// file absent); the no-replace `hard_link` publish closes that window
/// structurally rather than by a racy probe.
///
/// `lz4`-gated: the MID attempt only runs when the meta mirrors DIVERGE, driven
/// by a `compression#data` forge to Lz4 (as in the sibling MID-arbitration
/// tests). Without divergence `salvage_with_context` returns the tail attempt
/// and never reaches the publish path this test targets.
#[cfg(feature = "lz4")]
#[test]
fn salvage_does_not_clobber_an_occupied_destination_when_the_mid_attempt_wins() -> crate::Result<()>
{
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Diverge the meta mirrors so the MID attempt actually runs (the tail
    // attempt mis-decodes under the forged Lz4 codec and drops its block).
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    // A racing worker already owns `dest`. The tail attempt's `create_new` open
    // fails against it, so the tail errors and the MID attempt (writing to a
    // temp) wins arbitration and must PUBLISH into the occupied path.
    std::fs::write(&dest, b"racing worker's file")?;

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "publishing over an occupied destination must fail, got {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        b"racing worker's file",
        "the occupant's bytes must survive the failed publish",
    );
    for entry in std::fs::read_dir(dir.path())? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(
            !name.contains(".healtmp-"),
            "the MID temp copy must not leak, found {name:?}",
        );
    }
    Ok(())
}

/// A STALE arbitration temp file left by a crashed predecessor must not
/// block the MID attempt: the temp-name sequence is process-local, so a
/// fresh process would otherwise pick the same `.healtmp-0` name, fail its
/// `create_new` open, and return the tail attempt's inferior (or failing)
/// result without ever trying the recoverable MID mirror — and tree
/// recovery only sweeps this namespace inside table folders, so a
/// standalone salvage destination stays blocked indefinitely. The
/// arbitration must probe forward to a free name; the foreign artifact is
/// never reclaimed (it may belong to a concurrently running salvage).
///
/// `lz4`-gated: the divergent-mirror arbitration is driven by a
/// `compression#data` forge to Lz4 (see the sibling MID-cleanup test).
#[cfg(feature = "lz4")]
#[test]
fn salvage_arbitration_skips_a_stale_crash_artifact() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Divergent mirrors so the MID attempt wins and publishes via rename.
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    // A crashed predecessor's artifact occupies the FIRST temp name this
    // process would pick (the sequence is a process-local counter and
    // nextest runs each test in its own process, so it starts at zero).
    let stale = dest.with_extension("healtmp-0");
    std::fs::write(&stale, b"crashed predecessor artifact")?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert_eq!(
        report.entries_salvaged, 100,
        "the MID attempt must pick a fresh temp name past the stale artifact: {report:?}",
    );
    assert!(
        std::path::Path::new(&stale).exists(),
        "a foreign artifact is never reclaimed",
    );
    assert_eq!(
        std::fs::read(&stale)?,
        b"crashed predecessor artifact".to_vec(),
        "the stale artifact's bytes stay untouched",
    );
    Ok(())
}

/// A PRE-EXISTING destination must survive a divergent-mirror salvage: the
/// tail attempt correctly fails its `create_new` open, but the MID attempt
/// writes to a sibling temp path and wins the arbitration — publishing it
/// by renaming over `dest` would silently destroy an unrelated file that an
/// API call refusing "destination occupied" is supposed to leave untouched.
///
/// `lz4`-gated: the divergent-mirror arbitration is driven by a
/// `compression#data` forge to Lz4 (see the sibling MID-cleanup test).
#[cfg(feature = "lz4")]
#[test]
fn salvage_refuses_to_overwrite_a_preexisting_destination() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Divergent mirrors, so the arbitration runs a MID attempt after the
    // tail attempt fails on the occupied destination.
    crate::test_forge::forge_tail_meta_value(&source, b"compression#data", &[1])?;

    // An unrelated file already occupies the destination.
    let sentinel = b"unrelated pre-existing file".to_vec();
    std::fs::write(&dest, &sentinel)?;

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "an occupied destination must fail the salvage: {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        sentinel,
        "the pre-existing destination must survive byte-for-byte",
    );
    // The losing / refused MID copy must not leak its temp file either.
    for entry in std::fs::read_dir(dir.path())? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(
            !name.contains(".healtmp-"),
            "the refused MID attempt must clean up its temp copy, found {name:?}",
        );
    }
    Ok(())
}

/// DIVERGENT meta mirrors disable the verbatim copy-through: a divergence
/// confined to a DECODE-TRANSPARENT layout field (`restart_interval#data` —
/// full block decoding is trailer-driven, so every block still reads clean
/// and nothing drops) lets the tail-first attempt finish perfectly, yet a
/// byte-copied block keeps the ORIGINAL encoding while the copy's meta is
/// stamped with the forged interval. The partial-decode read path
/// reconstructs prefix-compressed entries FROM that metadata interval, so
/// the inconsistent pair silently truncates synthesized blocks. Since
/// neither mirror is provably genuine, every block must be RE-ENCODED under
/// the chosen meta — self-consistent whichever mirror wins.
#[test]
fn salvage_reencodes_all_blocks_when_meta_mirrors_diverge() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..100 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Forge the TAIL meta's data restart interval from the written 16 to 4 —
    // fresh block checksum, `meta_mid` untouched. Blocks keep decoding
    // (trailer-driven), so the tail attempt sees zero drops.
    crate::test_forge::forge_tail_meta_value(&source, b"restart_interval#data", &[4])?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert_eq!(
        report.entries_salvaged, 100,
        "all rows recovered: {report:?}"
    );
    assert!(report.salvaged_path.is_some(), "a copy was written");
    assert_eq!(
        report.blocks_copied_verbatim, 0,
        "divergent mirrors must force the re-encode path: a byte-copied \
         block would keep the original encoding under the chosen meta's \
         forged layout: {report:?}",
    );
    Ok(())
}

/// A checksum-clean row block that ITERATES to fewer entries than its
/// trailer declares must be dropped, not marked recovered: the entry decoder
/// turns a mid-stream parse failure into an ordinary end of iteration, so a
/// block with a valid prefix and a malformed tail would otherwise be
/// counted salvaged while silently losing the remaining keys (or byte-copied
/// verbatim, still malformed).
#[test]
fn salvage_drops_a_row_block_that_decodes_fewer_entries_than_declared() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..3 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Inflate the trailer's item count by one behind a re-stamped block
    // checksum: iteration now yields fewer entries than the block declares.
    crate::test_forge::forge_inflated_item_count(&source)?;

    let report = salvage_sst(&source, dest, &fs)?;
    assert_eq!(
        report.entries_salvaged, 0,
        "an under-decoding block must not contribute recovered entries: {report:?}",
    );
    assert!(
        report
            .dropped
            .iter()
            .any(|d| matches!(d.reason, DropReason::DecodeError(_))),
        "the count mismatch is a dropped DecodeError, not a clean recovery: {report:?}",
    );
    Ok(())
}

/// Regression: a data block can hold several MVCC versions of one user key
/// (same key, descending seqno). The verbatim copy-through path must accept
/// equal user keys — only columnar *ingest* requires strictly-unique keys — so
/// salvaging such a block recovers every version instead of erroring.
#[test]
fn salvage_recovers_a_block_with_multiple_versions_of_one_key() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // One block holding the same user key at several seqnos, surrounded by unique
    // keys. Valid SST order: user key ascending, seqno descending within a key.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(InternalValue::from_components(
        b"a".to_vec(),
        b"a".to_vec(),
        1,
        ValueType::Value,
    ))?;
    for seqno in [3u64, 2, 1] {
        writer.write(InternalValue::from_components(
            b"dup".to_vec(),
            format!("v{seqno}").into_bytes(),
            seqno,
            ValueType::Value,
        ))?;
    }
    writer.write(InternalValue::from_components(
        b"z".to_vec(),
        b"z".to_vec(),
        1,
        ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy SST with MVCC duplicates salvages cleanly: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged, 5,
        "every version is recovered, including all 3 of `dup`: {report:?}",
    );

    let recovered = open(dest, &fs)?;
    assert_eq!(
        recovered.metadata.item_count, 5,
        "all 5 entries (3 versions of `dup`) are recovered",
    );
    Ok(())
}

/// A block where a weak tombstone is immediately followed by a value for the
/// same key (a reclaimable pair) salvages verbatim and recovers both entries —
/// exercising the reclaimable-weak-tombstone accounting on the copy-through path.
#[test]
fn salvage_recovers_a_reclaimable_weak_tombstone_pair() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // SST order is user key ascending, seqno descending: the weak tombstone
    // (higher seqno) precedes the value it reclaims (lower seqno) for `dup`.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    writer.write(InternalValue::from_components(
        b"a".to_vec(),
        b"a".to_vec(),
        1,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"dup".to_vec(),
        b"".to_vec(),
        3,
        ValueType::WeakTombstone,
    ))?;
    writer.write(InternalValue::from_components(
        b"dup".to_vec(),
        b"v1".to_vec(),
        1,
        ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest, &fs)?;
    assert!(
        report.is_complete(),
        "healthy SST salvages cleanly: {report:?}"
    );
    assert_eq!(
        report.entries_salvaged, 3,
        "the weak tombstone and both values are recovered: {report:?}",
    );
    assert!(
        report.blocks_copied_verbatim >= 1,
        "the clean block is copied verbatim: {report:?}",
    );
    Ok(())
}

/// `verify_locator` must validate the SLOT hint, not only the block id: a
/// `Restart`-precision locator can keep the correct block id yet redirect
/// a key's slot to a later restart interval holding an older version, so
/// `point_read_at_slot` returns the stale value without falling back to
/// the sorted index. The `Writer` is driven directly (two versions of one
/// key in one block, `restart_interval = 1`, locator enabled) and the
/// locator section is rebuilt with that key's slot pushed from the newest
/// head (0) to the older head (1).
#[test]
fn verify_locator_rejects_a_slot_hint_pointing_at_an_older_version() -> crate::Result<()> {
    use crate::config::{LocatorPolicyEntry, LocatorPrecision};
    use crate::runtime_config::{ChecksumAlgorithm, KvChecksumPolicy};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_restart_interval(1)
        .use_kv_checksums(KvChecksumPolicy::AllLevels, ChecksumAlgorithm::Xxh3_64)
        .use_locator(LocatorPolicyEntry::Enabled {
            precision: LocatorPrecision::Restart,
            block_id_bits: None,
            slot_bits: None,
        });
    // Restart heads (restart_interval = 1) in one block: key-a@2 (head 0),
    // key-a@1 (head 1), key-b (head 2), key-c (head 3).
    writer.write(InternalValue::from_components(
        b"key-a".to_vec(),
        b"new".to_vec(),
        2,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"key-a".to_vec(),
        b"old".to_vec(),
        1,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"key-b".to_vec(),
        b"vb".to_vec(),
        1,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"key-c".to_vec(),
        b"vc".to_vec(),
        1,
        ValueType::Value,
    ))?;
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Sanity: the honest locator passes the gate.
    open(source.clone(), &fs)?.verify_locator()?;

    // Rebuild the locator with key-a's slot pushed to head 1 (the older
    // version); key-b / key-c keep their honest heads (2 / 3). block_id 0
    // for the single block.
    let h = |k: &[u8]| crate::hash::hash64(k);
    crate::test_forge::forge_locator_slots(
        &source,
        0,
        &[
            (h(b"key-a"), 0, 1),
            (h(b"key-b"), 0, 2),
            (h(b"key-c"), 0, 3),
        ],
    )?;

    let table = open(source, &fs)?;
    let Err(err) = table.verify_locator() else {
        panic!("a slot hint pointing at an older version must fail the gate");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader(
                "locator slot hint does not resolve a key's newest version"
            )
        ),
        "the redirect must be rejected by the slot-hint check, got {err:?}",
    );
    Ok(())
}

/// `verify_filter` must probe the extractor's PREFIX hashes, not only
/// complete-key hashes: a full filter built WITHOUT the configured
/// extractor (a salvage that dropped it, or a forge) still answers every
/// complete-key probe, yet `maybe_contains_prefix` treats the table as
/// definitely absent and prefix scans silently omit its rows. No forge is
/// needed — a `Writer` run without the extractor produces exactly the
/// filter that lacks the prefix hashes.
#[test]
fn verify_filter_rejects_missing_prefix_hashes() -> crate::Result<()> {
    /// First four key bytes (keys are `keyNNNNN`, so the prefix is `key0`).
    struct FixedLengthPrefix;
    impl crate::prefix::PrefixExtractor for FixedLengthPrefix {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            key.get(..4).map_or_else(
                || Box::new(std::iter::empty()) as Box<dyn Iterator<Item = &'a [u8]>>,
                |p| Box::new(std::iter::once(p)),
            )
        }

        fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
            prefix.len() == 4
        }
    }

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let extractor: Arc<dyn crate::prefix::PrefixExtractor> = Arc::new(FixedLengthPrefix);

    // Written WITHOUT the extractor: the full filter holds only complete-key
    // hashes, exactly the state a salvage-without-extractor leaves behind.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u32..100 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let table = open(source, &fs)?;
    // Without an extractor the gate only probes complete keys and passes.
    table.verify_filter(None)?;
    // WITH the extractor it probes each key's prefix hash, which the filter
    // never indexed — a false negative on an existing key's prefix.
    let Err(err) = table.verify_filter(Some(&extractor)) else {
        panic!("a filter missing the prefix hashes must fail the gate");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader(
                "filter reports an existing key's prefix as definitely absent"
            )
        ),
        "the missing prefix hash must be the rejection reason, got {err:?}",
    );
    Ok(())
}

/// The point-read reachability gate must require the probe to return the
/// NEWEST version of a key, not merely SOME version. A key spanning two
/// restart intervals has a CONFLICT-marked hash bucket; redirecting that
/// bucket to the later interval (an OLDER version) still makes
/// `point_read(MAX_SEQNO)` return `Some`, so an `is_none` check would pass
/// — yet reads after reopen return the stale value. The `Writer` is driven
/// directly (two versions of one key in one block, `restart_interval = 1`,
/// hashed, footered) because a memtable flush deduplicates shadowed
/// versions and cannot produce this layout.
#[test]
fn verify_point_read_reachability_rejects_a_bucket_redirected_to_an_older_version()
-> crate::Result<()> {
    use crate::runtime_config::{ChecksumAlgorithm, KvChecksumPolicy};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_hash_ratio(2.0)
        .use_data_block_restart_interval(1)
        .use_kv_checksums(KvChecksumPolicy::AllLevels, ChecksumAlgorithm::Xxh3_64);
    // "key-a" twice: newest (seqno 2) then older (seqno 1), so both are
    // restart heads in the first block and the key's bucket conflicts.
    writer.write(InternalValue::from_components(
        b"key-a".to_vec(),
        b"new".to_vec(),
        2,
        ValueType::Value,
    ))?;
    writer.write(InternalValue::from_components(
        b"key-a".to_vec(),
        b"old".to_vec(),
        1,
        ValueType::Value,
    ))?;
    for k in [b"key-b", b"key-c"] {
        writer.write(InternalValue::from_components(
            k.to_vec(),
            b"v".to_vec(),
            1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Sanity: the intact table passes the reachability gate.
    open(source.clone(), &fs)?.verify_point_read_reachability()?;

    // Redirect the conflicting bucket to binary-index pos 1 — the second
    // restart head, holding the OLDER (seqno 1) version.
    crate::test_forge::forge_hash_index_bucket(&source, b"key-a", 1, None)?;

    let table = open(source, &fs)?;
    let Err(err) = table.verify_point_read_reachability() else {
        panic!("a bucket redirected to an older version must fail the gate");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader(
                "a decoded key's point_read does not return its newest version \
                 (an in-block index disagrees with the entries)"
            )
        ),
        "the redirect must be rejected by the newest-version check, got {err:?}",
    );
    Ok(())
}

/// A `filter_tli` block that does not decode as a filter index must fail
/// SALVAGE closed on a table that exposes NO deletion metadata: a re-stamped
/// TOC can rename a `range_tombstones` / `delete_bitmap` section to
/// `filter_tli` and re-role its block, leaving a uniquely named, tiled
/// catalogue whose parsed table reports no deletion. Salvage re-derives the
/// filter from the recovered keys, so it would discard that section and
/// re-emit the suppressed rows as live. A genuinely rotted filter index is
/// indistinguishable from the relabel, so both fail closed; the operator
/// recovers the quarantined original by hand.
#[test]
fn salvage_refuses_a_corrupt_filter_index_that_may_hide_a_deletion() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_partitioned_filter()
        // A tiny partition budget so several filter partitions spill and
        // the writer emits the `filter_tli` top-level index over them.
        .use_meta_partition_size(3);
    for i in 0u32..64 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Rot one payload byte of the filter_tli block WITHOUT re-stamping its
    // checksum, so loading it fails like any bit-rotted block.
    {
        let pos = {
            let mut f = std::fs::File::open(&source)?;
            let reader = match crate::sfa::Reader::from_reader(&mut f) {
                Ok(r) => r,
                Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
            };
            let Some(entry) = reader.toc().iter().find(|e| e.name() == b"filter_tli") else {
                panic!("the source carries a filter_tli section");
            };
            let Ok(pos) = usize::try_from(entry.pos()) else {
                panic!("pos fits usize");
            };
            pos
        };
        let mut bytes = std::fs::read(&source)?;
        // First payload byte, derived from the block header length (filter_tli
        // is an Index block) so a header-layout change cannot slide the flip
        // into the header and leave the payload untouched.
        let at =
            pos + crate::table::block::Header::header_len(crate::table::block::BlockType::Index);
        let Some(slot) = bytes.get_mut(at) else {
            panic!("payload byte within the file");
        };
        *slot ^= 0xFF;
        std::fs::write(&source, bytes)?;
    }

    // Provably corrupt: a LIVE open fails closed on the rotted filter index.
    assert!(
        open(source.clone(), &fs).is_err(),
        "the rotted filter index must fail a live open",
    );

    // Salvage fails closed: the filter index did not decode and the table
    // exposes no deletion, so it may be a relabeled deletion salvage would
    // discard — quarantine for manual recovery instead of resurrecting rows.
    let Err(err) = salvage_sst(&source, dest, &fs) else {
        panic!("a corrupt filter index with no visible deletion must fail salvage");
    };
    assert!(
        matches!(err, crate::Error::FeatureUnsupported(_)),
        "the refusal names the unsupported salvage, got {err:?}",
    );
    Ok(())
}

/// The rebuilt filter must carry the source's PREFIX hashes: the extractor
/// is configuration (not persisted in the SST), so the salvage writer can
/// only index prefixes when the caller threads the tree's extractor
/// through [`SalvageOptions::prefix_extractor`]. Without it the salvaged
/// copy's filter holds complete-key hashes only, `maybe_contains_prefix`
/// reports every indexed prefix as definitely absent, and the copy's rows
/// silently vanish from prefix scans.
#[test]
fn salvage_preserves_prefix_filter_hashes() -> crate::Result<()> {
    /// First four key bytes, mirroring the tree-level extractor fixtures.
    struct FixedLengthPrefix;
    impl crate::prefix::PrefixExtractor for FixedLengthPrefix {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            key.get(..4).map_or_else(
                || Box::new(std::iter::empty()) as Box<dyn Iterator<Item = &'a [u8]>>,
                |p| Box::new(std::iter::once(p)),
            )
        }

        fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
            prefix.len() == 4
        }
    }

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let extractor: Arc<dyn crate::prefix::PrefixExtractor> = Arc::new(FixedLengthPrefix);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_prefix_extractor(Some(Arc::clone(&extractor)));
    for i in 0u32..100 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Sanity: the SOURCE's filter answers the prefix probes (keys are
    // `key000NN`, so the indexed 4-byte prefixes are `key0`).
    let prefix_hash = crate::hash::hash64(b"key0");
    {
        let table = open(source.clone(), &fs)?;
        assert!(
            table.maybe_contains_prefix(prefix_hash)?,
            "the source filter indexes the prefix",
        );
    }

    let options = SalvageOptions {
        prefix_extractor: Some(Arc::clone(&extractor)),
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.entries_salvaged, 100,
        "all rows recovered: {report:?}"
    );

    // A missing prefix hash is a DEFINITE-absent answer from the rebuilt
    // filter — the salvaged copy would silently vanish from prefix scans.
    let table = open(dest, &fs)?;
    assert!(
        table.maybe_contains_prefix(prefix_hash)?,
        "the salvaged copy's filter must keep the source's prefix hashes",
    );
    Ok(())
}

fn iv(i: u32) -> InternalValue {
    InternalValue::from_components(
        format!("key{i:05}").into_bytes(),
        format!("val{i:05}").into_bytes(),
        1,
        ValueType::Value,
    )
}

/// Opens an SST as a `Table`, stamping the open with the file's current digest
/// (the source may be corrupt; per-block checksums catch the actual damage).
fn open(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<Table> {
    open_with_id(path, fs, 0)
}

/// As [`open`] but under an explicit expected table id (the recover
/// cross-checks it against the SST's stored `table_id`).
fn open_with_id(
    path: std::path::PathBuf,
    fs: &Arc<dyn Fs>,
    table_id: crate::TableId,
) -> crate::Result<Table> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, &path)?);
    Table::recover(
        path,
        checksum,
        0,
        0,
        table_id,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        None,
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )
}

/// A reopen of a salvaged SST: recover it and return its live item count.
fn reopen_item_count(path: std::path::PathBuf, fs: &Arc<dyn Fs>) -> crate::Result<u64> {
    Ok(open(path, fs)?.metadata.item_count)
}

/// Point-reads `key` from the SST at `path` at the latest snapshot — the
/// LOGICAL visibility check behind the physical row counts (a delete either
/// masks the key or, under the resurrection opt-in, leaves it readable).
fn reopen_get(
    path: std::path::PathBuf,
    fs: &Arc<dyn Fs>,
    key: &[u8],
) -> crate::Result<Option<crate::InternalValue>> {
    open(path, fs)?.get(key, crate::MAX_SEQNO, crate::hash::hash64(key))
}

/// An SST from a KV-separated tree carries a `linked_blob_files` section
/// naming every blob file its `ValueHandle`s point into; blob GC / relocation
/// consults it (via `list_blob_file_references`) to decide whether a blob is
/// still referenced. The salvaged copy must carry the SOURCE's links —
/// omitting the section would let GC rewrite or delete a blob that only this
/// table still references, silently breaking its indirections.
#[test]
fn salvage_preserves_the_source_linked_blob_files() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A KV-separated tree: large values go to a blob file, the SST holds
    // indirections plus a linked_blob_files section.
    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "healthy SST: {report:?}");

    // The source's blob links survive into the recovered copy.
    let Some(source_links) = open(source, &fs)?.list_blob_file_references()? else {
        panic!("the source carries a linked_blob_files section");
    };
    assert!(!source_links.is_empty(), "the source references blob files");
    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy carries a linked_blob_files section");
    };
    assert_eq!(
        recovered_links, source_links,
        "the salvaged copy references the same blob files as the source",
    );
    Ok(())
}

/// `verify_blob_links` must reject a table that still carries indirection
/// entries but advertises NO `linked_blob_files` section (dropped or renamed
/// away): returning `Ok` there lets a healed-digest refresh or salvage accept a
/// table with `ValueHandle`s but no blob references, after which blob GC — which
/// consults `list_blob_file_references()` for liveness — can rewrite or drop a
/// blob file this table still points into.
#[test]
fn verify_blob_links_rejects_a_missing_section_with_live_indirections() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // Drop the linked_blob_files section from the TOC: the table keeps its
    // indirection entries but now advertises no blob references.
    crate::test_forge::forge_section_omitted(&source, b"linked_blob_files")?;

    let table = open(source, &fs)?;
    assert!(
        table.list_blob_file_references()?.is_none(),
        "the forge must leave the table with no blob-link section",
    );
    let Err(err) = table.verify_blob_links() else {
        panic!("a table with live indirections but no blob-link section must be rejected");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader(
                "table carries indirection entries but no linked_blob_files section"
            )
        ),
        "the rejection names the missing-blob-link-section reason, got {err:?}",
    );
    Ok(())
}

/// `verify_seqno_bounds` must reject a PRESENT `seqno_bounds` section that
/// decodes to an EMPTY map on a table that still holds data blocks: every real
/// writer emits one entry per block, so an empty map (a `delete_bitmap` renamed
/// to `seqno_bounds` and re-stamped empty) means the table's real bounds/deletes
/// metadata was laundered away. Accepting it lets a healed-digest refresh grade
/// the table clean and reopen it without its deletion metadata, resurrecting
/// positionally deleted rows.
#[test]
fn verify_seqno_bounds_rejects_a_present_empty_map_on_a_nonempty_table() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A PLAIN table with the per-block seqno_bounds section enabled.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_seqno_in_index(true)
        .use_data_block_size(128);
    for i in 0u64..64 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Re-stamp the seqno_bounds section as a valid but EMPTY map.
    crate::test_forge::forge_seqno_bounds_empty(&source, 0)?;

    let table = open(source, &fs)?;
    let Err(err) = table.verify_seqno_bounds() else {
        panic!("an empty seqno_bounds on a table with data blocks must be rejected");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader("seqno_bounds is missing a data block's entry")
        ),
        "the rejection names the missing-block-entry reason, got {err:?}",
    );
    Ok(())
}

/// `verify_metadata_bounds` must cross-check `seqno#min` against the decoded
/// entries even on a RANGE-TOMBSTONE-bearing table. The synthetic sentinel is
/// only written when the table has NO real KV items, so a table with both point
/// entries AND range tombstones has no sentinel and its decoded seqnos are all
/// real. Skipping the check for every RT-bearing table let both meta mirrors be
/// re-stamped with `seqno#min` raised above a real KV seqno, hiding live
/// versions from snapshots at/below the forged minimum after reopen.
#[test]
fn verify_metadata_bounds_rejects_a_raised_seqno_min_on_a_range_tombstone_table()
-> crate::Result<()> {
    use crate::UserKey;
    use crate::range_tombstone::RangeTombstone;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Point entries at seqnos 1..=8 PLUS a range tombstone: the table carries a
    // range_tombstones section but, having real KV items, writes no sentinel.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0u64..8 {
        writer.write(InternalValue::from_components(
            format!("key-{i:03}").into_bytes(),
            format!("val-{i:03}").into_bytes(),
            i + 1,
            ValueType::Value,
        ))?;
    }
    writer.write_range_tombstone(RangeTombstone::new(
        UserKey::from(b"key-002".as_slice()),
        UserKey::from(b"key-005".as_slice()),
        9,
    ));
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Raise seqno#min above the real minimum KV seqno (1) in BOTH meta mirrors.
    crate::test_forge::forge_meta_value_both_mirrors(&source, b"seqno#min", &5u64.to_le_bytes())?;

    let table = open(source, &fs)?;
    let Err(err) = table.verify_metadata_bounds() else {
        panic!("a seqno#min above the real minimum must be rejected even with range tombstones");
    };
    assert!(
        matches!(
            err,
            crate::Error::InvalidHeader("meta seqno#min is above the decoded minimum seqno")
        ),
        "the rejection names the seqno#min branch specifically, got {err:?}",
    );
    Ok(())
}

/// A `linked_blob_files` section that PARSES but under-reports its contents
/// (count word forged to 0, record bytes left in place) must not be trusted
/// as the sole source of the recovered copy's links: the recovered entries
/// still hold `ValueHandle` indirections into the blob file, and blob GC /
/// relocation consults the links to decide liveness — an under-reported list
/// would let GC delete or rewrite a blob the copy still references. Salvage
/// derives the links from the recovered indirections and unions them with
/// the source list.
#[test]
fn salvage_rebuilds_blob_links_from_recovered_indirections() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // The TRUE links, before the forgery.
    let Some(true_links) = open(source.clone(), &fs)?.list_blob_file_references()? else {
        panic!("the source carries a linked_blob_files section");
    };
    assert!(!true_links.is_empty(), "the source references blob files");

    // Forge the count word to 0: the section still parses (the bound check
    // passes trivially) but reports NO links.
    let pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the source must carry a linked_blob_files section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(count) = bytes.get_mut(pos..pos + 4) else {
        panic!("linked_blob_files count header within the file");
    };
    count.copy_from_slice(&0u32.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    // Sanity: the forgery took — the source now under-reports.
    let Some(forged) = open(source.clone(), &fs)?.list_blob_file_references()? else {
        panic!("the forged section still parses");
    };
    assert!(forged.is_empty(), "the forged count hides every link");

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "data blocks are healthy: {report:?}");

    // The copy's links are derived from its recovered indirections, not
    // parroted from the forged source list.
    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy must carry links derived from its indirections");
    };
    for link in &true_links {
        assert!(
            recovered_links
                .iter()
                .any(|l| l.blob_file_id == link.blob_file_id),
            "blob file {} is referenced by recovered indirections but missing \
             from the copy's links: {recovered_links:?}",
            link.blob_file_id,
        );
    }
    Ok(())
}

/// A parseable `linked_blob_files` section that OVER-reports — carries a blob
/// id no recovered `ValueHandle` actually references — must not have that
/// source-only id copied into the salvaged table: the id may not exist under
/// `blobs/` at all (a forged record), and a manifest whose table links a blob
/// absent from the blob-file list is a corrupt reference downstream consumers
/// must never see. The copy's links are the recovered indirections, exactly.
#[test]
fn salvage_drops_source_only_blob_links() -> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // Forge the FIRST link record's blob id (the leading u64 of the 32-byte
    // record, right after the LE u32 count) to an id that exists nowhere.
    const FORGED_ID: u64 = 9_999_999;
    let pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the source must carry a linked_blob_files section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(id_slot) = bytes.get_mut(pos + 4..pos + 12) else {
        panic!("first link record within the file");
    };
    id_slot.copy_from_slice(&FORGED_ID.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    // Sanity: the forgery took — the source reports the forged id.
    let Some(forged) = open(source.clone(), &fs)?.list_blob_file_references()? else {
        panic!("the forged section still parses");
    };
    assert!(
        forged.iter().any(|l| l.blob_file_id == FORGED_ID),
        "the source's section carries the forged id",
    );

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "data blocks are healthy: {report:?}");

    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy carries links derived from its indirections");
    };
    assert!(
        !recovered_links.is_empty(),
        "the recovered indirections reference the real blob file",
    );
    assert!(
        !recovered_links.iter().any(|l| l.blob_file_id == FORGED_ID),
        "a source-only id no recovered indirection references must not be \
         copied into the salvaged table: {recovered_links:?}",
    );
    Ok(())
}

/// A KV-separated source whose `linked_blob_files` section is UNREADABLE (a
/// count header claiming more records than the section holds) must not abort
/// the salvage: the section is not authoritative — the walk derives the
/// copy's links from the recovered `ValueHandle` indirections — so the
/// readable data blocks are still recovered and the copy carries the derived
/// links. Failing here would leave the whole table unrecovered over a
/// non-authoritative side section.
#[test]
fn salvage_recovers_with_derived_links_when_the_blob_link_section_is_unreadable()
-> crate::Result<()> {
    use crate::AbstractTree;

    let dir = tempdir()?;
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let crate::AnyTree::Blob(tree) = crate::Config::new(
        dir.path(),
        crate::SequenceNumberCounter::default(),
        crate::SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(crate::KvSeparationOptions::default()))
    .open()?
    else {
        unreachable!("kv separation configured");
    };
    let big = |i: u32| format!("{i:08}").repeat(512);
    for i in 0u32..10 {
        tree.insert(format!("key{i:05}"), big(i), u64::from(i) + 1);
    }
    tree.flush_active_memtable(10)?;
    let source = {
        let binding = tree.index.version_history.read().latest_version();
        let Some(table) = binding.version.iter_tables().next() else {
            panic!("flush produced one table");
        };
        (*table.path).clone()
    };
    drop(tree);

    // Overwrite the linked_blob_files count header (leading LE u32) with a
    // value far larger than the section can hold, so parsing the records
    // hits EOF and list_blob_file_references errors.
    let pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader
            .toc()
            .iter()
            .find(|e| e.name() == b"linked_blob_files")
        else {
            panic!("the source must carry a linked_blob_files section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(count) = bytes.get_mut(pos..pos + 4) else {
        panic!("linked_blob_files count header within the file");
    };
    count.copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let dest = dir.path().join("salvaged");
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(report.is_complete(), "data blocks are healthy: {report:?}");

    // The copy's links are derived from its recovered indirections; the
    // unreadable source section contributes nothing.
    let Some(recovered_links) = open(dest, &fs)?.list_blob_file_references()? else {
        panic!("the salvaged copy must carry links derived from its indirections");
    };
    assert!(
        !recovered_links.is_empty(),
        "the recovered indirections reference at least one blob file",
    );
    Ok(())
}

/// Standalone salvage preserves the SOURCE's persisted table id: an
/// unencrypted SST written under a non-zero id salvages WITHOUT the caller
/// supplying that id (the salvage-mode open reads it from the metadata
/// instead of failing the id cross-check against the options default of 0),
/// and the recovered copy is stamped with the source's id — so it keeps its
/// identity when an operator swaps it in for the original.
#[test]
fn salvage_preserves_a_nonzero_source_table_id() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    const TID: crate::TableId = 7;
    let mut writer = Writer::new(source.clone(), TID, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Default options (table_id = 0): the salvage must still open the source
    // and carry its real id through.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy non-zero-id SST salvages completely: {report:?}",
    );

    // The recovered copy reopens under the SOURCE's id (the recover
    // cross-checks the stored table_id against the expected one).
    let recovered = open_with_id(dest, &fs, TID)?;
    assert_eq!(
        recovered.metadata.id, TID,
        "the salvaged copy is stamped with the source's table id",
    );
    assert_eq!(recovered.metadata.item_count, u64::from(n));
    Ok(())
}

#[test]
fn salvage_of_a_healthy_sst_recovers_every_block() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Build a multi-block source SST: small data blocks force several blocks so
    // the per-block walk has more than one block to recover.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        report.is_complete(),
        "a healthy SST salvages with no dropped blocks: {report:?}",
    );
    assert!(
        report.blocks_total >= 2,
        "256-byte blocks over 200 entries should yield several data blocks, got {}",
        report.blocks_total,
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block of a healthy SST is salvaged",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered",
    );
    assert_eq!(
        report.salvaged_path.as_deref(),
        Some(dest.as_path()),
        "a salvaged file is written when at least one block is recovered",
    );

    // Every block of a healthy SST reads back clean, so every salvaged block is
    // copied through verbatim — none re-encoded.
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_salvaged,
        "a healthy SST's blocks are all copied verbatim",
    );

    // The salvaged copy is a valid SST that reopens and holds every key.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens with the full item count",
    );
    Ok(())
}

/// A clean block is byte-copied verbatim, not decoded and re-encoded: its raw
/// on-disk bytes in the salvaged SST are identical to the source's, and the walk
/// reports it under `blocks_copied_verbatim`.
#[test]
fn salvage_copies_a_clean_block_verbatim() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "healthy SST salvages clean: {report:?}"
    );
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_total,
        "every clean block is copied verbatim, none re-encoded",
    );

    // The first data block's raw on-disk bytes must be byte-identical between the
    // source and the salvaged copy (each resolved through its own intact index).
    let first_block = |path: &std::path::Path| -> crate::Result<(usize, usize)> {
        let table = open(path.to_path_buf(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        let off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        Ok((off, kh.as_ref().size() as usize))
    };
    let (src_off, src_size) = first_block(&source)?;
    let (dst_off, dst_size) = first_block(&dest)?;
    assert_eq!(
        src_size, dst_size,
        "the verbatim copy preserves the block's on-disk size",
    );

    let src_bytes = std::fs::read(&source)?;
    let dst_bytes = std::fs::read(&dest)?;
    let src_block = src_bytes.get(src_off..src_off + src_size);
    let dst_block = dst_bytes.get(dst_off..dst_off + dst_size);
    assert!(
        src_block.is_some() && src_block == dst_block,
        "the clean block is copied byte-for-byte into the salvaged SST",
    );
    Ok(())
}

/// One deliberately corrupted data block: salvage drops exactly that block
/// (naming its key range) and recovers every other block, instead of failing
/// the whole file. This is the core block-granular contract.
#[test]
fn salvage_drops_a_corrupted_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Resolve the second data block's on-disk offset from the (intact) index,
    // then flip a byte inside its PAYLOAD (past the fixed-size block header) so
    // the header still frames but the block's data checksum fails on load.
    // load_data_block reads the block by the index handle's size, so the
    // corruption surfaces as that one block failing at load — kept via the
    // index, dropped as a checksum mismatch — not as a physical-walk desync.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    // Land well past the fixed data-block header (magic + type + checksums +
    // lengths) so the frame stays intact and only the payload rots.
    let header_len = crate::table::block::Header::header_len(crate::table::block::BlockType::Data);
    let Ok(target_usize) = usize::try_from(target) else {
        panic!("data block offset {target} does not fit usize on this target");
    };
    let flip = target_usize + header_len + 8;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert!(
        !report.is_complete(),
        "a corrupted block must be reported as dropped: {report:?}",
    );
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert!(
        report.dropped.first().is_some_and(|d| {
            matches!(d.reason, DropReason::ChecksumMismatch) && d.key_range.is_some()
        }),
        "the dropped block reports a checksum mismatch and names the key range it lost: {report:?}",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy reopens and holds exactly the recovered entries.
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        report.entries_salvaged,
        "the salvaged SST holds exactly the entries the report counted",
    );
    Ok(())
}

/// A data block that needs ECC recovery to read is NOT copied verbatim — its
/// on-disk bytes are faulty, so propagating them would carry the corruption into
/// the recovered copy. Salvage re-encodes the healed payload instead (clean bytes
/// in the copy), while the surrounding clean blocks are still copied verbatim.
#[cfg(feature = "page_ecc")]
#[test]
fn salvage_reencodes_an_ecc_recovered_block_rather_than_copying_it() -> crate::Result<()> {
    use crate::table::block::{EccParams, Header};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // Data blocks carry RS(4,2) parity, so a small corruption is healed on read
    // rather than failing the block.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_ecc(Some(EccParams::RS_4_2));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Flip one payload byte of the FIRST data block so reading it must repair via
    // RS parity (an ECC-recovered read, not a clean one).
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        *kh.as_ref().offset()
    };
    let pos = usize::try_from(first_off).unwrap_or(usize::MAX) + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(pos) {
        *b ^= 0x80;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    // The healed block is recovered, not dropped — nothing is lost.
    assert!(
        report.is_complete(),
        "an ECC-recoverable block is healed, not dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block is recovered",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered",
    );
    // Exactly the healed block was re-encoded; the rest were copied verbatim.
    assert_eq!(
        report.blocks_copied_verbatim,
        report.blocks_salvaged - 1,
        "the ECC-recovered block is re-encoded, not copied verbatim",
    );

    // The salvaged copy reopens with every key; its bytes are freshly encoded, so
    // they no longer need ECC repair.
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// Bit rot confined to a block's PARITY trailer reads as clean (the payload
/// checksum passes and parity is only consulted on a mismatch), so a verbatim
/// copy would carry the rotted parity into the salvaged SST as latent ECC
/// corruption. Salvage must verify the trailer before copying and re-encode
/// (regenerating fresh parity) when it disagrees: every data block of the
/// recovered copy must carry parity that matches its payload.
#[cfg(feature = "page_ecc")]
#[test]
fn salvage_regenerates_a_rotted_parity_trailer_rather_than_copying_it() -> crate::Result<()> {
    use crate::coding::Decode;
    use crate::table::block::{EccParams, Header, expected_parity_len};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_ecc(Some(EccParams::RS_4_2));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Flip one byte INSIDE the first data block's parity trailer (right after
    // its `data_length` payload). The payload checksum still verifies, so the
    // block reads back clean with no ECC recovery.
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(mut cursor) = bytes.get(first_off..) else {
        panic!("first data block within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let trailer_pos =
        first_off + Header::header_len(header.block_type) + header.data_length as usize;
    let Some(slot) = bytes.get_mut(trailer_pos) else {
        panic!("parity trailer within the file");
    };
    *slot ^= 0xFF;
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "the payload is intact, every block is recovered: {report:?}",
    );
    assert_eq!(reopen_item_count(dest.clone(), &fs)?, u64::from(n));

    // Every data block of the salvaged copy carries parity that matches its
    // payload — the rotted trailer was regenerated, not byte-copied.
    let dest_bytes = std::fs::read(&dest)?;
    let dest_table = open(dest, &fs)?;
    let (ds, ps) = EccParams::RS_4_2.as_shards();
    for kh in dest_table.data_block_handles() {
        let kh = kh?;
        let off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        let Some(mut cursor) = dest_bytes.get(off..) else {
            panic!("block at offset {off} within the file");
        };
        let hdr = Header::decode_from(&mut cursor)?;
        let hlen = Header::header_len(hdr.block_type);
        let dl = hdr.data_length as usize;
        let Some(payload) = dest_bytes.get(off + hlen..off + hlen + dl) else {
            panic!("payload of block at offset {off} within the file");
        };
        let plen = expected_parity_len(hdr.data_length, EccParams::RS_4_2) as usize;
        let Some(trailer) = dest_bytes.get(off + hlen + dl..off + hlen + dl + plen) else {
            panic!("parity trailer of block at offset {off} within the file");
        };
        let fresh = crate::ecc::encode_parity(payload, ds, ps)?;
        assert_eq!(
            trailer,
            fresh.as_slice(),
            "block at offset {off}: the salvaged copy's parity matches its payload",
        );
    }
    Ok(())
}

/// A columnar source with one corrupted PAX data block: the columnar loader
/// fails to reconstruct that block (a torn sub-column frame), so salvage drops
/// it and recovers every other block, writing the survivors as a plain row SST.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_corrupted_columnar_block_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A columnar SST (PAX blocks + zone map), no deletes so there is no
    // delete-bitmap section to worry about here.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Corrupt the second columnar data block's PAYLOAD (offset from the intact
    // index, past the fixed block header) so the frame stays intact but its
    // reconstruction fails on load — kept via the index, dropped at load.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source columnar SST must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    // Land well past the fixed data-block header so only the payload rots and
    // the header still frames (otherwise the physical walk resyncs past it).
    let header_len = crate::table::block::Header::header_len(crate::table::block::BlockType::Data);
    let Ok(target_usize) = usize::try_from(target) else {
        panic!("data block offset {target} does not fit usize on this target");
    };
    let flip = target_usize + header_len + 8;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;

    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the one corrupted columnar block is dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged,
        report.blocks_total - 1,
        "every columnar block but the corrupted one is recovered",
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    // The salvaged copy stays COLUMNAR (mirrored from the source) and holds the
    // recovered rows — no longer degraded to a row-major copy.
    let recovered = open(dest, &fs)?;
    assert_eq!(recovered.metadata.item_count, report.entries_salvaged);
    assert!(
        recovered.metadata.columnar,
        "a columnar source salvages into a columnar copy, not a row-major one",
    );
    Ok(())
}

/// A columnar block whose outer `ColumnBatch` frame decodes but whose row
/// materialization fails (an invalid value-type byte in an otherwise
/// checksum-consistent block) is dropped like any other block-local decode
/// failure — one malformed block must not abort the whole salvage and discard
/// the destination while later blocks are still recoverable.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_columnar_block_with_an_invalid_value_type() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Poison the SECOND data block: decode its ColumnBatch, stamp an invalid
    // value-type tag into the first row, re-encode under the writer's Plain
    // codec (byte-identical framing => same length), and re-stamp the header
    // checksum. The block stays checksum-consistent, so the failure surfaces
    // in row materialization — not as an ordinary checksum drop.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    // Columns are ordered (key, seqno, value-type, values...); 0xFF is not a
    // defined ValueType tag.
    let Some(vt_byte) = batch.columns.get_mut(2).and_then(|col| col.data.get_mut(0)) else {
        panic!("value-type column present and non-empty");
    };
    *vt_byte = 0xFF;
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "a one-byte in-place mutation re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    assert_eq!(
        new_block.len(),
        header_len,
        "header re-encodes to its length"
    );
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // Salvage drops exactly the poisoned block and recovers every other one.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the poisoned block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the invalid value-type tag classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// A checksum-consistent columnar block whose entries are OUT OF internal-key
/// order (two adjacent keys swapped) must be dropped, not emitted: verbatim
/// paths skip the ingest ordering checks, so an unvalidated malformed block
/// would register a wrong last-key in the recovered SST's index and corrupt
/// binary search / scan order. The rest of the SST still salvages.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_columnar_block_with_out_of_order_keys() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Poison the SECOND data block: swap the first two rows' user keys inside
    // the key column (equal-length keys keep the Bytes framing intact),
    // re-encode, and re-stamp the header checksum. The block stays
    // checksum-consistent and its rows materialize fine — only the ordering
    // invariant is broken.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    assert!(batch.row_count >= 2, "block holds at least two rows");
    {
        // Key column framing: (row_count + 1) LE u32 offsets, then payload.
        let Some(key_col) = batch.columns.first_mut() else {
            panic!("key column present");
        };
        let table_len = (batch.row_count as usize + 1) * 4;
        let off = |data: &[u8], idx: usize| -> usize {
            let Some(b) = data.get(idx * 4..idx * 4 + 4) else {
                panic!("offset {idx} within the frame table");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4])) as usize
        };
        let (o0, o1, o2) = (
            off(&key_col.data, 0),
            off(&key_col.data, 1),
            off(&key_col.data, 2),
        );
        assert_eq!(o1 - o0, o2 - o1, "adjacent keys are equal-length");
        let len = o1 - o0;
        let Some(first) = key_col.data.get(table_len + o0..table_len + o0 + len) else {
            panic!("first key within the column");
        };
        let first = first.to_vec();
        let Some(second) = key_col.data.get(table_len + o1..table_len + o1 + len) else {
            panic!("second key within the column");
        };
        let second = second.to_vec();
        let Some(dst0) = key_col.data.get_mut(table_len + o0..table_len + o0 + len) else {
            panic!("first key range within the column");
        };
        dst0.copy_from_slice(&second);
        let Some(dst1) = key_col.data.get_mut(table_len + o1..table_len + o1 + len) else {
            panic!("second key range within the column");
        };
        dst1.copy_from_slice(&first);
    }
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "an in-place key swap re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // Salvage drops exactly the out-of-order block and recovers every other one.
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// A checksum-clean columnar block that decodes as a ZERO-ROW `ColumnBatch`
/// is malformed input (a real writer never emits an empty block): the writer
/// primitive emits nothing for it, so counting it as salvaged would let an
/// SST whose only block is empty report `salvaged_path = Some(dest)` while
/// the empty-table `finish` REMOVES `dest` — and a mixed SST would
/// under-report its dropped key ranges. Such a block must be dropped as a
/// decode error.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_a_zero_row_columnar_block() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::table::block::Header;
    use crate::table::columnar::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
    };

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A single-block columnar SST (few rows, default block size). An ODD row
    // count lets the retry below flip the payload-length parity by growing
    // every value one byte.
    let n = 9u32;
    let build = |value_pad: usize| -> crate::Result<()> {
        let _ = std::fs::remove_file(&source);
        let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_columnar(true);
        for i in 0..n {
            writer.write(InternalValue::from_components(
                format!("key{i:05}").into_bytes(),
                format!("val{i:05}{}", "x".repeat(value_pad)).into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        assert!(writer.finish()?.is_some(), "source SST is non-empty");
        Ok(())
    };
    build(0)?;

    // The zero-row replacement encodes to 8 (row/column counts) + 44 bytes of
    // intrinsic + value column headers, padded to the ORIGINAL payload length
    // with extra empty value sub-columns (Fixed = 10 bytes, Bytes = 14 — every
    // reachable length is even, so an odd source payload is rebuilt one byte
    // per value larger to flip its parity).
    let payload_len = |src: &std::path::Path| -> crate::Result<usize> {
        let bytes = std::fs::read(src)?;
        let mut cursor = bytes.as_slice();
        let header = Header::decode_from(&mut cursor)?;
        Ok(header.data_length as usize)
    };
    let mut target_len = payload_len(&source)?;
    if target_len % 2 != 0 {
        build(1)?;
        target_len = payload_len(&source)?;
    }
    assert_eq!(target_len % 2, 0, "an even payload length is reachable");

    let empty_fixed = |id: u16, width: u8| Column {
        column_id: id,
        type_tag: TypeTag::Fixed(width),
        validity: None,
        data: Vec::new(),
    };
    let mut columns = vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            // A zero-row Bytes column is exactly its (row_count + 1) * 4 = 4
            // byte offset table.
            data: vec![0u8; 4],
        },
        empty_fixed(COL_SEQNO, 8),
        empty_fixed(COL_VALUE_TYPE, 1),
        empty_fixed(COL_VALUE, 1),
    ];
    let Some(mut rem) = target_len.checked_sub(8 + 14 + 10 + 10 + 10) else {
        panic!("source payload larger than the zero-row skeleton");
    };
    let mut next_id = COL_VALUE + 1;
    // Greedy fill: Bytes columns (+14) until the remainder is divisible by
    // 10, then Fixed columns (+10).
    while rem % 10 != 0 {
        columns.push(Column {
            column_id: next_id,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        });
        next_id += 1;
        let Some(next_rem) = rem.checked_sub(14) else {
            panic!("remainder covers a Bytes column");
        };
        rem = next_rem;
    }
    while rem > 0 {
        columns.push(empty_fixed(next_id, 1));
        next_id += 1;
        rem -= 10;
    }
    let batch = ColumnBatch {
        row_count: 0,
        columns,
    };
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        target_len,
        "the zero-row batch pads to the original payload length",
    );

    // Splice it under a re-stamped checksum (frame length unchanged).
    let mut bytes = std::fs::read(&source)?;
    let mut cursor = bytes.as_slice();
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block: Vec<u8> = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(..new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    // The zero-row block is DROPPED, so nothing is recoverable: no destination
    // is left behind and no salvaged path is reported (the pre-fix behavior
    // counted it as salvaged, reporting Some(dest) for a file the empty-table
    // finish had just removed).
    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the zero-row block is dropped as malformed: {report:?}",
    );
    assert_eq!(report.blocks_salvaged, 0, "{report:?}");
    assert_eq!(
        report.salvaged_path, None,
        "an SST whose only block is empty reports nothing salvaged",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind",
    );
    Ok(())
}

/// The ROW-source twin of the columnar out-of-order drop: a checksum-clean
/// row block with two adjacent keys swapped passes frame decode and row
/// materialization, so the ordering guard before the emit is the only thing
/// standing between it and a recovered SST with a corrupt index order. The
/// rejection is block-local: the block drops, the rest still salvages.
#[test]
fn salvage_drops_a_row_block_with_out_of_order_keys() -> crate::Result<()> {
    use crate::coding::Encode;
    use crate::comparator::default_comparator;
    use crate::table::block::Header;
    use crate::table::block::decoder::ParsedItem as _;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    // Restart interval 1 + no hash index: every entry stores its full key, so
    // swapping two equal-length keys re-encodes to a byte-identical length.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_data_block_restart_interval(1)
        .use_data_block_hash_ratio(0.0);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Poison the SECOND data block: decode its entries, swap the first two
    // (equal-length keys), re-encode under the same block parameters, and
    // re-stamp the header checksum.
    let (block_off, poisoned_rows, new_block) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        let block_off = usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX);
        let sb = table.salvage_load_block(kh.as_ref(), crate::table::block::BlockType::Data)?;
        let header = sb.block.header;
        let db = crate::table::DataBlock::from_loaded(sb.block, false)?;
        let iter = db.try_iter(default_comparator())?;
        let mut entries: alloc::vec::Vec<crate::InternalValue> =
            iter.map(|p| p.materialize(db.as_slice())).collect();
        assert!(entries.len() >= 2, "block holds at least two rows");
        entries.swap(0, 1);
        let mut new_payload: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
        crate::table::DataBlock::encode_into(&mut new_payload, &entries, 1, 0.0)?;
        assert_eq!(
            new_payload.len(),
            header.data_length as usize,
            "an adjacent equal-length key swap re-encodes to the same length",
        );
        let header_len = Header::header_len(header.block_type);
        let new_header = Header {
            checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
            ..header
        };
        let mut new_block: alloc::vec::Vec<u8> =
            alloc::vec::Vec::with_capacity(header_len + new_payload.len());
        new_header.encode_into(&mut new_block)?;
        new_block.extend_from_slice(&new_payload);
        (block_off, entries.len() as u64, new_block)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows,
        "every row outside the poisoned block is recovered",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n) - poisoned_rows);
    Ok(())
}

/// A delete-bearing columnar SST whose TLI mirrors AND zone map both omit
/// the same TRAILING block: the positioning chain over the remaining
/// indexed blocks stays self-consistent, so `delete_positions_verified`
/// passes and the walk takes the masked re-emit — but the hidden block is
/// recovered by the PHYSICAL gap walk with no verified start position,
/// and masking it as "all rows live" would permanently resurrect the rows
/// the delete bitmap marked there, without the explicit resurrection
/// opt-in. The masked path must DROP an index-omitted block instead.
#[cfg(feature = "columnar")]
#[test]
fn salvage_does_not_resurrect_deletes_in_an_index_omitted_block() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Deletes land in the LAST block (the one both forges will hide).
    let deleted = [n - 1, n - 2, n - 3];
    for pos in deleted {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Row count of the block both forges hide, for the loss accounting.
    let hidden_rows = {
        let table = open(source.clone(), &fs)?;
        let Some(last) = table.data_block_handles().filter_map(Result::ok).last() else {
            panic!("source must have data blocks");
        };
        let Some(batch) = table.load_columnar_block_masked(last.as_ref())? else {
            panic!("the last block holds live rows");
        };
        // The masked load on the INTACT source applies the bitmap, so add
        // the deletes back for the block's physical row count.
        u64::from(batch.row_count) + deleted.len() as u64
    };

    // Consistent forge: the zone map loses its last entry FIRST (the TLI
    // still addresses the block for the entry lookup), then both TLI
    // mirrors hide the same trailing block.
    crate::test_forge::forge_zone_map_drop_last_entry(&source, 0)?;
    crate::test_forge::forge_tli_mirrors_truncated(&source, 0, None)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the index-omitted block has no verified delete position and must \
         drop, never re-emit live: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - deleted.len() as u64 - (hidden_rows - deleted.len() as u64),
        "every indexed block's surviving rows are recovered: {report:?}",
    );
    for pos in deleted {
        let key = format!("key{pos:05}");
        assert!(
            reopen_get(dest.clone(), &fs, key.as_bytes())?.is_none(),
            "a positionally deleted row must not resurrect via the gap walk",
        );
    }
    Ok(())
}

/// The DELETE-BEARING twin of the columnar out-of-order drop: the swapped
/// keys keep every block's row count intact, so the delete positions still
/// verify and the walk takes the masked re-emit — whose writer then rejects
/// the broken ordering. The rejection stays block-local: only the poisoned
/// block drops, deletes still apply to every other block.
#[cfg(feature = "columnar")]
#[test]
fn salvage_drops_an_out_of_order_columnar_block_in_a_delete_bearing_sst() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;
    use crate::table::columnar::{CodecId, ColumnBatch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    let deletes = [5u32, 50, 150];
    for pos in deletes {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Poison the SECOND data block exactly like the delete-free variant: swap
    // the first two rows' user keys inside the key column and re-stamp the
    // checksum. Row counts stay intact, so the delete positions still verify.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let Some(payload) = block.get(header_len..header_len + header.data_length as usize) else {
        panic!("payload range within the block");
    };
    let mut batch = ColumnBatch::decode(payload)?;
    let poisoned_rows = u64::from(batch.row_count);
    assert!(batch.row_count >= 2, "block holds at least two rows");
    {
        let Some(key_col) = batch.columns.first_mut() else {
            panic!("key column present");
        };
        let table_len = (batch.row_count as usize + 1) * 4;
        let off = |data: &[u8], idx: usize| -> usize {
            let Some(b) = data.get(idx * 4..idx * 4 + 4) else {
                panic!("offset {idx} within the frame table");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4])) as usize
        };
        let (o0, o1, o2) = (
            off(&key_col.data, 0),
            off(&key_col.data, 1),
            off(&key_col.data, 2),
        );
        assert_eq!(o1 - o0, o2 - o1, "adjacent keys are equal-length");
        let len = o1 - o0;
        let Some(first) = key_col.data.get(table_len + o0..table_len + o0 + len) else {
            panic!("first key within the column");
        };
        let first = first.to_vec();
        let Some(second) = key_col.data.get(table_len + o1..table_len + o1 + len) else {
            panic!("second key within the column");
        };
        let second = second.to_vec();
        let Some(dst0) = key_col.data.get_mut(table_len + o0..table_len + o0 + len) else {
            panic!("first key range within the column");
        };
        dst0.copy_from_slice(&second);
        let Some(dst1) = key_col.data.get_mut(table_len + o1..table_len + o1 + len) else {
            panic!("second key range within the column");
        };
        dst1.copy_from_slice(&first);
    }
    let new_payload = batch.encode(CodecId::Plain)?;
    assert_eq!(
        new_payload.len(),
        payload.len(),
        "an in-place key swap re-encodes to the same length",
    );
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the out-of-order block is dropped: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(DropReason::DecodeError(_))
        ),
        "the ordering violation classifies as a decode error: {report:?}",
    );
    // Every row outside the poisoned block is recovered, minus the deletes
    // that fall outside it (none of 5 / 50 / 150 land in the second block,
    // which spans rows ~17..34 at 256-byte blocks).
    assert_eq!(
        report.entries_salvaged,
        u64::from(n) - poisoned_rows - deletes.len() as u64,
        "rows outside the poisoned block are recovered with deletes applied",
    );
    // LOGICAL visibility: the deletes were applied faithfully, so the deleted
    // keys stay masked while a neighbouring live key reads back.
    for pos in deletes {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_none(),
            "the deleted key at position {pos} stays masked in the recovered copy",
        );
    }
    assert!(
        reopen_get(dest, &fs, b"key00051")?.is_some(),
        "a neighbouring live key reads back from the recovered copy",
    );
    Ok(())
}

/// A DESTINATION write failure mid-walk is a hard error, not a dropped block:
/// the salvage propagates it and removes the partial destination so a retry
/// or repair caller never sees half-written output.
#[test]
fn salvage_sst_errors_and_discards_the_dest_on_a_write_failure() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    // Values big enough that the destination's buffered writer flushes
    // mid-walk (so the failure surfaces through a block emit, not finish).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..200u32 {
        writer.write(InternalValue::from_components(
            format!("key{i:05}").into_bytes(),
            vec![0xAB; 1_024],
            1,
            ValueType::Value,
        ))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other))
            .on_path("salvaged")
            .once(),
    );
    let result = salvage_sst(&source, dest.clone(), &fs);
    injector.clear();

    assert!(
        result.is_err(),
        "a destination write failure errors the whole salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "the partial destination is removed on a write failure",
    );
    Ok(())
}

/// By default salvage FAILS CLOSED on a delete-bearing SST whose delete
/// bitmap is unreadable: recovering "all rows live" would resurrect
/// positionally-deleted rows, so that degradation requires the caller's
/// explicit [`SalvageOptions::allow_delete_resurrection`] opt-in. No
/// destination file is left behind.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_corrupt_delete_bitmap_by_default() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the `delete_bitmap` SFA section (data blocks stay intact).
    let (db_pos, db_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("source must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(db_pos + db_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "default salvage refuses to resurrect deleted rows: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// Same fail-closed default for the other degradation: a READABLE delete
/// bitmap whose positioning zone map is corrupt cannot be applied, so the
/// default salvage refuses rather than recovering all rows live.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_unpositionable_delete_bitmap_by_default() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the `zone_map` section (the bitmap stays readable but can no
    // longer be positioned).
    let (zm_pos, zm_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(zm_pos + zm_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "default salvage refuses to resurrect deleted rows: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// A columnar source carrying deletes whose `delete_bitmap` section is
/// corrupted (data blocks intact): normal recovery refuses to open it (opening
/// would resurrect deleted rows) and default salvage fails closed, but a
/// caller who explicitly opts into [`SalvageOptions::allow_delete_resurrection`]
/// degrades to "all rows live" and recovers every block.
#[cfg(feature = "columnar")]
#[test]
fn salvage_tolerates_a_corrupt_delete_bitmap_as_all_live() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Mark a few positions deleted so a delete-bitmap section is co-written.
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the middle of the `delete_bitmap` SFA section (the data blocks
    // stay intact, so only the sidecar is damaged).
    let (db_pos, db_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("source must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(db_pos + db_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a corrupt bitmap would resurrect deleted rows.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must fail closed on a corrupt delete-bitmap",
    );

    // With the explicit opt-in, salvage degrades to "all rows live": every
    // block recovers, nothing masked.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the sidecar was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live, the corrupt bitmap is ignored",
    );
    // The SST was written WITH deletes (it carries a delete-bitmap section), so
    // even though the degraded bitmap reads as empty, salvage must NOT take the
    // verbatim copy-through fast path: that would byte-copy the physical blocks
    // (including positionally-deleted rows) without the bitmap. It re-emits
    // instead, so nothing is copied verbatim here.
    assert_eq!(
        report.blocks_copied_verbatim, 0,
        "a delete-bearing SST is never copied verbatim, even with a degraded bitmap: {report:?}",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// An UNREADABLE data block inside a delete-bearing columnar SST makes every
/// later delete position unverifiable: the block's actual row count is
/// unknowable, and trusting the zone map's claim for it would let a
/// checksum-repatched count on exactly that block shift the masks of all
/// later readable blocks undetected. Default salvage must fail closed; the
/// explicit [`SalvageOptions::allow_delete_resurrection`] opt-in recovers the
/// readable rows live (never masking against unverified positions).
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_unreadable_block_in_a_delete_bearing_sst() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the SECOND data block's bytes (a plain checksum break): the
    // block becomes unreadable, so its actual row count is unknowable.
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&second) = offsets.get(1) else {
            panic!("source must have at least two data blocks, got {offsets:?}");
        };
        second
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the delete positions past the unreadable block
    // cannot be proven faithful.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "unverifiable delete positions fail the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: the readable rows are recovered LIVE; only the corrupt
    // block's rows are lost.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the dropped block's rows are lost: {report:?}",
    );
    assert_eq!(
        reopen_item_count(dest.clone(), &fs)?,
        report.entries_salvaged,
        "the recovered copy reopens with every salvaged row live",
    );
    // LOGICAL visibility of the resurrection: the deleted positions live
    // outside the corrupt block, so under the opt-in their keys read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A delete-bearing columnar SST where one block was REPLACED by a zero-row
/// batch and the zone map's claim for it patched to 0: the position verifier
/// would accept the block (decoded count 0 matches the claim) while the walk
/// drops it as malformed — leaving later blocks masked at starts that no
/// longer reflect the ORIGINAL row layout the bitmap was built against.
/// A zero-row batch is malformed input everywhere else in the salvage
/// pipeline, so the verifier must reject it too and fail the salvage closed.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_zero_row_block_in_a_delete_bearing_sst() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;
    use crate::table::columnar::{
        COL_SEQNO, COL_USER_KEY, COL_VALUE, COL_VALUE_TYPE, CodecId, Column, ColumnBatch, TypeTag,
    };

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Replace the SECOND data block with a length-preserving ZERO-ROW batch
    // (skeleton + padding columns, checksum re-stamped) — the same forgery
    // the plain zero-row test uses.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let target_len = header.data_length as usize;
    assert_eq!(
        target_len % 2,
        0,
        "the padded skeleton needs an even length"
    );
    let empty_fixed = |id: u16, width: u8| Column {
        column_id: id,
        type_tag: TypeTag::Fixed(width),
        validity: None,
        data: Vec::new(),
    };
    let mut columns = vec![
        Column {
            column_id: COL_USER_KEY,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        },
        empty_fixed(COL_SEQNO, 8),
        empty_fixed(COL_VALUE_TYPE, 1),
        empty_fixed(COL_VALUE, 1),
    ];
    let Some(mut rem) = target_len.checked_sub(8 + 14 + 10 + 10 + 10) else {
        panic!("source payload larger than the zero-row skeleton");
    };
    let mut next_id = COL_VALUE + 1;
    while rem % 10 != 0 {
        columns.push(Column {
            column_id: next_id,
            type_tag: TypeTag::Bytes,
            validity: None,
            data: vec![0u8; 4],
        });
        next_id += 1;
        let Some(next_rem) = rem.checked_sub(14) else {
            panic!("remainder covers a Bytes column");
        };
        rem = next_rem;
    }
    while rem > 0 {
        columns.push(empty_fixed(next_id, 1));
        next_id += 1;
        rem -= 10;
    }
    let new_payload = ColumnBatch {
        row_count: 0,
        columns,
    }
    .encode(CodecId::Plain)?;
    assert_eq!(new_payload.len(), target_len, "length-preserving forgery");
    let new_header = Header {
        checksum: crate::Checksum::from_raw(crate::hash::hash128(&new_payload)),
        ..header
    };
    let mut new_block: Vec<u8> = Vec::with_capacity(header_len + new_payload.len());
    new_header.encode_into(&mut new_block)?;
    new_block.extend_from_slice(&new_payload);
    let Some(target) = bytes.get_mut(block_off..block_off + new_block.len()) else {
        panic!("block range within the file");
    };
    target.copy_from_slice(&new_block);

    // Patch the zone map's row_count claim for the second block to 0 (the
    // first column's count drives the derived delete starts) and re-stamp
    // the zone-map block checksum, so the tampered chain is self-consistent.
    let zm_pos = {
        let mut f = std::io::Cursor::new(&bytes);
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let Some(mut zm_cursor) = bytes.get(zm_pos..) else {
        panic!("zone_map section within the file");
    };
    let zm_header = Header::decode_from(&mut zm_cursor)?;
    let zm_header_len = Header::header_len(zm_header.block_type);
    let zm_payload_range =
        zm_pos + zm_header_len..zm_pos + zm_header_len + zm_header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(zm_payload_range.clone()) else {
            panic!("zone_map payload within the file");
        };
        // Walk the wire layout (count u32; per block: block_offset u64 +
        // n_columns u16; per column: id u32 + type u8 + codec u8 +
        // null_count u32 + row_count u32 + min_len u32 + min + max_len u32
        // + max) to the SECOND block's FIRST column row_count — the field
        // the derived delete starts are built from.
        let read_u32 = |data: &[u8], at: usize| -> u32 {
            let Some(b) = data.get(at..at + 4) else {
                panic!("u32 at {at} within the zone map payload");
            };
            u32::from_le_bytes(b.try_into().unwrap_or([0; 4]))
        };
        let read_u16 = |data: &[u8], at: usize| -> u16 {
            let Some(b) = data.get(at..at + 2) else {
                panic!("u16 at {at} within the zone map payload");
            };
            u16::from_le_bytes(b.try_into().unwrap_or([0; 2]))
        };
        let mut at = 4; // past the block count
        // Skip block 1 entirely: offset u64 + n_columns u16, then each
        // column's fixed 14 bytes + variable min/max.
        at += 8;
        let block1_cols = read_u16(payload, at);
        at += 2;
        for _ in 0..block1_cols {
            at += 10; // id + type + codec + null_count
            at += 4; // row_count
            let min_len = read_u32(payload, at) as usize;
            at += 4 + min_len;
            let max_len = read_u32(payload, at) as usize;
            at += 4 + max_len;
        }
        // Block 2: seek to its first column's row_count and zero it.
        at += 8; // block_offset
        at += 2; // n_columns
        at += 10; // first column's id + type + codec + null_count
        let claimed = read_u32(payload, at);
        assert!(claimed > 0, "the second block originally holds rows");
        let Some(rc) = payload.get_mut(at..at + 4) else {
            panic!("second block's first row_count within the zone map payload");
        };
        rc.copy_from_slice(&0u32.to_le_bytes());
    }
    let new_zm_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(zm_payload_range).unwrap_or(&[]),
    ));
    let new_zm_header = Header {
        checksum: new_zm_checksum,
        ..zm_header
    };
    let mut zm_hdr_bytes: Vec<u8> = Vec::with_capacity(zm_header_len);
    new_zm_header.encode_into(&mut zm_hdr_bytes)?;
    let Some(zm_dst) = bytes.get_mut(zm_pos..zm_pos + zm_header_len) else {
        panic!("zone_map header within the file");
    };
    zm_dst.copy_from_slice(&zm_hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the zero-row block is unpositionable input (the
    // walk drops it while later blocks would be masked at starts the bitmap
    // was never built against).
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "a zero-row block in a delete-bearing SST fails the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );
    Ok(())
}

/// A footer-bearing row SST (per-KV checksums) whose block checksum was
/// re-stamped over a tampered entry: the BLOCK checksum verifies, but the
/// entry no longer matches its per-KV digest. Salvage must verify the footer
/// before emitting (verbatim or re-encoded) — otherwise it recovers a block
/// the live per-KV scrub would reject, laundering the corruption into a
/// "fully valid" copy.
#[test]
fn salvage_drops_a_row_block_with_a_stale_kv_digest() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::runtime_config::{ChecksumAlgorithm, KvChecksumPolicy};
    use crate::table::block::Header;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_kv_checksums(KvChecksumPolicy::AllLevels, ChecksumAlgorithm::Xxh3_64);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Tamper one entry byte inside the SECOND block's inner payload and
    // re-stamp the BLOCK checksum: the frame verifies clean, but the entry's
    // stored per-KV digest no longer matches its bytes.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("payload range within the block");
        };
        // A byte inside the first entry's value bytes (past the entry header),
        // well before the per-KV footer at the payload tail.
        let Some(b) = payload.get_mut(12) else {
            panic!("entry byte within the payload");
        };
        *b ^= 0xFF;
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes: Vec<u8> = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("block header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the block with a stale per-KV digest is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the tampered block's rows are not laundered into the copy: {report:?}",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, report.entries_salvaged);
    Ok(())
}

/// A delete-bearing columnar SST with a checksum-clean block whose
/// `ColumnBatch` does NOT decode (a repatched tamper that keeps the leading
/// row-count u32 intact but breaks the column framing): the block's ACTUAL row
/// count is unknowable, so every later block's delete positions are
/// unverifiable — the position verifier must fully decode each block rather
/// than trust the leading four bytes, and the default salvage must fail
/// closed instead of dropping the block and masking later rows at positions
/// it could not prove.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_an_undecodable_checksum_clean_block_with_deletes() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Poison the SECOND data block: stamp an invalid type tag into the first
    // column's header (payload layout: row_count u32, column_count u32, then
    // per column id u16 + type u8 + ... — so the first type byte sits at
    // payload offset 10) and re-stamp the block checksum. The leading
    // row-count u32 stays intact, the frame stays checksum-consistent, but
    // `ColumnBatch::decode` fails on the unknown tag.
    let (block_off, block_size) = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().filter_map(Result::ok).nth(1) else {
            panic!("source must have at least two data blocks");
        };
        (
            usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX),
            kh.as_ref().size() as usize,
        )
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(block) = bytes.get(block_off..block_off + block_size) else {
        panic!("block range within the file");
    };
    let mut cursor = block;
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range =
        block_off + header_len..block_off + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("payload range within the block");
        };
        let Some(tag) = payload.get_mut(10) else {
            panic!("first column's type byte within the payload");
        };
        *tag = 0xEE;
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(block_off..block_off + header_len) else {
        panic!("block header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — the block reads back checksum-clean but cannot
    // be decoded, so its actual row count (and every later block's delete
    // positions) cannot be proven faithful.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "unverifiable delete positions fail the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: the poisoned block is dropped, every other row is
    // recovered LIVE (never masked against unproven positions).
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the poisoned block is dropped: {report:?}",
    );
    assert!(
        report.entries_salvaged < u64::from(n),
        "the poisoned block's rows are lost: {report:?}",
    );
    assert_eq!(
        reopen_item_count(dest.clone(), &fs)?,
        report.entries_salvaged,
        "the recovered copy reopens with every salvaged row live",
    );
    // LOGICAL visibility of the resurrection: the deleted positions live
    // outside the poisoned block, so under the opt-in their keys read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A zone map that DECODES but carries wrong per-block row counts (a
/// checksum-repatched tamper) would misposition the delete bitmap: the masked
/// re-emit derives each block's start row from the zone map, so deletes land
/// on the wrong rows — deleted rows resurrect AND live rows vanish, silently.
/// Salvage must cross-check the claimed positions against the actual decoded
/// row counts and fail closed on a mismatch; with the explicit
/// [`SalvageOptions::allow_delete_resurrection`] opt-in it recovers all rows
/// live instead of masking against the wrong positions.
#[cfg(feature = "columnar")]
#[test]
fn salvage_fails_closed_on_a_zone_map_with_wrong_row_counts() -> crate::Result<()> {
    use crate::coding::{Decode, Encode};
    use crate::config::DeleteStrategy;
    use crate::table::block::Header;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Tamper the FIRST block's row count inside the zone map (wire layout:
    // count u32, then block_offset u64 + n_columns u16, then per column
    // id u32 + type_tag u8 + codec_id u8 + null_count u32 + row_count u32 —
    // so the first row_count sits at payload bytes 24..28) and re-stamp the
    // section block's checksum. The zone map still DECODES — only its claimed
    // positions are shifted for every block after the first.
    let zm_pos = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        usize::try_from(entry.pos()).unwrap_or(usize::MAX)
    };
    let mut bytes = std::fs::read(&source)?;
    let Some(mut cursor) = bytes.get(zm_pos..) else {
        panic!("zone_map section within the file");
    };
    let header = Header::decode_from(&mut cursor)?;
    let header_len = Header::header_len(header.block_type);
    let payload_range = zm_pos + header_len..zm_pos + header_len + header.data_length as usize;
    {
        let Some(payload) = bytes.get_mut(payload_range.clone()) else {
            panic!("zone_map payload within the file");
        };
        let Some(rc) = payload.get_mut(24..28) else {
            panic!("first row_count within the zone map payload");
        };
        let claimed = u32::from_le_bytes(rc.try_into().unwrap_or([0; 4]));
        assert!(claimed >= 2, "the first block holds at least two rows");
        rc.copy_from_slice(&(claimed - 1).to_le_bytes());
    }
    let new_checksum = crate::Checksum::from_raw(crate::hash::hash128(
        bytes.get(payload_range).unwrap_or(&[]),
    ));
    let new_header = Header {
        checksum: new_checksum,
        ..header
    };
    let mut hdr_bytes = Vec::with_capacity(header_len);
    new_header.encode_into(&mut hdr_bytes)?;
    let Some(hdr_dst) = bytes.get_mut(zm_pos..zm_pos + header_len) else {
        panic!("zone_map header within the file");
    };
    hdr_dst.copy_from_slice(&hdr_bytes);
    std::fs::write(&source, &bytes)?;

    // Default: fail closed — masking against the shifted positions would
    // silently corrupt visibility in the recovered SST.
    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        result.is_err(),
        "a mispositioning zone map fails the default salvage: {result:?}",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "no destination file is left behind by the refused salvage",
    );

    // Explicit opt-in: recover all rows LIVE (never mask against the wrong
    // positions).
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the zone map lies: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live under the opt-in",
    );
    assert_eq!(reopen_item_count(dest.clone(), &fs)?, u64::from(n));
    // LOGICAL visibility of the resurrection: "all rows live" means the keys
    // at the deleted positions read back.
    for pos in [5u32, 50, 150] {
        assert!(
            reopen_get(dest.clone(), &fs, format!("key{pos:05}").as_bytes())?.is_some(),
            "the opt-in resurrects the deleted key at position {pos}",
        );
    }
    Ok(())
}

/// A columnar SST with deletes whose ZONE MAP is corrupt (the bitmap stays
/// readable): the bitmap cannot be positioned without the zone map, so normal
/// recovery and default salvage fail closed, but a caller who explicitly opts
/// into [`SalvageOptions::allow_delete_resurrection`] ignores the bitmap
/// ("all rows live") and recovers every row.
#[cfg(feature = "columnar")]
#[test]
fn salvage_ignores_a_delete_bitmap_without_a_readable_zone_map() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    for pos in [5u32, 50, 150] {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    // Corrupt the zone_map section (the bitmap stays intact). The zone map
    // degrades to empty, leaving a readable bitmap that cannot be positioned.
    let (zm_pos, zm_len) = {
        let mut f = std::fs::File::open(&source)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"zone_map") else {
            panic!("source must carry a zone_map section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(zm_pos + zm_len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    // Normal recovery fails closed: a bitmap with no positioning zone map.
    assert!(
        open(source.clone(), &fs).is_err(),
        "normal recovery must reject a bitmap with no readable zone map",
    );

    // With the explicit opt-in, salvage ignores the unpositionable bitmap and
    // recovers every row live.
    let options = SalvageOptions {
        allow_delete_resurrection: true,
        ..SalvageOptions::default()
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert!(
        report.is_complete(),
        "the data blocks are intact; only the zone map was corrupt: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every row is recovered live once the unpositionable bitmap is ignored",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n));
    Ok(())
}

/// When the source cannot be opened at all (a corrupt SFA trailer makes even
/// salvage-mode recovery fail), `salvage_sst` returns an error rather than
/// writing a partial file.
#[test]
fn salvage_sst_errors_when_the_source_cannot_be_opened() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..50 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Truncate away the tail (SFA trailer + section mirrors) so the container is
    // unparseable and even salvage-mode recovery cannot open it.
    let mut bytes = std::fs::read(&source)?;
    bytes.truncate(bytes.len() / 2);
    std::fs::write(&source, &bytes)?;

    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an unparseable container must fail salvage, not write a partial file",
    );
    assert!(
        !dest.exists(),
        "no destination is written on an open failure"
    );
    Ok(())
}

/// A single-block SST whose only data block is corrupt salvages nothing: no
/// destination file is written and the report records the dropped block.
#[test]
fn salvage_sst_recovers_nothing_when_the_only_block_is_corrupt() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A handful of small keys fit in one default-sized data block.
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..8 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Corrupt the sole data block (offset from the intact index).
    let target = {
        let table = open(source.clone(), &fs)?;
        let offsets: alloc::vec::Vec<u64> = table
            .data_block_handles()
            .filter_map(Result::ok)
            .map(|kh| *kh.as_ref().offset())
            .collect();
        let Some(&only) = offsets.first() else {
            panic!("expected a single data block, got {offsets:?}");
        };
        assert_eq!(
            offsets.len(),
            1,
            "expected a single data block, got {offsets:?}"
        );
        only
    };
    let flip = usize::try_from(target).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert_eq!(report.blocks_salvaged, 0, "the only block was corrupt");
    assert_eq!(report.entries_salvaged, 0, "no entries recovered");
    assert_eq!(report.dropped.len(), 1, "the dropped block is reported");
    assert!(
        report.salvaged_path.is_none(),
        "nothing recoverable means no file is written",
    );
    assert!(!dest.exists(), "no destination file on an empty salvage");
    Ok(())
}

/// A columnar source whose delete-bitmap wholly covers its leading data
/// block(s): those blocks carry no live rows, so salvage skips them (nothing
/// salvaged, nothing dropped) and recovers the live rows of the rest.
#[cfg(feature = "columnar")]
#[test]
fn salvage_skips_a_wholly_deleted_block() -> crate::Result<()> {
    use crate::config::DeleteStrategy;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let n = 200u32;
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_data_block_size(256)
        .delete_strategy(DeleteStrategy::MergeOnRead);
    for i in 0..n {
        writer.write(iv(i))?;
    }
    // Delete the first 60 row positions: with 256-byte blocks this wholly covers
    // the leading data block(s), which then load as "no live rows".
    let deleted = 60u32;
    for pos in 0..deleted {
        writer.delete_bitmap_mut().insert(pos);
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar+deletes SST is non-empty",
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "wholly-deleted blocks are skipped, not dropped: {report:?}",
    );
    assert!(
        report.blocks_salvaged < report.blocks_total,
        "at least one leading block was wholly deleted and skipped: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n - deleted),
        "every live row is recovered, the deleted prefix is skipped",
    );
    assert_eq!(reopen_item_count(dest, &fs)?, u64::from(n - deleted));
    Ok(())
}

/// An SST carrying range tombstones cannot be salvaged: the positional KV walk
/// re-emits only point entries, so the tombstones would be silently dropped and
/// lower-level keys they cover could reappear after repair. Until the writer
/// path re-emits them, salvage fails closed.
#[test]
fn salvage_rejects_an_sst_with_range_tombstones() -> crate::Result<()> {
    use crate::UserKey;
    use crate::range_tombstone::RangeTombstone;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..20 {
        writer.write(iv(i))?;
    }
    // A range tombstone over part of the key space: the salvaged copy must not
    // silently drop it.
    writer.write_range_tombstone(RangeTombstone::new(
        UserKey::from(b"key00005".as_slice()),
        UserKey::from(b"key00010".as_slice()),
        2,
    ));
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    let result = salvage_sst(&source, dest.clone(), &fs);
    assert!(
        matches!(result, Err(crate::Error::FeatureUnsupported(_))),
        "an SST with range tombstones must fail closed, got {result:?}",
    );
    assert!(
        !dest.exists(),
        "no salvaged file is written when salvage fails closed",
    );
    Ok(())
}

/// Salvage drives every read and write through the injected `Fs`: an SST that
/// lives only in an in-memory backend (never on the real filesystem) salvages
/// and reopens purely through that backend. A source-digest path that bypassed
/// `fs` and read through `std::fs` would fail to find the file at all.
#[test]
fn salvage_sst_reads_and_writes_through_the_injected_fs() -> crate::Result<()> {
    use crate::fs::MemFs;

    let fs: Arc<dyn Fs> = Arc::new(MemFs::new());
    // `Writer::new` rewrites its path through `std::path::absolute`, which on
    // Windows resolves a `/`-rooted path against the current drive (`/memfs` ->
    // `D:\memfs`). Create the parent under that same absolutized form so the
    // writer's parent-directory check finds it on every platform (on Unix
    // `absolute` is a no-op, so this is just `/memfs`).
    let dir = std::path::absolute("/memfs")?;
    fs.create_dir_all(&dir)?;
    let source = dir.join("source");
    let dest = dir.join("salvaged");

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?.use_data_block_size(256);
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "in-memory source SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy in-memory SST salvages with no dropped blocks: {report:?}",
    );
    assert_eq!(
        report.entries_salvaged,
        u64::from(n),
        "every entry is recovered through the in-memory backend",
    );
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));
    assert_eq!(
        reopen_item_count(dest, &fs)?,
        u64::from(n),
        "the salvaged SST reopens through the same in-memory backend",
    );
    Ok(())
}

// --- Forwarded recovery context: encrypted + dictionary-compressed sources ---

/// Reads the second data block's on-disk offset from a context-aware reopen of
/// `source`, then flips a byte just past that block's header so its checksum /
/// AEAD tag fails on load while every other block stays intact.
#[cfg(any(feature = "encryption", zstd_any))]
fn corrupt_second_data_block(
    source: &std::path::Path,
    fs: &Arc<dyn Fs>,
    table_id: crate::table::TableId,
    encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
    #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
) -> crate::Result<()> {
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&**fs, source)?);
    let table = Table::recover(
        source.to_path_buf(),
        checksum,
        0,
        0,
        // Open under the source's table id so an encrypted index (AAD binds the
        // id) decrypts when reading the block offsets.
        table_id,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(fs),
        false,
        false,
        encryption,
        #[cfg(zstd_any)]
        zstd_dictionary,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    let offsets: alloc::vec::Vec<u64> = table
        .data_block_handles()
        .filter_map(Result::ok)
        .map(|kh| *kh.as_ref().offset())
        .collect();
    let Some(&second) = offsets.get(1) else {
        panic!("source SST must have at least two data blocks, got {offsets:?}");
    };
    let flip = usize::try_from(second).unwrap_or(0) + 16;
    let mut bytes = std::fs::read(source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(source, &bytes)?;
    Ok(())
}

/// An encrypted source: salvage cannot open it without the provider (the gap this
/// closes), but with the provider in `SalvageOptions` it block-salvages like a
/// plain SST and the recovered copy reopens under the same encryption.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_the_provider() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x42; 32]));

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        0,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Without the provider, the encrypted source cannot even be opened.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "an encrypted SST must not salvage without the provider",
    );

    // With the provider, it block-salvages: the corrupt block is dropped, the
    // rest recovered, and the copy is written encrypted.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
        expected_stored_id: None,
        allow_delete_resurrection: false,
        sync_mode: crate::fs::SyncMode::Normal,
        prefix_extractor: None,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER ENCRYPTION (a plaintext copy would fail the
    // encrypted reopen) and holds exactly the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the encrypted salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// A zstd-dictionary-compressed source: salvage cannot decompress it without the
/// dictionary, but with the dictionary in `SalvageOptions` it block-salvages and
/// the recovered copy reopens under the same dictionary.
#[cfg(zstd_any)]
#[test]
fn salvage_recovers_a_dictionary_sst_with_the_dictionary() -> crate::Result<()> {
    use crate::CompressionType;
    use crate::compression::ZstdDictionary;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    // A small training corpus so the dictionary has content to match against.
    let samples: alloc::vec::Vec<u8> = (0..4000u32).map(|i| (i % 251) as u8).collect();
    let dict = Arc::new(ZstdDictionary::new(&samples));
    let compression = CompressionType::ZstdDict {
        level: 3,
        dict_id: dict.id(),
    };

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_data_block_compression(compression)
        .use_zstd_dictionary(Some(Arc::clone(&dict)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source dictionary SST is non-empty"
    );

    corrupt_second_data_block(&source, &fs, 0, None, Some(Arc::clone(&dict)))?;

    // Without the dictionary, the source cannot even be opened: `recover_inner`
    // fail-fasts on the ZstdDict-id mismatch at open time (before any block
    // walk), so salvage returns `Err`, not a zero-recovered report.
    assert!(
        salvage_sst(&source, dest.clone(), &fs).is_err(),
        "a dictionary SST must not salvage without the dictionary",
    );

    let options = SalvageOptions {
        encryption: None,
        zstd_dictionary: Some(Arc::clone(&dict)),
        table_id: 0,
        expected_stored_id: None,
        allow_delete_resurrection: false,
        sync_mode: crate::fs::SyncMode::Normal,
        prefix_extractor: None,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The salvaged copy reopens UNDER THE DICTIONARY with the recovered entries.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        0,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        None,
        Some(Arc::clone(&dict)),
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the dictionary salvaged copy reopens with exactly the recovered entries",
    );
    Ok(())
}

/// An encrypted source sealed under a NON-ZERO table id: the encrypted-block AAD
/// binds the table id, so salvage must be given that id. With the wrong id the
/// AAD-bound blocks cannot be decrypted (the gap repair hit when it passed a
/// hardcoded `0`); with the right id it block-salvages and the copy reopens.
#[cfg(feature = "encryption")]
#[test]
fn salvage_recovers_an_encrypted_sst_with_a_nonzero_table_id() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let enc: Arc<dyn crate::encryption::EncryptionProvider> =
        Arc::new(crate::encryption::Aes256GcmProvider::new(&[0x37; 32]));
    const TID: crate::table::TableId = 7;

    let mut writer = Writer::new(source.clone(), TID, 0, Arc::clone(&fs))?
        .use_data_block_size(256)
        .use_encryption(Some(Arc::clone(&enc)));
    let n = 200u32;
    for i in 0..n {
        writer.write(iv(i))?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source encrypted SST is non-empty"
    );

    corrupt_second_data_block(
        &source,
        &fs,
        TID,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
    )?;

    // Wrong table id (the legacy hardcoded 0): the AAD-bound blocks cannot be
    // decrypted, so nothing is recovered (salvage either fails to open or drops
    // every block).
    let wrong = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: 0,
        expected_stored_id: None,
        allow_delete_resurrection: false,
        sync_mode: crate::fs::SyncMode::Normal,
        prefix_extractor: None,
    };
    let recovered_wrong = salvage_sst_with_options(&source, dest.clone(), &fs, &wrong)
        .map_or(0, |r| r.entries_salvaged);
    assert_eq!(
        recovered_wrong, 0,
        "the wrong table id cannot decrypt the AAD-bound encrypted source",
    );

    // Right table id: block-salvages, dropping only the corrupt block.
    let options = SalvageOptions {
        encryption: Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        table_id: TID,
        expected_stored_id: None,
        allow_delete_resurrection: false,
        sync_mode: crate::fs::SyncMode::Normal,
        prefix_extractor: None,
    };
    let report = salvage_sst_with_options(&source, dest.clone(), &fs, &options)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt block drops: {report:?}"
    );
    assert!(
        report.entries_salvaged > 0 && report.entries_salvaged < u64::from(n),
        "a partial key range is recovered, got {} of {n}",
        report.entries_salvaged,
    );

    // The recovered copy reopens under the same table id + encryption.
    let checksum = crate::Checksum::from_raw(crate::repair::compute_table_checksum(&*fs, &dest)?);
    let reopened = Table::recover(
        dest,
        checksum,
        0,
        0,
        TID,
        Arc::new(crate::cache::Cache::with_capacity_bytes(1 << 20)),
        Some(Arc::new(crate::descriptor_table::DescriptorTable::new(8))),
        Arc::clone(&fs),
        false,
        false,
        Some(Arc::clone(&enc)),
        #[cfg(zstd_any)]
        None,
        default_comparator(),
        #[cfg(feature = "metrics")]
        Arc::new(crate::Metrics::default()),
    )?;
    assert_eq!(
        reopened.metadata.item_count, report.entries_salvaged,
        "the recovered copy reopens under the same table id with the recovered entries",
    );
    Ok(())
}

// --- Blob (vlog) file record-granular salvage ---

use crate::vlog::blob_file::scanner::Scanner as BlobScanner;
use crate::vlog::blob_file::writer::Writer as BlobWriter;

/// Builds a blob file at `path` from `(key, value)` records (seqno 0, no
/// compression).
fn build_blob(
    path: &std::path::Path,
    fs: &Arc<dyn Fs>,
    records: &[(&[u8], &[u8])],
) -> crate::Result<()> {
    let mut writer = BlobWriter::new(path, 0, 0, &**fs)?;
    for (k, v) in records {
        writer.write(k, 0, v)?;
    }
    writer.finish()?;
    Ok(())
}

/// Scans a blob file into its `(key, value)` records (Ok records only).
fn scan_blob(path: &std::path::Path, fs: &Arc<dyn Fs>) -> crate::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    Ok(BlobScanner::new(path, &**fs, 0)?
        .filter_map(Result::ok)
        .map(|e| (e.key.to_vec(), e.value.to_vec()))
        .collect())
}

#[test]
fn salvage_blob_file_recovers_every_record_of_a_healthy_file() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"v0"),
        (b"k1", b"v1"),
        (b"k2", b"v2"),
        (b"k3", b"v3"),
    ];
    build_blob(&source, &fs, &records)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert!(
        report.is_complete(),
        "a healthy blob file drops nothing: {report:?}"
    );
    assert_eq!(report.records_salvaged, 4);
    assert_eq!(report.salvaged_path.as_deref(), Some(dest.as_path()));

    let recovered = scan_blob(&dest, &fs)?;
    let expected: Vec<(Vec<u8>, Vec<u8>)> = records
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    assert_eq!(
        recovered, expected,
        "every record round-trips through salvage"
    );
    Ok(())
}

/// `salvage_blob_file` must fsync the destination's PARENT DIRECTORY before
/// returning a salvaged path: the writer syncs the blob file's bytes, but
/// without the directory sync a power loss can discard the new directory entry
/// even though the report says recovery succeeded (the SST salvage writer
/// already syncs its parent directory). Fault-inject the directory fsync to
/// prove it happens — a build that never syncs the directory never triggers the
/// fault and wrongly reports the move durable.
#[test]
fn salvage_blob_file_syncs_the_destination_directory() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultInjector, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let out = dir.path().join("blobdest");
    std::fs::create_dir_all(&out)?;
    let dest = out.join("blob_salvaged");

    let injector = Arc::new(FaultInjector::new());
    let fs: Arc<dyn Fs> = Arc::new(FaultFs::with_injector(StdFs, Arc::clone(&injector)));

    let records: Vec<(&[u8], &[u8])> = vec![(b"k0", b"v0"), (b"k1", b"v1")];
    build_blob(&source, &fs, &records)?;

    // Arm AFTER building, so only the salvage's destination-directory sync trips.
    injector.arm(
        FaultRule::new(FaultOp::SyncDirectory, Fault::Error(ErrorKind::Other)).on_path("blobdest"),
    );

    let Err(err) = salvage_blob_file(&source, dest, &fs, 0) else {
        panic!("the destination-directory fsync fault must surface");
    };
    // Assert the surfaced error is the INJECTED directory-sync fault, not some
    // unrelated failure: a build that stopped syncing the directory would fail
    // elsewhere (or not at all), and a loose "any error" check would not notice.
    assert!(
        err.to_string().contains("injected fault on SyncDirectory"),
        "the salvage error must be the injected destination-directory sync fault, got {err:?}",
    );
    Ok(())
}

/// The TOCTOU variant of the pre-existing-destination guarantee: a file that
/// appears at `dest` AFTER any existence probe but BEFORE the writer's
/// `create_new` open (a concurrent worker winning the destination) must also
/// survive the failed salvage. Ownership is decided by `create_new` alone —
/// when it fails, this call created nothing and must remove nothing. The
/// injected `Metadata` fault materializes the race window deterministically:
/// the probe cannot see the file, yet `create_new` finds it.
#[test]
fn salvage_blob_file_keeps_a_racing_dest_created_after_the_existence_probe() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_dest");
    let plain: Arc<dyn Fs> = Arc::new(StdFs);
    build_blob(&source, &plain, &[(b"k0", b"v0")])?;

    // The "racing" worker's file is already at dest, but any metadata probe of
    // dest fails — exactly the window where the file lands between a stat and
    // the `create_new` open.
    std::fs::write(&dest, b"racing worker's blob")?;
    let fault = FaultFs::new(StdFs);
    fault.injector().arm(
        FaultRule::new(FaultOp::Metadata, Fault::Error(ErrorKind::NotFound)).on_path("blob_dest"),
    );
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "the destination is taken, the salvage fails: {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        b"racing worker's blob",
        "the racing worker's file survives the failed salvage",
    );
    Ok(())
}

/// A transient I/O failure on the verbatim REREAD must not drop the block:
/// the first, recovery-aware read has already produced a verified decoded
/// block, so the loader falls back to the re-encode path (`verbatim = None`)
/// exactly like a checksum / parity mismatch on the re-read frame. Reserving
/// `Err` for the initial verified read keeps one flaky pread from discarding
/// a block that is provably recoverable.
#[test]
fn salvage_load_block_reencodes_when_the_verbatim_reread_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;
    use crate::table::block::BlockType;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?;
    for i in 0..50u32 {
        writer.write(iv(i))?;
    }
    assert!(writer.finish()?.is_some(), "source SST is non-empty");

    // Collect the first data block's handle BEFORE arming the fault (the
    // open + index walk issue their own reads).
    let table = open(source, &fs)?;
    let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
        panic!("source has at least one data block");
    };
    let handle = *kh.as_ref();

    // Within `salvage_load_block` the FIRST positional read is the verified
    // recovery-aware load; the SECOND is the raw verbatim re-read. Fail
    // exactly that second read, once.
    injector.arm(
        FaultRule::new(FaultOp::ReadAt, Fault::Error(ErrorKind::Other))
            .on_path("source")
            .skip(1)
            .once(),
    );
    let result = table.salvage_load_block(&handle, BlockType::Data);
    injector.clear();

    let sb = match result {
        Ok(sb) => sb,
        Err(e) => panic!("a failed verbatim re-read falls back to re-encode, got Err({e:?})"),
    };
    assert!(
        sb.verbatim.is_none(),
        "the re-read was never verified, so the block must not be byte-copied",
    );
    assert!(
        !sb.block.data.is_empty(),
        "the verified first read's decoded payload is preserved for re-encoding",
    );
    Ok(())
}

/// `salvage_blob_file` must not delete a pre-existing file at `dest` when the
/// destination cannot be created (the writer's `create_new` open fails because
/// the path already exists): the error-path cleanup is only for a partial file
/// THIS call created. Deleting a pre-existing destination would turn an
/// argument mistake (a stale path collision, or `source == dest`) into data
/// loss.
#[test]
fn salvage_blob_file_keeps_a_preexisting_dest_on_open_failure() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_dest");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(&source, &fs, &[(b"k0", b"v0")])?;
    std::fs::write(&dest, b"pre-existing destination bytes")?;

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "an already-existing destination fails the salvage: {result:?}",
    );
    assert_eq!(
        std::fs::read(&dest)?,
        b"pre-existing destination bytes",
        "a pre-existing destination file survives the failed salvage",
    );
    Ok(())
}

/// The salvaged blob file is COMPACTED: after a dropped record every later
/// record shifts to a new offset, and existing SST `ValueHandle::offset`
/// values point into the SOURCE. The report's `offset_remap` must map every
/// salvaged record's source frame offset to its offset in the recovered file
/// (and omit the dropped one), so a caller can re-target handles before
/// swapping the file in.
#[test]
fn salvage_blob_file_reports_an_offset_remap_for_every_salvaged_record() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"v0-payload"),
        (b"k1", b"v1-payload"),
        (b"k2", b"v2-payload"),
        (b"k3", b"v3-payload"),
    ];
    build_blob(&source, &fs, &records)?;

    // Source frame offsets, in order, from a clean pre-corruption scan.
    let source_offsets: Vec<u64> = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.offset)
        .collect();
    assert_eq!(source_offsets.len(), 4, "four source records");

    // Corrupt the SECOND record's value bytes (a checksum break): the scanner
    // re-syncs at the next frame, so records 0, 2, 3 survive.
    {
        let Some(&second) = source_offsets.get(1) else {
            panic!("second record offset");
        };
        // Past the frame header, inside key/value bytes.
        let flip = usize::try_from(second).unwrap_or(0) + 45;
        let mut bytes = std::fs::read(&source)?;
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
        std::fs::write(&source, &bytes)?;
    }

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(report.records_salvaged, 3, "{report:?}");
    assert_eq!(report.dropped.len(), 1, "{report:?}");

    // The remap covers exactly the salvaged records, keyed by their SOURCE
    // offsets, and its targets are the actual frame offsets in the recovered
    // file (verified against a scan of the destination).
    let dest_offsets: Vec<u64> = BlobScanner::new(&dest, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.offset)
        .collect();
    let expected: Vec<(u64, u64)> = [0usize, 2, 3]
        .iter()
        .zip(&dest_offsets)
        .map(|(&src_idx, &new)| {
            (
                source_offsets.get(src_idx).copied().unwrap_or(u64::MAX),
                new,
            )
        })
        .collect();
    assert_eq!(
        report.offset_remap, expected,
        "the remap maps each surviving source frame to its compacted target",
    );
    // The dropped record's source offset is NOT in the map: its handle is lost.
    let Some(&dropped_src) = source_offsets.get(1) else {
        panic!("second record offset");
    };
    assert!(
        report
            .offset_remap
            .iter()
            .all(|(src, _)| *src != dropped_src),
        "a dropped record has no remap target: {report:?}",
    );
    Ok(())
}

/// When a record write to the destination fails mid-salvage, `salvage_blob_file`
/// must error AND remove the partial destination it created, so a retry / repair
/// caller never finds a half-written blob file.
#[test]
fn salvage_blob_file_removes_the_partial_dest_when_a_write_fails() -> crate::Result<()> {
    use crate::fs::{Fault, FaultFs, FaultOp, FaultRule};
    use crate::io::ErrorKind;

    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fault = FaultFs::new(StdFs);
    let injector = fault.injector();
    let fs: Arc<dyn Fs> = Arc::new(fault);

    let records: Vec<(&[u8], &[u8])> = vec![(b"k0", b"v0"), (b"k1", b"v1")];
    build_blob(&source, &fs, &records)?;

    // Fail every write to the destination file: the first recovered record's
    // write-back errors.
    injector.arm(
        FaultRule::new(FaultOp::Write, Fault::Error(ErrorKind::Other)).on_path("blob_salvaged"),
    );

    let result = salvage_blob_file(&source, dest.clone(), &fs, 0);
    assert!(
        result.is_err(),
        "a failed destination write must error the salvage",
    );
    assert!(
        !std::path::Path::new(&dest).exists(),
        "the partial destination is removed on a write failure",
    );
    Ok(())
}

#[test]
fn salvage_blob_file_drops_a_corrupt_record_and_keeps_the_rest() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"value-zero"),
        (b"k1", b"value-one"),
        (b"k2", b"value-two"),
        (b"k3", b"value-three"),
    ];
    build_blob(&source, &fs, &records)?;

    // Flip the last byte of the second record's value: the checksum (over
    // key + value) fails, but the frame header (lengths, magic) stays intact, so
    // the scanner reports a checksum mismatch and re-syncs at the next record.
    let Some(second_frame_end) = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .nth(1)
        .map(|e| e.frame_end)
    else {
        panic!("source blob must have at least two records");
    };
    let flip = usize::try_from(second_frame_end - 1).unwrap_or(0);
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "exactly the corrupt record drops: {report:?}"
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::ChecksumMismatch)
        ),
        "the dropped record reports a checksum mismatch: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 3,
        "the other three records are recovered"
    );

    // The salvaged file holds every record except the corrupted k1.
    let recovered = scan_blob(&dest, &fs)?;
    let keys: Vec<Vec<u8>> = recovered.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![b"k0".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        "the corrupt record's key is the only one missing",
    );
    Ok(())
}

/// A blob file where EVERY record is corrupt salvages nothing: the report
/// carries only drops, `salvaged_path` is `None`, and the empty destination
/// placeholder the writer created is removed (a repair caller would otherwise
/// re-quarantine a stray zero-record blob file in its place).
#[test]
fn salvage_blob_file_removes_the_empty_dest_when_nothing_is_recoverable() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![(b"k0", b"value-zero"), (b"k1", b"value-one")];
    build_blob(&source, &fs, &records)?;

    // Flip the last value byte of BOTH records: each frame header stays
    // intact, so the scanner reports one checksum mismatch per record and
    // re-syncs — leaving zero salvageable records.
    let frame_ends: Vec<u64> = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .map(|e| e.frame_end)
        .collect();
    assert_eq!(frame_ends.len(), 2, "source blob holds two records");
    let mut bytes = std::fs::read(&source)?;
    for end in frame_ends {
        let flip = usize::try_from(end - 1).unwrap_or(0);
        if let Some(b) = bytes.get_mut(flip) {
            *b ^= 0xFF;
        }
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(report.records_salvaged, 0, "{report:?}");
    assert_eq!(report.dropped.len(), 2, "both records drop: {report:?}");
    assert_eq!(
        report.salvaged_path, None,
        "nothing recoverable yields no salvaged path",
    );
    assert!(
        fs.metadata(&dest).is_err(),
        "the empty destination placeholder is removed",
    );
    Ok(())
}

/// A STRUCTURAL failure mid-walk (a record frame whose magic bytes are gone,
/// not a checksum miss) terminates the blob walk: the scanner cannot re-sync
/// past it, so the salvage records one `Corrupt` drop for the unreadable tail
/// and keeps everything scanned before it.
#[test]
fn salvage_blob_file_stops_at_a_smashed_frame_and_keeps_the_prefix() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    let records: Vec<(&[u8], &[u8])> = vec![
        (b"k0", b"value-zero"),
        (b"k1", b"value-one"),
        (b"k2", b"value-two"),
    ];
    build_blob(&source, &fs, &records)?;

    // Smash the LAST record's frame magic (the file structure and trailer
    // stay intact): the scanner reports it as a structural InvalidHeader it
    // cannot re-sync from, unlike a checksum miss.
    let Some(last_start) = BlobScanner::new(&source, &*fs, 0)?
        .filter_map(Result::ok)
        .nth(1)
        .map(|e| e.frame_end)
    else {
        panic!("source blob must have at least two records");
    };
    let mut bytes = std::fs::read(&source)?;
    let at = usize::try_from(last_start).unwrap_or(0);
    let Some(magic) = bytes.get_mut(at..at + 4) else {
        panic!("last record's frame magic within the file");
    };
    magic.copy_from_slice(b"????");
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.records_salvaged, 2,
        "the records before the smashed frame are recovered: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::Corrupt(_))
        ),
        "the truncated tail is recorded as a structural drop: {report:?}",
    );
    let recovered = scan_blob(&dest, &fs)?;
    assert_eq!(recovered.len(), 2, "the salvaged copy holds the prefix");
    Ok(())
}

/// A blob frame whose header CRC and data checksum are internally consistent
/// but whose `key_len` is ZERO yields an Ok scanner entry with an empty key —
/// which the blob writer's ingest asserts against. Salvage must route such a
/// frame through the corrupt-record path (dropped, walk continues) instead of
/// panicking in the writer and leaving a partial destination behind.
#[test]
fn salvage_blob_file_drops_an_empty_key_frame() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(&source, &fs, &[(b"k", b"vvvv"), (b"k2", b"second")])?;

    // Re-frame the FIRST record as `key_len = 0`: its key byte becomes the
    // first value byte (the hashed key||value byte span is unchanged), with
    // the header CRC and data checksum recomputed so the frame stays
    // internally consistent. V4 frame layout from offset 0:
    //   magic 4 | checksum 16 | seqno 8 | key_len 2 | real_val_len 4 |
    //   on_disk_val_len 4 | header_crc 4 | key | value.
    let mut bytes = std::fs::read(&source)?;
    let seqno = {
        let Some(b) = bytes.get(20..28) else {
            panic!("seqno within the first frame");
        };
        u64::from_le_bytes(b.try_into().unwrap_or([0; 8]))
    };
    // header_crc = truncated xxh3 over (seqno, key_len, real_val_len,
    // on_disk_val_len), matching the writer's framing.
    let new_hcrc = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&seqno.to_le_bytes());
        hasher.update(&0u16.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "intentionally truncated to the 4-byte header CRC"
        )]
        {
            hasher.digest() as u32
        }
    };
    // data checksum = xxh3_128(key || value || header_crc_le); with the empty
    // key the hashed span is the same "kvvvv" bytes plus the NEW header CRC.
    let new_checksum = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(b"kvvvv");
        hasher.update(&new_hcrc.to_le_bytes());
        hasher.digest128()
    };
    let patch = |bytes: &mut Vec<u8>, range: core::ops::Range<usize>, val: &[u8]| {
        let Some(slot) = bytes.get_mut(range) else {
            panic!("patch range within the first frame");
        };
        slot.copy_from_slice(val);
    };
    patch(&mut bytes, 4..20, &new_checksum.to_le_bytes());
    patch(&mut bytes, 28..30, &0u16.to_le_bytes());
    patch(&mut bytes, 30..34, &5u32.to_le_bytes());
    patch(&mut bytes, 34..38, &5u32.to_le_bytes());
    patch(&mut bytes, 38..42, &new_hcrc.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the empty-key frame drops as corrupt: {report:?}",
    );
    assert!(
        matches!(
            report.dropped.first().map(|d| &d.reason),
            Some(BlobDropReason::Corrupt(_))
        ),
        "the drop reason names the malformed frame: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 1,
        "the record after the malformed frame is still recovered",
    );
    let recovered = scan_blob(&dest, &fs)?;
    assert_eq!(
        recovered,
        vec![(b"k2".to_vec(), b"second".to_vec())],
        "the salvaged copy holds exactly the healthy record",
    );
    Ok(())
}

/// A blob frame whose header CRC and data checksum are internally consistent
/// but whose `real_val_len` field disagrees with the bytes actually stored
/// (`on_disk_val_len`, an uncompressed source) is REJECTED by the live blob
/// reader — salvage must not launder it: re-emitting through the writer would
/// restamp the frame with a consistent length and count a record the live
/// read path treats as corrupt as salvaged. It must drop through the
/// corrupt-record path instead.
#[test]
fn salvage_blob_file_drops_a_frame_with_a_forged_value_length() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    build_blob(&source, &fs, &[(b"k", b"vvvv"), (b"k2", b"second")])?;

    // Re-stamp the FIRST record's `real_val_len` (4 → 5) while the stored
    // bytes stay 4 (`on_disk_val_len` unchanged), with the header CRC and
    // data checksum recomputed so the frame stays internally consistent —
    // exactly the shape the scanner accepts and `Reader::get` rejects.
    // V4 frame layout from offset 0:
    //   magic 4 | checksum 16 | seqno 8 | key_len 2 | real_val_len 4 |
    //   on_disk_val_len 4 | header_crc 4 | key | value.
    let mut bytes = std::fs::read(&source)?;
    let seqno = {
        let Some(b) = bytes.get(20..28) else {
            panic!("seqno within the first frame");
        };
        u64::from_le_bytes(b.try_into().unwrap_or([0; 8]))
    };
    let new_hcrc = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&seqno.to_le_bytes());
        hasher.update(&1u16.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        hasher.update(&4u32.to_le_bytes());
        #[expect(
            clippy::cast_possible_truncation,
            reason = "intentionally truncated to the 4-byte header CRC"
        )]
        {
            hasher.digest() as u32
        }
    };
    let new_checksum = {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(b"kvvvv");
        hasher.update(&new_hcrc.to_le_bytes());
        hasher.digest128()
    };
    let patch = |bytes: &mut Vec<u8>, range: core::ops::Range<usize>, val: &[u8]| {
        let Some(slot) = bytes.get_mut(range) else {
            panic!("patch range within the first frame");
        };
        slot.copy_from_slice(val);
    };
    patch(&mut bytes, 4..20, &new_checksum.to_le_bytes());
    patch(&mut bytes, 30..34, &5u32.to_le_bytes());
    patch(&mut bytes, 38..42, &new_hcrc.to_le_bytes());
    std::fs::write(&source, &bytes)?;

    let report = salvage_blob_file(&source, dest.clone(), &fs, 0)?;
    assert_eq!(
        report.dropped.len(),
        1,
        "the forged-length frame drops as corrupt: {report:?}",
    );
    assert_eq!(
        report.records_salvaged, 1,
        "the record after the malformed frame is still recovered",
    );
    let recovered = scan_blob(&dest, &fs)?;
    assert_eq!(
        recovered,
        vec![(b"k2".to_vec(), b"second".to_vec())],
        "the salvaged copy holds exactly the healthy record",
    );
    Ok(())
}

/// A compressed blob source is rejected (fail-closed): the scanner yields on-disk
/// compressed bytes that this path cannot faithfully re-emit yet.
#[cfg(feature = "lz4")]
#[test]
fn salvage_blob_file_rejects_a_compressed_source() -> crate::Result<()> {
    let dir = tempdir()?;
    let source = dir.path().join("blob_source");
    let dest = dir.path().join("blob_salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);

    {
        let mut writer =
            BlobWriter::new(&source, 0, 0, &*fs)?.use_compression(crate::CompressionType::Lz4);
        writer.write(b"k0", 0, b"some compressible value aaaaaaaaaaaaaaaa")?;
        writer.finish()?;
    }

    assert!(
        matches!(
            salvage_blob_file(&source, dest, &fs, 0),
            Err(crate::Error::FeatureUnsupported(_)),
        ),
        "a compressed blob file must be rejected rather than mis-salvaged",
    );
    Ok(())
}

/// A columnar source carrying a per-field value sub-column salvages into a copy
/// that KEEPS the sub-column (verbatim `ColumnBatch` re-emit), instead of
/// collapsing it into a single value column via a row round-trip.
#[cfg(feature = "columnar")]
#[test]
fn salvage_preserves_columnar_value_subcolumns() -> crate::Result<()> {
    use crate::table::columnar::{Column, TypeTag, entries_to_column_batch};

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let cmp = default_comparator();

    // Two columnar blocks whose value is a single fixed-4 sub-column (id 3),
    // written verbatim through the ingest batch path (per-row seqno 0).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true);
    for block in 0..2u32 {
        let entries: Vec<InternalValue> = (0..4u32)
            .map(|i| {
                let k = format!("k{:04}", block * 4 + i);
                InternalValue::from_components(k.into_bytes(), b"x".to_vec(), 0, ValueType::Value)
            })
            .collect();
        let mut batch = entries_to_column_batch(&entries)?;
        batch.columns.pop();
        let mut data = Vec::new();
        for i in 0..4u32 {
            data.extend_from_slice(&(block * 4 + i).to_le_bytes());
        }
        batch.columns.push(Column {
            column_id: 3,
            type_tag: TypeTag::Fixed(4),
            validity: None,
            data,
        });
        writer.write_columnar_batch(&batch, &cmp)?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "a healthy columnar SST drops nothing: {report:?}"
    );
    // No deletes + clean blocks: each columnar block is copied through verbatim,
    // which is exactly why the per-field sub-columns survive byte-for-byte.
    assert_eq!(
        report.blocks_copied_verbatim, report.blocks_salvaged,
        "clean columnar blocks are copied verbatim",
    );

    // Reopen and project sub-column 3 via the per-SST scan: it survives as a
    // sub-column. A row round-trip would have collapsed it into the value column.
    let recovered = open(dest, &fs)?;
    assert!(
        recovered.metadata.columnar,
        "the recovered copy stays columnar"
    );
    let batches = recovered.columnar_scan(&[3], None)?;
    let rows: u32 = batches.iter().map(|b| b.row_count).sum();
    assert_eq!(rows, 8, "every row's sub-column is recovered");
    assert!(
        batches
            .iter()
            .all(|b| b.columns.iter().all(|c| c.column_id == 3)),
        "the value sub-column (id 3) is preserved verbatim, not collapsed",
    );
    Ok(())
}

/// A columnar Page-ECC SST with a single-byte RS-recoverable fault in a data
/// block (no deletes): salvage recovers the block from parity and **re-encodes**
/// the healed batch rather than copying the faulty on-disk bytes verbatim, so the
/// recovered copy carries clean bytes. The clean block around it is still copied
/// verbatim.
#[cfg(all(feature = "columnar", feature = "page_ecc"))]
#[test]
fn salvage_reencodes_an_ecc_recovered_columnar_block() -> crate::Result<()> {
    use crate::table::block::{EccParams, Header};
    use crate::table::columnar::entries_to_column_batch;

    let dir = tempdir()?;
    let source = dir.path().join("source");
    let dest = dir.path().join("salvaged");
    let fs: Arc<dyn Fs> = Arc::new(StdFs);
    let cmp = default_comparator();

    // Two columnar blocks under RS(4,2) parity, no deletes (so the no-deletes
    // copy-through / recover path is taken, not the delete-masked one).
    let mut writer = Writer::new(source.clone(), 0, 0, Arc::clone(&fs))?
        .use_columnar(true)
        .use_zone_map(true)
        .use_ecc(Some(EccParams::RS_4_2));
    for block in 0..2u32 {
        let entries: Vec<InternalValue> = (0..4u32)
            .map(|i| {
                let k = format!("k{:04}", block * 4 + i);
                InternalValue::from_components(k.into_bytes(), b"x".to_vec(), 0, ValueType::Value)
            })
            .collect();
        let batch = entries_to_column_batch(&entries)?;
        writer.write_columnar_batch(&batch, &cmp)?;
    }
    assert!(
        writer.finish()?.is_some(),
        "source columnar SST is non-empty"
    );

    // Flip one byte of the first columnar data block (RS(4,2) recovers a single
    // byte error).
    let first_off = {
        let table = open(source.clone(), &fs)?;
        let Some(kh) = table.data_block_handles().find_map(Result::ok) else {
            panic!("a non-empty SST has at least one data block");
        };
        usize::try_from(*kh.as_ref().offset()).unwrap_or(usize::MAX)
    };
    let pos = first_off + Header::MIN_LEN + 3;
    let mut bytes = std::fs::read(&source)?;
    if let Some(b) = bytes.get_mut(pos) {
        *b ^= 0x80;
    }
    std::fs::write(&source, &bytes)?;

    let report = salvage_sst(&source, dest.clone(), &fs)?;
    assert!(
        report.is_complete(),
        "an RS-recoverable columnar block is healed, not dropped: {report:?}",
    );
    assert_eq!(
        report.blocks_salvaged, report.blocks_total,
        "every block is recovered",
    );
    // The recovered block was re-encoded (verbatim:None), so fewer verbatim copies
    // than salvaged blocks; the other (clean) block is copied verbatim.
    assert!(
        report.blocks_copied_verbatim < report.blocks_salvaged,
        "the ECC-recovered columnar block is re-encoded, not copied verbatim: {report:?}",
    );

    let recovered = open(dest, &fs)?;
    assert!(
        recovered.metadata.columnar,
        "the recovered copy stays columnar"
    );
    assert_eq!(recovered.metadata.item_count, 8, "every row is recovered");
    Ok(())
}
