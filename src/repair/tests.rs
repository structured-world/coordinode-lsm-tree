use super::{compute_table_checksum, highest_existing_version_id};
use crate::fs::StdFs;
use test_log::test;

#[test]
fn compute_table_checksum_matches_oneshot_xxh3() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("000007");
    // Larger than the 256 KiB read buffer so the chunked read loop is
    // exercised across multiple iterations.
    let payload: Vec<u8> = (0..600_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&path, &payload)?;

    let got = compute_table_checksum(&StdFs, &path)?;
    let expected = xxhash_rust::xxh3::xxh3_128(&payload);
    assert_eq!(
        got, expected,
        "streamed digest must equal the one-shot xxh3-128 digest",
    );
    Ok(())
}

#[test]
fn highest_existing_version_id_picks_the_max_and_ignores_non_versions() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    for name in ["v2", "v10", "v3", "current", "vNaN", "notaversion"] {
        std::fs::write(dir.path().join(name), b"x")?;
    }
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, Some(10));
    Ok(())
}

#[test]
fn highest_existing_version_id_none_when_no_versions_present() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("current"), b"x")?;
    assert_eq!(highest_existing_version_id(&StdFs, dir.path())?, None);
    Ok(())
}

/// `repair_with_salvage` recovers an SST that normal recovery refuses: a
/// columnar segment whose delete-bitmap section is corrupt fails whole-file
/// recovery (it would resurrect deleted rows), but salvage degrades it to "all
/// rows live" and the rebuilt manifest references the recovered table.
#[cfg(feature = "columnar")]
#[test]
fn repair_with_salvage_recovers_a_corrupt_delete_bitmap_sst() -> crate::Result<()> {
    use crate::config::DeleteStrategy;
    use crate::table::Writer;
    use crate::{Config, InternalValue, SequenceNumberCounter, ValueType};
    use std::sync::Arc;

    let dir = tempfile::tempdir()?;
    let tables = dir.path().join("tables");
    std::fs::create_dir_all(&tables)?;
    let sst = tables.join("0");
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(StdFs);

    // A columnar SST (table id 0) carrying a delete-bitmap.
    let n = 200u32;
    {
        let mut w = Writer::new(sst.clone(), 0, 0, Arc::clone(&fs))?
            .use_columnar(true)
            .use_zone_map(true)
            .delete_strategy(DeleteStrategy::MergeOnRead);
        for i in 0..n {
            w.write(InternalValue::from_components(
                format!("k{i:05}").into_bytes(),
                format!("v{i}").into_bytes(),
                1,
                ValueType::Value,
            ))?;
        }
        for pos in [5u32, 50, 150] {
            w.delete_bitmap_mut().insert(pos);
        }
        assert!(w.finish()?.is_some(), "the SST is non-empty");
    }

    // Corrupt the middle of the delete_bitmap section so normal recovery refuses
    // the SST (the data blocks stay intact).
    let (pos, len) = {
        let mut f = std::fs::File::open(&sst)?;
        let reader = match crate::sfa::Reader::from_reader(&mut f) {
            Ok(r) => r,
            Err(e) => panic!("reading the SFA trailer failed: {e:?}"),
        };
        let Some(entry) = reader.toc().iter().find(|e| e.name() == b"delete_bitmap") else {
            panic!("the SST must carry a delete_bitmap section");
        };
        (entry.pos(), entry.len())
    };
    let flip = usize::try_from(pos + len / 2).unwrap_or(0);
    let mut bytes = std::fs::read(&sst)?;
    if let Some(b) = bytes.get_mut(flip) {
        *b ^= 0xFF;
    }
    std::fs::write(&sst, &bytes)?;

    let report = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .repair_with_salvage(true)?;
    assert_eq!(
        report.salvaged, 1,
        "the corrupt-bitmap SST is salvaged: {:?}",
        report.unreadable_files,
    );
    assert_eq!(report.recovered, 1, "the salvaged table joins the manifest");
    Ok(())
}
