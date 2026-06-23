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
