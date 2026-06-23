use super::*;
use crate::sfa::toc::reader::TocReader;
use crate::sfa::trailer::reader::TrailerReader;
use std::fs::File;
use std::io::Write;
use test_log::test;

#[test]
fn writer_empty() -> crate::sfa::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file.sfa");

    let mut file = File::create(&path)?;
    let writer = Writer::from_writer(&mut file);
    writer.finish()?;
    file.sync_all()?;
    drop(file);

    let mut reader = File::open(&path)?;
    let trailer = TrailerReader::from_reader(&mut reader)?;
    assert_eq!(0, trailer.toc_pos);

    let toc = TocReader::from_reader(
        &mut reader,
        trailer.toc_pos,
        trailer.toc_len,
        trailer.toc_checksum,
    )?;
    assert_eq!(0, toc.len());
    assert!(toc.is_empty());
    assert!(toc.section(b"hello").is_none());

    Ok(())
}

#[test]
fn writer_simple() -> crate::sfa::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file.sfa");

    let data = b"hello world";

    let mut file = File::create(&path)?;
    let mut writer = Writer::from_writer(&mut file);
    writer.write_all(data)?;
    writer.finish()?;
    file.sync_all()?;
    drop(file);

    let mut reader = File::open(&path)?;
    let trailer = TrailerReader::from_reader(&mut reader)?;
    assert_eq!(data.len() as u64, trailer.toc_pos);

    let toc = TocReader::from_reader(
        &mut reader,
        trailer.toc_pos,
        trailer.toc_len,
        trailer.toc_checksum,
    )?;
    assert_eq!(1, toc.len());
    assert!(toc.section(b"hello").is_none());
    assert!(toc.section(b"").is_some());

    assert_eq!(0, toc[0].pos);
    assert_eq!(data.len() as u64, toc[0].len);
    assert_eq!(&[] as &[u8], &*toc[0].name);

    Ok(())
}

#[test]
fn writer_multiple_sections() -> crate::sfa::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file.sfa");

    let data = b"hello world";
    let data2 = b"hello world2";
    let data3 = b"hello world3";

    let mut file = File::create(&path)?;
    let mut writer = Writer::from_writer(&mut file);
    writer.write_all(data)?;
    writer.start("section1")?;
    writer.write_all(data2)?;
    writer.start("section2")?;
    writer.write_all(data3)?;
    writer.finish()?;
    file.sync_all()?;
    drop(file);

    let mut reader = File::open(&path)?;
    let trailer = TrailerReader::from_reader(&mut reader)?;
    assert_eq!(
        data.len() as u64 + data2.len() as u64 + data3.len() as u64,
        trailer.toc_pos,
    );

    let toc = TocReader::from_reader(
        &mut reader,
        trailer.toc_pos,
        trailer.toc_len,
        trailer.toc_checksum,
    )?;
    assert_eq!(3, toc.len());
    assert!(toc.section(b"hello").is_none());
    assert!(toc.section(b"").is_some());
    assert!(toc.section(b"section1").is_some());
    assert!(toc.section(b"section2").is_some());

    assert_eq!(0, toc[0].pos);
    assert_eq!(data.len() as u64, toc[0].len());

    assert_eq!(&[] as &[u8], &*toc[0].name);
    assert_eq!(b"section1", &*toc[1].name);
    assert_eq!(b"section2", &*toc[2].name);

    assert_eq!(data.len() as u64, toc[0].len);
    assert_eq!(data2.len() as u64, toc[1].len);
    assert_eq!(data3.len() as u64, toc[2].len);

    Ok(())
}
