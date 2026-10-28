use super::*;
use crate::sfa::toc::reader::TocReader;
use crate::sfa::trailer::reader::TrailerReader;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use test_log::test;

/// An in-memory file that counts the calls a reader or writer makes on it:
/// each is a system call on a real, unbuffered file.
#[derive(Default)]
struct Counting {
    file: Cursor<Vec<u8>>,
    reads: usize,
    seeks: usize,
}

impl Read for Counting {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reads += 1;
        self.file.read(buf)
    }
}

impl Write for Counting {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for Counting {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.seeks += 1;
        self.file.seek(pos)
    }
}

/// An archive of `sections` sections, written as a table writes one: through
/// a checksumming writer.
fn archive(sections: usize) -> crate::sfa::Result<Counting> {
    let mut writer =
        Writer::from_writer(crate::checksum::ChecksummedWriter::new(Counting::default()));
    for i in 0..sections {
        writer.start(format!("section{i}").as_str())?;
        writer.write_all(b"payload")?;
    }
    let mut file = std::mem::take(writer.into_inner()?.inner_mut());
    file.reads = 0;
    file.seeks = 0;
    Ok(file)
}

/// Writing an archive asks the file for no position: the writer counts the
/// bytes it wrote. Guards against a seek per section, which on a buffered
/// file is also a flush.
#[test]
fn writing_an_archive_seeks_nothing() -> crate::sfa::Result<()> {
    let mut writer =
        Writer::from_writer(crate::checksum::ChecksummedWriter::new(Counting::default()));
    for i in 0..50 {
        writer.start(format!("section{i}").as_str())?;
        writer.write_all(b"payload")?;
    }
    let file = std::mem::take(writer.into_inner()?.inner_mut());
    assert_eq!(file.seeks, 0);
    Ok(())
}

/// Opening an archive reads its trailer and its table of contents in one read
/// each, however many sections it holds. Guards against a read per field,
/// which costs a system call per section on every table open.
#[test]
fn opening_an_archive_reads_its_trailer_and_toc_once_each() -> crate::sfa::Result<()> {
    for sections in [5, 50] {
        let mut file = archive(sections)?;
        let reader = crate::sfa::Reader::from_reader(&mut file)?;
        assert_eq!(reader.toc().len(), sections);
        assert_eq!(file.reads, 2, "{sections} sections");
        assert_eq!(file.seeks, 2, "{sections} sections");
    }
    Ok(())
}

/// A trailer whose table of contents runs past the end of the file is
/// refused before a buffer that long is allocated. Guards against a forged
/// length forcing an allocation of up to 16 EiB.
#[test]
fn a_toc_past_the_end_of_the_file_is_refused() -> crate::sfa::Result<()> {
    let mut file = archive(3)?;
    let bytes = file.file.get_mut();
    // The trailer ends in the TOC's length.
    let at = bytes.len() - 8;
    let forged = u64::MAX.to_le_bytes();
    bytes
        .get_mut(at..at + 8)
        .expect("the trailer ends in the TOC length")
        .copy_from_slice(&forged);
    assert!(crate::sfa::Reader::from_reader(&mut file).is_err());
    Ok(())
}

/// The length a table budgets for an archive's table of contents and trailer
/// is exactly what the writer appends after the sections.
#[test]
fn toc_and_trailer_len_matches_the_written_archive() -> crate::sfa::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("file.sfa");
    let sections: [(&str, &[u8]); 3] =
        [("a", b"one"), ("bc", b"three"), ("def", b"fifteen bytes!!")];

    let mut file = File::create(&path)?;
    let mut writer = Writer::from_writer(&mut file);
    for (name, data) in sections {
        writer.start(name)?;
        writer.write_all(data)?;
    }
    writer.finish()?;
    file.sync_all()?;
    drop(file);

    let data: usize = sections.iter().map(|(_, data)| data.len()).sum();
    let names = sections.map(|(name, _)| name);
    assert_eq!(
        std::fs::metadata(&path)?.len(),
        (data + crate::sfa::toc_and_trailer_len(&names)) as u64,
    );
    Ok(())
}

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

    let toc = TocReader::from_reader(&mut reader, &trailer)?;
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

    let toc = TocReader::from_reader(&mut reader, &trailer)?;
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

    let toc = TocReader::from_reader(&mut reader, &trailer)?;
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
