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
    let (_, toc_len) = bytes.split_at_mut(at);
    toc_len.copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(crate::sfa::Reader::from_reader(&mut file).is_err());
    Ok(())
}

/// A file of `len` bytes that stores only its head and its trailer; the bytes
/// between read as zeros. Counts the bytes it hands out.
struct Sparse {
    head: Vec<u8>,
    trailer: Vec<u8>,
    len: u64,
    pos: u64,
    served: u64,
}

impl Read for Sparse {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let trailer_pos = self.len - self.trailer.len() as u64;
        let (source, start, end): (&[u8], u64, u64) = if self.pos < self.head.len() as u64 {
            (&self.head, 0, self.head.len() as u64)
        } else if self.pos >= trailer_pos {
            (&self.trailer, trailer_pos, self.len)
        } else {
            (&[], self.pos, trailer_pos)
        };
        let n = buf
            .len()
            .min(usize::try_from(end - self.pos).unwrap_or(usize::MAX));
        let (out, _) = buf.split_at_mut(n);
        if source.is_empty() {
            out.fill(0);
        } else {
            let from = usize::try_from(self.pos - start).map_err(std::io::Error::other)?;
            out.copy_from_slice(&source[from..from + n]);
        }
        self.pos += n as u64;
        self.served += n as u64;
        Ok(n)
    }
}

impl Seek for Sparse {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = match pos {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(back) => self.len.checked_add_signed(back),
            SeekFrom::Current(by) => self.pos.checked_add_signed(by),
        }
        .ok_or_else(|| std::io::Error::other("a seek before the start"))?;
        Ok(self.pos)
    }
}

/// A trailer whose TOC length runs to the end of a huge file costs a read of
/// the TOC, not a buffer of the claimed length. Guards against a damaged
/// length aborting the open on an allocation the size of the file.
#[test]
fn a_toc_length_claiming_most_of_a_huge_file_reads_only_the_toc() -> crate::sfa::Result<()> {
    let bytes = archive(3)?.file.into_inner();
    let (head, trailer) = bytes.split_at(bytes.len() - crate::sfa::trailer::TRAILER_LEN);
    let mut trailer = trailer.to_vec();
    // The trailer ends in the TOC's position and length.
    let (_, fields) = trailer.split_at_mut(crate::sfa::trailer::TRAILER_LEN - 16);
    let (toc_pos, toc_len) = fields.split_at_mut(8);
    let toc_pos = u64::from_le_bytes(toc_pos.try_into().map_err(std::io::Error::other)?);
    let len: u64 = 1 << 44;
    toc_len
        .copy_from_slice(&(len - crate::sfa::trailer::TRAILER_LEN as u64 - toc_pos).to_le_bytes());

    let mut file = Sparse {
        head: head.to_vec(),
        trailer,
        len,
        pos: 0,
        served: 0,
    };
    let reader = crate::sfa::Reader::from_reader(&mut file)?;
    assert_eq!(reader.toc().len(), 3);
    assert!(file.served <= 8_192, "read {} bytes", file.served);
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
