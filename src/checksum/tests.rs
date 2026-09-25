use super::{Checksum, ChecksummedWriter};
use std::io::Write;

/// A writer that takes at most one byte per call, as a short write may.
struct OneByteAtATime(Vec<u8>);

impl Write for OneByteAtATime {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(&byte) = buf.first() else {
            return Ok(0);
        };
        self.0.push(byte);
        Ok(1)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A short write is retried from where it stopped, so the checksum and the
/// position must count each byte once: hashing the whole buffer on every call
/// would hash a retried tail again, and the checksum would describe bytes the
/// stream does not hold.
#[test]
fn a_short_write_is_hashed_and_counted_once() -> std::io::Result<()> {
    let mut writer = ChecksummedWriter::new(OneByteAtATime(Vec::new()));
    writer.write_all(b"context")?;
    assert_eq!(writer.inner_mut().0, b"context");
    assert_eq!(
        writer.checksum(),
        Checksum::from_raw(crate::hash::hash128(b"context")),
    );
    assert_eq!(writer.position(), 7);
    Ok(())
}
