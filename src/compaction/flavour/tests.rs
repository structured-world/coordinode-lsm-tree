use super::*;
use crate::{UserKey, UserValue, vlog::ValueHandle};

#[expect(clippy::unnecessary_wraps)]
fn entry(
    blob_file_id: BlobFileId,
    key: &[u8],
    offset: u64,
) -> crate::Result<(ScanEntry, BlobFileId)> {
    Ok((
        ScanEntry {
            key: UserKey::from(key),
            offset,
            seqno: 0,
            uncompressed_len: 0,
            value: UserValue::empty(),
            // These fixtures use ordinal offsets (0, 1, 2, ...), not real byte
            // positions, so the frame ends one ordinal past its start — never
            // at the start itself, which would mislead any future test that
            // inspects the consumed-frontier argument.
            frame_end: offset + 1,
        },
        blob_file_id,
    ))
}

#[test]
fn drain_blobs_simple() -> crate::Result<()> {
    let mut iter = [
        entry(0, b"a", 0),
        entry(0, b"a", 1),
        entry(0, b"a", 2),
        entry(0, b"a", 3),
        entry(0, b"a", 4),
    ]
    .into_iter()
    .peekable();

    drain_blobs(
        &mut iter,
        b"a",
        &BlobIndirection {
            size: 0,
            vhandle: ValueHandle {
                blob_file_id: 0,
                offset: 4,
                on_disk_size: 0,
            },
        },
        &mut |_, _| {},
    )?;

    assert_eq!(entry(0, b"a", 4)?, iter.next().unwrap()?);

    Ok(())
}

#[test]
fn drain_blobs_multiple_keys() -> crate::Result<()> {
    let mut iter = [
        entry(0, b"a", 0),
        entry(0, b"b", 0),
        entry(0, b"c", 0),
        entry(0, b"d", 0),
        entry(0, b"e", 0),
    ]
    .into_iter()
    .peekable();

    drain_blobs(
        &mut iter,
        b"e",
        &BlobIndirection {
            size: 0,
            vhandle: ValueHandle {
                blob_file_id: 0,
                offset: 0,
                on_disk_size: 0,
            },
        },
        &mut |_, _| {},
    )?;

    assert_eq!(entry(0, b"e", 0)?, iter.next().unwrap()?);

    Ok(())
}
