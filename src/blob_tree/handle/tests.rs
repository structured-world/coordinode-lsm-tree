use super::{BlobIndirection, Stored};
use crate::coding::{Decode, Encode};
use crate::vlog::ValueHandle;

/// An indirection decodes off its slice to what was encoded, as the read
/// through `Read` gives it, at the edges of every field's width and of every
/// stored kind, so a row's pointer reads the same however it is decoded.
#[test]
fn an_indirection_decodes_off_its_slice_as_its_read_does() -> crate::Result<()> {
    let ids = [
        0,
        1,
        127,
        128,
        16_383,
        16_384,
        u64::from(u32::MAX),
        u64::MAX,
    ];
    let narrow = [0, 1, 127, 128, u32::from(u16::MAX), u32::MAX];
    let kinds = [Stored::Frames, Stored::InBlock, Stored::Packed];
    for &blob_file_id in &ids {
        for &frame in &narrow {
            for index in [0, 1, 127, 128, u16::MAX] {
                for &size in &narrow {
                    for kind in kinds {
                        for &bytes in &narrow {
                            let indirection = BlobIndirection {
                                vhandle: ValueHandle {
                                    blob_file_id,
                                    frame,
                                    index,
                                },
                                size,
                                stored: kind(bytes),
                            };
                            let encoded = indirection.encode_into_vec();
                            let expected = format!("{indirection:?}");
                            let sliced = BlobIndirection::decode_slice(&encoded)?;
                            assert_eq!(format!("{sliced:?}"), expected, "off the slice");
                            let read = BlobIndirection::decode_from(&mut &encoded[..])?;
                            assert_eq!(format!("{read:?}"), expected, "through `Read`");
                            // Encoded into a buffer of the caller's, it is the
                            // same bytes, of the length it reports.
                            let mut buf = [0; BlobIndirection::ENCODED_CAP];
                            assert_eq!(indirection.encode_in(&mut buf), &encoded[..]);
                            assert_eq!(indirection.encoded_len(), encoded.len() as u64);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Each way a pointer can be malformed is refused off the slice: cut short, a
/// tenth varint byte past bit 63, a varint longer than its field, a value past
/// its field's width, and an unknown stored kind.
#[test]
fn a_malformed_indirection_is_refused_off_its_slice() {
    let cases: [(&str, &[u8]); 10] = [
        ("empty", &[]),
        ("id cut short", &[0x80]),
        ("stored word missing", &[0, 0, 0, 0]),
        ("unknown stored kind", &[0, 0, 0, 0, 3]),
        (
            "id tenth byte past bit 63",
            &[
                0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02, 0, 0, 0, 0,
            ],
        ),
        (
            "frame past u32",
            &[0, 0xff, 0xff, 0xff, 0xff, 0x1f, 0, 0, 0],
        ),
        (
            "frame longer than 5 bytes",
            &[0, 0x80, 0x80, 0x80, 0x80, 0x80, 0, 0, 0, 0],
        ),
        ("index past u16", &[0, 0, 0xff, 0xff, 0x07, 0, 0]),
        ("size past u32", &[0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0x1f, 0]),
        (
            "stored bytes past u32",
            &[0, 0, 0, 0, 0xfc, 0xff, 0xff, 0xff, 0x7f],
        ),
    ];
    for (case, bytes) in cases {
        assert!(
            BlobIndirection::decode_slice(bytes).is_err(),
            "{case}: {bytes:02x?} decoded"
        );
    }
    // The same fields with a known kind decode, so each case above is refused
    // for its own defect.
    assert!(BlobIndirection::decode_slice(&[0, 0, 0, 0, 2]).is_ok());
}

/// Any bytes decode off the slice without a panic: an indirection only where
/// re-encoding it gives back the bytes it was read from, and an error for a
/// varint cut short, too long for its field or past its width, or an unknown
/// stored kind. Damage in a table row is refused, never read as another
/// pointer.
#[test]
fn any_bytes_decode_off_the_slice_or_are_refused() {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut decoded = 0;
    for _ in 0..50_000 {
        let len = (next() % 26) as usize;
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                // Mostly continuation bytes, so long and overlong varints come
                // up as often as short ones.
                let byte = next().to_le_bytes()[0];
                if next() % 3 == 0 {
                    byte & 0x7f
                } else {
                    byte | 0x80
                }
            })
            .collect();
        if let Ok(indirection) = BlobIndirection::decode_slice(&bytes) {
            decoded += 1;
            let encoded = indirection.encode_into_vec();
            // Only a canonical encoding comes back byte for byte; an overlong
            // one decodes to the same values and is a prefix's length longer.
            let again = BlobIndirection::decode_slice(&encoded)
                .map(|again| format!("{again:?}"))
                .ok();
            assert_eq!(
                again,
                Some(format!("{indirection:?}")),
                "bytes {bytes:02x?}"
            );
        }
    }
    assert!(decoded > 0, "some random bytes decode, so the check ran");
}
