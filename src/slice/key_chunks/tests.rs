use super::KeyChunks;

fn key(i: u32, len: usize) -> Vec<u8> {
    let mut key = vec![b'p'; len.saturating_sub(4)];
    key.extend_from_slice(&i.to_be_bytes());
    key
}

/// Keys of every size, short enough to sit inline, sharing chunks or too long
/// for one, read back as they were fused, past the chunks they rolled over and
/// past the writer itself: each view keeps its chunk alive.
#[test]
fn every_key_outlives_the_chunks_and_the_writer() {
    let mut chunks = KeyChunks::new();
    let mut held = Vec::new();
    for i in 0..20_000_u32 {
        let len = [8, 20, 21, 32, 300, 4_095, 4_096, 4_097, 9_000][i as usize % 9];
        let bytes = key(i, len);
        let (left, right) = bytes.split_at(len / 3);
        held.push((chunks.fuse(left, right), bytes));
    }
    drop(chunks);
    for (view, bytes) in &held {
        assert_eq!(&**view, bytes.as_slice());
    }
    // Keys dropped from the front leave the later ones of a chunk readable.
    held.drain(..10_000);
    for (view, bytes) in &held {
        assert_eq!(&**view, bytes.as_slice());
    }
}

/// A key cloned out of a chunk reads the same after the original is gone.
#[test]
fn a_clone_keeps_the_chunk_alive() {
    let mut chunks = KeyChunks::new();
    let bytes = key(7, 64);
    let view = chunks.fuse(&bytes[..10], &bytes[10..]);
    let clone = view.clone();
    drop(view);
    drop(chunks);
    assert_eq!(&*clone, bytes.as_slice());
}

/// A writer that fused nothing, or only keys that stay inline, holds no chunk
/// to give back.
#[test]
fn a_writer_with_no_chunk_drops_cleanly() {
    drop(KeyChunks::new());
    let mut chunks = KeyChunks::new();
    assert_eq!(&*chunks.fuse(b"ab", b"cd"), b"abcd");
}
