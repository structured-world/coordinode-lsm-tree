// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::sfa::Checksum;

pub struct ChecksummedWriter<W: crate::io::Write> {
    inner: W,
    hasher: xxhash_rust::xxh3::Xxh3Default,
}

impl<W: crate::io::Write> ChecksummedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: writer,
            hasher: xxhash_rust::xxh3::Xxh3Default::new(),
        }
    }

    pub fn checksum(&self) -> Checksum {
        Checksum::from_raw(self.hasher.digest128())
    }
}

// `crate::io::Write` is the std trait (via the std-mode supertrait blanket)
// under `std` and the native trait under `no_std`; the impls differ only in
// the trait path and `Result` type, so each is gated to its build.
#[cfg(feature = "std")]
impl<W: crate::io::Write> std::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Hash only the bytes the inner writer actually accepted.
        // A short write (`n < buf.len()`) followed by a retry would
        // otherwise hash the unwritten tail twice — once on this
        // call (`buf`), once on the retry (`&buf[n..]`) — silently
        // diverging the running digest from the on-disk content.
        // `inner.write` first so a failed write does not corrupt
        // the hasher state.
        let n = self.inner.write(buf)?;
        // Safe slice: `n <= buf.len()` per the `Write::write` contract.
        #[expect(
            clippy::indexing_slicing,
            reason = "n bounded by buf.len() per Write::write contract"
        )]
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

#[cfg(not(feature = "std"))]
impl<W: crate::io::Write> crate::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> crate::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> crate::io::Result<usize> {
        // See the std impl above: hash only accepted bytes, inner.write
        // first so a failed write cannot corrupt the hasher state.
        let n = self.inner.write(buf)?;
        #[expect(
            clippy::indexing_slicing,
            reason = "n bounded by buf.len() per Write::write contract"
        )]
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}
