//! The blob file sweep: each corpus written into a blob tree under each layout
//! and codec, and what that layout costs measured on it.
//!
//! A layout is the block target (values below it share a compressed block),
//! the chunk size (values above it are split into chunks) and the size classes
//! a zstd dictionary compresses. The `per-value` layout writes every value as a
//! frame of its own and splits none, which is what a blob file held before
//! values were stored by size class: the baseline the others are read against.
//!
//! For every run the sweep reports, per corpus:
//!
//! - the compression ratio: blob file bytes over value bytes;
//! - the write time and the peak heap of the write;
//! - a full scan, and point reads of every key in a shuffled order, once with
//!   a cache far smaller than the data and once with one it fits in;
//! - 4 KiB range reads of the values above 1 MiB, with the bytes each decodes,
//!   and the peak heap of a whole read and of a range read of the largest one;
//! - the write amplification of collection: three rounds overwriting a third
//!   of the keys, each flushed and compacted, the blob bytes the compactions
//!   relocated over the blob bytes they removed.

pub mod corpus;
pub mod run;

/// What a run measures the heap with: the global allocator of the binary, or
/// nothing in a test.
pub trait HeapProbe {
    /// Starts a window: the peak is measured from the heap in use now.
    fn reset(&self);
    /// The most bytes the heap held above the start of the window, `None`
    /// when nothing measures it.
    fn peak(&self) -> Option<u64>;
}

/// A probe that measures nothing.
pub struct NoProbe;

impl HeapProbe for NoProbe {
    fn reset(&self) {}

    fn peak(&self) -> Option<u64> {
        None
    }
}

/// An error of the sweep.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
