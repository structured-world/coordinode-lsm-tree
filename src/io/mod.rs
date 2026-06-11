// Local I/O trait surface, mirroring `std::io::{Read, Write, Seek}`
// so that THE BOUNDS on `fs::Fs` / `fs::FsFile` no longer carry
// `std::io::*` directly. The signatures match `std::io` exactly, so
// backends gated behind `#[cfg(feature = "std")]` (such as `std_fs`
// and `io_uring_fs`) keep using `std::io::*` internally — they just
// satisfy this crate's traits via the supertrait alias + blanket
// impls below.
//
// Scope of this module's contribution to the no-std epic (see #311):
// it removes `std::io::{Read, Write, Seek}` from the trait BOUNDS.
// The `fs` module still uses `std::io::Result` for return types and
// `&std::path::Path` for path arguments in `Fs` / `FsFile` method
// signatures — those migrate to `crate::io::Result<T>` and a
// `crate::path` equivalent in follow-up commits. Until both follow-
// ups land, `fs::*` does NOT yet compile under
// `--no-default-features --features alloc`.
//
// Why not pull in an external `core_io` / `core2` / `core3` /
// `embedded-io` crate: those add a maintainer dependency for what
// is ultimately three stable trait signatures plus an error type.
// The signatures here have not meaningfully changed since Rust 1.0,
// so maintenance is near zero, and we keep the no-std contract
// under our own control.

use alloc::boxed::Box;
use alloc::string::String;
use core::fmt;

/// Specialised `Result` for I/O operations on this crate's traits.
pub type Result<T> = core::result::Result<T, Error>;

/// Mirrors [`std::io::ErrorKind`] for the variants this crate actually
/// constructs. Kept in alphabetical order so additions stay easy to spot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Entity (file, directory, key, etc.) already exists.
    AlreadyExists,
    /// Broken pipe — the other end of a stream is closed.
    BrokenPipe,
    /// Operation attempted across distinct devices/filesystems.
    CrossesDevices,
    /// Operation was interrupted (`EINTR`-equivalent); usually retriable.
    Interrupted,
    /// Data being read does not match the expected format/schema.
    InvalidData,
    /// A function argument had an invalid value.
    InvalidInput,
    /// Entity was not found.
    NotFound,
    /// Catch-all for errors that don't fit any other variant.
    Other,
    /// Operation denied due to lack of permissions.
    PermissionDenied,
    /// Reader hit end-of-file before satisfying the request.
    UnexpectedEof,
    /// Operation is not supported on this platform / backend / build.
    Unsupported,
    /// `write` returned `Ok(0)` while bytes still needed to be
    /// written. Mirrors [`std::io::ErrorKind::WriteZero`] so callers
    /// can distinguish a stuck-writer short write from a generic
    /// [`Other`](Self::Other) failure.
    WriteZero,
}

impl ErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyExists => "entity already exists",
            Self::BrokenPipe => "broken pipe",
            Self::CrossesDevices => "cross-device link or rename",
            Self::Interrupted => "operation interrupted",
            Self::InvalidData => "invalid data",
            Self::InvalidInput => "invalid input parameter",
            Self::NotFound => "entity not found",
            Self::Other => "other error",
            Self::PermissionDenied => "permission denied",
            Self::UnexpectedEof => "unexpected end of file",
            Self::Unsupported => "unsupported",
            Self::WriteZero => "write returned 0 bytes",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// I/O error mirroring [`std::io::Error`].
///
/// Carries an [`ErrorKind`] plus an optional message string for
/// context. The rendered `Display` form is `"<kind>"` when no
/// message is attached, or `"<kind>: <message>"` when one is.
///
/// Under `feature = "std"`, the `From<std::io::Error>` bridge
/// below applies a tri-state message-attachment policy so the
/// rendered text matches the information density of the input
/// without paying a heap allocation for plain kind-only inputs:
///
/// 1. **std error carries context** (`raw_os_error.is_some()` OR
///    `get_ref().is_some()` — the canonical std discriminator
///    for "more than just a kind") — the std `Display` output is
///    captured as the message. The original OS / errno / path
///    text survives the conversion and appears after the kind
///    tag.
/// 2. **std error is plain kind-only AND we mapped the kind**
///    (`std::io::Error::from(ErrorKind::NotFound)` etc.) — no
///    message is attached. The kind tag already conveys the
///    information; capturing the std `Display` output would just
///    repeat it (`"entity not found: entity not found"`) and burn
///    a heap allocation on the hot path.
/// 3. **std error is plain kind-only but we did NOT map the
///    kind** (the `#[non_exhaustive]` `std::io::ErrorKind`
///    catch-all branch — e.g. `OutOfMemory` mapping to our
///    `ErrorKind::Other`) — the std `Display` output IS captured
///    so the user-visible discriminant isn't lost in the
///    `Other` bucket. Renders as `"other error: out of memory"`
///    rather than just `"other error"`.
pub struct Error {
    kind: ErrorKind,
    message: Option<Box<str>>,
}

impl Error {
    /// Construct an error with the given kind and a context message.
    ///
    /// Analogous to [`std::io::Error::new`] but intentionally
    /// narrower: this constructor takes a `String`-coercible
    /// message and stores it verbatim, where std's `new()`
    /// accepts `E: Into<Box<dyn std::error::Error + Send + Sync>>`
    /// and carries a chained source via `Error::source()`. This
    /// crate's error type has no source-chaining surface (and
    /// can't have one under `no_std + alloc` without an alloc
    /// dyn-trait shim), so an analogous "wrap an inner error"
    /// helper would be misleading; callers wanting the source
    /// payload of a std error use the `From<std::io::Error>`
    /// bridge below, which renders the std Display into the
    /// message field.
    pub fn new<M: Into<String>>(kind: ErrorKind, message: M) -> Self {
        Self {
            kind,
            message: Some(message.into().into_boxed_str()),
        }
    }

    /// Construct an error with only an [`ErrorKind`] (no message).
    /// Matches [`std::io::Error::from`] for `ErrorKind`.
    #[must_use]
    pub const fn from_kind(kind: ErrorKind) -> Self {
        Self {
            kind,
            message: None,
        }
    }

    /// Return the [`ErrorKind`] this error carries.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Construct an [`ErrorKind::Other`] error carrying `message`.
    /// Mirrors [`std::io::Error::other`].
    pub fn other<M: Into<String>>(message: M) -> Self {
        Self::new(ErrorKind::Other, message)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::from_kind(kind)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Error");
        dbg.field("kind", &self.kind);
        if let Some(msg) = &self.message {
            dbg.field("message", msg);
        }
        dbg.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(msg) => write!(f, "{}: {msg}", self.kind.as_str()),
            None => f.write_str(self.kind.as_str()),
        }
    }
}

// `core::error::Error` (stable since 1.81, MSRV here is 1.92) so this error
// is usable as a trait-object source under both `std` and `no_std`. Under
// `std`, `std::error::Error` re-exports `core::error::Error`, so this is the
// same impl std callers have always seen.
impl core::error::Error for Error {}

/// Bridge from `std::io::Error`. Maps the std `ErrorKind` to
/// this crate's [`ErrorKind`] when a variant exists for it, and
/// falls back to [`ErrorKind::Other`] for any std variant we
/// don't track (the std type is `#[non_exhaustive]`, so the
/// catch-all is required). The `Display` message is captured via
/// the tri-state policy documented on [`Error`]: kept for
/// contextful std errors and for unmapped kinds (so the
/// discriminant is preserved even in the `Other` bucket),
/// dropped for plain kind-only errors whose kind we DO map (to
/// avoid the redundant `"<kind>: <kind>"` render and the heap
/// allocation). Net effect: `?` from std-backed backends
/// propagates the original context to operators, but callers
/// inspecting `err.kind()` after the conversion should be aware
/// that an unknown std kind now reads as
/// `ErrorKind::Other` rather than the originating variant.
#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        let std_kind = err.kind();
        let (kind, kind_is_mapped) = match std_kind {
            std::io::ErrorKind::AlreadyExists => (ErrorKind::AlreadyExists, true),
            std::io::ErrorKind::BrokenPipe => (ErrorKind::BrokenPipe, true),
            std::io::ErrorKind::CrossesDevices => (ErrorKind::CrossesDevices, true),
            std::io::ErrorKind::Interrupted => (ErrorKind::Interrupted, true),
            std::io::ErrorKind::InvalidData => (ErrorKind::InvalidData, true),
            std::io::ErrorKind::InvalidInput => (ErrorKind::InvalidInput, true),
            std::io::ErrorKind::NotFound => (ErrorKind::NotFound, true),
            std::io::ErrorKind::PermissionDenied => (ErrorKind::PermissionDenied, true),
            std::io::ErrorKind::UnexpectedEof => (ErrorKind::UnexpectedEof, true),
            std::io::ErrorKind::Unsupported => (ErrorKind::Unsupported, true),
            std::io::ErrorKind::WriteZero => (ErrorKind::WriteZero, true),
            // `Other` is a mapped kind: a kind-only `Other` std error
            // would otherwise fall through to the unmapped branch and
            // attach Display ("other error"), producing
            // "other error: other error" on render plus a heap alloc.
            std::io::ErrorKind::Other => (ErrorKind::Other, true),
            _ => (ErrorKind::Other, false),
        };
        // Message-attachment policy:
        //
        // - If the std error carries actual context (an `errno`, a
        //   path / OS message, or a custom payload, detected by
        //   `raw_os_error.is_some() || get_ref().is_some()` — the
        //   canonical std-side discriminator for "this error
        //   carries more than just a kind"), preserve its Display
        //   output as our message so the OS-level detail survives.
        //
        // - If the std error is a plain kind-only one
        //   (`std::io::Error::from(ErrorKind::X)`) AND we mapped
        //   the kind, skip the message — our `Display` already
        //   prefixes the kind tag, so storing "entity not found"
        //   as the message would produce
        //   "entity not found: entity not found" on render AND
        //   burn an unnecessary heap allocation.
        //
        // - If the std error is kind-only but we DIDN'T map the
        //   kind (`std::io::ErrorKind` is `#[non_exhaustive]`, e.g.
        //   `OutOfMemory` / `ResourceBusy`), the user-visible
        //   discriminant is otherwise lost in our `Other` bucket.
        //   Preserve the std `Display` text in that case so an
        //   unmapped kind still renders something useful (e.g.
        //   "other error: out of memory") instead of just
        //   "other error".
        if err.raw_os_error().is_some() || err.get_ref().is_some() {
            Self::new(kind, alloc::format!("{err}"))
        } else if kind_is_mapped {
            Self::from_kind(kind)
        } else {
            Self::new(kind, alloc::format!("{err}"))
        }
    }
}

/// Bridge back to `std::io::Error` so existing call sites that consume
/// `std::io::Result<_>` (and where `From` is invoked via `?`) keep
/// compiling once their input switches to this module.
#[cfg(feature = "std")]
impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        let kind = match err.kind {
            ErrorKind::AlreadyExists => std::io::ErrorKind::AlreadyExists,
            ErrorKind::BrokenPipe => std::io::ErrorKind::BrokenPipe,
            ErrorKind::CrossesDevices => std::io::ErrorKind::CrossesDevices,
            ErrorKind::Interrupted => std::io::ErrorKind::Interrupted,
            ErrorKind::InvalidData => std::io::ErrorKind::InvalidData,
            ErrorKind::InvalidInput => std::io::ErrorKind::InvalidInput,
            ErrorKind::NotFound => std::io::ErrorKind::NotFound,
            ErrorKind::Other => std::io::ErrorKind::Other,
            ErrorKind::PermissionDenied => std::io::ErrorKind::PermissionDenied,
            ErrorKind::UnexpectedEof => std::io::ErrorKind::UnexpectedEof,
            ErrorKind::Unsupported => std::io::ErrorKind::Unsupported,
            ErrorKind::WriteZero => std::io::ErrorKind::WriteZero,
        };
        match err.message {
            Some(msg) => Self::new(kind, msg.into_string()),
            None => Self::from(kind),
        }
    }
}

/// Seek target, mirroring [`std::io::SeekFrom`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekFrom {
    /// Seek to an absolute offset from the start of the stream.
    Start(u64),
    /// Seek to an offset relative to the end of the stream.
    End(i64),
    /// Seek to an offset relative to the current cursor position.
    Current(i64),
}

#[cfg(feature = "std")]
impl From<SeekFrom> for std::io::SeekFrom {
    fn from(s: SeekFrom) -> Self {
        match s {
            SeekFrom::Start(n) => Self::Start(n),
            SeekFrom::End(n) => Self::End(n),
            SeekFrom::Current(n) => Self::Current(n),
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::SeekFrom> for SeekFrom {
    fn from(s: std::io::SeekFrom) -> Self {
        match s {
            std::io::SeekFrom::Start(n) => Self::Start(n),
            std::io::SeekFrom::End(n) => Self::End(n),
            std::io::SeekFrom::Current(n) => Self::Current(n),
        }
    }
}

// Under `feature = "std"`, the `Read` / `Write` / `Seek` traits in
// this module are supertrait aliases over `std::io::{Read,Write,
// Seek}`. Any `T: std::io::Read` automatically implements
// `crate::io::Read` via the blanket below — AND `T: crate::io::Read`
// implies `T: std::io::Read` (because crate::io::Read is a supertrait).
// This second direction is what lets `dyn FsFile` (bounded on
// `crate::io::Read`) flow into `std::io::BufReader`, `byteorder`,
// and the rest of the std ecosystem without any explicit adapter.
//
// Under `--no-default-features --features alloc`, std::io does not
// exist, so the traits are standalone with their own method bodies
// returning `crate::io::Result<T>` (the local Error type above).
// Signatures stay identical between modes so call sites compile in
// both builds.
//
// This dual-shape is the whole reason we own this module instead of
// depending on `core2` / `core3` / `embedded-io` — none of those let
// the std-mode trait *be* `std::io::Read` (they're meant to replace
// std::io, not bridge to it), and bridging the other way needs an
// adapter shim at every backend boundary.

/// Read trait. Under `std` it's a supertrait alias for
/// [`std::io::Read`]; under `no_std + alloc` it carries its own
/// signature mirroring the std contract.
#[cfg(feature = "std")]
pub trait Read: std::io::Read {}
#[cfg(feature = "std")]
impl<R: std::io::Read + ?Sized> Read for R {}

/// Write trait. Under `std` it's a supertrait alias for
/// [`std::io::Write`]; under `no_std + alloc` it carries its own
/// signature mirroring the std contract.
#[cfg(feature = "std")]
pub trait Write: std::io::Write {}
#[cfg(feature = "std")]
impl<W: std::io::Write + ?Sized> Write for W {}

/// Seek trait. Under `std` it's a supertrait alias for
/// [`std::io::Seek`]; under `no_std + alloc` it carries its own
/// signature mirroring the std contract.
#[cfg(feature = "std")]
pub trait Seek: std::io::Seek {}
#[cfg(feature = "std")]
impl<S: std::io::Seek + ?Sized> Seek for S {}

/// Buffered-read trait. Under `std` this is a supertrait alias of
/// [`std::io::BufRead`] (blanket-implemented), so std readers satisfy it
/// directly; under `no_std` it is the native trait defined below.
#[cfg(feature = "std")]
pub trait BufRead: std::io::BufRead {}
#[cfg(feature = "std")]
impl<B: std::io::BufRead + ?Sized> BufRead for B {}

/// Read trait mirroring [`std::io::Read`]. Only the methods this
/// crate depends on are surfaced; default implementations follow the
/// std contract verbatim so behaviour matches.
#[cfg(not(feature = "std"))]
pub trait Read {
    /// Read bytes into `buf`. Returns the number of bytes read. A
    /// return value of `0` indicates the source has reached EOF.
    ///
    /// # Errors
    ///
    /// Returns any I/O error encountered by the backing reader.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Read exactly `buf.len()` bytes. Returns
    /// [`ErrorKind::UnexpectedEof`] if EOF is reached before the
    /// buffer is full.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying reader, or
    /// [`ErrorKind::UnexpectedEof`] on short read.
    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    // `split_at_mut(n)` keeps the crate-level
                    // `#![deny(clippy::indexing_slicing)]` happy
                    // under `--no-default-features --features
                    // alloc` clippy: indexing form `buf = &mut
                    // buf[n..]` would lint-fail on the no-std
                    // build even though `n` is bounded by the
                    // `Ok(n)` return contract of `read()`.
                    let (_, rest) = buf.split_at_mut(n);
                    buf = rest;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if buf.is_empty() {
            Ok(())
        } else {
            // Match the stable message text `std::io::Read::read_exact`
            // emits on the same short-read condition — callers that
            // grep diagnostics for "failed to fill whole buffer" keep
            // working without a feature-conditional branch.
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        }
    }

    /// Adapter that reads at most `limit` bytes from this reader, mirroring
    /// [`std::io::Read::take`]. Used to bound a parser against a forged length
    /// prefix.
    fn take(self, limit: u64) -> Take<Self>
    where
        Self: Sized,
    {
        Take { inner: self, limit }
    }
}

/// Limit-bounded reader returned by [`Read::take`], mirroring
/// [`std::io::Take`].
#[cfg(not(feature = "std"))]
pub struct Take<R> {
    inner: R,
    limit: u64,
}

#[cfg(not(feature = "std"))]
impl<R: Read> Read for Take<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.limit == 0 {
            return Ok(0);
        }
        // Cap this read at the remaining limit; `min` keeps `max <= buf.len()`.
        let max = (buf.len() as u64).min(self.limit) as usize;
        // `split_at_mut` instead of `&mut buf[..max]` to satisfy the crate's
        // `deny(clippy::indexing_slicing)` on the no_std build.
        let (head, _) = buf.split_at_mut(max);
        let n = self.inner.read(head)?;
        self.limit -= n as u64;
        Ok(n)
    }
}

/// Buffered-read trait mirroring the [`std::io::BufRead`] subset the storage
/// layer uses (`fill_buf` + `consume`). Lets a record reader peek at the next
/// bytes — and detect a clean EOF via an empty fill — without consuming them.
#[cfg(not(feature = "std"))]
pub trait BufRead: Read {
    /// Return the buffer's current contents, refilling from the underlying
    /// reader when empty. An empty return means EOF.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying reader.
    fn fill_buf(&mut self) -> Result<&[u8]>;

    /// Mark `amt` bytes from the start of the buffer as consumed so they are
    /// not returned by the next [`fill_buf`](Self::fill_buf)/[`read`](Read::read).
    fn consume(&mut self, amt: usize);
}

/// Write trait mirroring [`std::io::Write`].
#[cfg(not(feature = "std"))]
pub trait Write {
    /// Write `buf` into the sink. Returns the number of bytes
    /// accepted by the writer in this call.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying writer.
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// Flush buffered output to the underlying medium.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying writer.
    fn flush(&mut self) -> Result<()>;

    /// Write the entire `buf`, retrying on `Interrupted` and
    /// returning [`ErrorKind::WriteZero`] if the writer stops
    /// accepting bytes early. Matches the semantics of
    /// [`std::io::Write::write_all`].
    ///
    /// # Errors
    ///
    /// Returns the underlying writer's error, or
    /// [`ErrorKind::WriteZero`] on short write.
    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => {
                    return Err(Error::new(
                        ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                Ok(n) => {
                    // `split_at(n)` keeps the crate-level
                    // `#![deny(clippy::indexing_slicing)]` happy
                    // under `--no-default-features --features
                    // alloc` clippy: same reasoning as the
                    // matching `read_exact` default impl above.
                    let (_, rest) = buf.split_at(n);
                    buf = rest;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// Seek trait mirroring [`std::io::Seek`].
#[cfg(not(feature = "std"))]
pub trait Seek {
    /// Seek the stream cursor to the offset described by `pos`.
    /// Returns the resulting absolute byte offset from the start of
    /// the stream.
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying seeker.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;

    /// Current stream position (offset from the start). Mirrors
    /// [`std::io::Seek::stream_position`].
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying seeker.
    fn stream_position(&mut self) -> Result<u64> {
        self.seek(SeekFrom::Current(0))
    }

    /// Seek relative to the current position. Mirrors
    /// [`std::io::Seek::seek_relative`].
    ///
    /// # Errors
    ///
    /// Returns any I/O error from the underlying seeker.
    fn seek_relative(&mut self, offset: i64) -> Result<()> {
        self.seek(SeekFrom::Current(offset))?;
        Ok(())
    }
}

// Blanket forwarding impls so readers/writers behind a reference or a box
// (e.g. `&mut R`, `Box<dyn FsFile>`) satisfy the io traits — std provides the
// equivalent blankets for its own io traits, so this only fills the no_std gap.
#[cfg(not(feature = "std"))]
impl<R: Read + ?Sized> Read for &mut R {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact(buf)
    }
}

#[cfg(not(feature = "std"))]
impl<R: Read + ?Sized> Read for alloc::boxed::Box<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact(buf)
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write + ?Sized> Write for &mut W {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (**self).write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (**self).write_all(buf)
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write + ?Sized> Write for alloc::boxed::Box<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (**self).write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (**self).write_all(buf)
    }
}

#[cfg(not(feature = "std"))]
impl<S: Seek + ?Sized> Seek for &mut S {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }
    fn stream_position(&mut self) -> Result<u64> {
        (**self).stream_position()
    }
    fn seek_relative(&mut self, offset: i64) -> Result<()> {
        (**self).seek_relative(offset)
    }
}

#[cfg(not(feature = "std"))]
impl<S: Seek + ?Sized> Seek for alloc::boxed::Box<S> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }
    fn stream_position(&mut self) -> Result<u64> {
        (**self).stream_position()
    }
    fn seek_relative(&mut self, offset: i64) -> Result<()> {
        (**self).seek_relative(offset)
    }
}

#[cfg(not(feature = "std"))]
impl<B: BufRead + ?Sized> BufRead for &mut B {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        (**self).fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        (**self).consume(amt);
    }
}

#[cfg(not(feature = "std"))]
impl<B: BufRead + ?Sized> BufRead for alloc::boxed::Box<B> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        (**self).fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        (**self).consume(amt);
    }
}

// `varint-rs` only blankets its traits over `std::io` (under its `std`
// feature), and a foreign trait cannot be blanket-impl'd here (orphan rule).
// So `no_std` builds get crate-local varint extension traits with the same
// method surface, blanketed over this module's `Read`/`Write`. The encoding is
// canonical unsigned LEB128 — byte-identical to `varint-rs` — so frames written
// under `std` and `no_std` interoperate. Call sites swap only the import path.
/// Writes unsigned integers as canonical LEB128 varints over a [`Write`].
///
/// The `no_std` counterpart of `varint-rs`'s `VarintWriter`; the encoding is
/// identical so frames interoperate across `std` / `no_std` builds.
#[cfg(not(feature = "std"))]
pub trait VarintWriter: Write {
    /// Writes `value` as canonical unsigned LEB128.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u64_varint(&mut self, mut value: u64) -> Result<()> {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.write_all(&[byte])?;
            if value == 0 {
                return Ok(());
            }
        }
    }

    /// Writes `value` as canonical unsigned LEB128.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u32_varint(&mut self, value: u32) -> Result<()> {
        self.write_u64_varint(u64::from(value))
    }

    /// Writes `value` as canonical unsigned LEB128.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u16_varint(&mut self, value: u16) -> Result<()> {
        self.write_u64_varint(u64::from(value))
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write + ?Sized> VarintWriter for W {}

/// Reads canonical LEB128 varints over a [`Read`].
///
/// The `no_std` counterpart of `varint-rs`'s `VarintReader`; the encoding is
/// identical so frames interoperate across `std` / `no_std` builds.
#[cfg(not(feature = "std"))]
pub trait VarintReader: Read {
    /// Reads a canonical unsigned LEB128 value.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on a truncated value, or
    /// [`ErrorKind::InvalidData`] on an overlong encoding; plus any underlying
    /// reader error.
    fn read_u64_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let mut byte = [0u8; 1];
            self.read_exact(&mut byte)?;
            // 10 groups of 7 bits cover a full u64; reject overlong encodings.
            if shift >= 64 {
                return Err(Error::new(ErrorKind::InvalidData, "varint overflows u64"));
            }
            result |= (u64::from(byte[0] & 0x7f)) << shift;
            if byte[0] & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
    }

    /// Reads a canonical unsigned LEB128 value, narrowed to `u32`.
    ///
    /// # Errors
    /// As [`Self::read_u64_varint`], plus [`ErrorKind::InvalidData`] if the
    /// value does not fit `u32`.
    fn read_u32_varint(&mut self) -> Result<u32> {
        let v = self.read_u64_varint()?;
        u32::try_from(v).map_err(|_| Error::new(ErrorKind::InvalidData, "varint exceeds u32"))
    }

    /// Reads a canonical unsigned LEB128 value, narrowed to `u16`.
    ///
    /// # Errors
    /// As [`Self::read_u64_varint`], plus [`ErrorKind::InvalidData`] if the
    /// value does not fit `u16`.
    fn read_u16_varint(&mut self) -> Result<u16> {
        let v = self.read_u64_varint()?;
        u16::try_from(v).map_err(|_| Error::new(ErrorKind::InvalidData, "varint exceeds u16"))
    }
}

#[cfg(not(feature = "std"))]
impl<R: Read + ?Sized> VarintReader for R {}

// ---------------------------------------------------------------------------
// Fixed-width integer I/O — a `no_std`-capable, drop-in replacement for
// `byteorder`'s `WriteBytesExt` / `ReadBytesExt` / `ByteOrder` over this
// module's `Read` / `Write` (which is `std::io` under `std` and the native
// traits under `no_std`). The API mirrors `byteorder` exactly — same
// `w.write_u32::<LittleEndian>(x)` / `r.read_u32::<LittleEndian>()` call
// shape — so migrating a wire-format module off `byteorder` is just an
// import swap, and the encoding is byte-identical (`to_le_bytes` /
// `to_be_bytes`). High-level round-trip tests cover correctness; no
// low-level duplication here.
// ---------------------------------------------------------------------------

/// Byte-order marker for fixed-width integer encoding.
///
/// Mirrors `byteorder::ByteOrder`. Implemented by [`LittleEndian`] /
/// [`BigEndian`]; methods convert between fixed-size byte arrays and integers
/// so call sites stay free of slice indexing.
pub trait ByteOrder {
    /// Decode a `u16` from its 2-byte representation.
    fn u16_from(b: [u8; 2]) -> u16;
    /// Decode a `u32` from its 4-byte representation.
    fn u32_from(b: [u8; 4]) -> u32;
    /// Decode a `u64` from its 8-byte representation.
    fn u64_from(b: [u8; 8]) -> u64;
    /// Decode a `u128` from its 16-byte representation.
    fn u128_from(b: [u8; 16]) -> u128;
    /// Encode a `u16` to its 2-byte representation.
    fn u16_to(n: u16) -> [u8; 2];
    /// Encode a `u32` to its 4-byte representation.
    fn u32_to(n: u32) -> [u8; 4];
    /// Encode a `u64` to its 8-byte representation.
    fn u64_to(n: u64) -> [u8; 8];
    /// Encode a `u128` to its 16-byte representation.
    fn u128_to(n: u128) -> [u8; 16];

    // Static slice helpers matching `byteorder::ByteOrder`'s API, so call
    // sites of the form `LittleEndian::write_u32(buf, n)` / `read_u32(buf)`
    // migrate unchanged. `split_at[_mut]` (not indexing) keeps the crate-level
    // `deny(indexing_slicing)` happy; like byteorder, they panic if `buf` is
    // shorter than the integer width (a caller bug, not a data condition).
    /// Read a `u16` from the first 2 bytes of `buf`.
    #[must_use]
    fn read_u16(buf: &[u8]) -> u16 {
        let (head, _) = buf.split_at(2);
        let mut a = [0u8; 2];
        a.copy_from_slice(head);
        Self::u16_from(a)
    }
    /// Read a `u32` from the first 4 bytes of `buf`.
    #[must_use]
    fn read_u32(buf: &[u8]) -> u32 {
        let (head, _) = buf.split_at(4);
        let mut a = [0u8; 4];
        a.copy_from_slice(head);
        Self::u32_from(a)
    }
    /// Read a `u64` from the first 8 bytes of `buf`.
    #[must_use]
    fn read_u64(buf: &[u8]) -> u64 {
        let (head, _) = buf.split_at(8);
        let mut a = [0u8; 8];
        a.copy_from_slice(head);
        Self::u64_from(a)
    }
    /// Write `n` into the first 2 bytes of `buf`.
    fn write_u16(buf: &mut [u8], n: u16) {
        let (head, _) = buf.split_at_mut(2);
        head.copy_from_slice(&Self::u16_to(n));
    }
    /// Write `n` into the first 4 bytes of `buf`.
    fn write_u32(buf: &mut [u8], n: u32) {
        let (head, _) = buf.split_at_mut(4);
        head.copy_from_slice(&Self::u32_to(n));
    }
    /// Write `n` into the first 8 bytes of `buf`.
    fn write_u64(buf: &mut [u8], n: u64) {
        let (head, _) = buf.split_at_mut(8);
        head.copy_from_slice(&Self::u64_to(n));
    }
}

/// Little-endian [`ByteOrder`] (matches `byteorder::LittleEndian`).
#[derive(Clone, Copy, Debug)]
pub enum LittleEndian {}
/// Big-endian [`ByteOrder`] (matches `byteorder::BigEndian`).
#[derive(Clone, Copy, Debug)]
pub enum BigEndian {}

/// Short alias for [`LittleEndian`] (matches `byteorder::LE`).
pub type LE = LittleEndian;
/// Short alias for [`BigEndian`] (matches `byteorder::BE`).
pub type BE = BigEndian;

impl ByteOrder for LittleEndian {
    fn u16_from(b: [u8; 2]) -> u16 {
        u16::from_le_bytes(b)
    }
    fn u32_from(b: [u8; 4]) -> u32 {
        u32::from_le_bytes(b)
    }
    fn u64_from(b: [u8; 8]) -> u64 {
        u64::from_le_bytes(b)
    }
    fn u128_from(b: [u8; 16]) -> u128 {
        u128::from_le_bytes(b)
    }
    fn u16_to(n: u16) -> [u8; 2] {
        n.to_le_bytes()
    }
    fn u32_to(n: u32) -> [u8; 4] {
        n.to_le_bytes()
    }
    fn u64_to(n: u64) -> [u8; 8] {
        n.to_le_bytes()
    }
    fn u128_to(n: u128) -> [u8; 16] {
        n.to_le_bytes()
    }
}

impl ByteOrder for BigEndian {
    fn u16_from(b: [u8; 2]) -> u16 {
        u16::from_be_bytes(b)
    }
    fn u32_from(b: [u8; 4]) -> u32 {
        u32::from_be_bytes(b)
    }
    fn u64_from(b: [u8; 8]) -> u64 {
        u64::from_be_bytes(b)
    }
    fn u128_from(b: [u8; 16]) -> u128 {
        u128::from_be_bytes(b)
    }
    fn u16_to(n: u16) -> [u8; 2] {
        n.to_be_bytes()
    }
    fn u32_to(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }
    fn u64_to(n: u64) -> [u8; 8] {
        n.to_be_bytes()
    }
    fn u128_to(n: u128) -> [u8; 16] {
        n.to_be_bytes()
    }
}

/// Fixed-width integer writes over [`Write`], mirroring
/// `byteorder::WriteBytesExt`. Blanket-implemented for every [`Write`].
pub trait WriteBytesExt: Write {
    /// Write a single byte.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u8(&mut self, n: u8) -> Result<()> {
        self.write_all(&[n])?;
        Ok(())
    }
    /// Write a signed byte.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_i8(&mut self, n: i8) -> Result<()> {
        self.write_all(&n.to_le_bytes())?;
        Ok(())
    }
    /// Write a `u16` in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u16<T: ByteOrder>(&mut self, n: u16) -> Result<()> {
        self.write_all(&T::u16_to(n))?;
        Ok(())
    }
    /// Write a `u32` in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u32<T: ByteOrder>(&mut self, n: u32) -> Result<()> {
        self.write_all(&T::u32_to(n))?;
        Ok(())
    }
    /// Write a `u64` in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u64<T: ByteOrder>(&mut self, n: u64) -> Result<()> {
        self.write_all(&T::u64_to(n))?;
        Ok(())
    }
    /// Write a `u128` in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_u128<T: ByteOrder>(&mut self, n: u128) -> Result<()> {
        self.write_all(&T::u128_to(n))?;
        Ok(())
    }
    /// Write an `f32` (IEEE-754 bit pattern) in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_f32<T: ByteOrder>(&mut self, n: f32) -> Result<()> {
        self.write_u32::<T>(n.to_bits())
    }
    /// Write an `f64` (IEEE-754 bit pattern) in the byte order `T`.
    ///
    /// # Errors
    /// Propagates the underlying writer's error.
    fn write_f64<T: ByteOrder>(&mut self, n: f64) -> Result<()> {
        self.write_u64::<T>(n.to_bits())
    }
}
impl<W: Write + ?Sized> WriteBytesExt for W {}

/// Fixed-width integer reads over [`Read`], mirroring
/// `byteorder::ReadBytesExt`. Blanket-implemented for every [`Read`].
pub trait ReadBytesExt: Read {
    /// Read a single byte.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.read_exact(&mut b)?;
        Ok(u8::from_le_bytes(b))
    }
    /// Read a signed byte.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_i8(&mut self) -> Result<i8> {
        let mut b = [0u8; 1];
        self.read_exact(&mut b)?;
        Ok(i8::from_le_bytes(b))
    }
    /// Read a `u16` in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_u16<T: ByteOrder>(&mut self) -> Result<u16> {
        let mut b = [0u8; 2];
        self.read_exact(&mut b)?;
        Ok(T::u16_from(b))
    }
    /// Read a `u32` in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_u32<T: ByteOrder>(&mut self) -> Result<u32> {
        let mut b = [0u8; 4];
        self.read_exact(&mut b)?;
        Ok(T::u32_from(b))
    }
    /// Read a `u64` in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_u64<T: ByteOrder>(&mut self) -> Result<u64> {
        let mut b = [0u8; 8];
        self.read_exact(&mut b)?;
        Ok(T::u64_from(b))
    }
    /// Read a `u128` in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_u128<T: ByteOrder>(&mut self) -> Result<u128> {
        let mut b = [0u8; 16];
        self.read_exact(&mut b)?;
        Ok(T::u128_from(b))
    }
    /// Read an `f32` (IEEE-754 bit pattern) in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_f32<T: ByteOrder>(&mut self) -> Result<f32> {
        Ok(f32::from_bits(self.read_u32::<T>()?))
    }
    /// Read an `f64` (IEEE-754 bit pattern) in the byte order `T`.
    ///
    /// # Errors
    /// [`ErrorKind::UnexpectedEof`] on short read, or the reader's error.
    fn read_f64<T: ByteOrder>(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.read_u64::<T>()?))
    }
}
impl<R: Read + ?Sized> ReadBytesExt for R {}

// `no_std` concrete impls so wire-format code that writes into a `Vec<u8>` or
// reads from a `&[u8]` keeps compiling once it moves off `byteorder` (under
// `std` these come from `std::io`'s own impls via the supertrait aliases).
#[cfg(not(feature = "std"))]
impl Write for alloc::vec::Vec<u8> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
}

#[cfg(not(feature = "std"))]
impl Read for &[u8] {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = buf.len().min(self.len());
        let (head, rest) = self.split_at(n);
        let (dst, _) = buf.split_at_mut(n);
        dst.copy_from_slice(head);
        *self = rest;
        Ok(n)
    }
}

// In-memory cursor, mirroring `crate::io::Cursor`. Under `std` it IS
// `crate::io::Cursor` (re-export); under `no_std` it's a local equivalent so
// wire-format code reading/seeking over a `&[u8]` or writing into a `Vec<u8>`
// keeps compiling. Same API surface (`new` / `position` / `set_position` /
// `into_inner` / `get_ref`) so call sites are identical across both builds.
#[cfg(feature = "std")]
pub use std::io::Cursor;

/// In-memory `Read` + `Seek` (and `Write` for `Vec` inner) cursor over a
/// byte buffer, mirroring [`crate::io::Cursor`] for `no_std` builds.
#[cfg(not(feature = "std"))]
pub struct Cursor<T> {
    inner: T,
    pos: u64,
}

#[cfg(not(feature = "std"))]
impl<T> Cursor<T> {
    /// Wraps `inner`, starting the cursor at position 0.
    pub const fn new(inner: T) -> Self {
        Self { inner, pos: 0 }
    }
    /// Current byte position.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.pos
    }
    /// Sets the byte position.
    pub const fn set_position(&mut self, pos: u64) {
        self.pos = pos;
    }
    /// Consumes the cursor, returning the wrapped buffer.
    pub fn into_inner(self) -> T {
        self.inner
    }
    /// Borrows the wrapped buffer.
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }
    /// Mutably borrows the wrapped buffer.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

#[cfg(not(feature = "std"))]
impl<T: AsRef<[u8]>> Read for Cursor<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let inner = self.inner.as_ref();
        // Position past end → 0 bytes (matches std).
        let start = (self.pos as usize).min(inner.len());
        let (_, remaining) = inner.split_at(start);
        let n = buf.len().min(remaining.len());
        let (src, _) = remaining.split_at(n);
        let (dst, _) = buf.split_at_mut(n);
        dst.copy_from_slice(src);
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(not(feature = "std"))]
impl<T: AsRef<[u8]>> Seek for Cursor<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let len = self.inner.as_ref().len() as u64;
        let new = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(off) => len.saturating_add_signed(off),
            SeekFrom::Current(off) => self.pos.saturating_add_signed(off),
        };
        self.pos = new;
        Ok(new)
    }
}

#[cfg(not(feature = "std"))]
impl Write for Cursor<alloc::vec::Vec<u8>> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // Like std: writing at `pos` overwrites in place and extends the Vec
        // when the write runs past the current end.
        let pos = self.pos as usize;
        if pos > self.inner.len() {
            self.inner.resize(pos, 0);
        }
        let end = pos + buf.len();
        if end > self.inner.len() {
            self.inner.resize(end, 0);
        }
        let (_, tail) = self.inner.split_at_mut(pos);
        let (dst, _) = tail.split_at_mut(buf.len());
        dst.copy_from_slice(buf);
        self.pos = end as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "std")]
pub use std::io::BufReader;

/// Buffering reader, mirroring the subset of [`std::io::BufReader`] the storage
/// layer uses (record framing reads), for `no_std` builds. Coalesces small
/// reads and exposes [`BufRead`] so a record parser can peek for a clean EOF.
#[cfg(not(feature = "std"))]
pub struct BufReader<R: Read> {
    inner: R,
    // `capacity`-sized; the valid (filled, not-yet-consumed) window is
    // `buf[pos..cap]`.
    buf: alloc::vec::Vec<u8>,
    pos: usize,
    cap: usize,
}

#[cfg(not(feature = "std"))]
impl<R: Read> BufReader<R> {
    /// Wraps `inner` with the default 8 KiB buffer.
    pub fn new(inner: R) -> Self {
        Self::with_capacity(8 * 1024, inner)
    }

    /// Wraps `inner` with a `capacity`-byte buffer.
    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner,
            buf: alloc::vec![0u8; capacity],
            pos: 0,
            cap: 0,
        }
    }

    /// Mutable access to the inner reader. Bypasses the buffer — mixing direct
    /// inner reads with buffered ones loses buffered bytes, like std.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Shared access to the inner reader.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }
}

#[cfg(not(feature = "std"))]
impl<R: Read> Read for BufReader<R> {
    fn read(&mut self, dest: &mut [u8]) -> Result<usize> {
        // A read at least as large as the buffer, with nothing buffered, goes
        // straight to the inner reader (buffering would just add a copy).
        if self.pos >= self.cap && dest.len() >= self.buf.len() {
            return self.inner.read(dest);
        }
        let n = {
            let available = self.fill_buf()?;
            let n = available.len().min(dest.len());
            let (src, _) = available.split_at(n);
            let (dst, _) = dest.split_at_mut(n);
            dst.copy_from_slice(src);
            n
        };
        self.consume(n);
        Ok(n)
    }
}

#[cfg(not(feature = "std"))]
impl<R: Read> BufRead for BufReader<R> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        if self.pos >= self.cap {
            self.cap = self.inner.read(&mut self.buf)?;
            self.pos = 0;
        }
        // Return `buf[pos..cap]` via split_at to keep `deny(indexing_slicing)`.
        let (_, rest) = self.buf.split_at(self.pos);
        let (out, _) = rest.split_at(self.cap - self.pos);
        Ok(out)
    }

    fn consume(&mut self, amt: usize) {
        self.pos = (self.pos + amt).min(self.cap);
    }
}

#[cfg(not(feature = "std"))]
impl<R: Read + Seek> Seek for BufReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        // The buffered bytes belong to the old position; drop them.
        self.pos = 0;
        self.cap = 0;
        self.inner.seek(pos)
    }
}

#[cfg(feature = "std")]
pub use std::io::BufWriter;

/// Buffering writer, mirroring the subset of [`std::io::BufWriter`] the
/// storage layer uses, for `no_std` builds. Coalesces small writes into one
/// `capacity`-sized buffer and flushes it to the inner writer on overflow,
/// explicit [`flush`](Write::flush), [`seek`](Seek::seek), or drop.
#[cfg(not(feature = "std"))]
pub struct BufWriter<W: Write> {
    inner: W,
    buf: alloc::vec::Vec<u8>,
}

#[cfg(not(feature = "std"))]
impl<W: Write> BufWriter<W> {
    /// Wraps `inner` with the default 8 KiB buffer.
    pub fn new(inner: W) -> Self {
        Self::with_capacity(8 * 1024, inner)
    }

    /// Wraps `inner` with a `capacity`-byte buffer.
    pub fn with_capacity(capacity: usize, inner: W) -> Self {
        Self {
            inner,
            buf: alloc::vec::Vec::with_capacity(capacity),
        }
    }

    /// Mutable access to the inner writer. Does NOT flush the buffer first —
    /// matches [`std::io::BufWriter::get_mut`].
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Shared access to the inner writer.
    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    fn flush_buf(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            self.inner.write_all(&self.buf)?;
            self.buf.clear();
        }
        Ok(())
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write> Write for BufWriter<W> {
    fn write(&mut self, data: &[u8]) -> Result<usize> {
        // Flush first if this write wouldn't fit alongside what's buffered.
        if self.buf.len() + data.len() > self.buf.capacity() {
            self.flush_buf()?;
        }
        // A write at least as large as the whole buffer bypasses it entirely
        // (buffering it would just add a copy), matching std::io::BufWriter.
        if data.len() >= self.buf.capacity() {
            self.inner.write(data)
        } else {
            self.buf.extend_from_slice(data);
            Ok(data.len())
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_buf()?;
        self.inner.flush()
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write + Seek> Seek for BufWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        // Buffered bytes belong before the seek target, so flush them out
        // before moving the inner cursor (matches std::io::BufWriter).
        self.flush_buf()?;
        self.inner.seek(pos)
    }
}

#[cfg(not(feature = "std"))]
impl<W: Write> Drop for BufWriter<W> {
    fn drop(&mut self) {
        // Best-effort flush, like std: a drop can't surface an error, and the
        // storage writer always flushes explicitly before teardown anyway.
        let _ = self.flush_buf();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Type-level check that a value satisfies the supertrait-alias
    /// bound. Used by the alias-wiring tests to fail the build if a
    /// future refactor breaks `&[u8]: crate::io::Read` /
    /// `Vec<u8>: crate::io::Write` propagation.
    #[cfg(feature = "std")]
    fn assert_read_alias_bound<R: Read>(_: &R) {}

    #[cfg(feature = "std")]
    fn assert_write_alias_bound<W: Write>(_: &W) {}

    #[test]
    fn error_kind_strings_are_distinct() {
        // Belt-and-suspenders that the `as_str` table stays in
        // sync with the enum — a forgotten arm would either fail
        // compilation (exhaustive match) or, if someone collapses
        // arms into a wildcard later, produce a duplicate message
        // that this assertion catches.
        let all = [
            ErrorKind::AlreadyExists,
            ErrorKind::BrokenPipe,
            ErrorKind::CrossesDevices,
            ErrorKind::Interrupted,
            ErrorKind::InvalidData,
            ErrorKind::InvalidInput,
            ErrorKind::NotFound,
            ErrorKind::Other,
            ErrorKind::PermissionDenied,
            ErrorKind::UnexpectedEof,
            ErrorKind::Unsupported,
            ErrorKind::WriteZero,
        ];
        for (i, a) in all.iter().enumerate() {
            for b in all.iter().skip(i + 1) {
                assert_ne!(
                    a.as_str(),
                    b.as_str(),
                    "duplicate description for {a:?} vs {b:?}",
                );
            }
        }
    }

    #[test]
    fn error_carries_kind_and_optional_message() {
        let e = Error::from_kind(ErrorKind::NotFound);
        assert_eq!(e.kind(), ErrorKind::NotFound);
        assert_eq!(alloc::format!("{e}"), "entity not found");

        let e = Error::new(ErrorKind::InvalidData, "bad magic");
        assert_eq!(e.kind(), ErrorKind::InvalidData);
        assert_eq!(alloc::format!("{e}"), "invalid data: bad magic");
    }

    #[test]
    fn error_kind_from_kind_is_const_friendly() {
        // `Error::from_kind` is `const fn`; this test would fail
        // to compile if the constness ever regressed.
        const _E: Error = Error::from_kind(ErrorKind::Interrupted);
    }

    #[cfg(feature = "std")]
    #[test]
    fn from_std_io_error_preserves_kind_and_message() {
        let std_err = std::io::Error::new(std::io::ErrorKind::WriteZero, "ran out");
        let crate_err: Error = std_err.into();
        assert_eq!(crate_err.kind(), ErrorKind::WriteZero);
        // Display must carry the original std error's message
        // (the `From` impl uses `format!("{err}")` on the std side).
        let rendered = alloc::format!("{crate_err}");
        assert!(
            rendered.contains("ran out"),
            "expected std message to survive in {rendered:?}",
        );
    }

    #[cfg(feature = "std")]
    #[test]
    fn from_std_io_error_maps_unknown_to_other() {
        // `std::io::ErrorKind` is `#[non_exhaustive]`; variants
        // we don't map explicitly fall through to `Other` so the
        // bridge stays total.
        let std_err = std::io::Error::from(std::io::ErrorKind::OutOfMemory);
        let crate_err: Error = std_err.into();
        assert_eq!(crate_err.kind(), ErrorKind::Other);
    }

    #[cfg(feature = "std")]
    #[test]
    fn round_trip_through_std_io_error_preserves_writezero() {
        // The fix for the inverted From-mapping (PR #347): a
        // `crate::io::Error { WriteZero }` must round-trip
        // through `std::io::Error` back to `WriteZero`, NOT
        // collapse to `Other`.
        let original = Error::new(ErrorKind::WriteZero, "short write");
        let as_std: std::io::Error = original.into();
        assert_eq!(as_std.kind(), std::io::ErrorKind::WriteZero);
        let back: Error = as_std.into();
        assert_eq!(back.kind(), ErrorKind::WriteZero);
    }

    #[cfg(feature = "std")]
    #[test]
    fn kind_only_other_std_error_skips_message_attachment() {
        // A kind-only `std::io::Error::from(ErrorKind::Other)` carries
        // no `raw_os_error` and no `get_ref` payload. Without an
        // explicit `Other => mapped=true` arm in the `From` impl, it
        // would fall through to the unmapped branch and attach
        // Display ("other error") as the message, producing the
        // doubled render "other error: other error" plus a heap alloc.
        let std_err = std::io::Error::from(std::io::ErrorKind::Other);
        let ours: Error = std_err.into();
        assert_eq!(ours.kind(), ErrorKind::Other);
        // Display includes the message only when one is attached
        // ("<kind>: <message>"); a kind-only error renders as just
        // "<kind>". The doubled "other error: other error" rendering
        // was the symptom the explicit Other arm fixes.
        let rendered = alloc::format!("{ours}");
        assert!(
            !rendered.contains(':'),
            "kind-only Other must not attach a message, got: {rendered:?}"
        );
    }

    #[cfg(feature = "std")]
    #[test]
    fn seek_from_round_trips_through_std() {
        // Variant-by-variant round trip — catches a future
        // refactor that drops or re-orders a discriminant.
        for case in [SeekFrom::Start(42), SeekFrom::End(-7), SeekFrom::Current(0)] {
            let std_form: std::io::SeekFrom = case.into();
            let back: SeekFrom = std_form.into();
            assert_eq!(case, back);
        }
    }

    #[cfg(feature = "std")]
    #[test]
    fn read_exact_via_blanket_impl_on_slice() -> std::io::Result<()> {
        // `&[u8]` impls `std::io::Read`, and the std-mode supertrait
        // alias + blanket make it satisfy `crate::io::Read`. Exercise
        // the resulting `read_exact` path end-to-end so a future
        // regression in the supertrait wiring fails here. The
        // `assert_read_alias_bound` helper above enforces the alias
        // bound at compile time; the runtime portion just checks the
        // read produces the expected bytes.
        let mut src: &[u8] = b"\x01\x02\x03\x04";
        assert_read_alias_bound(&src);
        let mut buf = [0u8; 4];
        <&[u8] as std::io::Read>::read_exact(&mut src, &mut buf)?;
        assert_eq!(buf, [1, 2, 3, 4]);
        Ok(())
    }

    #[cfg(feature = "std")]
    #[test]
    fn write_all_via_blanket_impl_on_vec() -> std::io::Result<()> {
        // Same pattern for `Vec<u8>` — `std::io::Write` impl picks
        // up the supertrait alias and `write_all` flows through.
        let mut sink: Vec<u8> = Vec::new();
        assert_write_alias_bound(&sink);
        <Vec<u8> as std::io::Write>::write_all(&mut sink, b"hello")?;
        assert_eq!(sink, b"hello");
        Ok(())
    }
}
