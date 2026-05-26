// Local I/O trait surface, mirroring `std::io::{Read, Write, Seek}` so the
// `fs::Fs` / `fs::FsFile` trait definitions compile under
// `--no-default-features --features alloc`. The signatures match
// `std::io` exactly, so backends gated behind `#[cfg(feature = "std")]`
// (such as `std_fs` and `io_uring_fs`) keep using `std::io::*`
// internally — they just satisfy this crate's traits via the bridge
// impls below.
//
// Why not pull in an external `core_io` / `core2` / `core3` / `embedded-io`
// crate: those add a maintainer dependency for what is ultimately three
// stable trait signatures plus an error type. The signatures here have
// not meaningfully changed since Rust 1.0, so maintenance is near zero,
// and we keep the no-std contract under our own control.

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
        }
    }
}

/// I/O error mirroring [`std::io::Error`].
///
/// Carries an [`ErrorKind`] plus an optional message string for context.
/// Under `feature = "std"` the `From<std::io::Error>` bridge below
/// preserves the original error as the message payload via `Display`,
/// so callers using `?` see the same human-readable text as the
/// platform error.
pub struct Error {
    kind: ErrorKind,
    message: Option<Box<str>>,
}

impl Error {
    /// Construct an error with the given kind and a context message.
    /// Matches [`std::io::Error::new`].
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
            dbg.field("message", &msg);
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

#[cfg(feature = "std")]
impl std::error::Error for Error {}

/// Bridge from `std::io::Error`. Preserves the platform error's
/// `ErrorKind` mapping and its `Display` message so `?` from std-backed
/// backends propagates the original context.
#[cfg(feature = "std")]
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
            std::io::ErrorKind::BrokenPipe => ErrorKind::BrokenPipe,
            std::io::ErrorKind::CrossesDevices => ErrorKind::CrossesDevices,
            std::io::ErrorKind::Interrupted => ErrorKind::Interrupted,
            std::io::ErrorKind::InvalidData => ErrorKind::InvalidData,
            std::io::ErrorKind::InvalidInput => ErrorKind::InvalidInput,
            std::io::ErrorKind::NotFound => ErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            std::io::ErrorKind::UnexpectedEof => ErrorKind::UnexpectedEof,
            std::io::ErrorKind::Unsupported => ErrorKind::Unsupported,
            _ => ErrorKind::Other,
        };
        // Use Display on the std error so OS-level detail
        // (errno text, path context) survives the conversion.
        Self::new(kind, alloc::format!("{err}"))
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
// implies `T: std::io::Read` (because std::io::Read is a supertrait).
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
                Ok(n) => buf = &mut buf[n..],
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if buf.is_empty() {
            Ok(())
        } else {
            Err(Error::from_kind(ErrorKind::UnexpectedEof))
        }
    }
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
    /// returning [`ErrorKind::Other`] (matching std's `write_zero`)
    /// if the writer stops accepting bytes early.
    ///
    /// # Errors
    ///
    /// Returns the underlying writer's error, or a synthesised
    /// `WriteZero`-equivalent error on short write.
    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => {
                    return Err(Error::new(ErrorKind::Other, "failed to write whole buffer"));
                }
                Ok(n) => buf = &buf[n..],
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
}
