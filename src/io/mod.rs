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
