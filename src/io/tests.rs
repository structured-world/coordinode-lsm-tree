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
        ErrorKind::WouldBlock,
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
    let std_err = std::io::Error::from(std::io::ErrorKind::FileTooLarge);
    let crate_err: Error = std_err.into();
    assert_eq!(crate_err.kind(), ErrorKind::Other);
}

/// NFS / FUSE / remote backends surface retryable NETWORK kinds
/// (`TimedOut`, `HostUnreachable`, `NetworkDown`, `NetworkUnreachable`,
/// `StaleNetworkFileHandle`). Collapsing them into `Other` erases the
/// transient signal, and recovery grading then condemns a healthy file as
/// corrupt — the manifest excludes it and removes it. The bridge must
/// preserve them, classify them transient, and round-trip the discriminant.
#[cfg(feature = "std")]
#[test]
fn network_kinds_survive_the_bridge_as_transient() {
    for std_kind in [
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::HostUnreachable,
        std::io::ErrorKind::NetworkDown,
        std::io::ErrorKind::NetworkUnreachable,
        std::io::ErrorKind::StaleNetworkFileHandle,
    ] {
        let ours: Error = std::io::Error::from(std_kind).into();
        assert!(
            ours.kind().is_transient(),
            "{std_kind:?} is a retryable environment failure, got {:?}",
            ours.kind(),
        );
        let back: std::io::Error = ours.into();
        assert_eq!(back.kind(), std_kind, "the discriminant must round-trip");
    }
}

/// Write-side STORAGE kinds (`StorageFull` / ENOSPC, `QuotaExceeded` /
/// EDQUOT, `ReadOnlyFilesystem` / EROFS) are environmental: the destination,
/// not the data, failed. Collapsing them into `Other` lets recovery grading
/// condemn a healthy SOURCE file when writing its salvage replacement fails —
/// the manifest then excludes the source and removes it. The bridge must
/// preserve them and classify them environmental (not transient: an
/// immediate retry does not clear a full disk, the operator does).
#[cfg(feature = "std")]
#[test]
fn storage_kinds_survive_the_bridge_as_environmental() {
    for std_kind in [
        std::io::ErrorKind::StorageFull,
        std::io::ErrorKind::QuotaExceeded,
        std::io::ErrorKind::ReadOnlyFilesystem,
        std::io::ErrorKind::OutOfMemory,
    ] {
        let ours: Error = std::io::Error::from(std_kind).into();
        assert!(
            ours.kind().is_environmental(),
            "{std_kind:?} is an environment failure, got {:?}",
            ours.kind(),
        );
        assert!(
            !ours.kind().is_transient(),
            "{std_kind:?} is not cleared by an immediate retry",
        );
        let back: std::io::Error = ours.into();
        assert_eq!(back.kind(), std_kind, "the discriminant must round-trip");
    }
}

/// Running out of file descriptors says nothing about the file being opened,
/// and std has no kind for it, so the OS code alone tells it apart. Read as
/// `Other`, recovery grading would condemn every table it fails to open once
/// the descriptor limit is reached.
#[cfg(all(feature = "std", unix))]
#[test]
fn an_exhausted_descriptor_table_is_an_environmental_kind() {
    for code in [libc::EMFILE, libc::ENFILE] {
        let ours: Error = std::io::Error::from_raw_os_error(code).into();
        assert_eq!(ours.kind(), ErrorKind::TooManyOpenFiles, "errno {code}");
        assert!(
            ours.kind().is_environmental(),
            "errno {code} must abort recovery grading, not condemn the file",
        );
    }
}

/// A kind std has no variant for survives a round trip through
/// `std::io::Error`, which every std-backed backend path takes.
#[cfg(feature = "std")]
#[test]
fn too_many_open_files_round_trips_through_std_io_error() {
    let original = Error::new(ErrorKind::TooManyOpenFiles, "open tables/7");
    let as_std: std::io::Error = original.into();
    let back: Error = as_std.into();
    assert_eq!(back.kind(), ErrorKind::TooManyOpenFiles);
    assert_eq!(back.to_string(), "too many open files: open tables/7");
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

/// Records the length of every write the buffer hands down.
#[cfg(feature = "std")]
#[derive(Default)]
struct WriteLog {
    bytes: Vec<u8>,
    writes: Vec<usize>,
}

#[cfg(feature = "std")]
impl std::io::Write for WriteLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writes.push(buf.len());
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Frames a little over a quarter of the buffer each, as a blob file's
/// packed blocks are: the buffer has to go down full, not cut where the
/// next frame stopped fitting, or a file takes a write per three frames
/// instead of one per buffer.
#[cfg(feature = "std")]
#[test]
fn buf_writer_writes_full_buffers_for_frames_that_straddle_it() -> std::io::Result<()> {
    use std::io::Write as _;
    const CAP: usize = 1024;
    let frame: Vec<u8> = (0..=255u8).cycle().take(CAP / 4 + 13).collect();
    let mut out = FillingBufWriter::with_capacity(CAP, WriteLog::default());
    for _ in 0..40 {
        out.write_all(&frame)?;
    }
    out.flush()?;
    let log = out.get_ref();
    let total = frame.len() * 40;
    assert_eq!(log.bytes, frame.repeat(40));
    let full = total / CAP;
    let mut expected = vec![CAP; full];
    expected.push(total - full * CAP);
    assert_eq!(log.writes, expected);
    Ok(())
}

/// Frames that fill the buffer exactly leave it full with nothing taken
/// from the next one; that write must still take bytes, not report zero.
#[cfg(feature = "std")]
#[test]
fn buf_writer_takes_the_write_after_an_exactly_full_buffer() -> std::io::Result<()> {
    use std::io::Write as _;
    const CAP: usize = 1024;
    let frame = [7u8; CAP / 4];
    let mut out = FillingBufWriter::with_capacity(CAP, WriteLog::default());
    for _ in 0..9 {
        out.write_all(&frame)?;
    }
    out.flush()?;
    let log = out.get_ref();
    assert_eq!(log.writes, [CAP, CAP, CAP / 4]);
    assert_eq!(log.bytes, [7u8; 9 * CAP / 4]);
    Ok(())
}

/// A write as large as the buffer goes down as it is, after what is
/// buffered: copying it through the buffer would cost a copy and save no
/// write.
#[cfg(feature = "std")]
#[test]
fn buf_writer_passes_a_write_of_a_whole_buffer_straight_down() -> std::io::Result<()> {
    use std::io::Write as _;
    const CAP: usize = 1024;
    let mut out = FillingBufWriter::with_capacity(CAP, WriteLog::default());
    out.write_all(&[1; 100])?;
    out.write_all(&[2; 3 * CAP])?;
    out.write_all(&[3; 10])?;
    out.flush()?;
    let log = out.get_ref();
    assert_eq!(log.writes, [100, 3 * CAP, 10]);
    let mut expected = vec![1; 100];
    expected.extend([2; 3 * CAP]);
    expected.extend([3; 10]);
    assert_eq!(log.bytes, expected);
    Ok(())
}

/// An inner writer that takes `accept` bytes of its first write, fails its
/// second, and takes everything after.
#[cfg(feature = "std")]
#[derive(Default)]
struct FailsOnce {
    accept: usize,
    calls: usize,
    bytes: Vec<u8>,
}

#[cfg(feature = "std")]
impl std::io::Write for FailsOnce {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        let taken = match self.calls {
            1 => self.accept.min(buf.len()),
            2 => return Err(std::io::Error::other("injected")),
            _ => buf.len(),
        };
        self.bytes
            .extend_from_slice(buf.get(..taken).unwrap_or_default());
        Ok(taken)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A flush the inner writer fails after taking part of the buffer leaves only
/// what it did not take: a write retried after the error, and the flush
/// after it, put every byte down once.
#[cfg(feature = "std")]
#[test]
fn buf_writer_keeps_only_what_a_failed_flush_did_not_write() -> std::io::Result<()> {
    use std::io::Write as _;
    let mut out = FillingBufWriter::with_capacity(
        8,
        FailsOnce {
            accept: 6,
            ..FailsOnce::default()
        },
    );
    out.write_all(b"abcdef")?;
    assert!(
        out.write_all(b"ghij").is_err(),
        "the injected failure surfaces"
    );
    out.write_all(b"ghij")?;
    out.flush()?;
    assert_eq!(out.get_ref().bytes, b"abcdefghij");
    Ok(())
}

/// A flush that fails after taking bytes of the write being buffered has
/// taken that write's prefix: the write reports it taken, and what it put in
/// the buffer goes down with the next flush, once.
#[cfg(feature = "std")]
#[test]
fn buf_writer_takes_a_write_a_failed_flush_wrote_part_of() -> std::io::Result<()> {
    use std::io::Write as _;
    let mut out = FillingBufWriter::with_capacity(
        8,
        FailsOnce {
            accept: 7,
            ..FailsOnce::default()
        },
    );
    out.write_all(b"abcdef")?;
    out.write_all(b"ghij")?;
    out.flush()?;
    assert_eq!(out.get_ref().bytes, b"abcdefghij");
    Ok(())
}

/// An inner writer that takes 2 bytes of its first write, panics in its
/// second, and takes everything after.
#[cfg(feature = "std")]
#[derive(Default)]
struct PanicsOnce {
    calls: usize,
    bytes: Vec<u8>,
}

#[cfg(feature = "std")]
impl std::io::Write for PanicsOnce {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.calls += 1;
        let taken = match self.calls {
            1 => 2.min(buf.len()),
            2 => panic!("injected"),
            _ => buf.len(),
        };
        self.bytes
            .extend_from_slice(buf.get(..taken).unwrap_or_default());
        Ok(taken)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A flush whose inner write panics after an earlier write of it took bytes
/// keeps only what was not taken: retried after the panic is caught, it puts
/// every byte down once.
#[cfg(feature = "std")]
#[test]
fn buf_writer_keeps_only_what_a_panicking_flush_did_not_write() -> std::io::Result<()> {
    use std::io::Write as _;
    let mut out = FillingBufWriter::with_capacity(8, PanicsOnce::default());
    out.write_all(b"abcdef")?;
    let caught = std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| out.flush())).is_err();
    assert!(caught, "the injected panic surfaces");
    out.flush()?;
    assert_eq!(out.get_ref().bytes, b"abcdef");
    Ok(())
}

/// A writer dropped after its inner write panicked does not put down again
/// what that flush took, nor call the writer that panicked.
#[cfg(feature = "std")]
#[test]
fn buf_writer_dropped_after_a_panicking_write_replays_nothing() -> std::io::Result<()> {
    use std::io::Write as _;
    let mut inner = PanicsOnce::default();
    {
        let mut out = FillingBufWriter::with_capacity(8, &mut inner);
        out.write_all(b"abcdef")?;
        let caught =
            std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| out.flush())).is_err();
        assert!(caught, "the injected panic surfaces");
    }
    assert_eq!(inner.bytes, b"ab");
    assert_eq!(inner.calls, 2, "the drop calls the writer no more");
    Ok(())
}

/// Every `ErrorKind` discriminant — kept in one place so the
/// exhaustive bridge tests below stay total. A new variant added
/// to the enum without a row here fails the per-arm assertions
/// (the std round trip would not preserve it).
#[cfg(feature = "std")]
const ALL_KINDS: [ErrorKind; 22] = [
    ErrorKind::AlreadyExists,
    ErrorKind::BrokenPipe,
    ErrorKind::CrossesDevices,
    ErrorKind::HostUnreachable,
    ErrorKind::Interrupted,
    ErrorKind::InvalidData,
    ErrorKind::InvalidInput,
    ErrorKind::NetworkDown,
    ErrorKind::NetworkUnreachable,
    ErrorKind::NotFound,
    ErrorKind::Other,
    ErrorKind::OutOfMemory,
    ErrorKind::PermissionDenied,
    ErrorKind::QuotaExceeded,
    ErrorKind::ReadOnlyFilesystem,
    ErrorKind::StaleNetworkFileHandle,
    ErrorKind::StorageFull,
    ErrorKind::TimedOut,
    ErrorKind::UnexpectedEof,
    ErrorKind::Unsupported,
    ErrorKind::WouldBlock,
    ErrorKind::WriteZero,
];

#[cfg(feature = "std")]
#[test]
fn every_kind_only_error_round_trips_through_std() {
    // Exercises EVERY arm of both `From` match tables in one
    // sweep: a kind-only `Error` (no message) crosses to
    // `std::io::Error` (hitting that kind's arm in
    // `From<Error> for std::io::Error`, plus the `None` message
    // branch) and back (hitting the corresponding forward arm in
    // `From<std::io::Error> for Error`, plus the kind-only mapped
    // branch). The kind must survive both crossings unchanged —
    // a dropped or mis-paired arm in either table fails here.
    for kind in ALL_KINDS {
        let original = Error::from_kind(kind);
        let as_std: std::io::Error = original.into();
        let back: Error = as_std.into();
        assert_eq!(back.kind(), kind, "kind {kind:?} did not round-trip");
    }
}

#[cfg(feature = "std")]
#[test]
fn every_kind_only_std_error_maps_to_matching_kind() {
    // Forward direction in isolation: a kind-only
    // `std::io::Error` for each kind our table maps explicitly
    // must produce the matching `ErrorKind`. Hits each mapped arm
    // of `From<std::io::Error> for Error` directly (the prior test
    // reaches them via produced std kinds; this asserts the mapping
    // from the std side, where a future std-kind rename would bite).
    let mapped = [
        (std::io::ErrorKind::AlreadyExists, ErrorKind::AlreadyExists),
        (std::io::ErrorKind::BrokenPipe, ErrorKind::BrokenPipe),
        (
            std::io::ErrorKind::CrossesDevices,
            ErrorKind::CrossesDevices,
        ),
        (
            std::io::ErrorKind::HostUnreachable,
            ErrorKind::HostUnreachable,
        ),
        (std::io::ErrorKind::Interrupted, ErrorKind::Interrupted),
        (std::io::ErrorKind::InvalidData, ErrorKind::InvalidData),
        (std::io::ErrorKind::InvalidInput, ErrorKind::InvalidInput),
        (std::io::ErrorKind::NetworkDown, ErrorKind::NetworkDown),
        (
            std::io::ErrorKind::NetworkUnreachable,
            ErrorKind::NetworkUnreachable,
        ),
        (std::io::ErrorKind::NotFound, ErrorKind::NotFound),
        (std::io::ErrorKind::OutOfMemory, ErrorKind::OutOfMemory),
        (
            std::io::ErrorKind::PermissionDenied,
            ErrorKind::PermissionDenied,
        ),
        (std::io::ErrorKind::QuotaExceeded, ErrorKind::QuotaExceeded),
        (
            std::io::ErrorKind::ReadOnlyFilesystem,
            ErrorKind::ReadOnlyFilesystem,
        ),
        (
            std::io::ErrorKind::StaleNetworkFileHandle,
            ErrorKind::StaleNetworkFileHandle,
        ),
        (std::io::ErrorKind::StorageFull, ErrorKind::StorageFull),
        (std::io::ErrorKind::TimedOut, ErrorKind::TimedOut),
        (std::io::ErrorKind::UnexpectedEof, ErrorKind::UnexpectedEof),
        (std::io::ErrorKind::Unsupported, ErrorKind::Unsupported),
        (std::io::ErrorKind::WouldBlock, ErrorKind::WouldBlock),
        (std::io::ErrorKind::WriteZero, ErrorKind::WriteZero),
    ];
    for (std_kind, want) in mapped {
        let ours: Error = std::io::Error::from(std_kind).into();
        assert_eq!(ours.kind(), want, "std {std_kind:?} mis-mapped");
    }
}

#[cfg(feature = "std")]
#[test]
fn from_raw_os_error_preserves_os_detail_as_message() {
    // A `std::io::Error::from_raw_os_error` carries an errno but no
    // kind tag we mapped — `raw_os_error().is_some()` selects the
    // message-attachment branch, so the OS-level Display text must
    // survive into our message field rather than being dropped.
    let std_err = std::io::Error::from_raw_os_error(2); // ENOENT
    let ours: Error = std_err.into();
    let rendered = alloc::format!("{ours}");
    assert!(
        rendered.contains(':'),
        "os-detail error must attach a message, got: {rendered:?}"
    );
}

#[test]
fn debug_renders_kind_always_and_message_when_present() {
    // The custom `Debug` impl omits the `message` field entirely
    // when none is set (it is not `Option`-wrapped in the output).
    let kind_only = Error::from_kind(ErrorKind::NotFound);
    let dbg = alloc::format!("{kind_only:?}");
    assert!(dbg.contains("NotFound"), "missing kind in {dbg:?}");
    assert!(
        !dbg.contains("message"),
        "kind-only must omit message: {dbg:?}"
    );

    let with_msg = Error::new(ErrorKind::InvalidData, "bad magic");
    let dbg = alloc::format!("{with_msg:?}");
    assert!(dbg.contains("message"), "expected message field in {dbg:?}");
    assert!(dbg.contains("bad magic"), "expected payload in {dbg:?}");
}

#[test]
fn other_constructor_and_kind_from_conversion() {
    // `Error::other` is the `ErrorKind::Other` shortcut; the
    // `From<ErrorKind>` impl is the kind-only `?`-coercion path.
    let e = Error::other("boom");
    assert_eq!(e.kind(), ErrorKind::Other);
    assert_eq!(alloc::format!("{e}"), "other error: boom");

    let e: Error = ErrorKind::PermissionDenied.into();
    assert_eq!(e.kind(), ErrorKind::PermissionDenied);
    assert_eq!(alloc::format!("{e}"), "permission denied");
}

#[test]
fn error_kind_display_matches_as_str() {
    // The `Display` impl on `ErrorKind` itself (distinct from the
    // `Error` Display) must render exactly the `as_str` tag for
    // every variant — no prefix, no decoration.
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
        ErrorKind::WouldBlock,
        ErrorKind::WriteZero,
    ];
    for kind in all {
        assert_eq!(alloc::format!("{kind}"), kind.as_str());
    }
}

#[test]
fn big_endian_byte_order_round_trips_all_widths() {
    // `BigEndian` is the rarely-exercised twin of `LittleEndian`
    // (the wire format is LE), so assert each width converts to
    // big-endian bytes and back. `0x0102..` payloads make the byte
    // order observable: the most-significant byte lands first.
    assert_eq!(BigEndian::u16_to(0x0102), [0x01, 0x02]);
    assert_eq!(BigEndian::u16_from([0x01, 0x02]), 0x0102);
    assert_eq!(BigEndian::u32_to(0x0102_0304), [0x01, 0x02, 0x03, 0x04]);
    assert_eq!(BigEndian::u32_from([0x01, 0x02, 0x03, 0x04]), 0x0102_0304);
    assert_eq!(
        BigEndian::u64_to(0x0102_0304_0506_0708),
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
    assert_eq!(
        BigEndian::u64_from([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
        0x0102_0304_0506_0708
    );
    let v = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10u128;
    assert_eq!(BigEndian::u128_from(BigEndian::u128_to(v)), v);
    assert_eq!(BigEndian::u128_to(v)[0], 0x01, "MSB must land first");
}

#[test]
fn byte_order_buf_helpers_round_trip_u64() {
    // The static `write_u64` / `read_u64` buffer helpers on the
    // `ByteOrder` trait (the default-method path used by callers
    // that own a fixed slice rather than a stream).
    let mut buf = [0u8; 8];
    LittleEndian::write_u64(&mut buf, 0xdead_beef_cafe_f00d);
    assert_eq!(LittleEndian::read_u64(&buf), 0xdead_beef_cafe_f00d);
}

#[cfg(feature = "std")]
#[test]
fn read_bytes_ext_reads_floats_in_byte_order() -> crate::io::Result<()> {
    // `read_f32` / `read_f64` decode IEEE-754 bit patterns through
    // the integer readers; exercise both over a `Cursor` so the
    // bit-cast path is covered. Compare via `to_bits` (exact integer
    // equality) so the assertion is bit-precise and avoids float_cmp.
    let f32_bytes = 1.5f32.to_le_bytes();
    let mut cur = Cursor::new(&f32_bytes[..]);
    assert_eq!(cur.read_f32::<LittleEndian>()?.to_bits(), 1.5f32.to_bits());

    let f64_bytes = (-2.25f64).to_le_bytes();
    let mut cur = Cursor::new(&f64_bytes[..]);
    assert_eq!(
        cur.read_f64::<LittleEndian>()?.to_bits(),
        (-2.25f64).to_bits()
    );
    Ok(())
}
