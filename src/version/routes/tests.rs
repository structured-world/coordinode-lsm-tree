use super::relative;
use crate::path::Path;

/// A route inside the tree's folder is named from it, one beside or above it
/// through `..`. Guards against a route
/// recorded in a form an open of the same tree, moved with its routes, would
/// not match.
#[test]
fn a_route_is_named_relative_to_the_tree() {
    assert_eq!(
        relative(Path::new("/db"), Path::new("/db/cold/tables")),
        "cold/tables"
    );
    assert_eq!(
        relative(Path::new("/db"), Path::new("/fast/tables")),
        "../fast/tables"
    );
    assert_eq!(
        relative(Path::new("/srv/db"), Path::new("/mnt/fast/tables")),
        "../../mnt/fast/tables"
    );
    assert_eq!(
        relative(Path::new("/db/"), Path::new("/db/./x/tables")),
        "x/tables"
    );
    // A tree and its route moved together keep the same record.
    assert_eq!(
        relative(Path::new("/old/db"), Path::new("/old/fast/tables")),
        relative(Path::new("/new/db"), Path::new("/new/fast/tables")),
    );
}

/// Off Windows a backslash is a character of a folder's name, not a
/// separator. Guards against two different folders (`fast\cold` and
/// `fast/cold`) recording the same route, so an unclean open configured with
/// the one would pass the check of the other and not scan it.
#[cfg(not(windows))]
#[test]
fn a_backslash_off_windows_is_part_of_a_name() {
    assert_eq!(
        relative(Path::new("/db"), Path::new("/fast\\cold/tables")),
        "../fast\\cold/tables"
    );
    assert_ne!(
        relative(Path::new("/db"), Path::new("/fast\\cold/tables")),
        relative(Path::new("/db"), Path::new("/fast/cold/tables")),
    );
}

/// Sibling folders whose names differ only in bytes that are not UTF-8 record
/// different routes, and a `%` in a name cannot pass for such a byte. Guards
/// against an unclean open configured with the one folder passing the check
/// of the other, which replacing those bytes made read alike.
#[cfg(all(unix, feature = "std"))]
#[test]
fn folders_apart_only_in_bytes_not_utf8_record_apart() {
    use std::os::unix::ffi::OsStrExt as _;
    let named = |bytes: &[u8]| Path::new(std::ffi::OsStr::from_bytes(bytes)).to_path_buf();
    let ff = relative(Path::new("/db"), &named(b"/fast/\xFF/tables"));
    let fe = relative(Path::new("/db"), &named(b"/fast/\xFE/tables"));
    assert_ne!(ff, fe);
    assert_eq!(ff, "../fast/%FF/tables");
    assert_ne!(
        relative(Path::new("/db"), Path::new("/fast/%FF/tables")),
        ff,
        "a literal `%FF` names another folder",
    );
}

/// On Windows both separators part a path, and a route records `/` alone.
/// Guards against a route a tree written with `\` records apart from the
/// same route written with `/`.
#[cfg(windows)]
#[test]
fn both_separators_on_windows_read_alike() {
    assert_eq!(
        relative(Path::new("C:\\db"), Path::new("C:\\fast\\cold/tables")),
        "../fast/cold/tables"
    );
}
