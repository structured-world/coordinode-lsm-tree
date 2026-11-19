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
