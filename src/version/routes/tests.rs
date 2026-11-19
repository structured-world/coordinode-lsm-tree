use super::relative;
use crate::path::Path;

/// A route inside the tree's folder is named from it, one beside or above it
/// through `..`, and the two separators read alike. Guards against a route
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
