use super::*;
use crate::{AbstractTree, SequenceNumberCounter, Slice};
use test_log::test;

#[test]
fn run_scanner_basic() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(
        &tempdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let ids = [
        ["a", "b", "c"],
        ["d", "e", "f"],
        ["g", "h", "i"],
        ["j", "k", "l"],
    ];

    for batch in ids {
        for id in batch {
            tree.insert(id, vec![], 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let tables = tree
        .current_version()
        .iter_tables()
        .cloned()
        .collect::<Vec<_>>();

    let level = Arc::new(Run::new(tables).unwrap());

    #[expect(clippy::unwrap_used)]
    {
        let multi_reader = RunScanner::culled(level.clone(), (None, None), None)?;

        let mut iter = multi_reader.flatten();

        assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    #[expect(clippy::unwrap_used)]
    {
        let multi_reader = RunScanner::culled(level, (Some(1), None), None)?;

        let mut iter = multi_reader.flatten();

        assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    Ok(())
}

/// A scan told to go past the run's last table hands out every table it
/// holds and then fails, rather than ending as if the run were complete.
#[test]
fn run_scanner_past_the_run_fails_after_its_tables() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(
        &tempdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for id in ["a", "b"] {
        tree.insert(id, vec![], 0);
        tree.flush_active_memtable(0)?;
    }
    let tables = tree
        .current_version()
        .iter_tables()
        .cloned()
        .collect::<Vec<_>>();
    let run = Arc::new(Run::new(tables).unwrap());

    let mut items = RunScanner::culled(run, (None, Some(2)), None)?;

    assert_eq!(items.next().unwrap()?.key.user_key, Slice::from(*b"a"));
    assert_eq!(items.next().unwrap()?.key.user_key, Slice::from(*b"b"));
    assert!(items.next().unwrap().is_err(), "the missing table fails");
    assert!(items.next().is_none(), "nothing after the failure");
    Ok(())
}
