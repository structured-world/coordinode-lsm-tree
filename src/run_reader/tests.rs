use super::*;
use crate::{AbstractTree, SequenceNumberCounter, Slice};
use test_log::test;

#[test]
fn run_reader_skip() -> crate::Result<()> {
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

    assert!(RunReader::new(level.clone(), UserKey::from("y")..=UserKey::from("z"),).is_none());

    assert!(RunReader::new(level, UserKey::from("y")..).is_none());

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn run_reader_basic() -> crate::Result<()> {
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

    {
        let multi_reader = RunReader::culled(level.clone(), .., (Some(1), None));
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

    {
        let multi_reader = RunReader::new(level.clone(), ..).unwrap();

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

    {
        let multi_reader = RunReader::new(level.clone(), ..).unwrap();

        let mut iter = multi_reader.rev().flatten();

        assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    {
        let multi_reader = RunReader::new(level.clone(), ..).unwrap();

        let mut iter = multi_reader.flatten();

        assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"l"), iter.next_back().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next_back().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next_back().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next_back().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next_back().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"g"), iter.next_back().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    {
        let multi_reader = RunReader::new(level.clone(), UserKey::from("g")..).unwrap();

        let mut iter = multi_reader.flatten();

        assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    {
        let multi_reader = RunReader::new(level, UserKey::from("g")..).unwrap();

        let mut iter = multi_reader.flatten().rev();

        assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
        assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
        assert!(iter.next().is_none());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn run_reader_reseek_repositions_and_handles_no_overlap() -> crate::Result<()> {
    let tempdir = tempfile::tempdir()?;
    let tree = crate::Config::new(
        &tempdir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for batch in [
        ["a", "b", "c"],
        ["d", "e", "f"],
        ["g", "h", "i"],
        ["j", "k", "l"],
    ] {
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
    let cmp = crate::comparator::DefaultUserComparator;

    // Open over the full range, consume one key, then reseek to a disjoint
    // sub-window: the reader must serve exactly that window from a fresh
    // position (boundary readers re-open lazily against the new bounds).
    let mut reader = RunReader::new(level, ..).unwrap();
    assert_eq!(Slice::from(*b"a"), reader.next().unwrap()?.key.user_key);

    reader.reseek(UserKey::from("e")..=UserKey::from("h"), &cmp);
    let got: Vec<_> = std::iter::from_fn(|| reader.next())
        .map(|r| r.unwrap().key.user_key)
        .collect();
    assert_eq!(
        got,
        [
            Slice::from(*b"e"),
            Slice::from(*b"f"),
            Slice::from(*b"g"),
            Slice::from(*b"h"),
        ],
    );

    // Reseek to a range that does not overlap the run at all: both
    // directions must immediately report exhaustion.
    reader.reseek(UserKey::from("y")..=UserKey::from("z"), &cmp);
    assert!(
        reader.next().is_none(),
        "no-overlap reseek must be empty forward"
    );
    assert!(
        reader.next_back().is_none(),
        "no-overlap reseek must be empty backward",
    );

    Ok(())
}
