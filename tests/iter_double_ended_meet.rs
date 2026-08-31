//! A tree iterator's two ends must meet in the middle exactly once.
//!
//! `DoubleEndedIterator` guarantees that every item comes out of one end or
//! the other, and none out of both. The merger underneath keeps a separate
//! tournament per direction over the same sources, so a value pulled into one
//! tournament is no longer inside its source: the other direction has to pick
//! it up when that source runs dry, or the value is emitted by neither end and
//! the caller silently loses a row.
//!
//! These walk a tree from both ends in the patterns that expose it: a step one
//! way followed by a drain the other way, and a strict alternation.

use lsm_tree::{AbstractTree, Config, Guard as _, SeqNo, SequenceNumberCounter};

const N: usize = 10;

fn key_for(i: usize) -> String {
    format!("key{i:06}")
}

fn tree(path: &std::path::Path) -> lsm_tree::Result<lsm_tree::AnyTree> {
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(path, seqno.clone(), SequenceNumberCounter::default()).open()?;
    for i in 0..N {
        tree.insert(key_for(i).as_bytes(), b"v".as_ref(), seqno.next());
    }
    tree.flush_active_memtable(0)?;
    Ok(tree)
}

fn assert_covers_every_key(mut seen: Vec<String>) {
    seen.sort();
    let expected: Vec<String> = (0..N).map(key_for).collect();
    assert_eq!(
        seen, expected,
        "an entry was dropped or repeated between the two ends",
    );
}

/// One forward step parks a value in the merger's forward tournament; draining
/// backward from there has to emit it.
#[test_log::test]
fn a_forward_step_then_a_backward_drain_covers_every_key() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = tree(dir.path())?;

    let mut iter = tree.iter(SeqNo::MAX, None);
    let mut seen = Vec::new();

    if let Some(guard) = iter.next() {
        seen.push(String::from_utf8_lossy(&guard.key()?).into_owned());
    }
    while let Some(guard) = iter.next_back() {
        seen.push(String::from_utf8_lossy(&guard.key()?).into_owned());
    }

    assert_covers_every_key(seen);
    Ok(())
}

/// The mirror: one backward step parks a value in the backward tournament, and
/// the forward drain has to emit it.
#[test_log::test]
fn a_backward_step_then_a_forward_drain_covers_every_key() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = tree(dir.path())?;

    let mut iter = tree.iter(SeqNo::MAX, None);
    let mut seen = Vec::new();

    if let Some(guard) = iter.next_back() {
        seen.push(String::from_utf8_lossy(&guard.key()?).into_owned());
    }
    for guard in iter {
        seen.push(String::from_utf8_lossy(&guard.key()?).into_owned());
    }

    assert_covers_every_key(seen);
    Ok(())
}

/// Strict alternation, so the two ends converge one step at a time and meet
/// wherever the parity of the range puts them.
#[test_log::test]
fn alternating_ends_cover_every_key() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = tree(dir.path())?;

    let mut iter = tree.iter(SeqNo::MAX, None);
    let mut seen = Vec::new();

    loop {
        match iter.next() {
            Some(guard) => seen.push(String::from_utf8_lossy(&guard.key()?).into_owned()),
            None => break,
        }
        match iter.next_back() {
            Some(guard) => seen.push(String::from_utf8_lossy(&guard.key()?).into_owned()),
            None => break,
        }
    }

    assert_covers_every_key(seen);
    Ok(())
}
