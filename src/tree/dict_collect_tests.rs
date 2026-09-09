// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Dictionary collection against the deletion pause.
//!
//! Lives in the crate rather than beside the other dictionary integration
//! tests because the pause is not part of the public surface.

use crate::compression::ZstdDictionary;
use crate::config::CompressionPolicy;
use crate::{AbstractTree, CompressionType, Config, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

fn training_corpus() -> Vec<u8> {
    let mut samples = Vec::new();
    for i in 0u32..500 {
        samples.extend_from_slice(format!("key-{i:05}").as_bytes());
        samples.extend_from_slice(format!("value-{i:05}-padding-to-make-it-longer").as_bytes());
    }
    samples
}

fn config(path: &std::path::Path) -> Config {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
}

/// A dictionary a checkpoint may still need must not be unlinked under it.
///
/// The checkpoint holds its captured version as a LOCAL clone, so the history
/// can move past that version while the checkpoint is still going to link the
/// dictionaries it names. Removing one now would fail `link_dictionaries` on a
/// missing file and abort an otherwise valid checkpoint.
///
/// The pass SKIPS such a dictionary rather than queueing it: a queued removal
/// fires unconditionally when the pause drains, and by then the id may have been
/// registered again — the drain would then delete a dictionary a registration
/// had just published. A later collection takes it instead.
#[test]
fn a_collection_under_a_held_pause_leaves_the_dictionary_alone() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let dict = ZstdDictionary::new(&training_corpus());
    let dict_id = dict.id();
    let compression = CompressionType::zstd_dict(3, dict_id)?;

    // Written under the dictionary...
    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;
        for i in 0u32..100 {
            tree.insert(
                format!("key-{i:05}").as_bytes(),
                b"value-under-the-dictionary",
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }

    // ...then rewritten without it. The first collection only unregisters it
    // from the latest version; the file stays while a retained version names
    // it, so the reopen below is what makes it genuinely removable.
    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .open()?;
        tree.major_compact(u64::MAX, 0)?;
        let crate::AnyTree::Standard(tree) = tree else {
            panic!("a standard tree");
        };
        tree.collect_unreferenced_dictionaries()?;
    }

    let tree = config(dir.path())
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        panic!("a standard tree");
    };
    let dict_path = dir
        .path()
        .join(crate::file::DICTS_FOLDER)
        .join(dict_id.to_string());
    assert!(dict_path.exists(), "collectable, but still on disk");

    {
        let _pause = tree.deletion_pause.acquire();
        assert_eq!(
            tree.collect_unreferenced_dictionaries()?,
            0,
            "a collection under a held pause removes nothing itself",
        );
        assert!(
            dict_path.exists(),
            "a file a checkpoint may still link is not unlinked under the pause",
        );
    }

    // Nothing was queued, so closing the pause removes nothing by itself...
    assert!(
        dict_path.exists(),
        "closing the pause must not fire a removal the collection never queued",
    );

    // ...and the next pass, with no pause held, takes it.
    assert_eq!(tree.collect_unreferenced_dictionaries()?, 1);
    assert!(!dict_path.exists(), "a later collection removes it");
    Ok(())
}

/// Re-registering an id during a pause must not lose the dictionary.
///
/// The shape this guards: a collection runs while a checkpoint holds the pause,
/// then the same id is registered again before the pause closes. Registration
/// finds the file already there, makes its write a no-op and durably records the
/// id — so a removal queued by that collection would delete the dictionary the
/// registration had just published, and the loss stays invisible until a reopen
/// finds a version naming a file that is gone.
#[test]
fn a_dictionary_registered_during_a_pause_survives_it() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let dict_id = dict.id();
    let compression = CompressionType::zstd_dict(3, dict_id)?;

    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::clone(&dict)))
            .open()?;
        for i in 0u32..100 {
            tree.insert(
                format!("key-{i:05}").as_bytes(),
                b"value-under-the-dictionary",
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }

    // Rewritten without it, then unregistered, so it is collectable.
    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .open()?;
        tree.major_compact(u64::MAX, 0)?;
        let crate::AnyTree::Standard(tree) = tree else {
            panic!("a standard tree");
        };
        tree.collect_unreferenced_dictionaries()?;
    }

    let tree = config(dir.path())
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        panic!("a standard tree");
    };
    let dict_path = dir
        .path()
        .join(crate::file::DICTS_FOLDER)
        .join(dict_id.to_string());

    {
        let _pause = tree.deletion_pause.acquire();
        tree.collect_unreferenced_dictionaries()?;
        // The same id comes back while the pause is still held.
        tree.register_zstd_dictionary(Arc::clone(&dict))?;
    }

    assert!(
        dict_path.exists(),
        "a dictionary registered during the pause must survive it closing",
    );
    assert!(
        tree.zstd_dictionaries().get(dict_id).is_some(),
        "and stay resolvable",
    );

    // The durable state agrees: a reopen still finds it.
    drop(tree);
    let reopened = config(dir.path())
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .open()?;
    let crate::AnyTree::Standard(reopened) = reopened else {
        panic!("a standard tree");
    };
    assert!(
        reopened.zstd_dictionaries().get(dict_id).is_some(),
        "the registration survives a reopen, not just the live set",
    );
    Ok(())
}

/// A collected dictionary leaves the LIVE registry, not just the disk.
///
/// The registry holds each dictionary's raw bytes and prepared decoder state, so
/// a tree that rotates dictionaries would otherwise carry every generation it
/// ever held for its whole life, and keep reporting ids it no longer owns.
#[test]
fn a_collected_dictionary_is_evicted_from_the_live_registry() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let dict = ZstdDictionary::new(&training_corpus());
    let dict_id = dict.id();
    let compression = CompressionType::zstd_dict(3, dict_id)?;

    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(compression))
            .zstd_dictionary(Some(Arc::new(dict)))
            .open()?;
        for i in 0u32..100 {
            tree.insert(
                format!("key-{i:05}").as_bytes(),
                b"value-under-the-dictionary",
                u64::from(i),
            );
        }
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = config(dir.path())
            .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
            .open()?;
        tree.major_compact(u64::MAX, 0)?;
        let crate::AnyTree::Standard(tree) = tree else {
            panic!("a standard tree");
        };
        tree.collect_unreferenced_dictionaries()?;
    }

    let tree = config(dir.path())
        .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
        .open()?;
    let crate::AnyTree::Standard(tree) = tree else {
        panic!("a standard tree");
    };
    assert!(
        tree.zstd_dictionaries().get(dict_id).is_some(),
        "loaded at open, before the collection",
    );

    assert_eq!(tree.collect_unreferenced_dictionaries()?, 1);
    assert!(
        tree.zstd_dictionaries().get(dict_id).is_none(),
        "a collected dictionary is dropped from the live set as well as the disk",
    );
    Ok(())
}
