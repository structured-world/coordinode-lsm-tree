// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Compression policies changed on a live tree through `update_runtime_config`,
//! checked against the codec each file records. A blob file's codec is not part
//! of the public surface, which is why these live in the crate.

use crate::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
use crate::compression::ZstdDictionary;
use crate::config::{CompressionPolicy, KvSeparationOptions};
use crate::iter_guard::IterGuard;
use crate::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, SequenceNumberCounter, Tree,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use test_log::test;

fn training_corpus() -> Vec<u8> {
    let mut samples = Vec::new();
    for i in 0u32..500 {
        samples.extend_from_slice(format!("gen0-key-{i:05}").as_bytes());
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

fn open_blob_tree(config: Config) -> crate::Result<BlobTree> {
    match config
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?
    {
        AnyTree::Blob(tree) => Ok(tree),
        AnyTree::Standard(_) => panic!("a blob tree"),
    }
}

fn open_standard_tree(config: Config) -> crate::Result<Tree> {
    match config.open()? {
        AnyTree::Standard(tree) => Ok(tree),
        AnyTree::Blob(_) => panic!("a standard tree"),
    }
}

fn key(generation: u32, i: u32) -> String {
    format!("gen{generation}-key-{i:05}")
}

fn value(generation: u32, i: u32) -> Vec<u8> {
    format!("value-{i:05}-padding-to-make-it-longer-gen{generation}")
        .repeat(4)
        .into_bytes()
}

/// Writes one generation of 100 keys and flushes it.
fn write_generation(tree: &impl AbstractTree, generation: u32) -> crate::Result<()> {
    for i in 0u32..100 {
        tree.insert(
            key(generation, i).as_bytes(),
            value(generation, i),
            u64::from(generation * 1_000 + i),
        );
    }
    tree.flush_active_memtable(0)?;
    Ok(())
}

/// Every key of generations `0..generations` reads back its own value, by
/// point read and by scan.
fn assert_generations_read(tree: &impl AbstractTree, generations: u32) -> crate::Result<()> {
    for generation in 0..generations {
        for i in 0u32..100 {
            assert_eq!(
                tree.get(key(generation, i).as_bytes(), crate::MAX_SEQNO)?
                    .as_deref(),
                Some(value(generation, i).as_slice()),
                "generation {generation} key {i} by point read",
            );
        }
        let mut scanned = 0u32;
        for guard in tree.prefix(format!("gen{generation}-"), crate::MAX_SEQNO, None) {
            let (k, v) = guard.into_inner()?;
            let i = scanned;
            assert_eq!(k.as_ref(), key(generation, i).as_bytes());
            assert_eq!(v.as_ref(), value(generation, i).as_slice());
            scanned += 1;
        }
        assert_eq!(scanned, 100, "generation {generation} by scan");
    }
    Ok(())
}

/// The codec the newest blob file records.
fn newest_blob_codec(tree: &BlobTree) -> CompressionType {
    tree.current_version()
        .blob_files
        .iter()
        .max_by_key(|file| file.id())
        .map(crate::vlog::BlobFile::compression)
        .expect("a blob file exists")
}

#[test]
fn a_blob_compression_change_applies_to_the_next_blob_file() -> crate::Result<()> {
    // Each change reaches the next blob file written, and each file keeps the
    // codec it recorded, so every generation stays readable, including across
    // a reopen that supplies nothing.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let with_dict = CompressionType::zstd_dict(3, dict.id())?;

    {
        let tree = open_blob_tree(config(dir.path()).blob_compression(CompressionType::None))?;
        write_generation(&tree, 0)?;
        assert_eq!(newest_blob_codec(&tree), CompressionType::None);

        tree.register_zstd_dictionary(Arc::clone(&dict))?;
        tree.update_runtime_config(|c| c.blob_compression = with_dict)?;
        write_generation(&tree, 1)?;
        assert_eq!(newest_blob_codec(&tree), with_dict);

        tree.update_runtime_config(|c| c.blob_compression = CompressionType::Zstd(3))?;
        write_generation(&tree, 2)?;
        assert_eq!(newest_blob_codec(&tree), CompressionType::Zstd(3));

        tree.update_runtime_config(|c| c.blob_compression = CompressionType::None)?;
        write_generation(&tree, 3)?;
        assert_eq!(newest_blob_codec(&tree), CompressionType::None);

        assert_generations_read(&tree, 4)?;
    }

    let tree = open_blob_tree(config(dir.path()))?;
    assert_generations_read(&tree, 4)?;
    Ok(())
}

#[test]
fn a_compaction_filter_reads_every_blob_generation() -> crate::Result<()> {
    // A filter resolves the values it inspects out of blob files, which after
    // two policy changes hold three codecs, one of them a dictionary.
    struct ReadEveryValue(Arc<AtomicUsize>);

    impl CompactionFilter for ReadEveryValue {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &Context,
        ) -> crate::Result<Verdict> {
            let resolved = item.value()?;
            assert!(
                resolved.starts_with(b"value-"),
                "a blob value decodes under its own codec",
            );
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Verdict::Keep)
        }
    }

    struct ReadEveryValueFactory(Arc<AtomicUsize>);

    impl Factory for ReadEveryValueFactory {
        fn name(&self) -> &'static str {
            "read-every-value"
        }

        fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
            Box::new(ReadEveryValue(Arc::clone(&self.0)))
        }
    }

    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let read = Arc::new(AtomicUsize::new(0));
    let tree = open_blob_tree(
        config(dir.path())
            .blob_compression(CompressionType::None)
            .with_compaction_filter_factory(Some(Arc::new(ReadEveryValueFactory(Arc::clone(
                &read,
            ))))),
    )?;

    write_generation(&tree, 0)?;
    tree.register_zstd_dictionary(Arc::clone(&dict))?;
    tree.update_runtime_config(|c| {
        c.blob_compression = CompressionType::zstd_dict(3, dict.id()).expect("a valid level");
    })?;
    write_generation(&tree, 1)?;
    tree.update_runtime_config(|c| c.blob_compression = CompressionType::Zstd(3))?;
    write_generation(&tree, 2)?;

    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(
        read.load(Ordering::Relaxed),
        300,
        "the filter read every value"
    );
    assert_generations_read(&tree, 3)?;
    Ok(())
}

#[test]
fn a_relocation_after_a_live_blob_policy_change_keeps_each_file_decodable() -> crate::Result<()> {
    // GC relocation copies frames verbatim, so a file it writes out of a
    // generation older than the current policy must record that generation's
    // codec, a dictionary here, and still decode.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let with_dict = CompressionType::zstd_dict(3, dict.id())?;
    let tree = match config(dir.path())
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .staleness_threshold(0.0)
                .age_cutoff(1.0)
                .dict(Arc::clone(&dict)),
        ))
        .blob_compression(with_dict)
        .open()?
    {
        AnyTree::Blob(tree) => tree,
        AnyTree::Standard(_) => panic!("a blob tree"),
    };

    write_generation(&tree, 0)?;
    let first_files: Vec<u64> = tree
        .current_version()
        .blob_files
        .iter()
        .map(crate::vlog::BlobFile::id)
        .collect();

    tree.update_runtime_config(|c| c.blob_compression = CompressionType::None)?;
    // Overwrite half of the first generation, so its file carries dead bytes
    // and a relocation pass has a reason to rewrite it.
    let replacement = b"replacement-written-under-no-compression".repeat(4);
    for i in 0u32..50 {
        tree.insert(key(0, i).as_bytes(), &replacement, u64::from(10_000 + i));
    }
    tree.flush_active_memtable(0)?;

    // Which compaction relocates is a scheduling decision, so drive it until
    // the first generation's file is gone; never relocating fails the test.
    let mut relocated = false;
    for round in 0u32..5 {
        tree.insert(
            format!("filler-{round}").as_bytes(),
            &replacement,
            u64::from(20_000 + round),
        );
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 100_000)?;
        let live: Vec<u64> = tree
            .current_version()
            .blob_files
            .iter()
            .map(crate::vlog::BlobFile::id)
            .collect();
        if first_files.iter().any(|id| !live.contains(id)) {
            relocated = true;
            break;
        }
    }
    assert!(relocated, "the first generation's blob file was relocated");
    assert!(
        tree.current_version()
            .blob_files
            .iter()
            .any(|file| file.compression() == with_dict),
        "the relocated survivors keep the codec they were written under",
    );

    for i in 0u32..50 {
        assert_eq!(
            tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(replacement.as_slice()),
        );
    }
    for i in 50u32..100 {
        assert_eq!(
            tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(value(0, i).as_slice()),
            "a relocated value decodes under the dictionary it was written with",
        );
    }
    Ok(())
}

#[test]
fn a_data_block_policy_change_applies_to_the_next_flush_and_to_compaction() -> crate::Result<()> {
    // Tables record their own codec, so a change reaches the next flush and,
    // through compaction, the data already written, while every table written
    // before stays readable.
    let dir = tempfile::tempdir()?;
    let tree = open_standard_tree(
        config(dir.path())
            .data_block_compression_policy(CompressionPolicy::disabled())
            .index_block_compression_policy(CompressionPolicy::disabled()),
    )?;

    write_generation(&tree, 0)?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy = CompressionPolicy::all(CompressionType::Zstd(3));
        c.index_block_compression_policy = CompressionPolicy::all(CompressionType::Zstd(1));
    })?;
    write_generation(&tree, 1)?;

    let mut codecs: Vec<(u64, CompressionType, CompressionType)> = tree
        .current_version()
        .iter_tables()
        .map(|table| {
            (
                table.id(),
                table.metadata.data_block_compression,
                table.metadata.index_block_compression,
            )
        })
        .collect();
    codecs.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        codecs
            .iter()
            .map(|(_, data, index)| (*data, *index))
            .collect::<Vec<_>>(),
        vec![
            (CompressionType::None, CompressionType::None),
            (CompressionType::Zstd(3), CompressionType::Zstd(1)),
        ],
        "the flush before the change and the one after it",
    );

    tree.major_compact(u64::MAX, 0)?;
    for table in tree.current_version().iter_tables() {
        assert_eq!(
            table.metadata.data_block_compression,
            CompressionType::Zstd(3),
            "compaction rewrites the data under the current policy",
        );
    }
    assert_generations_read(&tree, 2)?;
    Ok(())
}

#[test]
fn an_update_naming_a_dictionary_the_tree_does_not_hold_changes_nothing() -> crate::Result<()> {
    // Refused before it is published: a flush under it would fail, or write
    // tables naming bytes the tree cannot store.
    let dir = tempfile::tempdir()?;
    let unheld = ZstdDictionary::new(&training_corpus()).id();
    let tree = open_blob_tree(config(dir.path()))?;
    let before = tree.runtime_config();

    for update in [
        (|c: &mut crate::runtime_config::RuntimeConfig, id: u32| {
            c.blob_compression = CompressionType::ZstdDict {
                level: 3,
                dict_id: id,
            };
        }) as fn(&mut crate::runtime_config::RuntimeConfig, u32),
        |c, id| {
            c.data_block_compression_policy = CompressionPolicy::all(CompressionType::ZstdDict {
                level: 3,
                dict_id: id,
            });
        },
    ] {
        let result = tree.update_runtime_config(|c| update(c, unheld));
        assert!(
            matches!(
                result,
                Err(crate::Error::ZstdDictMismatch { expected, got: None }) if expected == unheld
            ),
            "refused, naming the id: {result:?}",
        );
        assert_eq!(*tree.runtime_config(), *before, "and nothing was published");
    }
    Ok(())
}

#[test]
fn a_dictionary_a_writer_still_holds_is_not_collected() -> crate::Result<()> {
    // A flush or compaction keeps the runtime-config snapshot it started
    // under. A policy change in the meantime must not let a collection take
    // the dictionary that snapshot names: the writer is still compressing
    // against it, and its tables would name a dictionary the tree dropped.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let dict_id = dict.id();
    let tree = open_standard_tree(config(dir.path()))?;
    tree.register_zstd_dictionary(Arc::clone(&dict))?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy =
            CompressionPolicy::all(CompressionType::ZstdDict { level: 3, dict_id });
    })?;

    // What a writer started now would hold.
    let in_flight = tree.runtime_config();
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy = CompressionPolicy::disabled();
    })?;

    tree.collect_unreferenced_dictionaries()?;
    assert!(
        tree.current_version().dicts().contains(&dict_id),
        "still registered while the snapshot naming it is held",
    );

    drop(in_flight);
    tree.collect_unreferenced_dictionaries()?;
    assert!(
        !tree.current_version().dicts().contains(&dict_id),
        "collectable once no writer holds it",
    );
    Ok(())
}

#[test]
fn an_ingestion_started_under_a_replaced_policy_keeps_its_dictionary() -> crate::Result<()> {
    // The same, with a real writer: an ingestion resolves its dictionary when
    // it starts and writes against it until it finishes. A policy change and a
    // collection in between must leave it a dictionary to name, or its tables
    // fail to open on the next start.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let dict_id = dict.id();
    let with_dict = CompressionType::ZstdDict { level: 3, dict_id };

    {
        let tree = open_standard_tree(config(dir.path()))?;
        tree.register_zstd_dictionary(Arc::clone(&dict))?;
        tree.update_runtime_config(|c| {
            c.data_block_compression_policy = CompressionPolicy::all(with_dict);
        })?;

        let mut ingestion = crate::tree::ingest::Ingestion::new(&tree)?;
        tree.update_runtime_config(|c| {
            c.data_block_compression_policy = CompressionPolicy::disabled();
        })?;
        tree.collect_unreferenced_dictionaries()?;

        for i in 0u32..100 {
            ingestion.write(key(0, i).as_bytes().into(), value(0, i).into())?;
        }
        ingestion.finish()?;

        for table in tree.current_version().iter_tables() {
            assert_eq!(
                table.metadata.data_block_compression, with_dict,
                "written under the policy the ingestion started with",
            );
        }
        assert_generations_read(&tree, 1)?;
    }

    // Nothing supplied: the ingested tables resolve from the tree's own store.
    let tree = open_standard_tree(config(dir.path()))?;
    assert_generations_read(&tree, 1)?;
    Ok(())
}
