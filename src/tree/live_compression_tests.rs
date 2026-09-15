// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Compression policies changed on a live tree through `update_runtime_config`,
//! checked against the codec each file records. A blob file's codec is not part
//! of the public surface, which is why these live in the crate.

use crate::compaction::filter::{CompactionFilter, Context, Factory, ItemAccessor, Verdict};
use crate::compression::ZstdDictionary;
use crate::config::{CompressionPolicy, KvSeparationOptions};
use crate::iter_guard::IterGuard;
use crate::runtime_config::RuntimeConfig;
use crate::{
    AbstractTree, AnyTree, BlobTree, CompressionType, Config, SequenceNumberCounter, Tree,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
fn blob_compression_updated_on_a_live_tree_applies_to_the_next_blob_file() -> crate::Result<()> {
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
fn compaction_filter_over_three_blob_codecs_reads_every_value() -> crate::Result<()> {
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
fn blob_relocation_after_a_policy_change_keeps_each_file_decodable() -> crate::Result<()> {
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
fn data_block_policy_updated_on_a_live_tree_applies_to_flush_and_compaction() -> crate::Result<()> {
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
fn policy_update_naming_an_unheld_dictionary_is_refused_unpublished() -> crate::Result<()> {
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
fn dictionary_named_by_a_held_snapshot_survives_a_collection() -> crate::Result<()> {
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
fn ingestion_with_its_policy_replaced_while_writing_keeps_its_dictionary() -> crate::Result<()> {
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

/// Arms the window between a write finishing its files and installing them
/// with a policy change away from the dictionary and a collection, which is the
/// last moment the dictionary can be taken from the write. Returns whether the
/// window was reached, so a test cannot pass by never entering it.
fn replace_policy_and_collect_before_install(
    tree: &Tree,
    replace: fn(&mut RuntimeConfig),
) -> Arc<AtomicBool> {
    let reached = Arc::new(AtomicBool::new(false));
    let (tree_in_hook, reached_in_hook) = (tree.clone(), Arc::clone(&reached));
    tree.config.arm_before_output_install(move || {
        tree_in_hook
            .update_runtime_config(replace)
            .expect("the replacement names no dictionary");
        tree_in_hook
            .collect_unreferenced_dictionaries()
            .expect("a collection");
        reached_in_hook.store(true, Ordering::SeqCst);
    });
    reached
}

fn disable_data_dictionary(c: &mut RuntimeConfig) {
    c.data_block_compression_policy = CompressionPolicy::disabled();
}

fn disable_blob_dictionary(c: &mut RuntimeConfig) {
    c.blob_compression = CompressionType::None;
}

/// Registers `dict` and compresses the data blocks written from now on against
/// it at every level.
fn use_data_dictionary(tree: &Tree, dict: &Arc<ZstdDictionary>) -> crate::Result<()> {
    let with_dict = CompressionType::zstd_dict(3, dict.id())?;
    tree.register_zstd_dictionary(Arc::clone(dict))?;
    tree.update_runtime_config(|c| {
        c.data_block_compression_policy = CompressionPolicy::all(with_dict);
    })
}

/// A standard tree whose data blocks compress against `dict` at every level.
fn open_with_data_dictionary(config: Config, dict: &Arc<ZstdDictionary>) -> crate::Result<Tree> {
    let tree = open_standard_tree(config)?;
    use_data_dictionary(&tree, dict)?;
    Ok(tree)
}

/// A blob tree whose blob files compress against `dict`.
fn open_with_blob_dictionary(
    config: Config,
    dict: &Arc<ZstdDictionary>,
) -> crate::Result<BlobTree> {
    let with_dict = CompressionType::zstd_dict(3, dict.id())?;
    let tree = open_blob_tree(config.blob_compression(CompressionType::None))?;
    tree.register_zstd_dictionary(Arc::clone(dict))?;
    tree.update_runtime_config(|c| c.blob_compression = with_dict)?;
    Ok(tree)
}

#[test]
fn flush_with_its_policy_replaced_before_install_keeps_its_dictionary() -> crate::Result<()> {
    // A flush has finished its tables but not installed them. A policy change
    // and a collection landing there must still find the flush holding the
    // dictionary its tables name, or they are installed naming one the tree
    // dropped and fail to open on the next start.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));

    {
        let tree = open_with_data_dictionary(config(dir.path()), &dict)?;
        let reached = replace_policy_and_collect_before_install(&tree, disable_data_dictionary);
        write_generation(&tree, 0)?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the installed tables name it",
        );
        assert_generations_read(&tree, 1)?;
    }

    let tree = open_standard_tree(config(dir.path()))?;
    assert_generations_read(&tree, 1)?;
    Ok(())
}

#[test]
fn blob_flush_with_its_policy_replaced_before_install_keeps_its_dictionary() -> crate::Result<()> {
    // The same for the blob files of a key-value separated flush.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));

    {
        let tree = open_with_blob_dictionary(config(dir.path()), &dict)?;
        let reached =
            replace_policy_and_collect_before_install(&tree.index, disable_blob_dictionary);
        write_generation(&tree, 0)?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert_eq!(
            newest_blob_codec(&tree),
            CompressionType::zstd_dict(3, dict.id())?
        );
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the installed blob files name it",
        );
        assert_generations_read(&tree, 1)?;
    }

    let tree = open_blob_tree(config(dir.path()))?;
    assert_generations_read(&tree, 1)?;
    Ok(())
}

#[test]
fn ingestion_with_its_policy_replaced_before_install_keeps_its_dictionary() -> crate::Result<()> {
    // An ingestion finishes its writer before it takes the locks the install
    // needs, so the window is inside `finish` itself.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));

    {
        let tree = open_with_data_dictionary(config(dir.path()), &dict)?;
        let mut ingestion = crate::tree::ingest::Ingestion::new(&tree)?;
        for i in 0u32..100 {
            ingestion.write(key(0, i).as_bytes().into(), value(0, i).into())?;
        }
        let reached = replace_policy_and_collect_before_install(&tree, disable_data_dictionary);
        ingestion.finish()?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the ingested tables name it",
        );
        assert_generations_read(&tree, 1)?;
    }

    let tree = open_standard_tree(config(dir.path()))?;
    assert_generations_read(&tree, 1)?;
    Ok(())
}

#[test]
fn blob_ingestion_with_its_policy_replaced_before_install_keeps_its_dictionary() -> crate::Result<()>
{
    // The same for the blob files of a key-value separated ingestion.
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));

    {
        let tree = open_with_blob_dictionary(config(dir.path()), &dict)?;
        let mut ingestion = crate::blob_tree::ingest::BlobIngestion::new(&tree)?;
        for i in 0u32..100 {
            ingestion.write(key(0, i).as_bytes().into(), value(0, i).into())?;
        }
        let reached =
            replace_policy_and_collect_before_install(&tree.index, disable_blob_dictionary);
        ingestion.finish()?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the ingested blob files name it",
        );
        assert_generations_read(&tree, 1)?;
    }

    let tree = open_blob_tree(config(dir.path()))?;
    assert_generations_read(&tree, 1)?;
    Ok(())
}

#[cfg(feature = "parallel")]
#[test]
fn subcompaction_with_its_policy_replaced_before_install_keeps_its_dictionary() -> crate::Result<()>
{
    // Parallel sub-compactions finish their outputs on their own threads and
    // install them together afterwards, so the window spans all of them. The
    // inputs are written without the dictionary: an input still in the version
    // would name it and keep it out of the collection by itself.
    const N: u32 = 4_000;
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let make_config = || {
        config(dir.path())
            .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
            .compaction_threads(4)
            .subcompaction_min_bytes(0)
    };

    {
        let tree = open_standard_tree(make_config())?;
        // Several tables at the bottom level give the next compaction split
        // points; the overwrite in L0 gives it something to merge into them.
        for i in 0..N {
            tree.insert(key(0, i).as_bytes(), value(0, i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(4_096, 0)?;
        for i in 0..N {
            tree.insert(key(0, i).as_bytes(), value(1, i), u64::from(N + i));
        }
        tree.flush_active_memtable(0)?;
        use_data_dictionary(&tree, &dict)?;

        let reached = replace_policy_and_collect_before_install(&tree, disable_data_dictionary);
        tree.major_compact(u64::MAX, 0)?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the compacted tables name it",
        );
        for i in 0..N {
            assert_eq!(
                tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
                Some(value(1, i).as_slice()),
            );
        }
    }

    let tree = open_standard_tree(make_config())?;
    for i in 0..N {
        assert_eq!(
            tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(value(1, i).as_slice()),
        );
    }
    Ok(())
}

#[cfg(feature = "parallel")]
#[test]
fn filter_blob_files_with_their_policy_replaced_before_install_keep_their_dictionary()
-> crate::Result<()> {
    // A compaction filter that replaces a separated value writes it to a blob
    // file of its own, under the blob policy of the moment it opens that file,
    // and the file reaches the install beside the sub-compaction's tables. The
    // filter moves the policy to the dictionary on its first item, after the
    // table writers took theirs, so its blob files are the only output whose
    // write named the dictionary.
    type Arming = Arc<std::sync::Mutex<Option<(Tree, CompressionType)>>>;

    struct Rewrite(Arming);

    impl CompactionFilter for Rewrite {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _ctx: &Context,
        ) -> crate::Result<Verdict> {
            let armed = self.0.lock().expect("unpoisoned").take();
            if let Some((tree, with_dict)) = armed {
                tree.update_runtime_config(|c| c.blob_compression = with_dict)?;
            }
            Ok(Verdict::ReplaceValue(rewritten(item.key()).into()))
        }
    }

    struct RewriteFactory(Arming);

    impl Factory for RewriteFactory {
        fn name(&self) -> &'static str {
            "rewrite"
        }

        fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
            Box::new(Rewrite(Arc::clone(&self.0)))
        }
    }

    fn rewritten(key: &[u8]) -> Vec<u8> {
        let mut out = b"rewritten-".to_vec();
        out.extend_from_slice(key);
        out.repeat(4)
    }

    const N: u32 = 4_000;
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let arming: Arming = Arc::new(std::sync::Mutex::new(None));
    let make_config = || {
        config(dir.path())
            .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
            .compaction_threads(4)
            .subcompaction_min_bytes(0)
            .blob_compression(CompressionType::None)
            .with_compaction_filter_factory(Some(Arc::new(RewriteFactory(Arc::clone(&arming)))))
    };

    {
        let tree = open_blob_tree(make_config())?;
        for i in 0..N {
            tree.insert(key(0, i).as_bytes(), value(0, i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        tree.major_compact(4_096, 0)?;
        for i in 0..N {
            tree.insert(key(0, i).as_bytes(), value(1, i), u64::from(N + i));
        }
        tree.flush_active_memtable(0)?;

        tree.register_zstd_dictionary(Arc::clone(&dict))?;
        arming.lock().expect("unpoisoned").replace((
            tree.index.clone(),
            CompressionType::zstd_dict(3, dict.id())?,
        ));
        let reached =
            replace_policy_and_collect_before_install(&tree.index, disable_blob_dictionary);
        tree.major_compact(u64::MAX, 0)?;
        assert!(
            arming.lock().expect("unpoisoned").is_none(),
            "the filter moved the policy",
        );

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the filter's blob files name it",
        );
        for i in 0..N {
            let k = key(0, i);
            assert_eq!(
                tree.get(k.as_bytes(), crate::MAX_SEQNO)?.as_deref(),
                Some(rewritten(k.as_bytes()).as_slice()),
            );
        }
    }

    let tree = open_blob_tree(make_config())?;
    for i in 0..N {
        let k = key(0, i);
        assert_eq!(
            tree.get(k.as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(rewritten(k.as_bytes()).as_slice()),
        );
    }
    Ok(())
}

#[test]
fn tight_space_slice_with_its_policy_replaced_before_install_keeps_its_dictionary()
-> crate::Result<()> {
    // A tight-space compaction installs slice by slice, each after its outputs
    // are finished, so the window reopens on every slice. The input is written
    // without the dictionary, as in the sub-compaction case.
    const N: u32 = 2_000;
    let dir = tempfile::tempdir()?;
    let dict = Arc::new(ZstdDictionary::new(&training_corpus()));
    let fs: Arc<dyn crate::fs::Fs> = Arc::new(crate::fs::MemFs::with_capacity(u64::MAX));
    let make_config = || {
        config(dir.path())
            .data_block_size_policy(crate::config::BlockSizePolicy::all(512))
            .with_shared_fs(Arc::clone(&fs))
    };

    {
        let tree = open_standard_tree(make_config())?;
        for i in 0..N {
            tree.insert(key(0, i).as_bytes(), value(0, i), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        use_data_dictionary(&tree, &dict)?;

        // A quota with less headroom than the merge's output needs, which is
        // what makes the compaction reclaim in slices.
        let used = crate::storage_stats::compute_used_bytes(&tree.current_version())?;
        tree.update_runtime_config(|c| {
            c.storage_admission_check = true;
            c.tight_space_compaction = true;
            c.storage_limit_bytes = Some(used + used / 4);
        })?;

        let reached = replace_policy_and_collect_before_install(&tree, disable_data_dictionary);
        tree.major_compact(64 * 1024 * 1024, 0)?;

        assert!(reached.load(Ordering::SeqCst), "the window was reached");
        assert!(
            tree.current_version().dicts().contains(&dict.id()),
            "still registered: the slice outputs name it",
        );
        for i in 0..N {
            assert_eq!(
                tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
                Some(value(0, i).as_slice()),
            );
        }
    }

    let tree = open_standard_tree(make_config())?;
    for i in 0..N {
        assert_eq!(
            tree.get(key(0, i).as_bytes(), crate::MAX_SEQNO)?.as_deref(),
            Some(value(0, i).as_slice()),
        );
    }
    Ok(())
}
