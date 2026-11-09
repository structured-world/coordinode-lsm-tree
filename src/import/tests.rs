use super::*;
use crate::{SequenceNumberCounter, ValueType};

/// Settings for a plain imported table: uncompressed blocks, no filter.
fn plain_settings(created_at: u128) -> TableSettings {
    TableSettings {
        data_compression: CompressionType::None,
        index_compression: CompressionType::None,
        data_restart_interval: 16,
        index_restart_interval: 1,
        encryption: None,
        #[cfg(zstd_any)]
        zstd_dictionary: None,
        recency: 1,
        created_at,
        kv_checksum: None,
        ecc: None,
        partitioned_index: false,
        seqno_bounds: false,
        zone_map: false,
        bulk_ingested: Some(false),
        lineage: TableLineage::default(),
        columnar: false,
        split_fields: false,
        filter: None,
        locator: None,
        restriction: None,
        highest_kv_seqno: None,
    }
}

fn rows(count: u32) -> Vec<InternalValue> {
    (0..count)
        .map(|i| {
            InternalValue::from_components(
                format!("k{i:03}"),
                format!("v{i}"),
                u64::from(i) + 1,
                ValueType::Value,
            )
        })
        .collect()
}

/// A table imported with an age, a lineage, seqno bounds and a zone map
/// records each of them, read back as an open reads it, and the tree opens
/// over it and serves its rows.
#[test]
fn imported_table_records_the_properties_it_was_given() -> crate::Result<()> {
    let folder = crate::get_tmp_folder();
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let tables = folder.path().join(crate::file::TABLES_FOLDER);
    std::fs::create_dir_all(&tables)?;
    let comparator: SharedComparator = Arc::new(crate::DefaultUserComparator);

    let lineage = TableLineage {
        inputs: Some(vec![5, 6]),
        prev: Some(4),
        transformed: true,
        last: true,
    };
    let mut settings = plain_settings(1_234_567_890);
    settings.seqno_bounds = true;
    settings.zone_map = true;
    settings.lineage = lineage.clone();
    let mut table = TableImport::create(
        tables.join("1"),
        1,
        6,
        fs.clone(),
        comparator.clone(),
        settings,
    )?;
    let rows = rows(20);
    table.append_block(rows.clone())?;
    let checksum = table.finish()?;

    let mut levels = vec![Vec::new(); 7];
    levels[6] = vec![vec![TablePlacement {
        id: 1,
        checksum,
        global_seqno: 0,
        recency: 1,
    }]];
    let recorded = install_manifest(
        folder.path(),
        &|_| tables.clone(),
        &ManifestImage {
            tree_type: TreeType::Standard,
            version_id: 0,
            levels,
            blob_files: Vec::new(),
            restrictions: Vec::new(),
            retention_floor: 0,
            dicts: Vec::new(),
            comparator_name: "default",
        },
        &fs,
        &comparator,
        None,
        #[cfg(zstd_any)]
        &crate::compression::ZstdDictionaries::new(),
    )?;
    assert_eq!(
        recorded,
        vec![RecordedTable {
            id: 1,
            columnar: false,
            split_fields: false,
            created_at: 1_234_567_890,
            kv_checksum: None,
            ecc: None,
            partitioned_index: false,
            seqno_bounds: true,
            zone_map: true,
            bulk_ingested: Some(false),
            lineage,
            blob_links: Vec::new(),
            restriction: None,
            seqnos: (1, 20),
            highest_kv_seqno: 20,
            block_layout: false,
            filter_bits: None,
        }]
    );

    let tree = crate::Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;
    for row in &rows {
        use crate::AbstractTree as _;
        assert_eq!(
            tree.get(&*row.key.user_key, SeqNo::MAX)?.as_deref(),
            Some(&*row.value)
        );
    }
    Ok(())
}

/// A blob tree installed from an image records each table's blob links, the
/// entries its values keep in shared blocks apart from frames of their own,
/// and carries over the stale bytes the image gives each blob file.
#[test]
#[expect(clippy::expect_used, reason = "test code")]
fn an_imported_blob_tree_records_its_links_and_stale_bytes() -> crate::Result<()> {
    use crate::AbstractTree as _;
    use crate::version::recovery::{log_id_of, snapshot_id_of};

    let folder = crate::get_tmp_folder();
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let comparator: SharedComparator = Arc::new(crate::DefaultUserComparator);
    let config = || {
        crate::Config::new(
            folder.path(),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        ))
    };
    // Small values, in a shared block of one blob file.
    let (table, blob_file) = {
        let crate::AnyTree::Blob(tree) = config().open()? else {
            panic!("a tree with kv separation opens as a blob tree");
        };
        for i in 0..16_u32 {
            tree.insert(format!("k{i:02}"), format!("value {i}"), u64::from(i));
        }
        tree.flush_active_memtable(0)?;
        let version = tree.current_version();
        let table = version.iter_tables().next().cloned().expect("one table");
        let blob_file = version
            .blob_files
            .iter()
            .next()
            .cloned()
            .expect("one blob file");
        (
            (table.id(), table.checksum()),
            (blob_file.id(), blob_file.checksum()),
        )
    };
    for entry in fs.read_dir(folder.path())? {
        if snapshot_id_of(&entry.file_name)
            .or_else(|| log_id_of(&entry.file_name))
            .is_some()
        {
            fs.remove_file(&entry.path)?;
        }
    }

    let stale = crate::blob_tree::FragmentationEntry::new(1, 7, 7);
    let tables = folder.path().join(crate::file::TABLES_FOLDER);
    let mut levels = vec![Vec::new(); 7];
    levels[0] = vec![vec![TablePlacement {
        id: table.0,
        checksum: table.1,
        global_seqno: 0,
        recency: table.0,
    }]];
    let recorded = install_manifest(
        folder.path(),
        &|_| tables.clone(),
        &ManifestImage {
            tree_type: TreeType::Blob,
            version_id: 0,
            levels,
            blob_files: vec![BlobFilePlacement {
                id: blob_file.0,
                checksum: blob_file.1,
                stale,
                live_from: 0,
            }],
            restrictions: Vec::new(),
            retention_floor: 0,
            dicts: Vec::new(),
            comparator_name: "default",
        },
        &fs,
        &comparator,
        None,
        #[cfg(zstd_any)]
        &crate::compression::ZstdDictionaries::new(),
    )?;
    let [recorded] = recorded.as_slice() else {
        panic!("one table recorded");
    };
    let [link] = recorded.blob_links.as_slice() else {
        panic!("one blob link");
    };
    assert_eq!((link.blob_file_id, link.len), (blob_file.0, 16));
    assert!(link.block_bytes > 0, "its values share a block");
    assert_eq!(link.on_disk_bytes, 0, "no value has a frame of its own");

    let crate::AnyTree::Blob(tree) = config().open()? else {
        panic!("a tree with kv separation opens as a blob tree");
    };
    assert_eq!(
        tree.current_version().gc_stats().get(&blob_file.0),
        Some(&stale)
    );
    assert_eq!(
        tree.get("k03", SeqNo::MAX)?.as_deref(),
        Some(&b"value 3"[..])
    );
    Ok(())
}

/// A valid-looking membership solution: 128 slots of `r = 8` in blocks of 64.
fn burr_image() -> BurrImage {
    BurrImage {
        kind: BurrKind::Membership,
        r: 8,
        w: 64,
        b: 64,
        root_seed: 7,
        layers: vec![BurrLayerImage {
            m: 128,
            thresholds: vec![0; 2],
            rows: vec![0; 128],
        }],
    }
}

/// A solution whose layers do not match their parameters is refused before
/// anything is encoded: each would make the probe read past its rows or its
/// thresholds, or answer from bits the result width does not have.
#[test]
fn burr_bytes_refuses_layers_that_do_not_match_their_parameters() {
    let layer_refused = |image: &BurrImage| {
        matches!(
            burr_bytes(image),
            Err(crate::Error::InvalidHeader("imported BuRR layer"))
        )
    };

    let mut short_rows = burr_image();
    short_rows.layers[0].rows.pop();
    assert!(layer_refused(&short_rows), "a row count other than m");

    let mut thresholds = burr_image();
    thresholds.layers[0].thresholds.push(0);
    assert!(
        layer_refused(&thresholds),
        "a threshold count other than m / b"
    );

    let mut wide_row = burr_image();
    wide_row.layers[0].rows[3] = 1 << 8;
    assert!(layer_refused(&wide_row), "a row with bits above r");

    let mut no_width = burr_image();
    no_width.r = 0;
    assert!(
        matches!(
            burr_bytes(&no_width),
            Err(crate::Error::InvalidHeader("imported BuRR parameters"))
        ),
        "a zero result width"
    );
}

/// A locator must hold a retrieval solution whose result splits exactly into
/// the block and slot bits it declares.
#[test]
fn locator_section_refuses_a_solution_it_cannot_address_with() {
    let refused = |image: &LocatorImage| {
        matches!(
            locator_section(image),
            Err(crate::Error::InvalidHeader("imported locator"))
        )
    };

    let membership = LocatorImage {
        precision: 0,
        block_id_bits: 4,
        slot_bits: 4,
        solution: burr_image(),
    };
    assert!(refused(&membership), "a membership solution");

    let mut solution = burr_image();
    solution.kind = BurrKind::Retrieval;
    let wrong_split = LocatorImage {
        precision: 0,
        block_id_bits: 4,
        slot_bits: 3,
        solution,
    };
    assert!(refused(&wrong_split), "block and slot bits other than r");
}
