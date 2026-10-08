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
    let payload = crate::table::DataBlock::encode_into_vec(&rows, 16, 0.0)?;
    table.append_block(&payload, u32::try_from(payload.len()).unwrap(), &rows)?;
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

/// A restricted blob file's reclaimed prefix is skipped, not written: on a
/// filesystem with sparse files it takes no space while the import runs, so
/// a large frontier needs no room for it.
#[cfg(unix)]
#[test]
fn restricted_blob_file_import_leaves_its_prefix_unwritten() -> crate::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let folder = crate::get_tmp_folder();
    let path = folder.path().join("1");
    let live_from = 64 * 1024 * 1024;
    let fs: Arc<dyn Fs> = Arc::new(crate::fs::StdFs);
    let mut file = BlobFileImport::create(
        &path,
        1,
        fs,
        CompressionType::None,
        1,
        Some(BlobFileRestriction {
            live_from,
            item_count: 2,
            compressed_bytes: 10,
            uncompressed_bytes: 10,
            first_key: UserKey::from("a"),
            last_key: UserKey::from("b"),
        }),
    )?;
    file.append(live_from, b"b", 1, b"value", 5)?;
    let allocated = std::fs::metadata(&path)?.blocks() * 512;
    assert!(
        allocated < live_from / 16,
        "{allocated} bytes allocated for a {live_from}-byte prefix"
    );
    file.finish()?;
    Ok(())
}
