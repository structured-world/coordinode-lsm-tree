// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

mod cut_alignment;
mod tombstone_share;

pub use cut_alignment::Boundary;

use super::{filter::BloomConstructionPolicy, writer::Writer};
use crate::{
    Checksum, CompressionType, TableId, UserKey,
    blob_tree::handle::BlobIndirection,
    encryption::EncryptionProvider,
    fs::{Fs, SyncMode},
    prefix::PrefixExtractor,
    range_tombstone::RangeTombstone,
    table::writer::{LinkedBlobFiles, LinkedFile},
    value::InternalValue,
    vlog::BlobFileId,
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};

use crate::path::PathBuf;

/// Like `Writer` but will rotate to a new table, once a table grows larger than `target_size`
///
/// This results in a sorted "run" of tables
#[expect(
    clippy::struct_excessive_bools,
    reason = "writer config: each bool is an independent feature toggle carried \
              across table rotations (partitioned filter, range-tombstone clip, \
              seqno-in-index, disable-CoW); enums would obscure the per-feature wiring"
)]
pub struct MultiWriter {
    pub(crate) fs: Arc<dyn Fs>,

    pub(crate) base_path: PathBuf,

    data_block_hash_ratio: f32,

    data_block_size: u32,

    index_partition_size: u32,

    filter_partition_size: u32,

    row_group_size: u32,

    columnar_page_size: u32,

    column_encoding: crate::config::ColumnEncoding,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    use_partitioned_filter: bool,

    /// `Some(threshold)` selects the size-adaptive index (single-level
    /// until the index exceeds `threshold` bytes, then a streaming
    /// partitioned index). `None` leaves the writer's default single-level
    /// index. Re-applied to each rotated table writer. Pure
    /// always-partition is `Some(0)` (spill on the first entry).
    index_spill_threshold: Option<u64>,

    /// Target size of tables in bytes
    ///
    /// If a table reaches the target size, a new one is started,
    /// resulting in a sorted "run" of tables
    pub target_size: u64,

    results: Vec<(TableId, Checksum)>,

    /// Where each output's file comes from.
    files: TableFiles,

    /// Whether every output's directory entry was durable before its table
    /// was written, so the install that names the outputs syncs no folder for
    /// them.
    entries_durable: bool,

    /// The first output's id when its file was made as this writer started,
    /// the id taken from the counter then; `None` when it came from a pool,
    /// whose ids were taken when their batch was made.
    first_made_now: Option<TableId>,

    pub writer: Writer,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    bloom_policy: BloomConstructionPolicy,

    /// Sizes every output's filters against the tree's filter budget.
    filter_sizing: Option<crate::filter_budget::FilterPlan>,

    current_key: Option<UserKey>,
    comparator: crate::SharedComparator,

    linked_blobs: LinkedBlobFiles,

    /// The blob objects the current table's cell rows own, as `(blob file,
    /// offset)`: what dropping the table releases, handed to its writer at
    /// rotation for its `owned_blob_objects` section.
    owned_objects: Vec<(BlobFileId, u64)>,

    /// Range tombstones to distribute across output tables, ordered by start.
    /// During compaction these are clipped to each table's key range; during
    /// flush each is cut into the zones of the outputs it spans, the first and
    /// last zones open-ended so they still cover keys in older SSTs.
    range_tombstones: Vec<RangeTombstone>,

    /// When true, range tombstones are clipped to each output table's KV key range
    /// via `intersect_opt`. This is correct for compaction where the inputs held
    /// every key a tombstone covers (the pieces past the outputs' keys that a
    /// table outside the compaction still needs are kept by `outside`), and wrong
    /// for flush (RTs must cover keys in older SSTs outside the memtable's range).
    clip_range_tombstones: bool,

    /// Under clipping, the tombstones a table outside the compaction may still
    /// need: their pieces past the outputs' keys go to outputs of their own.
    outside: Option<TombstonesOutside>,

    /// The first key of the run's first output, once it closes.
    first_output_key: Option<UserKey>,

    /// The tombstones the gaps between key domains meet, from the last gap on.
    gap_cursor: tombstone_share::GapCursor,

    /// The current output's first key, the lower bound of its share of the
    /// range tombstones; `None` for the first output.
    output_lower: Option<UserKey>,

    /// The bytes of the current output's share of the range tombstones.
    tombstone_share: tombstone_share::TombstoneShare,

    /// The current output's held state and tombstone bytes at its first
    /// record, the tombstones carried into it included: what every output
    /// carries, which closing one does not shed. `None` until the output
    /// takes a record.
    output_base: Option<(u64, u64)>,

    /// Level the tables are written to
    initial_level: u8,

    prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    /// The key domains no output spans, and the current output's.
    domain: Option<crate::key_domain::DomainCursor>,

    encryption: Option<Arc<dyn EncryptionProvider>>,

    /// The tree's clock, handed to every successor [`Writer`].
    clock: Option<Arc<dyn crate::Clock>>,

    /// Resolved Page ECC scheme — preserved here so the rotation path
    /// can stamp the same scheme on every successor [`Writer`].
    ecc: Option<crate::table::block::EccParams>,

    /// `Config::sync_mode` — preserved so every successor [`Writer`]
    /// finishes its SST with the same durability level.
    sync_mode: SyncMode,

    /// `Config::writeback_bytes`, preserved for every successor [`Writer`].
    writeback_bytes: u64,

    /// Per-KV checksum policy + algorithm (from the runtime
    /// `kv_checksums` config) — preserved here so the rotation path
    /// stamps the same setting on every successor [`Writer`].
    kv_checksum: Option<(
        crate::runtime_config::KvChecksumPolicy,
        crate::runtime_config::ChecksumAlgorithm,
    )>,

    /// `seqno_in_index` runtime config — preserved here so the rotation
    /// path sets the same flag on every successor [`Writer`], so all SSTs
    /// of one flush / compaction uniformly emit (or omit) the `seqno_bounds`
    /// section.
    use_seqno_in_index: bool,

    /// Preserved like `use_seqno_in_index` so every successor [`Writer`] of one
    /// flush / compaction uniformly emits (or omits) the `zone_map` section.
    use_zone_map: bool,

    /// Preserved across writer rotation so every successor [`Writer`] of one
    /// flush / compaction compresses under the same strategy.
    #[cfg(zstd_any)]
    use_zstd_two_pass_seed: bool,

    /// Preserved across writer rotation so every successor [`Writer`] of one
    /// flush / compaction uniformly writes columnar (or row-major) data blocks.
    use_columnar: bool,

    /// How the current output of a columnar write stores each value: whole
    /// after a row, split after an ingested batch, `None` before either. A
    /// table records one layout, so a write of the other one rotates first.
    value_layout: Option<crate::table::meta::ValueLayout>,

    /// Preserved across writer rotation: every table of a tree whose rows may
    /// be written as cells splits them into their fields (see
    /// [`Writer::use_cell_rows`]).
    cell_rows: bool,

    /// Preserved across writer rotation so every successor [`Writer`] of one
    /// bulk ingest is uniformly flagged bulk-ingested (see
    /// [`Writer::use_bulk_ingested`]).
    bulk_ingested: bool,

    /// L0 recency key stamped on every successor [`Writer`], preserved across
    /// rotation (see [`Writer::use_recency`]): a compaction sets its inputs'
    /// highest recency so manifest repair can place each output where its
    /// content belongs, not where its id falls, and a flush the id it takes
    /// when it starts, since a pooled file's id was taken when its batch was
    /// made. `None` (an ingest, whose install sets the recency) stamps each
    /// output with ITS OWN id.
    recency: Option<TableId>,

    /// Compaction lineage stamped on this and every rotated successor (see
    /// [`Writer::use_lineage`]): the input ids a compaction merges, identical
    /// for every output of the run. `None` (flush / ingest) omits the key.
    lineage: Option<Vec<TableId>>,

    /// The compaction inputs every output takes its age from (see
    /// [`Writer::finish_deferring_dir_sync`]); `None` for a flush or an ingest.
    inherited_age: Option<super::writer::AgeSweep>,

    /// Counter of compaction-filter TRANSFORMATIONS (any non-`Keep` verdict),
    /// shared with the filter adapter. An output whose window saw one is not
    /// derivable from its inputs, so it is marked transformed before its meta
    /// is written (see [`Writer::mark_lineage_transformed`]); untouched
    /// outputs of the same run stay plain. `None` — no filter configured.
    transform_marker: Option<alloc::sync::Arc<portable_atomic::AtomicU64>>,

    /// The marker's value when the CURRENT output started, so each output is
    /// judged by the verdicts of its own window alone.
    transforms_at_output_start: u64,

    /// This writer receives the run's ENTIRE merged stream (a serial
    /// compaction), so its final output may carry the `lineage_last` marker.
    /// `false` for parallel sub-compactions and tight-space slices: several
    /// writers share one lineage there, and a slice's last output closing
    /// the run would let a surviving slice supersede inputs whose records
    /// live only in a LOST sibling slice.
    owns_whole_run: bool,

    /// The id of the writer CURRENTLY receiving records, so rotation can
    /// stamp the successor's `lineage_prev` adjacency link.
    current_writer_id: TableId,

    /// The marker's value observed AFTER the last record actually written.
    /// Rotation happens BEFORE the triggering record is inserted, so at that
    /// moment the live counter already includes verdicts belonging to the NEW
    /// output (the triggering record's own, and any removals between the two
    /// writes — the size threshold is a property of the finished old output,
    /// so every verdict since its last write belongs to the successor). The
    /// finishing output is therefore judged by this milestone, never the live
    /// counter.
    transforms_after_last_write: u64,

    /// Delete strategy applied to every successor [`Writer`], preserved across
    /// rotation. Under copy-on-write the writers persist no delete-bitmap; under
    /// merge-on-read / adaptive a populated bitmap is written.
    delete_strategy: crate::config::DeleteStrategy,

    /// When `true`, each output SST has per-file copy-on-write cleared at
    /// creation (Btrfs `FS_NOCOW_FL`) so write-once SSTs avoid the
    /// copy-on-write fragmentation penalty. Preserved across rotations so every successor
    /// table in the run is flagged the same way. No-op on non-CoW filesystems.
    disable_cow_on_sst: bool,

    /// Resolved retrieval-ribbon locator policy entry for this run's level,
    /// preserved here so the rotation path stamps the same setting on every
    /// successor [`Writer`]. Each table gets its own per-SST locator section
    /// (block ordinals reset per table). Defaults to `None` (disabled).
    locator_entry: crate::config::LocatorPolicyEntry,

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Optional parallel block compression, preserved here so every successor
    /// [`Writer`] of a rotated run shares the same pool and settings.
    #[cfg(feature = "std")]
    parallel: Option<crate::table::writer::ParallelCompression>,

    /// Where a compaction learns of every table file this writer creates, so
    /// it can remove the ones it never installs. `None` outside compaction.
    outputs: Option<crate::compaction::output_ledger::OutputLedger>,

    /// The boundaries of the level a compaction's outputs are merged into
    /// next, which an output prefers to end on. `None` cuts at the target
    /// size alone.
    alignment: Option<cut_alignment::CutAlignment>,
}

/// What a row a page of a partly carried group re-encodes may add past its key
/// and value: its fixed fields (sequence number, value type, offsets) and its
/// share of the rebuilt page's framing, generously.
#[cfg(feature = "columnar")]
const REBUILT_ROW_OVERHEAD: u64 = 64;

/// A bound on the bytes a partly carried group of `rows` writes, its source
/// group `raw_len` bytes: the copied pages come out of the source, and the
/// pages rebuilt from rows can grow past it (a value a filter replaced, a
/// merged one), so every row counts at its key and value plus
/// [`REBUILT_ROW_OVERHEAD`].
#[cfg(feature = "columnar")]
fn partly_carried_bound(raw_len: usize, rows: &[InternalValue]) -> u64 {
    let rebuilt: u64 = rows
        .iter()
        .map(|row| row.key.user_key.len() as u64 + row.value.len() as u64 + REBUILT_ROW_OVERHEAD)
        .sum();
    raw_len as u64 + rebuilt
}

/// Range tombstones of a compaction that a table outside it overlaps, and the
/// keys one writer of the compaction answers for.
///
/// Clipped to the outputs' keys, a tombstone loses its pieces where the
/// compaction writes no key. Where its inputs held every covered key, that is
/// safe; where a table outside the compaction holds one, the piece is all
/// that keeps it deleted, so the writer answering for that span keeps it.
pub struct TombstonesOutside {
    /// Ordered by start.
    pub tombstones: Vec<RangeTombstone>,
    /// The least key the writer answers for; `None` from the start of the key
    /// space.
    pub lower: Option<UserKey>,
    /// The first key past those the writer answers for; `None` to the end of
    /// the key space.
    pub upper: Option<UserKey>,
}

/// Which side of a writer's outputs a piece of a range tombstone lies on.
#[derive(Clone, Copy)]
enum Beside {
    /// Below the first output's first key.
    Below,
    /// Past the last output's last key.
    Above,
}

/// What the current table weighs, measured once and judged against any size
/// target: by its bytes, counting what `finish` appends, or by the state it
/// holds until `finish`, which is judged against the target whatever size the
/// table is cut at, since that bounds memory.
#[derive(Clone, Copy, Debug)]
struct OutputLoad {
    size: u64,
    held: u64,
    /// The least size target its bytes are judged against; `None` while the
    /// table holds no content its bytes could be cut for.
    size_floor: Option<u64>,
    held_target: u64,
}

impl OutputLoad {
    /// Whether the table is full at `size_target`.
    fn reaches(self, size_target: u64) -> bool {
        self.size_floor
            .is_some_and(|floor| self.size >= size_target.max(floor))
            || self.held >= self.held_target
    }
}

/// Where a [`MultiWriter`]'s output files come from: its folder's pool, each
/// id above `above` when given.
struct TableFiles {
    pools: Arc<super::file_pool::TableFilePools>,
    above: Option<TableId>,
    /// Whether an output was opened already: only the first refills the
    /// pool (see [`super::file_pool::TableFilePools::claim`]).
    opened: bool,
}

impl TableFiles {
    /// A writer for the next output in `folder`, and whether its file's
    /// directory entry is durable already.
    fn open(
        &mut self,
        folder: &crate::path::Path,
        level: u8,
        fs: &Arc<dyn Fs>,
    ) -> crate::Result<(Writer, bool)> {
        let may_refill = !self.opened;
        self.opened = true;
        let claimed = self.pools.claim(folder, fs, self.above, may_refill)?;
        let durable = claimed.durable_entry;
        Ok((Writer::into_claimed(claimed, level, fs.clone())?, durable))
    }
}

impl MultiWriter {
    /// A `MultiWriter` whose outputs take their ids from `table_id_generator`
    /// and their files as they start, with no pool.
    #[cfg(test)]
    pub fn new(
        base_path: PathBuf,
        table_id_generator: crate::SequenceNumberCounter,
        target_size: u64,
        initial_level: u8,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let pools = super::file_pool::TableFilePools::new(table_id_generator, 0, SyncMode::Normal);
        Self::from_pool(
            base_path,
            Arc::new(pools),
            None,
            target_size,
            initial_level,
            fs,
        )
    }

    /// Sets up a `MultiWriter` at the given tables folder, its outputs written
    /// into files claimed from `pools`, each output's id above `above` when
    /// given.
    pub(crate) fn from_pool(
        base_path: PathBuf,
        pools: Arc<super::file_pool::TableFilePools>,
        above: Option<TableId>,
        target_size: u64,
        initial_level: u8,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        let mut files = TableFiles {
            pools,
            above,
            opened: false,
        };
        // Own-id recency until `use_recency` overrides it (see the `recency`
        // field).
        let (writer, entries_durable) = files.open(&base_path, initial_level, &fs)?;
        let current_table_id = writer.table_id;

        Ok(Self {
            fs,
            initial_level,

            base_path,

            data_block_hash_ratio: 0.0,

            data_block_size: 4_096,

            index_partition_size: 4_096,

            filter_partition_size: 4_096,

            row_group_size: crate::config::DEFAULT_COLUMNAR_ROW_GROUP_SIZE,

            columnar_page_size: crate::config::DEFAULT_COLUMNAR_PAGE_SIZE,
            column_encoding: crate::config::ColumnEncoding::Plain,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            target_size,
            results: Vec::new(),
            files,
            entries_durable,
            first_made_now: (!entries_durable).then_some(current_table_id),
            writer,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            use_partitioned_filter: false,
            index_spill_threshold: None,

            bloom_policy: BloomConstructionPolicy::default(),
            filter_sizing: None,

            current_key: None,
            comparator: crate::comparator::default_comparator(),

            linked_blobs: LinkedBlobFiles::default(),
            owned_objects: Vec::new(),
            range_tombstones: Vec::new(),
            clip_range_tombstones: false,
            outside: None,
            first_output_key: None,
            gap_cursor: tombstone_share::GapCursor::default(),
            output_lower: None,
            tombstone_share: tombstone_share::TombstoneShare::new(),
            output_base: None,

            prefix_extractor: None,
            domain: None,

            encryption: None,
            clock: None,

            ecc: None,
            sync_mode: SyncMode::Normal,
            writeback_bytes: 0,

            kv_checksum: None,
            use_seqno_in_index: false,
            use_zone_map: false,
            #[cfg(zstd_any)]
            use_zstd_two_pass_seed: true,
            use_columnar: false,
            value_layout: None,
            cell_rows: false,
            bulk_ingested: false,
            recency: None,
            lineage: None,
            inherited_age: None,
            transform_marker: None,
            transforms_at_output_start: 0,
            transforms_after_last_write: 0,
            owns_whole_run: false,
            current_writer_id: current_table_id,
            delete_strategy: crate::config::DeleteStrategy::default(),
            disable_cow_on_sst: false,
            locator_entry: crate::config::LocatorPolicyEntry::None,

            #[cfg(zstd_any)]
            zstd_dictionary: None,

            #[cfg(feature = "std")]
            parallel: None,

            outputs: None,

            alignment: None,
        })
    }

    /// Prefers to end outputs on `boundaries`, the boundaries between the
    /// tables of the level the outputs are merged into next, sorted by the
    /// writer's comparator (see `cut_alignment`); `upper`, when known, is the
    /// last key the writer can be handed. An empty list cuts at the target size
    /// alone.
    #[must_use]
    pub(crate) fn use_cut_alignment(
        mut self,
        boundaries: Arc<[Boundary]>,
        upper: Option<UserKey>,
    ) -> Self {
        self.alignment =
            (!boundaries.is_empty()).then(|| cut_alignment::CutAlignment::new(boundaries, upper));
        self
    }

    /// Bounds the keys this writer can be handed by `upper`: a writer of one
    /// key range of a compaction does not wait for a boundary past it.
    pub(crate) fn limit_cut_alignment(&mut self, upper: UserKey) {
        if let Some(alignment) = &mut self.alignment {
            alignment.limit(upper);
        }
    }

    /// Whether the current output ends before `key`, the next new key, and
    /// where `key` falls among the boundaries, which [`Self::commit_cut`] takes
    /// once the output it goes into is known. Without boundaries the output
    /// ends at the target size.
    fn decide_cut(&self, key: &[u8]) -> (bool, Option<cut_alignment::Crossing>) {
        self.decide_cut_at(key, self.output_load())
    }

    /// [`Self::decide_cut`] for an output weighing `load`, measured once for
    /// every share of the target the rule compares it with.
    fn decide_cut_at(
        &self,
        key: &[u8],
        load: Option<OutputLoad>,
    ) -> (bool, Option<cut_alignment::Crossing>) {
        let Some(alignment) = &self.alignment else {
            let full = load.is_some_and(|load| load.reaches(self.target_size));
            return (full && self.rotation_sheds(key, (0, 0)), None);
        };
        let comparator = self.comparator.as_ref();
        let mut crossing = alignment.crossing(key, comparator);
        // An output with no record yet is below half of any target.
        let Some(load) = load else {
            crossing.counted = 0;
            return (false, Some(crossing));
        };
        let full = |percent| load.reaches(cut_alignment::share_of(self.target_size, percent));
        let past_half = full(50);
        // A boundary passed below half the target is no candidate, so it does
        // not raise the share the output is cut at later.
        if !past_half {
            crossing.counted = 0;
        }
        let cut = past_half
            && (full(200)
                || if crossing.counted > 0 {
                    full(alignment.floor_percent(crossing))
                } else {
                    !alignment.ahead(crossing, comparator) && full(100)
                });
        (cut && self.rotation_sheds(key, (0, 0)), Some(crossing))
    }

    /// The end of the current output's domain when `key`, the next new key,
    /// lies at or past it: the output ends there.
    fn crossed_domain_end(&self, key: &[u8]) -> Option<UserKey> {
        self.domain
            .as_ref()
            .and_then(|domain| domain.crosses(key, self.comparator.as_ref()))
            .cloned()
    }

    /// Takes `key` into the current output's domain, a new one when `crossed`.
    fn enter_domain(&mut self, key: &[u8], crossed: bool) {
        if let Some(domain) = &mut self.domain {
            domain.enter(key, crossed);
        }
    }

    /// Takes the key `crossing` was decided at into the output: a new one when
    /// `cut`.
    fn commit_cut(&mut self, crossing: Option<cut_alignment::Crossing>, cut: bool) {
        if let (Some(alignment), Some(crossing)) = (&mut self.alignment, crossing) {
            alignment.commit(crossing, cut);
        }
    }

    /// Records the table file already open and every one a rotation creates
    /// in `outputs`.
    #[must_use]
    pub(crate) fn use_output_ledger(
        mut self,
        outputs: crate::compaction::output_ledger::OutputLedger,
    ) -> Self {
        outputs.record_table(
            self.current_writer_id,
            self.writer.path.clone(),
            self.fs.clone(),
        );
        self.outputs = Some(outputs);
        self
    }

    /// Enables parallel block compression for this run, shared by every
    /// rotated successor table. No-op when `parallel` is `None`.
    #[cfg(feature = "std")]
    #[must_use]
    pub fn use_parallel_compression(
        mut self,
        parallel: Option<crate::table::writer::ParallelCompression>,
    ) -> Self {
        if let Some(parallel) = parallel {
            self.writer = self.writer.use_parallel_compression(parallel.clone());
            self.parallel = Some(parallel);
        }
        self
    }

    /// Enables RT clipping: each tombstone is intersected with the output
    /// Sets the user comparator used for output ordering and RT clipping.
    #[must_use]
    pub fn set_comparator(mut self, comparator: crate::SharedComparator) -> Self {
        self.comparator = comparator;
        self
    }

    /// Enables RT clipping to the output table's responsibility range.
    ///
    /// Clipped RTs may extend beyond the table's own KV key range to cover the
    /// gap up to the next output table. Use this for compaction where input
    /// tables are consumed; do NOT use for flush where RTs must cover older
    /// SSTs.
    #[must_use]
    pub fn use_clip_range_tombstones(mut self) -> Self {
        self.clip_range_tombstones = true;
        self
    }

    /// Under clipping, keeps the pieces of `outside`'s tombstones that fall
    /// past the outputs' keys, within the keys this writer answers for, in
    /// outputs of tombstones alone written after the last one.
    pub(crate) fn keep_tombstones_outside(&mut self, mut outside: TombstonesOutside) {
        let comparator = self.comparator.as_ref();
        outside
            .tombstones
            .sort_by(|a, b| comparator.compare(&a.start, &b.start));
        self.outside = (!outside.tombstones.is_empty()).then_some(outside);
    }

    /// Sets range tombstones to be distributed across output tables.
    pub fn set_range_tombstones(&mut self, mut tombstones: Vec<RangeTombstone>) {
        let comparator = self.comparator.as_ref();
        tombstones.sort_by(|a, b| comparator.compare(&a.start, &b.start));
        self.tombstone_share = tombstone_share::TombstoneShare::new();
        self.range_tombstones = tombstones;
    }

    /// Writes range tombstones to the given writer, respecting the clip mode.
    /// `tombstones`, in order by start, are those that can overlap the table's
    /// zone (see [`tombstone_share::TombstoneShare::zone`]), so a run of many
    /// outputs visits each tombstone in the outputs it spans only.
    ///
    /// - **clip=true** (compaction): intersect each RT with the table's
    ///   "responsibility range".  For intermediate tables (rotation)
    ///   `clip_upper` is the first key of the *next* output table, so the
    ///   range extends past the table's last KV key and covers the gap.
    ///   For the final table `clip_upper` is `None` and we fall back to
    ///   `upper_bound_exclusive(last_key)` under byte order, to the lower
    ///   bound alone under any other comparator.
    /// - **clip=false** (flush): cut each RT to the table's zone, from `lower`
    ///   (`None` for the first output) to `clip_upper` (`None` for the last),
    ///   so the outputs together hold every RT once and still cover keys in
    ///   older SSTs outside this memtable's key range.
    fn write_rts_to_writer<'t>(
        tombstones: impl IntoIterator<Item = &'t RangeTombstone>,
        clip: bool,
        writer: &mut Writer,
        lower: Option<&UserKey>,
        clip_upper: Option<&UserKey>,
        comparator: &dyn crate::comparator::UserComparator,
    ) {
        if let (Some(first_key), Some(last_key)) =
            (writer.meta.first_key.clone(), writer.meta.last_key.clone())
        {
            if clip {
                // Compaction mode: clip RTs to this table's responsibility range.
                //
                // For intermediate tables (rotation) `clip_upper` is the first key
                // of the next output table — the range [first_key, next_key) covers
                // the gap between tables so RTs spanning it are preserved.
                //
                // For the final table `clip_upper` is None and we derive the
                // exclusive upper bound from the table's last KV key: its byte
                // successor, which follows it only in byte order. Under another
                // comparator it may sort before the last key, so the final
                // table clips on the lower bound alone, as below.
                let derived_upper;
                let max_exclusive: Option<&[u8]> = if let Some(upper) = clip_upper {
                    Some(upper.as_ref())
                } else if comparator.is_lexicographic() {
                    derived_upper =
                        crate::range_tombstone::upper_bound_exclusive(last_key.as_ref());
                    derived_upper.as_deref()
                } else {
                    None
                };

                if let Some(max_exclusive) = max_exclusive {
                    for rt in tombstones {
                        if let Some(clipped) =
                            rt.intersect_opt_with(first_key.as_ref(), max_exclusive, comparator)
                        {
                            // Widen last_key so point reads for keys in the
                            // gap will consult this table for RT suppression.
                            //
                            // Only widen during rotation (clip_upper is Some)
                            // where we know the exact boundary.  For the final
                            // table (clip_upper is None) widening with the
                            // derived exclusive bound could overlap a non-
                            // compacted adjacent table at the same level.
                            //
                            // Even during rotation, clipped.end must be
                            // strictly less than clip_upper (the next table's
                            // first key) — equality would make key_ranges
                            // overlap, breaking Run::get_for_key_cmp.
                            //
                            // Only last_key needs widening: intersect_opt
                            // already clamps clipped.start >= first_key.
                            if let Some(existing) = &mut writer.meta.last_key {
                                let safe = clip_upper.is_some_and(|upper| {
                                    comparator.compare(&clipped.end, upper.as_ref())
                                        == core::cmp::Ordering::Less
                                });
                                if safe
                                    && comparator.compare(&clipped.end, existing.as_ref())
                                        == core::cmp::Ordering::Greater
                                {
                                    *existing = clipped.end.clone();
                                }
                            }

                            writer.write_range_tombstone(clipped);
                        }
                    }
                } else {
                    // `last_key` has no successor to cut at: it is the
                    // lexicographically maximal encodable user key, or the
                    // comparator is not byte order. Clip only on the lower
                    // bound and keep the persisted key_range unchanged;
                    // widening it during compaction would break the
                    // disjoint-run invariant that point reads rely on.
                    for rt in tombstones {
                        let clipped_start = if comparator.compare(&rt.start, first_key.as_ref())
                            == core::cmp::Ordering::Greater
                        {
                            rt.start.as_ref()
                        } else {
                            first_key.as_ref()
                        };

                        if comparator.compare(clipped_start, &rt.end) == core::cmp::Ordering::Less {
                            writer.write_range_tombstone(RangeTombstone::new(
                                UserKey::from(clipped_start),
                                rt.end.clone(),
                                rt.seqno,
                            ));
                        }
                    }
                }
            } else {
                Self::write_zone_cut(tombstones, writer, lower, clip_upper, comparator);
            }
        } else {
            // An output of tombstones alone takes its key range from them, in
            // the comparator's order: a compaction with no KV items at all
            // writes them whole, a flush cuts them to the output's zone.
            let coverage = if clip {
                let mut coverage = None;
                for rt in tombstones {
                    Self::widen_coverage(&mut coverage, &rt.start, &rt.end, comparator);
                    writer.write_range_tombstone(rt.clone());
                }
                coverage
            } else {
                Self::write_zone_cut(tombstones, writer, lower, clip_upper, comparator)
            };
            if let Some((start, end)) = coverage {
                writer.cover_range_tombstones(start, end);
            }
        }
    }

    /// Widens `coverage` to hold `start..end` under `comparator`. The pieces
    /// arrive in the order of their starts, so the first start is the least.
    fn widen_coverage(
        coverage: &mut Option<(UserKey, UserKey)>,
        start: &UserKey,
        end: &UserKey,
        comparator: &dyn crate::comparator::UserComparator,
    ) {
        use core::cmp::Ordering;

        match coverage {
            None => *coverage = Some((start.clone(), end.clone())),
            Some((least, greatest)) => {
                debug_assert_ne!(comparator.compare(start, least), Ordering::Less);
                if comparator.compare(end, greatest) == Ordering::Greater {
                    *greatest = end.clone();
                }
            }
        }
    }

    /// Flush mode: the outputs split the whole key space, so each tombstone is
    /// written once, cut into the zones it spans, from `lower` (`None` for the
    /// first output) to `upper` (`None` for the last). The first output's zone
    /// opens below its first key and the last one's above its last key, so a
    /// tombstone reaching past this memtable's keys still covers them in older
    /// tables.
    ///
    /// The key range widens to each piece written, so a point read for a key
    /// under it, in older tables or in the gap before the next output, consults
    /// this table. Flush outputs are separate L0 runs, which may overlap, so the
    /// widening may reach the next output's first key. Using the exclusive end
    /// as an inclusive upper bound over-approximates but does not lose entries.
    ///
    /// Returns the coverage of the pieces written, under `comparator`.
    fn write_zone_cut<'t>(
        tombstones: impl IntoIterator<Item = &'t RangeTombstone>,
        writer: &mut Writer,
        lower: Option<&UserKey>,
        upper: Option<&UserKey>,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> Option<(UserKey, UserKey)> {
        use core::cmp::Ordering;

        let mut coverage = None;
        for rt in tombstones {
            let start = match lower {
                Some(lower) if comparator.compare(&rt.start, lower) == Ordering::Less => lower,
                _ => &rt.start,
            };
            let end = match upper {
                Some(upper) if comparator.compare(&rt.end, upper) == Ordering::Greater => upper,
                _ => &rt.end,
            };
            if comparator.compare(start, end) != Ordering::Less {
                continue;
            }
            if let Some(existing) = &mut writer.meta.first_key
                && comparator.compare(start, existing.as_ref()) == Ordering::Less
            {
                *existing = start.clone();
            }
            if let Some(existing) = &mut writer.meta.last_key
                && comparator.compare(end, existing.as_ref()) == Ordering::Greater
            {
                *existing = end.clone();
            }
            Self::widen_coverage(&mut coverage, start, end, comparator);
            writer.write_range_tombstone(RangeTombstone::new(start.clone(), end.clone(), rt.seqno));
        }
        coverage
    }

    /// Records that the entry just written points into `indirection`'s blob
    /// file. Called after [`Self::write`] of that entry: rotation happens
    /// before a write, so the entry's key is the current key and belongs to
    /// the current table, and keys arrive in order, so the first key seen for
    /// a blob file is its first and the latest its last.
    ///
    /// # Panics
    ///
    /// In debug builds, if no entry has been written yet.
    pub fn register_blob(&mut self, indirection: BlobIndirection) {
        debug_assert!(
            self.current_key.is_some(),
            "a blob is registered after the entry that points into it"
        );
        let Some(key) = self.current_key.as_ref() else {
            return;
        };
        self.linked_blobs.register(
            indirection.vhandle.blob_file_id,
            u64::from(indirection.size),
            u64::from(indirection.vhandle.on_disk_size),
            key,
            true,
        );
    }

    /// Records the blob references of `rows`, written into the current table
    /// not one by one but as a copied row group, each under its own row's key,
    /// as [`Self::register_blob`] and [`Self::register_cell_row`] record a
    /// written row's under the current key.
    ///
    /// # Errors
    ///
    /// Returns an error if a row tagged as an indirection or a cell row does
    /// not decode as one.
    #[cfg(feature = "columnar")]
    pub(crate) fn register_carried_rows(&mut self, rows: &[InternalValue]) -> crate::Result<()> {
        use crate::coding::Decode;

        for row in rows {
            let key = &row.key.user_key;
            if row.key.value_type.is_cell_row() {
                for (indirection, owned) in crate::blob_tree::field_row::row_refs(&row.value)? {
                    self.linked_blobs.register(
                        indirection.vhandle.blob_file_id,
                        u64::from(indirection.size),
                        u64::from(indirection.vhandle.on_disk_size),
                        key,
                        owned,
                    );
                    if owned {
                        self.owned_objects
                            .push((indirection.vhandle.blob_file_id, indirection.vhandle.offset));
                    }
                }
            } else if row.key.value_type.is_indirection() {
                let indirection = BlobIndirection::decode_from(&mut &row.value[..])?;
                self.linked_blobs.register(
                    indirection.vhandle.blob_file_id,
                    u64::from(indirection.size),
                    u64::from(indirection.vhandle.on_disk_size),
                    key,
                    true,
                );
            }
        }
        Ok(())
    }

    /// Records the blob files the cell row just written references, as
    /// [`Self::register_blob`] does for an indirection: each owned reference
    /// adds to its file's counts, each borrowed one only links the file.
    ///
    /// # Errors
    ///
    /// Returns an error if `row` is not a well-formed cell row.
    pub fn register_cell_row(&mut self, row: &[u8]) -> crate::Result<()> {
        debug_assert!(
            self.current_key.is_some(),
            "a cell row is registered after it is written"
        );
        let Some(key) = self.current_key.as_ref() else {
            return Ok(());
        };
        for (indirection, owned) in crate::blob_tree::field_row::row_refs(row)? {
            self.linked_blobs.register(
                indirection.vhandle.blob_file_id,
                u64::from(indirection.size),
                u64::from(indirection.vhandle.on_disk_size),
                key,
                owned,
            );
            if owned {
                self.owned_objects
                    .push((indirection.vhandle.blob_file_id, indirection.vhandle.offset));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn use_adaptive_index(mut self, spill_threshold: u64) -> Self {
        self.index_spill_threshold = Some(spill_threshold);
        self.writer = self.writer.use_adaptive_index(spill_threshold);
        self
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.use_partitioned_filter = true;
        self.writer = self.writer.use_partitioned_filter();
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self.writer = self.writer.use_data_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        self.index_block_restart_interval = interval;
        self.writer = self.writer.use_index_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self.writer = self.writer.use_data_block_hash_ratio(ratio);
        self
    }

    #[must_use]
    pub(crate) fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= crate::config::MAX_BLOCK_SIZE,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self.writer = self.writer.use_data_block_size(size);
        self
    }

    /// Sets the size a partitioned block index cuts its partitions at; see
    /// [`Writer::use_index_partition_size`].
    #[must_use]
    pub(crate) fn use_index_partition_size(mut self, size: u32) -> Self {
        self.index_partition_size = size;
        self.writer = self.writer.use_index_partition_size(size);
        self
    }

    /// Sets the size a partitioned filter cuts its partitions at; see
    /// [`Writer::use_filter_partition_size`].
    #[must_use]
    pub(crate) fn use_filter_partition_size(mut self, size: u32) -> Self {
        self.filter_partition_size = size;
        self.writer = self.writer.use_filter_partition_size(size);
        self
    }

    /// Sets the size a columnar table's row groups are cut at; see
    /// [`Writer::use_row_group_size`].
    #[must_use]
    pub(crate) fn use_row_group_size(mut self, size: u32) -> Self {
        self.row_group_size = size;
        self.writer = self.writer.use_row_group_size(size);
        self
    }

    /// Sets the size a columnar row group's rows are cut into row pages at;
    /// see [`Writer::use_columnar_page_size`].
    #[must_use]
    pub(crate) fn use_columnar_page_size(mut self, size: u32) -> Self {
        self.columnar_page_size = size;
        self.writer = self.writer.use_columnar_page_size(size);
        self
    }

    /// Sets how the tables' column pages store their values.
    #[must_use]
    pub(crate) fn use_column_encoding(mut self, encoding: crate::config::ColumnEncoding) -> Self {
        self.column_encoding = encoding;
        self.writer = self.writer.use_column_encoding(encoding);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self.writer = self.writer.use_data_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        self.index_block_compression = compression;
        self.writer = self.writer.use_index_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.writer = self.writer.use_bloom_policy(bloom_policy);
        self
    }

    /// Sizes every output's filters by probe load against the tree's filter
    /// budget; `None` builds them at the bloom policy.
    #[must_use]
    pub(crate) fn use_filter_sizing(
        mut self,
        sizing: Option<crate::filter_budget::FilterPlan>,
    ) -> Self {
        self.filter_sizing.clone_from(&sizing);
        self.writer = self.writer.use_filter_sizing(sizing);
        self
    }

    /// The filter plan this writer sizes its outputs by, for its operation to
    /// hold until the outputs are installed.
    pub(crate) fn filter_sizing(&self) -> Option<crate::filter_budget::FilterPlan> {
        self.filter_sizing.clone()
    }

    /// Ends an output before the first key of the next of `domains`, whatever
    /// its size (see [`crate::KeyDomains`]).
    #[must_use]
    pub(crate) fn use_key_domains(mut self, domains: Option<Arc<dyn crate::KeyDomains>>) -> Self {
        self.domain = domains.map(crate::key_domain::DomainCursor::new);
        self
    }

    #[must_use]
    pub fn use_prefix_extractor(mut self, extractor: Option<Arc<dyn PrefixExtractor>>) -> Self {
        self.prefix_extractor.clone_from(&extractor);
        self.writer = self.writer.use_prefix_extractor(extractor);
        self
    }

    #[must_use]
    pub fn use_encryption(mut self, encryption: Option<Arc<dyn EncryptionProvider>>) -> Self {
        self.encryption.clone_from(&encryption);
        self.writer = self.writer.use_encryption(encryption);
        self
    }

    /// Reads the time on the tree's `clock` in every table written (see
    /// [`Writer::use_clock`]).
    #[must_use]
    pub(crate) fn use_clock(mut self, clock: Option<Arc<dyn crate::Clock>>) -> Self {
        self.clock.clone_from(&clock);
        self.writer = self.writer.use_clock(clock);
        self
    }

    /// Wires the tree's `Config::page_ecc` flag through to the
    /// inner [`Writer`] and preserves it across rotations so every
    /// successor writer stamps the same setting on its blocks.
    #[must_use]
    pub fn use_ecc(mut self, ecc: Option<crate::table::block::EccParams>) -> Self {
        self.ecc = ecc;
        self.writer = self.writer.use_ecc(ecc);
        self
    }

    /// Convenience: resolves `(page_ecc, EccScheme)` into the per-block
    /// scheme and applies it via [`Self::use_ecc`]. Mirrors
    /// [`crate::table::writer::Writer::use_page_ecc`].
    #[must_use]
    pub fn use_page_ecc(self, page_ecc: bool, scheme: crate::runtime_config::EccScheme) -> Self {
        self.use_ecc(crate::table::writer::resolve_ecc(page_ecc, scheme))
    }

    /// Wires the tree's `Config::sync_mode` through to the inner [`Writer`]
    /// and preserves it across rotations so every successor SST is finished
    /// at the same durability level.
    #[must_use]
    pub fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self.writer = self.writer.use_sync_mode(sync_mode);
        self
    }

    /// Wires the tree's `Config::writeback_bytes` through to the inner
    /// [`Writer`] and every successor.
    #[must_use]
    pub fn use_writeback_bytes(mut self, bytes: u64) -> Self {
        self.writeback_bytes = bytes;
        self.writer = self.writer.use_writeback_bytes(bytes);
        self
    }

    /// Wires the runtime `kv_checksums` policy + algorithm through to the
    /// inner [`Writer`] and preserves it across rotations so every
    /// successor writer applies the same per-KV checksum setting. `Off`
    /// emits no per-KV footer and leaves the `KV_CHECKSUM_FOOTER` flag clear
    /// (the data-block payload encoding is unchanged; the V5 header/meta
    /// layout still differs from pre-V5 regardless).
    #[must_use]
    pub fn use_kv_checksums(
        mut self,
        policy: crate::runtime_config::KvChecksumPolicy,
        algo: crate::runtime_config::ChecksumAlgorithm,
    ) -> Self {
        self.kv_checksum = if matches!(policy, crate::runtime_config::KvChecksumPolicy::Off) {
            None
        } else {
            Some((policy, algo))
        };
        self.writer = self.writer.use_kv_checksums(policy, algo);
        self
    }

    /// Wires the `seqno_in_index` runtime config through to the inner
    /// [`Writer`] and preserves it across rotations so every successor
    /// writer emits a matching `seqno_bounds` section.
    #[must_use]
    pub fn use_seqno_in_index(mut self, seqno_in_index: bool) -> Self {
        self.use_seqno_in_index = seqno_in_index;
        self.writer = self.writer.use_seqno_in_index(seqno_in_index);
        self
    }

    /// Wires the `zstd_two_pass_seed` runtime config through to the inner
    /// [`Writer`] and preserves it across rotations, so every SST of one flush
    /// or compaction is written under the same strategy.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_two_pass_seed(mut self, enabled: bool) -> Self {
        self.use_zstd_two_pass_seed = enabled;
        self.writer = self.writer.use_zstd_two_pass_seed(enabled);
        self
    }

    /// Enables the `zone_map` section on this writer and every successor it
    /// rotates to, so all SSTs of one flush / compaction emit it.
    #[must_use]
    pub fn use_zone_map(mut self, zone_map: bool) -> Self {
        self.use_zone_map = zone_map;
        self.writer = self.writer.use_zone_map(zone_map);
        self
    }

    /// Whether the tables this writes keep a zone map, so a group copied into
    /// them needs its per-column statistics.
    #[cfg(feature = "columnar")]
    pub(crate) const fn keeps_zone_map(&self) -> bool {
        self.use_zone_map
    }

    /// The bytes of rows a columnar group written here is closed at.
    #[cfg(feature = "columnar")]
    pub(crate) const fn row_group_size(&self) -> u32 {
        self.row_group_size
    }

    #[must_use]
    pub fn use_columnar(mut self, columnar: bool) -> Self {
        self.use_columnar = columnar;
        self.writer = self.writer.use_columnar(columnar);
        self
    }

    /// Splits the cell rows of every table of this run into their fields
    /// (see [`Writer::use_cell_rows`]), re-applied to each rotated successor.
    #[must_use]
    pub(crate) fn use_cell_rows(mut self, cell_rows: bool) -> Self {
        self.cell_rows = cell_rows;
        self.writer = self.writer.use_cell_rows(cell_rows);
        self
    }

    /// Marks every table in this run as bulk-ingested (re-applied to each rotated
    /// successor), so manifest repair can recognize their manifest-only
    /// `global_seqno` dependence. See [`Writer::use_bulk_ingested`].
    #[must_use]
    pub(crate) fn use_bulk_ingested(mut self, bulk_ingested: bool) -> Self {
        self.bulk_ingested = bulk_ingested;
        // A multi-writer only produces flush / compaction / ingest output, whose
        // provenance is always KNOWN — never the unknown (`None`) case that only
        // salvage's mirror path yields.
        self.writer = self.writer.use_bulk_ingested(Some(bulk_ingested));
        self
    }

    /// The first output's id when its file was made as this writer started:
    /// taken from the tree's counter then, it stands for when the write
    /// began. `None` for a pooled file.
    pub(crate) fn first_made_now(&self) -> Option<TableId> {
        self.first_made_now
    }

    /// Stamps a FIXED L0 recency key on this and every rotated successor
    /// writer (see [`Writer::use_recency`]). `None` keeps the default own-id
    /// stamping (see the `recency` field): the current writer already carries
    /// its own id, and each successor stamps its own.
    #[must_use]
    pub(crate) fn use_recency(mut self, recency: Option<TableId>) -> Self {
        self.recency = recency;
        if let Some(recency) = recency {
            self.writer = self.writer.use_recency(recency);
        }
        self
    }

    /// Stamps the compaction lineage on this and every rotated successor
    /// writer (see [`Writer::use_lineage`]).
    #[must_use]
    pub(crate) fn use_lineage(mut self, lineage: Option<Vec<TableId>>) -> Self {
        self.lineage.clone_from(&lineage);
        self.writer = self.writer.use_lineage(lineage);
        self
    }

    /// Lets this and every rotated successor writer take its age from the
    /// compaction `inputs` its key range meets (see
    /// [`Writer::finish_deferring_dir_sync`]), swept as this chain's outputs
    /// advance.
    #[must_use]
    pub(crate) fn use_inherited_age(mut self, inputs: super::writer::AgeSweep) -> Self {
        self.inherited_age = Some(inputs);
        self
    }

    /// Declares that this writer receives the run's ENTIRE merged stream
    /// (see the `owns_whole_run` field), allowing its final output to carry
    /// the `lineage_last` marker. Never set on a parallel sub-compaction or
    /// tight-space slice.
    #[must_use]
    pub(crate) fn use_lineage_whole_run(mut self, whole_run: bool) -> Self {
        self.owns_whole_run = whole_run;
        self
    }

    /// Wires the compaction-filter transform counter (see the
    /// `transform_marker` field). Call before writing starts.
    #[must_use]
    pub(crate) fn use_transform_marker(
        mut self,
        marker: alloc::sync::Arc<portable_atomic::AtomicU64>,
    ) -> Self {
        let now = marker.load(core::sync::atomic::Ordering::Relaxed);
        self.transforms_at_output_start = now;
        self.transforms_after_last_write = now;
        self.transform_marker = Some(marker);
        self
    }

    /// The transform counter's value now; without a counter, the milestone
    /// as it stands, so settling it changes nothing.
    #[cfg(feature = "columnar")]
    pub(crate) fn transforms_seen(&self) -> u64 {
        self.transform_marker
            .as_ref()
            .map_or(self.transforms_after_last_write, |marker| {
                marker.load(core::sync::atomic::Ordering::Relaxed)
            })
    }

    /// Takes `seen`, the counter's value when the record just put in the
    /// current output was emitted, as the after-last-write milestone, in
    /// place of the value read as it was written: a record written after the
    /// merge held it would otherwise take the verdicts ticked meanwhile,
    /// which belong to the next output (see `transforms_after_last_write`).
    #[cfg(feature = "columnar")]
    pub(crate) fn settle_transforms(&mut self, seen: u64) {
        self.transforms_after_last_write = seen;
    }

    /// Sets the delete strategy for this and every rotated successor writer.
    #[must_use]
    pub fn delete_strategy(mut self, strategy: crate::config::DeleteStrategy) -> Self {
        self.delete_strategy = strategy;
        self.writer = self.writer.delete_strategy(strategy);
        self
    }

    /// Wires the resolved retrieval-ribbon locator policy entry through to the
    /// inner [`Writer`] and preserves it across rotations, so every successor
    /// table in the run emits its own per-SST locator section (or none, when
    /// disabled). Block ordinals reset per table.
    #[must_use]
    pub fn use_locator(mut self, entry: crate::config::LocatorPolicyEntry) -> Self {
        self.locator_entry = entry;
        self.writer = self.writer.use_locator(entry);
        self
    }

    /// Wires the `disable_cow_on_sst_files` runtime config through to the inner
    /// [`Writer`] (clearing per-file copy-on-write on the current output file) and
    /// preserves it across rotations so every successor SST is flagged the same
    /// way. A no-op on non-CoW filesystems.
    #[must_use]
    pub fn use_disable_cow_on_sst(mut self, disable_cow: bool) -> Self {
        self.disable_cow_on_sst = disable_cow;
        self.writer = self.writer.use_disable_cow(disable_cow);
        self
    }

    #[cfg(zstd_any)]
    #[must_use]
    pub fn use_zstd_dictionary(
        mut self,
        dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary.clone_from(&dictionary);
        self.writer = self.writer.use_zstd_dictionary(dictionary);
        self
    }

    /// Flushes the current writer, stores its metadata, and sets up a new
    /// writer for the next table.
    ///
    /// `domain_end`, when given, is the end of the closing output's key
    /// domain, which the next key lies past: its share of the range tombstones
    /// stops there, and the pieces between that end and the next output's
    /// first key go to an output of tombstones alone, so no output holding
    /// keys reaches into another domain.
    fn rotate_at(&mut self, domain_end: Option<&UserKey>) -> crate::Result<()> {
        log::debug!("Rotating table writer");
        self.output_base = None;
        self.value_layout = None;

        if self.first_output_key.is_none() {
            self.write_pieces_below_the_first_output()?;
        }
        let gap = domain_end
            .map(|end| self.tombstones_up_to_the_next_output(end))
            .unwrap_or_default();
        let gap_writer = if gap.is_empty() {
            None
        } else {
            Some(self.successor()?)
        };
        let new_writer = self.successor()?;
        let old_writer = core::mem::replace(&mut self.writer, new_writer);
        if self.first_output_key.is_none() {
            self.first_output_key.clone_from(&old_writer.meta.first_key);
        }
        self.close_output(old_writer, domain_end)?;
        if let Some(writer) = gap_writer {
            self.write_tombstones_alone(writer, gap, false)?;
        }
        Ok(())
    }

    /// Writes the pieces of the range tombstones below the first output, the
    /// current one, to outputs of tombstones alone placed ahead of it in the
    /// run's lineage chain: manifest repair reads an unbroken chain's ranges
    /// as following key order, from its head's least key to its tail's
    /// greatest, so a table below the first output chained after it would
    /// let a chain missing the first output claim its keys.
    fn write_pieces_below_the_first_output(&mut self) -> crate::Result<()> {
        let (below, _) = self.flush_pieces_outside_the_domain(false);
        let flush_below = !below.is_empty();
        let mut groups: Vec<Vec<RangeTombstone>> = Vec::new();
        if flush_below {
            groups.push(below);
        }
        groups.extend(self.tombstones_beside_the_outputs(Beside::Below));
        if groups.is_empty() {
            return Ok(());
        }
        let first = self.current_writer_id;
        let mut prev = self.writer.relink_lineage_prev(None);
        for pieces in groups {
            let mut writer = self.successor()?;
            writer.relink_lineage_prev(prev);
            prev = Some(writer.table_id);
            self.write_tombstones_alone(writer, pieces, false)?;
        }
        self.writer.relink_lineage_prev(prev);
        // The next output follows the first one, not the pieces below it.
        self.current_writer_id = first;
        // The first output's share starts at its first key when the pieces
        // below it went to outputs of their own.
        if flush_below {
            self.output_lower.clone_from(&self.writer.meta.first_key);
        }
        Ok(())
    }

    /// The pieces of the range tombstones from `start` to the next output's
    /// first key, the key last written, in order by start: under clipping only
    /// those a table outside the compaction needs, since the outputs hold no
    /// key there. The gaps come in key order, so only the tombstones a gap
    /// meets are visited.
    fn tombstones_up_to_the_next_output(&mut self, start: &UserKey) -> Vec<RangeTombstone> {
        let Some(next) = self.current_key.as_ref() else {
            return Vec::new();
        };
        let tombstones = if self.clip_range_tombstones {
            self.outside
                .as_ref()
                .map_or(&[][..], |outside| outside.tombstones.as_slice())
        } else {
            self.range_tombstones.as_slice()
        };
        let cmp = self.comparator.as_ref();
        Self::pieces_within(
            self.gap_cursor.overlapping(tombstones, (start, next), cmp),
            Some(start),
            Some(next),
            cmp,
        )
    }

    /// Under a key-domain policy, the pieces of a flush's range tombstones
    /// the current output would take outside its domain: below its first key
    /// when it is the run's first output, and past its domain's end when it
    /// is the `last`. A flush widens an output's key range to the pieces it
    /// holds, so they go to outputs of tombstones alone instead. An output
    /// with no key holds tombstones alone and takes them all.
    fn flush_pieces_outside_the_domain(
        &self,
        last: bool,
    ) -> (Vec<RangeTombstone>, Vec<RangeTombstone>) {
        let Some(domain) = self.domain.as_ref().filter(|_| !self.clip_range_tombstones) else {
            return (Vec::new(), Vec::new());
        };
        let Some(first) = self.writer.meta.first_key.as_ref() else {
            return (Vec::new(), Vec::new());
        };
        let cmp = self.comparator.as_ref();
        let below = if self.output_lower.is_none() {
            Self::pieces_within(&self.range_tombstones, None, Some(first), cmp)
        } else {
            Vec::new()
        };
        let above = match domain.end() {
            Some(end) if last => Self::pieces_within(&self.range_tombstones, Some(end), None, cmp),
            _ => Vec::new(),
        };
        (below, above)
    }

    /// The pieces of `tombstones`, in order by start, within `[lower, upper)`,
    /// `None` for an end of the key space.
    fn pieces_within<'t>(
        tombstones: impl IntoIterator<Item = &'t RangeTombstone>,
        lower: Option<&UserKey>,
        upper: Option<&UserKey>,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Vec<RangeTombstone> {
        use core::cmp::Ordering::Less;

        tombstones
            .into_iter()
            .filter_map(|rt| {
                let start = match lower {
                    Some(lower) if cmp.compare(&rt.start, lower) == Less => lower,
                    _ => &rt.start,
                };
                let end = match upper {
                    Some(upper) if cmp.compare(upper, &rt.end) == Less => upper,
                    _ => &rt.end,
                };
                (cmp.compare(start, end) == Less)
                    .then(|| RangeTombstone::new(start.clone(), end.clone(), rt.seqno))
            })
            .collect()
    }

    /// Writes `pieces`, in order by start, as the only content of `writer`'s
    /// output, its key range their span; `closes_run` when it is the run's
    /// last output.
    fn write_tombstones_alone(
        &mut self,
        mut writer: Writer,
        pieces: Vec<RangeTombstone>,
        closes_run: bool,
    ) -> crate::Result<()> {
        let cmp = self.comparator.as_ref();
        let mut span: Option<(UserKey, UserKey)> = None;
        for piece in pieces {
            Self::widen_coverage(&mut span, &piece.start, &piece.end, cmp);
            writer.write_range_tombstone(piece);
        }
        if let Some((start, end)) = span {
            writer.cover_range_tombstones(start, end);
        }
        if closes_run && self.owns_whole_run {
            writer.mark_lineage_last();
        }
        let path = writer.path.clone();
        // Its age is its own, not the inputs': these outputs fall outside the
        // key order the age sweep takes the others in, and a table of
        // tombstones aged as of now is dropped by age no sooner than the rows
        // it deletes.
        if let Some((table_id, checksum)) = writer.finish_deferring_dir_sync(None)? {
            self.results.push((table_id, checksum));
            if let Some(outputs) = &self.outputs {
                outputs.landed(&*self.fs, &path)?;
            }
        }
        Ok(())
    }

    /// A writer for the output after the current one, configured as this
    /// run's outputs are.
    fn successor(&mut self) -> crate::Result<Writer> {
        let (new_writer, durable) =
            self.files
                .open(&self.base_path, self.initial_level, &self.fs)?;
        self.entries_durable &= durable;
        let new_table_id = new_writer.table_id;
        // Recorded once it exists: a failed constructor created nothing ours.
        if let Some(outputs) = &self.outputs {
            outputs.record_table(new_table_id, new_writer.path.clone(), self.fs.clone());
        }
        let mut new_writer = new_writer
            .use_data_block_compression(self.data_block_compression)
            .use_index_block_compression(self.index_block_compression)
            .use_data_block_size(self.data_block_size)
            .use_index_partition_size(self.index_partition_size)
            .use_filter_partition_size(self.filter_partition_size)
            .use_row_group_size(self.row_group_size)
            .use_columnar_page_size(self.columnar_page_size)
            .use_column_encoding(self.column_encoding)
            .use_data_block_restart_interval(self.data_block_restart_interval)
            .use_index_block_restart_interval(self.index_block_restart_interval)
            .use_bloom_policy(self.bloom_policy)
            .use_filter_sizing(self.filter_sizing.clone())
            .use_data_block_hash_ratio(self.data_block_hash_ratio);

        if let Some(threshold) = self.index_spill_threshold {
            new_writer = new_writer.use_adaptive_index(threshold);
        }
        if self.use_partitioned_filter {
            new_writer = new_writer.use_partitioned_filter();
        }

        new_writer = new_writer.use_prefix_extractor(self.prefix_extractor.clone());
        new_writer = new_writer.use_encryption(self.encryption.clone());
        new_writer = new_writer.use_clock(self.clock.clone());
        new_writer = new_writer.use_ecc(self.ecc);
        new_writer = new_writer
            .use_sync_mode(self.sync_mode)
            .use_writeback_bytes(self.writeback_bytes);
        if let Some((policy, algo)) = self.kv_checksum {
            new_writer = new_writer.use_kv_checksums(policy, algo);
        }
        new_writer = new_writer.use_seqno_in_index(self.use_seqno_in_index);
        #[cfg(zstd_any)]
        {
            new_writer = new_writer.use_zstd_two_pass_seed(self.use_zstd_two_pass_seed);
        }
        new_writer = new_writer.use_zone_map(self.use_zone_map);
        new_writer = new_writer.use_columnar(self.use_columnar);
        new_writer = new_writer.use_cell_rows(self.cell_rows);
        new_writer = new_writer.use_bulk_ingested(Some(self.bulk_ingested));
        if let Some(recency) = self.recency {
            new_writer = new_writer.use_recency(recency);
        }
        new_writer = new_writer.use_lineage(self.lineage.clone());
        // The adjacency link: this successor follows the writer being closed,
        // which is what lets manifest repair union UNBROKEN sibling chains.
        new_writer = new_writer.use_lineage_prev(Some(self.current_writer_id));
        self.current_writer_id = new_table_id;
        new_writer = new_writer.delete_strategy(self.delete_strategy);
        new_writer = new_writer.use_disable_cow(self.disable_cow_on_sst);
        new_writer = new_writer.use_locator(self.locator_entry);

        #[cfg(zstd_any)]
        {
            new_writer = new_writer.use_zstd_dictionary(self.zstd_dictionary.clone());
        }

        #[cfg(feature = "std")]
        if let Some(parallel) = self.parallel.clone() {
            new_writer = new_writer.use_parallel_compression(parallel);
        }
        Ok(new_writer)
    }

    /// Writes out `old_writer`, the output a rotation just closed, its share
    /// of the range tombstones ending at `upper` when given, else at the next
    /// output's first key.
    fn close_output(
        &mut self,
        mut old_writer: Writer,
        upper: Option<&UserKey>,
    ) -> crate::Result<()> {
        // The finishing output's meta is about to be written: if a compaction
        // filter transformed a record within ITS window, the output is not
        // derivable from its inputs — mark the lineage transformed before it
        // lands, so manifest repair supersedes the inputs it covers instead
        // of trading it back for them. Judged by the AFTER-LAST-WRITE
        // milestone, not the live counter: rotation runs before the
        // triggering record is inserted, so verdicts ticked since the old
        // output's last write (the trigger's own, and removals in between)
        // belong to the NEW output.
        if self.transforms_after_last_write > self.transforms_at_output_start {
            old_writer.mark_lineage_transformed();
        }
        self.transforms_at_output_start = self.transforms_after_last_write;
        old_writer.spill_block()?;

        // Write range tombstones to the finishing writer, cut to its zone:
        // up to current_key, the first key of the NEW table, so the gap
        // between tables stays covered. The share has advanced to that key.
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                self.tombstone_share.zone(&self.range_tombstones, false),
                self.clip_range_tombstones,
                &mut old_writer,
                self.output_lower.as_ref(),
                upper.or(self.current_key.as_ref()),
                self.comparator.as_ref(),
            );
        }
        self.output_lower.clone_from(&self.current_key);

        for linked in self.linked_blobs.take() {
            old_writer.link_blob_file(linked);
        }
        old_writer.own_blob_objects(core::mem::take(&mut self.owned_objects));

        // The install that names the tables syncs their folder once.
        let path = old_writer.path.clone();
        if let Some((table_id, checksum)) =
            old_writer.finish_deferring_dir_sync(self.inherited_age.as_mut())?
        {
            self.results.push((table_id, checksum));
            // Before the next output takes more room.
            if let Some(outputs) = &self.outputs {
                outputs.landed(&*self.fs, &path)?;
            }
        }

        Ok(())
    }

    /// The current table reached its target: by its bytes, counting what
    /// `finish` will append, or by the heap its per-key state holds for
    /// `finish`. Rows that compress well reach the second first: their data
    /// stays small while the filter, index and locator state grow per key.
    #[cfg(any(test, feature = "columnar"))]
    fn table_full(&self) -> bool {
        self.output_load()
            .is_some_and(|load| load.reaches(self.target_size))
    }

    /// What the current table weighs at the current key, measured once so it
    /// can be judged against several size targets; `None` before its first
    /// record, which no target counts as full: closing an empty table would
    /// write its tombstones alone, unclipped, over its successors' keys.
    fn output_load(&self) -> Option<OutputLoad> {
        if self.writer.meta.key_count == 0 {
            return None;
        }
        // The blob files this table links, and its share of the range
        // tombstones, are handed to its writer only when it rotates, and it
        // writes them at `finish`. The tombstone block is encoded into a buffer
        // it holds while writing. The tombstones themselves are the caller's
        // input and the share tracks only those open at the current key:
        // neither grows with the table, and rotating frees neither. The
        // tombstones starting at the key go where the key goes, and the last
        // key has no successor whose check would count them, so they count here.
        let (tombstones, pieces, longest) = self.tombstones_at_key();
        Some(self.load_with_tombstones(tombstones, pieces, longest, (0, 0)))
    }

    /// The current output's share of the range tombstones at the current key:
    /// its encoded bytes, its entries and its longest bound, counting the
    /// tombstones starting at the key, which go where the key goes.
    fn tombstones_at_key(&self) -> (u64, u64, u64) {
        match &self.current_key {
            Some(key) if !self.range_tombstones.is_empty() => {
                let group = self.tombstone_share.group(
                    &self.range_tombstones,
                    key,
                    self.comparator.as_ref(),
                );
                (
                    self.tombstone_share.bytes(key) + group.bytes,
                    self.tombstone_share.pieces() + group.entries,
                    self.tombstone_share.longest_bound(key).max(group.longest),
                )
            }
            _ => (0, self.tombstone_share.pieces(), 0),
        }
    }

    /// What the current table weighs if it closes holding `pieces`
    /// range-tombstone entries of `tombstones` encoded bytes, with bounds of
    /// up to `longest` bytes. `alone` is what a table of tombstones alone
    /// writes and holds for its synthetic entry, which its writer does not
    /// count, and zero for a table with records. The writer holds each entry
    /// until `finish`, which encodes them into a block buffer and frames that
    /// when the block is transformed. The table's key range widens to the
    /// entries' bounds.
    fn load_with_tombstones(
        &self,
        tombstones: u64,
        pieces: u64,
        longest: u64,
        alone: (u64, u64),
    ) -> OutputLoad {
        use crate::table::block::{BlockType, framed_len_bound};

        let linked = self.linked_blobs.section_len();
        let owned = if self.owned_objects.is_empty() {
            0
        } else {
            framed_len_bound(
                crate::table::writer::owned_blob_objects_len(self.owned_objects.len()),
                BlockType::OwnedBlobObjects,
                CompressionType::None,
                self.encryption.as_deref(),
                self.ecc,
            )
        };
        let (tombstone_block, tombstones_held) = if tombstones == 0 {
            (0, 0)
        } else {
            let (range_out, range_held) = self.writer.widened_range_growth(longest);
            (
                framed_len_bound(
                    tombstones,
                    BlockType::RangeTombstone,
                    CompressionType::None,
                    self.encryption.as_deref(),
                    self.ecc,
                ) + range_out,
                self.tombstones_held(tombstones, pieces) + range_held,
            )
        };
        // Each linked blob file is an entry in the map here and, at rotation,
        // a copy in the writer's list.
        let linked_held = self.linked_blobs.len() as u64
            * (core::mem::size_of::<(BlobFileId, LinkedFile)>()
                + 1
                + core::mem::size_of::<LinkedFile>()) as u64;
        // The owned objects' list moves to the writer at rotation, which sorts
        // it in place.
        let owned_held =
            self.owned_objects.capacity() as u64 * core::mem::size_of::<(BlobFileId, u64)>() as u64;
        let (alone_written, alone_held) = alone;
        let size_hint = self.writer.output_size_hint();
        let writer_held = self.writer.held_state_bytes();
        let size = size_hint + alone_written + linked + owned + tombstone_block;
        let held = writer_held + alone_held + tombstones_held + linked_held + owned_held;
        // Closing a table sheds none of its metadata or of what it held at its
        // first record, the tombstones carried into it included, which the
        // next table carries alike: a target below that counts only once the
        // table holds more. A table with records closes by size once it has
        // written data or its tombstones grew by a block, and by state once it
        // holds a block's worth past its first record; one of tombstones
        // alone, once they take a block past its synthetic table. Otherwise
        // every key, or every tombstone, would get a table of its own, none of
        // them smaller.
        let block = self.writer.block_len();
        if alone_written > 0 {
            return OutputLoad {
                size,
                held,
                size_floor: Some(alone_written + block),
                held_target: self.target_size.max(writer_held + alone_held + block),
            };
        }
        let (base_held, base_tombstones) = self.output_base.unwrap_or((0, 0));
        let holds_content = size_hint > self.writer.finish_metadata_bytes()
            || tombstones >= base_tombstones + block;
        let held_target = if self.output_base.is_some() {
            self.target_size.max(base_held + block)
        } else {
            self.target_size
        };
        OutputLoad {
            size,
            held,
            size_floor: holds_content.then_some(0),
            held_target,
        }
    }

    /// Closing the current table at `key` sheds what it holds: the next one
    /// starts with a piece of every tombstone open at `key`. When those alone
    /// fill half the target, closing would carry them from output to output
    /// without shedding any, so the table grows past its target instead; a
    /// set of tombstones overlapping one another cannot be split below them.
    /// What is carried is judged as fullness is: by the bytes it encodes to
    /// and by the entries it holds in memory, against the part of the target
    /// left past what the next output carries anyway, `next_alone` for one of
    /// tombstones alone (see [`Self::load_with_tombstones`]).
    fn rotation_sheds(&self, key: &[u8], next_alone: (u64, u64)) -> bool {
        let entries = self.tombstone_share.open_count();
        if entries == 0 {
            return true;
        }
        let carry = self.tombstone_share.carry(key);
        let held = self.tombstones_held(carry, entries);
        let block = self.writer.block_len();
        let budget = |fixed: u64| {
            if fixed == 0 {
                self.target_size
            } else {
                self.target_size.max(fixed + block) - fixed
            }
        };
        carry < budget(next_alone.0) / 2 && held < budget(next_alone.1) / 2
    }

    /// Heap an output holding `pieces` tombstone entries of `tombstones`
    /// encoded bytes takes for them until `finish`: the entries and the
    /// bounds they own, which the encoded bytes bound from above, the block
    /// buffer they are encoded into, and its frame when the block is
    /// transformed.
    fn tombstones_held(&self, tombstones: u64, pieces: u64) -> u64 {
        use crate::table::block::{BlockType, transform_scratch_bound};

        pieces * core::mem::size_of::<RangeTombstone>() as u64
            + 2 * tombstones
            + transform_scratch_bound(
                tombstones,
                BlockType::RangeTombstone,
                CompressionType::None,
                self.encryption.as_deref(),
                self.ecc,
            )
    }

    /// Writes an item
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        // A new user key is a change of byte IDENTITY, never of byte order:
        // the stream arrives in comparator order, and a comparator whose order
        // is not the byte order (reversed, numeric) makes `<` false for keys
        // that are new, which silently disabled rotation. Same-key versions
        // stay in one output because identity does not change between them.
        let is_next_key = !self
            .current_key
            .as_ref()
            .is_some_and(|c| crate::comparator::same_user_key(c, &item.key.user_key));

        if is_next_key {
            let first_key = self.current_key.is_none();
            self.current_key = Some(item.key.user_key.clone());

            if !self.range_tombstones.is_empty() {
                self.tombstone_share.advance(
                    &self.range_tombstones,
                    &item.key.user_key,
                    self.comparator.as_ref(),
                );
                // Clipping bounds the first output's zone by its first key too.
                if first_key && self.clip_range_tombstones {
                    self.tombstone_share.open_output(&item.key.user_key);
                }
            }

            // A table records one value layout, so a row after an ingested
            // batch starts the next table.
            let layout_changes = self.value_layout == Some(crate::table::meta::ValueLayout::Split);
            let (cut, crossing) = self.decide_cut(&item.key.user_key);
            let crossed = self.crossed_domain_end(&item.key.user_key);
            let rotates = layout_changes || cut || crossed.is_some();
            if rotates {
                self.rotate_at(crossed.as_ref())?;
                self.tombstone_share.open_output(&item.key.user_key);
            }
            self.commit_cut(crossing, rotates);
            self.enter_domain(&item.key.user_key, crossed.is_some());
        }

        self.writer.write(item)?;
        if self.use_columnar {
            self.value_layout = Some(if self.cell_rows {
                crate::table::meta::ValueLayout::Cells
            } else {
                crate::table::meta::ValueLayout::Whole
            });
        }
        self.note_output_base();

        // The transform-attribution milestone: verdicts ticked up to a
        // record that actually LANDED belong to the output holding it (see
        // the `transforms_after_last_write` field).
        if let Some(marker) = &self.transform_marker {
            self.transforms_after_last_write = marker.load(core::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    /// Writes `raw`, a row group of another table copied whole, in place of its
    /// `rows`, which the compaction would otherwise write one by one; `group`
    /// is its index entry there, `layout` how its values are stored and
    /// `columns` its zone-map statistics. The group's first key is taken as
    /// [`Self::write`] takes a new key, rotating first when the table is full.
    ///
    /// Returns `false` when the group cannot be copied here, having written
    /// none of it: one that does not start a new key, or one the current
    /// table refuses (see [`Writer::append_carried_row_group`]). The caller
    /// then writes `rows` through [`Self::write`].
    ///
    /// # Errors
    ///
    /// Any error of the rotation or of the copy.
    #[cfg(feature = "columnar")]
    pub(crate) fn carry_row_group(
        &mut self,
        (raw, source): (&[u8], super::writer::VerbatimSource),
        group: super::index_block::RowGroupRef,
        (compression, layout): (CompressionType, crate::table::meta::ValueLayout),
        rows: &[InternalValue],
        columns: Option<Vec<crate::table::zone_map::ColumnStats>>,
    ) -> crate::Result<bool> {
        self.carry_into(
            (compression, layout),
            (rows, raw.len() as u64),
            |writer, comparator| {
                writer.append_carried_row_group(
                    (raw, source),
                    group,
                    layout,
                    rows,
                    columns,
                    comparator,
                )
            },
        )
    }

    /// Writes `rows` as one group that copies the pages `carry` names from
    /// another table's group, encoding the rest (see
    /// [`Writer::append_partly_carried_row_group`]); `compression` is the
    /// source's data codec. The group's first key is taken as [`Self::write`]
    /// takes a new key.
    ///
    /// Returns `false`, having written none of it, when the group cannot be
    /// written so; the caller then writes `rows` through [`Self::write`].
    ///
    /// # Errors
    ///
    /// Any error of the rotation or of the write.
    #[cfg(feature = "columnar")]
    pub(crate) fn carry_row_pages(
        &mut self,
        rows: &[InternalValue],
        carry: &super::writer::PageCarry<'_>,
        compression: CompressionType,
    ) -> crate::Result<bool> {
        self.carry_into(
            (compression, crate::table::meta::ValueLayout::Whole),
            (rows, partly_carried_bound(carry.raw.len(), rows)),
            |writer, comparator| writer.append_partly_carried_row_group(rows, carry, comparator),
        )
    }

    /// Whether a group about to be copied whole, `bytes` on disk and ending
    /// at `last`, is cheaper written key by key so the output can end on the
    /// boundary between its keys: the current output, not cut before it, is
    /// already past the share of the target it would be cut at there. Copied,
    /// the output straddles the boundary and the next merge reads and writes
    /// again every table past it that the group reaches, several where runs
    /// overlap there; written key by key, the group's bytes are
    /// encoded again now. The cheaper of the two is taken.
    #[cfg(feature = "columnar")]
    fn splits_cheaper_than_straddling(
        &self,
        load: Option<OutputLoad>,
        crossing: Option<cut_alignment::Crossing>,
        last: &InternalValue,
        bytes: u64,
    ) -> bool {
        let (Some(alignment), Some(crossing), Some(load)) = (&self.alignment, crossing, load)
        else {
            return false;
        };
        let Some(boundary) =
            alignment.inside(crossing, &last.key.user_key, self.comparator.as_ref())
        else {
            return false;
        };
        // One boundary past those `crossing` counts: the one inside. Counts
        // are bounded by the boundaries, one per table of a level.
        let inside = cut_alignment::Crossing {
            passed: crossing.passed + 1,
            counted: crossing.counted + 1,
        };
        bytes < boundary.reached_bytes(&last.key.user_key, self.comparator.as_ref())
            && load.reaches(cut_alignment::share_of(
                self.target_size,
                alignment.floor_percent(inside),
            ))
    }

    /// Whether a group about to be copied whole, `bytes` on disk and ending at
    /// `last`, is written key by key instead, given whether the output
    /// `rotates` before it: when the copy would take an aligned output past
    /// twice the target, where written rows would have ended it, or when
    /// splitting it costs less than straddling a boundary inside it.
    #[cfg(feature = "columnar")]
    fn writes_group_by_rows(
        &self,
        load: Option<OutputLoad>,
        rotates: bool,
        crossing: Option<cut_alignment::Crossing>,
        last: &InternalValue,
        bytes: u64,
    ) -> bool {
        if self.alignment.is_some() {
            let ceiling = cut_alignment::share_of(self.target_size, 200);
            let past_ceiling = if rotates {
                bytes > ceiling
            } else {
                // A group past the ceiling on its own takes any output with a
                // record past it; a smaller one, an output past what it leaves.
                load.is_some_and(|load| bytes > ceiling || load.reaches(ceiling - bytes))
            };
            if past_ceiling {
                return true;
            }
        }
        !rotates && self.splits_cheaper_than_straddling(load, crossing, last, bytes)
    }

    /// Takes `rows`' first key as [`Self::write`] takes a new key and has
    /// `write` put the group in the current table, when this run writes the
    /// copied pages' transform; `false`, having written nothing, otherwise.
    #[cfg(feature = "columnar")]
    fn carry_into(
        &mut self,
        (compression, layout): (CompressionType, crate::table::meta::ValueLayout),
        (rows, bytes): (&[InternalValue], u64),
        write: impl FnOnce(&mut Writer, &crate::comparator::SharedComparator) -> crate::Result<bool>,
    ) -> crate::Result<bool> {
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Ok(false);
        };
        // The copied pages keep the transform they were written under: this
        // run's must be the same, so a read of the copy decodes them with the
        // codec and dictionary its table names, and an encrypted or parity
        // run gets blocks bound to its own identity and scheme.
        if self.encryption.is_some()
            || self.ecc.is_some()
            || self.data_block_compression != compression
        {
            return Ok(false);
        }
        // A key's versions stay in one output, and a group that continues the
        // key written last would split them across the copy's boundary.
        if self
            .current_key
            .as_ref()
            .is_some_and(|c| crate::comparator::same_user_key(c, &first.key.user_key))
        {
            return Ok(false);
        }
        let previous_key = self.current_key.replace(first.key.user_key.clone());
        if !self.range_tombstones.is_empty() {
            self.tombstone_share.advance(
                &self.range_tombstones,
                &first.key.user_key,
                self.comparator.as_ref(),
            );
            // A tombstone starting among the group's keys could take the table
            // past its target between two of them, where rows written one by
            // one would close it: those rows are written so. Advancing to the
            // first key again for them changes nothing.
            if self.tombstone_share.starts_within(
                &self.range_tombstones,
                (&first.key.user_key, &last.key.user_key),
                self.comparator.as_ref(),
            ) {
                self.current_key = previous_key;
                return Ok(false);
            }
            if previous_key.is_none() && self.clip_range_tombstones {
                self.tombstone_share.open_output(&first.key.user_key);
            }
        }
        // A group running into the next key domain is written row by row,
        // which ends the output where the domain does.
        if self.domain.as_ref().is_some_and(|domain| {
            !domain.holds(
                &first.key.user_key,
                &last.key.user_key,
                self.comparator.as_ref(),
            )
        }) {
            self.current_key = previous_key;
            return Ok(false);
        }
        // A table records one value layout: a group stored the other way
        // starts the next table, a run of groups stored one way shares it.
        let layout_changes = self.value_layout.is_some_and(|current| current != layout);
        // The output is weighed once for the cut and for the copy's checks.
        let load = self.output_load();
        let (cut, crossing) = self.decide_cut_at(&first.key.user_key, load);
        let crossed = self.crossed_domain_end(&first.key.user_key);
        let cut = cut || crossed.is_some();
        if self.writes_group_by_rows(load, layout_changes || cut, crossing, last, bytes) {
            self.current_key = previous_key;
            return Ok(false);
        }
        if layout_changes || cut {
            self.rotate_at(crossed.as_ref())?;
            self.tombstone_share.open_output(&first.key.user_key);
        }
        // Taken now: rows written one by one after a refused copy reach the
        // same key in the same output, which has passed these boundaries.
        self.commit_cut(crossing, layout_changes || cut);
        self.enter_domain(&first.key.user_key, crossed.is_some());

        let comparator = self.comparator.clone();
        if !write(&mut self.writer, &comparator)? {
            // The rows are written next, as rows: their first key must reach
            // [`Self::write`] as a new key, so it rotates for their layout as
            // it would without this attempt. Advancing the tombstone share to
            // that key and opening an output there do the same again.
            self.current_key = previous_key;
            return Ok(false);
        }
        // The group's later keys move the tombstone share and the boundaries
        // as written rows would; both only move forward, so its last key is
        // enough.
        if !self.range_tombstones.is_empty() {
            self.tombstone_share.advance(
                &self.range_tombstones,
                &last.key.user_key,
                self.comparator.as_ref(),
            );
        }
        // A boundary counts once the output holds half the target when a key
        // crosses it. Where among the group's keys the copy took the output
        // past half is unknown, so its boundaries count only when the output
        // held half before it: never overstating the floor rows would reach.
        let past_half = self.alignment.is_some()
            && !(layout_changes || cut)
            && load.is_some_and(|load| load.reaches(cut_alignment::share_of(self.target_size, 50)));
        if let Some(alignment) = &mut self.alignment {
            alignment.pass_through(&last.key.user_key, past_half, self.comparator.as_ref());
        }
        self.current_key = Some(last.key.user_key.clone());
        self.value_layout = Some(layout);
        self.note_output_base();
        if let Some(marker) = &self.transform_marker {
            self.transforms_after_last_write = marker.load(core::sync::atomic::Ordering::Relaxed);
        }
        Ok(true)
    }

    /// Writes a consumer-provided columnar batch as one columnar block, rotating
    /// to a fresh table first if the current one has reached the target size (a
    /// batch is a block boundary, mirroring the new-key rotation in
    /// [`Self::write`]). Forwards to the inner writer's columnar-ingest path.
    ///
    /// Under a key-domain policy a batch whose keys reach past a domain's end
    /// is written as one block per domain, each in a table of that domain.
    #[cfg(feature = "columnar")]
    pub(crate) fn write_columnar_batch(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<Option<crate::UserKey>> {
        let Some(domain) = self.domain.as_ref() else {
            return self.write_columnar_part(batch);
        };
        let keys = batch.user_keys()?;
        let cmp = self.comparator.clone();
        // Rows `[start, end)` of each domain, keys ascending.
        let mut parts = Vec::new();
        let mut start = 0;
        while start < batch.row_count {
            let end = match domain.domain_end(keys.get(start)?) {
                None => batch.row_count,
                Some(domain_end) => {
                    // The first row at or past the domain's end.
                    let (mut lo, mut hi) = (start + 1, batch.row_count);
                    while lo < hi {
                        let mid = lo + (hi - lo) / 2;
                        if cmp.compare(keys.get(mid)?, &domain_end) == core::cmp::Ordering::Less {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    lo
                }
            };
            parts.push((start, end));
            start = end;
        }
        if let [(0, end)] = parts.as_slice()
            && *end == batch.row_count
        {
            return self.write_columnar_part(batch);
        }
        let mut last = None;
        for (start, end) in parts {
            last = self.write_columnar_part(&batch.rows(start, end)?)?;
        }
        Ok(last)
    }

    /// [`Self::write_columnar_batch`] for a batch within one key domain.
    #[cfg(feature = "columnar")]
    fn write_columnar_part(
        &mut self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<Option<crate::UserKey>> {
        let crossed = match batch.first_user_key()? {
            Some(first) => self.crossed_domain_end(first),
            None => None,
        };
        // A table records one value layout, so a batch after rows starts the
        // next table.
        if self.table_full()
            || crossed.is_some()
            || matches!(
                self.value_layout,
                Some(
                    crate::table::meta::ValueLayout::Whole | crate::table::meta::ValueLayout::Cells
                )
            )
        {
            self.rotate_at(crossed.as_ref())?;
        }
        if let Some(first) = batch.first_user_key()? {
            self.enter_domain(first, crossed.is_some());
        }
        // A batch lands whole, so the output's base is what it held before
        // it: closing after the batch sheds all of the batch's state.
        self.note_output_base();
        let comparator = self.comparator.clone();
        let last = self.writer.write_columnar_batch(batch, &comparator)?;
        self.value_layout = Some(crate::table::meta::ValueLayout::Split);
        Ok(last)
    }

    /// Records what the current output carries at its first record, or before
    /// its first batch: the state its writer holds and the tombstones carried
    /// into it.
    fn note_output_base(&mut self) {
        if self.output_base.is_none() {
            let (tombstones, pieces, _) = self.tombstones_at_key();
            let tombstones_held = if tombstones == 0 {
                0
            } else {
                self.tombstones_held(tombstones, pieces)
            };
            self.output_base = Some((self.writer.held_state_bytes() + tombstones_held, tombstones));
        }
    }

    /// Validates a columnar batch against the ingest contract without writing,
    /// so the ingestion can reject a malformed batch eagerly while block emission
    /// stays deferred. Forwards to the inner writer's validation.
    #[cfg(feature = "columnar")]
    pub(crate) fn validate_columnar_batch(
        &self,
        batch: &crate::table::columnar::ColumnBatch,
    ) -> crate::Result<()> {
        let comparator = self.comparator.clone();
        self.writer.validate_columnar_batch(batch, &comparator)
    }

    /// A flush's tombstones past its last key reach no rotation check on a
    /// key, and the last output's zone is open above. They are checked at the
    /// points where the entries change, each start and each end, in order:
    /// where the output would pass its target, it closes at that point and an
    /// output of tombstones alone takes the zone above. At a start, the check
    /// counts the tombstones starting there, whole, so a group sharing the
    /// last start is split from what came before. Such outputs are separate
    /// L0 runs, like every flush output. Clipping drops these tombstones from
    /// a compaction's outputs instead.
    fn split_tombstones_past_the_last_key(&mut self) -> crate::Result<()> {
        use core::cmp::Ordering;

        if self.clip_range_tombstones {
            return Ok(());
        }
        let comparator = self.comparator.clone();
        // The longest bound any output's key range and sentinel can take.
        let longest = self
            .range_tombstones
            .iter()
            .map(|rt| rt.start.len().max(rt.end.len()))
            .max()
            .unwrap_or(0) as u64;
        // The most filter prefixes an output's sentinel can yield: it is one of
        // the tombstones' bounds.
        let prefixes = self.prefix_extractor.as_ref().map_or(0, |extractor| {
            self.range_tombstones
                .iter()
                .flat_map(|rt| [&rt.start, &rt.end])
                .map(|bound| extractor.prefixes(bound).count())
                .max()
                .unwrap_or(0)
        });
        // An output of tombstones alone writes a synthetic table around them,
        // which its writer, holding no record, does not count yet. Every
        // output is configured alike, so the table is the same for each. It is
        // sized for the longest bound of the whole set, an upper bound for any
        // one zone; since such an output closes only once its tombstones take
        // a block past that table, a long bound further on does not split the
        // short ones before it.
        let alone_overhead = self.writer.tombstone_only_overhead(longest, prefixes)?;
        loop {
            let tombstones = &self.range_tombstones;
            let share = &self.tombstone_share;
            let start = tombstones.get(share.next_pending()).map(|rt| &rt.start);
            let end = share.first_open_end(tombstones);
            let point = match (start, end) {
                (Some(start), Some(end)) => {
                    if comparator.compare(start, end) == Ordering::Greater {
                        end.clone()
                    } else {
                        start.clone()
                    }
                }
                (Some(point), None) | (None, Some(point)) => point.clone(),
                (None, None) => break,
            };
            self.tombstone_share
                .advance(&self.range_tombstones, &point, comparator.as_ref());
            let bytes = self.tombstone_share.bytes(&point);
            let group =
                self.tombstone_share
                    .group(&self.range_tombstones, &point, comparator.as_ref());
            let alone = if self.writer.meta.key_count == 0 {
                alone_overhead
            } else {
                (0, 0)
            };
            // An empty zone is never closed: that output would hold nothing,
            // nor is the last one, with nothing above it. An output holding
            // records is not empty, whatever tombstones it has so far.
            if (bytes > 0 || self.writer.meta.key_count > 0)
                && self.tombstone_share.has_more(&self.range_tombstones)
                && self
                    .load_with_tombstones(
                        bytes + group.bytes,
                        self.tombstone_share.pieces() + group.entries,
                        self.tombstone_share
                            .longest_bound(&point)
                            .max(group.longest),
                        alone,
                    )
                    .reaches(self.target_size)
                // The output after this one holds tombstones alone.
                && self.rotation_sheds(&point, alone_overhead)
            {
                self.current_key = Some(point.clone());
                // Past the output's key domain, its share stops at the
                // domain's end. An output of tombstones alone holds no key and
                // so no domain: the domain the cursor still names is the last
                // row's, which the first output past it already closed at.
                let domain_end = if self.writer.meta.key_count > 0 {
                    self.crossed_domain_end(&point)
                } else {
                    None
                };
                self.rotate_at(domain_end.as_ref())?;
                self.tombstone_share.open_output(&point);
            }
            self.tombstone_share
                .open_through(&self.range_tombstones, &point, comparator.as_ref());
        }
        Ok(())
    }

    /// The pieces of the tombstones kept for tables outside the compaction
    /// that fall below the first output's first key, or past the last
    /// output's last key, within the keys this writer answers for, in runs of
    /// overlapping pieces.
    ///
    /// The outputs together cover from the first one's first key up to just
    /// past the last one's last key, and clipping cuts every tombstone to
    /// that; what lies below or above is a piece nothing else writes. A writer
    /// with no key writes its tombstones whole and has none.
    fn tombstones_beside_the_outputs(&self, side: Beside) -> Vec<Vec<RangeTombstone>> {
        use core::cmp::Ordering::Less;

        let Some(outside) = self.outside.as_ref().filter(|_| self.clip_range_tombstones) else {
            return Vec::new();
        };
        let cmp = self.comparator.as_ref();
        // `[lower, upper)` with `None` as the end of the key space.
        let past_last;
        let (lower, upper) = match side {
            Beside::Below => {
                let Some(first) =
                    self.first_output_key
                        .as_ref()
                        .or(self.writer.meta.first_key.as_ref())
                else {
                    return Vec::new();
                };
                (outside.lower.as_ref(), Some(first))
            }
            Beside::Above => {
                past_last = self
                    .writer
                    .meta
                    .last_key
                    .as_ref()
                    .and_then(|last| crate::range_tombstone::upper_bound_exclusive(last));
                // Nothing lies above a last key with no successor.
                let Some(past_last) = past_last.as_ref() else {
                    return Vec::new();
                };
                (Some(past_last), outside.upper.as_ref())
            }
        };
        // Each group with the end of its span.
        let mut groups: Vec<(Vec<RangeTombstone>, UserKey)> = Vec::new();
        for piece in Self::pieces_within(&outside.tombstones, lower, upper, cmp) {
            match groups.last_mut() {
                Some((pieces, span_end))
                    if cmp.compare(&piece.start, span_end) != core::cmp::Ordering::Greater =>
                {
                    if cmp.compare(span_end, &piece.end) == Less {
                        *span_end = piece.end.clone();
                    }
                    pieces.push(piece);
                }
                _ => {
                    let end = piece.end.clone();
                    groups.push((vec![piece], end));
                }
            }
        }
        groups.into_iter().map(|(pieces, _)| pieces).collect()
    }

    /// Finishes the last table, making sure all data is written durably
    ///
    /// Returns the created tables and whether their directory entries are
    /// durable: the outputs opened here, for tombstones past the last key,
    /// count as much as those opened before.
    pub fn finish(mut self) -> crate::Result<Written> {
        self.split_tombstones_past_the_last_key()?;

        // Same judgment as `rotate` for the LAST output's window — by the
        // LIVE counter here: with no successor output, trailing verdicts
        // (removals after the final write) have nowhere else to land, and a
        // record they removed would otherwise be resurrected by the dedup
        // trading this output back for its inputs.
        if let Some(marker) = &self.transform_marker
            && marker.load(core::sync::atomic::Ordering::Relaxed) > self.transforms_at_output_start
        {
            self.writer.mark_lineage_transformed();
        }
        // This output closes the run: together with the run's first output
        // (no `lineage_prev`) and an unbroken adjacency chain, the marker
        // proves a surviving set is the COMPLETE output set. Only a writer
        // that owned the WHOLE merged stream may claim it (see
        // `owns_whole_run`). Rotation always feeds the successor its
        // triggering record, so the writer finishing here is the run's last
        // non-empty output (or the run produced nothing at all and no meta
        // is written).
        self.writer.spill_block()?;

        // Outputs of tombstones alone, read once the last block is spilled,
        // which sets the last output's key bounds: a flush's pieces outside
        // the last output's key domain, and the pieces past a compaction's
        // outputs that a table outside it needs, each run of overlapping
        // pieces an output of its own, since one spanning keys between two
        // runs could cover a table that lies there. Those below go ahead of
        // the first output, this one when no rotation closed another.
        if self.first_output_key.is_none() {
            self.write_pieces_below_the_first_output()?;
        }
        let (_, above) = self.flush_pieces_outside_the_domain(true);
        let upper = if above.is_empty() {
            None
        } else {
            self.domain
                .as_ref()
                .and_then(|domain| domain.end())
                .cloned()
        };
        let mut alone: Vec<Vec<RangeTombstone>> = Vec::new();
        if !above.is_empty() {
            alone.push(above);
        }
        alone.extend(self.tombstones_beside_the_outputs(Beside::Above));
        if self.owns_whole_run && alone.is_empty() {
            self.writer.mark_lineage_last();
        }

        // Write range tombstones to the last writer. No next table exists,
        // so clip_upper=None falls back to upper_bound_exclusive(last_key)
        // under clipping, and leaves the flush zone open above, up to the
        // output's domain's end under a key-domain policy.
        if !self.range_tombstones.is_empty() {
            Self::write_rts_to_writer(
                self.tombstone_share.zone(&self.range_tombstones, true),
                self.clip_range_tombstones,
                &mut self.writer,
                self.output_lower.as_ref(),
                upper.as_ref(),
                self.comparator.as_ref(),
            );
        }

        for linked in self.linked_blobs.take() {
            self.writer.link_blob_file(linked);
        }
        self.writer
            .own_blob_objects(core::mem::take(&mut self.owned_objects));

        // They follow the last output in the run's lineage, the final one
        // closing it, and are written before it.
        let count = alone.len();
        for (index, pieces) in alone.into_iter().enumerate() {
            let writer = self.successor()?;
            self.write_tombstones_alone(writer, pieces, index + 1 == count)?;
        }

        let path = self.writer.path.clone();
        if let Some((table_id, checksum)) = self
            .writer
            .finish_deferring_dir_sync(self.inherited_age.as_mut())?
        {
            self.results.push((table_id, checksum));
            if let Some(outputs) = &self.outputs {
                outputs.landed(&*self.fs, &path)?;
            }
        }

        Ok(Written {
            tables: self.results,
            entries_durable: self.entries_durable,
        })
    }
}

/// The tables a [`MultiWriter`] wrote.
pub struct Written {
    pub tables: Vec<(TableId, Checksum)>,
    /// Whether every table's directory entry was durable before its table was
    /// written, so the install naming them syncs no folder for them (see
    /// [`super::Table::mark_entry_durable`]).
    pub entries_durable: bool,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests;
