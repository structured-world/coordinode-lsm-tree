// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::writer::Writer;
use crate::blob_tree::handle::BlobIndirection;
use crate::fs::FsFile;
use crate::path::{Path, PathBuf};
use crate::{
    BlobFile, CompressionType, DescriptorTable, SeqNo, SequenceNumberCounter, TreeId,
    file_accessor::FileAccessor,
    fs::{Fs, SyncMode, WriteOrigin},
    vlog::{
        ValueHandle,
        blob_file::{Inner as BlobFileInner, Metadata},
    },
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::ToString, vec::Vec};
use core::sync::atomic::AtomicBool;

#[cfg(test)]
mod tests;

/// Blob file writer, may write multiple blob files
pub(crate) struct MultiWriter {
    fs: Arc<dyn Fs>,

    folder: PathBuf,
    target_size: u64,

    active_writer: Writer,

    /// The lifetime class the active writer's file holds.
    active_class: u8,

    /// The source file whose blocks the active writer's file holds as they
    /// were stored, if it is such a file (see [`Self::copy_block`]).
    active_source: Option<crate::vlog::BlobFileId>,

    /// The dictionary of its own that the source of the active writer's
    /// copied frames holds, which the active writer's file holds as its own.
    active_own: Option<OwnDictionary>,

    /// Writers for the other outputs a write has met, each still filling its
    /// own file while the active one takes frames of its codec and class, by
    /// codec, class, the source of the blocks it copies and the dictionary of
    /// its own it holds. That dictionary goes by its full fingerprint: the
    /// codec names it by a 32-bit id, and two sources' dictionaries can share
    /// one, being hashes of content the user writes.
    ///
    /// A relocation merges its sources by key, so two compression generations
    /// over the same keys alternate on nearly every frame, and values of
    /// different lifetime classes interleave the same way in any write. A file
    /// records one codec and one class, and closing it on every switch would
    /// produce a file per value; keeping one open per pair leaves every file
    /// as full as the size target allows. At most one entry per codec the
    /// sources carry times [`Self::lifetime_groups`], and one per source a
    /// relocation copies blocks of.
    parked: Vec<Parked>,

    /// How many lifetime classes the files are split into.
    lifetime_groups: crate::config::LifetimeGroups,

    results: Vec<BlobFile>,

    id_generator: SequenceNumberCounter,

    compression: CompressionType,
    passthrough_compression: CompressionType,

    /// How every file this writer produces sizes its frames.
    sizes: super::writer::FrameSizes,

    /// The frame classes a dictionary codec compresses with its dictionary in
    /// every file this writer produces.
    dictionary_classes: crate::config::DictionaryClasses,

    /// Durability level wired from `Config::sync_mode`, stamped on every
    /// rotated blob writer.
    sync_mode: SyncMode,

    /// Whether the files and their folder are synced at finish, stamped on
    /// every rotated blob writer.
    durability: crate::config::FlushDurability,

    /// `Config::writeback_bytes`, stamped on every rotated blob writer.
    writeback_bytes: u64,

    /// Where every file's large values compress their chunks.
    #[cfg(feature = "std")]
    parallel: Option<crate::table::writer::ParallelCompression>,

    /// Dictionary for `ZstdDict` compression, shared across all rotated writers.
    #[cfg(zstd_any)]
    zstd_dictionary: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,

    /// Whether zstd levels 19-22 run the `btultra2` two-pass seed, shared
    /// across all rotated writers so one run is uniform.
    #[cfg(zstd_any)]
    zstd_two_pass_seed: bool,

    /// The bytes of the dictionary each file this writer compresses itself
    /// builds from its own values (see `Writer::use_file_dictionary`); a file
    /// of frames copied as they are builds none.
    #[cfg(zstd_any)]
    file_dictionary: u32,

    /// The tree's dictionary set, used to PIN the dictionary each finished file
    /// records on its handle (see `blob_file::Inner::zstd_dictionary`).
    ///
    /// Not the same question as [`Self::zstd_dictionary`], which is what this
    /// writer COMPRESSES with. A relocation pass compresses with nothing (it
    /// copies frames verbatim) yet still records a codec, so the file it
    /// produces needs a dictionary its own descriptor names and the write slot
    /// cannot answer.
    #[cfg(zstd_any)]
    zstd_dictionaries: crate::compression::ZstdDictionaries,

    tree_id: TreeId,
    descriptor_table: Option<Arc<DescriptorTable>>,

    /// The tree's clock, handed to every blob file writer.
    clock: Option<Arc<dyn crate::Clock>>,

    /// Where a compaction learns of every blob file this writer creates, so
    /// it can remove the ones it never installs. `None` outside compaction.
    outputs: Option<crate::compaction::output_ledger::OutputLedger>,

    /// How many of `results`, the first ones, have landed in `outputs`.
    landed: usize,

    /// The key domains no blob file spans, the current one's, and the
    /// comparator they are intervals of.
    domain: Option<(crate::key_domain::DomainCursor, crate::SharedComparator)>,

    /// What the blob files are written for.
    origin: Option<WriteOrigin>,

    /// The sources whose blocks this writer copied whole, each into files of
    /// its own, at most [`MAX_COPY_SOURCES`].
    copy_sources: Vec<crate::vlog::BlobFileId>,
}

/// At most how many sources one writer copies blocks of whole. Each holds a
/// writer of its own open, a file and its buffer, until the pass ends; past
/// this many, a relocation over many stale files writes the blocks of the
/// rest value by value instead of opening a file per source.
pub(crate) const MAX_COPY_SOURCES: usize = 16;

/// A dictionary a copied file holds of its own.
#[cfg(zstd_any)]
type OwnDictionary = alloc::sync::Arc<crate::compression::ZstdDictionary>;

/// Without zstd no file holds a dictionary of its own: a type with no value.
#[cfg(not(zstd_any))]
#[derive(Clone)]
struct OwnDictionary(core::convert::Infallible);

/// A parked output: its codec, class, the source of the blocks it copies,
/// the dictionary of its own it holds, and its writer.
type Parked = (
    CompressionType,
    u8,
    Option<crate::vlog::BlobFileId>,
    Option<OwnDictionary>,
    Writer,
);

/// The identity `own` keeps an output apart by: the dictionary's full
/// fingerprint, which its 32-bit codec id is a truncation of.
fn own_key(own: Option<&OwnDictionary>) -> Option<u64> {
    #[cfg(zstd_any)]
    {
        own.map(|dict| dict.id64())
    }
    #[cfg(not(zstd_any))]
    {
        let _ = own;
        None
    }
}

/// `codec` with its dictionary left out: the codec a value of a file with a
/// dictionary of its own is written anew under, decoded.
fn without_dictionary(codec: CompressionType) -> CompressionType {
    #[cfg(zstd_any)]
    if let CompressionType::ZstdDict { level, .. } = codec {
        return CompressionType::Zstd(level);
    }
    codec
}

/// The dictionary of its own `source` holds, which the file its frames are
/// copied into holds as its own.
fn own_dictionary(source: &BlobFile) -> Option<&OwnDictionary> {
    #[cfg(zstd_any)]
    {
        source
            .0
            .zstd_dictionary
            .as_ref()
            .filter(|_| source.has_own_dictionary())
    }
    #[cfg(not(zstd_any))]
    {
        let _ = source;
        None
    }
}

impl MultiWriter {
    /// A blob file writer whose files carry no origin.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[cfg(test)]
    pub(crate) fn new<P: AsRef<Path>>(
        id_generator: SequenceNumberCounter,
        folder: P,
        tree_id: TreeId,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
    ) -> crate::Result<Self> {
        Self::open(id_generator, folder, tree_id, descriptor_table, fs, None)
    }

    /// Initializes a new blob file writer, its blob files written as
    /// `origin`'s.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn new_for<P: AsRef<Path>>(
        id_generator: SequenceNumberCounter,
        folder: P,
        tree_id: TreeId,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
        origin: WriteOrigin,
    ) -> crate::Result<Self> {
        Self::open(
            id_generator,
            folder,
            tree_id,
            descriptor_table,
            fs,
            Some(origin),
        )
    }

    fn open<P: AsRef<Path>>(
        id_generator: SequenceNumberCounter,
        folder: P,
        tree_id: TreeId,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
        origin: Option<WriteOrigin>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        let blob_file_id = id_generator.next();
        let blob_file_path = folder.join(blob_file_id.to_string());

        Ok(Self {
            id_generator,
            folder: folder.into(),
            target_size: 64 * 1_024 * 1_024,

            active_writer: Writer::open(blob_file_path, blob_file_id, tree_id, &*fs, origin)?,
            origin,
            active_class: 0,
            active_source: None,
            active_own: None,
            parked: Vec::new(),
            lifetime_groups: crate::config::LifetimeGroups::ONE,

            results: Vec::new(),

            compression: CompressionType::None,
            passthrough_compression: CompressionType::None,
            sizes: super::writer::FrameSizes::DEFAULT,
            dictionary_classes: crate::config::DictionaryClasses::DEFAULT,
            sync_mode: SyncMode::Normal,
            durability: crate::config::FlushDurability::Synced,
            writeback_bytes: 0,
            #[cfg(feature = "std")]
            parallel: None,

            #[cfg(zstd_any)]
            zstd_dictionary: None,
            #[cfg(zstd_any)]
            zstd_two_pass_seed: true,
            #[cfg(zstd_any)]
            file_dictionary: 0,
            #[cfg(zstd_any)]
            zstd_dictionaries: crate::compression::ZstdDictionaries::new(),

            tree_id,
            descriptor_table,
            clock: None,
            fs,
            outputs: None,
            landed: 0,
            domain: None,
            copy_sources: Vec::new(),
        })
    }

    /// Closes every file this writer holds open before a value whose key lies
    /// past the key domain of the values written so far, so no blob file holds
    /// values of two of `domains`, intervals of `comparator`'s order: expiring
    /// a domain leaves its blob files wholly dead (see [`crate::KeyDomains`]).
    #[must_use]
    pub(crate) fn use_key_domains(
        mut self,
        domains: Option<Arc<dyn crate::KeyDomains>>,
        comparator: crate::SharedComparator,
    ) -> Self {
        self.domain = domains.map(|d| (crate::key_domain::DomainCursor::new(d), comparator));
        self
    }

    /// Takes `key`, the key of the value about to be written, into the
    /// current domain, closing every open file first when it lies past it.
    fn enter_key(&mut self, key: &[u8]) -> crate::Result<()> {
        let Some((domain, comparator)) = &mut self.domain else {
            return Ok(());
        };
        let crossed = domain.crosses(key, comparator.as_ref()).is_some();
        domain.enter(key, crossed);
        if crossed {
            for (codec, _, _, _, writer) in core::mem::take(&mut self.parked) {
                self.land(writer, codec)?;
            }
            if self.active_writer.item_count > 0 {
                self.rotate()?;
            }
        }
        Ok(())
    }

    /// Records the blob file already open and every one this writer creates
    /// after it in `outputs`.
    #[must_use]
    pub(crate) fn use_output_ledger(
        mut self,
        outputs: crate::compaction::output_ledger::OutputLedger,
    ) -> Self {
        outputs.record_blob_file(
            self.active_writer.blob_file_id,
            self.active_writer.path.clone(),
            self.fs.clone(),
        );
        self.outputs = Some(outputs);
        self
    }

    /// Wires the tree's `Config::sync_mode` through to every blob file this
    /// writer finalizes (active + rotated).
    #[must_use]
    pub(crate) fn use_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self.active_writer.sync_mode = sync_mode;
        self
    }

    /// Leaves every blob file this writer finishes, and their folder,
    /// unsynced under
    /// [`FlushDurability::Deferred`](crate::config::FlushDurability::Deferred),
    /// for the install that persists them to sync.
    #[must_use]
    pub(crate) fn use_durability(mut self, durability: crate::config::FlushDurability) -> Self {
        self.durability = durability;
        self.active_writer.durability = durability;
        self
    }

    /// Reads the time on the tree's `clock` in every blob file written.
    #[must_use]
    pub(crate) fn use_clock(mut self, clock: Option<Arc<dyn crate::Clock>>) -> Self {
        self.active_writer = self.active_writer.use_clock(clock.clone());
        self.clock = clock;
        self
    }

    /// Wires the tree's `Config::writeback_bytes` through to every blob file
    /// this writer writes (active + rotated).
    #[must_use]
    pub(crate) fn use_writeback_bytes(mut self, bytes: u64) -> Self {
        self.writeback_bytes = bytes;
        self.active_writer = self.active_writer.use_writeback_bytes(bytes);
        self
    }

    /// Compresses the chunks of large values on `parallel`'s workers in every
    /// file this writer writes (active + rotated).
    #[cfg(feature = "std")]
    #[must_use]
    pub(crate) fn use_parallel_compression(
        mut self,
        parallel: Option<crate::table::writer::ParallelCompression>,
    ) -> Self {
        self.active_writer = self
            .active_writer
            .use_parallel_compression(parallel.clone());
        self.parallel = parallel;
        self
    }

    /// Sets the blob file target size.
    #[must_use]
    pub(crate) fn use_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Sets the codec the files this writer produces hold, for a writer that
    /// copies the frames of files under that codec: a copied frame goes as it
    /// is, and a block's values, which are copied out of the block they shared,
    /// are compressed into new blocks under the same codec.
    pub(crate) fn use_passthrough_compression(mut self, compression: CompressionType) -> Self {
        assert_eq!(self.compression, CompressionType::None);
        self.passthrough_compression = compression;
        self.restamp_active(compression, None);
        self
    }

    /// The codec a file of output `passthrough` compresses with and records:
    /// the passthrough codec of a copying writer, this writer's own otherwise.
    fn codec_for(&self, passthrough: CompressionType) -> CompressionType {
        if passthrough == CompressionType::None {
            self.compression
        } else {
            passthrough
        }
    }

    /// The dictionary `codec` compresses with: `own`, the one a copied file
    /// holds of its own, when given; else the one this writer was given for
    /// its own codec, else the one the tree's set holds for it.
    #[cfg(zstd_any)]
    fn dictionary_for(
        &self,
        codec: CompressionType,
        own: Option<&OwnDictionary>,
    ) -> Option<alloc::sync::Arc<crate::compression::ZstdDictionary>> {
        if let Some(own) = own {
            return Some(alloc::sync::Arc::clone(own));
        }
        if codec == self.compression {
            return self.zstd_dictionary.clone();
        }
        self.zstd_dictionaries.for_compression(codec).ok().flatten()
    }

    /// Makes the active writer, which holds nothing yet, compress with and
    /// record output `passthrough`'s codec, and hold `own` as its own
    /// dictionary.
    fn restamp_active(&mut self, passthrough: CompressionType, own: Option<&OwnDictionary>) {
        let codec = self.codec_for(passthrough);
        self.active_writer.compression = codec;
        #[cfg(zstd_any)]
        {
            self.active_writer.zstd_dictionary = self.dictionary_for(codec, own);
            self.active_writer.dictionary_in_file = own.is_some();
            self.active_writer
                .set_file_dictionary(self.file_dictionary_for(passthrough));
        }
        #[cfg(not(zstd_any))]
        let _ = own;
    }

    /// The dictionary a file of output `passthrough` builds of its own: one
    /// this writer compresses itself only.
    #[cfg(zstd_any)]
    fn file_dictionary_for(&self, passthrough: CompressionType) -> u32 {
        if passthrough == CompressionType::None {
            self.file_dictionary
        } else {
            0
        }
    }

    /// Has every file this writer compresses itself build a dictionary of
    /// `bytes` from its own values.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_file_dictionary(mut self, bytes: u32) -> Self {
        self.file_dictionary = bytes;
        let own = self.file_dictionary_for(self.passthrough_compression);
        self.active_writer.set_file_dictionary(own);
        self
    }

    /// Sizes the frames of every file this writer produces by `sizes`.
    #[must_use]
    pub(crate) fn use_frame_sizes(mut self, sizes: super::writer::FrameSizes) -> Self {
        self.sizes = sizes;
        self.active_writer.sizes = sizes;
        self
    }

    /// Compresses only the frames of `classes` with a dictionary codec's
    /// dictionary in every file this writer produces.
    #[must_use]
    pub(crate) fn use_dictionary_classes(
        mut self,
        classes: crate::config::DictionaryClasses,
    ) -> Self {
        self.dictionary_classes = classes;
        self.active_writer.dictionary_classes = classes;
        self
    }

    /// Splits the files this writer produces into `groups` lifetime classes,
    /// the files written so far taking the class of a fresh value.
    #[must_use]
    pub(crate) fn use_lifetime_groups(mut self, groups: crate::config::LifetimeGroups) -> Self {
        self.lifetime_groups = groups;
        let class = groups.fresh();
        self.active_class = class;
        self.active_writer
            .set_lifetime_class(class, groups.write_lifetime(class));
        self
    }

    /// The lifetime classes this writer splits its files into.
    pub(crate) fn lifetime_groups(&self) -> crate::config::LifetimeGroups {
        self.lifetime_groups
    }

    /// Directs the next frames to a file of lifetime `class` that records the
    /// codec of the frames written so far. `class` past the last group goes to
    /// the last group.
    ///
    /// # Errors
    ///
    /// Propagates the creation of the first file for a class not met before.
    pub(crate) fn select_lifetime_class(&mut self, class: u8) -> crate::Result<()> {
        self.select_output(self.passthrough_compression, class)
    }

    /// Directs the next frames to a file that records `compression` and holds
    /// lifetime `class`.
    ///
    /// The relocation counterpart of [`Self::use_passthrough_compression`],
    /// which fixes one codec for the whole pass. A relocation copies frames
    /// VERBATIM out of sources that need not share a codec (the blob policy may
    /// have moved since they were written), while a blob file records exactly
    /// one. Each codec therefore gets its own output file, and so does each
    /// lifetime class; switching parks the current one rather than closing it
    /// (see [`Self::parked`]).
    ///
    /// # Errors
    ///
    /// Propagates the creation of the first file for a pair not met before.
    pub(crate) fn select_output(
        &mut self,
        compression: CompressionType,
        class: u8,
    ) -> crate::Result<()> {
        self.select(compression, class, None, None)
    }

    /// Directs the next frames, copied as they are out of `source`, to a file
    /// of lifetime `class` that records `source`'s codec and holds the
    /// dictionary of its own `source` holds, if any: the one that decodes
    /// them, whatever other dictionary shares its id. Returns whether the
    /// frames go as they are.
    ///
    /// Each output holding a source's dictionary is a file held open until
    /// the pass ends, so at most [`MAX_COPY_SOURCES`] are: past them, a
    /// source's values are written anew, decoded, into an output of its
    /// codec without the dictionary, and this returns `false`.
    ///
    /// # Errors
    ///
    /// Propagates the creation of the first file for an output not met
    /// before.
    pub(crate) fn select_copy_output(
        &mut self,
        source: &BlobFile,
        class: u8,
    ) -> crate::Result<bool> {
        let own = own_dictionary(source);
        if own.is_some() && !self.holds_own_output(own) && self.own_outputs() >= MAX_COPY_SOURCES {
            self.select(without_dictionary(source.compression()), class, None, None)?;
            return Ok(false);
        }
        self.select(source.compression(), class, None, own)?;
        Ok(true)
    }

    /// Whether an output, active or parked, holds `own`.
    fn holds_own_output(&self, own: Option<&OwnDictionary>) -> bool {
        let key = own_key(own);
        own_key(self.active_own.as_ref()) == key
            || self
                .parked
                .iter()
                .any(|(_, _, _, o, _)| own_key(o.as_ref()) == key)
    }

    /// How many outputs, active or parked, hold a source's dictionary.
    fn own_outputs(&self) -> usize {
        usize::from(self.active_own.is_some())
            + self
                .parked
                .iter()
                .filter(|(_, _, _, o, _)| o.is_some())
                .count()
    }

    /// Directs the next frames to the output of `compression`, `class`,
    /// `source`, the file whose blocks it copies, if any, and `own`, the
    /// dictionary of its own its file holds, if any.
    fn select(
        &mut self,
        compression: CompressionType,
        class: u8,
        source: Option<crate::vlog::BlobFileId>,
        own: Option<&OwnDictionary>,
    ) -> crate::Result<()> {
        let class = self.lifetime_groups.cap(class);
        let own_id = own_key(own);
        if self.passthrough_compression == compression
            && self.active_class == class
            && self.active_source == source
            && own_key(self.active_own.as_ref()) == own_id
        {
            return Ok(());
        }
        let next = if let Some(at) = self.parked.iter().position(|(c, k, s, o, _)| {
            *c == compression && *k == class && *s == source && own_key(o.as_ref()) == own_id
        }) {
            self.parked.swap_remove(at).4
        } else if self.active_writer.item_count == 0 && self.active_writer.len_written() == 0 {
            // Nothing written constrains an untouched writer: restamp it.
            // Only a codec or dictionary switch restamps the codec: a class
            // switch keeps what the writer records, which for a compressing
            // writer is its own.
            if compression != self.passthrough_compression
                || own_key(self.active_own.as_ref()) != own_id
            {
                self.restamp_active(compression, own);
            }
            self.active_writer
                .set_lifetime_class(class, self.lifetime_groups.write_lifetime(class));
            self.passthrough_compression = compression;
            self.active_class = class;
            self.active_source = source;
            self.active_own = own.cloned();
            return Ok(());
        } else {
            self.fresh_writer(compression, class, own)?
        };
        let previous = core::mem::replace(&mut self.active_writer, next);
        self.parked.push((
            self.passthrough_compression,
            self.active_class,
            self.active_source,
            core::mem::replace(&mut self.active_own, own.cloned()),
            previous,
        ));
        self.passthrough_compression = compression;
        self.active_class = class;
        self.active_source = source;
        Ok(())
    }

    /// Whether a block of `source` holding the keys from `first` to `last` can
    /// be copied whole: its keys lie in one key domain (always, without a
    /// domain policy), so its file spans none, and `source` is one of the
    /// sources this writer copies blocks of, or one more fits.
    pub(crate) fn can_copy_block(
        &self,
        source: crate::vlog::BlobFileId,
        first: &[u8],
        last: &[u8],
    ) -> bool {
        let source_fits =
            self.copy_sources.len() < MAX_COPY_SOURCES || self.copy_sources.contains(&source);
        source_fits
            && self
                .domain
                .as_ref()
                .is_none_or(|(domain, comparator)| domain.holds(first, last, comparator.as_ref()))
    }

    /// Writes `block`, a block frame of `source` a scan checked, as it is into
    /// a file of lifetime `class` that records `source`'s codec, holds blocks
    /// of `source` alone and holds the dictionary of its own `source` holds,
    /// if any, and returns that file and the block's frame in it: each value
    /// keeps its place in the block. `key` is one of the block's keys.
    ///
    /// A source's blocks reach this in the source's order, which is key order,
    /// while the other values of the pass interleave with them: a file of its
    /// own keeps every file's values in key order.
    ///
    /// # Errors
    ///
    /// `block` is not one whole block frame, or an IO error occurs.
    pub(crate) fn copy_block(
        &mut self,
        key: &[u8],
        source: &BlobFile,
        class: u8,
        block: &super::writer::CopiedBlock<'_>,
    ) -> crate::Result<(crate::vlog::BlobFileId, u32)> {
        self.enter_key(key)?;
        let id = source.id();
        self.select(
            source.compression(),
            class,
            Some(id),
            own_dictionary(source),
        )?;
        if !self.copy_sources.contains(&id) {
            self.copy_sources.push(id);
        }
        let frame = self.active_writer.write_block_verbatim(block)?;
        let file = self.active_writer.blob_file_id();
        if self.active_writer.len_written() >= self.target_size {
            self.rotate()?;
        }
        Ok((file, frame))
    }

    /// A new writer for the next blob file of lifetime `class`, under output
    /// `passthrough`'s codec (see [`Self::codec_for`]).
    fn fresh_writer(
        &self,
        passthrough: CompressionType,
        class: u8,
        own: Option<&OwnDictionary>,
    ) -> crate::Result<Writer> {
        let id = self.id_generator.next();
        let path = self.folder.join(id.to_string());
        let w = Writer::open(path, id, self.tree_id, &*self.fs, self.origin)?;
        // Recorded once it exists: a failed constructor created nothing ours.
        if let Some(outputs) = &self.outputs {
            outputs.record_blob_file(id, w.path.clone(), self.fs.clone());
        }
        let codec = self.codec_for(passthrough);
        let mut w = w
            .use_compression(codec)
            .use_frame_sizes(self.sizes)
            .use_dictionary_classes(self.dictionary_classes)
            .use_sync_mode(self.sync_mode)
            .use_writeback_bytes(self.writeback_bytes)
            .use_clock(self.clock.clone());
        w.durability = self.durability;
        #[cfg(feature = "std")]
        let mut w = w.use_parallel_compression(self.parallel.clone());
        w.set_lifetime_class(class, self.lifetime_groups.write_lifetime(class));
        #[cfg(zstd_any)]
        let mut w = w
            .use_zstd_dictionary(self.dictionary_for(codec, own))
            .use_zstd_two_pass_seed(self.zstd_two_pass_seed)
            .use_file_dictionary(self.file_dictionary_for(passthrough));
        #[cfg(zstd_any)]
        {
            w.dictionary_in_file = own.is_some();
        }
        #[cfg(not(zstd_any))]
        let _ = own;
        Ok(w)
    }

    /// Sets the compression method.
    #[must_use]
    #[doc(hidden)]
    pub(crate) fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression.clone_from(&compression);
        self.active_writer.compression = compression;
        self
    }

    /// Provides the zstd dictionary for [`CompressionType::ZstdDict`] writes.
    ///
    /// The dictionary is propagated to every rotated writer so that all blob
    /// files produced by this `MultiWriter` use the same dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_zstd_dictionary(
        mut self,
        dict: Option<alloc::sync::Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.active_writer = self.active_writer.use_zstd_dictionary(dict.clone());
        self.zstd_dictionary = dict;
        self
    }

    /// Selects whether zstd levels 19-22 run the `btultra2` two-pass seed,
    /// propagated to every rotated writer so all blob files of one run are
    /// written under the same strategy.
    ///
    /// See [`crate::runtime_config::RuntimeConfig::zstd_two_pass_seed`].
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_zstd_two_pass_seed(mut self, enabled: bool) -> Self {
        self.active_writer = self.active_writer.use_zstd_two_pass_seed(enabled);
        self.zstd_two_pass_seed = enabled;
        self
    }

    /// Provides the tree's dictionary set, so each finished file can pin the
    /// dictionary its own recorded descriptor names.
    ///
    /// Required wherever the files this writer produces are `ZstdDict`, whether
    /// this writer compresses them itself or passes already-compressed frames
    /// through: the handle a reader gets must carry its own dictionary.
    #[cfg(zstd_any)]
    #[must_use]
    pub(crate) fn use_zstd_dictionaries(
        mut self,
        dicts: crate::compression::ZstdDictionaries,
    ) -> Self {
        self.zstd_dictionaries = dicts;
        self
    }

    /// Sets up a new writer for the next blob file.
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating blob file writer");

        let new_writer = self.fresh_writer(
            self.passthrough_compression,
            self.active_class,
            self.active_own.as_ref(),
        )?;

        let old_writer = core::mem::replace(&mut self.active_writer, new_writer);
        self.land(old_writer, self.passthrough_compression)
    }

    /// Finishes `writer`, whose file records `passthrough`, into the results.
    fn land(&mut self, writer: Writer, passthrough: CompressionType) -> crate::Result<()> {
        let blob_file = Self::consume_writer(
            writer,
            passthrough,
            self.descriptor_table.clone(),
            &self.fs,
            #[cfg(zstd_any)]
            &self.zstd_dictionaries,
        )?;
        // Before the next file takes more room.
        if let (Some(outputs), Some(file)) = (&self.outputs, &blob_file) {
            outputs.landed(&*self.fs, file.path())?;
            self.landed += 1;
        }
        self.results.extend(blob_file);

        Ok(())
    }

    fn consume_writer(
        writer: Writer,
        passthrough_compression: CompressionType,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: &Arc<dyn Fs>,
        #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
    ) -> crate::Result<Option<BlobFile>> {
        if writer.item_count > 0 {
            let blob_file_id = writer.blob_file_id;
            let path = writer.path.clone();

            log::debug!(
                "Created blob file #{blob_file_id:?} ({} items, {} userdata bytes)",
                writer.item_count,
                writer.uncompressed_bytes,
            );

            let tree_id = writer.tree_id;
            // Taken before `finish` consumes the writer.
            #[cfg(zstd_any)]
            let writer_dictionary = writer.zstd_dictionary.clone();
            #[cfg(zstd_any)]
            let dictionary_in_file = writer.dictionary_in_file;

            // A failed finish (the last block's compression, a write, the
            // sync) leaves a partial file no handle will ever own.
            let (metadata, checksum, frames) = match writer.finish() {
                Ok(finished) => finished,
                Err(e) => {
                    if let Err(remove) = fs.remove_file(&path) {
                        log::warn!(
                            "Could not delete unfinished blob file at {}: {remove:?}",
                            path.display(),
                        );
                    }
                    return Err(e);
                }
            };

            // What the FILE will record, which is what its reader resolves
            // against: the passthrough codec when relocation is stamping the
            // source's own, else what this writer compressed with.
            let recorded_compression = if passthrough_compression == CompressionType::None {
                metadata.compression
            } else {
                passthrough_compression
            };

            // Resolved BEFORE the file is opened and its descriptor published,
            // because those are side effects only the finished handle's `Drop`
            // knows how to undo. Failing between them would strand both: an
            // orphan file no version names, and an open descriptor in the
            // shared table for the life of the process.
            //
            // The set answers first, then the dictionary this writer compressed
            // with when THAT is the one the file records: a writer holding the
            // matching dictionary needs no set to pin it, since it just used
            // those bytes. The set is what answers for a relocation, which
            // compresses nothing and records the source's codec.
            //
            // A file that stores its own dictionary decodes with that one
            // alone, so the writer's comes first there: a dictionary of the
            // set under the same 32-bit id would be another dictionary.
            #[cfg(zstd_any)]
            let own_dictionary = match (&recorded_compression, &writer_dictionary) {
                (CompressionType::ZstdDict { dict_id, .. }, Some(d))
                    if dictionary_in_file && d.id() == *dict_id =>
                {
                    Some(alloc::sync::Arc::clone(d))
                }
                _ => None,
            };
            #[cfg(zstd_any)]
            let zstd_dictionary = match own_dictionary.map_or_else(
                || zstd_dictionaries.for_compression(recorded_compression),
                |own| Ok(Some(own)),
            ) {
                Ok(dict) => dict,
                Err(e) => match (&recorded_compression, &writer_dictionary) {
                    (CompressionType::ZstdDict { dict_id, .. }, Some(d)) if d.id() == *dict_id => {
                        Some(alloc::sync::Arc::clone(d))
                    }
                    _ => {
                        // No handle exists yet, so no `Drop` will reclaim this;
                        // the unlink has to happen here.
                        if let Err(remove) = fs.remove_file(&path) {
                            log::warn!(
                                "Could not delete unusable blob file at {}: {remove:?}",
                                path.display(),
                            );
                        }
                        return Err(e);
                    }
                },
            };

            let file: Arc<dyn FsFile> =
                Arc::from(fs.open(&path, &crate::fs::FsOpenOptions::new().read(true))?);
            let file_accessor = if let Some(dt) = descriptor_table {
                FileAccessor::DescriptorTable {
                    table: dt,
                    fs: fs.clone(),
                }
            } else {
                FileAccessor::File(file.clone())
            };
            file_accessor.insert_for_blob_file((tree_id, blob_file_id).into(), file);

            let blob_file = BlobFile(Arc::new(BlobFileInner {
                checksum,
                tree_id,
                path,
                is_deleted: AtomicBool::new(false),
                punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
                punched_through: once_cell::race::OnceBox::new(),
                // A freshly written file is whole: nothing has been reclaimed.
                live_data_start: 0,
                frames,
                id: blob_file_id,
                file_accessor,
                #[cfg(zstd_any)]
                zstd_dictionary,
                #[cfg(zstd_any)]
                dictionary_in_file,
                meta: Metadata {
                    id: blob_file_id,
                    version: metadata.version,
                    created_at: metadata.created_at,
                    item_count: metadata.item_count,
                    total_compressed_bytes: metadata.total_compressed_bytes,
                    total_uncompressed_bytes: metadata.total_uncompressed_bytes,
                    block_entry_bytes: metadata.block_entry_bytes,
                    block_frame_bytes: metadata.block_frame_bytes,
                    key_range: metadata.key_range,

                    compression: recorded_compression,
                    lifetime_class: metadata.lifetime_class,
                },
                fs: fs.clone(),
                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),
            }));

            Ok(Some(blob_file))
        } else {
            log::debug!(
                "Blob file writer at {} has written no data, deleting empty blob file",
                writer.path.display(),
            );

            if let Err(e) = fs.remove_file(&writer.path) {
                log::warn!(
                    "Could not delete empty blob file at {}: {e:?}",
                    writer.path.display(),
                );
            }

            Ok(None)
        }
    }

    /// The indirection of a value of `size` bytes written into the active
    /// file at `at`, rotating to the next file once the active one reached
    /// its target.
    fn written(
        &mut self,
        at: super::writer::FrameRef,
        size: u32,
    ) -> crate::Result<BlobIndirection> {
        let indirection = BlobIndirection {
            vhandle: ValueHandle {
                blob_file_id: self.active_writer.blob_file_id(),
                frame: at.frame,
                index: at.index,
            },
            size,
            stored: at.stored,
        };
        if self.active_writer.len_written() >= self.target_size {
            self.rotate()?;
        }
        Ok(indirection)
    }

    /// Writes an item, into the frame its size calls for.
    ///
    /// Returns the [`BlobIndirection`] the table stores for it.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn write(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
    ) -> crate::Result<BlobIndirection> {
        self.write_pre(key, seqno, value, None)
    }

    /// The codec a value of `value_len` bytes of lifetime `class` written
    /// next would be compressed under as a whole, for a worker to compress it
    /// ahead (see [`super::writer::Writer::value_codec`]): asked of the file
    /// that is taking `class` now, `None` when another one is.
    #[cfg(feature = "std")]
    pub(crate) fn value_codec(
        &self,
        class: u8,
        value_len: u32,
        queued: u64,
    ) -> Option<super::writer::OwnedCodec> {
        let takes_class = self.lifetime_groups.cap(class) == self.active_class
            && self.active_source.is_none()
            && self.domain.is_none();
        takes_class
            .then(|| self.active_writer.value_codec(value_len, queued))
            .flatten()
    }

    /// [`Self::write`], the value compressed ahead as `pre` when it was (see
    /// [`Self::value_codec`]).
    ///
    /// # Errors
    ///
    /// As [`Self::write`].
    pub(crate) fn write_pre(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        pre: Option<super::writer::PreCompressed>,
    ) -> crate::Result<BlobIndirection> {
        let size = super::writer::check_size_cap(value.len())?;
        self.enter_key(key)?;
        let at = self.active_writer.write_pre(key, seqno, value, pre)?;
        self.written(at, size)
    }

    /// Writes `frames`, the frames of one whole value under the codec the
    /// active file records, as they are. `value_len` is the value's decoded
    /// length.
    ///
    /// # Errors
    ///
    /// The frames are not one whole value under `key`, or an IO error occurs.
    pub(crate) fn write_verbatim(
        &mut self,
        key: &[u8],
        frames: &[crate::Slice],
        value_len: u32,
    ) -> crate::Result<BlobIndirection> {
        self.enter_key(key)?;
        let at = self.active_writer.write_verbatim(key, frames, value_len)?;
        self.written(at, value_len)
    }

    /// [`Self::write_verbatim`] with the value listed at `seqno`, its payload
    /// still written as stored.
    ///
    /// # Errors
    ///
    /// As [`Self::write_verbatim`].
    pub(crate) fn write_verbatim_at(
        &mut self,
        key: &[u8],
        frames: &[crate::Slice],
        value_len: u32,
        seqno: SeqNo,
    ) -> crate::Result<BlobIndirection> {
        self.enter_key(key)?;
        let at = self
            .active_writer
            .write_verbatim_at(key, frames, value_len, seqno)?;
        self.written(at, value_len)
    }

    /// Writes `value`, a value new to the value log, into a file of lifetime
    /// `class` under `compression`: the file a relocation copies frames of the
    /// same codec into, so a pass that both relocates and separates new values
    /// cuts one set of files.
    ///
    /// # Errors
    ///
    /// Propagates the encoding (a dictionary codec without the dictionary this
    /// writer holds) and the write.
    pub(crate) fn write_fresh(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        compression: CompressionType,
        class: u8,
    ) -> crate::Result<BlobIndirection> {
        let size = super::writer::check_size_cap(value.len())?;
        self.enter_key(key)?;
        self.select_output(compression, class)?;
        let at = self.active_writer.write(key, seqno, value)?;
        self.written(at, size)
    }

    pub(crate) fn finish(mut self) -> crate::Result<Vec<BlobFile>> {
        let mut writers = core::iter::once((self.passthrough_compression, self.active_writer))
            .chain(
                self.parked
                    .into_iter()
                    .map(|(codec, _, _, _, writer)| (codec, writer)),
            );
        while let Some((passthrough, writer)) = writers.next() {
            match Self::consume_writer(
                writer,
                passthrough,
                self.descriptor_table.clone(),
                &self.fs,
                #[cfg(zstd_any)]
                &self.zstd_dictionaries,
            ) {
                Ok(blob_file) => self.results.extend(blob_file),
                Err(e) => {
                    // The write as a whole failed, so no version will ever name
                    // the files finished so far: marked deleted, their handles'
                    // drop unlinks them and evicts their descriptors. The
                    // writers not reached yet never produced a handle, so their
                    // files are removed here.
                    for file in &self.results {
                        file.mark_as_deleted();
                    }
                    for (_, unfinished) in writers {
                        let path = unfinished.path.clone();
                        drop(unfinished);
                        if let Err(rm) = self.fs.remove_file(&path) {
                            log::warn!(
                                "Could not delete unfinished blob file at {}: {rm:?}",
                                path.display(),
                            );
                        }
                    }
                    return Err(e);
                }
            }
        }
        // A new file's directory entry is durable only once its directory is
        // synced (POSIX `fsync(2)`), and the manifest edit that names these
        // files follows: synced once here for every file the write made. The
        // manifest is in the folder above; on another device (a nested mount)
        // a barrier would not order this sync before it, so that is in full.
        // A deferred write leaves this to the install that persists it.
        let mode = crate::file::folder_sync_mode(&*self.fs, &self.folder, self.sync_mode);
        if !self.results.is_empty()
            && self.durability == crate::config::FlushDurability::Synced
            && let Err(e) = crate::file::fsync_directory(&self.folder, &*self.fs, mode)
        {
            // Not durable, so no version may name them.
            for file in &self.results {
                file.mark_as_deleted();
            }
            return Err(e.into());
        }
        // The files the rotations finished have landed already; the rest land
        // now. A run stopped here removes them with the rest of its outputs.
        if let Some(outputs) = &self.outputs {
            for file in self.results.iter().skip(self.landed) {
                outputs.landed(&*self.fs, file.path())?;
            }
        }
        Ok(self.results)
    }
}
