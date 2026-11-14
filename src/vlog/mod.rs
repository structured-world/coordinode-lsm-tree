// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

pub(crate) mod accessor;
pub(crate) mod blob_file;
mod handle;

pub use blob_file::BlobFile;

pub(crate) use {
    accessor::{Accessor, PrefetchInto, Prefetched},
    blob_file::merge::MergeScanner as BlobFileMergeScanner,
    blob_file::multi_writer::MultiWriter as BlobFileWriter,
    blob_file::scanner::Scanner as BlobFileScanner,
    handle::ValueHandle,
};

use crate::path::{Path, PathBuf};
use crate::{
    Checksum, DescriptorTable, TreeId,
    file_accessor::FileAccessor,
    fs::Fs,
    vlog::blob_file::{Inner as BlobFileInner, Metadata},
};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;
use core::sync::atomic::AtomicBool;

/// Keeps ONE recovered copy per blob id, moving the rest to `orphaned`.
///
/// The manifest names exactly one file per id and records the checksum of its
/// live suffix, so a copy that reproduces that digest IS the one it named. When
/// none does — both copies rotted, or the digest cannot be read — the canonical
/// spelling (`{id}`) is kept over any alternate one, and first-seen order
/// breaks a remaining tie. That is a deterministic answer rather than a correct
/// one, but it beats letting directory order decide, and the surviving copy's
/// damage still surfaces on the read that meets it.
///
/// A copy whose metadata or stored dictionary did not parse (`damaged`) is
/// never the one the manifest digested, but it still counts as a duplicate:
/// the readable copy of its id must reproduce the digest to be kept.
///
/// # Errors
///
/// A damaged copy's own error when its id has no readable copy.
///
/// Propagates an ENVIRONMENTAL failure while reading a copy's digest. The rank
/// below would otherwise read "could not be determined" as "does not match" and
/// fall through to the filename tiebreak — and if the unreadable copy is the
/// authoritative one under an alternate spelling, the stale canonical file wins
/// and the healthy one is deleted as an orphan. A retryable fault must never
/// decide that.
fn dedupe_blob_sightings(
    fs: &Arc<dyn Fs>,
    blob_files: &mut Vec<BlobFile>,
    damaged: Vec<(BlobFileId, PathBuf, crate::Error)>,
    orphaned: &mut Vec<PathBuf>,
) -> crate::Result<()> {
    let mut seen: crate::HashMap<BlobFileId, usize> = crate::HashMap::default();
    let mut duplicate_ids: crate::HashSet<BlobFileId> = crate::HashSet::default();
    for (idx, bf) in blob_files.iter().enumerate() {
        if seen.insert(bf.id(), idx).is_some() {
            duplicate_ids.insert(bf.id());
        }
    }
    for (id, path, e) in damaged {
        if !seen.contains_key(&id) {
            return Err(e);
        }
        log::warn!(
            "blob file {} of id {id} does not parse ({e:?}); the id's other copy decides, \
             and this one is an orphan",
            path.display(),
        );
        duplicate_ids.insert(id);
        orphaned.push(path);
    }
    if duplicate_ids.is_empty() {
        return Ok(());
    }

    // Score each sighting: a matching digest wins outright, then the canonical
    // spelling, then first-seen. `checksum_from_with_overrides` re-reads the
    // file, so this runs ONLY on the rare id that was seen twice.
    let rank = |bf: &BlobFile| -> crate::Result<u8> {
        let digest = match crate::file::checksum_from_with_overrides(
            &**fs,
            &bf.0.path,
            bf.0.live_data_start,
            &[],
        ) {
            Ok(d) => Some(d),
            // A fault in the ENVIRONMENT says nothing about which copy the
            // manifest named, and guessing costs the loser its file.
            Err(e) if e.is_environmental() => return Err(e),
            // A read that fails on the BYTES is evidence: this copy cannot be
            // the intact one the manifest digested.
            Err(_) => None,
        };
        if digest.is_some_and(|d| Checksum::from_raw(d) == bf.0.checksum) {
            return Ok(0);
        }
        // Rebuilt rather than string-compared: `file_name` is `&OsStr` with
        // `std` and `&str` without it, and the join answers both.
        let canonical = bf.0.path.parent().is_some_and(|dir| {
            dir.join(alloc::string::ToString::to_string(&bf.id())) == *bf.0.path
        });
        Ok(if canonical { 1 } else { 2 })
    };

    let mut best: crate::HashMap<BlobFileId, (u8, usize)> = crate::HashMap::default();
    for (idx, bf) in blob_files.iter().enumerate() {
        let score = rank(bf)?;
        match best.get(&bf.id()) {
            Some(&(prev, _)) if prev <= score => {}
            _ => {
                best.insert(bf.id(), (score, idx));
            }
        }
    }
    // The manifest digested exactly one file per id. When an id has duplicates
    // and NONE of them reproduces that digest, the filename is not a tiebreak,
    // it is a guess between two wrong answers, and acting on it opens the tree
    // on the wrong generation while deleting the other copy as an orphan. The
    // damaged authoritative file may still be salvageable frame by frame, which
    // a repair can do and this cannot. Refuse instead.
    if let Some(id) = best
        .iter()
        .find(|(id, (score, _))| *score > 0 && duplicate_ids.contains(*id))
        .map(|(id, _)| *id)
    {
        log::error!(
            "blob file {id} has multiple copies and none reproduces the manifest's \
             checksum; refusing to pick one. Run a repair, which can salvage the \
             authoritative copy's intact frames",
        );
        return Err(crate::Error::Unrecoverable);
    }
    let winners: crate::HashSet<usize> = best.values().map(|&(_, idx)| idx).collect();
    let mut idx = 0;
    blob_files.retain(|bf| {
        let keep = winners.contains(&idx);
        if !keep {
            log::warn!(
                "blob file {} duplicates id {}; the manifest names another copy, so this one \
                 is an orphan",
                bf.0.path.display(),
                bf.id(),
            );
            orphaned.push(bf.0.path.clone());
        }
        idx += 1;
        keep
    });
    Ok(())
}

pub(crate) fn recover_blob_files(
    folder: &Path,
    ids: &[(BlobFileId, Checksum, u64)],
    tree_id: TreeId,
    descriptor_table: Option<&Arc<DescriptorTable>>,
    fs: &Arc<dyn Fs>,
    #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
) -> crate::Result<(Vec<BlobFile>, Vec<PathBuf>)> {
    // Recover directly from read_dir; treat NotFound as empty only for
    // standard (non-blob) trees where no blob folder is expected.
    // If the manifest references blob files (ids non-empty) but the folder
    // is missing, that is unrecoverable corruption — fail fast.
    let entries = match fs.read_dir(folder) {
        Ok(entries) => entries,
        Err(e) if e.kind() == crate::io::ErrorKind::NotFound => {
            if ids.is_empty() {
                return Ok((vec![], vec![]));
            }
            return Err(crate::Error::Unrecoverable);
        }
        Err(e) => return Err(e.into()),
    };

    let cnt = ids.len();

    let progress_mod = match cnt {
        _ if cnt <= 20 => 1,
        _ if cnt <= 100 => 10,
        _ => 100,
    };

    log::debug!("Recovering {cnt} blob files from {:?}", folder.display());

    let mut blob_files = Vec::with_capacity(ids.len());
    let mut orphaned_blob_files = vec![];
    let mut damaged_sightings = vec![];

    for (idx, dirent) in entries.into_iter().enumerate() {
        let file_name = &dirent.file_name;

        if dirent.is_dir {
            continue;
        }

        // The naming grammar decides, not a list of names to tolerate: a shape
        // the engine does not name is not engine state, so it is passed over
        // untouched rather than read, swept, or made a reason to refuse the
        // store.
        let blob_file_id = match crate::file::BlobDirEntry::classify(file_name) {
            crate::file::BlobDirEntry::Blob(id) => id,
            // Published by an atomic rename, so a survivor is from a crashed
            // repair and is referenced by no manifest.
            crate::file::BlobDirEntry::SalvageTmp(_) => {
                orphaned_blob_files.push(dirent.path.clone());
                continue;
            }
            crate::file::BlobDirEntry::Foreign => {
                log::debug!("Ignoring {file_name:?} in the blobs folder: not an engine file");
                continue;
            }
        };

        let blob_file_path = &dirent.path;

        if let Some(&(_, checksum, live_data_start)) =
            ids.iter().find(|(id, _, _)| id == &blob_file_id)
        {
            log::trace!(
                "Recovering blob file #{blob_file_id:?} from {}",
                blob_file_path.display(),
            );

            let mut file = fs.open(blob_file_path, &crate::fs::FsOpenOptions::new().read(true))?;

            let parsed = read_meta_and_frames(&mut file, blob_file_id);
            #[cfg(zstd_any)]
            let parsed = parsed.and_then(|(meta, frames)| {
                read_file_dictionary(&mut file, &meta).map(|dict| (meta, frames, dict))
            });
            #[cfg(not(zstd_any))]
            let parsed = parsed.map(|(meta, frames)| (meta, frames, ()));
            // Damaged bytes are not the copy the manifest digested, but another
            // directory entry of the same id may be: deduplication below decides,
            // and only an id left with no copy at all fails on this error.
            #[cfg_attr(
                not(zstd_any),
                expect(unused_variables, reason = "no dictionary without zstd")
            )]
            let (meta, frames, own_dictionary) = match parsed {
                Ok(parsed) => parsed,
                Err(e) if e.is_environmental() => return Err(e),
                Err(e) => {
                    damaged_sightings.push((blob_file_id, blob_file_path.clone(), e));
                    continue;
                }
            };

            // With a descriptor cache the file is closed once its metadata is
            // read, and reopened through the cache by its first read: holding
            // every blob file's descriptor until all are recovered would need
            // one per file, however few the cache keeps. The first read also
            // opens the path of the copy deduplication below keeps, never a
            // discarded sighting of the same id.
            let file_accessor = if let Some(dt) = descriptor_table.cloned() {
                drop(file);
                FileAccessor::DescriptorTable {
                    table: dt,
                    fs: fs.clone(),
                }
            } else {
                FileAccessor::File(Arc::from(file))
            };

            blob_files.push(BlobFile(Arc::new(BlobFileInner {
                id: blob_file_id,
                path: blob_file_path.clone(),
                // Fail-fast on an id the tree does not hold, exactly as a
                // table's recovery does: the file cannot decode a single value
                // that way, so the open names the id while an operator can
                // still restore it, rather than turning every value in the
                // file into a failed read far from the cause.
                #[cfg(zstd_any)]
                dictionary_in_file: own_dictionary.is_some(),
                #[cfg(zstd_any)]
                zstd_dictionary: match own_dictionary {
                    Some(dict) => Some(dict),
                    None => zstd_dictionaries.for_compression(meta.compression)?,
                },
                meta,
                is_deleted: AtomicBool::new(false),
                punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
                punched_through: once_cell::race::OnceBox::new(),
                checksum,
                live_data_start,
                frames,
                file_accessor,
                tree_id,
                fs: fs.clone(),
                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),
            })));

            if idx % progress_mod == 0 {
                log::debug!("Recovered {idx}/{cnt} blob files");
            }
        } else {
            orphaned_blob_files.push(blob_file_path.clone());
        }
    }

    // Two directory entries can parse to the SAME id — `blobs/0` beside a
    // noncanonical `blobs/00`, which is what a repair leaves when its
    // post-commit removal of the displaced copy fails. Both matched the
    // manifest id above, and the version this feeds builds a map keyed by id,
    // so without resolving them here whichever came last would silently win:
    // a copy with readable metadata but a rotted value frame could replace the
    // authoritative blob and make reads fail. The manifest's checksum covers
    // the live suffix of the copy it named, so it decides; the losers are
    // orphans, which the caller sweeps.
    dedupe_blob_sightings(
        fs,
        &mut blob_files,
        damaged_sightings,
        &mut orphaned_blob_files,
    )?;

    if blob_files.len() < ids.len() {
        return Err(crate::Error::Unrecoverable);
    }

    log::debug!("Successfully recovered {} blob files", blob_files.len());

    Ok((blob_files, orphaned_blob_files))
}

/// Recovers a SINGLE blob file from its path, for the manifest-rebuild
/// (`Config::repair`) path where there is no manifest id list to filter against.
///
/// Reads the file's `meta` SFA section and constructs a [`BlobFile`] bound to
/// the caller-computed `checksum`. Unlike [`recover_blob_files`] (which filters
/// a known id list and fails fast on any miss), the caller discovers ids from a
/// directory scan and handles per-file errors itself: a corrupt blob is reported
/// and left in place (it reads back as a harmless orphan on the next open)
/// rather than aborting the whole repair.
///
/// `tree_id` is `0` and there is no descriptor table, mirroring the table
/// recovery in `repair`: the repaired tree is reopened fresh afterwards, so a
/// transient handle must not pollute any shared cache.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or its `meta` section is
/// missing / undecodable.
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "single-file blob recovery for the std-gated repair surface; the no_std open path uses recover_blob_files"
    )
)]
pub(crate) fn recover_blob_file(
    path: &Path,
    id: BlobFileId,
    checksum: Checksum,
    tree_id: TreeId,
    fs: &Arc<dyn Fs>,
    #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
) -> crate::Result<BlobFile> {
    recover_blob_file_from(
        path,
        id,
        checksum,
        tree_id,
        fs,
        0,
        #[cfg(zstd_any)]
        zstd_dictionaries,
    )
}

/// As [`recover_blob_file`], but for a view whose consumed prefix below
/// `live_data_start` was reclaimed by a tight-space relocation: `checksum`
/// covers only `[live_data_start, end)`, and integrity checks hash from there
/// rather than over the punched (zeroed) prefix. `live_data_start = 0` is a
/// whole, unreclaimed file.
///
/// # Errors
///
/// Propagates any error from opening or parsing the blob file.
pub(crate) fn recover_blob_file_from(
    path: &Path,
    id: BlobFileId,
    checksum: Checksum,
    tree_id: TreeId,
    fs: &Arc<dyn Fs>,
    live_data_start: u64,
    #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
) -> crate::Result<BlobFile> {
    recover_blob_file_cached(
        path,
        id,
        checksum,
        tree_id,
        fs,
        live_data_start,
        #[cfg(zstd_any)]
        zstd_dictionaries,
        None,
    )
}

/// As [`recover_blob_file_from`], with the file's descriptor kept in
/// `descriptors` under `tree_id` instead of by the handle, when given: a
/// caller holding many blob files at once then needs no descriptor per file.
///
/// # Errors
///
/// Propagates any error from opening or parsing the blob file.
#[cfg_attr(
    not(feature = "std"),
    allow(
        dead_code,
        reason = "cached single-file blob recovery for the std-gated repair surface"
    )
)]
#[cfg_attr(
    zstd_any,
    expect(
        clippy::too_many_arguments,
        reason = "the single-file recovery's inputs plus where its descriptor is kept"
    )
)]
pub(crate) fn recover_blob_file_cached(
    path: &Path,
    id: BlobFileId,
    checksum: Checksum,
    tree_id: TreeId,
    fs: &Arc<dyn Fs>,
    live_data_start: u64,
    #[cfg(zstd_any)] zstd_dictionaries: &crate::compression::ZstdDictionaries,
    descriptors: Option<&Arc<DescriptorTable>>,
) -> crate::Result<BlobFile> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;

    // Same read as `recover_blob_files`' per-id branch above.
    let (meta, frames) = read_meta_and_frames(&mut file, id)?;
    #[cfg(zstd_any)]
    let own_dictionary = read_file_dictionary(&mut file, &meta)?;

    let file: Arc<dyn crate::fs::FsFile> = Arc::from(file);
    let file_accessor = match descriptors {
        Some(table) => {
            table.insert_for_blob_file((tree_id, id).into(), file);
            FileAccessor::DescriptorTable {
                table: Arc::clone(table),
                fs: fs.clone(),
            }
        }
        None => FileAccessor::File(file),
    };
    Ok(BlobFile(Arc::new(BlobFileInner {
        id,
        path: path.to_path_buf(),
        // LENIENT, unlike the plural recovery that serves `Tree::open`. This
        // opens ONE named file for a caller inspecting or repairing it:
        // `is_blob_file` only asks whether the bytes parse at all, and a
        // salvage reads the descriptor to decide what to do about the file and
        // reports a missing dictionary itself, naming the one the caller DID
        // supply. Refusing here would collapse both answers into "not a blob
        // file" and throw that context away.
        #[cfg(zstd_any)]
        dictionary_in_file: own_dictionary.is_some(),
        #[cfg(zstd_any)]
        zstd_dictionary: match own_dictionary {
            Some(dict) => Some(dict),
            None => zstd_dictionaries
                .for_compression(meta.compression)
                .ok()
                .flatten(),
        },
        meta,
        is_deleted: AtomicBool::new(false),
        punch_on_drop: portable_atomic::AtomicU64::new(u64::MAX),
        punched_through: once_cell::race::OnceBox::new(),
        checksum,
        live_data_start,
        frames,
        file_accessor,
        tree_id,
        fs: fs.clone(),
        deletion_pause: once_cell::race::OnceBox::new(),

        #[cfg(feature = "std")]
        background_deleter: once_cell::race::OnceBox::new(),
    })))
}

/// Reads a blob file's metadata, then its frame index, from its archive.
///
/// The metadata is read first: it carries the layout version, so a file of an
/// earlier layout, which has no frame index, is refused as one rather than as
/// a file missing a section.
///
/// # Errors
///
/// [`crate::Error::UnsupportedFormat`] for a file of another layout,
/// [`crate::Error::Unrecoverable`] for a missing section, and any error
/// reading or decoding them.
pub(crate) fn read_meta_and_frames(
    file: &mut alloc::boxed::Box<dyn crate::fs::FsFile>,
    id: BlobFileId,
) -> crate::Result<(Metadata, blob_file::writer::FrameIndex)> {
    let (meta, frames_at, data) = read_meta_and_index_at(file, id)?;
    let (Some((pos, len)), Some(data)) = (frames_at, data) else {
        log::error!("frames or data section in blob file #{id} is missing (file may be corrupted)");
        return Err(crate::Error::Unrecoverable);
    };
    // The index lies within the file: a length past it is damage, refused
    // before a buffer is sized from it.
    let file_len = file.metadata()?.len;
    if pos.checked_add(len).is_none_or(|end| end > file_len) {
        log::error!("frames section in blob file #{id} runs past the file (file may be corrupted)");
        return Err(crate::Error::Unrecoverable);
    }
    let len = usize::try_from(len).map_err(|_| crate::Error::Unrecoverable)?;
    let frames =
        blob_file::writer::decode_frame_index(&crate::file::read_exact(&**file, pos, len)?)?;
    // Every frame a read is sized from lies in the data section: the index
    // rises from within it to its end, as the writer lays it down.
    let fits = frames.first().is_some_and(|&first| first >= data.start)
        && frames.last() == Some(&data.end);
    if !fits {
        log::error!(
            "frame index of blob file #{id} does not span its data (file may be corrupted)"
        );
        return Err(crate::Error::Unrecoverable);
    }
    Ok((meta, frames))
}

/// The dictionary a blob file stores of its own, when it does: the one the
/// codec its metadata records names, since a dictionary's id is its
/// content's.
///
/// # Errors
///
/// [`crate::Error::Unrecoverable`] for a stored dictionary another than the
/// one the metadata names, and any error reading it.
#[cfg(zstd_any)]
pub(crate) fn read_file_dictionary(
    file: &mut alloc::boxed::Box<dyn crate::fs::FsFile>,
    meta: &Metadata,
) -> crate::Result<Option<Arc<crate::compression::ZstdDictionary>>> {
    let at = {
        let reader = crate::sfa::Reader::from_reader(file)?;
        reader
            .toc()
            .section(b"dict")
            .map(|entry| (entry.pos(), entry.len()))
    };
    let Some((pos, len)) = at else {
        return Ok(None);
    };
    // The dictionary lies within the file: a length past it is damage, refused
    // before a buffer is sized from it.
    let file_len = file.metadata()?.len;
    if pos.checked_add(len).is_none_or(|end| end > file_len) {
        log::error!(
            "dict section in blob file #{} runs past the file (file may be corrupted)",
            meta.id
        );
        return Err(crate::Error::Unrecoverable);
    }
    let len = usize::try_from(len).map_err(|_| crate::Error::Unrecoverable)?;
    let dict =
        crate::compression::ZstdDictionary::new(&crate::file::read_exact(&**file, pos, len)?);
    match meta.compression {
        crate::CompressionType::ZstdDict { dict_id, .. } if dict_id == dict.id() => {
            Ok(Some(Arc::new(dict)))
        }
        _ => {
            log::error!(
                "blob file #{} stores a dictionary its codec does not name",
                meta.id
            );
            Err(crate::Error::Unrecoverable)
        }
    }
}

/// The metadata of the blob file at `path`, without its frame index: what a
/// salvage needs of a file whose index may be what is damaged.
///
/// # Errors
///
/// As [`read_meta_and_frames`], short of the frame index.
#[cfg(feature = "std")]
pub(crate) fn read_blob_meta(path: &Path, fs: &dyn Fs, id: BlobFileId) -> crate::Result<Metadata> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    read_meta_and_index_at(&mut file, id).map(|(meta, ..)| meta)
}

/// [`read_file_dictionary`] of the blob file at `path`, whose metadata is
/// `meta`.
///
/// # Errors
///
/// As [`read_file_dictionary`], and any error opening the file.
#[cfg(all(feature = "std", zstd_any))]
pub(crate) fn read_blob_file_dictionary(
    path: &Path,
    fs: &dyn Fs,
    meta: &Metadata,
) -> crate::Result<Option<Arc<crate::compression::ZstdDictionary>>> {
    let mut file = fs.open(path, &crate::fs::FsOpenOptions::new().read(true))?;
    read_file_dictionary(&mut file, meta)
}

/// Where a section lies: its position and length.
type SectionAt = (u64, u64);

/// The metadata, where the frame index lies, and the span of the data
/// section, when the file lists them: a missing section is reported by the
/// caller that needs it, after the metadata has said whether the file is of
/// this layout at all.
fn read_meta_and_index_at(
    file: &mut alloc::boxed::Box<dyn crate::fs::FsFile>,
    id: BlobFileId,
) -> crate::Result<(Metadata, Option<SectionAt>, Option<core::ops::Range<u64>>)> {
    let (meta_at, frames_at, data) = {
        let reader = crate::sfa::Reader::from_reader(file)?;
        let toc = reader.toc();
        let section = |name: &[u8]| toc.section(name).map(|entry| (entry.pos(), entry.len()));
        let Some(meta_at) = section(b"meta") else {
            log::error!("meta section in blob file #{id} is missing (file may be corrupted)");
            return Err(crate::Error::Unrecoverable);
        };
        let data = section(b"data").and_then(|(pos, len)| pos.checked_add(len).map(|end| pos..end));
        (meta_at, section(b"frames"), data)
    };
    let (pos, len) = meta_at;
    let len = usize::try_from(len).map_err(|_| crate::Error::Unrecoverable)?;
    let meta = Metadata::from_slice(&crate::file::read_exact(&**file, pos, len)?)?;
    Ok((meta, frames_at, data))
}

/// The unique identifier for a value log blob file.
pub(crate) type BlobFileId = u64;

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test code")]
mod tests;
