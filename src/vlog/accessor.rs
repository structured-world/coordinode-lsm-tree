// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{
    Cache, GlobalTableId, TreeId, UserValue,
    version::BlobFileList,
    vlog::{ValueHandle, blob_file::reader::Reader},
};
#[cfg(not(feature = "std"))]
use alloc::string::ToString;

pub struct Accessor<'a> {
    blob_files: &'a BlobFileList,
    #[cfg(zstd_any)]
    zstd_dictionary: Option<&'a crate::compression::ZstdDictionary>,
}

impl<'a> Accessor<'a> {
    pub fn new(blob_files: &'a BlobFileList) -> Self {
        Self {
            blob_files,
            #[cfg(zstd_any)]
            zstd_dictionary: None,
        }
    }

    /// Supplies the zstd dictionary for [`CompressionType::ZstdDict`](crate::CompressionType::ZstdDict) blob reads.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_dict(mut self, dict: Option<&'a crate::compression::ZstdDictionary>) -> Self {
        self.zstd_dictionary = dict;
        self
    }

    /// Reads one separated value.
    ///
    /// The blob file is reopened from the path it was RECOVERED under, so no
    /// caller supplies a base directory: a file can legitimately sit under a
    /// noncanonical spelling of its own id (`blobs/00` for id 0), and a path
    /// rebuilt from the id would miss it on every cache miss.
    ///
    /// # Errors
    ///
    /// Propagates the blob file's open / read failures.
    pub fn get(
        &self,
        tree_id: TreeId,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(tree_id, vhandle) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.blob_files.get(vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let (file, _) = blob_file
            .file_accessor()
            .get_or_open_blob_file(&bf_id, &blob_file.0.path)?;

        let reader = {
            let r = Reader::new(blob_file, file.as_ref());
            #[cfg(zstd_any)]
            let r = r.with_dict(self.zstd_dictionary);
            r
        };

        let value = reader.get(key, vhandle)?;
        cache.insert_blob(tree_id, vhandle, value.clone());

        Ok(Some(value))
    }
}
