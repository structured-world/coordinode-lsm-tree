// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    Cache, GlobalTableId, TreeId, UserValue,
    version::BlobFileList,
    vlog::{ValueHandle, blob_file::reader::Reader},
};
use std::path::Path;

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

    /// Supplies the zstd dictionary for [`CompressionType::ZstdDict`] blob reads.
    #[cfg(zstd_any)]
    #[must_use]
    pub fn with_dict(mut self, dict: Option<&'a crate::compression::ZstdDictionary>) -> Self {
        self.zstd_dictionary = dict;
        self
    }

    pub fn get(
        &self,
        tree_id: TreeId,
        base_path: &Path,
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
            .get_or_open_blob_file(&bf_id, &base_path.join(vhandle.blob_file_id.to_string()))?;

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
