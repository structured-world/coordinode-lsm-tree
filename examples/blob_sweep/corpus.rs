//! The corpora a sweep runs on, as `fetch-data.ts` lays them out.

use std::path::{Path, PathBuf};

/// One value of a corpus: read from a line of a JSON lines file, or a file of
/// its own.
pub(crate) enum Source {
    Line { file: PathBuf, at: u64, len: u32 },
    File(PathBuf),
}

/// A corpus: its name and its values, in key order.
pub(crate) struct Corpus {
    pub(crate) name: String,
    pub(crate) values: Vec<(Vec<u8>, Source)>,
}

impl Corpus {
    /// The bytes of the values together.
    ///
    /// # Errors
    ///
    /// A file of the corpus cannot be inspected.
    pub(crate) fn bytes(&self) -> std::io::Result<u64> {
        self.values
            .iter()
            .map(|(_, source)| match source {
                Source::Line { len, .. } => Ok(u64::from(*len)),
                Source::File(path) => std::fs::metadata(path).map(|m| m.len()),
            })
            .sum()
    }
}

/// Reads the value `source` names.
///
/// # Errors
///
/// The file cannot be read.
pub(crate) fn read(source: &Source) -> std::io::Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    match source {
        Source::Line { file, at, len } => {
            let mut f = std::fs::File::open(file)?;
            f.seek(SeekFrom::Start(*at))?;
            let mut value = vec![0; *len as usize];
            f.read_exact(&mut value)?;
            Ok(value)
        }
        Source::File(path) => std::fs::read(path),
    }
}

/// The lines of the JSON lines files `files` of `dir`, each a value under a
/// key naming its file and line.
fn lines(name: &str, dir: &Path, files: &[&str]) -> std::io::Result<Corpus> {
    use std::io::BufRead;
    let mut values = Vec::new();
    for file in files {
        let path = dir.join(file);
        let reader = std::io::BufReader::new(std::fs::File::open(&path)?);
        let mut at = 0_u64;
        for (n, line) in reader.split(b'\n').enumerate() {
            let line = line?;
            let len = u32::try_from(line.len()).map_err(std::io::Error::other)?;
            if len > 0 {
                values.push((
                    format!("{file}/{n:09}").into_bytes(),
                    Source::Line {
                        file: path.clone(),
                        at,
                        len,
                    },
                ));
            }
            at += u64::from(len) + 1;
        }
    }
    values.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Corpus {
        name: name.to_owned(),
        values,
    })
}

/// Every file under `dir`, each a value under its path below `dir`.
fn tree_of_files(name: &str, dir: &Path) -> std::io::Result<Corpus> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(Vec<u8>, Source)>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                walk(root, &path, out)?;
            } else {
                let key = path
                    .strip_prefix(root)
                    .map_err(std::io::Error::other)?
                    .to_string_lossy()
                    .into_owned()
                    .into_bytes();
                out.push((key, Source::File(path)));
            }
        }
        Ok(())
    }
    let mut values = Vec::new();
    walk(dir, dir, &mut values)?;
    values.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(Corpus {
        name: name.to_owned(),
        values,
    })
}

/// The corpora under `data`: `documents`, `chain` and `files`, those present.
///
/// # Errors
///
/// A corpus that is present cannot be read; only a corpus that is not there
/// is left out.
pub(crate) fn load(data: &Path) -> std::io::Result<Vec<Corpus>> {
    let mut corpora = Vec::new();
    let documents = data.join("documents");
    if documents.join("records.jsonl").try_exists()? {
        corpora.push(lines("documents", &documents, &["records.jsonl"])?);
    }
    let chain = data.join("chain");
    if chain.join("blocks.jsonl").try_exists()? {
        corpora.push(lines(
            "chain",
            &chain,
            &["blocks.jsonl", "transactions.jsonl"],
        )?);
    }
    // The Silesia tar is the part's mark of completion, written last: a fetch
    // that stopped before it leaves a partial tree, which is not a corpus.
    let files = data.join("files");
    if files.join("silesia.tar").try_exists()? {
        corpora.push(tree_of_files("files", &files)?);
    }
    Ok(corpora)
}
