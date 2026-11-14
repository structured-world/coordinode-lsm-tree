// A run of each kind over a small corpus laid out as `fetch-data.ts` lays the
// real ones out: what it measures is there, and the values it wrote read back.

use super::corpus;
use super::run::{Codec, Layout, measure, train};
use super::{Error, HeapProbe};

/// A probe that measures nothing.
struct NoProbe;

impl HeapProbe for NoProbe {
    fn reset(&self) {}

    fn peak(&self) -> Option<u64> {
        None
    }
}

/// A data directory with a JSON lines corpus of documents and a corpus of
/// files, one of them 3 MiB so range reads run, and the tar that marks the
/// files complete.
fn data() -> std::io::Result<tempfile::TempDir> {
    let dir = tempfile::tempdir()?;
    let documents = dir.path().join("documents");
    std::fs::create_dir_all(&documents)?;
    let lines: String = (0..400)
        .map(|i| format!("{{\"id\":{i},\"type\":\"PushEvent\",\"repo\":\"org/repo-{}\",\"payload\":\"{}\"}}\n", i % 7, "x".repeat(i % 300)))
        .collect();
    std::fs::write(documents.join("records.jsonl"), lines)?;
    let files = dir.path().join("files");
    std::fs::create_dir_all(&files)?;
    std::fs::write(files.join("small.txt"), b"a small file\n".repeat(100))?;
    let large: Vec<u8> = (0..3_u32 << 20).map(|i| (i % 251) as u8).collect();
    std::fs::write(files.join("large.bin"), large)?;
    std::fs::write(files.join("silesia.tar"), b"a tar\n".repeat(10))?;
    Ok(dir)
}

/// A files part whose fetch stopped before its tar is not loaded: a partial
/// tree would be swept as if it were the corpus.
#[test]
fn a_files_part_without_its_tar_is_left_out() -> Result<(), Error> {
    let data = data()?;
    std::fs::remove_file(data.path().join("files").join("silesia.tar"))?;
    let corpora = corpus::load(data.path())?;
    assert_eq!(
        corpora.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["documents"]
    );
    Ok(())
}

#[test]
fn every_kind_of_run_measures_what_it_wrote() -> Result<(), Error> {
    let data = data()?;
    let work = tempfile::tempdir()?;
    let corpora = corpus::load(data.path())?;
    assert_eq!(
        corpora.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        ["documents", "files"]
    );
    for corpus in &corpora {
        let dictionary = Codec::ZstdDict(3, train(corpus, 1 << 20, 4 << 10)?);
        for (layout, codec) in [
            (Layout::per_value(), Codec::Lz4),
            (Layout::sized(16 << 10, 256 << 10), Codec::Zstd(3)),
            (Layout::sized(4 << 10, 64 << 10), dictionary),
        ] {
            let measured = measure(corpus, &layout, &codec, work.path(), &NoProbe)?;
            assert_eq!(measured.value_bytes, corpus.bytes()?, "{}", corpus.name);
            assert!(measured.blob_bytes > 0);
            assert_eq!(measured.gets, corpus.values.len() as u64);
            if corpus.name == "files" {
                assert_eq!(measured.range_reads, 16, "the large file is read by range");
                assert_eq!(measured.large_value, 3 << 20);
            }
            let json = measured.to_json(&corpus.name, &layout, &codec);
            assert!(json["ratio"].as_f64().is_some_and(|r| r > 0.0));
        }
    }
    Ok(())
}
