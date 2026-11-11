// Builds the data the blob sweep runs on, from public sources, into a
// directory outside the repository:
//
//   documents/records.jsonl  GitHub events of one archive hour, one JSON
//                            document of a few KiB per line
//   chain/blocks.jsonl       TRON blocks, one per line
//   chain/transactions.jsonl the transactions of those blocks, one per line
//   files/                   the Canterbury corpus (KiB to 1 MiB), the Silesia
//                            corpus (6 to 51 MiB), the Silesia tar (about 200
//                            MiB) and the Silesia zip, an archive no codec
//                            shrinks
//
// Every source is fetched once; a directory that already holds a part keeps
// it. `manifest.json` records what each part was built from and its size.
//
//   node examples/blob_sweep/fetch-data.ts [DIR]
//
// DIR defaults to $BLOB_SWEEP_DATA, else ~/.cache/lsm-tree-blob-sweep. TronGrid
// serves keyless clients slowly; $TRONGRID_API_KEY is sent when set.

import { execFileSync } from "node:child_process";
import {
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createGunzip } from "node:zlib";

const MIB = 1024 * 1024;

/// The archive hour the documents come from: fixed, so every run sees the same
/// documents.
const GHARCHIVE_HOUR = "https://data.gharchive.org/2024-01-15-12.json.gz";
const DOCUMENTS_BYTES = 64 * MIB;

/// The first block height of the chain sample: fixed for the same reason.
const TRON_START = 60_000_000;
const TRON_BATCH = 100;
const CHAIN_BYTES = 128 * MIB;

const CANTERBURY = "https://corpus.canterbury.ac.nz/resources/cantrbry.tar.gz";
const SILESIA = "https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip";

interface Part {
  source: string;
  bytes: number;
  records?: number;
}

interface TronBlock {
  transactions?: unknown[];
}

interface TronBatch {
  block?: TronBlock[];
}

const dir = process.argv[2] ?? process.env.BLOB_SWEEP_DATA ?? join(homedir(), ".cache", "lsm-tree-blob-sweep");
mkdirSync(dir, { recursive: true });

const manifestPath = join(dir, "manifest.json");
/// What earlier runs built, kept for the parts this run does not build again.
const manifest: Record<string, Part> = existsSync(manifestPath)
  ? (JSON.parse(readFileSync(manifestPath, "utf8")) as Record<string, Part>)
  : {};

/// Bytes of every file under `path`.
function sizeOf(path: string): number {
  const stat = statSync(path);
  if (!stat.isDirectory()) {
    return stat.size;
  }
  return readdirSync(path).reduce((sum, name) => sum + sizeOf(join(path, name)), 0);
}

async function fetchOk(url: string, init?: RequestInit): Promise<Response> {
  for (let attempt = 1; ; attempt++) {
    const response = await fetch(url, init);
    if (response.ok) {
      return response;
    }
    if (attempt === 5 || (response.status < 500 && response.status !== 429)) {
      throw new Error(`${url}: HTTP ${response.status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, attempt * 2_000));
  }
}

/// Writes the lines of `url`, a gzip of JSON lines, into `out` until `limit`
/// bytes are written; returns the line count.
async function documents(out: string, limit: number): Promise<number> {
  const response = await fetchOk(GHARCHIVE_HOUR);
  if (response.body === null) {
    throw new Error(`${GHARCHIVE_HOUR}: no body`);
  }
  const gunzip = createGunzip();
  const sink = createWriteStream(out);
  let written = 0;
  let lines = 0;
  let pending = "";
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const done = new Promise<void>((resolve, reject) => {
    gunzip.on("data", (chunk: Buffer) => {
      if (written >= limit) {
        return;
      }
      pending += decoder.decode(chunk, { stream: true });
      const parts = pending.split("\n");
      pending = parts.pop() ?? "";
      for (const line of parts) {
        if (written >= limit) {
          break;
        }
        sink.write(`${line}\n`);
        written += Buffer.byteLength(line) + 1;
        lines += 1;
      }
    });
    gunzip.on("end", resolve);
    gunzip.on("error", reject);
  });
  for (;;) {
    const { done: end, value } = await reader.read();
    if (end || written >= limit) {
      break;
    }
    gunzip.write(value);
  }
  await reader.cancel().catch(() => undefined);
  gunzip.end();
  // A stream cut short on purpose, at the limit, may fail its gzip check;
  // one that fails before it is a broken download, never a finished corpus.
  let failure: unknown = undefined;
  await done.catch((error: unknown) => {
    if (written < limit) {
      failure = error;
    }
  });
  await new Promise<void>((resolve) => sink.end(resolve));
  if (failure !== undefined) {
    throw failure;
  }
  return lines;
}

/// Writes TRON blocks from `TRON_START` on into `blocks` and their
/// transactions into `transactions`, until both hold `limit` bytes together.
async function chain(blocks: string, transactions: string, limit: number): Promise<number> {
  const blockSink = createWriteStream(blocks);
  const txSink = createWriteStream(transactions);
  const headers: Record<string, string> = { "content-type": "application/json" };
  const key = process.env.TRONGRID_API_KEY;
  // An empty key is no key: sent without it, and paced as a keyless client.
  const keyless = key === undefined || key === "";
  if (!keyless) {
    headers["TRON-PRO-API-KEY"] = key;
  }
  let written = 0;
  let count = 0;
  for (let start = TRON_START; written < limit; start += TRON_BATCH) {
    const response = await fetchOk("https://api.trongrid.io/wallet/getblockbylimitnext", {
      method: "POST",
      headers,
      body: JSON.stringify({ startNum: start, endNum: start + TRON_BATCH }),
    });
    const batch = (await response.json()) as TronBatch;
    // A batch past the chain's head holds no block: asking on would never
    // reach the limit.
    if ((batch.block ?? []).length === 0) {
      throw new Error(`TronGrid returned no block from height ${start}`);
    }
    for (const block of batch.block ?? []) {
      const line = `${JSON.stringify(block)}\n`;
      blockSink.write(line);
      written += Buffer.byteLength(line);
      count += 1;
      for (const tx of block.transactions ?? []) {
        const txLine = `${JSON.stringify(tx)}\n`;
        txSink.write(txLine);
        written += Buffer.byteLength(txLine);
      }
    }
    // Keyless clients are held to a few requests a second.
    await new Promise((resolve) => setTimeout(resolve, keyless ? 1_000 : 100));
  }
  await new Promise<void>((resolve) => blockSink.end(resolve));
  await new Promise<void>((resolve) => txSink.end(resolve));
  return count;
}

/// Where a part is written before it is complete: a run that stops part way
/// leaves no file under the part's own name, so the next run builds it again.
function partial(path: string): string {
  return `${path}.part`;
}

async function download(url: string, out: string): Promise<void> {
  const response = await fetchOk(url);
  writeFileSync(partial(out), Buffer.from(await response.arrayBuffer()));
  renameSync(partial(out), out);
}

function writeManifest(): void {
  writeFileSync(partial(manifestPath), `${JSON.stringify(manifest, null, 2)}\n`);
  renameSync(partial(manifestPath), manifestPath);
}

async function files(out: string): Promise<void> {
  mkdirSync(out, { recursive: true });
  const work = join(dir, "downloads");
  mkdirSync(work, { recursive: true });

  const canterbury = join(work, "cantrbry.tar.gz");
  if (!existsSync(canterbury)) {
    await download(CANTERBURY, canterbury);
  }
  const canterburyDir = join(out, "canterbury");
  mkdirSync(canterburyDir, { recursive: true });
  execFileSync("tar", ["-xzf", canterbury, "-C", canterburyDir]);

  const silesiaZip = join(out, "silesia.zip");
  if (!existsSync(silesiaZip)) {
    await download(SILESIA, silesiaZip);
  }
  const silesiaDir = join(out, "silesia");
  mkdirSync(silesiaDir, { recursive: true });
  execFileSync("unzip", ["-o", "-q", silesiaZip, "-d", silesiaDir]);
  // The tar is the part's mark of completion, so it lands last.
  const silesiaTar = join(out, "silesia.tar");
  execFileSync("tar", ["-cf", partial(silesiaTar), "-C", silesiaDir, "."]);
  renameSync(partial(silesiaTar), silesiaTar);
}

// Each part lands under its own name only once complete, and the manifest is
// written after each, so a run that fails keeps what it finished.
const documentsDir = join(dir, "documents");
const recordsPath = join(documentsDir, "records.jsonl");
if (!existsSync(recordsPath)) {
  mkdirSync(documentsDir, { recursive: true });
  const records = await documents(partial(recordsPath), DOCUMENTS_BYTES);
  renameSync(partial(recordsPath), recordsPath);
  manifest.documents = { source: GHARCHIVE_HOUR, bytes: sizeOf(recordsPath), records };
  writeManifest();
}

const chainDir = join(dir, "chain");
const blocksPath = join(chainDir, "blocks.jsonl");
const transactionsPath = join(chainDir, "transactions.jsonl");
if (!existsSync(blocksPath)) {
  mkdirSync(chainDir, { recursive: true });
  const blocks = await chain(partial(blocksPath), partial(transactionsPath), CHAIN_BYTES);
  // The blocks are the part's mark of completion, so they land last.
  renameSync(partial(transactionsPath), transactionsPath);
  renameSync(partial(blocksPath), blocksPath);
  manifest.chain = {
    source: `TronGrid blocks from ${TRON_START}`,
    bytes: sizeOf(blocksPath) + sizeOf(transactionsPath),
    records: blocks,
  };
  writeManifest();
}

const filesDir = join(dir, "files");
if (!existsSync(join(filesDir, "silesia.tar"))) {
  await files(filesDir);
  manifest.files = { source: `${CANTERBURY}, ${SILESIA}`, bytes: sizeOf(filesDir) };
  writeManifest();
}

console.log(`blob sweep data in ${dir}`);
