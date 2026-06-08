# AAD-bound encrypted block wire format

**Status:** Draft v1. Design decisions in §4 / §6 / §7 are locked; layout in §5 is locked; test vectors in §9 are locked. Implementation lands in #251, conformance tests in #253. ECC-at-rest (#254) is a separate orthogonal layer (Page ECC parity trailer) outside this encryption envelope.

**Scope:** Encrypted-at-rest SST block payloads. Plaintext blocks (no encryption) use the existing `Header` envelope from `src/table/block/header.rs` and are out of scope here.

**Related:** #250 (this spec), #251 (encoder/decoder), #253 (threat-model regression suite), #254 (ECC-at-rest on the Page ECC path), #255 (SECDED single-bit fast path), #256 (forensics CLI), #257 (partial-decode for range queries).

---

## 1. Goals

- **Authenticated integrity** of every encrypted block under an AEAD construction (AES-256-GCM or ChaCha20-Poly1305). A bit flip in any byte of the encrypted payload or its surrounding metadata fails AEAD verification.
- **Block-context binding** via AEAD's Additional Authenticated Data (AAD): the AAD includes the file's `table_id`, the block's `block_type`, the compression context (`compression_type`, dictionary id, window log), the transform-presence `block_flags`, and the AEAD `suite_id` + `key_epoch`. Swapping a block from a different table, a different block type, a different codec, or decrypting under a different key/suite fails AEAD verification. A block's byte offset and the owning tree id are deliberately NOT bound (see §5.3): position integrity is supplied by the authenticated index, and cross-tree isolation by per-tree encryption keys.
- **Crypto-agility:** the on-disk record carries an explicit `suite_id` byte so a deployment can rotate to a new AEAD without rewriting old data, and so a future suite can be added by registry update alone.
- **Key rotation without rewrite:** the `key_epoch` byte indexes into a caller-managed key chain. Old blocks under epoch `N` stay readable as long as the corresponding key is still in the chain; new writes pick the latest epoch.
- **Single-pass decode:** all *on-disk* metadata that feeds the AAD lives at known fixed offsets inside the MetadataFrame. The one remaining AAD field, `TableID`, is caller-supplied from the read context (the process knows which table it is reading) and is deliberately NOT on disk; see §5.3 (the caller-supplied vs on-disk contract) and the "Why not store them on disk" rationale in §5.1. Decode reads the metadata frame once, combines the on-disk fields with the caller-supplied `TableID` to form AAD, then verifies + decrypts the body frame.
- **Zstd-skippable framing:** the on-disk record uses the [Zstandard skippable frame](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) magic range `0x184D2A50..0x184D2A5F`. A reader that doesn't understand encrypted blocks (e.g. a plain `zstd` CLI on the payload region) skips over them cleanly instead of corrupting on parse.

## 2. Non-goals

- **Confidentiality of the file's existence or size.** A passive observer of the SST file on disk still learns: the file's existence, total size, byte distribution (high entropy), and the count + sizes of blocks. AEAD protects per-block content, not file-level metadata.
- **Key-disclosure recovery.** If an attacker obtains the AEAD key for an epoch, every block written under that epoch is decipherable. Key management (rotation cadence, HSM integration, KMS plumbing) is the caller's responsibility; this spec defines only how block-level encryption uses the supplied key.
- **Side-channel resistance** in the AEAD primitives beyond what the chosen suite already provides. Constant-time AES-GCM requires AES-NI / NEON hardware acceleration on the deployment platform; ChaCha20-Poly1305 is constant-time on all platforms.
- **Authentication of the surrounding file structure.** The SFA TOC and the per-file XXH3 in the manifest cover that surface; this spec only addresses per-block integrity for encrypted blocks. Plaintext / unencrypted blocks continue to use the existing `Header` envelope (`src/table/block/header.rs`) with `Header.checksum` = XXH3-128 over the on-disk data-segment bytes (post-compression for compressed blocks; raw payload for uncompressed). Encrypted blocks under this v1 format **replace** the `Header` envelope entirely with MetadataFrame + BodyFrame; an encrypted block is NOT wrapped in `Header`. **Migration note:** today's codebase still writes encrypted blocks via the legacy `Header`-wrapped path (`Block::write_into` / `Block::from_reader` in `src/table/block/mod.rs`) with the encryption provider operating over the payload bytes. That is a transitional layout, not the format described here. #251 cuts over to the skippable-frame v1 layout defined in this spec; until then the on-disk reality lags the spec, and readers MUST NOT assume an arbitrary `coordinode-lsm-tree` SST on disk is already in the v1 format. The integrity that `Header.checksum` provided for the unencrypted path is provided for encrypted blocks by (a) AEAD-tag in MetadataFrame for transport-layer integrity (catches at-rest corruption and tampering of the ciphertext) and (b) the codec's built-in content checksum inside the encrypted body for plaintext-layer integrity for compressed blocks (catches codec-internal bugs and library drift, see §4.11).
- **Block-level forward secrecy.** Compromise of the current epoch's key reveals all blocks under that epoch. Rotation gives forward secrecy for *future* writes, not past ones.

## 3. Threat model

| Threat | Defended? | Defending mechanism |
|---|---|---|
| Random bit flip in encrypted payload (bit-rot, bad sector) | Yes | AEAD tag mismatch on decrypt → `DecryptError::AeadVerificationFailed` |
| Random bit flip in AAD-bound disk bytes (suite_id, key_epoch, block_type, compression_type, dict_id, window_log, block_flags) | Yes | AAD mismatch on decrypt. AEAD verification fails as `DecryptError::AeadVerificationFailed` — a single tag-mismatch event, NOT a per-field diagnostic: the cipher cannot tell whether the flipped byte was in the ciphertext or in one of the AAD-mirrored disk bytes, so per-field attribution is impossible by construction (not a missing feature). Structural pre-AEAD failures surface as the more specific typed variants (`MalformedMetadataFrame`, `UnsupportedSuite`, `UnknownKeyEpoch`, …). (The caller-supplied AAD field `table_id` is not on disk, so it cannot be bit-flipped at rest; it can only fail to match if the reader supplies wrong context.) |
| **Block swap** within the same table (move block N's bytes to a different block slot of the same table) | **No (by design)** | A block's byte offset is intentionally NOT bound in AAD (offset-independent AAD keeps encryption parallel — see §5.3). Same-table blocks share identical AAD, so an intra-table swap VERIFIES at the AEAD layer and returns the swapped block's plaintext. Position integrity is enforced one layer up by the authenticated index (key-range → offset) + the structural SST layout: a swapped block surfaces as a lookup miss, not as forged content (a value is inseparable from its key inside the authenticated block). |
| **Block swap** across tables in the same tree (move a block from table A to table B) | Yes | AAD carries caller-supplied `table_id` (derived from the SST file's path / table metadata); decrypt under the wrong table_id fails. |
| **Block swap** across trees (move a block from tree A to tree B) | Yes (via key isolation) | The owning tree id is NOT bound in AAD (`TreeId` is a process-ephemeral counter, not durable across reopen — see §5.3). Cross-tree substitution is instead prevented by per-tree key isolation: each tree owns a distinct encryption key, so a block sealed by tree A's key fails AEAD verification under tree B's key. (If a deployment were to share one key across trees, this row would be undefended — production MUST use per-tree keys.) |
| **Block type swap** (relabel a Filter block as Data, e.g. to confuse a partial-decode path) | Yes | AAD carries `block_type`; decrypt under the wrong type fails |
| **Transform-flag relabel** (flip a `block_flags` bit, e.g. clear the per-KV checksum footer bit so a verifying reader stops stripping/checking the footer, or set the compressed/encrypted bit) | Yes | AAD carries the whole `block_flags` byte; decrypt under any flipped transform bit fails. Closes the gap that `block_flags` lives in the plaintext `Block::Header` under a non-cryptographic XXH3 checksum an attacker could recompute. |
| **Compression codec substitution** (relabel a zstd block as Lz4 or vice versa to confuse the decompressor selection step) | Yes | AAD carries `compression_type` (codec discriminator byte); decrypt under the wrong codec tag fails. Defends per-block codec rotation: an attacker cannot mix up old-codec and new-codec blocks during a policy migration. |
| **Codec / decompression pipeline bug** (zstd / lz4 library version drift between write-time and read-time; non-deterministic decoder output; in-memory corruption between AEAD-verify and decompression-end producing wrong plaintext after a successful AEAD verify) | Yes (for compressed blocks) | Codec's built-in content checksum (zstd `Content_Checksum_flag` bit 2 / LZ4 `ContentChecksum` bit) is **required** per §4.11. The codec verifies this automatically during streaming decompression and fails on mismatch. For `CompressionType = None` this defence does not apply: there's no codec to drift, and AEAD primitives have no analogous version-drift failure mode, so AEAD-tag alone is sufficient. |
| **Compression dictionary substitution** (decode under a different zstd dictionary to manipulate decompressed output) | Yes | AAD carries `dict_id`; structured-zstd's `FrameDecoder::expect_dict_id` cross-checks during inner-frame decode. Wiring lives in this repo's encoder/decoder issue #251; the matching enforcement on the structured-zstd side is tracked there as `S-ZSTD-T7`. |
| **Decompression bomb** (forged compressed payload that expands to TBs of plaintext) | Yes | AAD carries raw `window_log` (base-2 log of max window size, not the encoded RFC 8878 `Window_Descriptor` byte); structured-zstd's `FrameDecoder::expect_window_log` rejects frames whose decoded window exceeds the AAD-declared value |
| **Key epoch downgrade** (replay a block encrypted under epoch N as if it were epoch M ≠ N) | Yes | AAD carries `key_epoch`; the wrong key is selected and AEAD verification fails |
| **Suite downgrade** (relabel an AES-256-GCM block as ChaCha20-Poly1305 to coerce a different decrypt path) | Yes | AAD carries `suite_id`; the wrong primitive is selected and AEAD verification fails |
| **Replay across versions** (re-introduce an old block after compaction has logically deleted it) | **Partial** | AAD does not include a per-block version / generation counter. The same `(table_id, block_type, compression_type, dict_id, window_log, block_flags, key_epoch, suite_id)` tuple is valid for the same block content. Detection lives one layer up: the manifest's per-table XXH3 + per-file checksum catch a swap of an entire SST file, and the SFA TOC catches reorder within a file. Per-block replay within a single SST is **not defended at the AEAD layer.** |
| Key disclosure | **No** (non-goal, §2) | n/a |
| Brute-force AEAD key search | **No** (assumed infeasible for 256-bit keys with the chosen suites) | n/a |

## 4. Locked design decisions

| # | Decision | Value | Rationale |
|---|---|---|---|
| 4.1 | Outer framing | [Zstandard skippable frame](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) | Already standard, already understood by `zstd` CLI / libzstd readers, has dedicated magic range. Unknown-to-reader frames skip cleanly. |
| 4.2 | Magic allocation | `0x184D2A50` (metadata frame), `0x184D2A51` (body frame). No other magic in the RFC 8878 skippable range is claimed by this format. | Stable magic split keeps the metadata size known-at-parse before the variable-length body. Unused magics are not pre-reserved: a future extension that needs one claims it then. |
| 4.3 | Endianness, outer framing | Little-endian per RFC 8878 §3.1.1 | Mandatory by the framing spec we're inside of. |
| 4.4 | Endianness, AAD payload content | **Multi-byte numeric fields:** big-endian (TableID as u64 BE; DictID as u32 BE). **MagicMetadata:** literal 4-byte byte string (the on-disk LE bytes `50 2A 4D 18` of the framing magic, used verbatim, NOT re-encoded as a BE integer). **Single-byte fields** (HeaderByte / KeyEpoch / BlockType / SuiteID / CompressionType / WindowLog / BlockFlags): no endianness; one byte. | Crypto convention for numerics (RFC 8446 TLS, RFC 5116 AEAD framework). Avoids endianness ambiguity across deployments. MagicMetadata keeps the LE on-disk bytes as a fixed literal so that the AAD's first 4 bytes are byte-for-byte identical to the on-disk MetadataFrame magic the decoder just read; this binds the AAD to the format identity without conversion logic. |
| 4.5 | AEAD suite registry | `0x00` reserved (Plain, encryption disabled, not used in this format), `0x01` reserved (future stream cipher), `0x02 = AES-256-GCM`, `0x03 = ChaCha20-Poly1305`, `0x04..0xFF` reserved | Two initial suites cover the hardware-accelerated (AES-GCM via AES-NI/NEON) and the constant-time-on-any-CPU (ChaCha-Poly) cases. |
| 4.6 | Nonce field width on disk | **Suite-defined** (registry lookup; see §7). v1 suites (AES-256-GCM, ChaCha20-Poly1305) both store 12 bytes. Future suites with different nonce lengths (e.g. XChaCha20-Poly1305 with 24 B) introduce their own SuiteID and their own nonce length in the registry; the decoder reads SuiteID first, then reads exactly the registered nonce length. No padding bytes wasted on speculative future suites. | Saves 12 B per block today vs. always reserving 24 B "for future use". MetadataFrame is variable-length but its `PayloadLen` (u32 LE in the framing header) tells the decoder how many bytes to read, so variable size doesn't complicate parsing. |
| 4.7 | Authentication tag field width | Fixed 16 bytes | Both initial suites (AES-256-GCM, ChaCha20-Poly1305) emit a 128-bit tag. Reserving 16 bytes covers any plausible future tag-truncation policy and keeps the metadata frame size constant. |
| 4.8 | `HeaderByte` layout | High nibble = format version (`0b0001` = v1 in this spec), low nibble reserved (MUST be zero on write, ignored on read until a future spec assigns it) | Single byte covers version + future spare. Forward extension can promote low-nibble bits to typed fields without breaking v1 readers. |
| 4.9 | Inner-frame integrity (dict / window) | Delegated to [structured-zstd](https://github.com/structured-world/structured-zstd) `FrameDecoder::expect_dict_id` / `expect_window_log` (the call-out is structured-zstd PR-F; this spec describes the contract, the implementation lives in S-ZSTD-T7). | Avoids re-validating zstd internals at the LSM layer. The AAD carries `dict_id` + `window_log` so the LSM decoder can pass them through. |
| 4.10 | Number of frames per block | Exactly **two** frames: one MetadataFrame (magic `0x184D2A50`, payload size suite-defined: 39 bytes for v1 suites) immediately followed by one BodyFrame (magic `0x184D2A51`, variable payload), and nothing after the BodyFrame. v1 readers MUST reject a block if MetadataFrame or BodyFrame is missing or out of order, or if any bytes follow the BodyFrame. | Splitting metadata out keeps it known-size and parseable without first probing the body. The format defines no trailing frames, so trailing bytes are malformed data, not a forward-compat extension point. ECC-at-rest is NOT carried here: it lives in the Page ECC parity trailer outside the encryption envelope, so it stays orthogonal to encryption and covers non-encrypted blocks too. |
| 4.11 | Plaintext-side integrity | **Delegated to codec.** For `CompressionType ∈ {Lz4, Zstd, ZstdDict}` the writer MUST enable the codec's built-in content checksum (zstd: `Content_Checksum_flag` bit 2 of Frame_Header_Descriptor per RFC 8878 §3.1.1.1.3, emitting a 4-byte XXH64-truncated-to-32-bits at the end of the zstd frame; LZ4: `ContentChecksum` bit in FLG byte of FrameDescriptor per LZ4 Frame Format Description, emitting a 4-byte XXH32). The decoder verifies this automatically during streaming decompression. For `CompressionType = None` the AEAD tag is the sole integrity layer; there's no codec to drift and no separate codec checksum to enable. | AEAD authenticates the ciphertext round-trip but never inspects decrypted+decompressed bytes; codec content checksum catches codec-internal failures (library version drift, non-deterministic decoder output) that AEAD cannot see. Using the codec's built-in mechanism instead of an external XXH3-32 field in MetadataFrame: (a) reuses RFC-standardised, well-fuzzed reference implementations, (b) streaming verification is "free" inside the codec library, (c) zero MetadataFrame overhead (the 4 bytes live inside the codec frame, which is encrypted as part of BodyFrame ciphertext anyway, and AEAD-tag still covers them). For `CompressionType = None`, an external checksum was considered and rejected: AEAD primitives (AES-256-GCM, ChaCha20-Poly1305) have stable reference implementations with no "version drift" failure mode analogous to codec drift, so AEAD-tag alone provides the integrity guarantee. |

## 5. Wire format

The on-disk layout for an AAD-bound encrypted block is exactly two consecutive [Zstandard skippable frames](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) and nothing more. Per §4.10, v1 readers MUST reject any bytes following the BodyFrame:

```text
MetadataFrame  (8-byte framing header + 39-byte payload   = 47 bytes total for v1 suites)
BodyFrame      (8-byte framing header + N-byte payload    = 8 + N bytes total)
```

Both framing headers are little-endian per RFC 8878 §3.1.1; both payloads are AAD-bound.

### 5.1 MetadataFrame

```text
Offset  Size  Field             Description
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicMetadata     0x50 0x2A 0x4D 0x18  (LE for 0x184D2A50)
4       4     PayloadLen        u32 LE; equals 39 for v1 suites
                                (AES-256-GCM, ChaCha20-Poly1305). For
                                future suites with longer or shorter
                                nonces (see §4.6 / §7), PayloadLen
                                tracks the actual on-disk size:
                                `PayloadLen = 27 + NONCE_LEN`.
8       1     HeaderByte        High nibble = version (0b0001 for v1),
                                low nibble = 0 (reserved, MUST be zero)
9       1     KeyEpoch          Index into the caller's key chain
10      1     BlockType         0=Data 1=Index 2=Filter 3=Meta 4=RangeTombstone
11      1     SuiteID           AEAD primitive (see §4.5 / §7 registry).
                                Also determines the on-disk Nonce length
                                read at offset 19 (after the BlockFlags
                                byte at offset 18).
12      1     CompressionType   Compression codec applied to the block's
                                plaintext BEFORE encryption. Stores ONLY
                                the leading 1-byte codec discriminator
                                (the first tag byte emitted by the
                                `impl Encode for CompressionType` block
                                in `src/compression/mod.rs`); the rest of
                                that on-disk encoding (level for Zstd,
                                level + dict_id LE for ZstdDict) is NOT
                                serialized here. Tag values:
                                  0 = None
                                  1 = Lz4
                                  3 = Zstd (no dict)
                                  4 = ZstdDict
                                  others = reserved
                                Level / dict-fingerprint do NOT live here:
                                compression level is an encoder-side
                                parameter only and is not persisted
                                anywhere on disk (the decoder does not
                                need it: zstd / lz4 frames decode
                                without knowing the level the encoder
                                used), and the dict fingerprint is
                                `DictID` at offset 13.
                                On disk so the decoder picks the right
                                decompressor per block (enables per-block
                                codec rotation: old blocks under old
                                codec / level / dict, new blocks under
                                new policy, compaction migrates cold
                                blocks; analogous to `KeyEpoch` for
                                crypto rotation and `DictID` for
                                dictionary rotation).
13      4     DictID            u32 BE, zstd dictionary id (0 if no dict).
                                On disk because the dict id can vary per
                                block (different LSM levels under different
                                compression policies) and the decoder needs
                                it BEFORE attempting decryption to construct
                                the AAD.
17      1     WindowLog         Raw zstd window log: the base-2 logarithm
                                of the max decompression window size, in
                                bytes (so 21 means 2^21 = 2 MiB). NOT the
                                encoded `Window_Descriptor` byte from
                                RFC 8878 §3.1.1.1.2 (which packs an
                                exponent and a mantissa); the spec stores
                                the raw log because that's what
                                structured-zstd's `FrameDecoder::
                                expect_window_log` consumes. Valid values:
                                `0` means "no zstd / no window enforcement"
                                (used for blocks under CompressionType::None
                                or a non-zstd codec), otherwise `10..=31`
                                per RFC 8878 §3.1.1.1.2 (the decoded
                                equivalent of the descriptor byte's allowed
                                values). Any other value is malformed and
                                MUST be rejected. On disk for the same
                                reason as DictID.
18      1     BlockFlags        Transform-presence bitfield, mirror of the
                                `Block::Header` `block_flags` byte
                                (`crate::table::block::header::block_flags`):
                                  bit 0 = KV_CHECKSUM_FOOTER
                                  bit 1 = ECC_PARITY
                                  bit 2 = COMPRESSED
                                  bit 3 = ENCRYPTED
                                  others = reserved (MUST be 0 on write,
                                           IGNORED on read)
                                On disk + bound in the AAD so an attacker
                                cannot relabel the block's transform stack
                                (e.g. clear the per-KV footer bit) under a
                                forged non-cryptographic header checksum,
                                the same anti-relabel rationale as BlockType
                                / CompressionType. The full byte is mirrored
                                verbatim; the COMPRESSED / ENCRYPTED bits are
                                redundant with CompressionType / SuiteID but
                                kept so the AAD is a faithful mirror with no
                                masking logic.
19     NONCE_LEN  Nonce         Nonce bytes; length determined by SuiteID
                                via the §7 registry (v1: 12 bytes for both
                                AES-256-GCM and ChaCha20-Poly1305). The
                                decoder MUST consult the registry by
                                SuiteID before reading this field; the
                                next field (AEADTag) starts at offset
                                `19 + NONCE_LEN`. No padding bytes:
                                future suites with different nonce
                                lengths use their own SuiteID and pay
                                exactly their own nonce-length cost.
19+NONCE_LEN  16  AEADTag       AEAD authentication tag over the body
                                payload + the AAD (see §5.3)
═════════
Total  for v1 suites: 47 bytes on disk
       (8-byte framing header + 39-byte payload).
       For other suites: 8 + (27 + NONCE_LEN) bytes.
```

**On-disk minimalism.** The MetadataFrame on disk carries ONLY the fields the decoder needs *before* it can construct the AAD: the version byte, the key epoch (so the right key is selected), the block type (mirrors the existing `Header` pattern), the AEAD suite id (so the right primitive is selected), the compression-context fields (`DictID` + `WindowLog`, which can vary per block and which the decoder must know to bind the AAD before any decompression / decryption work), the nonce, and the AEAD tag. One further identifier (`TableID`) participates in the AAD but is **NOT** stored on disk: it is caller-supplied from the read context (the SST file's per-tree `TableId`). The block's byte offset and the owning tree id participate in neither the AAD nor the on-disk frame (see §5.3 for why each is excluded). See §5.3.

**Why not store it on disk.** Industry-standard LSMs (RocksDB / LevelDB / Pebble) put zero identity bytes in per-block headers: a per-block trailer is 5 bytes total (1 byte compression + 4 byte checksum). Block identity is purely positional: the SST footer points at the index, the index gives `BlockHandle { offset, size }`, and the file's path/manifest gives the table id. The same model applies here: spending 8 bytes per block on an on-disk `TableID` would duplicate context the caller already has at decrypt time, and it would be cryptographically *weaker* than the AAD binding it would replace (a tamperer could just patch the on-disk bytes; tampering with the AAD-bound value is infeasible). Orphan-block forensics is addressed at the per-file layer (the META blocks introduced in #295 carry the file-level identity), not by fattening every block header.

### 5.2 BodyFrame

```text
Offset  Size  Field             Description
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicBody         0x51 0x2A 0x4D 0x18  (LE for 0x184D2A51)
4       4     PayloadLen        u32 LE, length of EncryptedBody in bytes
8       N     EncryptedBody     N = PayloadLen. The AEAD ciphertext of the
                                block's data segment / payload bytes only
                                (after optional compression, if any). It
                                does NOT include any legacy plaintext
                                `Header` envelope bytes.
═════════
Total  8 + N bytes on disk
```

### 5.3 AAD construction

The AEAD's Additional Authenticated Data is constructed by the writer immediately before encrypting the body, and reconstructed by the reader immediately before decrypting. AAD is **NEVER** written to disk; both sides construct it from a mix of disk-mirrored fields (which the decoder has just read from the MetadataFrame) and one caller-supplied field (the SST file's per-tree `TableId`, which the decoder knows from its read context):

```text
Offset  Size  Field             Source
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicMetadata     The literal four bytes 0x50 0x2A 0x4D 0x18
                                (binds the AAD to this format identity; an
                                attacker that lifts the metadata bytes into
                                a future format with a different magic gets
                                a different AAD and verification fails)
4       1     HeaderByte        Mirror of MetadataFrame offset 8 (disk)
5       1     KeyEpoch          Mirror of MetadataFrame offset 9 (disk)
6       1     BlockType         Mirror of MetadataFrame offset 10 (disk)
7       1     SuiteID           Mirror of MetadataFrame offset 11 (disk)
8       8     TableID           u64 BE, caller-supplied from the SST file's
                                per-tree `TableId` (derived from the file
                                path / table metadata). NOT on disk: the
                                reader already knows which file it opened.
                                Defeats cross-TABLE substitution. Cross-TREE
                                substitution is NOT bound here (see below)
                                and is prevented by per-tree key isolation.
16      1     CompressionType   Mirror of MetadataFrame offset 12 (disk).
                                Binds the AAD to the codec the writer
                                used so an attacker cannot relabel
                                a zstd block as Lz4 (or vice versa)
                                to confuse downstream decompression.
17      4     DictID            u32 BE, mirror of MetadataFrame offset 13
                                (disk)
21      1     WindowLog         Mirror of MetadataFrame offset 17 (disk)
22      1     BlockFlags        Mirror of MetadataFrame offset 18 (disk).
                                Binds the block's transform-presence
                                bitfield (KV footer / ECC / compressed /
                                encrypted) so an attacker cannot relabel
                                the transform stack — e.g. clear the per-KV
                                checksum footer bit — under a forged
                                non-cryptographic header checksum.
═════════
Total  23 bytes (NEVER written to disk, passed to AEAD as AAD only)
```

**Two identifiers are deliberately NOT bound in the AAD:**

- **A block's byte offset.** The on-disk offset is unknown to several writers (the sfa-wrapped index / filter writers and the parallel block compressor do not surface a byte cursor) and it varies across the MID / tail / head mirrors of one logical block. Binding it would break those paths and force serial encryption (the offset is only known at placement). The cost is that two blocks of the SAME table are interchangeable at the AEAD layer; block-position integrity is provided one layer up by the authenticated index (which maps key ranges to offsets) plus the structural SST layout, not by per-block AEAD. Intra-file block swap therefore degrades only to a lookup miss (a value is inseparable from its key inside the authenticated block), not to forgery.
- **The owning tree id.** `TreeId` in this codebase is a process-ephemeral counter (`get_next_tree_id()`), not a durable identifier — the same SST is assigned a different tree id when its tree is reopened. Binding it would make every encrypted block fail AEAD verification after a restart. Cross-tree substitution is instead prevented by **per-tree key isolation**: each tree owns a distinct encryption key, so a block sealed by tree A's key fails to decrypt under tree B's key regardless of AAD.

**Disk vs caller-supplied: the contract.** Fields marked "mirror of MetadataFrame offset X (disk)" are read from the on-disk MetadataFrame the decoder has just parsed. The `TableID` field is passed in by the calling code from its own context (`BlockIdentity` struct in `src/table/block/identity.rs`). The writer feeds *the same value* from its own context into AAD construction. The AEAD's authentication tag binds all 23 bytes together: an attacker who modifies any disk byte, or who relocates a block to a different table, produces an AAD that doesn't match the one the AEAD was sealed under, and decryption fails.

The `Nonce` and `AEADTag` fields are **not** part of the AAD, they're the AEAD's nonce and tag inputs, respectively.

The `MagicBody` and `PayloadLen` from BodyFrame are also **not** part of the AAD. RFC 8878 skippable framing carries no integrity check (a non-conformant reader is expected to *skip* unknown frames, not validate them), so a decoder MUST NOT rely on framing for authentication. Instead the decoder MUST enforce these structural invariants explicitly before doing any further work:

- MetadataFrame `MagicMetadata` equals `0x184D2A50` (LE bytes `50 2A 4D 18`). If not, treat as a non-AAD-bound block and refuse to decrypt.
- MetadataFrame `PayloadLen` equals `27 + NONCE_LEN`, where `NONCE_LEN` is the nonce length registered for the suite byte at offset 11 (v1 suites: 39 = 27 + 12). Decoder sequencing for this check: (1) read the framing header's 8 bytes (`MagicMetadata` + `PayloadLen`); (2) read the first 4 bytes of payload (HeaderByte + KeyEpoch + BlockType + SuiteID) and resolve `NONCE_LEN` from SuiteID via the §7 registry; (3) validate `PayloadLen == 27 + NONCE_LEN` BEFORE reading the variable-length tail (DictID + WindowLog + Nonce + AEADTag) or touching the BodyFrame. Any mismatch is malformed and MUST be rejected: no AAD can be constructed, so AEAD cannot bind context.
- BodyFrame `MagicBody` equals `0x184D2A51` (LE bytes `51 2A 4D 18`). If not, reject.
- BodyFrame `PayloadLen` is in the range `[1, 256 MiB]` for the v1 suites. `256 MiB` is the plaintext upper bound on a single block's on-disk data segment, mirroring the internal 256 MiB hard cap enforced by block I/O (`table::block::MAX_DECOMPRESSION_SIZE`, a private const) and the scrub / verify paths (`verify::MAX_BLOCK_DATA_LENGTH`, also a private const that intentionally tracks the same value). Neither is a public API constant; the 256 MiB value is the spec-level invariant. In this wire format, the nonce and authentication tag live in `MetadataFrame`, and for the v1 suites `ciphertext_len == plaintext_len`, so `EncryptionProvider::max_overhead()` does **not** apply to BodyFrame sizing. A larger value means either a forged TOC or a header bit-flip and MUST be rejected before allocating the read buffer. If a future suite permits ciphertext expansion in BodyFrame, that expansion MUST be defined explicitly as a suite-specific ciphertext expansion term in the suite registry/spec, and decoders MUST validate against that term rather than a provider-generic `max_overhead()`.

These checks are not AEAD-authenticated, but they bound the attack surface so that any bypass attempt either (a) fails the structural check above, or (b) reaches the AEAD and fails AAD verification on the metadata-mirror fields.

### 5.4 ABNF grammar

[RFC 5234](https://datatracker.ietf.org/doc/html/rfc5234) syntax:

```abnf
;; ABNF: v1 wire format with v1 suites (AES-256-GCM, ChaCha20-Poly1305),
;; both of which declare NONCE_LEN = 12 in the §7 registry. Terminal
;; constants (`metadata-payload-len`, `nonce` width) are hardcoded
;; below for the v1-with-v1-suites concrete bytes that appear on disk
;; today. Future suites with different NONCE_LEN (or a future spec
;; revision that adds / removes payload fields) get their own ABNF
;; revision; the abstract framing contract is `PayloadLen == 27 +
;; NONCE_LEN` (10 fixed pre-nonce bytes: HeaderByte + KeyEpoch +
;; BlockType + SuiteID + CompressionType + 4-byte DictID + WindowLog;
;; plus the suite-defined NONCE_LEN bytes; plus 16 bytes of AEADTag)
;; and the per-field structure described in §5.1.
;;
;; A single AAD-bound encrypted block on disk is exactly the pair
;; (metadata-frame, body-frame) with no trailing frames. v1 readers
;; MUST reject any bytes following the body-frame.
encrypted-block   = metadata-frame body-frame
                                                   ; (see prose below)

metadata-frame    = metadata-magic metadata-payload-len metadata-payload
metadata-magic    = %x50.2A.4D.18                  ; 0x184D2A50 LE
metadata-payload-len = %x27.00.00.00               ; u32 LE = 39 (v1 suites: 27 + 12-byte nonce)
                                                   ; For other suites: u32 LE = 27 + suite's NONCE_LEN.
                                                   ; Decoder MUST resolve NONCE_LEN from SuiteID
                                                   ; via the §7 registry before reading the payload.
metadata-payload  = header-byte                    ; 1B
                    key-epoch                      ; 1B
                    block-type                     ; 1B
                    suite-id                       ; 1B
                    compression-type               ; 1B
                    dict-id                        ; 4B BE
                    window-log                     ; 1B
                    block-flags                    ; 1B
                    nonce                          ; NONCE_LEN B (suite-defined; 12B for v1)
                    aead-tag                       ; 16B

;; Note: tree-id, table-id, block-offset are AAD-bound (see §5.3)
;; but NOT part of metadata-payload. They are caller-supplied from
;; read context (owning Tree, SST file path, seek cursor) and never
;; transmitted on the wire.

body-frame        = body-magic body-payload-len encrypted-body
body-magic        = %x51.2A.4D.18                  ; 0x184D2A51 LE
body-payload-len  = 4OCTET                         ; u32 LE
encrypted-body    = *OCTET                         ; suite-dependent ciphertext

;; Field shapes
header-byte       = OCTET                          ; high nibble == 0x1 in v1
key-epoch         = OCTET
block-type        = %x00 / %x01 / %x02 / %x03 / %x04
                                                   ; Data / Index / Filter / Meta / RangeTombstone
suite-id          = %x02 / %x03                    ; AES-256-GCM / ChaCha20-Poly1305 in v1
compression-type  = %x00 / %x01 / %x03 / %x04      ; None / Lz4 / Zstd / ZstdDict
                                                   ; (tags match `impl Encode for CompressionType` in src/compression/mod.rs)
dict-id           = 4OCTET                         ; u32 BE
window-log        = %x00 / %x0A-1F                 ; 0 = no zstd, 10..=31 = raw log2 window
block-flags       = OCTET                          ; transform-presence bitfield: bit0 KV footer,
                                                   ; bit1 ECC, bit2 compressed, bit3 encrypted;
                                                   ; other bits reserved (0 on write, ignored on read)
nonce             = 12OCTET                       ; v1 suites only. Length is suite-defined
                                                   ; (see §7 registry); other suites may use
                                                   ; different lengths under their own SuiteID.
aead-tag          = 16OCTET

;; Core terminal, RFC 5234 itself does not define OCTET in its core
;; rules (only ALPHA / DIGIT / CRLF / etc.), so the grammar defines
;; it explicitly. The `NOCTET` notation (e.g. `4OCTET`, `8OCTET`) is
;; the standard ABNF repetition shorthand for "exactly N occurrences
;; of OCTET" per RFC 5234 §3.6.
OCTET             = %x00-FF                        ; any 8-bit byte
```

**Length constraints (cannot be expressed in ABNF).** ABNF's `*OCTET` matches an unbounded run of bytes, which doesn't capture the contract that a payload is **exactly** the number of bytes declared by its preceding length field. This applies to `metadata-payload` (length 39 for v1 suites with 12-byte nonces; in general `27 + NONCE_LEN`) and `encrypted-body` (length declared by `body-payload-len`): both are `*OCTET` in the grammar but constrained by their preceding length fields. A reader that consumes more or fewer bytes than the declared length, or that finds any bytes after the body-frame, MUST treat the file as malformed.

## 6. Magic allocation

The `0x184D2A50..0x184D2A5F` range is reserved by [RFC 8878 §3.1.2](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.2) for user-defined skippable frames. This spec claims exactly two magics from that range and pre-reserves none of the rest:

| Magic | Frame | Status |
|---|---|---|
| `0x184D2A50` | MetadataFrame v1 (this spec) | Locked |
| `0x184D2A51` | BodyFrame v1 (this spec) | Locked |
| `0x184D2A52..0x184D2A5F` | Not used by this format | Unclaimed |

Implementations MUST reject blocks whose first frame magic is not `0x184D2A50` (any conformant v1 writer, including the #251 encoder implementation, always emits this magic as the first byte of an encrypted block). The remaining magics in the range are unclaimed: a future spec revision that needs one allocates it at that point.

## 7. AEAD suite registry

| SuiteID | Name | Key size | Nonce length (on disk) | Tag |
|---|---|---|---|---|
| `0x00` | Reserved (Plain, not used in this format) | n/a | n/a | n/a |
| `0x01` | Reserved | n/a | n/a | n/a |
| `0x02` | AES-256-GCM ([RFC 5116](https://datatracker.ietf.org/doc/html/rfc5116) + [NIST SP 800-38D](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf)) | 32 B | 12 B | 16 B |
| `0x03` | ChaCha20-Poly1305 ([RFC 8439](https://datatracker.ietf.org/doc/html/rfc8439)) | 32 B | 12 B | 16 B |
| `0x04..0xFF` | Reserved | n/a | n/a | n/a |

The on-disk Nonce field length is **determined by SuiteID via this registry**. Decoders MUST read SuiteID at MetadataFrame offset 11, look up its registered nonce length, then read exactly that many bytes at offset 19 (immediately after the BlockFlags byte at offset 18). There is no padding: future suites with longer nonces (e.g. a hypothetical XChaCha20-Poly1305 with a 24-byte nonce) declare their own length here and pay exactly that cost; no v1 block carries bytes reserved "for future use".

Adding a new suite requires:
1. Allocating a SuiteID byte in the registry (above table).
2. Specifying key size and effective nonce length. **The on-disk authentication tag is locked at 16 bytes per §4.7**; the registry's tag column documents the suite's natural tag width for cross-reference, but the on-disk field is fixed-width and a new suite MUST emit a 16-byte tag on disk. A suite whose primitive cannot produce (or be safely truncated to) 16 bytes is not eligible under the v1 format and would require a format version revision before it could be registered here.
3. Updating the conformance test suite (#253) with at least one test vector per block type.

A new suite does NOT require a format version bump (provided the 16-byte-tag constraint above is met), readers select the suite from the `SuiteID` byte at decode time. Old blocks under the old suite remain readable as long as the implementation links the corresponding AEAD primitive.

## 8. Security properties

Per-attack mapping of which AAD-bound field defeats which threat:

| Attack | Defending field(s) | How it defeats the attack |
|---|---|---|
| Bit flip in encrypted payload | (AEAD tag) | Standard AEAD: any payload modification invalidates the tag. |
| Bit flip in MetadataFrame **payload-mirrored** fields (HeaderByte / KeyEpoch / BlockType / SuiteID / CompressionType / DictID / WindowLog / BlockFlags) | The mirrored AAD field | The flipped byte ends up in the AAD; decryption derives a different tag and fails (cryptographically infeasible to forge). |
| Bit flip in MetadataFrame **framing** fields (MagicMetadata / PayloadLen / MagicBody / BodyFrame PayloadLen) | Structural decoder checks per §5.3 | These fields are NOT in the AAD (they're framing layer. A flipped Magic byte causes structural rejection ("not a v1 encrypted block" or "BodyFrame absent"). A flipped PayloadLen byte produces a value ≠ `27 + NONCE_LEN` and the decoder rejects before reading the rest of the payload. AEAD is never reached for these. |
| Bit flip in Nonce | (AEAD construction) | Different nonce → AEAD verifies against a tag computed under a different keystream/counter → fails. |
| Bit flip in AEADTag | (AEAD construction) | Standard AEAD: the on-disk tag doesn't match the recomputed one → fails. |
| Block swap within the same table | (not AAD-bound — by design) | A block's byte offset is intentionally NOT in AAD (offset-independent AAD keeps encryption parallel). Same-table swaps verify at the AEAD layer; position integrity is enforced by the authenticated index + structural SST layout, so a swap surfaces as a lookup miss, not forged content. |
| Block swap across tables in the same tree | `TableID` (caller-supplied from file path) | The reader's file-path-derived TableID doesn't match what the writer used; AEAD verification fails. |
| Block swap across trees | (per-tree key isolation) | The owning tree id is NOT in AAD (`TreeId` is process-ephemeral). Each tree owns a distinct key, so a cross-tree-substituted block fails AEAD verification at the key layer. |
| Block type relabel (Filter → Data) | `BlockType` | The bytes are valid but the type byte differs → AAD mismatch. |
| Compression codec relabel (zstd → Lz4 or similar) | `CompressionType` | AAD binds the codec discriminator byte; decoding the block under a different codec selection produces an AAD that doesn't match. |
| Codec / decompression pipeline bug (library version drift, non-deterministic decoder, in-memory corruption between AEAD-verify and decompression-end) | Codec's built-in content checksum (zstd `Content_Checksum_flag`; LZ4 `ContentChecksum` bit) | Mandated by §4.11 for compressed blocks. Verified by the codec library during streaming decompression; mismatch surfaces as a codec-library error. AEAD covers the ciphertext including the codec's trailing checksum bytes, so tampering with the checksum itself fails AEAD before the codec even sees it. For `CompressionType = None` there's no codec and no codec checksum; AEAD-tag is the sole integrity layer (no version-drift risk for stable AEAD primitives). |
| Compression dict substitution | `DictID` (lsm-tree wiring: #251; structured-zstd enforcement: S-ZSTD-T7) | LSM-side: AAD binds dict_id, so reading under a different dict fails AAD. Inside-frame: structured-zstd's `expect_dict_id` re-checks the dict id encoded in the zstd frame header. |
| Decompression bomb (forged window) | `WindowLog` (lsm-tree wiring: #251; structured-zstd enforcement: S-ZSTD-T7) | AAD binds raw `window_log` (base-2 log of max window in bytes). structured-zstd's `expect_window_log` decodes each frame's `Window_Descriptor` byte to its raw log equivalent and rejects frames whose decoded value exceeds the AAD-bound limit. |
| Key epoch downgrade | `KeyEpoch` | Selecting the wrong key (because the wrong epoch is declared) yields the wrong AEAD primitive instance → fails. |
| Suite downgrade | `SuiteID` | Selecting the wrong primitive yields the wrong tag → fails. |
| AAD format substitution (lifting metadata bytes into a future format with a different magic) | `MagicMetadata` (first 4 bytes of AAD) | The literal magic bytes are part of the AAD; a different format with a different magic produces a different AAD → fails. |

## 9. Reference test vectors

The **inputs** of each vector (key, plaintext, AAD-bound fields, nonce) are normative: the conformance harness in #253 generates the AEAD output for those inputs and pins the resulting on-disk byte sequence. The **outputs** (ciphertext + tag bytes) are NOT hand-transcribed here; they are produced by a known reference implementation in the harness, then checked into the test data alongside the test as fixed expectations.

Why outputs are not inline: hand-transcribing 94 bytes of AES-256-GCM output is error-prone, and the binding cryptographic property is "this AAD + key + nonce + plaintext produces these bytes", which the harness verifies by byte-equality. Third-party / cross-language implementations should reproduce the vectors by running their own AEAD primitive on the listed inputs and comparing against the bytes published by the harness alongside #253, that file becomes the canonical wire test data once #253 lands.

All `key` / `value` / `nonce` byte sequences are shown hex, big-endian to small-endian for byte 0 to byte N (i.e. byte 0 is leftmost). Vectors use fixed nonces for reproducibility, production writers MUST generate nonces per [RFC 5116 §3.2](https://datatracker.ietf.org/doc/html/rfc5116#section-3.2) (random 96-bit / counter, not repeated under the same key).

### Vector 1: Data block, AES-256-GCM, no dict

**MetadataFrame fields (stored on disk, plaintext, alongside the encrypted body):**

| Field | Value |
|---|---|
| KeyEpoch | `01` |
| BlockType | `00` (Data) |
| SuiteID | `02` (AES-256-GCM) |
| CompressionType | `00` (None) |
| DictID (BE) | `00000000` (= 0, no dict) |
| WindowLog | `00` (CompressionType=None → no zstd, window enforcement disabled per §5.1) |
| Nonce (12 B; v1 suite length) | `000102030405060708090a0b` |
| AEADTag (16 B) | produced by the AEAD library at encrypt time; stored on disk |

**BodyFrame contents (the plaintext that gets encrypted into the on-disk ciphertext; NOT itself on disk):**

| Field | Value |
|---|---|
| Plaintext body | `48656c6c 6f2c2057 6f726c64 21` ("Hello, World!", 13 bytes) |

**AEAD key material (NEVER on disk; resolved by the caller from `KeyEpoch` via the per-tree encryption-provider key chain):**

| Field | Value |
|---|---|
| Key (32 B) | `00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000` |

**Caller-supplied AAD context (NEVER on disk; fed into AAD construction at encrypt/decrypt time from read context):**

| Field | Value | Source in the conformance harness |
|---|---|---|
| TableID (BE) | `00000000 00002a30` (= 10800) | The SST file's per-tree TableId |

**AAD (23 B, NEVER on disk):** `502a4d18 10 01 00 02 0000000000002a30 00 00000000 00 00` (canonical order per §5.3: MagicMetadata | HeaderByte=0x10 [v1, low nibble reserved] | KeyEpoch=0x01 | BlockType=0x00 | SuiteID=0x02 | TableID BE | CompressionType=0x00 [None] | DictID BE | WindowLog=0x00 [no zstd] | BlockFlags=0x00 [no transform layers]). A block's byte offset and the owning tree id are not bound — see §5.3.

**Expected on-disk size:** 68 B total = 47 (MetadataFrame = 8 framing + 39 payload for v1 suite) + 8 (BodyFrame framing header) + 13 (EncryptedBody). For AES-256-GCM and ChaCha20-Poly1305, `ciphertext_len == plaintext_len` because the tag is stored in MetadataFrame, not appended to the ciphertext.

The 68-byte on-disk output for this vector ships as fixed binary test data with #253 and becomes the canonical wire reference once that PR lands. The test asserts byte equality between the AEAD library's output and the published bytes.

### Vector 2: Index block, ChaCha20-Poly1305, no dict, no zstd

Same key, KeyEpoch=01, plaintext body `00 01 02 03 04 05 06 07` (8 bytes). BlockType=`01`, SuiteID=`03`, CompressionType=`00` (None). DictID=0. WindowLog=`00` (no zstd compression on this block; window enforcement disabled). Nonce (12 B per §7 registry for SuiteID=0x03): `0c0d0e0f10111213 14151617`. Caller-supplied AAD context: TableID=`00000000 00002a30`.

### Vector 3: Data block, AES-256-GCM, with dict

Key as above, KeyEpoch=01, BlockType=`00`, SuiteID=`02`, CompressionType=`04` (ZstdDict), DictID=`deadbeef`, WindowLog=`15`. Plaintext body 32 bytes of `aa`. Caller-supplied AAD context: TableID=`00000000 00002a30`. The AAD now carries the non-zero DictID and the ZstdDict codec tag, exercising both the dict-substitution and codec-relabel defences.

### Vector 4: Negative, window-bomb (rejection)

Construct a block whose inner zstd frame's `Window_Descriptor` byte (encoded per RFC 8878 §3.1.1.1.2) decodes to a 1 GiB window, but the AAD-bound `WindowLog` field declares 21 (raw log2, = 2 MiB). A conformant decoder MUST reject this block. The reject path is split across two issues by repo: this repo's encoder/decoder (issue #251) wires the AAD-bound `WindowLog` into the inner-validator call, and the structured-zstd side (tracked as `S-ZSTD-T7` in [structured-world/structured-zstd](https://github.com/structured-world/structured-zstd)) implements `FrameDecoder::expect_window_log` to decode the frame's descriptor byte and compare the decoded log against the AAD-bound limit. Error variant: a typed [`DecryptError`](../src/encryption/error.rs) (the WindowLog-validation reject path itself is wired under #251); conformance tests assert on the variant family, not on a static string.

### Vector 5: Negative, key-epoch mismatch (rejection)

Encrypt a block under KeyEpoch=`01`. Tamper the on-disk `KeyEpoch` byte to `02`. The reader selects key `02` from the chain (different from the actual encryption key), AEAD verification fails. Error variant: `DecryptError::AeadVerificationFailed` (standard AEAD tag-mismatch path); conformance tests assert on the variant family.

## 10. Worked hex-dump example

A minimum-size Data block (single-byte plaintext = `41`, "A") encrypted under AES-256-GCM with all-zero key, KeyEpoch=`01`, CompressionType=0 (None), DictID=0, WindowLog=`0` (no zstd, no window enforcement), Nonce = first 12 bytes `00..0b`. Caller context (NOT on disk): TableID=0.

```text
;; MetadataFrame (47 bytes = 8-byte framing + 39-byte payload)
0000: 50 2a 4d 18         ; MagicMetadata (0x184D2A50 LE)
0004: 27 00 00 00         ; PayloadLen = 39 (u32 LE)
0008: 10                  ; HeaderByte: version=0x1, low nibble reserved=0
0009: 01                  ; KeyEpoch
000a: 00                  ; BlockType = Data
000b: 02                  ; SuiteID = AES-256-GCM (NONCE_LEN = 12 per §7)
000c: 00                  ; CompressionType = None (tag 0 per `impl Encode for CompressionType` in src/compression/mod.rs)
000d: 00 00 00 00         ; DictID = 0 (u32 BE)
0011: 00                  ; WindowLog = 0 (CompressionType=None → no zstd, no window enforcement)
0012: 00                  ; BlockFlags = 0 (no transform layers: no KV footer / ECC / compression / encryption-flag bits set)
0013: 00 01 02 03 04 05 06 07   ; Nonce bytes 0..7
001b: 08 09 0a 0b         ; Nonce bytes 8..11 (12-byte AES-GCM nonce, no padding)
001f: <16 bytes AEADTag>  ; depends on the AEAD library output, not literal

;; BodyFrame (8 + 1 = 9 bytes)
002f: 51 2a 4d 18         ; MagicBody (0x184D2A51 LE)
0033: 01 00 00 00         ; PayloadLen = 1 (u32 LE)
0037: <1 byte ciphertext> ; AES-GCM ciphertext of "A" under the AAD below

;; AAD (23 bytes; NEVER written to disk, input to AEAD only)
;; Canonical byte sequence per §5.3:
;;   MagicMetadata | HeaderByte | KeyEpoch | BlockType | SuiteID
;;     | TableID (caller)
;;     | CompressionType | DictID | WindowLog | BlockFlags
;; (a block's byte offset and the owning tree id are NOT bound — see §5.3)
     50 2a 4d 18          ; MagicMetadata
     10                   ; HeaderByte         (mirror of disk byte 0008)
     01                   ; KeyEpoch           (mirror of disk byte 0009)
     00                   ; BlockType          (mirror of disk byte 000a)
     02                   ; SuiteID            (mirror of disk byte 000b)
     00 00 00 00 00 00 00 00   ; TableID BE     (caller-supplied, not on disk)
     00                   ; CompressionType    (mirror of disk byte 000c)
     00 00 00 00          ; DictID BE          (mirror of disk bytes 000d-0010)
     00                   ; WindowLog          (mirror of disk byte 0011)
     00                   ; BlockFlags         (mirror of disk byte 0012)
```

Total on-disk size: 56 bytes (47 metadata + 9 body). The AEADTag and ciphertext bytes are generated by the AEAD library and not literal in this example, the conformance harness in #253 computes them and asserts exact byte equality.

## 11. Implementation hand-off

| Component | Tracking issue | Notes |
|---|---|---|
| Encoder / decoder | #251 | Reads SuiteID, selects primitive, builds AAD per §5.3. AES-256-GCM goes through the existing [`aes-gcm`](https://crates.io/crates/aes-gcm) dependency. ChaCha20-Poly1305 lands behind its own SuiteID (`0x03`) when #251 adds the [`chacha20poly1305`](https://crates.io/crates/chacha20poly1305) crate as a new dependency; until then the encoder rejects writes with SuiteID `0x03`. |
| Codec content-checksum enforcement | #251 (writer-side mandate) + codec libraries (verification side) | Per §4.11, writer MUST enable the codec's built-in content checksum for `CompressionType ∈ {Lz4, Zstd, ZstdDict}`. Zstd: set `Content_Checksum_flag` bit 2 of Frame_Header_Descriptor (RFC 8878 §3.1.1.1.3); the zstd library appends 4-byte XXH64-truncated-to-32-bits and verifies it on streaming decompress automatically. LZ4: set `ContentChecksum` bit in FLG byte of FrameDescriptor; the lz4 library appends 4-byte XXH32 and verifies similarly. For `CompressionType = None` no codec checksum is required (AEAD-tag is sole integrity). |
| Inner-frame validation | lsm-tree wiring: #251 (this repo). Structured-zstd enforcement: `S-ZSTD-T7` in [structured-world/structured-zstd](https://github.com/structured-world/structured-zstd). | This repo's encoder/decoder (#251) passes the AAD-bound `dict_id` / `window_log` into `FrameDecoder::expect_dict_id` / `expect_window_log` before letting decompression proceed. Those `expect_*` hooks are implemented on the structured-zstd side under tracker `S-ZSTD-T7`. |
| Conformance test suite | #253 | One test per row in §9 (vectors 1-5) plus regressions for each row in §8 (attack → defending field). |
| ECC-at-rest (Reed-Solomon parity) | #254 | Built on the Page ECC parity trailer (`crate::ecc`, `BlockTransform`) **outside** the encryption envelope — orthogonal to encryption/compression, covers non-encrypted blocks too. Does NOT use an encryption-envelope skippable frame. |
| SECDED single-bit fast path | #255 (this repo) | DIMM-style single-bit auto-correct on the Page ECC read path (on-disk block bytes, verify = block checksum), ahead of the heavier RS recover. Independent of the encryption wire-format. |
| Forensics CLI | #256 | Reads MetadataFrame in isolation to dump per-block structure (table_id, offset, type, suite, epoch, dict, window) without requiring the key. |
| Partial decode for range queries | #257 | Uses structured-zstd's block-subset API to decode only the matching key range of a compressed block; the AEAD verification covers the entire body, so partial-decode only saves decompression work, not verification work. |

## 12. Open questions

These intentionally remain open for follow-up spec revisions; resolving them does not require breaking the v1 wire format because the HeaderByte's low nibble (§4.8) leaves room for forward-compatible extensions, and the unclaimed magics in the RFC 8878 skippable range (§6) can be allocated by a future revision when one is actually needed.

- **Per-block version / generation counter** to defend against block-level replay within a single SST (currently §3 documents this as a known gap). Adding a 4-byte generation counter to the AAD would close the gap at the cost of 4 bytes per block; deferred until a deployment incident motivates it.
- **AEAD nonce derivation policy.** Random 96-bit nonces under AES-GCM hit the birthday bound at ~2³² distinct nonces under the same key; a deployment that ever approaches 4 billion writes per key needs an explicit counter-mode nonce policy. The spec leaves nonce generation to the writer; a future revision may pin a recommended policy.
- **Suite registry expansion.** AES-256-OCB3 or AEGIS-256 are plausible additions if a deployment needs higher throughput; would slot into SuiteIDs `0x04+` without a format bump.
- **Header-only verify mode.** A reader that only needs to confirm block integrity (e.g. for offline scrubbing) currently still has to decrypt the body. A future "header-MAC only" mode could expose this, would need its own AAD-only MAC field added to a v2 MetadataFrame.
