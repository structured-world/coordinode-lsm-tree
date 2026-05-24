# AAD-bound encrypted block wire format

**Status:** Draft v1. Design decisions in §4 / §6 / §7 are locked; layout in §5 is locked; test vectors in §11 are locked. Implementation lands in #251, conformance tests in #253, ECC layer in #254.

**Scope:** Encrypted-at-rest SST block payloads. Plaintext blocks (no encryption) use the existing `Header` envelope from `src/table/block/header.rs` and are out of scope here.

**Related:** #250 (this spec), #251 (encoder/decoder), #253 (threat-model regression suite), #254 (ECC layer), #255 (lazy block-precise repair), #256 (forensics CLI), #257 (partial-decode for range queries).

---

## 1. Goals

- **Authenticated integrity** of every encrypted block under an AEAD construction (AES-256-GCM or ChaCha20-Poly1305). A bit flip in any byte of the encrypted payload or its surrounding metadata fails AEAD verification.
- **Block-context binding** via AEAD's Additional Authenticated Data (AAD): the AAD includes the file's `table_id`, the block's `block_offset`, its `block_type`, the compression dictionary id, and the AEAD `suite_id` + `key_epoch`. Swapping a block from a different file, a different offset within the same file, a different block type, or under a different key/suite fails AEAD verification.
- **Crypto-agility:** the on-disk record carries an explicit `suite_id` byte so a deployment can rotate to a new AEAD without rewriting old data, and so a future suite can be added by registry update alone.
- **Key rotation without rewrite:** the `key_epoch` byte indexes into a caller-managed key chain. Old blocks under epoch `N` stay readable as long as the corresponding key is still in the chain; new writes pick the latest epoch.
- **Single-pass decode:** all metadata needed to reconstruct the AAD lives at known fixed offsets inside the on-disk record. Decode reads the metadata frame once, computes AAD, then verifies + decrypts the body frame.
- **Zstd-skippable framing:** the on-disk record uses the [Zstandard skippable frame](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) magic range `0x184D2A50..0x184D2A5F`. A reader that doesn't understand encrypted blocks (e.g. a plain `zstd` CLI on the payload region) skips over them cleanly instead of corrupting on parse.

## 2. Non-goals

- **Confidentiality of the file's existence or size.** A passive observer of the SST file on disk still learns: the file's existence, total size, byte distribution (high entropy), and the count + sizes of blocks. AEAD protects per-block content, not file-level metadata.
- **Key-disclosure recovery.** If an attacker obtains the AEAD key for an epoch, every block written under that epoch is decipherable. Key management (rotation cadence, HSM integration, KMS plumbing) is the caller's responsibility; this spec defines only how block-level encryption uses the supplied key.
- **Side-channel resistance** in the AEAD primitives beyond what the chosen suite already provides. Constant-time AES-GCM requires AES-NI / NEON hardware acceleration on the deployment platform; ChaCha20-Poly1305 is constant-time on all platforms.
- **Authentication of the surrounding file structure.** The SFA TOC, the per-block `Header` checksum, and the per-file XXH3 in the manifest cover that surface; this spec only addresses what AEAD adds on top.
- **Block-level forward secrecy.** Compromise of the current epoch's key reveals all blocks under that epoch. Rotation gives forward secrecy for *future* writes, not past ones.

## 3. Threat model

| Threat | Defended? | Defending mechanism |
|---|---|---|
| Random bit flip in encrypted payload (bit-rot, bad sector) | Yes | AEAD tag mismatch on decrypt → `crate::Error::Decrypt(&'static str)` |
| Random bit flip in AAD-bound disk bytes (suite_id, key_epoch, block_type, dict_id, window_log) | Yes | AAD mismatch on decrypt. AEAD verification fails as a single opaque tag-mismatch event under `crate::Error::Decrypt`, NOT a per-field diagnostic: the cipher cannot tell whether the flipped byte was in the ciphertext or in one of the AAD-mirrored disk bytes. Per-field attribution would require typed decrypt errors, which is a follow-up tracked in #251. (The caller-supplied AAD fields `tree_id`, `table_id`, `block_offset` are not on disk, so they cannot be bit-flipped at rest; they can only fail to match if the reader supplies wrong context.) |
| **Block swap** within the same file (move block N's bytes to offset M, M ≠ N) | Yes | AAD carries caller-supplied `block_offset`; decrypt at the wrong offset fails because the reader's seek position doesn't match what the writer used. |
| **Block swap** across files in the same tree (move a block from file A to file B) | Yes | AAD carries caller-supplied `table_id` (derived from the SST file's path / table metadata); decrypt under the wrong table_id fails. |
| **Block swap** across trees (same encryption key reused, colliding per-tree `table_id` values) | Yes | AAD carries caller-supplied `tree_id` paired with `table_id`. `table_id` alone is per-tree in this codebase, so the pair `(tree_id, table_id)` gives the globally unique block identity. Substitution under the wrong tree_id fails. |
| **Block type swap** (relabel a Filter block as Data, e.g. to confuse a partial-decode path) | Yes | AAD carries `block_type`; decrypt under the wrong type fails |
| **Compression dictionary substitution** (decode under a different zstd dictionary to manipulate decompressed output) | Yes | AAD carries `dict_id`; structured-zstd's `FrameDecoder::expect_dict_id` cross-checks during inner-frame decode. Wiring lives in this repo's encoder/decoder issue #251; the matching enforcement on the structured-zstd side is tracked there as `S-ZSTD-T7`. |
| **Decompression bomb** (forged compressed payload that expands to TBs of plaintext) | Yes | AAD carries raw `window_log` (base-2 log of max window size, not the encoded RFC 8878 `Window_Descriptor` byte); structured-zstd's `FrameDecoder::expect_window_log` rejects frames whose decoded window exceeds the AAD-declared value |
| **Key epoch downgrade** (replay a block encrypted under epoch N as if it were epoch M ≠ N) | Yes | AAD carries `key_epoch`; the wrong key is selected and AEAD verification fails |
| **Suite downgrade** (relabel an AES-256-GCM block as ChaCha20-Poly1305 to coerce a different decrypt path) | Yes | AAD carries `suite_id`; the wrong primitive is selected and AEAD verification fails |
| **Replay across versions** (re-introduce an old block at its same offset after compaction has logically deleted it) | **Partial** | AAD does not include a per-block version / generation counter. The same `(table_id, block_offset, block_type, dict_id, key_epoch, suite_id)` tuple is valid for the same block content. Detection lives one layer up: the manifest's per-table XXH3 + per-file checksum catch a swap of an entire SST file, and the SFA TOC catches reorder within a file. Per-block replay within a single SST is **not defended at the AEAD layer.** |
| Key disclosure | **No** (non-goal, §2) | n/a |
| Brute-force AEAD key search | **No** (assumed infeasible for 256-bit keys with the chosen suites) | n/a |

## 4. Locked design decisions

| # | Decision | Value | Rationale |
|---|---|---|---|
| 4.1 | Outer framing | [Zstandard skippable frame](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) | Already standard, already understood by `zstd` CLI / libzstd readers, has dedicated magic range. Unknown-to-reader frames skip cleanly. |
| 4.2 | Magic allocation | `0x184D2A50` (metadata frame), `0x184D2A51` (body frame), `0x184D2A52` (ECC parity frame, owned by #254), `0x184D2A53..0x184D2A5F` reserved for future spec revisions | Stable magic split keeps the metadata size known-at-parse before the variable-length body; ECC magic is locked here so this spec and #254 can't drift. |
| 4.3 | Endianness, outer framing | Little-endian per RFC 8878 §3.1.1 | Mandatory by the framing spec we're inside of. |
| 4.4 | Endianness, AAD payload content | Big-endian | Crypto convention (RFC 8446 TLS, RFC 5116 AEAD framework). Avoids endianness ambiguity across deployments. |
| 4.5 | AEAD suite registry | `0x00` reserved (Plain, encryption disabled, not used in this format), `0x01` reserved (future stream cipher), `0x02 = AES-256-GCM`, `0x03 = ChaCha20-Poly1305`, `0x04..0xFF` reserved | Two initial suites cover the hardware-accelerated (AES-GCM via AES-NI/NEON) and the constant-time-on-any-CPU (ChaCha-Poly) cases. |
| 4.6 | Nonce field width on disk | Fixed 24 bytes | Largest nonce across supported suites (XChaCha20-Poly1305 if added later wants 24; ChaCha20-Poly1305 uses 12; AES-256-GCM uses 12). Suite-determined effective length; trailing bytes MUST be zero in the wire encoding for suites with shorter nonces. |
| 4.7 | Authentication tag field width | Fixed 16 bytes | Both initial suites (AES-256-GCM, ChaCha20-Poly1305) emit a 128-bit tag. Reserving 16 bytes covers any plausible future tag-truncation policy and keeps the metadata frame size constant. |
| 4.8 | `HeaderByte` layout | High nibble = format version (`0b0001` = v1 in this spec), low nibble reserved (MUST be zero on write, ignored on read until a future spec assigns it) | Single byte covers version + future spare. Forward extension can promote low-nibble bits to typed fields without breaking v1 readers. |
| 4.9 | Inner-frame integrity (dict / window) | Delegated to [structured-zstd](https://github.com/structured-world/structured-zstd) `FrameDecoder::expect_dict_id` / `expect_window_log` (the call-out is structured-zstd PR-F; this spec describes the contract, the implementation lives in S-ZSTD-T7). | Avoids re-validating zstd internals at the LSM layer. The AAD carries `dict_id` + `window_log` so the LSM decoder can pass them through. |
| 4.10 | Number of frames per block | Exactly **two required** frames: one MetadataFrame (magic `0x184D2A50`, fixed 49-byte payload) immediately followed by one BodyFrame (magic `0x184D2A51`, variable payload). Optionally followed by **zero or more** ECC parity frames (magic `0x184D2A52`, owned by #254) and other reserved-range frames. v1 readers MUST accept MetadataFrame + BodyFrame and MUST skip any trailing skippable frame in the reserved range `0x184D2A52..0x184D2A5F` without rejecting the block (RFC 8878 skippable-frame semantics). v1 readers MUST reject if MetadataFrame or BodyFrame is missing or out of order. | Splitting metadata out keeps it known-size and parseable without first probing the body. ECC and future extensions ride alongside as additional skippable frames; v1 readers ignore what they don't recognise and recover the block from the MetadataFrame + BodyFrame pair alone, so a v1 reader on an ECC-augmented block still works (it just doesn't get the parity-based repair path). |

## 5. Wire format

The on-disk layout for an AAD-bound encrypted block begins with two required consecutive [Zstandard skippable frames](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) and MAY be followed by zero or more optional skippable frames (e.g. an `EccFrame` owned by #254 at magic `0x184D2A52`). Per §4.10, v1 readers MUST accept and skip any trailing skippable frame in the reserved range `0x184D2A52..0x184D2A5F` without rejecting the block:

```text
MetadataFrame  (8-byte framing header + 49-byte payload   = 57 bytes total)
BodyFrame      (8-byte framing header + N-byte payload    = 8 + N bytes total)
```

Both framing headers are little-endian per RFC 8878 §3.1.1; both payloads are AAD-bound.

### 5.1 MetadataFrame

```text
Offset  Size  Field             Description
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicMetadata     0x50 0x2A 0x4D 0x18  (LE for 0x184D2A50)
4       4     PayloadLen        u32 LE = 49  (fixed for v1)
8       1     HeaderByte        High nibble = version (0b0001 for v1),
                                low nibble = 0 (reserved, MUST be zero)
9       1     KeyEpoch          Index into the caller's key chain
10      1     BlockType         0=Data 1=Index 2=Filter 3=Meta 4=RangeTombstone
11      1     SuiteID           AEAD primitive (see §4.5 registry)
12      4     DictID            u32 BE, zstd dictionary id (0 if no dict).
                                On disk because the dict id can vary per
                                block (different LSM levels under different
                                compression policies) and the decoder needs
                                it BEFORE attempting decryption to construct
                                the AAD.
16      1     WindowLog         Raw zstd window log: the base-2 logarithm
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
17     24     Nonce             Fixed 24 bytes; suite_id determines how many
                                are actually used (AES-256-GCM: first 12,
                                ChaCha20-Poly1305: first 12; trailing bytes
                                MUST be zero on write, MUST be ignored on
                                read for the suite's defined length)
41     16     AEADTag           AEAD authentication tag over the body
                                payload + the AAD (see §5.3)
═════════
Total  57 bytes on disk (8-byte framing header + 49-byte payload)
```

**On-disk minimalism.** The MetadataFrame on disk carries ONLY the fields the decoder needs *before* it can construct the AAD: the version byte, the key epoch (so the right key is selected), the block type (mirrors the existing `Header` pattern), the AEAD suite id (so the right primitive is selected), the compression-context fields (`DictID` + `WindowLog`, which can vary per block and which the decoder must know to bind the AAD before any decompression / decryption work), the nonce, and the AEAD tag. Three further identifiers (`TreeID`, `TableID`, `BlockOffset`) participate in the AAD but are **NOT** stored on disk: they are caller-supplied from the read context (the owning `Tree`, the SST file's per-tree `TableId`, and the read cursor's byte position). See §5.3.

**Why not store them on disk.** Industry-standard LSMs (RocksDB / LevelDB / Pebble) put zero identity bytes in per-block headers: a per-block trailer is 5 bytes total (1 byte compression + 4 byte checksum). Block identity is purely positional: the SST footer points at the index, the index gives `BlockHandle { offset, size }`, and the file's path/manifest gives the table id. The same model applies here: spending 24 bytes per block on `TreeID + TableID + BlockOffset` would duplicate context the caller already has at decrypt time, and it would be cryptographically *weaker* than the AAD binding it would replace (a tamperer could just patch the on-disk bytes; tampering with the AAD-bound values is infeasible). Orphan-block forensics is addressed at the per-file layer (the META blocks introduced in #295 carry the file-level identity), not by fattening every block header.

### 5.2 BodyFrame

```text
Offset  Size  Field             Description
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicBody         0x51 0x2A 0x4D 0x18  (LE for 0x184D2A51)
4       4     PayloadLen        u32 LE, length of EncryptedBody in bytes
8       N     EncryptedBody     N = PayloadLen. The AEAD ciphertext of:
                                <whatever-the-block-was-before-encryption,
                                 i.e. the same byte sequence the plaintext
                                 path would have written>
═════════
Total  8 + N bytes on disk
```

### 5.3 AAD construction

The AEAD's Additional Authenticated Data is constructed by the writer immediately before encrypting the body, and reconstructed by the reader immediately before decrypting. AAD is **NEVER** written to disk; both sides construct it from a mix of disk-mirrored fields (which the decoder has just read from the MetadataFrame) and caller-supplied fields (which the decoder knows from its read context: the owning `Tree`, the SST file's path-derived `TableId`, the seek offset):

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
8       8     TreeID            u64 BE, caller-supplied from the owning
                                Tree's id (`AbstractTree::id()`, the
                                accessor method on the trait that both
                                `Tree` and `BlobTree` implement).
                                NOT on disk:
                                a process knows which tree it opened. `0`
                                is the allowed-zero default at call sites
                                that haven't plumbed tree_id yet, falling
                                back to per-tree encryption-provider key
                                isolation as the substitute defence (see
                                `BlockIdentity` module docs in
                                `src/table/block/identity.rs`).
16      8     TableID           u64 BE, caller-supplied from the SST
                                file's per-tree `TableId` (derived from
                                the file path / table metadata). NOT on
                                disk: the reader already knows which file
                                it opened. Pair with TreeID gives the
                                globally unique block identity that
                                defeats cross-tree substitution.
24      8     BlockOffset       u64 BE, caller-supplied from the read or
                                write cursor's byte position within the
                                SST file. NOT on disk: the reader already
                                knows its own seek offset. Binds the
                                block to its position to defeat
                                same-file relocations.
32      4     DictID            u32 BE, mirror of MetadataFrame offset 12
                                (disk)
36      1     WindowLog         Mirror of MetadataFrame offset 16 (disk)
═════════
Total  37 bytes (NEVER written to disk, passed to AEAD as AAD only)
```

**Disk vs caller-supplied: the contract.** Fields marked "mirror of MetadataFrame offset X (disk)" are read from the on-disk MetadataFrame the decoder has just parsed. Fields marked "caller-supplied" are passed in by the calling code from its own context (`BlockIdentity` struct in `src/table/block/identity.rs`). The writer feeds *the same values* from its own context into AAD construction. The AEAD's authentication tag binds all 37 bytes together: an attacker who modifies any disk byte, or who relocates a block to a different file / different offset, produces an AAD that doesn't match the one the AEAD was sealed under, and decryption fails.

The `Nonce` and `AEADTag` fields are **not** part of the AAD, they're the AEAD's nonce and tag inputs, respectively.

The `MagicBody` and `PayloadLen` from BodyFrame are also **not** part of the AAD. RFC 8878 skippable framing carries no integrity check (a non-conformant reader is expected to *skip* unknown frames, not validate them), so a decoder MUST NOT rely on framing for authentication. Instead the decoder MUST enforce these structural invariants explicitly before doing any further work:

- MetadataFrame `MagicMetadata` equals `0x184D2A50` (LE bytes `50 2A 4D 18`). If not, treat as a non-AAD-bound block and refuse to decrypt.
- MetadataFrame `PayloadLen` equals 49 exactly. Any other value is malformed and MUST be rejected without reading the body frame (no AAD can be constructed, so AEAD cannot bind context).
- BodyFrame `MagicBody` equals `0x184D2A51` (LE bytes `51 2A 4D 18`). If not, reject.
- BodyFrame `PayloadLen` is in the range `[1, 256 MiB + max_overhead]`, where `256 MiB` is the plaintext upper bound on a single block's on-disk data segment (mirrors the public constant the LSM scrub path enforces) and `max_overhead` is the value reported by the active `EncryptionProvider::max_overhead()` (zero when encryption is disabled or for plaintext blocks). A larger value means either a forged TOC or a header bit-flip and MUST be rejected before allocating the read buffer.

These checks are not AEAD-authenticated, but they bound the attack surface so that any bypass attempt either (a) fails the structural check above, or (b) reaches the AEAD and fails AAD verification on the metadata-mirror fields.

### 5.4 ABNF grammar

[RFC 5234](https://datatracker.ietf.org/doc/html/rfc5234) syntax:

```abnf
;; A single AAD-bound encrypted block on disk begins with the
;; required pair (metadata-frame, body-frame) and may carry zero or
;; more optional trailing skippable frames (e.g. an ECC parity frame
;; per #254 at magic 0x184D2A52). v1 readers MUST skip optional
;; frames they don't recognise (per RFC 8878 skippable-frame
;; semantics) and MUST NOT reject the block on their presence.
encrypted-block   = metadata-frame body-frame *optional-frame

optional-frame    = optional-magic optional-payload-len optional-payload
optional-magic    = %x52.2A.4D.18                  ; 0x184D2A52 (EccFrame, #254)
                  / %x53.2A.4D.18                  ; 0x184D2A53 (reserved, future)
                  / %x54.2A.4D.18                  ; 0x184D2A54 (reserved, future)
                  / %x55.2A.4D.18 / %x56.2A.4D.18  ; 0x184D2A55..0x184D2A5F (reserved)
                  / %x57.2A.4D.18 / %x58.2A.4D.18
                  / %x59.2A.4D.18 / %x5A.2A.4D.18
                  / %x5B.2A.4D.18 / %x5C.2A.4D.18
                  / %x5D.2A.4D.18 / %x5E.2A.4D.18
                  / %x5F.2A.4D.18
optional-payload-len = 4OCTET                       ; u32 LE
optional-payload  = *OCTET                          ; opaque bytes, length
                                                   ; constraint enforced
                                                   ; outside the grammar
                                                   ; (see prose below)

metadata-frame    = metadata-magic metadata-payload-len metadata-payload
metadata-magic    = %x50.2A.4D.18                  ; 0x184D2A50 LE
metadata-payload-len = %x31.00.00.00               ; u32 LE = 49
metadata-payload  = header-byte                    ; 1B
                    key-epoch                      ; 1B
                    block-type                     ; 1B
                    suite-id                       ; 1B
                    dict-id                        ; 4B BE
                    window-log                     ; 1B
                    nonce                          ; 24B
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
dict-id           = 4OCTET                         ; u32 BE
window-log        = %x00 / %x0A-1F                 ; 0 = no zstd, 10..=31 = raw log2 window
nonce             = 24OCTET
aead-tag          = 16OCTET

;; Core terminal, RFC 5234 itself does not define OCTET in its core
;; rules (only ALPHA / DIGIT / CRLF / etc.), so the grammar defines
;; it explicitly. The `NOCTET` notation (e.g. `4OCTET`, `8OCTET`) is
;; the standard ABNF repetition shorthand for "exactly N occurrences
;; of OCTET" per RFC 5234 §3.6.
OCTET             = %x00-FF                        ; any 8-bit byte
```

**Length constraint on `optional-payload` (cannot be expressed in ABNF).** ABNF's `*OCTET` matches an unbounded run of bytes, which doesn't capture the contract that the payload is **exactly** the number of bytes declared by the preceding `optional-payload-len` u32 LE. Decoders MUST:

1. Read the 4-byte `optional-payload-len` field into a `u32` (LE).
2. Read **exactly** that many bytes for `optional-payload`.
3. Stop consuming bytes for this optional-frame at that point; the next byte is either the start of another optional-frame's magic or end-of-block.

A reader that consumes more or fewer bytes than the declared length MUST treat the file as malformed. The same applies to `metadata-payload` (length 49) and `encrypted-body` (length declared by `body-payload-len`): these are also `*OCTET` in the grammar but constrained by their preceding length fields in the same way.

## 6. Magic allocation

The `0x184D2A50..0x184D2A5F` range is reserved by [RFC 8878 §3.1.2](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.2) for user-defined skippable frames. This spec claims a subset of that range:

| Magic | Frame | Status |
|---|---|---|
| `0x184D2A50` | MetadataFrame v1 (this spec) | Locked |
| `0x184D2A51` | BodyFrame v1 (this spec) | Locked |
| `0x184D2A52` | EccFrame (ECC parity, owned by #254 - kept consistent with that issue's ABNF) | Locked |
| `0x184D2A53` | (Reserved for spec v2 metadata) | Reserved |
| `0x184D2A54` | (Reserved for spec v2 body) | Reserved |
| `0x184D2A55..0x184D2A5F` | Reserved for future use | Reserved |

Implementations MUST reject blocks whose first frame magic is not `0x184D2A50` (current writer always emits this magic). Implementations MAY recognise reserved magics in a future spec revision; until then a reserved magic is treated as an unknown-format error.

## 7. AEAD suite registry

| SuiteID | Name | Key size | Nonce (effective) | Tag |
|---|---|---|---|---|
| `0x00` | Reserved (Plain, not used in this format) | n/a | n/a | n/a |
| `0x01` | Reserved | n/a | n/a | n/a |
| `0x02` | AES-256-GCM ([RFC 5116](https://datatracker.ietf.org/doc/html/rfc5116) + [NIST SP 800-38D](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf)) | 32 B | 12 B | 16 B |
| `0x03` | ChaCha20-Poly1305 ([RFC 8439](https://datatracker.ietf.org/doc/html/rfc8439)) | 32 B | 12 B | 16 B |
| `0x04..0xFF` | Reserved | n/a | n/a | n/a |

The on-disk nonce field is fixed 24 bytes (§4.6). For suites with shorter effective nonces, only the first N bytes carry the nonce material; the remaining `24 - N` bytes MUST be zero on write and MUST be ignored on read.

Adding a new suite requires:
1. Allocating a SuiteID byte in the registry (above table).
2. Specifying key size, effective nonce length, and tag length.
3. Updating the conformance test suite (#253) with at least one test vector per block type.

A new suite does NOT require a format version bump, readers select the suite from the `SuiteID` byte at decode time. Old blocks under the old suite remain readable as long as the implementation links the corresponding AEAD primitive.

## 8. Security properties

Per-attack mapping of which AAD-bound field defeats which threat:

| Attack | Defending field(s) | How it defeats the attack |
|---|---|---|
| Bit flip in encrypted payload | (AEAD tag) | Standard AEAD: any payload modification invalidates the tag. |
| Bit flip in MetadataFrame fields (except Nonce / AEADTag) | All AAD-bound fields | The flipped byte ends up in the AAD; decryption derives a different tag and fails. |
| Bit flip in Nonce | (AEAD construction) | Different nonce → AEAD verifies against a tag computed under a different keystream/counter → fails. |
| Bit flip in AEADTag | (AEAD construction) | Standard AEAD: the on-disk tag doesn't match the recomputed one → fails. |
| Block swap within the same file | `BlockOffset` (caller-supplied) | The block's bytes are valid but at a different offset; the reader's seek position feeds a different BlockOffset into AAD construction; AEAD verification fails. |
| Block swap across files in the same tree | `TableID` (caller-supplied from file path) | Same as above for cross-file moves; the reader's file-path-derived TableID doesn't match what the writer used. |
| Block swap across trees | `TreeID` paired with `TableID` (both caller-supplied) | The pair `(tree_id, table_id)` is the globally unique block identity; substitution under the wrong tree_id fails AEAD verification. |
| Block type relabel (Filter → Data) | `BlockType` | The bytes are valid but the type byte differs → AAD mismatch. |
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
| DictID (BE) | `00000000` (= 0, no dict) |
| WindowLog | `15` (= 21, 2 MiB window) |
| Nonce (24 B) | `000102030405060708090a0b 0000000000000000 00000000` (first 12 used; remaining 12 MUST be zero) |
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
| TreeID (BE) | `00000000 00000007` (= 7) | The owning tree's id |
| TableID (BE) | `00000000 00002a30` (= 10800) | The SST file's per-tree TableId |
| BlockOffset (BE) | `00000000 00000000` (= 0) | The block's offset in the file (first block) |

**AAD (37 B, NEVER on disk):** `502a4d18 10 01 00 02 0000000000000007 0000000000002a30 0000000000000000 00000000 15` (MagicMetadata | HeaderByte=0x10 [v1, low nibble reserved] | KeyEpoch=0x01 | BlockType=0x00 | SuiteID=0x02 | TreeID BE | TableID BE | BlockOffset BE | DictID BE | WindowLog=0x15)

**Expected on-disk size:** 78 B total = 57 (MetadataFrame = 8 framing + 49 payload) + 8 (BodyFrame framing header) + 13 (EncryptedBody). For AES-256-GCM and ChaCha20-Poly1305, `ciphertext_len == plaintext_len` because the tag is stored in MetadataFrame, not appended to the ciphertext.

The 78-byte on-disk output for this vector ships as fixed binary test data with #253 and becomes the canonical wire reference once that PR lands. The test asserts byte equality between the AEAD library's output and the published bytes.

### Vector 2: Index block, ChaCha20-Poly1305, no dict, no zstd

Same key, KeyEpoch=01, plaintext body `00 01 02 03 04 05 06 07` (8 bytes). BlockType=`01`, SuiteID=`03`. DictID=0. WindowLog=`00` (no zstd compression on this block; window enforcement disabled). Nonce = first 12 bytes `0c0d0e0f10111213 14151617`, trailing 12 zero. Caller-supplied AAD context: TreeID=7, TableID=`00000000 00002a30`, BlockOffset=`00000000 00010000` (= 65536).

### Vector 3: Data block, AES-256-GCM, with dict

Key as above, KeyEpoch=01, BlockType=`00`, SuiteID=`02`, DictID=`deadbeef`, WindowLog=`15`. Plaintext body 32 bytes of `aa`. Caller-supplied AAD context: TreeID=7, TableID=`00000000 00002a30`, BlockOffset=`00000000 00020000`. The AAD now carries the non-zero DictID, exercising the dict-substitution defence.

### Vector 4: Negative, window-bomb (rejection)

Construct a block whose inner zstd frame's `Window_Descriptor` byte (encoded per RFC 8878 §3.1.1.1.2) decodes to a 1 GiB window, but the AAD-bound `WindowLog` field declares 21 (raw log2, = 2 MiB). A conformant decoder MUST reject this block. The reject path is split across two issues by repo: this repo's encoder/decoder (issue #251) wires the AAD-bound `WindowLog` into the inner-validator call, and the structured-zstd side (tracked as `S-ZSTD-T7` in [structured-world/structured-zstd](https://github.com/structured-world/structured-zstd)) implements `FrameDecoder::expect_window_log` to decode the frame's descriptor byte and compare the decoded log against the AAD-bound limit. Error variant: `crate::Error::Decrypt(_)` with an implementation-defined reason. The exact reason string is NOT part of this spec (it varies by suite, see `src/encryption.rs` for the current AES-256-GCM string) and may be replaced by a typed decrypt-error variant when #251 introduces one; conformance tests assert on the variant family, not on the static string.

### Vector 5: Negative, key-epoch mismatch (rejection)

Encrypt a block under KeyEpoch=`01`. Tamper the on-disk `KeyEpoch` byte to `02`. The reader selects key `02` from the chain (different from the actual encryption key), AEAD verification fails. Error variant: `crate::Error::Decrypt(_)` (standard AEAD tag-mismatch path). The reason string is implementation-defined (see `src/encryption.rs` for the current per-suite strings) and may move to a typed variant under #251; conformance tests assert on the variant family.

## 10. Worked hex-dump example

A minimum-size Data block (single-byte plaintext = `41`, "A") encrypted under AES-256-GCM with all-zero key, KeyEpoch=`01`, DictID=0, WindowLog=`15`, Nonce = first 12 bytes `00..0b`. Caller context (NOT on disk): TreeID=0, TableID=0, BlockOffset=0.

```text
;; MetadataFrame (57 bytes = 8-byte framing + 49-byte payload)
0000: 50 2a 4d 18         ; MagicMetadata (0x184D2A50 LE)
0004: 31 00 00 00         ; PayloadLen = 49 (u32 LE)
0008: 10                  ; HeaderByte: version=0x1, low nibble reserved=0
0009: 01                  ; KeyEpoch
000a: 00                  ; BlockType = Data
000b: 02                  ; SuiteID = AES-256-GCM
000c: 00 00 00 00         ; DictID = 0 (u32 BE)
0010: 15                  ; WindowLog = 21
0011: 00 01 02 03 04 05 06 07   ; Nonce bytes 0..7
0019: 08 09 0a 0b         ; Nonce bytes 8..11 (AES-GCM uses first 12)
001d: 00 00 00 00 00 00 00 00 00 00 00 00   ; Nonce bytes 12..23 (MUST be zero)
0029: <16 bytes AEADTag>  ; depends on the AEAD library output, not literal

;; BodyFrame (8 + 1 = 9 bytes)
0039: 51 2a 4d 18         ; MagicBody (0x184D2A51 LE)
003d: 01 00 00 00         ; PayloadLen = 1 (u32 LE)
0041: <1 byte ciphertext> ; AES-GCM ciphertext of "A" under the AAD below

;; AAD (37 bytes; NEVER written to disk, input to AEAD only)
     50 2a 4d 18          ; MagicMetadata
     10                   ; HeaderByte         (mirror of disk byte 0008)
     01                   ; KeyEpoch           (mirror of disk byte 0009)
     00                   ; BlockType          (mirror of disk byte 000a)
     02                   ; SuiteID            (mirror of disk byte 000b)
     00 00 00 00 00 00 00 00   ; TreeID BE      (caller-supplied, not on disk)
     00 00 00 00 00 00 00 00   ; TableID BE     (caller-supplied, not on disk)
     00 00 00 00 00 00 00 00   ; BlockOffset BE (caller-supplied, not on disk)
     00 00 00 00          ; DictID BE          (mirror of disk bytes 000c-000f)
     15                   ; WindowLog
```

Total on-disk size: 66 bytes (57 metadata + 9 body). The AEADTag and ciphertext bytes are generated by the AEAD library and not literal in this example, the conformance harness in #253 computes them and asserts exact byte equality.

## 11. Implementation hand-off

| Component | Tracking issue | Notes |
|---|---|---|
| Encoder / decoder | #251 | Reads SuiteID, selects primitive, builds AAD per §5.3. AES-256-GCM goes through the existing [`aes-gcm`](https://crates.io/crates/aes-gcm) dependency. ChaCha20-Poly1305 lands behind its own SuiteID (`0x03`) when #251 adds the [`chacha20poly1305`](https://crates.io/crates/chacha20poly1305) crate as a new dependency; until then the encoder rejects writes with SuiteID `0x03`. |
| Inner-frame validation | lsm-tree wiring: #251 (this repo). Structured-zstd enforcement: `S-ZSTD-T7` in [structured-world/structured-zstd](https://github.com/structured-world/structured-zstd). | This repo's encoder/decoder (#251) passes the AAD-bound `dict_id` / `window_log` into `FrameDecoder::expect_dict_id` / `expect_window_log` before letting decompression proceed. Those `expect_*` hooks are implemented on the structured-zstd side under tracker `S-ZSTD-T7`. |
| Conformance test suite | #253 | One test per row in §9 (vectors 1-5) plus regressions for each row in §8 (attack → defending field). |
| ECC layer (outer Reed-Solomon parity) | #254 | Uses frame magic `0x184D2A52` (locked here in §6, matching #254's `EccFrame` ABNF) for parity blocks; spec extension lands with that PR. |
| Lazy block-precise repair | #255 (this repo) | Uses the inner-validator error categories produced by the structured-zstd `S-ZSTD-T7` work to attempt single-block repair before reporting whole-SST corruption. |
| Forensics CLI | #256 | Reads MetadataFrame in isolation to dump per-block structure (table_id, offset, type, suite, epoch, dict, window) without requiring the key. |
| Partial decode for range queries | #257 | Uses structured-zstd's block-subset API to decode only the matching key range of a compressed block; the AEAD verification covers the entire body, so partial-decode only saves decompression work, not verification work. |

## 12. Open questions

These intentionally remain open for follow-up spec revisions; resolving them does not require breaking the v1 wire format because the HeaderByte's low nibble (§4.8) and the reserved magic range (§6) leave room for forward-compatible extensions.

- **Per-block version / generation counter** to defend against block-level replay within a single SST (currently §3 documents this as a known gap). Adding a 4-byte generation counter to the AAD would close the gap at the cost of 4 bytes per block; deferred until a deployment incident motivates it.
- **AEAD nonce derivation policy.** Random 96-bit nonces under AES-GCM hit the birthday bound at ~2³² distinct nonces under the same key; a deployment that ever approaches 4 billion writes per key needs an explicit counter-mode nonce policy. The spec leaves nonce generation to the writer; a future revision may pin a recommended policy.
- **Suite registry expansion.** AES-256-OCB3 or AEGIS-256 are plausible additions if a deployment needs higher throughput; would slot into SuiteIDs `0x04+` without a format bump.
- **Header-only verify mode.** A reader that only needs to confirm block integrity (e.g. for offline scrubbing) currently still has to decrypt the body. A future "header-MAC only" mode could expose this, would need its own AAD-only MAC field added to a v2 MetadataFrame.
