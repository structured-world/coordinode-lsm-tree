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
| Random bit flip in AAD-bound metadata bytes (suite_id, key_epoch, table_id, block_offset, dict_id, window_log) | Yes | AAD mismatch on decrypt, same `Error::Decrypt` surface, distinguishable by the static reason string in the per-block error context |
| **Block swap** within the same file (move block N's bytes to offset M, M ≠ N) | Yes | AAD carries `block_offset`; decrypt at the wrong offset fails |
| **Block swap** across files (move a block from file A to file B) | Yes | AAD carries `table_id`; decrypt under the wrong table_id fails |
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
| 4.10 | Number of frames per block | Exactly **two required** frames: one MetadataFrame (magic `0x184D2A50`, fixed 65-byte payload) immediately followed by one BodyFrame (magic `0x184D2A51`, variable payload). Optionally followed by **zero or more** ECC parity frames (magic `0x184D2A52`, owned by #254) and other reserved-range frames. v1 readers MUST accept MetadataFrame + BodyFrame and MUST skip any trailing skippable frame in the reserved range `0x184D2A52..0x184D2A5F` without rejecting the block (RFC 8878 skippable-frame semantics). v1 readers MUST reject if MetadataFrame or BodyFrame is missing or out of order. | Splitting metadata out keeps it known-size and parseable without first probing the body. ECC and future extensions ride alongside as additional skippable frames; v1 readers ignore what they don't recognise and recover the block from the MetadataFrame + BodyFrame pair alone, so a v1 reader on an ECC-augmented block still works (it just doesn't get the parity-based repair path). |

## 5. Wire format

The on-disk layout for an AAD-bound encrypted block begins with two required consecutive [Zstandard skippable frames](https://datatracker.ietf.org/doc/html/rfc8878#section-3.1.1) and MAY be followed by zero or more optional skippable frames (e.g. an `EccFrame` owned by #254 at magic `0x184D2A52`). Per §4.10, v1 readers MUST accept and skip any trailing skippable frame in the reserved range `0x184D2A52..0x184D2A5F` without rejecting the block:

```text
MetadataFrame  (8-byte framing header + 65-byte payload   = 73 bytes total)
BodyFrame      (8-byte framing header + N-byte payload    = 8 + N bytes total)
```

Both framing headers are little-endian per RFC 8878 §3.1.1; both payloads are AAD-bound.

### 5.1 MetadataFrame

```text
Offset  Size  Field             Description
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicMetadata     0x50 0x2A 0x4D 0x18  (LE for 0x184D2A50)
4       4     PayloadLen        u32 LE = 65  (fixed for v1)
8       1     HeaderByte        High nibble = version (0b0001 for v1),
                                low nibble = 0 (reserved, MUST be zero)
9       1     KeyEpoch          Index into the caller's key chain
10      1     BlockType         0=Data 1=Index 2=Filter 3=Meta 4=RangeTombstone
11      1     SuiteID           AEAD primitive (see §4.5 registry)
12      8     TableID           u64 BE, the owning SST's table id
20      8     BlockOffset       u64 BE, the block's byte offset within the
                                file (used by the AAD to bind the block to
                                its position; reader knows its own offset
                                because the caller passed it)
28      4     DictID            u32 BE, zstd dictionary id (0 if no dict)
32      1     WindowLog         Raw zstd window log: the base-2 logarithm
                                of the max decompression window size, in
                                bytes (so 21 means 2^21 = 2 MiB). NOT the
                                encoded `Window_Descriptor` byte from
                                RFC 8878 §3.1.1.1.2 (which packs an
                                exponent and a mantissa); the spec stores
                                the raw log because that's what
                                structured-zstd's `FrameDecoder::
                                expect_window_log` consumes. Valid range
                                10..=31 per RFC 8878 §3.1.1.1.2 (the
                                decoded equivalent of the descriptor
                                byte's allowed values).
33     24     Nonce             Fixed 24 bytes; suite_id determines how many
                                are actually used (AES-256-GCM: first 12,
                                ChaCha20-Poly1305: first 12; trailing bytes
                                MUST be zero on write, MUST be ignored on
                                read for the suite's defined length)
57     16     AEADTag           AEAD authentication tag over the body
                                payload + the AAD (see §5.3)
═════════
Total  73 bytes on disk
```

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

The AEAD's Additional Authenticated Data is constructed by the writer immediately before encrypting the body, and reconstructed by the reader immediately before decrypting:

```text
Offset  Size  Field             Source
══════  ════  ═══════════════   ═══════════════════════════════════════════════
0       4     MagicMetadata     The literal four bytes 0x50 0x2A 0x4D 0x18
                                (binds the AAD to this format identity; an
                                attacker that lifts the metadata bytes into
                                a future format with a different magic gets
                                a different AAD and verification fails)
4       1     HeaderByte        Mirror of MetadataFrame offset 8
5       1     KeyEpoch          Mirror of MetadataFrame offset 9
6       1     BlockType         Mirror of MetadataFrame offset 10
7       1     SuiteID           Mirror of MetadataFrame offset 11
8       8     TableID           u64 BE, mirror of MetadataFrame offset 12
16      8     BlockOffset       u64 BE, mirror of MetadataFrame offset 20
24      4     DictID            u32 BE, mirror of MetadataFrame offset 28
28      1     WindowLog         Mirror of MetadataFrame offset 32
═════════
Total  29 bytes (NOT written to disk, passed to AEAD as AAD only)
```

The `Nonce` and `AEADTag` fields are **not** part of the AAD, they're the AEAD's nonce and tag inputs, respectively.

The `MagicBody` and `PayloadLen` from BodyFrame are also **not** part of the AAD. RFC 8878 skippable framing carries no integrity check (a non-conformant reader is expected to *skip* unknown frames, not validate them), so a decoder MUST NOT rely on framing for authentication. Instead the decoder MUST enforce these structural invariants explicitly before doing any further work:

- MetadataFrame `MagicMetadata` equals `0x184D2A50` (LE bytes `50 2A 4D 18`). If not, treat as a non-AAD-bound block and refuse to decrypt.
- MetadataFrame `PayloadLen` equals 65 exactly. Any other value is malformed and MUST be rejected without reading the body frame (no AAD can be constructed, so AEAD cannot bind context).
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

optional-frame    = optional-magic optional-payload-len *OCTET
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

metadata-frame    = metadata-magic metadata-payload-len metadata-payload
metadata-magic    = %x50.2A.4D.18                  ; 0x184D2A50 LE
metadata-payload-len = %x41.00.00.00               ; u32 LE = 65
metadata-payload  = header-byte                    ; 1B
                    key-epoch                      ; 1B
                    block-type                     ; 1B
                    suite-id                       ; 1B
                    table-id                       ; 8B BE
                    block-offset                   ; 8B BE
                    dict-id                        ; 4B BE
                    window-log                     ; 1B
                    nonce                          ; 24B
                    aead-tag                       ; 16B

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
table-id          = 8OCTET                         ; u64 BE
block-offset      = 8OCTET                         ; u64 BE
dict-id           = 4OCTET                         ; u32 BE
window-log        = OCTET
nonce             = 24OCTET
aead-tag          = 16OCTET
```

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
| Block swap within the same file | `BlockOffset` | The block's bytes are valid but at a different offset; AAD mismatch → fails. |
| Block swap across files | `TableID` | Same as above but for cross-file moves. |
| Block type relabel (Filter → Data) | `BlockType` | The bytes are valid but the type byte differs → AAD mismatch. |
| Compression dict substitution | `DictID` (lsm-tree wiring: #251; structured-zstd enforcement: S-ZSTD-T7) | LSM-side: AAD binds dict_id, so reading under a different dict fails AAD. Inside-frame: structured-zstd's `expect_dict_id` re-checks the dict id encoded in the zstd frame header. |
| Decompression bomb (forged window) | `WindowLog` (lsm-tree wiring: #251; structured-zstd enforcement: S-ZSTD-T7) | AAD binds raw `window_log` (base-2 log of max window in bytes). structured-zstd's `expect_window_log` decodes each frame's `Window_Descriptor` byte to its raw log equivalent and rejects frames whose decoded value exceeds the AAD-bound limit. |
| Key epoch downgrade | `KeyEpoch` | Selecting the wrong key (because the wrong epoch is declared) yields the wrong AEAD primitive instance → fails. |
| Suite downgrade | `SuiteID` | Selecting the wrong primitive yields the wrong tag → fails. |
| AAD format substitution (lifting metadata bytes into a future format with a different magic) | `MagicMetadata` (first 4 bytes of AAD) | The literal magic bytes are part of the AAD; a different format with a different magic produces a different AAD → fails. |

## 9. Reference test vectors

These vectors are normative. An implementation that produces or accepts byte sequences differing from these for the listed inputs is non-conformant.

All `key` / `value` / `nonce` byte sequences are shown hex, big-endian to small-endian for byte 0 to byte N (i.e. byte 0 is leftmost). Vectors use fixed nonces for reproducibility, production writers MUST generate nonces per [RFC 5116 §3.2](https://datatracker.ietf.org/doc/html/rfc5116#section-3.2) (random 96-bit / counter, not repeated under the same key).

### Vector 1: Data block, AES-256-GCM, no dict

| Field | Value |
|---|---|
| Key (32 B) | `00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000` |
| KeyEpoch | `01` |
| BlockType | `00` (Data) |
| SuiteID | `02` (AES-256-GCM) |
| TableID (BE) | `00000000 00002a30` (= 10800) |
| BlockOffset (BE) | `00000000 00000000` (= 0) |
| DictID (BE) | `00000000` (= 0, no dict) |
| WindowLog | `15` (= 21, 2 MiB window) |
| Nonce (24 B) | `000102030405060708090a0b 0000000000000000 00000000` (first 12 used) |
| Plaintext body | `48656c6c 6f2c2057 6f726c64 21` ("Hello, World!", 13 bytes) |

**AAD (29 B):** `502a4d18 10 01 00 02 0000000000002a30 0000000000000000 00000000 15` (MagicMetadata | HeaderByte=0x10 [v1, low nibble reserved] | KeyEpoch=0x01 | BlockType=0x00 | SuiteID=0x02 | TableID BE | BlockOffset BE | DictID BE | WindowLog=0x15)

**Expected on-disk size:** 94 B total = 73 (MetadataFrame) + 8 (BodyFrame framing header) + 13 (EncryptedBody). For AES-256-GCM and ChaCha20-Poly1305, `ciphertext_len == plaintext_len` because the tag is stored in MetadataFrame, not appended to the ciphertext.

Conformance: a test in #253 will encrypt the above inputs under the deterministic nonce and assert the resulting on-disk byte sequence is exactly the encoded MetadataFrame || BodyFrame. The actual hex (94 bytes including ciphertext and tag) is generated by the conformance harness, not hand-written here, hand-transcribing AES-GCM output is error-prone and the test asserts byte equality against the AEAD library's output.

### Vector 2: Index block, ChaCha20-Poly1305, no dict

Same key, KeyEpoch=01, plaintext body `00 01 02 03 04 05 06 07` (8 bytes). BlockType=`01`, SuiteID=`03`. Same TableID, BlockOffset=`00000000 00010000` (= 65536). DictID=0, WindowLog=`14`. Nonce = first 12 bytes `0c0d0e0f10111213 14151617`, trailing 12 zero.

### Vector 3: Data block, AES-256-GCM, with dict

Key as above, KeyEpoch=01, BlockType=`00`, SuiteID=`02`, TableID=`00000000 00002a30`, BlockOffset=`00000000 00020000`, DictID=`deadbeef`, WindowLog=`15`. Plaintext body 32 bytes of `aa`. The AAD now carries the non-zero DictID, exercising the dict-substitution defence.

### Vector 4: Negative, window-bomb (rejection)

Construct a block whose inner zstd frame's `Window_Descriptor` byte (encoded per RFC 8878 §3.1.1.1.2) decodes to a 1 GiB window, but the AAD-bound `WindowLog` field declares 21 (raw log2, = 2 MiB). A conformant decoder MUST reject this block. The reject path is split across two issues by repo: this repo's encoder/decoder (issue #251) wires the AAD-bound `WindowLog` into the inner-validator call, and the structured-zstd side (tracked as `S-ZSTD-T7` in [structured-world/structured-zstd](https://github.com/structured-world/structured-zstd)) implements `FrameDecoder::expect_window_log` to decode the frame's descriptor byte and compare the decoded log against the AAD-bound limit. Error variant: `crate::Error::Decrypt("window log exceeds AAD-bound limit")` (or equivalent reason string) - the inner-validator rejection surfaces through the same `Decrypt` enum variant as an AEAD tag mismatch, with the reason string distinguishing inner-frame rejection from AAD/tag failure.

### Vector 5: Negative, key-epoch mismatch (rejection)

Encrypt a block under KeyEpoch=`01`. Tamper the on-disk `KeyEpoch` byte to `02`. The reader selects key `02` from the chain (different from the actual encryption key), AEAD verification fails. Error variant: `crate::Error::Decrypt("AEAD tag mismatch")` (standard AEAD tag-mismatch path).

## 10. Worked hex-dump example

A minimum-size Data block (single-byte plaintext = `41`, "A") encrypted under AES-256-GCM with all-zero key, all-zero TableID, BlockOffset=0, DictID=0, WindowLog=`15`, KeyEpoch=`01`, Nonce = first 12 bytes `00..0b`:

```text
;; MetadataFrame (73 bytes)
0000: 50 2a 4d 18         ; MagicMetadata (0x184D2A50 LE)
0004: 41 00 00 00         ; PayloadLen = 65 (u32 LE)
0008: 10                  ; HeaderByte: version=0x1, low nibble reserved=0
0009: 01                  ; KeyEpoch
000a: 00                  ; BlockType = Data
000b: 02                  ; SuiteID = AES-256-GCM
000c: 00 00 00 00 00 00 00 00   ; TableID = 0 (u64 BE)
0014: 00 00 00 00 00 00 00 00   ; BlockOffset = 0 (u64 BE)
001c: 00 00 00 00         ; DictID = 0 (u32 BE)
0020: 15                  ; WindowLog = 21
0021: 00 01 02 03 04 05 06 07   ; Nonce bytes 0..7
0029: 08 09 0a 0b         ; Nonce bytes 8..11 (AES-GCM uses first 12)
002d: 00 00 00 00 00 00 00 00 00 00 00 00   ; Nonce bytes 12..23 (MUST be zero)
0039: <16 bytes AEADTag>  ; depends on the AEAD library output, not literal

;; BodyFrame (8 + 1 = 9 bytes)
0049: 51 2a 4d 18         ; MagicBody (0x184D2A51 LE)
004d: 01 00 00 00         ; PayloadLen = 1 (u32 LE)
0051: <1 byte ciphertext> ; AES-GCM ciphertext of "A" under the AAD below

;; AAD (29 bytes; NOT written to disk, input to AEAD only)
     50 2a 4d 18          ; MagicMetadata
     10                   ; HeaderByte
     01                   ; KeyEpoch
     00                   ; BlockType
     02                   ; SuiteID
     00 00 00 00 00 00 00 00   ; TableID BE
     00 00 00 00 00 00 00 00   ; BlockOffset BE
     00 00 00 00          ; DictID BE
     15                   ; WindowLog
```

Total on-disk size: 82 bytes (73 metadata + 9 body). The AEADTag and ciphertext bytes are generated by the AEAD library and not literal in this example, the conformance harness in #253 computes them and asserts exact byte equality.

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
