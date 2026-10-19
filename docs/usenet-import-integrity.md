# Usenet import integrity

The streaming importer builds a byte map, not an extracted copy of the media.
It must not estimate missing bytes or join disconnected archive fragments.

## What is validated

- Per-file yEnc boundaries and fixed-size article geometry, independent of
  subject counters, encoded NZB byte counts, and other volumes' sizes.
- Contiguous NZB segment numbering, unique article IDs within a file, and
  complete logical output ranges. Randomized per-article names are allowed.
- RAR volume signatures, plaintext header CRCs, ordered split-before/split-after
  chains, and exact stored-member sizes. A failed volume is not silently dropped.
  Required AES block padding is distinct from the advertised logical size.
- Actual sampled article bodies before the parser returns an importable result.
  Both decoder backends check supplied per-article CRCs and decoded sizes.
- Sampled bodies must match the original posted file's size, byte range, and
  supplied part counters, not just be internally consistent. This source
  geometry survives archive slicing and is checked on subsequent streaming
  reads before bytes are published to the cache. Zero-based NZB numbering is
  normalized once; it does not permit neighboring yEnc parts to substitute.
- A genuinely empty RAR member is skipped after archive-chain validation. It
  cannot invalidate a separate complete media member or excuse missing bytes
  from a nonempty member. Empty-only archives are not successful media imports.
- The pure-Go decoder independently parses the required yEnc footer size. It
  rejects missing, malformed, overflowing, or duplicate sizes even if the
  article's supplied CRC matches its decoded payload.

An article listing is not proof of a readable body: some servers answer `STAT`
successfully while retrieval fails. When `BODY` returns 430, Tessarr tries
`ARTICLE` once for the same message ID on that provider. It validates the response
and Message-ID header, bounds header parsing, and sends the body through the
normal decoder. This does not relax checksum or missing-article handling.
The normal successful BODY path makes no extra request. Authentication,
temporary connection errors, and CRC errors do not trigger this command fallback.

## Cost and limits

`usenet.import_availability_sample_percent` also controls the parser's body
sample, with beginning/middle/end coverage (or all articles for tiny files).
Samples are streamed through bounded decoder buffers rather than retained in
memory. Validation adds article-transfer bandwidth during import, in addition
to archive header/boundary reads. At 100%, every article is read; this can approach
a full download. Existing lightweight repair/STAT sweeps are unchanged.

Sampling below 100% does **not** establish complete availability or verify the
whole media file's archive checksum. Even a structurally valid map can later
encounter an unavailable article. Do not promote a candidate that fails known
read/playback checks. Irregular or conflicting geometry is rejected rather than
guessed; obtaining another complete NZB or verified PAR2 reconstruction may be
necessary.

These changes affect new parses. They do not rewrite existing metadata, replace
library files, perform automatic PAR2 repair, or migrate a deployment. Existing
failures should be reparsed and compared in isolation before any approved,
checkpointed replacement.

## Saved-map compatibility

New maps retain source geometry in an optional extension to the existing v2
numeric metadata region. Header-only scans and sampled message-ID scans do not
read that region, and streaming validation uses the existing decode pass with
no additional article request. There is a small per-segment metadata cost.

Legacy maps remain readable, but their missing source geometry is not guessed;
they retain the existing checksum and requested-slice checks rather than the
new source-position guarantee. Reparse them in isolation to validate stronger
maps. Older readers can read the unchanged v2 columns, but an older writer will
drop the optional geometry if it rewrites a map. Keep a metadata checkpoint
before any deployment or rollback; no automatic metadata migration is performed.

Format references: [RAR 5 specification](https://www.rarlab.com/technote.htm),
[NNTP article retrieval, RFC 3977](https://datatracker.ietf.org/doc/html/rfc3977#section-6.2).
