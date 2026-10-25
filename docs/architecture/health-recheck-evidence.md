# Health recheck evidence

A healthy availability sample cannot acknowledge a failed stream range. The
health rechecker now keeps the failed logical file ranges with the dirty
revision, validates them through the production reader, and treats unfinished
work as incomplete. This uses the existing hybrid repair-state store, with no
new database or service.

## Evidence and completion

- Terminal failures persist owner hash, exact filename, offset and length
  before event admission. Duplicate ranges coalesce; at most 64 distinct ranges
  are retained. Invalid or overflowed evidence makes the check incomplete.
- All health rechecks perform supported NZB media-head verification in addition
  to sampled STAT availability. The existing global content slot limit, one
  foreground reader connection and zero prefetch remain in force. This also
  finds a wrong head on a historically clean row whose old failure was lost.
- Failed NZB ranges use isolated production readers. Complete-segment slices,
  yEnc integrity, source geometry and exact read counts remain required. Range
  buffers are capped at 64 KiB. Supported heads are checked as well.
- Torrent failures use exact bounded production HTTP stream ranges with repair
  disabled. A generated link or provider presence cannot acknowledge recovery.
- Connection errors, cancellation, short/incomplete reads, missing evidence,
  mismatched owners and changed sources cannot establish healthy state. Unknown
  results cannot be hidden by another healthy file. Confirmed broken files may
  coexist with incomplete work; that entry remains dirty and cannot enter the
  destructive Arr repair or clear-broken selection.
- Newer failure revisions override stale probe saves, including their ranges,
  dirty state and last successful timestamp. A stale success becomes unknown.

Import acceptance, provider deletion fences, cleanup workflow authority,
administrative account binding, library paths and Usenet metadata are unchanged.
No provider reinsertion is used to turn failed-range evidence into success.

## Bounded progress

Each completed successful file probe checkpoints a source fingerprint. Progress
survives cancellation and restart and is reusable only for the same dirty
revision, file-set fingerprint, probe mode and current source fingerprint.
Progress expires after 24 hours. A new runtime failure invalidates it. Unknown
and failed files are retried, never cached as verified. Two probes per entry and
the existing outer worker/event limits remain unchanged.

NZB source fingerprints hash compressed metadata without decoding segment maps.
A bounded 256-record cache reuses a digest only while the opened metadata file's
identity, size and modification time match. Atomic metadata replacement or a
provider/file change invalidates the relevant proof. `file_count` remains an
inventory count; `probe_progress.verified` is completed probe coverage, and
`incomplete_files` identifies unfinished filenames. These are bounded health
checks, not claims of complete media decode or every-byte integrity.

## Canonical health names

An absent runtime name can be resolved only through one exact main-entry owner,
its configured derived folder name, and an index whose files belong to that
owner. Ambiguous names and mixed ownership fail closed. Names are never merged
using case folding or suffix stripping alone.

The canonical dirty signal and merged revision are synced before a forwarding
record replaces the old health key. A replay marker prevents duplicate handoff
after a crash between these writes. Later failures under the old name forward
to the canonical row. A stale alias probe cannot overwrite it, and forwarding
records do not appear as independent health entries. No media path is renamed.

Historical read-failure rows without recorded ranges remain incomplete; the
software does not invent missing evidence from availability samples. Their
canonical identity can still be repaired, and a bad head can still be detected.
An operator must supply trustworthy original failure evidence or establish a
separately verified recovery before those warnings can be acknowledged. This
change does not backfill live warnings or clear historical records.

## Regression coverage

- Process death without Close after range persistence, progress persistence,
  canonical handoff sync and forwarding publication; restart/replay and later
  alias failures retain the signal without duplicate revisions.
- Exact failed tail recovery and truncation through the real torrent stream.
- Local verified-TLS NNTP fixtures: valid head/tail, absent tail behind
  successful STAT, restored tail, CRC-valid short mapped article, wrong media
  head, metadata replacement, canceled and connection-only availability checks.
- Partial and canceled packs, resumed file coverage, new failure revisions,
  provider replacement, missing/legacy ranges, invalid offsets and overflow.
- Real main-entry/index producers for the .mkv suffix and You./YOU. cases,
  including ambiguous owner and mixed-owner rejection.

The existing cleanup, crash recovery, operator resolution and account authority
regressions must pass alongside these tests before merge or deployment.
