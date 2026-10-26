# Health recheck evidence

A healthy availability sample cannot acknowledge a failed stream range. The
health rechecker now keeps the failed logical file ranges with the dirty
revision, validates them through the production reader, and treats unfinished
work as incomplete. This uses the existing hybrid repair-state store, with no
new database or service.

## Evidence and completion

- Terminal failures persist owner hash, stable file ID when known, exact filename, offset and length
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
- Clear State resets only disposable status. Dirty rows, failed ranges, missing
  evidence, active work, progress and durable handoff/retirement records remain.
  Cleared display rows retain hidden revision fences, so an old probe cannot
  erase a later failure after a status reset.
- Source checks fence broken verdicts as well as successful ones, including a
  final pass across all results before repair authority is published. A change
  during a later file invalidates earlier proofs from that account/source.

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

Torrent progress also includes the existing account-binding MACs for all
configured placements. Replacing a provider client/account cannot reuse a
successful range proof from the old account even when the entry itself did not
change. NZB progress likewise binds the NNTP provider/account configuration.
API keys and NNTP credentials are never stored in health records. Coalesced
queue-generation uncertainty retains dirty state without inventing a new
range-less failure after the actual ranges have already been validated.

## Canonical health names

An absent runtime name can be resolved only through one exact main-entry owner,
its configured derived folder name, and an index whose files belong to that
owner. Ambiguous names and mixed ownership fail closed. Names are never merged
using case folding or suffix stripping alone.

Every probe resolves current ownership and refreshes its item, including Arr
media candidates that already carry a preloaded snapshot. Refreshing the item
retains their Arr/file context for operator resolution; preloading never grants
permission to skip failed-range or missing-evidence handoff.

The canonical dirty signal and merged revision are synced before a forwarding
record replaces the old health key. A replay marker prevents duplicate handoff
after a crash between these writes. Later failures under the old name forward
to the canonical row. A stale alias probe cannot overwrite it, and forwarding
records do not appear as independent health entries. No media path is renamed.

Bindings retain their owner hash and revalidate the original runtime name
against the current main entry and canonical index. Renames migrate evidence
before retargeting forwarding records. Reused names do not transfer the previous
owner's evidence. Revision fences survive renames back to a previously used key.

Before a main-entry rename changes its index, the old canonical and legacy
runtime rows acquire a synced owner binding. Missing legacy read evidence is
retained explicitly, including a runtime-name change that leaves the canonical
folder unchanged. Checks through the new name complete the owner's handoff
before probing; they do not depend on a second stream failure or a request
through the old name. If another owner reuses the old canonical key, its prior
owner's evidence is moved first. Failed ownership proof never permits appending
the new owner's range to that protected row. Stale saves from another owner
are rejected even when they carry a higher revision.
If a shared folder has unresolved proof that cannot be assigned to the moving
owner, the rename is rejected before index/main changes instead of guessing
which owner's future checks may ignore that evidence.

Owner and retirement lookups reuse the existing hybrid metadata indexes. Legacy
records are indexed once per open without changing their proof or revision;
ordinary probes read the matching owner's records rather than decoding every
health row. No extra database, service or background worker is introduced.

When a shared folder's former owner has no main row, its validated retired or
deleting fence must prove local retirement before its health key is released.
Missing rows alone, corrupt fences and pending replacements remain unresolved.
The previous owner's lifecycle must also have no active operations: a deletion
can temporarily remove its row and then restore it after a failed sync. Health
handoff defers until that rollback window closes, without waiting for lifecycle
locks while holding the entry index lock.
This proof grants no provider absence or administrative cleanup authority and
does not change the original tombstone or its account obligations.

The former health snapshot is retained under an opaque archive key in the same
store. Its digest makes exact retries idempotent. Publication syncs the archive,
then redirects old-owner aliases to it, then syncs the survivor's current root.
Crashes and failed barriers retain at least one durable copy of the evidence.
The survivor keeps its own ranges, receipts and missing-evidence uncertainty;
unresolved third-owner data blocks handoff rather than being hidden. Archived
records are read-only history, excluded from active work and health counts.
Old aliases cannot mutate them, and reusing a name cannot reactivate their
retired proof. Historical receipts retain their existing feedback protection.

## Operator file retirement

Clear Broken and explicit mount-file removal persist an exact owner/file-ID,
size and byte-range intent before publishing the local index's Deleted flag.
Only after that index is synced does the health row acknowledge matching failed
ranges and retain a confirmed receipt. Other files, changed incarnations, newer
failures and unscoped legacy evidence remain unresolved. Startup and subsequent
rechecks replay pending intents only against the same exact file identity.
Confirmed receipts suppress delayed feedback for that retired incarnation.
These receipts grant no provider deletion or administrative retry authority.

A fresh exact operator request can supersede a receipt for a different file
incarnation. The old receipt is retained as history in the same durable intent
write. History is never replay authority: stale intent still cannot remove the
replacement. Only confirmed historical identities suppress delayed feedback,
and a filename alone cannot identify a retired incarnation. Remaining files,
newer failures and incomplete legacy evidence keep their existing protection.

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
  provider/account replacement, missing/legacy ranges, invalid offsets and
  overflow; generic updates and alias merges cannot hide legacy evidence gaps.
- Real main-entry/index producers for the .mkv suffix and You./YOU. cases,
  including ambiguous owner and mixed-owner rejection, canonical rename and
  runtime-name reuse without transferring another owner's failures.
- Status resets with unresolved evidence and high-revision stale probes;
  process death at all three file-retirement and owned-alias migration phases;
  exact retirement, unrelated/newer failures and rename-back revision fences.
- Direct new-name checks read truncated/restored HTTP tails; legacy warnings
  survive producer renames; reused canonical keys retain each owner's proof.
- Process death before index rename, after index publication and after main
  publication, for both legacy and exact-range warnings; reused-name handoff
  and replacement-removal crashes at all durability boundaries.
- Public Clear Broken resolves a shared-folder replacement while preserving
  the other file and both parents; superseded receipts retain delayed-feedback
  protection without authorizing stale replay.
- Preloaded Arr candidates preserve exact tails and legacy warnings; a real
  local Arr collector runs on platforms with symlink support while the same
  preloaded-candidate boundary runs on all platforms.
- Retired shared-folder parent recovery preserves survivor failure intake and
  public Clear Broken resolution. Archive, alias and current-root crash/failure
  boundaries retain proof and deletion fences; uncertain retirement is refused.
- In-flight main and fence sync failures roll deletion back without archiving
  active proof, through both deletion APIs. Contended lifecycle directories
  defer health handoff without reversing the lock order.

An existing orphan-reconciliation limitation remains a separate follow-up:
an interrupted manual recheck of a previously clean, fresh row retains its
checkpoint but may wait for another trigger or the normal recheck interval.
Dirty incomplete checks remain eligible for recovery. These changes do not
change clean-row scheduling after restart.

The existing cleanup, crash recovery, operator resolution and account authority
regressions must pass alongside these tests before merge or deployment.
