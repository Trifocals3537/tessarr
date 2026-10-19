# qBittorrent queue cleanup and provider retention

Servarr uses `deleteFiles=true` both after successful import and when removing
failed downloads. It is not permission to delete a persistent library/provider
entry.

The qBittorrent delete endpoint retires the active-download queue row only:

- With `deleteFiles=true`, owned staging files are removed; persistent main
  entries and provider placements are preserved by default.
- With `deleteFiles=false` or an omitted flag, staging files and provider/main
  entries are preserved.
- An absent queue row is an idempotent success, never a fallback to main deletion.
- The explicit administrative deletion API remains destructive when requested.

## Failed uncached transfers

Provider cancellation is permitted only for a torrent with an uncached-download
failure (`stalledDL`, a recognized watchdog handoff reason, a submitting Arr
category/client endpoint, and a valid active provider placement). The owning Arr
must first validate its uniquely matched qBittorrent endpoint and exact queue
item. A before-delete callback durably records authorization **after validation
and before Arr DELETE**, since Arr can synchronously call the qBittorrent delete
endpoint during that request. No worker/deletion lease is held across this Arr
network call.

Authorization binds the queue incarnation, owner/endpoint, active provider/ID,
failure reason/state and progress baseline. A queue update that changes this
evidence durably revokes authorization; changing it back does not restore it.
Missing or stale authorization falls back to benign queue/staging cleanup.
A failed authorization or revocation sync is different: an in-memory durability
latch rejects subsequent evidence updates, destructive preparation and
reauthorization until the pending barrier succeeds. A proof disappearing from
the memory index does not acknowledge its durable revocation.

After workers drain, the final incarnation is classified while preparing its
durable deletion intent. Client intents carry `queued_transfer_only` scope and,
when authorized, exactly one active placement snapshot. Retry/restart recovery
cannot add current main entries or inactive placements to this scope. Unknown
scopes and malformed placement snapshots fail closed. Legacy intents without a
scope retain their previous all-placement semantics.

A placement also present in main storage (even under another hash) is not safe
to cancel: cleanup returns a manual-review error instead of removing a possible
library target. Immediately before provider deletion, collision checking and a
synced placement fence share a short local mutex with every main publication
(including provider synchronization and rollback writes). The mutex is released
before network work: same-hash and cross-hash main writes for the reserved ID
fail closed rather than wait behind provider calls. Retry/recovery uses the same
boundary. Provider failures retain retryable intent; only typed provider-not-found
errors count as already absent.

The fence remains durable even after success or an ambiguous provider failure.
Stale provider listings must not republish a library row backed by a deleted ID.
A different provider ID is unaffected; reuse of a fenced ID requires manual
reconciliation. There is deliberately no automatic fence expiry or clearing API.

Explicit admin removal fulfills main-provider destruction through a separately
synced snapshot in the main deletion tombstone, even when resuming a benign
client intent with no provider callback. Main cleanup runs before queue files;
provider errors preserve the admin obligation for retry/restart. The existing
client intent is neither widened nor upgraded from preserving files.

## Failed requests and retries

- A false or omitted `deleteFiles` request cannot resume an unfinished true
  client deletion. It returns an error without cancelling providers, removing
  files, or forgetting the original intent—even when provider cleanup already
  succeeded and only files remain. Retry the original `deleteFiles=true`
  operation, or allow restart recovery to finish its durable obligations.
- A false-first intent remains preserving when subsequently retried with true.
- Pending administrative main cleanup gates same-hash local file deletion and
  queue-intent completion in live calls as well as recovery. An ordinary client
  retry cannot perform the admin's provider deletion. The queue row may already
  be retired to prevent workers restarting; its intent and staging remain.
  Retry the administrative operation while the main row exists, or let startup
  recovery finish the independent snapshot after restart. Unrelated hashes are
  not blocked by that snapshot.
- Queue-only administrative deletion (no main row) cannot adopt an existing
  client intent. It fails closed rather than widen scope or report provider
  deletion it did not perform. Record the retained intent's provider IDs and
  manually reconcile the provider obligation; the original client operation can
  then finish according to its own policy. Repeating the incompatible admin
  request, including after reopening storage, does not upgrade the intent.
  A fresh queue-only admin deletion still persists and retries its destructive
  intent normally.
- Known migration metadata is excluded from provider collision scans. Malformed
  actual entry records still block destructive cleanup.
- Failed queue-row retirement and intent-removal syncs remain barriers on every
  retry, even after the record disappears from the memory index. Completion
  reopens the queue key only after the cleared intent is durably synced, so a
  replacement cannot be admitted early or remain blocked after a successful retry.

### Regression matrix

| Boundary | Coverage |
| --- | --- |
| Handoff durability | Repeated failed sync; changed/reverted evidence; failed authorization and reauthorization; completion retries; subprocess exit without Close and reopen before/after the successful barrier |
| Preserving retries | Manager false retry with provider pending or only files pending; HTTP false/omitted flags; false-first preservation |
| Administrative gate | Both admin APIs followed by true/false client retries; preserving and non-preserving intents; reopen/recovery; unrelated hashes; completion-sync failure; shared key lock through local cleanup |
| Queue-only admin | Existing benign intent rejected without expansion or false success; reopen/retry; fresh destructive intent retries after provider failure |
| Migration | Legitimate migration status accepted; corrupt real record rejected |
| Queue durability | Repeated row-retirement and intent-removal sync failures; direct and client completion retries; replacement admission and reopen after a successful barrier |

The crash test models process termination, not physical power loss or a storage
device that lies about fsync. The durability argument depends on successful
filesystem sync having its documented meaning.

## Protocol limitation

The qBittorrent delete protocol supplies only a hash, not a queue-incarnation
token. An arbitrarily delayed delete for an old submission cannot be distinguished
from a delete for a new submission of the same hash. Durable authorization is
never inherited by a new incarnation, so such a callback does not acquire old
provider-deletion permission, but it can still retire the new queue/staging row.
This change does not claim to solve that protocol limitation.
