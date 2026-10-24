# Durable cleanup workflows

This first execution milestone covers retained queue-deletion intents. Cleanup
that finishes synchronously keeps its existing fast path. Interrupted or failed
work uses a shared workflow record, embedded in the same authoritative tombstone
as its queue incarnation, saved files policy and placement snapshots. No new
database, service, dual writer or data migration is introduced.

## Claims and recovery

The workflow record persists a version, revision, attempt count, random owner and
claim token, retry deadline, fixed reason code, account restrictions and operator
decisions. New claims publish through `PutExistingSynced`: effects cannot begin
after a failed claim checkpoint. State transitions also check the exact queue
incarnation. Legacy mutation methods reject an intent once the workflow owns it.

A live claim has no timeout-based takeover. The manager holds its per-entry
deletion lease across the attempt; shutdown drains manager-owned work before
storage/process ownership is released. Only exclusive startup can reclaim an
interrupted owner. The previous token cannot retire a row, acknowledge provider
cleanup, remove local files or complete the replacement attempt.

One manager-owned worker checks due work every five seconds, processing at most
32 intents per pass. Retries use persisted exponential delays from five seconds
to fifteen minutes, and stop for review after ten attempts. Blocked work requires
an operator decision. Restart preserves retry deadlines and decisions.

## External and local effects

Before a provider mutation, the workflow checkpoints that an external effect may
have started. Recovery checks exact provider IDs with read-only lookups. Only a
typed not-found response acknowledges absence. A list miss, network error,
HTTP 500 or an existing transfer cannot authorize a repeated uncertain DELETE.
Provider/account restrictions and recovered-placement fences remain in force.
Only the immutable saved placement scope is used; current main entries cannot
expand a recovered intent. Transfer-only cleanup still reserves its placement
fence and refuses collisions with library placements.

Queue retirement remains durable before filesystem work. Independent pending
administrative main cleanup still gates the associated queue. Main-deletion
obligations keep their existing recovery path in this milestone; repair and
other external-effect workflows are not migrated yet.

Normal cleanup uses the existing rooted ownership checks. Operator reconciliation
can additionally acknowledge an exactly absent legacy output after checking
portable aliases and quarantine remnants. It can remove an otherwise-empty NZB
staging directory only through the existing UUID-owned, marker-only quarantine
protocol. A stale display-name derivation is corrected only for that operation;
persisted names and library paths are not rewritten. Foreign owners, nonempty
legacy folders, symlinks, invalid roots, traversal, aliases and ambiguous
quarantines remain blocked. Only an exact UUID-bound `.queued` source may be
removed; persistent Usenet maps and original NZBs are preserved.

## Operator controls and audit

The Repair page lists pending cleanup, attempts, retry times and reason codes.
`GET /api/cleanup/workflows` returns a bounded active view and up to 100 completion
checkpoints. Claim tokens, account fingerprints, callback URLs, source payloads
and provider error bodies are not exposed.

`POST /api/cleanup/workflows/{key}/resolve` accepts `incarnation`, `revision`, a
UUID `request_id`, and `action` (`retry` or `reconcile`). Authentication, browser
origin protections and request-size bounds apply. Stale revisions/incarnations
conflict; repeated identical decision IDs are idempotent. Decisions checkpoint
before another attempt. Neither action changes saved deletion scope, preserving
policy, account restrictions or uncertainty about a provider mutation. There is
no force-complete or fence-clearing operation.

Completion checkpoints preserve operator decisions before the tombstone is
cleared. A checkpoint with a surviving intent is explicitly incomplete. A crash
between those writes leaves recoverable work, not a false completion; restart
reconciles the same intent and receipt. Storage faults suspend workflow admission.

## Validation and rollback

Coverage includes real subprocess exits after claim, before/after a provider
mutation, after local cleanup, after an operator decision, and between the
completion receipt and intent removal. Tests also cover stale workers and UI
requests, account changes, bounded background retries, panic recovery, secret
redaction, HTTP authentication/origin checks, ownership conflicts and preservation
of nonempty folders, aliases, quarantines, main entries and Usenet metadata.

Older binaries ignore workflow metadata and can replay older recovery behavior.
Before any downgrade, stop the writer, preserve a consistent checkpoint and
review outstanding work and external effects. A binary-only rollback is not a
safe way to discard claims or decisions. No merge, release or deployment is
included in this implementation.
