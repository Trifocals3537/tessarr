# Durable workflow foundation

Status: first implementation milestone, October 2026.

Tessarr's next architectural workstream is a shared durable workflow core. This
milestone establishes its safety boundaries while retaining the current storage
format, provider behavior, and deployment platforms. A transactional database
replacement, durable external-effect outbox, and generic persisted worker leases
are subsequent milestones; this change does not claim to implement them.

## Invariants

- Only one process may mutate a configuration directory, storage directory, or
  canonical append-log path. Ownership is acquired before restart recovery,
  storage cleanup, or compaction recovery respectively.
- Ownership lasts until resources have drained and file handles have closed.
  OS locks are released after a crash. Coordination files remain on disk and
  must never be unlinked to "clear" an active instance.
- Existing rooted paths, output ownership markers, generation fences, queue
  incarnations, deletion tombstones, provider-placement authorization, and
  cleanup obligations remain authoritative. A job outcome cannot bypass them.
- A worker panic is an explicit failure outcome. It is recorded while the worker
  still holds its lifecycle lease. It cannot silently disappear from admission
  accounting while the durable row remains queued.
- A job outcome updates a fresh queue snapshot under the existing storage gate,
  requires the same durable incarnation and an active downloading state, and
  synchronizes the record before returning success. It never resurrects a row
  or overwrites newer file/provider metadata.
- Completion notifications and Arr refresh begin only after the completion
  checkpoint succeeds. Optional notification delivery remains best effort.
- Synchronization failures are visible through readiness and block further
  writes to the affected store until synchronization succeeds. A failed workflow
  outcome checkpoint latches manager admission closed until a successful restart
  reconstructs work from durable state. Readable indexed bytes are not evidence
  that a failed checkpoint was durably committed.

## Ownership boundaries

`internal/stateowner` uses the existing rooted file-lock implementation on Unix
and Windows. Acquisition is nonblocking and verifies the visible coordination
file after obtaining its lock. The process lock spans in-process configuration
restarts. The storage lock precedes legacy cleanup and multi-store recovery.
Each append log additionally owns a hashed sidecar lock that is independent of
the data-file inode, so compaction cannot release ownership by replacing a file.

Locks coordinate builds that implement this protocol. Older binaries do not
honor it; an eventual deployment must stop the previous writer before opening
its data with this build. Live state must not be shared across hosts through a
filesystem whose lock semantics have not been validated.

## Execution and persistence boundaries

`internal/workflow.Run` distinguishes successful invocation, failure,
cancellation, and panic. It does not choose retries or assert that a media import
has completed. Recovered panic payloads are deliberately excluded from persisted
errors because they can contain credentials or even unsafe formatting methods.

The manager's `queueOutcomeStore` is a narrow repository contract. Its current
implementation, `storage.CommitQueuedOutcome`, uses a single existing queue
record and an explicit sync barrier. Outcomes cover failed work, provider-slot
deferral, and completed output. The in-process generation fence is acquired
before the storage gate; the durable incarnation is checked again inside it.
Terminal or deleting rows reject stale outcomes without degrading the service.
No network operation is performed while either gate is held.

An outcome checkpoint can fail after bytes reached the operating system. The
caller must therefore treat that outcome as uncertain, suppress success side
effects, and retain the existing source/ownership state. Startup recovery remains
the authority for the next attempt. There is no exactly-once claim for external
provider, Arr, or filesystem effects.

## Next milestones

1. Evaluate a transactional catalog/workflow store behind repository contracts.
   SQLite on local storage is a candidate, subject to benchmarks, Windows CGO=0,
   native Linux/glibc compatibility, and explicit durability settings.
2. Define durable job/attempt identities, fenced claims, not-before timestamps,
   tri-state request policies, and critical external-effect records. Commit
   intent before making an external call, then reconcile uncertain outcomes.
3. Build an offline migration from an immutable checkpoint, comparing semantic
   digests of entries, files, placements, queue incarnations, tombstones, cleanup
   obligations, health revisions, source references, and output ownership.
4. Keep one authoritative writer at cutover. Do not introduce independent dual
   writes. Rollback after new external effects requires reconciling those effects
   in addition to restoring a database checkpoint.
5. Migrate repair and cleanup execution onto shared claims without broadening
   their destructive authorization. Preserve immutable library snapshots and
   current caches on the streaming path.

## Verification

Required coverage includes real subprocess exclusion/crash release, compaction
crash recovery, immediate recovery without a final Close/fsync, failed periodic
sync and retry, outcome durability, stale generations and incarnations, deletion
races, panic survival, canceled work, and suppressed completion side effects.
The existing cleanup/reopen/ownership matrices, provider and protocol fixtures,
Windows/Linux suites, Linux race/static/vulnerability checks, and seedbox build
remain release gates. Real FUSE canaries run on a capable isolated Linux runner.

No merge or deployment is part of implementing this milestone.
