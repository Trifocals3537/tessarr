# Native streaming-cache hotfix R2 (2026-10-10/11)

## Status and scope

**R2 implementation integrated for beta; deployment remains separate.** This change brings the preserved native-cache repair into the fork, with an additional metadata-publication fix found during integration review. The provenance and live observations below describe the historical native hotfix. They are not deployment or live-acceptance results for the current beta integration. No runtime configuration or live service is changed by this PR.

The original incident involved a consumer blocked on its first backing-file read. The FUSE mount accumulated waiting requests while its HTTP control plane remained responsive; eventually the consumer's HTTP service also stopped responding. Cache quota pressure was present. Reproduced source hazards match the failure conditions, but the exact original live producer stack was not captured.

## Artifact provenance

| Item | Identity |
| --- | --- |
| Source base | `42a35f5a5126079a97aec8519c54281d018e0715` |
| Version | `fork-beta-base-42a35f5a5126-repair-r2-30d230fd9b3d` |
| R2 patch SHA256 | `30d230fd9b3dc1928b7d303cad4d3476ec6586fc6c5ff25729567365ff8e1c61` |
| Native binary SHA256 | `931e1bb89bdcad287b14c6c85d5d5ccf12487d8985e57840f06906af51b248cc` |

The deployed candidate was built from that exact source base plus the reviewed repair, not from an unrelated latest-beta update. Four production files and two tests changed: `pkg/mount/dfs/vfs/cache.go`, `cleanup_mutex.go`, `disk_quota.go`, `downloaders.go`, `pressure_liveness_test.go`, and `quota_recovery_test.go`. Of 663 original files, 660 were unchanged and three were modified; three additions yielded 666 candidate files. Old CLI, frontend, configuration, authentication and storage behavior were retained.

## Repair history

### R1: useful liveness fix, insufficient recovery

Two source hazards were reproduced with real cache/downloader implementations and a synthetic source-reader boundary:

1. A request parked in `DownloadWithPriority` could ignore cancellation while `StopAll` joined workers.
2. Cleanup could hold its maintenance mutex while joining a downloader whose cache writer waited for the same mutex.

R1 added cancellation-triggered condition broadcasts and replaced blocking pressure-reclamation acquisition with `TryLock`, retaining authoritative quota accounting. Initial consumer range reads passed. However, periodic cleanup under a full cache caused a quota-denial storm and a cold range returned **HTTP 206 with zero bytes**. R1 was therefore not accepted as the final repair.

A separately approved temporary budget increase from configured `100GB` to `200GB` restored headroom while R2 was developed. R2 activation removed this workaround and restored the original configuration byte-for-byte.

### R2: recover active readers without quota bypass

- Preserve cancellation-aware teardown condition waits.
- Use zero-value-ready, context-cancellable maintenance admission; propagate downloader context to private cache admission. A closer cancels its downloader before joining, allowing a parked writer to exit the lock/join dependency.
- Capture a quota-generation notification **before** the authoritative admission check. Real persistent-byte release, canceled reservations and partial commits wake parked admission, including when unrelated cleanup is still joining another source worker. No polling or artificial capacity is introduced.
- Enforce the original hard **used-plus-reserved** budget on every admission. A notification is not permission to exceed the limit.
- Acknowledge zero bytes on cache-writer failure so exact-length `io.CopyN` cannot hide quota/cancellation errors.
- Honor an explicit zero-target pressure eviction without changing ordinary disabled-threshold policy or open/in-map protections.

No circuit breaker was disabled, no backend was switched, and no library media or cache payload was manually deleted or purged.

## Fork integration (2026-10-19)

The preserved `repair-r2.patch` was recovered and its SHA256 verified against the identity above before applying it to beta `4de33cda8e75cfd539b168711cd4881415676bfe`. The current Tessarr module path and per-consumer stream diagnostics are retained. The historical patch and native-binary hashes identify the original artifacts, not a build of this integration.

Integration review found that a write spanning several missing ranges could publish its first range, then fail a later quota reservation before marking metadata dirty. An ordinary metadata flush would omit the successfully cached prefix. Each successfully published range now marks metadata dirty immediately, including when later admission fails or is canceled.

The regression suite covers cancellation during teardown and maintenance, independent quota-release wakeups, zero-target eviction, preservation of quota errors at the real `io.CopyN` boundary, partial-write metadata persistence and exact retry bytes, notification generations, and 64 concurrent writers contending for released capacity. The metadata test fails on the original R2 patch and passes with the additional fix; the error-masking and zero-target tests also reproduce failures on unchanged beta.

Current integration validation is recorded in PR #136 and its checks. Deployment and live acceptance of a newly built beta artifact require a separate action.

## Historical verification

### Source and build

The historical independent review reported no security or logic defects in the exact patch; the additional metadata issue was found during fork integration. The preserved verification record reports these passing runs:

- Unmodified exact-base full test suite.
- Expected RED reproduction on R1: real active read returned zero bytes and incurred 10 denials before cleanup reclaimed space; exact-length error masking and zero-target eviction tests also failed as expected.
- R2 full test suite and full race suite.
- Targeted stress x100 and race stress x25.
- Vet and module verification.
- Additional 64-writer admission, generation/wakeup, partial-publication/retry and eviction-policy probes.

Tests execute real cache files, quota accounting, downloader scheduling, copying and teardown; synthetic behavior is limited to the provider/source boundary and scaled quotas. A real physical hole-punch release woke an active reader while unrelated cleanup remained gated.

Native Linux/amd64 build: Go 1.26.6, CGO enabled, GOAMD64=v1, both hanwen and cgofuse included, maximum requirement GLIBC_2.16. Debian 11 loader smoke and target-host offline help/config checks passed. Independent runtime code/data sections and symbols matched; working-directory-dependent external-linker DWARF and the resulting build-ID bytes differed. Bit-identical rebuilds are not claimed.

### Live acceptance under the original budget

Thirteen preserved consumer range observations spanned more than 15 minutes after mount readiness, including the first configured periodic-cleanup boundary:

- HTTP 206 and the exact requested 32 KiB in every observation; no empty response accepted as success.
- Consumer identity remained HTTP 200.
- Live executable hash and configured `100GB` policy checked in every observation.
- Used-plus-reserved remained within the hard budget.
- Observed quota pressure reached 605 events with **zero quota denials and zero circuit breakers** in those samples; actual reclamation progressed across cleanup.

Post-cleanup range checks:

| Probe | Result | Duration |
| --- | --- | --- |
| First 32 KiB | HTTP 206, exact bytes | 321 ms |
| Sequential 8 MiB | HTTP 206, exact bytes | 200 ms |
| Final 32 KiB | HTTP 206, exact bytes | 99 ms |

Some cold seeks during pressure took 14–19 seconds, but recovered complete data. Server-side data-plane recovery is verified; an end-to-end client viewing session and indefinite stability are not claimed.

## Operational preservation and rollback

Activation changed only the executable path in the existing service override. Disabled profiling and existing CLI arguments were retained. Configuration/authentication and effective consumer preferences were preserved; the temporary larger budget was removed. No bulk rescan or import was forced.

The old binaries and protected configuration/unit checkpoints remain available, along with consistent stopped native HYBR state backups, failed/successful verification receipts and a recursively verified final checksum manifest. Private credentials, host addresses, account paths, library details and raw logs are intentionally excluded from this public record.

Rollback requires explicit approval: stop the consumer before the producer, verify any stalled FUSE abort against fresh producer/UID/alias evidence, restore a deliberately selected paired override/configuration, reload and start the producer, verify its usable mount, then start and validate the consumer. FUSE device/connection numbers may recycle. Do not automatically restore stale application state: this hotfix performed no schema migration.

## Follow-ups

1. Promote the integrated implementation through normal beta review and CI gates, then handle deployment separately. Avoid a later update silently overwriting the native hotfix with an artifact that omits it.
2. Investigate noticeable cold-provider latency separately if it persists; no client settings were changed.
3. Uncooperative provider Close/I/O after maintenance acquisition and legitimately exhausted protected capacity remain possible. The repair does not guarantee admission without safe reclaimable bytes.
4. Improve deterministic native packaging if bit-identical artifacts are required; normalize external-linker debug paths.
