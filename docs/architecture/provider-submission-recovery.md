# Provider submission recovery

This milestone makes queued torrent creation recoverable without introducing a
database, new service, or polling loop. Both asynchronous admission and the
synchronous import API save the queue/source before provider submission. The
low-level `SendToDebrid` routing API remains available for callers that manage
their own durability; service admission uses the queue-backed path.

## Intent and identity

Each queue hash has one current intent in the existing `storage_state` append
log. It contains a version, attempt UUID, durable queue incarnation, configured
provider name, primary-account fingerprint, effective uncached policy, phase,
and an optional provider ID. Credentials, magnets, and provider response bodies
are not stored in the intent. The account tag is a versioned, domain-separated
HMAC authenticated by the primary API key; changing that account blocks
automatic reconciliation. It is not a password verifier.

The intent is synchronized before creating remote work. A returned ID is
synchronized before checking status or publishing a queue placement. The queue
placement is synchronized before the intent becomes bound. Recovery finalizes
a surviving queue placement if a crash lost only that last marker. Storage
errors latch admission closed and preserve the recoverable queue state.

Every journal mutation checks the queue incarnation and deletion fence under
the existing per-key lifecycle lock. Provider calls run after releasing local
locks. Delayed responses cannot mutate a replacement or deleting queue item.
Legacy queued requests acquire a synced incarnation under their existing
generation fence before creating remote work.
An unresolved intent from an older incarnation blocks a new create rather than
silently discarding evidence of possible remote work.

## Routing and reconciliation

Fresh requests retain deterministic cached-first routing and the existing
uncached policy. Explicit cache misses, content refusals, and active-slot
refusals without a provider ID can be checkpointed as rejected. Transport
failures, incomplete responses, and unknown errors remain uncertain and stop
fallback. Queue errors expose that uncertainty. Restart restores those pending
failures for reconciliation, while ordinary terminal failures remain failed.

Reconciliation always uses the original configured account. An acknowledged ID
is read directly, including active transfers omitted from library listings.
The read must confirm the exact ID and a nonempty matching hash before status
processing can select files or restart a transfer. Missing or mismatched
identity remains unresolved. Without
an acknowledged ID, exactly one matching ID must appear in a successful full
listing. Hash-based recovery reuses only completed content and does not select
files, create work, or grant rollback deletion authority. Empty, multiple,
incomplete, inconsistent, or failed snapshots remain unresolved. In particular,
an empty listing is not proof that a timed-out create failed.
If an HTTP caller disconnects during submission, the accepted job keeps its
bounded admission slot and passes to a service-owned worker for reconciliation.
Service shutdown leaves recovery to startup; deletion fences prevent the
handoff from reviving canceled work. A failed handoff records a durable error,
and storage failures latch readiness closed until restart.
Later queued polling and uncached Arr handoff also reject destructive cleanup
authorization derived only from a recovered hash match.
That negative cleanup fence is keyed by configured provider and ID and survives
queue retirement, season splitting, and copied placement snapshots. All manager
provider-deletion paths check it, including deferred cleanup. Resolving such a
fence requires explicit ownership review; automatic clearing is not supported.

Fresh acknowledged attempts retain the existing bounded rollback behavior.
Cleanup intent is durable before DELETE, and successful cleanup is checkpointed
before another provider may be attempted. Failed cleanup stops fallback.
Restart never replays that DELETE; a typed not-found result for the exact ID
can resolve its intent. Remaining ambiguous cleanup requires operator review.

Queue-backed creates mark their HTTP context as one mutation attempt. Built-in
providers propagate that context into the shared request client, which sends
the request without automatic retries or redirects while retaining its existing
TLS, proxy, timeout, rate-limit, and traffic controls. Third-party legacy clients
remain responsible for their own internal retry behavior.

## Limits and verification

This is not an exactly-once guarantee for provider APIs. A crash immediately
before a request is sent can be indistinguishable from a lost response after
acceptance. Unresolved attempts deliberately remain blocked for inspection;
automatic clearing and a dedicated operator-resolution UI are not implemented.
Provider-side IDs and listing consistency remain part of the recovery contract.
No release, deployment, data migration, or live-provider mutation is included.

Tests cover real subprocess exits before create, after acceptance, after the ID
checkpoint, and after binding; timeout recovery; account changes; zero, one,
multiple, and incomplete matches; cached-first fallback; rollback ambiguity;
sync failures; stale attempts; explicit deletion and replacement; and HTTP
retry/redirect suppression. Existing platform, race, static, vulnerability,
ownership, cleanup, browser, and real FUSE checks remain required.
