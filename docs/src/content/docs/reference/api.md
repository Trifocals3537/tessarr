---
title: API Reference
description: REST API endpoints.
---

Tessarr provides a REST API for programmatic access.

## Authentication

Include API token in Authorization header:

```bash
curl -H "Authorization: Bearer YOUR_API_TOKEN" \
  http://localhost:8282/api/torrents
```

Get API token from **Settings** → **Auth** after login.

## Endpoints

### GET /version

Get Tessarr version.

```bash
curl http://localhost:8282/version
```

**Response:**

```json
{
  "version": "1.0.0"
}
```

### GET /api/config

Get the editable configuration without returning stored credentials. Configured
provider keys, Arr tokens, Usenet and rclone passwords, proxy values, webhook
URLs, and callback URLs are represented by the reserved
`__TESSARR_REDACTED__` placeholder. The control-plane API token is never
returned; `api_token_configured` reports whether one exists.

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/config
```

### PATCH /api/config

Update only the supplied configuration fields. Omitted fields are preserved.
The request uses [JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7396)
semantics, so setting a field to `null` removes it.

When updating an existing provider entry, send the redaction placeholder back
unchanged to preserve that entry's stored secret, send a new value to replace
it, or send an empty string to request an empty value (normal configuration
validation still applies). A placeholder whose provider identity no longer
matches an existing entry is rejected instead of being persisted.

```bash
curl -X PATCH \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"log_level": "debug"}' \
  http://localhost:8282/api/config
```

### PUT /api/config

Replace the editable configuration with a complete document. Authentication
credentials and authentication enablement are managed separately and are not
overwritten by this endpoint.

`POST /api/config` remains temporarily available for older clients, but it is
deprecated and rejects requests that omit the `debrids`, `mount`, or `usenet`
sections. In compatibility mode it preserves other omitted fields. Use `PATCH`
for all new partial-update integrations.

### GET /api/torrents

List all torrents.

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/torrents
```

**Query Parameters:**

- `category`: Filter by category
- `hash`: Get specific torrent

### POST /api/add

Add torrent or NZB.

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -F "files=@file.torrent" \
  http://localhost:8282/api/add
```

Or with a magnet link, torrent URL, 40-character hex infohash, or 32-character
base32 infohash:

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -F "urls=8a19577fb5f690970ca43a57ff1011ae202244b8" \
  http://localhost:8282/api/add
```

Separate multiple `urls` values with newlines. Raw infohash imports are
converted to tracker-free magnets; the provider supplies the torrent name and
metadata after submission.

### DELETE /api/torrents

Delete torrent(s).

```bash
curl -X DELETE \
  -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/torrents?category=sonarr&hash=abc123
```

### GET /api/repair/config

Read the current health-checker config.

### PUT /api/repair/config

Update the health-checker config (validates cron, workers, source).

### GET /api/repair/status

Active run summary, last completed run, and counts of entries by status.

### POST /api/repair/run

Trigger a sweep now. Optional JSON body fields:

| Field                 | Type    | Description                                                        |
|-----------------------|---------|--------------------------------------------------------------------|
| `ignore_last_checked` | boolean | Probe entries even when their last health check is still fresh.    |
| `auto_repair`         | boolean | Override the configured auto-repair setting for this run.          |
| `unrestrict_link`     | boolean | For torrent entries, probe by generating an unrestricted link instead of calling the provider check endpoint. |
| `verify_content`      | boolean | Manually add a bounded NZB media-head check for supported containers. With this enabled, omitted `auto_repair` defaults to `false` and omitted `protocol` to `nzb`; unsupported formats remain availability-only. |
| `protocol`            | string  | `all`, `torrent`, or `nzb`. Selects which protocols this run probes. |

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"protocol":"all","unrestrict_link":true}' \
  http://localhost:8282/api/repair/run
```

Returns `409 Conflict` when a sweep is already running.

Run a deeper, detect-only NZB check and include entries whose health is still
fresh:

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"verify_content":true,"ignore_last_checked":true,"auto_repair":false,"protocol":"nzb"}' \
  http://localhost:8282/api/repair/run
```

`verify_content` is rejected with `protocol: "torrent"`. It does not affect
imports, scheduled sweeps, or single-entry UI rechecks.

### POST /api/repair/stop

Cancel the active sweep.

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/repair/stop
```

### GET /api/repair/runs

Run history.

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/repair/runs
```

### GET /api/repair/runs/{id}

Run detail.

### DELETE /api/repair/runs

Clear run history.

### GET /api/repair/health

List entry health. Optional `?status=broken` filter.

```bash
curl -H "Authorization: Bearer TOKEN" \
  'http://localhost:8282/api/repair/health?status=broken'
```

### GET /api/repair/health/{name}

Per-entry health, including the broken-file list.

### POST /api/repair/health/{name}/check

Force-recheck a single entry.

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  'http://localhost:8282/api/repair/health/My.Show.S01/check'
```

### POST /api/repair/recheck/media

Recheck a single Arr media item. Set `fix` to `true` to repair through Arr after checking.

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"arr":"Sonarr","media_id":"123","fix":true}' \
  http://localhost:8282/api/repair/recheck/media
```

### POST /webhooks/tautulli

Trigger a targeted media recheck from Tautulli. When authentication is
enabled, configure the notification agent to send Tessarr's API token in an
`Authorization: Bearer TOKEN` header. A payload without a media identifier
starts a full sweep.

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topic":"tautulli","arr":"Sonarr","media_id":"123","fix":false}' \
  http://localhost:8282/webhooks/tautulli
```

### GET /api/arrs

List connected Arrs.

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/arrs
```

### POST /api/refresh-token

Regenerate API token.

```bash
curl -X POST \
  -H "Authorization: Bearer OLD_TOKEN" \
  http://localhost:8282/api/refresh-token
```

**Response:**

```json
{
	"token": "NEW_TOKEN",
	"message": "API token refreshed successfully"
}
```

## QBitTorrent API

Tessarr implements QBitTorrent Web API for Arr compatibility.

### POST /api/v2/auth/login

Login (compatibility endpoint).

```bash
curl -X POST \
  -d "username=admin&password=pass" \
  http://localhost:8282/api/v2/auth/login
```

### GET /api/v2/torrents/info

List torrents (QBit format).

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/v2/torrents/info
```

### POST /api/v2/torrents/add

Add torrent (QBit format).

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -F "urls=magnet:?xt=..." \
  -F "category=sonarr" \
  http://localhost:8282/api/v2/torrents/add
```

When every eligible provider gives a definite cache miss, the endpoint returns
`409 Conflict` with `X-Tessarr-Error-Code: torrent_not_cached`. Mixed failures
(for example, a cache miss followed by a provider outage) are not mislabeled as
an all-provider cache miss.

When every eligible provider rejects the content permanently, the endpoint
returns `422 Unprocessable Entity` with
`X-Tessarr-Error-Code: torrent_content_rejected`. Real-Debrid HTTP 451
responses use this outcome. Tessarr cools down that provider and info hash
for 24 hours, so recurring Arr grabs do not repeatedly call the rejecting
provider; other providers remain eligible. Cache misses and operational
failures are never placed in this cooldown.

### POST /api/v2/torrents/delete

Delete torrents (QBit format).

```bash
curl -X POST \
  -H "Authorization: Bearer TOKEN" \
  -d "hashes=hash1|hash2" \
  http://localhost:8282/api/v2/torrents/delete
```

## Browse API

Hierarchical file browsing (WebDAV-style).

### GET /api/browse/

List root groups (`__all__`, `__bad__`, categories).

### GET /api/browse/{group}

List torrents in group.

### GET /api/browse/{group}/{torrent}

List files in torrent.

### GET /api/browse/download/{torrent}/{file}

Download specific file.

## STRM Library

### POST /api/strm/regenerate

Starts a complete reconciliation of the configured STRM export and returns
`202 Accepted`. This control-plane endpoint requires the same authenticated
session or bearer token as the rest of `/api`.

Generated media URLs use `/stream/v1/{entryID}/{fileID}/{name}?s={signature}`.
They are read-only and require a valid HMAC signature even when WebDAV or UI
authentication is disabled. `HEAD` is answered from stored metadata without a
provider or NNTP request; `GET` supports one HTTP byte range.

## Error Responses

```json
{
  "error": "Error message",
  "code": 400
}
```

**Status Codes:**

- `200`: Success
- `400`: Bad Request
- `401`: Unauthorized (invalid token)
- `404`: Not Found
- `500`: Internal Server Error

## Rate Limiting

API calls respect Debrid provider rate limits configured in `config.json`.
CDN response bodies use a separate automatic concurrency governor: playback
and seeks take priority over background downloads, one playback slot is kept
available whenever the budget has at least two slots, and `429` responses
reduce concurrency until the provider's `Retry-After` window has passed. This
behavior requires no configuration.

The authenticated `GET /debug/stats` response exposes a secret-free
`cdn_traffic` snapshot with active and waiting request counts, adaptive limits,
and throttle counters grouped by provider. API keys and download-account
tokens are never included.

The same response includes `torrent_admission`, an aggregate count of content
rejections, submissions avoided by the bounded cooldown, active cooldowns, and
the cooldown duration. It does not expose release names or info hashes.

## Examples

### Python

```python
import requests

TOKEN = "your_api_token"
BASE_URL = "http://localhost:8282"

headers = {"Authorization": f"Bearer {TOKEN}"}

# List torrents
r = requests.get(f"{BASE_URL}/api/torrents", headers=headers)
torrents = r.json()

# Add torrent
r = requests.post(
    f"{BASE_URL}/api/add",
    headers=headers,
    json={"url": "magnet:?xt=..."}
)
```

### JavaScript

```javascript
const TOKEN = 'your_api_token';
const BASE_URL = 'http://localhost:8282';

const headers = {
  'Authorization': `Bearer ${TOKEN}`,
  'Content-Type': 'application/json'
};

// List torrents
fetch(`${BASE_URL}/api/torrents`, { headers })
  .then(r => r.json())
  .then(data => console.log(data));

// Add torrent
fetch(`${BASE_URL}/api/add`, {
  method: 'POST',
  headers,
  body: JSON.stringify({ url: 'magnet:?xt=...' })
});
```
