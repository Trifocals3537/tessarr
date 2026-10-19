---
title: Configuration Reference
description: Complete config.json reference.
---

Configuration is stored in `config.json`. Most settings can be managed via the Web UI under Settings.

## Server

```json
{
  "bind_address": "127.0.0.1",
  "port": "8282",
  "url_base": "",
  "app_url": "http://localhost:8282",
  "allowed_client_cidrs": [],
  "log_level": "info"
}
```

| Field                  | Type   | Description                                                   | Default       |
|------------------------|--------|---------------------------------------------------------------|---------------|
| `bind_address`         | string | IP to bind to                                                 | `127.0.0.1`   |
| `port`                 | string | Port to listen on                                             | `8282`        |
| `url_base`             | string | Base path for reverse proxy                                   | `""`          |
| `app_url`              | string | External URL for callbacks                                    | Auto-detected |
| `allowed_client_cidrs` | array  | TCP peer IP addresses or CIDRs allowed to use the listener     | Allow all     |
| `log_level`            | string | Logging level (`debug`, `info`, `warn`, `error`)              | `info`        |

`allowed_client_cidrs` applies to every route on the listener, including the
UI, qBittorrent-compatible API, SABnzbd-compatible API, and WebDAV. It checks
the actual TCP peer and deliberately ignores `X-Forwarded-For` and similar
headers. An empty list keeps the backward-compatible allow-all behavior.
Changing the list requires a restart.

On a shared host, bind directly to the private address assigned to the account
instead of listening on every interface. Point local automation applications
and any private HTTPS proxy backend at that same address:

```json
{
  "bind_address": "192.0.2.10",
  "allowed_client_cidrs": [
    "192.0.2.10/32"
  ]
}
```

`192.0.2.10` is a documentation-only address. Replace it with the private
address assigned to your account. Test both the proxy and automation
integrations before removing a working configuration. The single-address
allowlist remains useful defense in depth if the listener address is changed
accidentally later.

## Authentication

```json
{
  "use_auth": true,
  "secure_session_cookie": false
}
```

The username, bcrypt password hash, API token, and session secret are stored
separately in the private `auth.json` file. Use `--set-auth USERNAME` to set
credentials without placing a password in shell history.

Set `secure_session_cookie` to `true` when the UI is available only through an
HTTPS proxy such as Tailscale Serve. Browsers will then refuse to send the UI
session cookie over plain HTTP. Leave it `false` only when the UI itself must
be used over a trusted plain-HTTP connection. This setting does not change the
separate qBittorrent-compatible login cookie used by local Arr clients.

Native installations use the loopback-only bind default so the first-run setup
wizard is not exposed to the network. The official container image explicitly
overrides this to `0.0.0.0`; control container access with published ports,
firewall rules, and a reverse proxy.

After upgrading, an older native configuration that omitted `bind_address`
also uses `127.0.0.1`. If the service intentionally needs to accept remote
connections, set a specific trusted interface address (or `0.0.0.0`) in
`config.json` or with `TESSARR_BIND_ADDRESS`. A non-loopback Tessarr
listener is plain HTTP; application authentication controls access but does
not encrypt credentials. Use a trusted network or TLS proxy, and use
`allowed_client_cidrs` when the underlying port could otherwise be reached
directly. Normal startup and API configuration updates reject a non-loopback
listener without application authentication, an invalid client allowlist, or
an unprotected WebDAV endpoint.

## Downloads

```json
{
  "max_active_downloads": 5,
  "job_queue_capacity": 256,
  "refresh_interval": "30s",
  "remove_stalled_after": "10m",
  "uncached_stall_timeout": "",
  "allowed_file_types": ["mkv", "mp4"],
  "allow_samples": false,
  "min_file_size": "10MB",
  "max_file_size": ""
}
```

`max_active_downloads` is the shared active-processing limit for torrent and NZB downloads. Additional imports remain queued until an active download completes.

`job_queue_capacity` bounds all reserved, waiting, active, and delayed-retry
imports. New qBittorrent and SABnzbd requests receive a retryable overload
response when the bound is reached. Values above `4096` are clamped.

| Field                    | Type   | Description                                                                        | Default       |
|--------------------------|--------|------------------------------------------------------------------------------------|---------------|
| `max_active_downloads`   | int    | Shared active-processing limit                                                     | `5`           |
| `job_queue_capacity`     | int    | Total admitted import limit (maximum `4096`)                                       | `256`         |
| `refresh_interval`       | string | Provider status polling interval                                                   | `30s`         |
| `remove_stalled_after`   | string | Legacy cleanup interval for inactive zero-progress queue items                     | `10m`         |
| `uncached_stall_timeout` | string | Maximum no-progress wait for uncached Arr replacement; empty disables it           | `""`          |
| `allowed_file_types`     | array  | Extensions eligible for import                                                     | Media formats |
| `allow_samples`          | bool   | Include files identified as samples                                                | `false`       |
| `min_file_size`          | string | Minimum eligible file size                                                         | `""`          |
| `max_file_size`          | string | Maximum eligible file size; empty is unlimited                                     | `""`          |

`uncached_stall_timeout` applies only to transfers that Tessarr knowingly
started through an uncached provider pass. A successful provider poll must
first establish a progress baseline. Tessarr then requires both no progress
for the configured duration and zero provider-reported transfer speed before it
considers an ordinary transfer stalled. An explicit no-seeds state can use a
shorter `10m` wait; a metadata-download (`metaDL`) state can use `20m`. The
configured timeout remains a maximum for these states. Every handoff needs two
matching fresh provider checks at least 30 seconds apart, so a resumed transfer
or a changed state is retained. The minimum configured timeout is `10m`;
invalid or shorter values disable the watchdog instead of risking premature
removal. A restart discards a pending confirmation, not the progress baseline.

For a configured Arr category, Tessarr resolves the exact torrent hash to one
Sonarr/Radarr queue row, asks the Arr to remove and blocklist that release, and
allows its normal failed-download handling to search for a replacement. An
ambiguous hash or a category without an Arr owner is left untouched for manual
review. Provider/API errors are never interpreted as stalls, and cached
transfers are never eligible.

## Debrid Providers

Array of Debrid services. When an Arr does not set `selected_debrid`, providers
are tried in this array order. Tessarr makes one cached-only pass over every
eligible provider before it permits any uncached transfer. If every cached pass
misses, only providers with `download_uncached: true` enter a second pass, in
the same configured order. Provider/API failures remain errors and do not count
as cache misses.

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "name": "RD Primary",
      "api_key": "YOUR_API_KEY",
      "download_uncached": false,
      "rate_limit": "200/minute",
      "workers": 50,
      "minimum_free_slot": 0,
      "limit": 100,
      "torrents_refresh_interval": "5m",
      "download_links_refresh_interval": "10m",
      "auto_expire_links_after": "24h",
      "proxy": "",
      "unpack_rar": true
    }
  ]
}
```

### Provider Fields

| Field                             | Type   | Description                                                                    | Default                         |
|-----------------------------------|--------|--------------------------------------------------------------------------------|---------------------------------|
| `provider`                        | string | Provider type: `realdebrid`, `alldebrid`, `debridlink`, `torbox`, `premiumize` | **Required**                    |
| `name`                            | string | Unique provider instance key and mount-folder name                             | Provider type                   |
| `api_key`                         | string | API key from provider dashboard                                                | **Required**                    |
| `download_api_keys`               | array  | Additional keys for download rotation                                          | `[api_key]`                     |
| `download_uncached`               | bool   | Permit this provider in the uncached pass after all eligible providers miss     | `false`                         |
| `rate_limit`                      | string | API rate limit (`200/minute`, `10/second`)                                     | `200/minute`                    |
| `repair_rate_limit`               | string | Separate limit for repair operations                                           | Same as `rate_limit`            |
| `download_rate_limit`             | string | Separate limit for downloads                                                   | Same as `rate_limit`            |
| `proxy`                           | string | HTTP(S) proxy URL                                                              | `""`                            |
| `unpack_rar`                      | bool   | Auto-extract RAR archives                                                      | `true`                          |
| `minimum_free_slot`               | int    | Minimum free torrent slots to use this provider                                | `0`                             |
| `limit`                           | int    | Max torrents allowed on this provider                                          | `0` (unlimited)                 |
| `workers`                         | int    | Concurrent API workers                                                         | Auto (CPU * 50 / num_providers) |
| `torrents_refresh_interval`       | string | How often to refresh torrent list                                              | `5m`                            |
| `download_links_refresh_interval` | string | How often to refresh download links                                            | `10m`                           |
| `auto_expire_links_after`         | string | Auto-remove links after duration                                               | `24h`                           |
| `user_agent`                      | string | Custom User-Agent header                                                       | Default                         |

## Usenet

```json
{
  "usenet": {
    "providers": [
      {
        "host": "news.provider.com",
        "port": 563,
        "username": "user",
        "password": "pass",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      }
    ],
    "max_connections": 15,
    "processing_max_connections": 15,
    "read_ahead": "16MB",
    "processing_timeout": "10m",
    "availability_sample_percent": 10,
    "import_availability_sample_percent": 1,
    "disk_buffer_path": "/cache/usenet/streams"
  }
}
```

### Usenet Fields

| Field                         | Type   | Description                     | Default                      |
|-------------------------------|--------|---------------------------------|------------------------------|
| `providers`                   | array  | NNTP server configurations      | `[]`                         |
| `max_connections`             | int    | Max connections per streaming file | `15`                      |
| `processing_max_connections`  | int    | Max connections per file for parsing and NZB downloads | Same as `max_connections` |
| `read_ahead`                  | string | Prefetch buffer size            | `16MB`                       |
| `processing_timeout`          | string | Max time for NZB processing     | `10m`                        |
| `availability_sample_percent` | int    | % of segments to check during repairs (1-100) | `10`             |
| `import_availability_sample_percent` | int | % of segments to check when adding an NZB (1-100) | `1`         |
| `disk_buffer_path`            | string | Disk buffer location            | `{main_path}/usenet/streams` |

### Provider Fields

| Field             | Type   | Description                        | Default             |
|-------------------|--------|------------------------------------|---------------------|
| `host`            | string | NNTP server hostname               | **Required**        |
| `port`            | int    | NNTP port                          | `119` (563 for SSL) |
| `username`        | string | NNTP username                      | **Required**        |
| `password`        | string | NNTP password                      | **Required**        |
| `backbone`        | string | Optional shared article backbone for article-not-found failover | `""` |
| `ssl`             | bool   | Use SSL/TLS                        | `false`             |
| `max_connections` | int    | Max connections to this server     | `20`                |
| `priority`        | int    | Provider priority (lower = higher) | Index + 1           |

## Mounting

Mount configuration determines how files are exposed on the filesystem.

### Mount Type Selection

```json
{
  "mount": {
    "type": "dfs",
    "mount_path": "/mnt/tessarr"
  }
}
```

| Type              | Description                            |
|-------------------|----------------------------------------|
| `dfs`             | Custom VFS optimized for streaming     |
| `rclone`          | Embedded Rclone with full VFS features |
| `external_rclone` | Connect to existing Rclone RC instance |
| `none`            | No filesystem mounting                 |

### DFS Configuration

```json
{
  "mount": {
    "type": "dfs",
    "mount_path": "/mnt/tessarr",
    "dfs": {
      "cache_dir": "/cache/dfs",
      "chunk_size": "10MB",
      "disk_cache_size": "50GB",
      "cache_expiry": "24h",
      "cache_cleanup_interval": "1h",
      "daemon_timeout": "30m",
      "uid": 1000,
      "gid": 1000,
      "umask": "022",
    }
  }
}
```

| Field                    | Description                  | Default         |
|--------------------------|------------------------------|-----------------|
| `cache_dir`              | Local cache storage          | Required        |
| `chunk_size`             | Initial chunk size for reads | `10MB`          |
| `disk_cache_size`        | Max disk cache size          | `0` (unlimited) |
| `cache_expiry`           | Chunk expiry time            | `1h`            |
| `cache_cleanup_interval` | Cache cleanup frequency      | `10m`           |
| `daemon_timeout`         | Idle timeout before unmount  | `""` (never)    |
| `uid`                    | File owner UID               | Current user    |
| `gid`                    | File owner GID               | Current group   |
| `umask`                  | Permission mask              | `022`           |
| `allow_other`            | Allow other users to access  | `false`         |
| `default_permissions`    | Enable permission checks     | `false`         |

### Rclone Configuration

```json
{
  "mount": {
    "type": "rclone",
    "mount_path": "/mnt/tessarr",
    "rclone": {
      "cache_dir": "/cache/rclone",
      "vfs_cache_mode": "writes",
      "vfs_cache_max_size": "10GB",
      "vfs_read_chunk_size": "128MB",
      "vfs_read_ahead": "256MB",
      "buffer_size": "16MB",
      "transfers": 4,
      "uid": 1000,
      "gid": 1000
    }
  }
}
```

| Field                 | Description                        | Default         |
|-----------------------|------------------------------------|-----------------|
| `cache_dir`           | VFS cache directory                | Required        |
| `vfs_cache_mode`      | `off`, `minimal`, `writes`, `full` | `writes`        |
| `vfs_cache_max_size`  | Max VFS cache size                 | `0` (unlimited) |
| `vfs_read_chunk_size` | Read chunk size                    | `128MB`         |
| `vfs_read_ahead`      | Read-ahead buffer                  | `0`             |
| `buffer_size`         | I/O buffer size                    | `16MB`          |
| `bw_limit`            | Bandwidth limit                    | `0` (unlimited) |
| `transfers`           | Concurrent transfers               | `4`             |
| `uid` / `gid`         | File ownership                     | Current user    |

### External Rclone

```json
{
  "mount": {
    "type": "external_rclone",
    "external_rclone": {
      "rc_url": "http://localhost:5572",
      "rc_username": "user",
      "rc_password": "pass"
    }
  }
}
```

Connect to an existing Rclone instance's RC API.

## STRM Library

The optional STRM library exposes completed debrid and Usenet entries without a
filesystem mount. It can run with any mount setting, including `mount.type:
"none"`.

```json
{
  "app_url": "https://tessarr.example.net",
  "strm": {
    "enabled": true,
    "path": "/srv/media-strm",
    "delivery_mode": "proxy",
    "keep_media_extension": false
  }
}
```

| Field                  | Description                                                       | Default |
|------------------------|-------------------------------------------------------------------|---------|
| `enabled`              | Maintain the mountless export                                     | `false` |
| `path`                 | Dedicated directory containing generated `.strm` files            | —       |
| `delivery_mode`        | `proxy`, or `redirect` for eligible debrid streams                | `proxy` |
| `keep_media_extension` | Write `Movie.mkv.strm` instead of `Movie.strm`                    | `false` |

`app_url` must be reachable by the media server. When it is empty,
Tessarr writes its configured listener address and substitutes loopback for
an unspecified listener. That fallback is appropriate only when the player
runs on the same host.

Tessarr generates and persists a 256-bit signing key when STRM is first
configured. The key is redacted from the configuration API and is never shown
in the Web UI. Changing it invalidates existing `.strm` URLs; use a new empty
export directory if deliberate key rotation is required.

See the [STRM Library guide](../mounting/strm/) for media-server setup,
delivery choices, and ownership safeguards.

## Health Checker

```json
{
  "repair": {
    "enabled": true,
    "source": "arr",
    "schedule": "0 4 * * *",
    "workers": 5,
    "strategy": "per_entry",
    "recheck_interval": "168h",
    "auto_repair": true,
    "skip_nzb_repair": false,
    "nntp_connection_percent": 20
  }
}
```

| Field                     | Description                                                                | Default     |
|---------------------------|----------------------------------------------------------------------------|-------------|
| `enabled`                 | Master switch for the recurring sweep                                      | `false`     |
| `source`                  | `arr` (walk Arr media) or `managed` (walk managed entries)                 | `arr`       |
| `schedule`                | Cron expression. Required when enabled                                     | —           |
| `workers`                 | Concurrent probe workers                                                   | `5`         |
| `strategy`                | `per_entry` (stop at first broken file) or `per_file` (probe every file)   | `per_entry` |
| `recheck_interval`        | How long a healthy entry stays fresh before becoming a candidate again     | `168h`      |
| `arrs`                    | Optional Arr filter when `source=arr`. Empty = all eligible                | `[]`        |
| `auto_repair`             | When `true`, brokens are repaired in-sweep. When `false`, detect-only      | `false`     |
| `skip_nzb_repair`         | Skip NZB / Usenet entries during scheduled repair sweeps                   | `false`     |
| `nntp_connection_percent` | Share of NNTP connections probes may use, to avoid starving downloads      | `20`        |

See the [Health Checker & Repair guide](../repair/) for the full model, API, and Browse-page integration.

## Arr Configuration

```json
{
  "arrs": [
    {
      "name": "Sonarr",
      "host": "http://sonarr:8989",
      "token": "API_TOKEN",
      "skip_repair": false,
      "selected_debrid": ""
    }
  ]
}
```

| Field               | Description                      | Default     |
|---------------------|----------------------------------|-------------|
| `name`              | Display name                     | Required    |
| `host`              | Arr URL                          | Required    |
| `token`             | Arr API key                      | Required    |
| `skip_repair`       | Skip repair for this Arr         | `false`     |
| `download_uncached` | Tri-state per-Arr override of provider uncached policy. Omitted/null = inherit each provider's own `download_uncached` setting; `true` = allow an uncached pass for every provider after all cached attempts miss; `false` = strictly cached-only for this Arr | omitted (inherit) |
| `selected_debrid`   | Exact `name` of a Debrid instance | `""` (auto) |
| `source`            | Config source (`auto`, `config`) | `config`    |

:::tip
Leave `download_uncached` unset on the Arr unless you deliberately want to
override provider policy. The most common misconfiguration is setting it to
`false` on an Arr whose TorBox provider allows uncached fallback — an explicit
`false` wins over the provider setting and silently disables the fallback for
every grab from that Arr.
:::

## Queue Cleanup

Tessarr periodically scans each connected Arr's **Activity → Queue** and acts on stuck or
failed downloads based on a global, rules-driven policy. This is configured once (not per-Arr)
under **Settings → Arrs → Queue Cleanup Actions** in the Web UI, and stored in the
`queue_cleanup` block of `config.json`. See the [Arrs guide](../arrs/#queue-cleanup) for a
walkthrough.

```json
{
  "queue_cleanup": {
    "rules": [
      { "id": "failed_download", "action": "blacklist_research" },
      { "id": "title_mismatch", "action": "import" },
      { "id": "no_eligible_files", "action": "blacklist_research" },
      { "match": "stalled with no connections", "action": "blacklist" }
    ]
  }
}
```

Each rule resolves a queue issue to one **action**:

| Action               | Effect                                                                    |
|----------------------|---------------------------------------------------------------------------|
| `""` (ignore)        | Leave the item in the queue, do nothing                                    |
| `import`             | Force a manual import of the downloaded files                             |
| `blacklist`          | Blocklist the release and remove it, **without** searching for a new one  |
| `blacklist_research` | Blocklist the release, remove it, and trigger a re-search                  |

Rules are evaluated top-to-bottom, **first match wins**. Built-in catalog rules (those with an
`id`) always run before custom rules (those with a `match`), and unmatched warnings/errors are
left untouched. Changes apply on the next cleanup cycle — **no restart required**.

### Built-in catalog rules

| `id`                 | Triggered by                                            | Default action       |
|----------------------|---------------------------------------------------------|----------------------|
| `failed_download`    | Download reported as failed                             | `blacklist_research` |
| `title_mismatch`     | Title mismatch; automatic import not possible           | `import`             |
| `matched_by_id`      | Release matched to series/movie by ID                   | `import`             |
| `unable_to_parse`    | Unable to parse the download                            | `blacklist_research` |
| `no_eligible_files`  | No files eligible for import                            | `blacklist_research` |
| `episodes_missing`   | Episodes not imported / missing from the release        | `blacklist_research` |
| `file_empty`         | Downloaded file is empty                                | `blacklist_research` |
| `invalid_local_path` | Invalid local path (needs a Remote Path Mapping)        | `""` (ignore)        |
| `not_grabbed`        | Not grabbed by the Arr / no category                    | `""` (ignore)        |

For catalog rules you only set `action`; the match text is fixed. **Custom rules** use a
`match` field instead of an `id` — a case-insensitive substring tested against the queue
item's status message text (e.g. `"stalled with no connections"`).

## Environment Variables

All config options support environment variable overrides using double underscore notation:

```bash
# Server
PORT=8282
LOG_LEVEL=debug
TESSARR_JOB_QUEUE_CAPACITY=256

# Debrid
DEBRIDS__0__PROVIDER=realdebrid
DEBRIDS__0__API_KEY=your_key

# Usenet
USENET__MAX_CONNECTIONS=20
USENET__PROVIDERS__0__HOST=news.provider.com
USENET__PROVIDERS__0__PORT=563
USENET__PROVIDERS__0__BACKBONE=Omicron

# Mount - DFS
MOUNT__DFS__CACHE_DIR=/cache
MOUNT__DFS__CHUNK_SIZE=10MB

# Repair
REPAIR__ENABLED=true
REPAIR__INTERVAL=30m
```

See [defaults.go](https://github.com/Trifocals3537/tessarr/blob/beta/internal/config/defaults.go) for all defaults.
