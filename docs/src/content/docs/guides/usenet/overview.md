---
title: Usenet Configuration
description: Direct NNTP streaming configuration.
---

Tessarr supports direct NNTP streaming from Usenet providers - no additional download client required.

## How It Works

Tessarr connects directly to NNTP servers to:

1. Parse NZB files for segment information
2. Stream segments on-demand for playback
3. Download and assemble complete files

## Provider Configuration

### Add Provider

```json
{
  "usenet": {
    "providers": [
      {
        "host": "news.provider.com",
        "port": 563,
        "username": "your_username",
        "password": "your_password",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      }
    ]
  }
}
```

### Multiple Providers

Tessarr can use multiple providers with priority and failover:

```json
{
  "usenet": {
    "providers": [
      {
        "host": "primary.news.com",
        "port": 563,
        "username": "user1",
        "password": "pass1",
        "backbone": "UsenetExpress",
        "ssl": true,
        "max_connections": 20,
        "priority": 1
      },
      {
        "host": "backup.news.com",
        "port": 563,
        "username": "user2",
        "password": "pass2",
        "backbone": "Omicron",
        "ssl": true,
        "max_connections": 10,
        "priority": 2
      }
    ]
  }
}
```

Lower `priority` = higher preference.

`backbone` is optional. Set it when two providers share the same article spool so Tessarr can skip same-backbone providers after `423/430 article not found` responses.

### TorBox News Server

TorBox News works through the same generic NNTP provider settings; no
TorBox-specific integration is required. Copy the News Server connection
details from the [TorBox Tools page](https://torbox.app/tools/) into
Tessarr:

```json
{
  "usenet": {
    "providers": [
      {
        "host": "nntp.torbox.app",
        "port": 563,
        "username": "your_torbox_news_username",
        "password": "your_torbox_news_password",
        "ssl": true,
        "max_connections": 10,
        "priority": 1
      }
    ]
  }
}
```

Use the exact host and credentials TorBox provides. The News Server password
is shown only when created or reset, so store it before leaving the TorBox
page. Keep TLS enabled and do not configure more than the ten connections
allowed by TorBox.

## Performance Tuning

### Connection Limits

```json
{
  "usenet": {
    "max_connections": 15,
    "processing_max_connections": 15
  }
}
```

- `max_connections`: Per-file streaming connection limit
- `processing_max_connections`: Per-file parsing and NZB download connection limit
- Provider `max_connections`: Per-provider limit

**Example:**

- Global: `15`
- Provider A: `20`
- Provider B: `10`

→ Up to 15 connections per file, split between providers based on priority

### Read-Ahead Buffer

```json
{
  "usenet": {
    "read_ahead": "16MB"
  }
}
```

Prefetch buffer for smoother playback. Higher = smoother but more memory.

### Connection Idle Timeout

```json
{
  "usenet": {
    "conn_idle_timeout": "5m"
  }
}
```

How long an unused NNTP connection stays warm in the pool before it is closed
(default: `5m`). Tessarr periodically keepalive-pings idle connections and
verifies them before reuse. This avoids repeated TCP, TLS, and authentication
setup when a player reads in bursts or resumes after a short pause. Lower the
value only when a provider enforces a shorter idle-session limit.

Container users can set the same value with
`USENET__CONN_IDLE_TIMEOUT=5m`.

### Processing Limits

```json
{
  "max_active_downloads": 5,
  "job_queue_capacity": 256,
  "usenet": {"processing_timeout": "10m"}
}
```

- `max_active_downloads`: Shared active-download limit for torrents and NZBs
- `job_queue_capacity`: Total bound for reserved, waiting, active, and retrying imports
- `processing_timeout`: Mark as bad if processing exceeds this

### Availability Checking

```json
{
  "usenet": {
    "availability_sample_percent": 10,
    "import_availability_sample_percent": 1
  }
}
```

Use `availability_sample_percent` for repair checks and
`import_availability_sample_percent` for the availability gate when adding an NZB.

- `100`: Check all segments (slow but accurate)
- `10`: Check 10% (fast but may miss issues)
- `1`: Quick import check (default)

Availability checks do not inspect decoded media bytes. A manual repair run
can optionally add a conservative, bounded content-signature check for common
media containers. It is never run automatically during import or scheduled
repair; see [Health Checker & Repair](../../repair/#optional-nzb-content-verification).

## Disk Buffer

```json
{
  "usenet": {
    "disk_buffer_path": "/cache/usenet/streams"
  }
}
```

Streams use disk buffer for assembly. Ensure sufficient disk space.

## Repair

```json
{
  "usenet": {
    "skip_repair": false
  }
}
```

## Arr Integration

Arrs send NZB files to Tessarr via the Sabnzbd API endpoint:

See [Sabnzbd Integration](./sabnzbd/) for details.

If every configured provider reports that a required article is missing during
NZB admission, Tessarr reports a release rejection through the SAB-compatible
response. Sonarr and Radarr can then continue to another search result instead
of treating Tessarr as unavailable. Transient connection, timeout,
authentication, and server errors remain download-client failures so the Arr
can retry them later.

## Troubleshooting

### Connection Failures

- Verify host/port/SSL settings
- Test manually: `telnet news.provider.com 563`
- Check provider status

### Slow Streaming

1. Increase `max_connections` per provider
2. Increase global `max_connections`
3. Increase `read_ahead` buffer

### Processing Timeouts

- Increase `processing_timeout` for large files
- Reduce `availability_sample_percent` for faster checks
- Increase `max_active_downloads` if the system and providers have capacity

### Incomplete Downloads

- Enable `skip_repair: false` for PAR2 repair
- Check provider retention (old files may be incomplete)
- Try backup provider if available

## Example Configuration

Full Usenet config with optimal settings:

```json
{
  "max_active_downloads": 5,
  "usenet": {
    "providers": [
      {
        "host": "us.news.provider.com",
        "port": 563,
        "username": "user",
        "password": "pass",
        "ssl": true,
        "max_connections": 30,
        "priority": 1
      }
    ],
    "max_connections": 15,
    "processing_max_connections": 15,
    "read_ahead": "32MB",
    "processing_timeout": "15m",
    "availability_sample_percent": 5,
    "disk_buffer_path": "/cache/usenet",
    "skip_repair": false
  }
}
```
