---
title: All Debrid Setup
description: Configure All Debrid provider.
---

All Debrid is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "alldebrid",
      "name": "All Debrid",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the All Debrid dashboard.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

## Transient magnet recovery

When uncached downloading is enabled and All Debrid reports status code `7`
(`Not downloaded in 20 min`), Tessarr uses All Debrid's documented
[magnet restart endpoint](https://docs.alldebrid.com/#restart) instead of
immediately deleting the download. Recovery is bounded to two restart calls
with a 30-minute cooldown; after that, the exact provider status is reported
as terminal. The retry state expires after 24 hours and is capped at 4,096
entries.

Cached-only imports are never restarted. They continue to return the typed
`torrent_not_cached` result, and other All Debrid error codes remain terminal.

See [Configuration Reference](../configuration/#debrid-providers) for full options.
