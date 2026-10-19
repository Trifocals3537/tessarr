---
title: Torbox Setup
description: Configure Torbox provider.
---

Torbox is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "torbox",
      "name": "Torbox",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the Torbox dashboard.

When an import supplies a `.torrent` file, Tessarr uploads that file directly
to TorBox instead of reducing it to a magnet. A validated private copy is kept
for restart recovery and later repair; magnet-only imports continue to use the
magnet endpoint. If tracker removal is enabled, announce URLs are removed from
both the submitted file and its generated magnet without changing the infohash.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.
