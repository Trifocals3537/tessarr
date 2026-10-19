---
title: Debrid Link Setup
description: Configure Debrid Link provider.
---

Debrid Link is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "debridlink",
      "name": "Debrid Link",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the Debrid Link dashboard.

Tessarr submits magnet links directly and, when the original `.torrent` file is available, uploads that file to Debrid Link. Keeping and submitting the exact source preserves metadata that cannot be recovered reliably from an info hash alone.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.
