---
title: Real Debrid Setup
description: Configure Real Debrid provider.
---

Real Debrid is one of the supported Debrid providers.

## Get API Key

1. Go to [https://real-debrid.com/apitoken](https://real-debrid.com/apitoken)
2. Copy your API key

## Configuration

### Via Setup Wizard

During first run:

1. Select **Real Debrid** as provider type
2. Paste your API key
3. Continue setup

### Via config.json

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "name": "Real Debrid",
      "api_key": "YOUR_API_KEY_HERE"
    }
  ]
}
```

## Advanced Options

### Multiple API Keys

Rotate between multiple keys for higher throughput:

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "download_api_keys": [
        "KEY_1",
        "KEY_2",
        "KEY_3"
      ]
    }
  ]
}
```

### Rate Limiting

Tessarr automatically enforces Real Debrid's documented 250 API
requests/minute limit once per API token. Main, repair, download-link, and retry
traffic using the same token share that budget. A conservative short-burst limit
also avoids sending the add-magnet bursts that Real Debrid can reject with
misleading errors.

The configuration limits remain useful as additional, tighter workload controls:

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "rate_limit": "200/minute",
      "repair_rate_limit": "60/minute"
    }
  ]
}
```

These settings cannot raise the built-in provider limit. Media bytes from Real
Debrid download hosts are not API calls and do not consume this request budget.

### Proxy Support

Route requests through a proxy:

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "proxy": "http://proxy.example.com:8080"
    }
  ]
}
```

### Torrent Slot Management

Set minimum free slots before using this provider:

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "minimum_free_slot": 5,
      "limit": 100
    }
  ]
}
```

- `minimum_free_slot`: Don't use if fewer than N slots free
- `limit`: Max torrents allowed on this account

## Multiple Providers

You can add multiple Real Debrid accounts:

```json
{
  "debrids": [
    {
      "provider": "realdebrid",
      "name": "RD Primary",
      "api_key": "KEY_1"
    },
    {
      "provider": "realdebrid",
      "name": "RD Secondary",
      "api_key": "KEY_2",
      "minimum_free_slot": 10
    }
  ]
}
```

Tessarr will use the first provider with available slots.

## Troubleshooting

### API Key Invalid

- Verify key at [https://real-debrid.com/apitoken](https://real-debrid.com/apitoken)
- Ensure no extra spaces when copying

### Rate Limit Errors

The built-in controller automatically honors HTTP 429 backoff across every
client using the affected token. If errors persist, reduce the configured
`rate_limit`, `repair_rate_limit`, or `download_rate_limit` to further smooth
your workload.

### No Free Slots

- Remove old torrents from [https://real-debrid.com/torrents](https://real-debrid.com/torrents)
- Or configure `minimum_free_slot` to use backup provider

See [Configuration Reference](../configuration/#debrid-providers) for all Debrid options.
