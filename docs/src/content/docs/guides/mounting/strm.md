---
title: STRM Library
description: Build a signed, mountless library for Plex, Jellyfin, Emby, and other players.
---

The STRM Library is a mountless alternative to DFS, Rclone, and WebDAV
browsing. Tessarr writes small `.strm` files to a normal directory. Each file
contains a signed URL that resolves the durable entry and file identity at
playback time.

This design works for Real-Debrid, TorBox, the other supported debrid
providers, and Usenet. Temporary provider URLs can expire or an entry can move
to another provider without changing the URL stored in the media server.

## Configure it

1. Open **Settings → STRM Library**.
2. Choose an empty, dedicated export directory.
3. Set **Application URL** on the General tab to an address the media server
   can reach.
4. Leave delivery on **Proxy through Tessarr** unless direct debrid traffic
   is specifically preferred.
5. Save, then add the export directory as a library root in the media server.

The STRM library can run alongside a mount or with **Mount type → No Mount**.
It does not require Docker or FUSE.

## Delivery modes

| Mode | Behavior | Best for |
|------|----------|----------|
| `proxy` | Tessarr resolves and streams the current source | Usenet, privacy, consistent seeking, and the safest default |
| `redirect` | Eligible debrid requests receive a temporary provider URL; Usenet and failures fall back to proxy | Reducing Tessarr bandwidth when the player can reach the provider directly |

Proxy playback reuses Tessarr's provider-link refresh, retry, CDN concurrency,
range, and active-stream lifecycle. A metadata `HEAD` request does not consume a
debrid API call or NNTP connection.

## Native layout

Example for a user-level Linux installation:

```text
/home/media/.config/tessarr/   Tessarr configuration
/home/media/library-strm/       Generated library
```

Give the Tessarr service write access to `library-strm`. The media server
needs read access only.

## Container layout

When Tessarr and the media server use separate containers, bind the same host
directory into both. No shared mount propagation or FUSE device is required:

```yaml
services:
  tessarr:
    volumes:
      - /srv/tessarr-strm:/strm

  jellyfin:
    volumes:
      - /srv/tessarr-strm:/media/strm:ro
```

Configure `/strm` as Tessarr's export path and `/media/strm` as the Jellyfin
library path. The URL inside each file must use an Application URL reachable
from the Jellyfin container; `localhost` would refer to Jellyfin itself.

## Ownership and security

- Every playback URL has an HMAC signature. Unsigned requests are rejected
  regardless of WebDAV or application-auth settings.
- The signing key is generated from cryptographic randomness, persisted in
  `config.json`, redacted from API responses, and never displayed in the UI.
- The export root and each entry directory carry ownership markers. Tessarr
  refuses a non-empty unrecognized root or entry directory.
- Reconciliation replaces or removes only `.strm` files whose signatures prove
  Tessarr ownership. Foreign files are preserved and reported as conflicts.
- Generated paths are constrained beneath the export root, reject symlinked
  path components, and are normalized for portable Windows/Linux names.

Treat `.strm` URLs like bearer links: do not publish or share them. Use HTTPS
when the URL crosses an untrusted network. Listener CIDR rules still apply to
the stream endpoint.

## Regeneration and changes

Tessarr updates a completed entry automatically and performs a full sweep at
startup. Use **Regenerate library** after restoring storage or when you want an
immediate full reconciliation.

Changing the Application URL, export path, naming option, or delivery setting
triggers a live sweep when no service restart is needed. Changing the signing
key manually is intentionally fail-closed: existing ownership markers will not
validate. To rotate it deliberately, disable STRM, select a new empty export
directory, and enable it with the new key.

## Current scope

The first release exports eligible media files. Subtitle, NFO, and artwork
sidecars are not copied; keep using a mount for workflows that require local
sidecar files. A player must also support URL-based `.strm` libraries.
