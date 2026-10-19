# Tessarr

<img src="docs/src/assets/logo.png" alt="Tessarr mosaic mark" width="160">

**Many sources. One library.**

**Tessarr** is a **Media Gateway** for Debrid services and Usenet written in Go.

## What is Tessarr?

Tessarr provides a unified interface for Sonarr, Radarr, and other *Arr applications to access Debrid providers and
Usenet streaming.

## Features

- Mock Qbittorent and Sabnzbd API that supports the Arrs (Sonarr, Radarr, Lidarr etc)
- Multiple Debrid and usenet providers support with a single interface
- Direct Usenet streaming via NNTP (no separate download client required)
- Optional signed STRM library for mountless Plex, Jellyfin, and Emby playback

## Supported Debrid Providers

- [Real Debrid](https://real-debrid.com)
- [Torbox](https://torbox.app)
- [Debrid Link](https://debrid-link.com)
- [All Debrid](https://alldebrid.com)
- [Premiumize](https://www.premiumize.me)

## Quick Start

### Native Linux

Native binaries are the primary deployment path for seedboxes and other
user-managed Linux hosts. Download the archive for your architecture from this
fork's [GitHub Releases](https://github.com/Trifocals3537/tessarr/releases),
verify it with the published `SHA256SUMS`, and install it:

```bash
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf tessarr_Linux_x86_64.tar.gz
install -Dm755 tessarr ~/.local/bin/tessarr
mkdir -p ~/.tessarr
~/.local/bin/tessarr --config ~/.tessarr
```

The first launch creates the configuration and listens on
`127.0.0.1:8282`. From another computer, use an SSH tunnel such as
`ssh -L 8282:127.0.0.1:8282 user@seedbox`, then open
`http://127.0.0.1:8282`. Put a trusted reverse proxy in front of Tessarr or
explicitly change `bind_address` only when remote access is intended.

On Ubuntu or Debian, the host needs FUSE support and the compatible FUSE
runtime (commonly `libfuse2`). Rclone is needed only when using the rclone
mount backend. Existing installations can validate their configuration without
starting services:

```bash
~/.local/bin/tessarr --config ~/.tessarr --check-config
```

For a shared seedbox that must listen beyond loopback, create credentials
interactively on the host before exposing the new process:

```bash
~/.local/bin/tessarr --config ~/.tessarr --set-auth admin
```

Use the host's TLS reverse proxy for the single assigned Tessarr port.
Remote first-time registration is intentionally rejected on non-loopback
listeners.

On a shared host, bind Tessarr directly to a provider-assigned private
address instead of `0.0.0.0` when one is available. The native installation
guide includes a rootless private HTTPS setup that keeps Tessarr
username/password authentication as a second layer and uses no additional
public port.

The release archive includes a user-level systemd unit. See the
[native installation guide](docs/src/content/docs/guides/installation.md) for
service and non-systemd options.

### Docker

Docker remains supported, but it is not required:

```yaml
services:
  tessarr:
    image: ghcr.io/trifocals3537/tessarr:beta
    container_name: tessarr
    ports:
      - "8282:8282"
    volumes:
      - /mnt/:/mnt:rshared
      - ./configs/:/app # config.json must be in this directory
    restart: unless-stopped
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

Initialize the administrator credentials before the first start. This keeps
the container's published listener and WebDAV endpoint authenticated without
placing a password in Compose or the image environment:

```bash
mkdir -p configs
docker compose run --rm tessarr /usr/bin/tessarr --config /app --set-auth admin
docker compose up -d
```

The command prompts for the password and stores its hash in the protected
`./configs/auth.json` file. Do not start a remotely published fresh container
until this initialization succeeds.

If Plex, Jellyfin, Emby, or another consumer runs in a separate Linux
container, do not use Docker's default `rprivate` bind for the Tessarr mount.
Mount it read-only with `rslave` so a Tessarr unmount/remount propagates from
the host into the already-running media container:

```yaml
services:
  jellyfin:
    volumes:
      - type: bind
        source: /mnt/tessarr
        target: /mnt/tessarr
        read_only: true
        bind:
          propagation: rslave
```

Without this, the host mount can be healthy while the media container keeps the
old FUSE mount and reports `Transport endpoint is not connected`. See the
[Docker installation notes](docs/src/content/docs/guides/installation.md#media-server-containers)
for verification and managed-host guidance.

## Documentation

See the [documentation source](docs/src/content/docs/) while the fork's
standalone documentation site is being prepared.

See the [changelog](CHANGELOG.md) for the upcoming release and upgrade notes.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Project history

Tessarr began as a fork of
[Decypharr](https://github.com/sirrobot01/decypharr) and remains grateful for
that project's foundation. Tessarr is now independently named, packaged, and
maintained. Existing installations can use the
[offline migration guide](docs/src/content/docs/guides/migration-from-decypharr.md)
to copy and verify their state without altering the original installation.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
