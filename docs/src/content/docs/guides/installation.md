---
title: Installation
description: Install Tessarr as a native Linux service or with Docker.
---

## Native Linux (Recommended)

Download the archive for your architecture from this fork's
[GitHub Releases](https://github.com/Trifocals3537/decypharr/releases).
Linux hosts need FUSE support and a compatible FUSE runtime (commonly
`libfuse2` on Ubuntu and Debian). Install rclone only if you plan to use the
rclone mount backend.

```bash
# Verify the downloaded archive against SHA256SUMS, then extract it.
sha256sum --check --ignore-missing SHA256SUMS
tar -xzf tessarr_Linux_x86_64.tar.gz

install -Dm755 tessarr ~/.local/bin/tessarr
mkdir -p ~/.tessarr

# First launch: creates config.json and starts the setup wizard.
~/.local/bin/tessarr --config ~/.tessarr
```

Native installs listen on `127.0.0.1:8282` by default so the unauthenticated
first-run setup wizard is not exposed to the network. To configure a remote
seedbox, open a tunnel:

```bash
ssh -L 8282:127.0.0.1:8282 user@seedbox
```

Then visit `http://127.0.0.1:8282`. For permanent remote access, prefer a
trusted reverse proxy. Set `bind_address` explicitly only when you intend to
listen on another interface. Tessarr refuses to start when authentication
is disabled on a non-loopback listener because the UI, APIs, and provider
configuration could otherwise be exposed over plain HTTP.

### Shared seedboxes

A native shared-seedbox installation uses one HTTP listener for the Web UI,
qBittorrent-compatible API, SABnzbd-compatible API, and WebDAV routes. When
the Arr applications are on the same host, bind Tessarr to `127.0.0.1`,
point those clients to `127.0.0.1`, and use an SSH tunnel for the UI; this
requires no public assigned port. If remote clients need direct access, only
one assigned inbound application port is required. Real-Debrid and TorBox use
outbound HTTPS connections; Usenet providers, including an NNTP endpoint
supplied by TorBox, use outbound NNTP or NNTPS connections. Those outbound
connections do not require additional assigned application ports.

Some shared hosts provide a private per-user address for communication between
applications. Bind to that address, point local automation clients to it, and
reach the UI through a private HTTPS service. Avoid binding a native shared-host
installation to `0.0.0.0`.
If the WebDAV endpoint is not needed, set `disable_webdav` to `true`. Otherwise
enable both application authentication and `enable_webdav_auth`.

Prefer the DFS mount backend when the host supplies `/dev/fuse` and allows
user mounts. DFS does not start rclone's remote-control listener. Select the
rclone backend only when rclone is installed and its extra local control
listener is acceptable on the host.

Do not expose a first-time registration page on a shared listener. When an
existing configuration uses a non-loopback address, set credentials
interactively on the host before starting the new binary:

```bash
~/.local/bin/tessarr --config ~/.tessarr --set-auth admin
```

The password is read twice without echo and is never placed in shell history
or process arguments. Remote registration and unauthenticated remote
credential changes are rejected. Authentication also cannot be disabled while
the service is listening on a non-loopback address. Put the single assigned
HTTP port behind the host's TLS reverse proxy; do not publish the same
unencrypted port directly to the Internet.

#### Rootless Tailscale HTTPS

Tailscale Serve is a good fit when a shared host has no supported TLS reverse
proxy for custom applications. It terminates private HTTPS inside the tailnet
and proxies to Tessarr's existing HTTP listener, so it does not require
Docker or another public port. Keep Tessarr authentication enabled as a
second layer; do not use Tailscale Funnel.

Bind Tessarr to the account's private address and restrict the listener to
that same source address:

```json
{
  "bind_address": "192.0.2.10",
  "port": "8282",
  "use_auth": true,
  "secure_session_cookie": true,
  "disable_webdav": true,
  "allowed_client_cidrs": [
    "192.0.2.10/32"
  ]
}
```

`192.0.2.10` is a documentation-only address. Replace it and the example port
with values assigned to your account. The list applies to the UI and all
compatibility routes and ignores forwarded-IP headers. Binding to the private
address keeps the listener off the public interface, while the allowlist
continues to admit local automation clients and the local Tailscale proxy.

For a named Tailscale Service:

1. In the Tailscale admin console, define `svc:tessarr` with endpoint
   `tcp:443` and grant only the intended users or devices access.
2. Confirm the seedbox node has a tag-based identity and Tailscale 1.86 or
   later.
3. Advertise the HTTPS proxy from the seedbox, using the correct path to the
   rootless client and socket:

   ```bash
   ~/.local/bin/tailscale --socket=/path/to/tailscaled.sock serve \
     --service=svc:tessarr --https=443 http://192.0.2.10:8282
   ```

4. Approve the service host if the tailnet does not auto-approve it.
5. Verify the named `https://tessarr.<tailnet>.ts.net` URL reaches the
   Tessarr login, both Arr clients still pass their tests, and the public
   host cannot connect to the assigned application port.

Named services run in the background by default and resume with the Tailscale
daemon. To roll back only this mapping, first drain it, then clear it:

```bash
~/.local/bin/tailscale --socket=/path/to/tailscaled.sock serve drain svc:tessarr
~/.local/bin/tailscale --socket=/path/to/tailscaled.sock serve clear svc:tessarr
```

See the official
[Tailscale Services guide](https://tailscale.com/docs/features/tailscale-services)
for service definition, approval, and access grants.

Before replacing an existing seedbox binary:

1. Run `tessarr --config PATH --check-config`.
2. Inspect the effective supervisor command and any overrides so you replace
   the binary it actually executes, not merely a similarly named file.
3. Confirm `bind_address`, `port`, and `use_auth` in the effective
   configuration.
4. Confirm the download, mount, cache, and Usenet buffer paths are owned and
   writable by the service account.
5. Confirm `/dev/fuse` and `fusermount3` are available when using DFS.
6. Keep a copy of the working binary and configuration outside the install
   path for rollback.
7. Stop the old process cleanly and verify its FUSE mount is gone before
   starting the replacement.
8. If the service was remotely exposed without authentication, run
   `--set-auth` while it is stopped, before starting the replacement.
9. Rerun `--check-config` after the final authentication, bind, and WebDAV
   choices; deploy only when it passes.

The live application tightens existing `config.json` and `auth.json`
permissions to `0600`. Ensure both files are owned by the service account.
Allow at least 90 seconds for a supervised shutdown so active HTTP work can
drain and the DFS mount can be released. Managed seedboxes often carry a
provider-specific service override; preserve its CPU/task limits and paths,
but update its effective binary path deliberately.

`--check-config` is for an existing configuration and never creates or changes
files. It rejects invalid client networks, a non-loopback deployment when
application authentication is disabled, or a deployment where WebDAV would
remain unprotected. It warns when a non-loopback listener has no client
network boundary. Normal startup and configuration updates enforce the same
deployment-safety checks:

```bash
~/.local/bin/tessarr --config ~/.tessarr --check-config
```

### Run as a user service

Linux release archives include a systemd user-service template. This keeps the
service under your account and does not require Docker or root access after the
host's FUSE prerequisites have been installed.

```bash
mkdir -p ~/.config/systemd/user
install -m 0644 tessarr.service ~/.config/systemd/user/tessarr.service

systemctl --user daemon-reload
systemctl --user enable --now tessarr
systemctl --user status tessarr
```

The supplied unit uses `~/.tessarr`, matching the application's native
default. Before relying on a user service after logout, check whether lingering
is enabled:

```bash
loginctl show-user "$USER" -p Linger
```

If it reports `Linger=no`, ask the host administrator or seedbox provider to
enable lingering for your account. Some managed seedboxes do not expose a user
systemd manager at all; in that case, use the provider's service manager,
supervisord, s6, or another process supervisor. As a basic non-restarting
fallback:

```bash
nohup ~/.local/bin/tessarr --config ~/.tessarr \
  >> ~/.tessarr/tessarr.log 2>&1 &
```

Logs for the systemd service are available with:

```bash
journalctl --user -u tessarr -f
```

## Docker (Alternative)

### Docker Compose

Create a `docker-compose.yml`:

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

Run:

```bash
mkdir -p configs
docker compose run --rm tessarr /usr/bin/tessarr --config /app --set-auth admin
docker compose up -d
```

The one-off command prompts for the administrator password before the
container publishes its listener. It writes the protected authentication state
to `./configs/`; the password is not placed in Compose or an environment
variable. The image also requires authentication on its WebDAV endpoint.

Access at `http://localhost:8282`

### Media server containers

Docker bind mounts default to `rprivate`. That is unsafe for a long-running
Plex, Jellyfin, or Emby container consuming a FUSE mount that Tessarr may
replace during an update or restart: the host sees the new mount, but the media
container can retain the disconnected old one.

Use a read-only bind with `rslave` propagation for each media consumer. This
propagates mount changes from the host into the consumer without propagating
consumer-created mounts back to the host:

```yaml
services:
  jellyfin:
    image: jellyfin/jellyfin:latest
    volumes:
      - type: bind
        source: /mnt/tessarr
        target: /mnt/tessarr
        read_only: true
        bind:
          propagation: rslave
```

The source path must be a bind mount on a Linux host whose parent supports
mount propagation. Keep the Tessarr side `rshared`, as shown above. After
changing propagation, recreate the media container once and verify the result:

```bash
docker compose up -d --force-recreate jellyfin
docker inspect jellyfin --format '{{range .Mounts}}{{.Destination}} {{.Propagation}}{{println}}{{end}}'
docker exec jellyfin stat /mnt/tessarr
```

If a managed host does not let you change the media container's bind
propagation, restart that media-server application after every Tessarr
unmount/remount. Restarting playback alone cannot replace the container's stale
mount reference.

### Docker Run

Initialize the administrator credentials in the persistent configuration
directory first:

```bash
mkdir -p config
docker run --rm -it \
  -v ./config:/app \
  -e PUID=1000 \
  -e PGID=1000 \
  ghcr.io/trifocals3537/tessarr:beta \
  /usr/bin/tessarr --config /app --set-auth admin
```

Then start the service:

```bash
docker run -d \
  --name=tessarr \
  -p 8282:8282 \
  -v ./config:/app \
  -v ./downloads:/downloads \
  -v ./cache:/cache \
  -e PUID=1000 \
  -e PGID=1000 \
    --restart unless-stopped \
    --device /dev/fuse:/dev/fuse:rwm \
    --cap-add SYS_ADMIN \
    --security-opt apparmor:unconfined \
  ghcr.io/trifocals3537/tessarr:beta
```

The container image explicitly listens on `0.0.0.0`; publish only the ports you
intend to expose.

## Next Steps

After installation, access the web UI. You'll be redirected to the [Setup Wizard](./quick-start/) for first-run
configuration.
