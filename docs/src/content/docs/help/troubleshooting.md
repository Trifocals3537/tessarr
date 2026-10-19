---
title: Troubleshooting
description: Solutions to common issues.
---

## Installation Issues

### Docker container won't start

**Check logs:**

```bash
docker logs tessarr
```

**Common causes:**

- Port 8282 already in use
- Invalid volume paths
- Permission issues on mounted directories

**Fix:**

```yaml
# docker-compose.yml
services:
  tessarr:
    ports:
      - "8283:8282"  # Use different external port
    volumes:
      - ./config:/config
    user: "1000:1000"  # Match your user ID
```

### Binary: "permission denied"

Make executable:

```bash
chmod +x tessarr
./tessarr
```

## Authentication Issues

### API token not working

1. While logged in, regenerate the token from **Settings** → **Auth**, or use
   the old token once to rotate it:
   ```bash
   curl -X POST -H "Authorization: Bearer OLD_TOKEN" \
     http://localhost:8282/api/refresh-token
   ```
2. Save the returned `token` value; configuration reads intentionally do not
   reveal it.
3. Ensure the Authorization header format is `Bearer YOUR_TOKEN`.

## Mount Issues

### Mount point empty

**Check mount status:**

```bash
ls -la /mnt/tessarr
mount | grep tessarr
```

**Common causes:**

- FUSE not available
- Permission issues
- Mount path doesn't exist

**Fix (Docker):**

```yaml
services:
  tessarr:
    devices:
      - /dev/fuse
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

**Fix (Binary):**

```bash
# Install FUSE
sudo apt install fuse  # Debian/Ubuntu
sudo yum install fuse  # CentOS/RedHat

# Add user to fuse group
sudo usermod -a -G fuse $USER
```

### "Transport endpoint is not connected"

First determine whether the Tessarr mount itself failed or only a Docker
consumer retained an old private bind:

```bash
# Host view
stat /mnt/tessarr

# Media-container view
docker exec jellyfin stat /mnt/tessarr
docker inspect jellyfin --format '{{range .Mounts}}{{.Destination}} {{.Propagation}}{{println}}{{end}}'
```

If the host succeeds but the container reports `Transport endpoint is not
connected`, restart the media container so it attaches to the current mount,
then configure its bind as read-only `rslave` to prevent recurrence:

```bash
docker restart jellyfin
```

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

If the host check also fails, unmount the disconnected FUSE mount and restart
Tessarr:

```bash
# Force unmount
sudo fusermount -u /mnt/tessarr
# or
sudo umount -l /mnt/tessarr

# Restart Tessarr
docker restart tessarr
```

### "Permission denied" accessing files

Set correct UID/GID:

```json
{
  "mount": {
    "dfs": {
      "uid": 1001,
      "gid": 1001
    }
  }
}
```

Find your user ID:

```bash
id your_username
```

## Debrid Provider Issues

### "API key invalid"

1. Verify key at provider dashboard
2. Check for typos/extra spaces
3. Regenerate API key
4. Update config and restart

### "No free slots"

**Check slots:**

- Real Debrid: https://real-debrid.com/torrents
- All Debrid: Dashboard

**Solutions:**

1. Remove old torrents manually
2. Configure slot management:
   ```json
   {
     "debrids": [
       {
         "minimum_free_slot": 10,
         "limit": 100
       }
     ]
   }
   ```
3. Add backup provider

### Rate limit errors

Reduce request rate:

```json
{
  "debrids": [
    {
      "rate_limit": "100/minute",
      "workers": 25
    }
  ]
}
```

Or add more API keys:

```json
{
  "download_api_keys": ["KEY1", "KEY2", "KEY3"]
}
```

### "Link unavailable" or "Download failed"

Links expire after `auto_expire_links_after` (default 24h). Repair worker should fix automatically.

**Manual fix:**

1. Go to Repair page
2. Click "Scan Now"
3. Process detected jobs

## Usenet Issues

### Connection failed to NNTP server

**Check connectivity:**

```bash
telnet news.provider.com 563
```

**Verify config:**

- Correct host/port
- Valid username/password
- SSL enabled if provider requires
- Provider account active

### NZB processing timeout

Increase timeout:

```json
{
  "usenet": {
    "processing_timeout": "20m",
    "import_availability_sample_percent": 1
  }
}
```

### Incomplete downloads

1. Enable repair:
   ```json
   {"repair": {"skip_nzb_repair": false}}
   ```
2. Check provider retention (old content may be incomplete)
3. Try different provider

### "Too many connections"

Provider limit exceeded. Reduce:

```json
{
  "usenet": {
    "max_connections": 10
  }
}
```

And per-provider:

```json
{
  "usenet": {
    "providers": [
      {"max_connections": 15}
    ]
  }
}
```

## Arr Integration Issues

### Test connection fails

1. **Check accessibility:**
   ```bash
   curl http://tessarr:8282/version
   ```
2. **Verify credentials in Arr match config**
3. **Check firewall rules**
4. **Try IP instead of hostname**

### Downloads stuck "Queued" in Arr

1. Check Tessarr logs for errors
2. Verify Debrid provider has free slots
3. Check `download_uncached` setting in Arr config
4. Manually test adding torrent via Tessarr UI

### Files not importing

**Path mapping issue:**

Arr and Tessarr must see files at identical paths:

```yaml
# Both services
volumes:
  - /mnt/media:/mnt/media
```

**Check download action:**

```json
{
  "arrs": [
    {
      "name": "Sonarr",
      "download_action": "symlink"  # For mounts
    }
  ]
}
```

### Torrent shows completed but Arr shows downloading

Arr sync delay. Wait 1-2 minutes or trigger manual import in Arr.

## Performance Issues

### High CPU usage

**Check:**

```bash
docker stats tessarr
```

**Causes:**

- Too many concurrent workers
- Repair loop
- Usenet processing

**Solutions:**

1. Reduce workers:
   ```json
   {
     "debrids": [{"workers": 25}],
     "repair": {"workers": 1, "enabled": false}
   }
   ```
2. Temporarily disable repair
3. Check logs for errors/loops

### High memory usage

**Usenet streams use disk buffer**. Check:

```json
{
  "usenet": {
    "disk_buffer_path": "/cache/usenet"
  }
}
```

Ensure sufficient disk space.

For Rclone:

```json
{
  "mount": {
    "rclone": {
      "vfs_cache_max_size": "5GB"
    }
  }
}
```

### Slow streaming/buffering

**DFS:**

```json
{
  "mount": {
    "dfs": {
      "chunk_size": "20MB",
      "disk_cache_size": "100GB"
    }
  }
}
```

**Usenet:**

```json
{
  "usenet": {
    "read_ahead": "32MB",
    "max_connections": 20
  }
}
```

**Check provider performance:**

- Test speed from Debrid website
- Try different Usenet provider
- Check network bandwidth

### Repeated playback errors after a Debrid link expires

Tessarr validates a generated download URL and replaces it once when the CDN
rejects it. If the replacement is also rejected, Tessarr continues probing
that cached URL for recovery but temporarily defers another provider API
refresh. The delay increases from 30 seconds to a maximum of 5 minutes for a
file that keeps failing, and clears immediately when the URL works again.

If logs report `link_refresh_cooldown`, check the provider status and the host's
clock. No configuration change is required; the cooldown is isolated to that
provider placement and file, so other playback remains unaffected.

### Database growing large

Config stored in `config.json` (text). No separate database.

If logs are large:

```json
{"log_level": "warn"}
```

## WebDAV Issues

### Can't connect to WebDAV

1. **Test URL:**
   ```bash
   curl http://localhost:8282/webdav/
   ```
2. **Check auth:**
   ```json
   {
     "use_auth": true,
     "enable_webdav_auth": true
   }
   ```
3. **Verify credentials match config**

### Authentication keeps prompting

Clear browser cache or saved credentials.

For apps, provide full URL with auth:

```
http://username:password@tessarr:8282/webdav/
```

### Files won't play in WebDAV client

WebDAV has no local cache - streaming depends on Debrid/Usenet speed. For better performance, use DFS mount instead of
WebDAV.

### Debrid streams return 429 or stall under concurrent load

Tessarr automatically shares a concurrency budget across playback, seeks,
link probes, and background downloads for each Debrid download account.
Playback keeps a reserved slot whenever the current budget has at least two
slots and is admitted ahead of queued bulk work. A
`429 Too Many Requests` response temporarily reduces that account's budget,
honors `Retry-After`, and then restores capacity gradually after healthy
responses.

Inspect the secret-free governor snapshot without enabling debug logging:

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/debug/stats | jq .cdn_traffic
```

The `throttles`, `current_limit`, `waiting_interactive`, and
`waiting_background` fields show whether provider-side CDN pressure is the
cause. `rate_limit` and `download_rate_limit` govern provider API calls; they
do not control CDN response bodies.

## Repair Worker Issues

### Repair jobs stuck "Processing"

1. **Raise workers:**
   ```json
   {"repair": {"workers": 10}}
   ```
2. **Check provider rate limits** (probes may be throttled by `repair_rate_limit` or `nntp_connection_percent`).
3. **Restart Tessarr:**
   ```bash
   docker restart tessarr
   ```

### False positives in repair

Lengthen `recheck_interval` so brief outages don't flap entries through `broken`, and turn off `auto_repair` to review brokens before they're acted on:

```json
{
  "repair": {
    "recheck_interval": "336h",
    "auto_repair": false
  }
}
```

Brokens then sit in the Browse UI with their reason; you can fire **Recheck health** on individual entries. Fixes should be triggered through the Arr integration.

### Repair not detecting issues

1. **Verify repair enabled and scheduled:**
   ```json
   {"repair": {"enabled": true, "schedule": "0 4 * * *"}}
   ```
2. **Check per-Arr skip:**
   ```json
   {
     "arrs": [
       {"skip_repair": false}
     ]
   }
   ```
3. **Trigger a sweep now:**
   ```bash
   curl -X POST -H "Authorization: Bearer TOKEN" \
     http://localhost:8282/api/repair/run
   ```
4. **Force-recheck a specific entry:**
   ```bash
   curl -X POST -H "Authorization: Bearer TOKEN" \
     'http://localhost:8282/api/repair/health/My.Show.S01/check'
   ```

## Debugging

### Enable debug logging

```json
{"log_level": "debug"}
```

**Docker:**

```bash
docker logs -f tessarr
```

**Binary:**

```bash
./tessarr 2>&1 | tee tessarr.log
```

### Check configuration

**View current config:**

```bash
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/config | jq
```

**Validate JSON:**

```bash
cat /config/config.json | jq
```

### Health check

```bash
# Version
curl http://localhost:8282/version

# Torrents (requires auth)
curl -H "Authorization: Bearer TOKEN" \
  http://localhost:8282/api/torrents
```

### Reset to defaults

Stop Tessarr first, then move the configuration aside so it can be restored
if needed. For a native install:

```bash
mv ~/.tessarr/config.json \
  ~/.tessarr/config.json.backup-$(date +%Y%m%d-%H%M%S)
```

For the documented container image, use the mounted `/app` directory instead:

```bash
mv /app/config.json /app/config.json.backup-$(date +%Y%m%d-%H%M%S)
```

Restart Tessarr and the setup wizard will run. Keep the backup until the new
configuration is verified.

## Getting Help

If you can't resolve the issue:

1. **Report a reproducible issue in this fork:** https://github.com/Trifocals3537/tessarr/issues
2. **Provide:**
    - Tessarr version (`/version`)
    - Relevant logs (with `log_level: debug`)
    - Config (sensitive values redacted)
    - Steps to reproduce
3. **Include system info:**
    - OS/Docker version
    - Mount type
    - Providers used

**Sanitize config before sharing:**

```bash
cat config.json | sed 's/"api_key": ".*"/"api_key": "REDACTED"/g'
```
