---
title: Migrate from Decypharr
description: Safely copy and verify an existing installation before switching to Tessarr.
---

Tessarr includes an offline migration command for existing Decypharr state.
It copies the source tree, renames known internal state markers, verifies every
copied file, and leaves the source unchanged.

## Before you begin

1. Stop the old service so its database and configuration cannot change during
   the copy.
2. Keep the old binary, service file, and state directory until Tessarr has
   been validated.
3. Choose a new Tessarr state directory. Do not place it inside the source
   directory or make either path a symlink.

Configured media, staging, cache, and mount paths are preserved. The migration
does not move media or rewrite paths outside the state directory.

## Preview the migration

```bash
tessarr migrate-from-decypharr \
  --source "$HOME/.decypharr" \
  --target "$HOME/.tessarr" \
  --dry-run
```

Review the reported source, target, file count, and marker renames. The command
rejects unsafe roots, symlinks, special files, and destination-name collisions.

## Copy and verify

```bash
tessarr migrate-from-decypharr \
  --source "$HOME/.decypharr" \
  --target "$HOME/.tessarr"
```

The destination is built in a staging directory and published only after all
files pass hash verification. A migration receipt records the completed copy.
Re-running the same completed migration is safe.

## Start Tessarr

Install and enable the `tessarr` binary and `tessarr.service`, then start it
with the new directory:

```bash
tessarr --config "$HOME/.tessarr" --check-config
systemctl --user start tessarr.service
```

Confirm the version endpoint, web interface, mount, and real beginning/middle/end
file reads before restarting Plex, Jellyfin, or Arr applications.

Environment variables now use the `TESSARR_` prefix. Translate any explicit
old service overrides to their equivalent Tessarr names before starting the
new service.

## Roll back

Stop Tessarr and start the old service with its original, unchanged state
directory. Do not point both programs at the same state tree or run them at the
same time. Because migration is copy-only, rollback does not require restoring
the source directory.
