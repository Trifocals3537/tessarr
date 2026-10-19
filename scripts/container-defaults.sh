#!/bin/sh
set -e

# Keep security defaults identical for the main process and Docker healthcheck.
# Empty values are not valid overrides and must retain the secure default.
TESSARR_ENABLE_WEBDAV_AUTH=${TESSARR_ENABLE_WEBDAV_AUTH:-true}
export TESSARR_ENABLE_WEBDAV_AUTH

exec "$@"
