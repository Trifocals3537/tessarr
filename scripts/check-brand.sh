#!/usr/bin/env bash
set -euo pipefail

# Legacy identifiers are allowed only where compatibility, migration, or
# attribution requires their exact spelling. The current GitHub repository URL
# is also permitted; its name is independent of the Tessarr application name.
git rev-parse --is-inside-work-tree >/dev/null

failed=0

allowed_legacy_reference() {
    local path="$1"
    local content="$2"
    case "$path" in
        README.md|Dockerfile|packaging/systemd/tessarr.service|\
        docs/astro.config.mjs|docs/src/components/Header.astro|\
        docs/src/content/docs/index.mdx|\
        docs/src/content/docs/guides/installation.md|\
        docs/src/content/docs/guides/configuration.md|\
        docs/src/content/docs/help/troubleshooting.md|\
        pkg/server/templates/layout.html|pkg/server/assets/js/common.js|\
        pkg/server/assets/build/js/common.js)
            # Permit only the verified repository address on these lines,
            # keeping the check active for unrelated legacy product wording.
            local without_repo_url="${content//https:\/\/github.com\/Trifocals3537\/decypharr/}"
            if [[ "$content" != "$without_repo_url" && "${without_repo_url,,}" != *decypharr* ]]; then
                return 0
            fi
            ;;
    esac
    case "$path" in
        docs/src/content/docs/guides/migration-from-decypharr.md|\
        internal/migration/legacy.go|internal/migration/legacy_test.go|\
        migration_command.go|migration_command_test.go|\
        pkg/manager/legacy_artifact_names.go|pkg/manager/legacy_usenet_adoption.go|\
        pkg/server/legacy_protocol_names.go|pkg/usenet/fs/reader/legacy_cache_names.go|\
        scripts/check-brand.sh)
            return 0
            ;;
        README.md)
            [[ "$content" == *'[Decypharr](https://github.com/sirrobot01/decypharr)'* ||
               "$content" == *'migration-from-decypharr.md'* ]]
            return
            ;;
        docs/astro.config.mjs)
            [[ "$content" == *"label: 'Migrate from Decypharr', link: '/guides/migration-from-decypharr'"* ]]
            return
            ;;
        main.go)
            [[ "$content" == *'os.Args[1] == "migrate-from-decypharr"'* ]]
            return
            ;;
        pkg/manager/strm.go)
            [[ "$content" == *'decypharr-strm-root'* ]]
            return
            ;;
        pkg/manager/usenet_watcher_identity.go)
            [[ "$content" == *'decypharr/watched-nzb/v1'* ]]
            return
            ;;
        pkg/strm/strm.go)
            [[ "$content" == *'decypharr-strm'* ]]
            return
            ;;
        pkg/usenet/fs/reader/cache.go)
            [[ "$content" == *'decypharr stream cache v1'* ||
               "$content" == *'decypharr segment cache v1'* ]]
            return
            ;;
        pkg/debrid/types/file_keys.go)
            [[ "$content" == *'lower == ".decypharr-torrent-owner-v1"'* ||
               "$content" == *'lower == ".decypharr-torrent-ownership.lock"'* ||
               "$content" == *'strings.HasPrefix(lower, ".decypharr-torrent-quarantine-")'* ||
               "$content" == *'strings.HasPrefix(lower, ".decypharr-torrent-part-")'* ]]
            return
            ;;
        pkg/debrid/types/file_keys_test.go)
            [[ "$content" == *'{Id: "legacy-part", Path: ".decypharr-torrent-part-provider/Episode.mkv"}'* ]]
            return
            ;;
        pkg/manager/torrent_ownership_test.go)
            [[ "$content" == *'{Id: "partial", Path: ".decypharr-torrent-part-provider/Episode.mkv", Size: 2}'* ]]
            return
            ;;
    esac
    return 1
}

while IFS=: read -r path line content; do
    if ! allowed_legacy_reference "$path" "$content"; then
        printf 'unexpected legacy brand reference: %s:%s:%s\n' "$path" "$line" "$content" >&2
        failed=1
    fi
done < <(git grep -Iin 'decypharr' -- . || true)

while IFS= read -r path; do
    if [[ "$path" != "docs/src/content/docs/guides/migration-from-decypharr.md" ]]; then
        printf 'unexpected legacy brand in tracked path: %s\n' "$path" >&2
        failed=1
    fi
done < <(git ls-files | grep -i 'decypharr' || true)

for workflow in .github/workflows/beta-docker.yml .github/workflows/release.yml; do
    for expected in \
        'org.opencontainers.image.title=Tessarr' \
        'org.opencontainers.image.description=Self-hosted debrid and Usenet media bridge with qBittorrent and SABnzbd-compatible APIs' \
        'org.opencontainers.image.source=https://github.com/${{ github.repository }}' \
        'org.opencontainers.image.url=https://github.com/${{ github.repository }}' \
        'org.opencontainers.image.documentation=https://github.com/${{ github.repository }}/blob/${{ github.sha }}/README.md' \
        'org.opencontainers.image.authors=Tessarr contributors' \
        'org.opencontainers.image.licenses=MIT'; do
        if ! grep -Fq "$expected" "$workflow"; then
            printf 'missing Tessarr image metadata in %s: %s\n' "$workflow" "$expected" >&2
            failed=1
        fi
    done
done

if grep -Eq '^ENV[[:space:]]+[^#]*(AUTH|PASSWORD|SECRET|TOKEN)' Dockerfile; then
    printf 'Dockerfile contains an ENV name that Docker will classify as a secret\n' >&2
    failed=1
fi
if ! grep -Fxq '# check=error=true' Dockerfile; then
    printf 'Dockerfile must fail the build when Docker reports a validation warning\n' >&2
    failed=1
fi
for expected in \
    'HEALTHCHECK --interval=10s --timeout=35s --start-period=120s --retries=10 CMD ["/container-defaults.sh", "/usr/bin/healthcheck", "--config", "/app"]' \
    'ENTRYPOINT ["/container-defaults.sh", "/entrypoint.sh"]'; do
    if ! grep -Fq "$expected" Dockerfile; then
        printf 'Docker process is missing shared secure defaults: %s\n' "$expected" >&2
        failed=1
    fi
done
if ! grep -Fxq 'TESSARR_ENABLE_WEBDAV_AUTH=${TESSARR_ENABLE_WEBDAV_AUTH:-true}' scripts/container-defaults.sh; then
    printf 'container defaults must protect unset and empty WebDAV authentication values\n' >&2
    failed=1
fi

if (( failed )); then
    exit 1
fi

printf 'Tessarr brand boundary verified.\n'
