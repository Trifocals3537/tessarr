#!/usr/bin/env bash
set -euo pipefail

failed=0

if grep -RIn --include='*.yml' --include='*.yaml' 'ubuntu-latest' .github/workflows; then
    printf 'GitHub Actions workflows must use an explicit Ubuntu runner version\n' >&2
    failed=1
fi

if grep -RIn --include='*.yml' --include='*.yaml' 'mlugg/setup-zig' .github/workflows; then
    printf 'Workflows must use the repository-owned, checksum-verified Zig installer\n' >&2
    failed=1
fi

for workflow in .github/workflows/ci.yml .github/workflows/release.yml; do
    if ! grep -Fq 'bash scripts/install-zig.sh 0.14.1' "${workflow}"; then
        printf 'Pinned Zig installer invocation is missing from %s\n' "${workflow}" >&2
        failed=1
    fi
done

if ! grep -Fq \
    '24aeeec8af16c381934a6cd7d95c807a8cb2cf7df9fa40d359aa884195c4716c' \
    scripts/install-zig.sh; then
    printf 'Official Zig 0.14.1 checksum is missing from scripts/install-zig.sh\n' >&2
    failed=1
fi

if (( failed )); then
    exit 1
fi

printf 'GitHub Actions runner and Zig pins verified.\n'
