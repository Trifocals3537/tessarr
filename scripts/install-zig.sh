#!/usr/bin/env bash
set -euo pipefail

readonly supported_version="0.14.1"
readonly expected_sha256="24aeeec8af16c381934a6cd7d95c807a8cb2cf7df9fa40d359aa884195c4716c"

version="${1:-${supported_version}}"
if [[ "${version}" != "${supported_version}" ]]; then
    printf 'Unsupported Zig version: %s (expected %s)\n' \
        "${version}" "${supported_version}" >&2
    exit 1
fi

case "$(uname -m)" in
    x86_64|amd64)
        platform="x86_64-linux"
        ;;
    *)
        printf 'Unsupported Zig installer architecture: %s\n' "$(uname -m)" >&2
        exit 1
        ;;
esac

archive="zig-${platform}-${version}.tar.xz"
download_url="https://ziglang.org/download/${version}/${archive}"
temporary_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
install_root="$(mktemp -d "${temporary_root%/}/tessarr-zig-${version}.XXXXXX")"
archive_path="${install_root}/${archive}"
zig_dir="${install_root}/${archive%.tar.xz}"

curl \
    --fail \
    --location \
    --retry 2 \
    --retry-connrefused \
    --retry-delay 2 \
    --connect-timeout 15 \
    --max-time 600 \
    --output "${archive_path}" \
    "${download_url}"

printf '%s  %s\n' "${expected_sha256}" "${archive_path}" | sha256sum --check --status
tar --extract --file "${archive_path}" --directory "${install_root}"

if [[ ! -x "${zig_dir}/zig" ]]; then
    printf 'Downloaded Zig archive did not contain the expected executable\n' >&2
    exit 1
fi

if [[ -n "${GITHUB_PATH:-}" ]]; then
    printf '%s\n' "${zig_dir}" >> "${GITHUB_PATH}"
else
    printf 'GITHUB_PATH is unset; add %s to PATH manually\n' "${zig_dir}" >&2
fi

printf 'Installed Zig %s from %s\n' "$("${zig_dir}/zig" version)" "${download_url}"
