# CodeQL developer-tool findings (2026-10-19)

This review covers the five open findings on beta
`a0b2cc4f70c29cc96ac52ba094f4aab4f1c3eb48`. Alert numbers refer to the standalone
`Trifocals3537/tessarr` repository. These tools are outside the shipped Tessarr
server, but build inputs and diagnostic output still deserve protection.

## Disposition

| Alert | Rule and original location | Review and action |
| --- | --- | --- |
| [1](https://github.com/Trifocals3537/tessarr/security/code-scanning/1) | `js/http-to-file-access`, `scripts/download-assets.js:96` | Hardened: verify all downloaded bytes against committed SHA-256 pins before writing any asset. Fixed destinations, HTTPS without redirects, a 30-second deadline, a 1 MiB per-file limit, and atomic per-file replacement bound failures. |
| [2](https://github.com/Trifocals3537/tessarr/security/code-scanning/2) | `go/path-injection`, `cmd/test-parser/main.go:42` | False positive for the current local CLI trust boundary; evidence below. No path restriction or scanner exclusion added. |
| [3](https://github.com/Trifocals3537/tessarr/security/code-scanning/3) | `go/log-injection`, `cmd/test-parser/main.go:104` | Fixed: quote NZB names in the summary. |
| [4](https://github.com/Trifocals3537/tessarr/security/code-scanning/4) | `go/log-injection`, `cmd/test-parser/main.go:111` | Fixed: quote file names in the file heading. |
| [5](https://github.com/Trifocals3537/tessarr/security/code-scanning/5) | `go/log-injection`, `cmd/test-parser/main.go:120` | Fixed: quote file names in the entry details. |

The summary also quotes IDs, internal archive paths, and newsgroup names using
Go's `%q` format. This escapes line breaks, terminal escape sequences, and other
non-printable characters while preserving readable Unicode. It changes display
only; parser inputs, paths, and stored identifiers remain intact. Passwords remain
redacted. Diagnostic logging uses zerolog's JSON writer instead of a console
formatter, so untrusted names and errors stay JSON-escaped.

## Why the CLI path alert is a false positive

The entire reported path flow is `os.Args[1]` -> `nzbFile` ->
`os.ReadFile(nzbFile)`. The documented interface is `test-parser <nzb-file>`.
An operator intentionally chooses a local NZB file, including absolute paths or
paths outside the current directory. The process reads with that operator's OS
permissions. There is no advertised restricted directory to escape, privileged
helper, HTTP handler, or remote caller choosing this argument.

`cmd/test-parser` is a separate `package main`. Production builds in `Dockerfile`,
`.github/workflows/release.yml`, and `.github/workflows/ci.yml` compile the root
Tessarr application; the container separately compiles `cmd/healthcheck/main.go`.
None packages or invokes `cmd/test-parser`. The root application does not import
this diagnostic program. Its NZB metadata can still be hostile, which is why the
three output findings are fixed rather than dismissed.

Restricting this argument to a basename or the current directory would break the
intended CLI without enforcing an existing security boundary. Do not expose this
utility through an elevated service or a wrapper accepting untrusted callers. If
that boundary changes, re-open this assessment and enforce the wrapper's allowed
root using an OS-backed directory boundary. This finding may be dismissed as
**false positive**, with this document linked as the rationale; it is not an
accepted exploitable path traversal.

## Asset pin provenance and maintenance

The four downloads retain their existing versions and filenames. On 2026-10-19,
each CDN response matched the same file from its upstream npm package archive.
Before comparing files, the archives were verified against the registry's
published SHA-512 integrity value:

- [bootstrap-icons 1.11.2 metadata](https://registry.npmjs.org/bootstrap-icons/1.11.2),
  archive integrity `sha512-TgdiPv+IM9tgDb+dsxrnGIyocsk85d2M7T0qIgkvPedZeoZfyeG/j+yiAE4uHCEayKef2RP05ahQ0/e9Sv75Wg==`.
- [jquery 3.7.1 metadata](https://registry.npmjs.org/jquery/3.7.1), archive integrity
  `sha512-m4avr8yL8kmFN8psrbFFFmB/If14iN5o9nw/NgnnM+kybDJpRsAynV2BsfpTYrTRysYUdADVD7CkUUizgkpLfg==`.

| Upstream package file | SHA-256 of original bytes |
| --- | --- |
| `bootstrap-icons/font/bootstrap-icons.css` | `bd6146337f91518bdf30381cd35ab6db9ea4056572bfb7d8c7d6a3bd642c7fba` |
| `bootstrap-icons/font/fonts/bootstrap-icons.woff` | `d0346eea5a5ca9998be3c506fc655f0cbfccb42deb5fc6df39815425af867bb5` |
| `bootstrap-icons/font/fonts/bootstrap-icons.woff2` | `5b2dd4d4f81cd1f52a50b0833ea12c8f63f2c4ae4d2c5a799fcc741feb2ea40f` |
| `jquery/dist/jquery.min.js` | `fc9a93dd241f6b045cbff0481cf4e1901becd0e12fb45166a8f17f95823f0b1a` |

Verify raw CSS before rewriting `./fonts/` URLs to `../fonts/`. Review version,
URL, package integrity, file digests, licenses, and regenerated assets together
when updating these pins. Do not automatically replace a pin on mismatch. The
existing checked-in browser assets remain usable when an upstream download
fails. All four responses must pass before any destination changes; each file
is replaced atomically, although a filesystem failure during the write phase
can still leave a partially updated set of already-verified files.

The [network-to-file query](https://codeql.github.com/codeql-query-help/javascript/js-http-to-file-access/)
also flags intentional downloads. If it still reports this flow after hardening,
the remaining result is a false positive only after confirming that the committed
digest comparison dominates the write. Do not dismiss an unverified download or
disable the query. No query, language, threat-model, or directory exclusion is
introduced by this change.

## Validation

`npm test` includes regression coverage for tampering, unsuccessful and redirected
responses, size limits, interrupted streams, deadlines, and preservation of
existing assets on verification failure. `go test ./cmd/test-parser` checks the
actual summary output with hostile control characters and readable Unicode,
including password redaction and unchanged parser data. A real pinned download
and normal browser build verify compatibility. The integration PR records the
protected CI and extended CodeQL results for the final revision.

Main promotion and deployment remain separate actions.
