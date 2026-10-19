# Go dependency advisory follow-up (2026-10-19)

The release-readiness scan of beta `993ea2f7df5ea02840fb0401a6b66df0de1b50e9`
reported ten advisories at module level, with none in imported packages or called
symbols. This update removes all nine findings that have patched versions. The
remaining OpenPGP finding has no fixed version and affects packages that the
application does not import.

## Dependency changes

| Module | Previous | Selected |
| --- | --- | --- |
| `golang.org/x/net` | `v0.56.0` | `v0.60.0` |
| `golang.org/x/crypto` | `v0.53.0` | `v0.57.0` |
| `golang.org/x/text` | `v0.39.0` | `v0.42.0` |
| `golang.org/x/sys` | `v0.46.0` | `v0.48.0` |
| `golang.org/x/term` | `v0.44.0` | `v0.46.0` |
| `golang.org/x/sync` | `v0.21.0` | `v0.23.0` |

`x/net v0.60.0` is the first release fixing all five networking findings. Its
module requirements select `x/crypto v0.57.0`, `x/text v0.42.0`, `x/sys v0.48.0`,
and `x/term v0.46.0`; `x/text` also requires `x/sync v0.23.0`. These requirements
explain the companion updates. They all support the existing **Go 1.26.9** pin.
`go mod tidy` records `x/text` as a direct dependency because application path
normalization imports it directly.

## Findings removed by the updates

| Advisory | Affected code | First fixed module version |
| --- | --- | --- |
| [GO-2026-6355](https://pkg.go.dev/vuln/GO-2026-6355) | SSH established-channel deadlock | `x/crypto v0.56.0` |
| [GO-2026-6354](https://pkg.go.dev/vuln/GO-2026-6354) | SSH undecided-channel deadlock | `x/crypto v0.56.0` |
| [GO-2026-6303](https://pkg.go.dev/vuln/GO-2026-6303) | SSH source-address option enforcement | `x/crypto v0.55.0` |
| [GO-2026-6617](https://pkg.go.dev/vuln/GO-2026-6617) | HTTP/2 HPACK encoder race | `x/net v0.60.0` |
| [GO-2026-6612](https://pkg.go.dev/vuln/GO-2026-6612) | HTTP/2 flow-control refunds | `x/net v0.60.0` |
| [GO-2026-6611](https://pkg.go.dev/vuln/GO-2026-6611) | HTTP/2 window-change CPU exhaustion | `x/net v0.60.0` |
| [GO-2026-6610](https://pkg.go.dev/vuln/GO-2026-6610) | HTTP/2 framing-header validation | `x/net v0.60.0` |
| [GO-2026-6603](https://pkg.go.dev/vuln/GO-2026-6603) | HTTP/2 trailer memory exhaustion | `x/net v0.60.0` |
| [GO-2026-6629](https://pkg.go.dev/vuln/GO-2026-6629) | PRECIS crafted-input panic | `x/text v0.41.0` |

Some HTTP/2 records also cover the Go standard library. The retained Go 1.26.9
toolchain already contains the applicable standard-library fixes; the updates
above address the separately versioned `x/net` module.

## Remaining unused-code finding

[GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932) covers
`golang.org/x/crypto/openpgp` and its subpackages. Upstream marks this OpenPGP
implementation unmaintained and unsafe by design; **all versions are affected,
with no known fixed version**. Updating `x/crypto` therefore cannot remove this
module-level finding.

The application retains `x/crypto` for bcrypt authentication. Neither production
code nor test dependencies import `openpgp` or any of its subpackages in the
Linux/amd64 (CGO enabled) or Windows/amd64 (CGO disabled) dependency graphs. The
vulnerable packages are outside the compiled dependency closure for those tested
targets. This is a documented unused-package finding, not a claim that the entire
`x/crypto` module is free of advisories.

No advisory suppression, vendored patch, package replacement, or scanner bypass
is added. Keep the existing vulnerability check enabled. Any future OpenPGP
feature must select a maintained implementation and repeat the import and
vulnerability checks; this record does not authorize using the affected package.

## Verification and reproduction

The post-update source scans used **Go 1.26.9**, **govulncheck 1.7.0**, and the
official database updated **2026-10-19 21:08:39 UTC**. Both Linux/amd64 with CGO
enabled and Windows/amd64 with CGO disabled report:

- Zero vulnerabilities in called symbols.
- Zero vulnerabilities in imported packages.
- One module-level finding: `GO-2026-5932` only.

Run these from the repository root with the pinned scanner installed. Repeat for
each supported build target, including the platform-specific CGO setting:

```sh
GOTOOLCHAIN=go1.26.9 GOOS=linux GOARCH=amd64 CGO_ENABLED=1 govulncheck -show=verbose ./...
GOTOOLCHAIN=go1.26.9 GOOS=windows GOARCH=amd64 CGO_ENABLED=0 govulncheck -show=verbose ./...
GOTOOLCHAIN=go1.26.9 GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go list -mod=readonly -deps -test ./...
GOTOOLCHAIN=go1.26.9 GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go list -mod=readonly -deps -test ./...
```

The dependency listings must contain no package equal to
`golang.org/x/crypto/openpgp` or beginning with `golang.org/x/crypto/openpgp/`.
Recheck against the current database whenever dependencies, imports, build tags,
or supported targets change. CI results and the integration PR record the test,
race, static-analysis, and native compatibility checks for the final revision.

This change updates source dependencies and this record. Live-service validation
and deployment are separate actions.
