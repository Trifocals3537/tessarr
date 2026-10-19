# Tessarr release process

Tessarr releases are deliberate, manually dispatched operations. Do not create
or push `v*` tags directly.

## Cutover prerequisites

Before publishing the first Tessarr release:

1. Merge the release workflow into the protected `main` and `beta` branches.
2. Confirm that required tests and review rules protect both branches.
3. Add an active repository tag ruleset for `v*` that restricts tag creation,
   updates, and deletion.
4. Give only the release workflow credential permission to create release tags.
   Test this boundary in ruleset evaluation mode before enforcing it. Prefer a
   narrowly scoped GitHub App if the repository's built-in workflow identity
   cannot be granted the required bypass safely.
5. Confirm the Tessarr container package is linked to the repository and has
   the intended public visibility.

The tag ruleset is required because historical commits contain an older
tag-triggered release workflow. The current workflow cannot change those
historical files.

## Beta container builds

Merges to `beta` publish the shared `beta` container tag automatically. A
manual rerun must select the `beta` branch; the workflow rejects every other
ref. Each build also receives an immutable version tag in the form
`2.6.0-beta.RUN.SHA`.

## Versioned releases

Open **Actions**, choose **Release**, and select the source branch that matches
the requested version:

- For `vMAJOR.MINOR.PATCH-beta.N` or `vMAJOR.MINOR.PATCH-rc.N`, select `beta`.
- For `vMAJOR.MINOR.PATCH`, select `main`.

Enter the complete tag in the workflow input. The workflow rejects malformed
or existing tags and verifies the selected branch. Every job builds the
immutable commit associated with that dispatch. Runs from the same branch are
serialized so shared channel tags cannot race each other.

The workflow builds and validates all platform artifacts, publishes the
container image, then creates the GitHub release and its tag. A failed run does
not silently reuse an existing release tag.

## Stable promotion

Promote the already validated release candidate from `beta` to `main` through
the protected pull-request process. Dispatch the stable release from `main`
only after the exact promoted commit and all required checks are confirmed.

Never move or replace a published release tag. Correct a bad release with a new
version and preserve the previous artifacts for rollback.
