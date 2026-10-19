# Torrent output paths

New torrent downloads use a persisted local output folder name that is separate
from the provider's display title. A readable, sanitized title prefix is followed
by an identity suffix derived from the torrent hash. The component is bounded in
length and does not change when a provider updates its title.

Downloads, download-action symlinks, download-action `.strm` files and owned
cleanup all resolve this same local output folder. Titles and provider identifiers
remain unchanged in the UI. Virtual mount naming and the separate managed STRM
library are not changed by this feature.

## Compatibility and safety

- Existing entries without `output_name` keep their exact legacy output paths.
  There is no automatic rename, move, deletion, or migration of existing files.
- Re-admission or merging an existing local entry preserves its output location.
  A provider-only entry with no local download location does not override a new
  admission's location. Season children of new-layout imports have their own
  persisted names; existing queue children retain their recorded locations.
- Display punctuation such as a colon can be represented by a safe output
  folder. Only a recognized provider release-root prefix is stripped from nested
  file paths. Nested folders and media filenames still require safe, portable
  components; this is not a general media-file renamer.
- Symlink imports still require a portable mounted source folder, independently
  of the output folder. The selected filename, original-name or hash naming
  policy is checked at admission, after provider resolution, and before local
  processing. An unsafe source is rejected, not silently renamed. Download and
  STRM actions do not require a mounted source; hash-named mounts can also avoid
  title-based source restrictions. Existing mount names are never rewritten.
- Traversal, absolute paths, malformed titles, symlink escapes and foreign
  ownership remain blocked. A saved output name never grants deletion authority.
- Ambiguous or nonportable legacy paths that still fail validation remain blocked
  for review. Do not rename their folders or clear cleanup records just to bypass
  the ownership checks.

## Rollback

Older binaries do not understand `output_name`. Do not downgrade against a
database containing newly admitted stable-layout entries: use a coordinated
pre-upgrade backup of state and output, or retain a binary with output-name
support. Restoring only an old executable is not a compatible rollback for those
new entries.

No deployment or on-disk migration is required to build and test this change.
