package manager

// These durable external-root names remain readable so upgrades and rollbacks
// can safely manage work created before the Tessarr cutover. New artifacts are
// always created under the Tessarr namespace.
const (
	legacyTorrentOwnerMarkerName   = ".decypharr-torrent-owner-v1"
	legacyTorrentOwnershipLockName = ".decypharr-torrent-ownership.lock"
	legacyTorrentQuarantinePrefix  = ".decypharr-torrent-quarantine-"
	legacyTorrentPartialPrefix     = ".decypharr-torrent-part-"

	legacyUsenetOwnerMarkerName   = ".decypharr-nzb-owner-v1"
	legacyUsenetOwnershipLockName = ".decypharr-nzb-ownership.lock"
	legacyUsenetQuarantinePrefix  = ".decypharr-nzb-quarantine-"

	legacyStrmRootMarker  = ".decypharr-strm-root"
	legacyStrmEntryMarker = ".decypharr-strm-entry"
)
