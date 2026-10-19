package reader

// Durable names used by cache roots created before the Tessarr cutover.
const (
	legacyOwnedCacheDirName  = ".decypharr-stream-cache-v1"
	legacyCacheOwnerFileName = ".decypharr-cache-owner"
	legacyCacheInstanceFile  = ".decypharr-cache-instance"
	legacyCacheCleanupLock   = ".decypharr-cache-cleanup.lock"
	legacyCacheQuarantine    = ".decypharr-cache-quarantine-"
)
