package reader

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"github.com/rs/zerolog"

	"github.com/Trifocals3537/tessarr/internal/buffer"
	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// SegmentCache is a usenet-segment-aware view over a buffer.Buffer.
//
// The storage layer is the buffer: it owns the sparse disk file, the
// in-RAM block cache, the page-cache discipline, the hole punching, and
// the bookkeeping for what bytes are present anywhere. SegmentCache adds
// the usenet-specific policy on top:
//
//   - State machine per segment (Empty / Fetching / OnDisk / Failed)
//   - Pin counts so in-flight reads can't race against eviction
//   - Per-segment access timestamps driving the sliding-window evictor
//   - "Last byte delivered to a client" high-water mark for the sliding
//     window's distance test
//   - Hard-disk budget backstop on top of the proactive sweeper
//
// All the actual byte movement (write, read, hole-punch) goes through the
// buffer. That's the entire integration boundary.
type SegmentCache struct {
	pools     *Pools
	ownsPools bool

	// Segment metadata
	segments   []SegmentMeta
	segCount   int
	segOffsets []int64 // cumulative byte offsets for binary-search lookup
	totalSize  int64
	segLengths []atomic.Int64 // bytes actually stored per segment

	// Per-segment state
	states     []atomic.Uint32
	pinCounts  []atomic.Int32
	errors     []atomic.Pointer[error]
	accessTime []atomic.Int64

	// Storage layer.
	buf        *buffer.Buffer
	cacheRoot  string // application-owned parent used to prove cleanup containment
	diskPath   string // marked per-cache directory removed on Close
	cacheToken string // unique owner token required for this instance's cleanup

	// Hard-disk budget. The sliding-window sweeper does the routine eviction
	// work; drainOverBudget is the backstop if pinned-segment count or burst
	// inflow pushes curDisk past maxDisk anyway.
	maxDisk      int64
	curDisk      atomic.Int64
	evictSignal  chan struct{}
	evictMu      sync.Mutex          // serializes hard-budget scans and hole punching
	evictScratch []evictionCandidate // reused by findEvictableBatch under evictMu
	evictWg      sync.WaitGroup

	// Sliding-window state. See sweepWindow for the policy.
	maxConsumedOff atomic.Int64
	sweepWg        sync.WaitGroup

	// Sharded waiters: readers blocking on WaitForSegment park on one of
	// numShards condition variables to avoid global wakeup storms.
	shardMu   [numShards]sync.Mutex
	shardCond [numShards]*sync.Cond

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	closed  atomic.Bool
	closeMu sync.Mutex
	logger  zerolog.Logger

	stats *ReaderStats
}

const (
	numShards = 64
	shardMask = numShards - 1

	ownedCacheDirName  = ".tessarr-stream-cache-v1"
	cacheOwnerFileName = ".tessarr-cache-owner"
	cacheInstanceFile  = ".tessarr-cache-instance"
	cacheCleanupLock   = ".tessarr-cache-cleanup.lock"
	cacheQuarantine    = ".tessarr-cache-quarantine-"
	// These payloads are durable cache schema identifiers. Marker filenames
	// change during migration, but existing verified caches remain reusable.
	cacheOwnerContents = "decypharr stream cache v1\n"
	cacheInstanceData  = "decypharr segment cache v1 "
	cacheMarkerMaxSize = len(cacheInstanceData) + 64 + 1

	cacheQuarantineReadBatch  = 128
	cacheQuarantineMaxEntries = 100_000
	cacheQuarantineMaxDepth   = 64

	// Sliding-window eviction tunables. Hardcoded — the cache is internal
	// temporary storage; exposing knobs invites mis-tuning.
	//
	// backWindowBytes keeps a generous slice of recently-played history
	// pinned so brief scrub-back gestures don't trigger a re-fetch. ~170
	// segments at 750 KB each ≈ 25 s of 1080p / 12 s of 4K — covers
	// typical "10 second rewind" buttons in media players with margin.
	backWindowBytes = 128 << 20

	// segmentMinRetentionAge is the minimum time a segment must be
	// untouched before it is eligible for window-based eviction. Defends
	// against the pause-and-resume case: even if a segment is technically
	// "behind" the last delivered offset, we keep it for a moment because
	// the player may still be drawing from the same area.
	segmentMinRetentionAge = 30 * time.Second

	// segmentSweepInterval is how often the proactive sliding-window
	// evictor wakes.
	segmentSweepInterval = 5 * time.Second

	// segmentSweepBatch caps how many segments a single sweep evicts so
	// a large jump in playback position doesn't punch holes for thousands
	// of segments in one burst. Sweeps are cheap; the next tick picks up
	// the rest.
	segmentSweepBatch = 128

	// bufferMemorySize is the per-stream RAM ceiling for the underlying buffer:
	// forward prefetch + recent reads. 32 MB covers ~40 segments hot in RAM —
	// enough headroom that a bursty download or a seek-back within the window
	// doesn't stall playback or force a re-download. Aggregate RAM across many
	// concurrent streams is bounded separately by the global buffer budget
	// (buffer.SetGlobalMemoryBudget), so this can stay generous without the
	// per-stream-size x concurrency blowup that a small ceiling was guarding.
	bufferMemorySize = 32 << 20
)

var (
	ownedCacheDirNames  = []string{ownedCacheDirName, legacyOwnedCacheDirName}
	cacheOwnerFileNames = []string{cacheOwnerFileName, legacyCacheOwnerFileName}
	cacheInstanceFiles  = []string{cacheInstanceFile, legacyCacheInstanceFile}
	cacheCleanupLocks   = []string{legacyCacheCleanupLock, cacheCleanupLock}
)

var (
	cacheCleanupMu          sync.Mutex
	cacheCleanupLockTimeout = 5 * time.Second
	cacheCleanupRetryDelay  = 25 * time.Millisecond
	cacheCleanupBackoff     = 100 * time.Millisecond
)

const cacheCleanupAttempts = 3

var errRetryableCacheCleanup = errors.New("retryable cache cleanup failure")

type retryableCacheCleanupError struct {
	err error
}

func (e *retryableCacheCleanupError) Error() string {
	return e.err.Error()
}

func (e *retryableCacheCleanupError) Unwrap() error {
	return e.err
}

func (e *retryableCacheCleanupError) Is(target error) bool {
	return target == errRetryableCacheCleanup || errors.Is(e.err, target)
}

func retryableCacheCleanup(err error) error {
	if err == nil {
		return nil
	}
	return &retryableCacheCleanupError{err: err}
}

func retryCacheCleanup(cleanup func() error) error {
	var cleanupErr error
	for attempt := range cacheCleanupAttempts {
		cleanupErr = cleanup()
		if cleanupErr == nil || !errors.Is(cleanupErr, errRetryableCacheCleanup) {
			return cleanupErr
		}
		if attempt+1 < cacheCleanupAttempts {
			time.Sleep(cacheCleanupBackoff * time.Duration(attempt+1))
		}
	}
	return cleanupErr
}

// PrepareDiskCacheRoot establishes and validates Tessarr's private namespace
// under the configured disk-buffer path. It deliberately does not reap cache
// instances from a previous process: a marker proves type, not liveness, and
// startup cannot distinguish a crash remnant from another live process.
func PrepareDiskCacheRoot(configuredRoot string) (string, error) {
	return ensureDiskCacheRoot(configuredRoot)
}

func ensureDiskCacheRoot(configuredRoot string) (string, error) {
	baseRoot, base, err := safepath.EnsureOpenRoot(configuredRoot, 0o700)
	if err != nil {
		return "", fmt.Errorf("invalid disk buffer path: %w", err)
	}
	defer baseRoot.Close()
	cacheDirName, cacheInfo, err := selectCacheNamespace(baseRoot)
	if err != nil {
		return "", err
	}
	cacheRoot, err := safepath.JoinIdentifiers(base, cacheDirName)
	if err != nil {
		return "", fmt.Errorf("resolve owned cache root: %w", err)
	}

	switch {
	case cacheInfo != nil:
		if cacheInfo.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("owned cache root %q is a symlink", cacheRoot)
		}
		if !cacheInfo.IsDir() {
			return "", fmt.Errorf("owned cache root %q is not a directory", cacheRoot)
		}
	default:
		if err := baseRoot.Mkdir(cacheDirName, 0o700); err != nil {
			return "", fmt.Errorf("create owned cache root: %w", err)
		}
	}
	cacheHandle, err := baseRoot.OpenRoot(cacheDirName)
	if err != nil {
		return "", fmt.Errorf("pin owned cache root: %w", err)
	}
	defer cacheHandle.Close()
	openedInfo, err := cacheHandle.Stat(".")
	if err != nil || !openedInfo.IsDir() || (cacheInfo != nil && !os.SameFile(cacheInfo, openedInfo)) {
		return "", fmt.Errorf("owned cache root changed while opening")
	}

	contents, markerErr := readCacheOwnerMarkerFromRoot(cacheHandle)
	switch {
	case markerErr == nil:
		if string(contents) != cacheOwnerContents {
			return "", fmt.Errorf("cache ownership marker is invalid")
		}
	case os.IsNotExist(markerErr):
		empty, err := cacheDirectoryEmpty(cacheHandle)
		if err != nil {
			return "", fmt.Errorf("inspect unowned cache directory: %w", err)
		}
		if !empty {
			return "", fmt.Errorf("refusing to claim non-empty unowned cache directory %q", cacheRoot)
		}
		if err := writeExclusiveMarkerToRoot(cacheHandle, cacheOwnerFileName, cacheOwnerContents); err != nil {
			return "", fmt.Errorf("create cache ownership marker: %w", err)
		}
	default:
		return "", fmt.Errorf("inspect cache ownership marker: %w", markerErr)
	}

	if err := cacheHandle.Chmod(".", 0o700); err != nil {
		return "", fmt.Errorf("secure owned cache root permissions: %w", err)
	}
	return cacheRoot, nil
}

func selectCacheNamespace(baseRoot *os.Root) (string, os.FileInfo, error) {
	var selected string
	var selectedInfo os.FileInfo
	for _, name := range ownedCacheDirNames {
		info, err := baseRoot.Lstat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", nil, fmt.Errorf("inspect owned cache root %q: %w", name, err)
		}
		if selected != "" {
			return "", nil, fmt.Errorf("both Tessarr and legacy cache roots exist")
		}
		selected = name
		selectedInfo = info
	}
	if selected == "" {
		return ownedCacheDirName, nil, nil
	}
	return selected, selectedInfo, nil
}

func cacheDirectoryEmpty(rooted *os.Root) (bool, error) {
	directory, err := rooted.Open(".")
	if err != nil {
		return false, err
	}
	entries, readErr := directory.ReadDir(1)
	directoryCloseErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return false, errors.Join(readErr, directoryCloseErr)
	}
	if directoryCloseErr != nil {
		return false, directoryCloseErr
	}
	return len(entries) == 0, nil
}

func newCacheInstance(configuredRoot string) (cacheRoot, diskPath, cacheToken string, err error) {
	if configuredRoot == "" {
		cacheRoot, err = filepath.EvalSymlinks(os.TempDir())
		if err != nil {
			return "", "", "", fmt.Errorf("resolve temporary cache root: %w", err)
		}
	} else {
		cacheRoot, err = ensureDiskCacheRoot(configuredRoot)
		if err != nil {
			return "", "", "", err
		}
	}

	cacheCleanupMu.Lock()
	defer cacheCleanupMu.Unlock()
	cacheLock, err := acquireCacheCleanupLock(cacheRoot)
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		if unlockErr := cacheLock.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("unlock cache root after instance creation: %w", unlockErr))
		}
	}()

	diskPath, err = makeCacheInstance(cacheRoot)
	if err != nil {
		return "", "", "", err
	}
	cacheToken, err = newCacheInstanceToken()
	if err != nil {
		if rooted, openErr := os.OpenRoot(cacheRoot); openErr == nil {
			_ = rooted.Remove(filepath.Base(diskPath))
			_ = rooted.Close()
		}
		return "", "", "", err
	}
	if err := writeExclusiveMarker(diskPath, cacheInstanceFile, cacheInstanceData+cacheToken+"\n"); err != nil {
		// The directory was just created and is still empty when the marker
		// write fails, so a non-recursive remove is sufficient and cannot
		// affect anything outside this instance.
		if rooted, openErr := os.OpenRoot(cacheRoot); openErr == nil {
			_ = rooted.Remove(filepath.Base(diskPath))
			_ = rooted.Close()
		}
		return "", "", "", fmt.Errorf("mark cache instance: %w", err)
	}
	return cacheRoot, diskPath, cacheToken, nil
}

func newCacheInstanceToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate cache instance owner token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func makeCacheInstance(cacheRoot string) (string, error) {
	rooted, err := os.OpenRoot(cacheRoot)
	if err != nil {
		return "", fmt.Errorf("open cache root: %w", err)
	}
	defer rooted.Close()

	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate cache instance name: %w", err)
		}
		name := "cache-" + hex.EncodeToString(random[:])
		if err := rooted.Mkdir(name, 0o700); err != nil {
			if os.IsExist(err) {
				continue
			}
			return "", fmt.Errorf("create cache instance: %w", err)
		}
		return filepath.Join(cacheRoot, name), nil
	}
	return "", fmt.Errorf("create cache instance: exhausted unique names")
}

func writeExclusiveMarker(rootPath, name, contents string) error {
	rooted, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	writeErr := writeExclusiveMarkerToRoot(rooted, name, contents)
	return errors.Join(writeErr, rooted.Close())
}

func writeExclusiveMarkerToRoot(rooted *os.Root, name, contents string) error {
	file, err := rooted.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.WriteString(contents)
	if writeErr == nil && written != len(contents) {
		writeErr = io.ErrShortWrite
	}
	fileSyncErr := file.Sync()
	fileCloseErr := file.Close()
	if err := errors.Join(writeErr, fileSyncErr, fileCloseErr); err != nil {
		removeErr := rooted.Remove(name)
		if os.IsNotExist(removeErr) {
			removeErr = nil
		}
		directorySyncErr := syncCacheDirectory(rooted)
		return errors.Join(err, removeErr, directorySyncErr)
	}
	return syncCacheDirectory(rooted)
}

func syncCacheDirectory(rooted *os.Root) error {
	directory, err := rooted.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	// Windows does not expose the same portable directory flush primitive.
	// The file itself is still flushed and exclusive creation remains safe.
	if runtime.GOOS == "windows" {
		syncErr = nil
	}
	return errors.Join(syncErr, closeErr)
}

func readCacheOwnerMarkerFromRoot(rooted *os.Root) ([]byte, error) {
	var contents []byte
	found := false
	for _, name := range cacheOwnerFileNames {
		current, err := readCacheOwnerMarkerNamed(rooted, name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if found && string(contents) != string(current) {
			return nil, fmt.Errorf("cache ownership markers disagree")
		}
		contents = current
		found = true
	}
	if !found {
		return nil, os.ErrNotExist
	}
	return contents, nil
}

func readCacheOwnerMarkerNamed(rooted *os.Root, name string) ([]byte, error) {
	info, err := rooted.Lstat(name)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("cache ownership marker is not a regular file")
	}
	file, err := rooted.Open(name)
	if err != nil {
		return nil, err
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, int64(len(cacheOwnerContents)+1)))
	fileCloseErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if fileCloseErr != nil {
		return nil, fileCloseErr
	}
	if len(contents) > len(cacheOwnerContents) {
		return nil, fmt.Errorf("cache ownership marker is too large")
	}
	return contents, nil
}

type cacheCleanupLockSet struct {
	locks []*flock.Flock
}

func (set *cacheCleanupLockSet) Unlock() error {
	var unlockErr error
	for index := len(set.locks) - 1; index >= 0; index-- {
		unlockErr = errors.Join(unlockErr, set.locks[index].Unlock())
	}
	set.locks = nil
	return unlockErr
}

func acquireCacheCleanupLock(cacheRoot string) (*cacheCleanupLockSet, error) {
	absoluteRoot, err := safepath.ValidateRoot(cacheRoot)
	if err != nil {
		return nil, err
	}
	set := &cacheCleanupLockSet{}
	for _, name := range cacheCleanupLocks {
		cacheLock, err := acquireCacheCleanupLockNamed(absoluteRoot, name)
		if err != nil {
			return nil, errors.Join(err, set.Unlock())
		}
		set.locks = append(set.locks, cacheLock)
	}
	return set, nil
}

func acquireCacheCleanupLockNamed(absoluteRoot, name string) (*flock.Flock, error) {
	lockPath := filepath.Join(absoluteRoot, name)
	if info, statErr := os.Lstat(lockPath); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("cache cleanup lock %q is not a regular file", lockPath)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("inspect cache cleanup lock: %w", statErr)
	}

	cacheLock := flock.New(lockPath, flock.SetPermissions(0o600))
	lockContext, cancel := context.WithTimeout(context.Background(), cacheCleanupLockTimeout)
	locked, lockErr := cacheLock.TryLockContext(lockContext, cacheCleanupRetryDelay)
	cancel()
	if lockErr != nil {
		if errors.Is(lockErr, context.DeadlineExceeded) || errors.Is(lockErr, context.Canceled) {
			return nil, retryableCacheCleanup(fmt.Errorf("lock cache root: %w", lockErr))
		}
		return nil, fmt.Errorf("lock cache root: %w", lockErr)
	}
	if !locked {
		return nil, retryableCacheCleanup(fmt.Errorf("lock cache root: timed out after %s", cacheCleanupLockTimeout))
	}
	info, err := os.Lstat(lockPath)
	if err != nil {
		_ = cacheLock.Unlock()
		return nil, fmt.Errorf("inspect locked cache cleanup file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		_ = cacheLock.Unlock()
		return nil, fmt.Errorf("locked cache cleanup path %q is not a regular file", lockPath)
	}
	return cacheLock, nil
}

func removeCacheInstance(cacheRoot, diskPath, expectedToken string) (err error) {
	if expectedToken == "" {
		return fmt.Errorf("cache instance owner token is empty")
	}
	absolutePath, err := safepath.ValidateUnderRoot(cacheRoot, diskPath)
	if err != nil {
		return err
	}

	cacheCleanupMu.Lock()
	defer cacheCleanupMu.Unlock()
	cacheLock, err := acquireCacheCleanupLock(cacheRoot)
	if err != nil {
		return err
	}
	defer func() {
		if unlockErr := cacheLock.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("unlock cache root after cleanup: %w", unlockErr))
		}
	}()

	absoluteRoot, err := safepath.ValidateRoot(cacheRoot)
	if err != nil {
		return err
	}
	relativeInstance, err := filepath.Rel(absoluteRoot, absolutePath)
	if err != nil {
		return fmt.Errorf("make cache instance relative: %w", err)
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return fmt.Errorf("open cache root: %w", err)
	}
	defer rooted.Close()

	quarantines, err := findCacheQuarantines(rooted, relativeInstance, expectedToken)
	if err != nil {
		return err
	}
	visibleInfo, statErr := rooted.Lstat(relativeInstance)
	visibleExists := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return fmt.Errorf("inspect cache instance: %w", statErr)
	}
	if len(quarantines) > 0 {
		if len(quarantines) > 1 || visibleExists {
			return fmt.Errorf("refusing ambiguous cache cleanup state")
		}
		return removeVerifiedCacheQuarantine(rooted, quarantines[0], expectedToken, nil)
	}
	if !visibleExists {
		return nil
	}

	instanceRoot, err := rooted.OpenRoot(relativeInstance)
	if err != nil {
		return fmt.Errorf("open cache instance: %w", err)
	}
	pinnedInfo, statErr := instanceRoot.Stat(".")
	actualToken, verifyErr := readCacheInstanceToken(instanceRoot)
	closeErr := instanceRoot.Close()
	if statErr != nil {
		return fmt.Errorf("stat opened cache instance: %w", statErr)
	}
	if !os.SameFile(visibleInfo, pinnedInfo) {
		return fmt.Errorf("cache instance changed during ownership verification")
	}
	if verifyErr != nil {
		return verifyErr
	}
	if closeErr != nil {
		return fmt.Errorf("close cache instance: %w", closeErr)
	}
	if actualToken != expectedToken {
		return fmt.Errorf("cache instance owner token mismatch")
	}

	quarantineRelative, err := newCacheQuarantinePath(rooted, relativeInstance, expectedToken)
	if err != nil {
		return err
	}
	if err := rooted.Rename(relativeInstance, quarantineRelative); err != nil {
		return retryableCacheCleanup(fmt.Errorf("quarantine cache instance: %w", err))
	}
	if err := syncCacheDirectory(rooted); err != nil {
		return retryableCacheCleanup(fmt.Errorf("sync quarantined cache instance: %w", err))
	}
	return removeVerifiedCacheQuarantine(rooted, quarantineRelative, expectedToken, pinnedInfo)
}

func readCacheInstanceToken(instanceRoot *os.Root) (string, error) {
	var token string
	found := false
	for _, name := range cacheInstanceFiles {
		current, err := readCacheInstanceTokenNamed(instanceRoot, name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if found && token != current {
			return "", fmt.Errorf("cache instance markers disagree")
		}
		token = current
		found = true
	}
	if !found {
		return "", fmt.Errorf("inspect cache instance marker: %w", os.ErrNotExist)
	}
	return token, nil
}

func readCacheInstanceTokenNamed(instanceRoot *os.Root, name string) (string, error) {
	info, err := instanceRoot.Lstat(name)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("cache instance marker is not a regular file")
	}
	file, err := instanceRoot.Open(name)
	if err != nil {
		return "", fmt.Errorf("open cache instance marker: %w", err)
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, int64(cacheMarkerMaxSize+1)))
	closeErr := file.Close()
	if readErr != nil {
		return "", fmt.Errorf("read cache instance marker: %w", readErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close cache instance marker: %w", closeErr)
	}
	if len(contents) > cacheMarkerMaxSize {
		return "", fmt.Errorf("cache instance marker is too large")
	}
	text := string(contents)
	if !strings.HasPrefix(text, cacheInstanceData) || !strings.HasSuffix(text, "\n") {
		return "", fmt.Errorf("cache instance marker is malformed")
	}
	token := strings.TrimSuffix(strings.TrimPrefix(text, cacheInstanceData), "\n")
	decoded, err := hex.DecodeString(token)
	if err != nil || len(decoded) != 32 {
		return "", fmt.Errorf("cache instance marker token is invalid")
	}
	return token, nil
}

func removeVerifiedCacheQuarantine(
	rooted *os.Root,
	quarantineRelative, expectedToken string,
	expectedInfo os.FileInfo,
) error {
	info, err := rooted.Lstat(quarantineRelative)
	if err != nil {
		return fmt.Errorf("inspect quarantined cache instance: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("quarantined cache instance is not a regular directory")
	}
	quarantineRoot, err := rooted.OpenRoot(quarantineRelative)
	if err != nil {
		return fmt.Errorf("open quarantined cache instance: %w", err)
	}
	pinned, statErr := quarantineRoot.Stat(".")
	if statErr != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("stat quarantined cache instance: %w", statErr)
	}
	if !os.SameFile(info, pinned) ||
		(expectedInfo != nil && !os.SameFile(expectedInfo, pinned)) {
		_ = quarantineRoot.Close()
		return fmt.Errorf("quarantined cache instance changed during verification")
	}
	actualToken, verifyErr := readCacheInstanceToken(quarantineRoot)
	if verifyErr != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("verify quarantined cache instance; preserved for safe retry: %w", verifyErr)
	}
	if actualToken != expectedToken {
		_ = quarantineRoot.Close()
		return fmt.Errorf("quarantined cache owner token mismatch; preserved for safe retry")
	}
	if err := safepath.RemovePinnedTreeContents(
		quarantineRoot,
		safepath.PinnedTreeRemovalOptions{
			MaxEntries:       cacheQuarantineMaxEntries,
			MaxDepth:         cacheQuarantineMaxDepth,
			ReadBatch:        cacheQuarantineReadBatch,
			PreserveTopLevel: cacheInstanceFiles,
		},
	); err != nil {
		_ = quarantineRoot.Close()
		return retryableCacheCleanup(fmt.Errorf("empty pinned cache quarantine: %w", err))
	}
	afterContents, err := rooted.Lstat(quarantineRelative)
	if err != nil || !os.SameFile(pinned, afterContents) {
		_ = quarantineRoot.Close()
		if err != nil {
			return fmt.Errorf("reinspect emptied cache quarantine: %w", err)
		}
		return fmt.Errorf("cache quarantine changed before marker removal")
	}
	actualToken, err = readCacheInstanceToken(quarantineRoot)
	if err != nil || actualToken != expectedToken {
		_ = quarantineRoot.Close()
		if err != nil {
			return fmt.Errorf("reverify emptied cache quarantine: %w", err)
		}
		return fmt.Errorf("emptied cache quarantine owner token mismatch")
	}
	if err := removeCacheInstanceMarkers(quarantineRoot); err != nil {
		_ = quarantineRoot.Close()
		return retryableCacheCleanup(fmt.Errorf("remove cache quarantine marker: %w", err))
	}
	if err := syncCacheDirectory(quarantineRoot); err != nil {
		_ = quarantineRoot.Close()
		return retryableCacheCleanup(fmt.Errorf("sync emptied cache quarantine: %w", err))
	}
	if err := quarantineRoot.Close(); err != nil {
		return retryableCacheCleanup(fmt.Errorf("close emptied cache quarantine: %w", err))
	}
	afterClose, err := rooted.Lstat(quarantineRelative)
	if err != nil || !os.SameFile(pinned, afterClose) {
		if err != nil {
			return fmt.Errorf("reinspect closed cache quarantine: %w", err)
		}
		return fmt.Errorf("cache quarantine changed before final unlink")
	}
	if err := rooted.Remove(quarantineRelative); err != nil {
		return retryableCacheCleanup(fmt.Errorf("remove emptied cache quarantine: %w", err))
	}
	if err := syncCacheDirectory(rooted); err != nil {
		return retryableCacheCleanup(fmt.Errorf("sync removed cache quarantine: %w", err))
	}
	return nil
}

func findCacheQuarantines(rooted *os.Root, relativeInstance, token string) ([]string, error) {
	dir, err := rooted.Open(filepath.Dir(relativeInstance))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open cache parent for quarantine recovery: %w", err)
	}
	prefixes := cacheQuarantinePrefixesForInstance(relativeInstance, token)
	var quarantines []string
	scanned := 0
	for scanned < cacheQuarantineMaxEntries {
		remaining := cacheQuarantineMaxEntries - scanned
		entries, readErr := dir.ReadDir(min(cacheQuarantineReadBatch, remaining))
		scanned += len(entries)
		for _, entry := range entries {
			matched := false
			for _, prefix := range prefixes {
				if strings.HasPrefix(strings.ToLower(entry.Name()), strings.ToLower(prefix)) {
					matched = true
					break
				}
			}
			if matched {
				quarantines = append(
					quarantines,
					filepath.Join(filepath.Dir(relativeInstance), entry.Name()),
				)
				// The caller rejects any state with multiple quarantines, so
				// there is no reason to scan or retain more names.
				if len(quarantines) > 1 {
					if closeErr := dir.Close(); closeErr != nil {
						return nil, fmt.Errorf("close ambiguous cache quarantine inspection: %w", closeErr)
					}
					return quarantines, nil
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			if closeErr := dir.Close(); closeErr != nil {
				return nil, fmt.Errorf("close cache quarantine inspection: %w", closeErr)
			}
			return quarantines, nil
		}
		if readErr != nil {
			_ = dir.Close()
			return nil, fmt.Errorf("inspect cache quarantines: %w", readErr)
		}
		if len(entries) == 0 {
			_ = dir.Close()
			return nil, fmt.Errorf("inspect cache quarantines made no progress")
		}
	}
	_ = dir.Close()
	return nil, fmt.Errorf(
		"cache quarantine inspection exceeded %d directory entries",
		cacheQuarantineMaxEntries,
	)
}

func newCacheQuarantinePath(rooted *os.Root, relativeInstance, token string) (string, error) {
	parent := filepath.Dir(relativeInstance)
	prefix := cacheQuarantinePrefixForInstance(relativeInstance, token)
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate cache quarantine name: %w", err)
		}
		relative := filepath.Join(parent, prefix+hex.EncodeToString(random[:]))
		if _, err := rooted.Lstat(relative); os.IsNotExist(err) {
			return relative, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect cache quarantine candidate: %w", err)
		}
	}
	return "", fmt.Errorf("generate cache quarantine name: exhausted unique names")
}

func cacheQuarantinePrefixForInstance(relativeInstance, token string) string {
	digest := sha256.Sum256([]byte(token + "\x00" + filepath.ToSlash(relativeInstance)))
	return cacheQuarantine + hex.EncodeToString(digest[:16]) + "-"
}

func cacheQuarantinePrefixesForInstance(relativeInstance, token string) []string {
	digest := sha256.Sum256([]byte(token + "\x00" + filepath.ToSlash(relativeInstance)))
	suffix := hex.EncodeToString(digest[:16]) + "-"
	return []string{cacheQuarantine + suffix, legacyCacheQuarantine + suffix}
}

func removeCacheInstanceMarkers(rooted *os.Root) error {
	var removeErr error
	for _, name := range cacheInstanceFiles {
		if err := rooted.Remove(name); err != nil && !os.IsNotExist(err) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return removeErr
}

// NewSegmentCache creates a new segment cache backed by a freshly-created
// buffer.Buffer on a sparse disk file under config.DiskPath (or a temp dir).
func NewSegmentCache(
	ctx context.Context,
	segments []SegmentMeta,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) (*SegmentCache, error) {
	ctx, cancel := context.WithCancel(ctx)
	segCount := len(segments)

	offsets := computeOffsets(segments)
	totalSize := int64(0)
	if len(offsets) > 0 {
		totalSize = offsets[len(offsets)-1]
	}

	// Resolve a fresh, individually-marked cache directory inside
	// Tessarr's owned namespace. Cleanup later proves both containment and
	// ownership before removing it.
	cacheRoot, diskPath, cacheToken, err := newCacheInstance(config.DiskPath)
	if err != nil {
		cancel()
		return nil, err
	}

	// sc is referenced by the buffer's OnEvict closure; assigned just below
	// before any read/write can trigger a pool-driven punch.
	var sc *SegmentCache

	pools := config.Pools
	ownsPools := pools == nil
	if ownsPools {
		pools = defaultPools()
	}
	pools.mu.Lock()
	if pools.closed {
		pools.mu.Unlock()
		cancel()
		cleanupErr := retryCacheCleanup(func() error {
			return removeCacheInstance(cacheRoot, diskPath, cacheToken)
		})
		return nil, errors.Join(buffer.ErrClosed, cleanupErr)
	}
	buf, err := pools.buffers.NewBuffer(buffer.Config{
		MemorySize: bufferMemorySize,
		DiskPath:   filepath.Join(diskPath, "segments.bin"),
		TotalSize:  totalSize,
		// Only fires if the usenet pool is given a disk limit (off by default —
		// usenet bounds disk via its own sliding-window sweep). If a pool-driven
		// punch ever does happen, mark the covered segments Empty so they
		// re-fetch instead of pointing at a hole.
		OnEvict: func(off, length int64) {
			if sc != nil {
				sc.onBufferEvict(off, length)
			}
		},
	})
	if err != nil {
		pools.mu.Unlock()
		cancel()
		if ownsPools {
			_ = pools.Close()
		}
		if cleanupErr := retryCacheCleanup(func() error {
			return removeCacheInstance(cacheRoot, diskPath, cacheToken)
		}); cleanupErr != nil {
			return nil, errors.Join(fmt.Errorf("create buffer: %w", err), fmt.Errorf("cleanup failed cache instance: %w", cleanupErr))
		}
		return nil, fmt.Errorf("create buffer: %w", err)
	}

	sc = &SegmentCache{
		pools:       pools,
		ownsPools:   ownsPools,
		segments:    segments,
		segCount:    segCount,
		segOffsets:  offsets,
		totalSize:   totalSize,
		segLengths:  make([]atomic.Int64, segCount),
		states:      make([]atomic.Uint32, segCount),
		pinCounts:   make([]atomic.Int32, segCount),
		errors:      make([]atomic.Pointer[error], segCount),
		accessTime:  make([]atomic.Int64, segCount),
		buf:         buf,
		cacheRoot:   cacheRoot,
		diskPath:    diskPath,
		cacheToken:  cacheToken,
		maxDisk:     config.MaxDisk,
		evictSignal: make(chan struct{}, 1),
		ctx:         ctx,
		cancel:      cancel,
		logger:      logger.With().Str("component", "cache").Logger(),
		stats:       stats,
	}

	for i := range numShards {
		sc.shardCond[i] = sync.NewCond(&sc.shardMu[i])
	}

	// Hard-budget backstop. The sliding-window sweeper does the routine
	// eviction work — see sweepLoop.
	sc.evictWg.Add(1)
	go sc.evictLoop()

	// Proactive sliding-window evictor: this is what keeps the cache tight
	// to actual playback instead of growing to the file size.
	sc.sweepWg.Add(1)
	go sc.sweepLoop()
	pools.caches[sc] = struct{}{}
	pools.mu.Unlock()

	return sc, nil
}

// computeOffsets calculates cumulative byte offsets for segment lookup.
func computeOffsets(segments []SegmentMeta) []int64 {
	offsets := make([]int64, len(segments)+1)
	if offsetsAreContiguous(segments) {
		for i, seg := range segments {
			offsets[i] = seg.StartOffset
		}
		offsets[len(segments)] = segments[len(segments)-1].EndOffset + 1
	} else {
		cumulative := int64(0)
		for i, seg := range segments {
			offsets[i] = cumulative
			size := seg.Bytes
			if size <= 0 {
				size = 750 * 1024
			}
			cumulative += size
		}
		offsets[len(segments)] = cumulative
	}
	return offsets
}

// offsetsAreContiguous verifies the stored layout before binary-search and
// read code trusts it. Current parsers produce dense ascending output ranges,
// but metadata written by older versions can contain zero-filled, overlapping,
// out-of-order, or gapped slots. Falling back to cumulative sizes keeps every
// reader and cache operation on one self-consistent layout.
func offsetsAreContiguous(segments []SegmentMeta) bool {
	if len(segments) == 0 || segments[0].StartOffset != 0 {
		return false
	}
	for i, segment := range segments {
		if segment.StartOffset < 0 || segment.EndOffset < segment.StartOffset || segment.EndOffset == math.MaxInt64 {
			return false
		}
		if segment.Bytes > 0 && segment.EndOffset-segment.StartOffset+1 != segment.Bytes {
			return false
		}
		if i > 0 && segment.StartOffset != segments[i-1].EndOffset+1 {
			return false
		}
	}
	return true
}

// Get returns segment data, loading via the buffer.
// Returns nil, false if the segment isn't cached. Pin before calling.
func (sc *SegmentCache) Get(segIdx int) ([]byte, bool) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil, false
	}
	if SegmentState(sc.states[segIdx].Load()) != StateOnDisk {
		sc.stats.CacheMisses.Add(1)
		return nil, false
	}

	off := sc.segOffsets[segIdx]
	size := sc.SegmentDataSize(segIdx)
	data := make([]byte, size)
	if _, err := sc.buf.ReadAt(data, off); err != nil {
		if !errors.Is(err, buffer.ErrNotPresent) {
			sc.logger.Warn().Err(err).Int("segment", segIdx).Msg("buffer read failed")
		}
		sc.stats.CacheMisses.Add(1)
		return nil, false
	}
	sc.stats.CacheHits.Add(1)
	return data, true
}

// ReadInto reads the full segment into buf. buf must be at least
// SegmentDataSize(segIdx) bytes.
func (sc *SegmentCache) ReadInto(segIdx int, dst []byte) (int, bool) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return 0, false
	}
	if SegmentState(sc.states[segIdx].Load()) != StateOnDisk {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}

	off := sc.segOffsets[segIdx]
	size := sc.SegmentDataSize(segIdx)
	if int64(len(dst)) < size {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	n, err := sc.buf.ReadAt(dst[:size], off)
	if err != nil {
		if !errors.Is(err, buffer.ErrNotPresent) {
			sc.logger.Warn().Err(err).Int("segment", segIdx).Msg("buffer read failed")
		}
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	sc.stats.CacheHits.Add(1)
	return n, true
}

// ReadRangeInto is the zero-amplification read path: copies only the
// requested [segOffset, segOffset+length) slice of the segment.
func (sc *SegmentCache) ReadRangeInto(segIdx int, segOffset, length int64, dst []byte) (int, bool) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return 0, false
	}
	if SegmentState(sc.states[segIdx].Load()) != StateOnDisk {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	if segOffset < 0 || length < 0 || int64(len(dst)) < length {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}

	size := sc.SegmentDataSize(segIdx)
	if segOffset > size {
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	if segOffset+length > size {
		length = size - segOffset
	}
	if length <= 0 {
		sc.stats.CacheHits.Add(1)
		return 0, true
	}

	absoluteOffset := sc.segOffsets[segIdx] + segOffset
	n, err := sc.buf.ReadAt(dst[:length], absoluteOffset)
	if err != nil {
		if !errors.Is(err, buffer.ErrNotPresent) {
			sc.logger.Warn().Err(err).Int("segment", segIdx).Msg("buffer read failed")
		}
		sc.stats.CacheMisses.Add(1)
		return 0, false
	}
	sc.stats.CacheHits.Add(1)
	return n, true
}

// SegmentDataSize returns the stored or expected size of a segment.
func (sc *SegmentCache) SegmentDataSize(segIdx int) int64 {
	if segIdx < 0 || segIdx >= sc.segCount {
		return 0
	}
	size := sc.segLengths[segIdx].Load()
	if size <= 0 {
		size = sc.segments[segIdx].Bytes
		if size <= 0 {
			size = sc.segOffsets[segIdx+1] - sc.segOffsets[segIdx]
		}
	}
	return size
}

// Put writes segment data through the buffer.
func (sc *SegmentCache) Put(segIdx int, data []byte) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return fmt.Errorf("segment index out of range: %d", segIdx)
	}
	if sc.closed.Load() {
		return io.ErrClosedPipe
	}

	if sc.maxDisk > 0 && sc.curDisk.Load() > sc.maxDisk {
		sc.drainOverBudget()
	}

	off := sc.segOffsets[segIdx]
	if _, err := sc.buf.WriteAt(data, off); err != nil {
		return fmt.Errorf("write segment %d: %w", segIdx, err)
	}

	sc.curDisk.Add(int64(len(data)))
	sc.segLengths[segIdx].Store(int64(len(data)))
	sc.states[segIdx].Store(uint32(StateOnDisk))
	sc.touchSegment(segIdx)
	sc.wakeWaiters(segIdx)
	sc.signalEvict()
	return nil
}

// segmentWriter is the contract doFetch uses to stream a segment body into
// the cache. A failed Finalize is followed by Discard.
type segmentWriter interface {
	Write(p []byte) (int, error)
	Finalize() error
	Discard()
}

var errIncompleteSegment = errors.New("incomplete segment")

// StreamWriter returns a buffer-backed writer for the segment. The writer
// skips the yEnc dataStart header and caps writes at the segment's max
// expected size.
func (sc *SegmentCache) StreamWriter(segIdx int) segmentWriter {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}

	if sc.maxDisk > 0 && sc.curDisk.Load() > sc.maxDisk {
		sc.drainOverBudget()
	}

	seg := sc.segments[segIdx]
	return &bufferStreamWriter{
		buf:       sc.buf,
		offset:    sc.segOffsets[segIdx],
		dataStart: seg.SegmentDataStart,
		maxBytes:  seg.Bytes,
		cache:     sc,
		segIdx:    segIdx,
	}
}

// bufferStreamWriter pipes decoded body bytes from NNTP into the buffer at
// the segment's reserved offset. Writes that exceed maxBytes are silently
// dropped (the decoder may include some trailing padding).
type bufferStreamWriter struct {
	buf       *buffer.Buffer
	offset    int64
	dataStart int64
	maxBytes  int64
	skipped   int64
	written   int64
	cache     *SegmentCache
	segIdx    int
}

func (w *bufferStreamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	consumed := 0

	if w.skipped < w.dataStart {
		skip := min(w.dataStart-w.skipped, int64(len(p)))
		w.skipped += skip
		consumed += int(skip)
		p = p[skip:]
		if len(p) == 0 {
			return consumed, nil
		}
	}

	if w.written >= w.maxBytes {
		return consumed + len(p), nil
	}

	remaining := w.maxBytes - w.written
	writeLen := min(int64(len(p)), remaining)

	n, err := w.buf.WriteAt(p[:writeLen], w.offset+w.written)
	w.written += int64(n)
	if err != nil {
		return consumed + n, err
	}
	if int64(n) != writeLen {
		return consumed + n, io.ErrShortWrite
	}
	return consumed + len(p), nil
}

// Discard is a no-op for the buffer writer: the buffer slot is fixed-offset
// and gets overwritten in place on the next attempt, so there's nothing
// to release on a failed/partial write.
func (w *bufferStreamWriter) Discard() {}

// Finalize commits only a complete segment to the cache. Publishing a short
// write as OnDisk would make later readers consume stale/zero-filled bytes
// without another chance to fail over to a provider that has a complete copy.
func (w *bufferStreamWriter) Finalize() error {
	if w.cache == nil || w.segIdx < 0 {
		return fmt.Errorf("%w: invalid cache writer", errIncompleteSegment)
	}
	if w.maxBytes <= 0 {
		return fmt.Errorf("%w: invalid expected size %d", errIncompleteSegment, w.maxBytes)
	}
	if w.written != w.maxBytes {
		return fmt.Errorf(
			"%w: decoded %d of %d bytes after offset %d",
			errIncompleteSegment,
			w.written,
			w.maxBytes,
			w.dataStart,
		)
	}
	w.cache.curDisk.Add(w.written)
	w.cache.segLengths[w.segIdx].Store(w.written)
	w.cache.states[w.segIdx].Store(uint32(StateOnDisk))
	w.cache.touchSegment(w.segIdx)
	w.cache.wakeWaiters(w.segIdx)
	w.cache.signalEvict()
	return nil
}

// PinRange marks segments as in-use, preventing eviction.
func (sc *SegmentCache) PinRange(start, end int) {
	for i := start; i <= end && i < sc.segCount; i++ {
		sc.pinCounts[i].Add(1)
	}
}

// UnpinRange decrements the pin count for the range.
func (sc *SegmentCache) UnpinRange(start, end int) {
	for i := start; i <= end && i < sc.segCount; i++ {
		sc.pinCounts[i].Add(-1)
	}
}

// IsPinned returns true if the segment has a positive pin count.
func (sc *SegmentCache) IsPinned(segIdx int) bool {
	if segIdx < 0 || segIdx >= sc.segCount {
		return false
	}
	return sc.pinCounts[segIdx].Load() > 0
}

// GetState returns the current state of a segment.
func (sc *SegmentCache) GetState(segIdx int) SegmentState {
	if segIdx < 0 || segIdx >= sc.segCount {
		return StateEmpty
	}
	return SegmentState(sc.states[segIdx].Load())
}

// SetState sets the state of a segment.
func (sc *SegmentCache) SetState(segIdx int, state SegmentState) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	sc.states[segIdx].Store(uint32(state))
}

// MarkFetching atomically transitions Empty → Fetching. Returns true if
// the transition succeeded (caller owns the fetch).
func (sc *SegmentCache) MarkFetching(segIdx int) bool {
	if segIdx < 0 || segIdx >= sc.segCount {
		return false
	}
	return sc.states[segIdx].CompareAndSwap(uint32(StateEmpty), uint32(StateFetching))
}

// MarkFailed records a permanent fetch failure.
func (sc *SegmentCache) MarkFailed(segIdx int, err error) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	sc.errors[segIdx].Store(&err)
	sc.states[segIdx].Store(uint32(StateFailed))
	sc.wakeWaiters(segIdx)
}

// GetError returns the error for a failed segment.
func (sc *SegmentCache) GetError(segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}
	if errPtr := sc.errors[segIdx].Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}

// ResetFailed transitions Failed → Empty so a retry can re-fetch the segment.
// It is a CAS, not a blind store: a concurrent reader may have successfully
// fetched the segment between attempts, and flipping OnDisk → Empty would both
// force a spurious re-download and leak the segment's bytes out of the curDisk
// accounting (inflating it for the life of the reader, making the budget
// backstop over-evict). It must also never clobber another fetcher's Fetching.
func (sc *SegmentCache) ResetFailed(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	if sc.states[segIdx].CompareAndSwap(uint32(StateFailed), uint32(StateEmpty)) {
		sc.errors[segIdx].Store(nil)
	}
}

// ReleaseFetching transitions Fetching → Empty. Only the fetcher that owns the
// Fetching state (won MarkFetching) may call it, on its cancellation paths.
func (sc *SegmentCache) ReleaseFetching(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	sc.states[segIdx].CompareAndSwap(uint32(StateFetching), uint32(StateEmpty))
}

// WaitForSegment blocks until the segment is OnDisk, fails, or the context
// is canceled.
func (sc *SegmentCache) WaitForSegment(ctx context.Context, segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return fmt.Errorf("segment index out of range: %d", segIdx)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	state := SegmentState(sc.states[segIdx].Load())
	switch state {
	case StateOnDisk:
		return nil
	case StateFailed:
		if err := sc.GetError(segIdx); err != nil {
			return err
		}
		return fmt.Errorf("segment %d failed", segIdx)
	}

	shardIdx := segIdx & shardMask
	cond := sc.shardCond[shardIdx]
	mu := &sc.shardMu[shardIdx]

	wakeShard := func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}
	var stopWatchers []func()
	if ctx != nil {
		stopper := context.AfterFunc(ctx, wakeShard)
		stopWatchers = append(stopWatchers, func() { stopper() })
	}
	cacheStopper := context.AfterFunc(sc.ctx, wakeShard)
	stopWatchers = append(stopWatchers, func() { cacheStopper() })
	defer func() {
		for _, stop := range stopWatchers {
			if stop != nil {
				stop()
			}
		}
	}()

	mu.Lock()
	defer mu.Unlock()

	for {
		state = SegmentState(sc.states[segIdx].Load())
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			if err := sc.GetError(segIdx); err != nil {
				return err
			}
			return fmt.Errorf("segment %d failed", segIdx)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.ctx.Done():
			return sc.ctx.Err()
		default:
		}

		cond.Wait()
	}
}

// WaitForEvictionRelease blocks while the segment is in StateEvicting, returning
// once the evictor has finished punching its range and dropped it to Empty (or
// the context/cache is canceled). Callers in the fetch path use this so a
// re-fetch never starts writing into a range mid-Discard.
func (sc *SegmentCache) WaitForEvictionRelease(ctx context.Context, segIdx int) error {
	if segIdx < 0 || segIdx >= sc.segCount {
		return fmt.Errorf("segment index out of range: %d", segIdx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if SegmentState(sc.states[segIdx].Load()) != StateEvicting {
		return nil
	}

	shardIdx := segIdx & shardMask
	cond := sc.shardCond[shardIdx]
	mu := &sc.shardMu[shardIdx]

	wakeShard := func() {
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}
	ctxStopper := context.AfterFunc(ctx, wakeShard)
	cacheStopper := context.AfterFunc(sc.ctx, wakeShard)
	defer ctxStopper()
	defer cacheStopper()

	mu.Lock()
	defer mu.Unlock()

	for SegmentState(sc.states[segIdx].Load()) == StateEvicting {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sc.ctx.Done():
			return sc.ctx.Err()
		default:
		}
		cond.Wait()
	}
	return nil
}

// invalidateForRefetch forces a segment that is marked OnDisk but whose backing
// bytes are unreadable back to Empty so the next Fetch actually re-downloads it
// instead of trusting the stale OnDisk state and short-circuiting. The CAS
// guarantees the disk accounting is rolled back exactly once even if two readers
// hit the same wedged segment concurrently. Safe to call on a pinned segment —
// the subsequent re-fetch overwrites the slot in place.
func (sc *SegmentCache) invalidateForRefetch(segIdx int) {
	if segIdx < 0 || segIdx >= sc.segCount {
		return
	}
	if sc.states[segIdx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEmpty)) {
		if size := sc.segLengths[segIdx].Load(); size > 0 {
			sc.curDisk.Add(-size)
		}
	}
	sc.errors[segIdx].Store(nil)
}

// wakeWaiters wakes any WaitForSegment callers parked on this segment's shard.
func (sc *SegmentCache) wakeWaiters(segIdx int) {
	shardIdx := segIdx & shardMask
	sc.shardMu[shardIdx].Lock()
	sc.shardCond[shardIdx].Broadcast()
	sc.shardMu[shardIdx].Unlock()
}

// touchSegment records the current time as the last access for a segment.
func (sc *SegmentCache) touchSegment(segIdx int) {
	sc.accessTime[segIdx].Store(time.Now().UnixNano())
}

// signalEvict pokes the background evictor (non-blocking).
func (sc *SegmentCache) signalEvict() {
	select {
	case sc.evictSignal <- struct{}{}:
	default:
	}
}

// evictLoop runs the budget-backstop evictor. The proactive sliding-window
// sweeper does the routine work; this only runs if curDisk exceeds maxDisk
// despite the sweeper (e.g. burst of pinned segments).
func (sc *SegmentCache) evictLoop() {
	defer sc.evictWg.Done()
	for {
		select {
		case <-sc.ctx.Done():
			return
		case <-sc.evictSignal:
		}
		sc.drainOverBudget()
	}
}

// drainOverBudget is the hard-disk backstop.
func (sc *SegmentCache) drainOverBudget() {
	if sc.maxDisk <= 0 {
		return
	}

	// StreamWriter, Put, and the background evictor can all notice the same
	// overshoot concurrently. Let one caller do the scan and punching while
	// the others wait; once they acquire the lock the budget is normally
	// already satisfied. Without this guard, N concurrent segment completions
	// can each scan the full segment table and race to evict the same batch.
	sc.evictMu.Lock()
	defer sc.evictMu.Unlock()

	for sc.curDisk.Load() > sc.maxDisk {
		batch := sc.findEvictableBatch(segmentSweepBatch)
		if len(batch) == 0 {
			break
		}
		sc.evictBatch(batch)
	}
}

type evictionCandidate struct {
	idx int
	t   int64
}

// findEvictableBatch returns up to maxN unpinned OnDisk segments, sorted
// oldest-first by access time. Used by drainOverBudget only, with evictMu
// held. The scratch slice is retained so repeated budget checks do not create
// a large allocation-and-GC cycle; its size follows the number of actually
// cached segments, not the total NZB segment count.
func (sc *SegmentCache) findEvictableBatch(maxN int) []int {
	if maxN <= 0 {
		return nil
	}

	cands := sc.evictScratch[:0]
	if cands == nil {
		cands = make([]evictionCandidate, 0, min(maxN*2, sc.segCount))
	}
	for i := 0; i < sc.segCount; i++ {
		if sc.pinCounts[i].Load() > 0 {
			continue
		}
		if SegmentState(sc.states[i].Load()) != StateOnDisk {
			continue
		}
		cands = append(cands, evictionCandidate{i, sc.accessTime[i].Load()})
	}
	if len(cands) == 0 {
		sc.evictScratch = cands
		return nil
	}
	sort.Slice(cands, func(a, b int) bool {
		if cands[a].t != cands[b].t {
			return cands[a].t < cands[b].t
		}
		return cands[a].idx < cands[b].idx
	})
	out := make([]int, min(len(cands), maxN))
	for i, c := range cands[:len(out)] {
		out[i] = c.idx
	}
	sc.evictScratch = cands[:0]
	return out
}

// MarkConsumed records that bytes in [off, off+length) have been delivered
// to a client. Monotonic high-water mark used by the sliding-window
// evictor; backward seeks don't lower it because the back-window already
// absorbs them.
func (sc *SegmentCache) MarkConsumed(off, length int64) {
	if length <= 0 {
		return
	}
	end := off + length
	for {
		cur := sc.maxConsumedOff.Load()
		if end <= cur {
			return
		}
		if sc.maxConsumedOff.CompareAndSwap(cur, end) {
			// Plumb the cursor into the buffer's eviction policy: blocks
			// behind the consumed offset are safe to evict (we're done
			// with them), blocks ahead are the active window the reader
			// will still hit. Cheap atomic store, no buffer lock.
			//
			// Skip the back-window margin (we keep some history pinned at
			// the SegmentCache level for scrub-back); the buffer can be
			// stricter — anything we've explicitly consumed past is fair
			// game for promotion to evict.
			if sc.buf != nil {
				sc.buf.SetReadHead(end)
			}
			return
		}
	}
}

// sweepLoop runs the proactive sliding-window evictor.
func (sc *SegmentCache) sweepLoop() {
	defer sc.sweepWg.Done()
	ticker := time.NewTicker(segmentSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-sc.ctx.Done():
			return
		case <-ticker.C:
			sc.sweepWindow()
		}
	}
}

// sweepWindow picks segments that are both:
//
//  1. Behind the back-window (segEnd < maxConsumedOff - backWindowBytes), and
//  2. Untouched for at least segmentMinRetentionAge.
//
// Both conditions must hold — see the package comment in cache.go for the
// rationale behind each.
func (sc *SegmentCache) sweepWindow() {
	consumedHi := sc.maxConsumedOff.Load()
	if consumedHi <= 0 {
		return
	}
	cutoffOff := consumedHi - backWindowBytes
	if cutoffOff <= 0 {
		return
	}
	cutoffAccessNs := time.Now().Add(-segmentMinRetentionAge).UnixNano()

	indices := make([]int, 0, segmentSweepBatch)
	for i := 0; i < sc.segCount && len(indices) < segmentSweepBatch; i++ {
		if SegmentState(sc.states[i].Load()) != StateOnDisk {
			continue
		}
		if sc.pinCounts[i].Load() > 0 {
			continue
		}
		segEnd := sc.segOffsets[i+1]
		if segEnd > cutoffOff {
			continue
		}
		if sc.accessTime[i].Load() > cutoffAccessNs {
			continue
		}
		indices = append(indices, i)
	}
	if len(indices) == 0 {
		return
	}
	sc.evictBatch(indices)
}

// evictBatch transitions the given segments out of the cache and releases
// their byte ranges through the buffer. Adjacent ranges are coalesced into
// fewer Discard calls — for sequential playback eviction, ~dozen segments
// merge into one buffer.Discard (and thus one fallocate(PUNCH_HOLE)).
//
// Each segment moves OnDisk -> Evicting -> (Discard) -> Empty. The Evicting
// hold is what makes eviction safe against a concurrent re-fetch: MarkFetching
// only transitions Empty -> Fetching, so no fetcher can begin writing into a
// segment's range while we are punching it. Only after the Discard completes do
// we drop the slot to Empty and wake any reader/fetcher that parked on it; that
// re-fetch then writes into a freshly-punched, no-longer-contended range.
//
// Previously the slot went straight to Empty before the (deferred, coalesced)
// Discard, so a reader could re-download the segment in the gap and have its
// bytes punched right back out — leaving the slot OnDisk but unreadable and the
// "segment N still missing after re-fetch" wedge.
func (sc *SegmentCache) evictBatch(indices []int) {
	type rng struct {
		off  int64
		size int64
	}
	pieces := make([]rng, 0, len(indices))
	evicted := make([]int, 0, len(indices))

	for _, idx := range indices {
		if sc.pinCounts[idx].Load() > 0 {
			continue
		}
		// Reserve the segment for eviction. The CAS from OnDisk fences out both
		// a concurrent re-fetch (MarkFetching needs Empty) and another evictor.
		if !sc.states[idx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEvicting)) {
			continue
		}
		size := sc.segLengths[idx].Load()
		if size <= 0 {
			size = sc.segments[idx].Bytes
			if size <= 0 {
				size = sc.segOffsets[idx+1] - sc.segOffsets[idx]
			}
		}
		sc.curDisk.Add(-size)
		sc.stats.Evictions.Add(1)
		pieces = append(pieces, rng{sc.segOffsets[idx], size})
		evicted = append(evicted, idx)
	}
	if len(pieces) == 0 {
		return
	}

	sort.Slice(pieces, func(a, b int) bool { return pieces[a].off < pieces[b].off })

	// Coalesce adjacent ranges into the fewest possible Discard calls.
	merged := pieces[:1]
	for _, r := range pieces[1:] {
		last := &merged[len(merged)-1]
		if last.off+last.size == r.off {
			last.size += r.size
		} else {
			merged = append(merged, r)
		}
	}
	for _, r := range merged {
		if err := sc.buf.Discard(r.off, r.size); err != nil {
			sc.logger.Debug().
				Err(err).
				Int64("offset", r.off).
				Int64("size", r.size).
				Msg("buffer discard failed; slot will be overwritten on next fetch")
		}
	}

	// The disk ranges are gone; release the slots and wake anyone waiting so
	// they re-fetch into the now-punched (and no-longer-contended) range.
	for _, idx := range evicted {
		sc.states[idx].Store(uint32(StateEmpty))
		sc.wakeWaiters(idx)
	}
}

// onBufferEvict is invoked by the buffer pool after it punches a hole behind
// the read head (only when the usenet pool is configured with a disk limit —
// off by default). It marks every segment fully inside the reclaimed range
// Empty so a later read re-fetches it rather than reading a hole. Segments that
// only partially overlap are left alone; the pool only punches present ranges,
// so a partial overlap means the segment straddles the back-window boundary and
// should be kept.
func (sc *SegmentCache) onBufferEvict(off, length int64) {
	end := off + length
	startIdx, endIdx := sc.SegmentsForRange(off, length)
	for idx := startIdx; idx <= endIdx && idx < sc.segCount; idx++ {
		segStart := sc.segOffsets[idx]
		segEnd := sc.segOffsets[idx+1]
		if segStart < off || segEnd > end {
			continue // not fully contained
		}
		if sc.pinCounts[idx].Load() > 0 {
			continue
		}
		if !sc.states[idx].CompareAndSwap(uint32(StateOnDisk), uint32(StateEmpty)) {
			continue
		}
		size := sc.segLengths[idx].Load()
		if size <= 0 {
			size = segEnd - segStart
		}
		sc.curDisk.Add(-size)
		sc.stats.Evictions.Add(1)
	}
}

// SegmentsForRange returns the segment indices covering [offset, offset+length).
func (sc *SegmentCache) SegmentsForRange(offset, length int64) (int, int) {
	if sc.segCount == 0 {
		return 0, 0
	}
	endOffset := offset + length - 1
	startIdx := sc.binarySearchSegment(offset)
	if startIdx >= sc.segCount {
		startIdx = sc.segCount - 1
	}
	endIdx := sc.binarySearchSegment(endOffset)
	if endIdx >= sc.segCount {
		endIdx = sc.segCount - 1
	}
	return startIdx, endIdx
}

// binarySearchSegment finds the segment containing the given offset.
func (sc *SegmentCache) binarySearchSegment(offset int64) int {
	lo, hi := 0, sc.segCount
	for lo < hi {
		mid := (lo + hi) / 2
		if sc.segOffsets[mid+1] <= offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// GetSegment returns segment metadata by index.
func (sc *SegmentCache) GetSegment(segIdx int) *SegmentMeta {
	if segIdx < 0 || segIdx >= sc.segCount {
		return nil
	}
	return &sc.segments[segIdx]
}

// SegmentCount returns the total number of segments.
func (sc *SegmentCache) SegmentCount() int { return sc.segCount }

// TotalSize returns the total size of all segments.
func (sc *SegmentCache) TotalSize() int64 { return sc.totalSize }

// SegmentOffset returns the byte offset of a segment.
func (sc *SegmentCache) SegmentOffset(segIdx int) int64 {
	if segIdx < 0 || segIdx > sc.segCount {
		return 0
	}
	return sc.segOffsets[segIdx]
}

// closeAttempt stops the cache once and makes one independently retryable
// attempt to remove its disk instance. Keeping the two error classes separate
// lets StreamingReader retry only cleanup without losing a buffer-close error.
func (sc *SegmentCache) closeAttempt() (shutdownErr, cleanupErr error) {
	sc.closeMu.Lock()
	defer sc.closeMu.Unlock()

	if !sc.closed.Swap(true) {
		sc.cancel()

		for i := range numShards {
			sc.shardMu[i].Lock()
			sc.shardCond[i].Broadcast()
			sc.shardMu[i].Unlock()
		}

		sc.evictWg.Wait()
		sc.sweepWg.Wait()

		if sc.buf != nil {
			if err := sc.buf.Close(); err != nil {
				shutdownErr = fmt.Errorf("close segment buffer: %w", err)
			}
			sc.buf = nil
		}
		if sc.pools != nil {
			sc.pools.unregister(sc)
		}
		if sc.ownsPools {
			if err := sc.pools.Close(); err != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close segment buffer pool: %w", err))
			}
		}
	}
	if sc.diskPath != "" {
		if err := removeCacheInstance(sc.cacheRoot, sc.diskPath, sc.cacheToken); err != nil {
			cleanupErr = fmt.Errorf("remove segment cache: %w", err)
		} else {
			sc.diskPath = ""
		}
	}
	return shutdownErr, cleanupErr
}

// Close releases all resources. A caller may call Close again after a cleanup
// error; shutdown remains one-shot while the owned disk instance is retried.
func (sc *SegmentCache) Close() error {
	shutdownErr, cleanupErr := sc.closeAttempt()
	return errors.Join(shutdownErr, cleanupErr)
}
