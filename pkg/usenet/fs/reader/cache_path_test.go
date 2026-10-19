package reader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrepareDiskCacheRootPreservesExistingInstances(t *testing.T) {
	base := t.TempDir()
	baseSentinel := filepath.Join(base, "keep.txt")
	if err := os.WriteFile(baseSentinel, []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}

	cacheRoot, stale, _, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "segments.bin"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	unmarked := filepath.Join(cacheRoot, "cache-user-data")
	if err := os.Mkdir(unmarked, 0o700); err != nil {
		t.Fatal(err)
	}
	unmarkedSentinel := filepath.Join(unmarked, "keep.txt")
	if err := os.WriteFile(unmarkedSentinel, []byte("owned-root sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	unknownFile := filepath.Join(cacheRoot, "notes.txt")
	if err := os.WriteFile(unknownFile, []byte("unknown"), 0o600); err != nil {
		t.Fatal(err)
	}

	prepared, err := PrepareDiskCacheRoot(base)
	if err != nil {
		t.Fatal(err)
	}
	if prepared != cacheRoot {
		t.Fatalf("PrepareDiskCacheRoot() = %q, want %q", prepared, cacheRoot)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("marked pre-existing cache was removed: %v", err)
	}
	assertFileContents(t, filepath.Join(stale, "segments.bin"), "stale")
	assertFileContents(t, baseSentinel, "base")
	assertFileContents(t, unmarkedSentinel, "owned-root sentinel")
	assertFileContents(t, unknownFile, "unknown")

	if runtime.GOOS != "windows" {
		info, err := os.Stat(cacheRoot)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("owned cache mode = %o, want 700", got)
		}
	}
}

func TestPrepareDiskCacheRootAcceptsPreCutoverNamespace(t *testing.T) {
	base := t.TempDir()
	legacyRoot := filepath.Join(base, legacyOwnedCacheDirName)
	if err := os.Mkdir(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(legacyRoot, legacyCacheOwnerFileName),
		[]byte(cacheOwnerContents),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(legacyRoot, "preserve")
	if err := os.WriteFile(sentinel, []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}

	prepared, err := PrepareDiskCacheRoot(base)
	if err != nil {
		t.Fatalf("pre-cutover cache root was not accepted: %v", err)
	}
	if prepared != legacyRoot {
		t.Fatalf("prepared cache root = %q, want %q", prepared, legacyRoot)
	}
	assertFileContents(t, sentinel, "cache")
	if _, err := os.Lstat(filepath.Join(base, ownedCacheDirName)); !os.IsNotExist(err) {
		t.Fatalf("current cache namespace was created beside the legacy namespace: %v", err)
	}
}

func TestPrepareDiskCacheRootRejectsAmbiguousNamespacesAndMarkers(t *testing.T) {
	base := t.TempDir()
	for _, name := range ownedCacheDirNames {
		if err := os.Mkdir(filepath.Join(base, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := PrepareDiskCacheRoot(base); err == nil || !strings.Contains(err.Error(), "both Tessarr and legacy") {
		t.Fatalf("ambiguous cache namespaces error = %v", err)
	}

	markerBase := t.TempDir()
	cacheRoot := filepath.Join(markerBase, ownedCacheDirName)
	if err := os.Mkdir(cacheRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheRoot, cacheOwnerFileName), []byte(cacheOwnerContents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheRoot, legacyCacheOwnerFileName), []byte("wrong\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareDiskCacheRoot(markerBase); err == nil || !strings.Contains(err.Error(), "markers disagree") {
		t.Fatalf("conflicting cache markers error = %v", err)
	}
}

func TestPrepareDiskCacheRootRefusesUnownedNonEmptyNamespace(t *testing.T) {
	base := t.TempDir()
	cacheRoot := filepath.Join(base, ownedCacheDirName)
	if err := os.Mkdir(cacheRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(cacheRoot, "do-not-delete")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := PrepareDiskCacheRoot(base); err == nil {
		t.Fatal("PrepareDiskCacheRoot() claimed a non-empty unowned namespace")
	}
	assertFileContents(t, sentinel, "keep")
}

func TestPrepareDiskCacheRootRejectsOversizedOwnershipMarker(t *testing.T) {
	base := t.TempDir()
	cacheRoot := filepath.Join(base, ownedCacheDirName)
	if err := os.Mkdir(cacheRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	oversized := cacheOwnerContents + strings.Repeat("x", 4096)
	if err := os.WriteFile(filepath.Join(cacheRoot, cacheOwnerFileName), []byte(oversized), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := PrepareDiskCacheRoot(base); err == nil {
		t.Fatal("PrepareDiskCacheRoot() accepted an oversized ownership marker")
	}
	assertFileContents(t, filepath.Join(cacheRoot, cacheOwnerFileName), oversized)
}

func TestPrepareDiskCacheRootRejectsDangerousRoots(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	volumeRoot := filepath.VolumeName(home) + string(filepath.Separator)
	for name, root := range map[string]string{
		"empty":           "",
		"filesystem root": volumeRoot,
		"user home":       home,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := PrepareDiskCacheRoot(root); err == nil {
				t.Fatalf("PrepareDiskCacheRoot(%q) error = nil", root)
			}
		})
	}
}

func TestPrepareDiskCacheRootRejectsSymlinkRootButIgnoresOldInstances(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(base, "linked-cache")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := PrepareDiskCacheRoot(link); err == nil {
		t.Fatal("PrepareDiskCacheRoot() accepted a symlink root")
	}

	safeBase := t.TempDir()
	cacheRoot, err := ensureDiskCacheRoot(safeBase)
	if err != nil {
		t.Fatal(err)
	}
	outsideInstance := filepath.Join(outside, "instance")
	if err := os.Mkdir(outsideInstance, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outsideInstance, cacheInstanceFile), []byte(cacheInstanceData+strings.Repeat("a", 64)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	instanceLink := filepath.Join(cacheRoot, "cache-linked")
	if err := os.Symlink(outsideInstance, instanceLink); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareDiskCacheRoot(safeBase); err != nil {
		t.Fatalf("PrepareDiskCacheRoot() inspected an old cache instance: %v", err)
	}
	if _, err := os.Stat(outsideInstance); err != nil {
		t.Fatalf("outside cache instance was changed: %v", err)
	}
}

func TestPrepareDiskCacheRootRejectsSymlinkNamespaceButIgnoresOldMarker(t *testing.T) {
	base := t.TempDir()
	outside := t.TempDir()
	outsideSentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideSentinel, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, ownedCacheDirName)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := PrepareDiskCacheRoot(base); err == nil {
		t.Fatal("PrepareDiskCacheRoot() accepted a symlink owned namespace")
	}
	assertFileContents(t, outsideSentinel, "outside")

	markerBase := t.TempDir()
	cacheRoot, err := ensureDiskCacheRoot(markerBase)
	if err != nil {
		t.Fatal(err)
	}
	instance := filepath.Join(cacheRoot, "cache-bad-marker")
	if err := os.Mkdir(instance, 0o700); err != nil {
		t.Fatal(err)
	}
	outsideMarker := filepath.Join(outside, "marker")
	if err := os.WriteFile(outsideMarker, []byte(cacheInstanceData+strings.Repeat("b", 64)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideMarker, filepath.Join(instance, cacheInstanceFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareDiskCacheRoot(markerBase); err != nil {
		t.Fatalf("PrepareDiskCacheRoot() inspected an old cache marker: %v", err)
	}
	assertFileContents(t, outsideSentinel, "outside")
}

func TestRemoveCacheInstanceRejectsUnmarkedAndOutsideTargets(t *testing.T) {
	base := t.TempDir()
	cacheRoot, err := ensureDiskCacheRoot(base)
	if err != nil {
		t.Fatal(err)
	}

	unmarked := filepath.Join(cacheRoot, "cache-unmarked")
	if err := os.Mkdir(unmarked, 0o700); err != nil {
		t.Fatal(err)
	}
	unmarkedSentinel := filepath.Join(unmarked, "keep")
	if err := os.WriteFile(unmarkedSentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeCacheInstance(cacheRoot, unmarked, strings.Repeat("c", 64)); err == nil {
		t.Fatal("removeCacheInstance() accepted an unmarked directory")
	}
	assertFileContents(t, unmarkedSentinel, "keep")

	outside := t.TempDir()
	outsideToken := strings.Repeat("d", 64)
	if err := os.WriteFile(filepath.Join(outside, cacheInstanceFile), []byte(cacheInstanceData+outsideToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	outsideSentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideSentinel, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeCacheInstance(cacheRoot, outside, outsideToken); err == nil {
		t.Fatal("removeCacheInstance() accepted an outside directory")
	}
	assertFileContents(t, outsideSentinel, "outside")
}

func TestRemoveCacheInstanceRemovesCurrentMarkedInstance(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(instance, "segments.bin"), []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeCacheInstance(cacheRoot, instance, token); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(instance); !os.IsNotExist(err) {
		t.Fatalf("current cache instance still exists: %v", err)
	}
}

func TestRemoveCacheInstanceRejectsWrongUniqueTokenWithoutMovingInstance(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, _, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(instance, "segments.bin")
	if err := os.WriteFile(sentinel, []byte("live"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, strings.Repeat("e", 64)); err == nil {
		t.Fatal("removeCacheInstance() accepted the wrong unique owner token")
	}
	assertFileContents(t, sentinel, "live")
}

func TestRemoveCacheInstanceRejectsSpoofedQuarantine(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(instance); err != nil {
		t.Fatal(err)
	}
	relative := filepath.Base(instance)
	spoof := filepath.Join(cacheRoot, cacheQuarantinePrefixForInstance(relative, token)+"spoof")
	if err := os.Mkdir(spoof, 0o700); err != nil {
		t.Fatal(err)
	}
	wrongToken := strings.Repeat("f", 64)
	if err := os.WriteFile(filepath.Join(spoof, cacheInstanceFile), []byte(cacheInstanceData+wrongToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(spoof, "keep")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, token); err == nil {
		t.Fatal("removeCacheInstance() accepted a spoofed quarantine")
	}
	assertFileContents(t, sentinel, "preserve")
}

func TestRemoveCacheInstanceRecoversMatchingQuarantine(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	relative := filepath.Base(instance)
	quarantine := filepath.Join(cacheRoot, cacheQuarantinePrefixForInstance(relative, token)+"crash")
	if err := os.Rename(instance, quarantine); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, token); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(quarantine); !os.IsNotExist(err) {
		t.Fatalf("matching quarantine still exists: %v", err)
	}
}

func TestRemoveCacheInstanceRecoversPreCutoverMarkerAndQuarantine(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		filepath.Join(instance, cacheInstanceFile),
		filepath.Join(instance, legacyCacheInstanceFile),
	); err != nil {
		t.Fatal(err)
	}
	relative := filepath.Base(instance)
	legacyQuarantine := filepath.Join(
		cacheRoot,
		legacyCacheQuarantine+strings.TrimPrefix(
			cacheQuarantinePrefixForInstance(relative, token),
			cacheQuarantine,
		)+"crash",
	)
	if err := os.Rename(instance, legacyQuarantine); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, token); err != nil {
		t.Fatalf("pre-cutover cache quarantine recovery failed: %v", err)
	}
	if _, err := os.Lstat(legacyQuarantine); !os.IsNotExist(err) {
		t.Fatalf("pre-cutover cache quarantine remained: %v", err)
	}
}

func TestRemoveCacheInstanceFindsPortableCaseQuarantine(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	relative := filepath.Base(instance)
	quarantine := filepath.Join(cacheRoot, strings.ToUpper(cacheQuarantinePrefixForInstance(relative, token))+"crash")
	if err := os.Rename(instance, quarantine); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, token); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(quarantine); !os.IsNotExist(err) {
		t.Fatalf("portable-case quarantine still exists: %v", err)
	}
}

func TestCacheCleanupLockTimesOutUnderContention(t *testing.T) {
	cacheRoot, err := ensureDiskCacheRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := acquireCacheCleanupLock(cacheRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unlock()

	previousTimeout := cacheCleanupLockTimeout
	previousDelay := cacheCleanupRetryDelay
	cacheCleanupLockTimeout = 75 * time.Millisecond
	cacheCleanupRetryDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		cacheCleanupLockTimeout = previousTimeout
		cacheCleanupRetryDelay = previousDelay
	})
	started := time.Now()
	if _, err := acquireCacheCleanupLock(cacheRoot); err == nil {
		t.Fatal("second cache cleanup lock unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cache cleanup lock timeout took %s", elapsed)
	}
}

func TestCacheCleanupLockContendsWithPreCutoverLock(t *testing.T) {
	cacheRoot, err := ensureDiskCacheRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := acquireCacheCleanupLockNamed(cacheRoot, legacyCacheCleanupLock)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Unlock()

	previousTimeout := cacheCleanupLockTimeout
	previousDelay := cacheCleanupRetryDelay
	cacheCleanupLockTimeout = 75 * time.Millisecond
	cacheCleanupRetryDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		cacheCleanupLockTimeout = previousTimeout
		cacheCleanupRetryDelay = previousDelay
	})
	if combined, err := acquireCacheCleanupLock(cacheRoot); err == nil {
		_ = combined.Unlock()
		t.Fatal("current cache cleanup ignored the pre-cutover lock")
	}
}

func TestCacheMarkerReadIsBounded(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	oversized := cacheInstanceData + token + strings.Repeat("x", 4096) + "\n"
	if err := os.WriteFile(filepath.Join(instance, cacheInstanceFile), []byte(oversized), 0o600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(instance, "keep")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := removeCacheInstance(cacheRoot, instance, token); err == nil {
		t.Fatal("removeCacheInstance() accepted an oversized owner marker")
	}
	assertFileContents(t, sentinel, "preserve")
}

func TestSegmentCacheCloseRetriesCleanupAfterLockTimeout(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	cacheLock, err := acquireCacheCleanupLock(cacheRoot)
	if err != nil {
		t.Fatal(err)
	}

	previousTimeout := cacheCleanupLockTimeout
	previousDelay := cacheCleanupRetryDelay
	cacheCleanupLockTimeout = 75 * time.Millisecond
	cacheCleanupRetryDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		cacheCleanupLockTimeout = previousTimeout
		cacheCleanupRetryDelay = previousDelay
	})

	cache := &SegmentCache{
		cacheRoot:  cacheRoot,
		diskPath:   instance,
		cacheToken: token,
	}
	cache.closed.Store(true)
	if err := cache.Close(); err == nil {
		t.Fatal("first Close() unexpectedly succeeded while cleanup lock was held")
	}
	if _, err := os.Stat(instance); err != nil {
		t.Fatalf("cache instance disappeared after failed cleanup: %v", err)
	}
	if err := cacheLock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("second Close() did not retry cleanup: %v", err)
	}
	if _, err := os.Stat(instance); !os.IsNotExist(err) {
		t.Fatalf("cache instance still exists after retry: %v", err)
	}
}

func TestStreamingReaderProductionCleanupRetriesTransientCacheLock(t *testing.T) {
	base := t.TempDir()
	cacheRoot, instance, token, err := newCacheInstance(base)
	if err != nil {
		t.Fatal(err)
	}
	cacheLock, err := acquireCacheCleanupLock(cacheRoot)
	if err != nil {
		t.Fatal(err)
	}

	previousTimeout := cacheCleanupLockTimeout
	previousLockDelay := cacheCleanupRetryDelay
	previousCleanupBackoff := cacheCleanupBackoff
	cacheCleanupLockTimeout = 75 * time.Millisecond
	cacheCleanupRetryDelay = 5 * time.Millisecond
	cacheCleanupBackoff = 10 * time.Millisecond
	t.Cleanup(func() {
		cacheCleanupLockTimeout = previousTimeout
		cacheCleanupRetryDelay = previousLockDelay
		cacheCleanupBackoff = previousCleanupBackoff
	})

	cache := &SegmentCache{
		cacheRoot:  cacheRoot,
		diskPath:   instance,
		cacheToken: token,
	}
	cache.closed.Store(true)

	readerCtx, readerCancel := context.WithCancel(context.Background())
	fetcherCtx, fetcherCancel := context.WithCancel(readerCtx)
	var cancelCalls atomic.Int32
	streamReader := &StreamingReader{
		cache: cache,
		fetcher: &SegmentFetcher{
			ctx:    fetcherCtx,
			cancel: fetcherCancel,
		},
		cancel: func() {
			cancelCalls.Add(1)
			readerCancel()
		},
	}

	unlockResult := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		unlockResult <- cacheLock.Unlock()
	}()

	// This mirrors each production owner: invoke the cleanup closure once,
	// discard Close's error, and release its only reader reference.
	cleanup := func() {
		_ = streamReader.Close()
	}
	cleanup()

	if err := <-unlockResult; err != nil {
		t.Fatal(err)
	}
	if got := cancelCalls.Load(); got != 1 {
		t.Fatalf("reader cancellation called %d times, want 1", got)
	}
	if _, err := os.Stat(instance); !os.IsNotExist(err) {
		t.Fatalf("cache instance still exists after one production cleanup call: %v", err)
	}
}

func TestRetryCacheCleanupDoesNotRetryOwnershipFailure(t *testing.T) {
	var attempts atomic.Int32
	ownershipErr := fmt.Errorf("cache instance owner token mismatch")
	err := retryCacheCleanup(func() error {
		attempts.Add(1)
		return ownershipErr
	})
	if !errors.Is(err, ownershipErr) {
		t.Fatalf("retryCacheCleanup() error = %v, want %v", err, ownershipErr)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("cleanup attempted %d times for a permanent ownership failure, want 1", got)
	}
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if string(contents) != want {
		t.Fatalf("%q contents = %q, want %q", path, contents, want)
	}
}
