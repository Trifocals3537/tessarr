package manager

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/sourcegraph/conc/pool"
)

const (
	MaxCacheWarmWorkers     = 10
	MaxImportReadinessFiles = 5
	// MaxNZBPreCacheFiles is retained for source compatibility. Import
	// readiness now samples both torrent and NZB media files.
	MaxNZBPreCacheFiles    = MaxImportReadinessFiles
	CacheWarmTimeout       = 60 * time.Second
	ImportReadinessTimeout = 2 * time.Minute

	// Container metadata lives at the head (streamable MP4 moov, EBML header)
	// or the tail (non-streamable MP4 moov, MKV cues/seek index), so warming
	// head+tail covers what a downstream ffprobe/import scan will seek to.
	cacheWarmHeadSize = 2 * 1024 * 1024 // 2MB
	cacheWarmTailSize = 2 * 1024 * 1024 // 2MB
)

var ErrCacheWarmUnavailable = errors.New("cache warm unavailable")

type CacheWarmFile interface {
	ReadAtContext(ctx context.Context, p []byte, off int64) (int, error)
	Size() int64
	Close() error
}

type CacheWarmOpener interface {
	OpenCacheWarmFile(ctx context.Context, filePath string) (CacheWarmFile, error)
}

type MountManager interface {
	Start(ctx context.Context) error
	Stop() error
	Stats() map[string]any
	IsReady() bool
	Type() string
	Refresh(dirs []string) error
}

func (m *Manager) RefreshEntries(refreshMount bool) {
	// Refresh entries
	m.entry.Refresh()

	// Refresh mount if needed
	if refreshMount {
		m.startBackground("mount refresh", func() {
			_ = m.RefreshMount()
		})
	}
}

func (m *Manager) RefreshMount() error {
	dirs := strings.FieldsFunc(m.config.RefreshDirs, func(r rune) bool {
		return r == ',' || r == '&'
	})
	if len(dirs) == 0 {
		dirs = []string{"__all__"}
	}

	// Call event handler if set
	if m.mountManager != nil {
		return m.mountManager.Refresh(dirs)
	}
	return nil
}

// WarmFileCache reads the head and tail of each media file through the mount
// to warm the VFS disk cache, so a subsequent media probe or import scan over
// the mount is fast. This replaces spawning ffprobe: the read pattern is
// deterministic, needs no external binary, and warms the exact bytes a
// downstream probe seeks to (see cacheWarmHeadSize/cacheWarmTailSize).
func (m *Manager) WarmFileCache(ctx context.Context, filePaths []string) error {
	if len(filePaths) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.mountManager == nil || !m.mountManager.IsReady() {
		return ErrCacheWarmUnavailable
	}
	opener, ok := m.mountManager.(CacheWarmOpener)
	if !ok || opener == nil {
		return ErrCacheWarmUnavailable
	}

	mediaFiles := make([]string, 0, len(filePaths))
	for _, fp := range filePaths {
		if utils.IsMediaFile(fp) {
			mediaFiles = append(mediaFiles, fp)
		}
	}
	if len(mediaFiles) == 0 {
		return nil
	}

	warmCtx, cancel := context.WithTimeout(ctx, CacheWarmTimeout)
	defer cancel()
	if m.ctx != nil {
		stop := context.AfterFunc(m.ctx, cancel)
		defer stop()
	}

	// Use a worker pool to limit per-batch concurrency. Each worker also
	// acquires a manager-wide slot so concurrent imports share the same cap.
	p := pool.New().
		WithContext(warmCtx).
		WithMaxGoroutines(min(len(mediaFiles), MaxCacheWarmWorkers))

	for _, fp := range mediaFiles {
		fp := fp
		p.Go(func(workerCtx context.Context) error {
			release, err := m.acquireCacheWarmSlot(workerCtx)
			if err != nil {
				return err
			}
			defer release()

			if err := m.warmOneFile(workerCtx, opener, fp); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				if errors.Is(err, ErrCacheWarmUnavailable) {
					m.logger.Debug().
						Err(err).
						Str("file", fp).
						Msg("cache warm skipped")
					return nil
				}
				// Log error but continue
				m.logger.Warn().
					Err(err).
					Str("file", fp).
					Msg("cache warm failed")
			}
			return nil
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}
	return warmCtx.Err()
}

func (m *Manager) acquireCacheWarmSlot(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sem := m.cacheWarmSemaphore()
	select {
	case sem <- struct{}{}:
		return func() {
			<-sem
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Manager) cacheWarmSemaphore() chan struct{} {
	m.cacheWarmMu.Lock()
	defer m.cacheWarmMu.Unlock()
	if m.cacheWarmSem == nil {
		m.cacheWarmSem = make(chan struct{}, MaxCacheWarmWorkers)
	}
	return m.cacheWarmSem
}

// warmOneFile reads the head and (for large enough files) the tail through a
// context-aware mount handle. It deliberately avoids os.Open/os.Stat on the
// symlink target because those operations can block indefinitely in FUSE.
func (m *Manager) warmOneFile(ctx context.Context, opener CacheWarmOpener, path string) error {
	f, err := opener.OpenCacheWarmFile(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	size := f.Size()
	if size == 0 {
		return nil
	}

	head := min(int64(cacheWarmHeadSize), size)
	if err := drainRange(ctx, f, 0, head); err != nil {
		return err
	}

	// Only warm the tail when it doesn't overlap the head we just read.
	if size > int64(cacheWarmHeadSize)+int64(cacheWarmTailSize) {
		if err := drainRange(ctx, f, size-int64(cacheWarmTailSize), int64(cacheWarmTailSize)); err != nil {
			return err
		}
	}
	return nil
}

// drainRange reads length bytes starting at off, in chunks, discarding the
// data and passing ctx into every read so a stalled mount can't pin a worker
// past CacheWarmTimeout.
func drainRange(ctx context.Context, r CacheWarmFile, off, length int64) error {
	const chunk = 1 << 20 // 1MB
	buf := make([]byte, chunk)
	for read := int64(0); read < length; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(length-read, chunk)
		got, err := r.ReadAtContext(ctx, buf[:int(n)], off+read)
		read += int64(got)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if got == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

type stubMountManager struct{}

func (s *stubMountManager) Refresh(dirs []string) error {
	return nil
}

func NewStubMountManager() MountManager {
	return &stubMountManager{}
}

func (s *stubMountManager) Start(ctx context.Context) error {
	return nil
}
func (s *stubMountManager) Stop() error {
	return nil
}
func (s *stubMountManager) Stats() map[string]any {
	return map[string]any{
		"message": "no mount configured",
	}
}
func (s *stubMountManager) IsReady() bool {
	return false
}
func (s *stubMountManager) Type() string {
	return "none"
}
