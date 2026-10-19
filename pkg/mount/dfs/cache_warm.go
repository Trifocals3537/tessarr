package dfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs"
)

var _ manager.CacheWarmOpener = (*Manager)(nil)

type cacheWarmStreamFile struct {
	vfs    *vfs.Manager
	info   *manager.FileInfo
	stream *vfs.StreamingFile

	once sync.Once
	err  error
}

func (f *cacheWarmStreamFile) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return f.stream.ReadAtContext(ctx, p, off)
}

func (f *cacheWarmStreamFile) Size() int64 {
	return f.stream.Size()
}

func (f *cacheWarmStreamFile) Close() error {
	f.once.Do(func() {
		if f.stream != nil {
			f.err = f.stream.Close()
		}
		if f.vfs != nil && f.info != nil {
			f.vfs.ReleaseFile(f.info)
		}
	})
	return f.err
}

// OpenCacheWarmFile resolves a Tessarr-created symlink or direct mount path
// to stored entry metadata, then opens a DFS streaming handle directly. It
// must not follow the symlink with os.Open/os.Stat because those calls enter
// FUSE and can block past the import worker's context deadline.
func (m *Manager) OpenCacheWarmFile(ctx context.Context, filePath string) (manager.CacheWarmFile, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m == nil || m.vfs == nil || !m.ready.Load() {
		return nil, fmt.Errorf("%w: DFS VFS is not initialized", manager.ErrCacheWarmUnavailable)
	}

	info, err := m.resolveCacheWarmFileInfo(filePath)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%w: %q resolves to a directory", manager.ErrCacheWarmUnavailable, filePath)
	}
	if !info.IsRemote() {
		return nil, fmt.Errorf("%w: %q is not a remote DFS file", manager.ErrCacheWarmUnavailable, filePath)
	}

	stream, err := m.vfs.GetFile(info)
	if err != nil {
		return nil, err
	}
	return &cacheWarmStreamFile{
		vfs:    m.vfs,
		info:   info,
		stream: stream,
	}, nil
}

func (m *Manager) resolveCacheWarmFileInfo(filePath string) (*manager.FileInfo, error) {
	if m == nil || m.manager == nil {
		return nil, fmt.Errorf("%w: DFS manager is not initialized", manager.ErrCacheWarmUnavailable)
	}
	target, err := m.cacheWarmTarget(filePath)
	if err != nil {
		return nil, err
	}

	rel, ok := m.mountRelativePath(target)
	if !ok {
		return nil, fmt.Errorf("%w: %q is outside DFS mount %q", manager.ErrCacheWarmUnavailable, target, m.config.MountPath)
	}
	return m.resolveVirtualFileInfo(rel)
}

func (m *Manager) cacheWarmTarget(filePath string) (string, error) {
	if m == nil || m.config == nil || strings.TrimSpace(m.config.MountPath) == "" {
		return "", fmt.Errorf("%w: DFS mount path is not configured", manager.ErrCacheWarmUnavailable)
	}

	absolute, err := filepath.Abs(filePath)
	if err != nil {
		return "", fmt.Errorf("resolve cache warm path %q: %w", filePath, err)
	}
	absolute = filepath.Clean(absolute)
	if _, ok := m.mountRelativePath(absolute); ok {
		return absolute, nil
	}

	info, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf("%w: inspect cache warm symlink %q: %v", manager.ErrCacheWarmUnavailable, filePath, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return "", fmt.Errorf("%w: %q is not a symlink into DFS", manager.ErrCacheWarmUnavailable, filePath)
	}

	target, err := os.Readlink(absolute)
	if err != nil {
		return "", fmt.Errorf("read cache warm symlink %q: %w", filePath, err)
	}
	resolved, err := resolveCacheWarmSymlinkTarget(absolute, target)
	if err != nil {
		return "", err
	}
	if _, ok := m.mountRelativePath(resolved); !ok {
		return "", fmt.Errorf("%w: symlink target %q is outside DFS mount %q", manager.ErrCacheWarmUnavailable, resolved, m.config.MountPath)
	}
	return resolved, nil
}

func (m *Manager) mountRelativePath(path string) (string, bool) {
	if m == nil || m.config == nil {
		return "", false
	}
	root, err := filepath.Abs(m.config.MountPath)
	if err != nil {
		return "", false
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}
	root = filepath.Clean(root)
	absolute = filepath.Clean(absolute)

	rel, err := filepath.Rel(root, absolute)
	if err != nil || rel == "." || rel == "" || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

func resolveCacheWarmSymlinkTarget(linkPath, target string) (string, error) {
	if strings.TrimSpace(target) == "" {
		return "", fmt.Errorf("%w: symlink target is empty", manager.ErrCacheWarmUnavailable)
	}
	if filepath.IsAbs(target) {
		return filepath.Clean(target), nil
	}
	return filepath.Clean(filepath.Join(filepath.Dir(linkPath), target)), nil
}

func (m *Manager) resolveVirtualFileInfo(rel string) (*manager.FileInfo, error) {
	rel = strings.Trim(strings.TrimSpace(filepath.ToSlash(filepath.Clean(rel))), "/")
	if rel == "" {
		return nil, fmt.Errorf("%w: empty DFS relative path", manager.ErrCacheWarmUnavailable)
	}
	parts := strings.Split(rel, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: %q is not a DFS file path", manager.ErrCacheWarmUnavailable, rel)
	}

	if len(parts) == 2 {
		if isCanonicalMountGroup(parts[0]) {
			return nil, fmt.Errorf("%w: %q resolves to an entry directory", manager.ErrCacheWarmUnavailable, rel)
		}
		info, err := m.manager.GetTorrentFile(parts[0], parts[1])
		if err != nil {
			return nil, fmt.Errorf("%w: %v", manager.ErrCacheWarmUnavailable, err)
		}
		return info, nil
	}

	info, err := m.manager.GetTorrentFile(parts[1], strings.Join(parts[2:], "/"))
	if err == nil {
		return info, nil
	}
	if !isCanonicalMountGroup(parts[0]) {
		if direct, directErr := m.manager.GetTorrentFile(parts[0], strings.Join(parts[1:], "/")); directErr == nil {
			return direct, nil
		}
	}
	return nil, fmt.Errorf("%w: %v", manager.ErrCacheWarmUnavailable, err)
}

func isCanonicalMountGroup(name string) bool {
	switch name {
	case manager.EntryAllFolder, manager.EntryBadFolder, manager.EntryTorrentFolder, manager.EntryNZBFolder:
		return true
	default:
		return false
	}
}
