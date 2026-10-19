package manager

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"golang.org/x/sync/errgroup"
)

const (
	importReadinessWorkers   = 2
	importReadinessProbeSize = 256 * 1024
)

type importReadinessFile struct {
	path         string
	logicalName  string
	expectedSize int64
}

func (d *Downloader) importReadinessFiles(entry *storage.Entry, filePaths []string, limit int) ([]importReadinessFile, error) {
	if d == nil || entry == nil {
		return nil, errors.New("missing downloader or entry")
	}
	if len(filePaths) == 0 || limit == 0 {
		return nil, nil
	}

	var root string
	filesByPath := make(map[string]*storage.File, len(entry.Files))
	if entry.IsNZB() {
		var err error
		root, err = safeUsenetEntryDownloadPath(d.dest, entry)
		if err != nil {
			return nil, err
		}
		for _, file := range entry.GetActiveFiles() {
			if file == nil {
				continue
			}
			filesByPath[portableReadinessPathKey(file.Name)] = file
		}
	} else {
		var err error
		root, err = safeTorrentEntryDownloadPath(d.dest, entry)
		if err != nil {
			return nil, err
		}
		layouts, err := torrentEntryFileLayouts(entry)
		if err != nil {
			return nil, err
		}
		for _, layout := range layouts {
			filesByPath[portableReadinessPathKey(layout.relative)] = layout.file
		}
	}

	candidates := make([]importReadinessFile, 0, len(filePaths))
	seen := make(map[string]struct{}, len(filePaths))
	for _, path := range filePaths {
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." || relative == "" || filepath.IsAbs(relative) ||
			relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("output path %q is outside owned import root", path)
		}
		key := portableReadinessPathKey(relative)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate import output path %q", relative)
		}
		seen[key] = struct{}{}
		file := filesByPath[key]
		if file == nil {
			return nil, fmt.Errorf("output path %q has no matching file metadata", relative)
		}
		if !utils.IsMediaFile(file.Name) {
			continue
		}
		if file.Size <= 0 {
			return nil, fmt.Errorf("media file %q has invalid expected size %d", file.Name, file.Size)
		}
		candidates = append(candidates, importReadinessFile{
			path:         path,
			logicalName:  file.Name,
			expectedSize: file.Size,
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return portableReadinessPathKey(candidates[i].path) < portableReadinessPathKey(candidates[j].path)
	})
	return evenlySampleReadinessFiles(candidates, limit), nil
}

func evenlySampleReadinessFiles(files []importReadinessFile, limit int) []importReadinessFile {
	if limit <= 0 || len(files) == 0 {
		return nil
	}
	if len(files) <= limit {
		return append([]importReadinessFile(nil), files...)
	}
	if limit == 1 {
		return []importReadinessFile{files[0]}
	}
	out := make([]importReadinessFile, 0, limit)
	for i := 0; i < limit; i++ {
		index := i * (len(files) - 1) / (limit - 1)
		out = append(out, files[index])
	}
	return out
}

func portableReadinessPathKey(path string) string {
	return strings.ToLower(strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/"))
}

func countMediaPaths(paths []string) int {
	count := 0
	for _, path := range paths {
		if utils.IsMediaFile(path) {
			count++
		}
	}
	return count
}

func (m *Manager) verifyImportReadiness(ctx context.Context, infoHash string, files []importReadinessFile) error {
	if len(files) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.storage == nil || m.mountManager == nil || !m.mountManager.IsReady() {
		return ErrCacheWarmUnavailable
	}
	opener, ok := m.mountManager.(CacheWarmOpener)
	if !ok || opener == nil {
		return ErrCacheWarmUnavailable
	}

	before, err := m.GetEntry(infoHash)
	if err != nil {
		return fmt.Errorf("load import target: %w", err)
	}
	if before == nil {
		return errors.New("load import target: entry not found")
	}
	beforeFingerprint, err := importReadinessTargetFingerprint(before)
	if err != nil {
		return err
	}

	probeCtx, cancel := context.WithTimeout(ctx, ImportReadinessTimeout)
	defer cancel()
	if m.ctx != nil {
		stop := context.AfterFunc(m.ctx, cancel)
		defer stop()
	}
	g, groupCtx := errgroup.WithContext(probeCtx)
	g.SetLimit(min(importReadinessWorkers, len(files)))
	for _, file := range files {
		file := file
		g.Go(func() error {
			release, err := m.acquireCacheWarmSlot(groupCtx)
			if err != nil {
				return err
			}
			defer release()
			if err := verifyImportReadinessFile(groupCtx, opener, file); err != nil {
				return fmt.Errorf("file %q: %w", file.logicalName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if err := probeCtx.Err(); err != nil {
		return err
	}

	after, err := m.GetEntry(infoHash)
	if err != nil {
		return fmt.Errorf("reload import target: %w", err)
	}
	if after == nil {
		return errors.New("reload import target: entry not found")
	}
	afterFingerprint, err := importReadinessTargetFingerprint(after)
	if err != nil {
		return err
	}
	if beforeFingerprint != afterFingerprint {
		return errors.New("provider target changed during import readiness verification")
	}
	return nil
}

func verifyImportReadinessFile(ctx context.Context, opener CacheWarmOpener, file importReadinessFile) (err error) {
	handle, err := opener.OpenCacheWarmFile(ctx, file.path)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, handle.Close())
	}()

	actualSize := handle.Size()
	if actualSize != file.expectedSize {
		return fmt.Errorf("logical size %d does not match expected size %d", actualSize, file.expectedSize)
	}
	for _, offset := range importReadinessOffsets(actualSize, importReadinessProbeSize) {
		length := min(int64(importReadinessProbeSize), actualSize-offset)
		if err := readExactReadinessRange(ctx, handle, offset, length); err != nil {
			return fmt.Errorf("range %d-%d: %w", offset, offset+length-1, err)
		}
	}
	return nil
}

func importReadinessOffsets(size int64, probeSize int) []int64 {
	if size <= 0 || probeSize <= 0 {
		return nil
	}
	width := min(int64(probeSize), size)
	candidates := []int64{0, (size - width) / 2, size - width}
	offsets := make([]int64, 0, len(candidates))
	seen := make(map[int64]struct{}, len(candidates))
	for _, offset := range candidates {
		if _, exists := seen[offset]; exists {
			continue
		}
		seen[offset] = struct{}{}
		offsets = append(offsets, offset)
	}
	return offsets
}

func readExactReadinessRange(ctx context.Context, reader CacheWarmFile, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	buffer := make([]byte, int(length))
	read := 0
	for read < len(buffer) {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := reader.ReadAtContext(ctx, buffer[read:], offset+int64(read))
		if n < 0 || n > len(buffer)-read {
			return fmt.Errorf("invalid read count %d", n)
		}
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) && read == len(buffer) {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func importReadinessTargetFingerprint(entry *storage.Entry) ([sha256.Size]byte, error) {
	if entry == nil {
		return [sha256.Size]byte{}, errors.New("import target is nil")
	}
	placement := entry.GetActiveProvider()
	if placement == nil {
		return [sha256.Size]byte{}, errors.New("import target has no active provider placement")
	}
	h := sha256.New()
	writeReadinessFingerprint(h, string(entry.Protocol))
	writeReadinessFingerprint(h, entry.InfoHash)
	writeReadinessFingerprint(h, entry.ActiveProvider)
	writeReadinessFingerprint(h, placement.Provider)
	writeReadinessFingerprint(h, placement.ID)
	writeReadinessFingerprint(h, string(placement.Status))

	fileNames := make([]string, 0, len(entry.Files))
	for name := range entry.Files {
		fileNames = append(fileNames, name)
	}
	sort.Strings(fileNames)
	for _, name := range fileNames {
		file := entry.Files[name]
		if file == nil || file.Deleted {
			continue
		}
		writeReadinessFingerprint(h, name)
		writeReadinessFingerprint(h, file.ID)
		writeReadinessFingerprint(h, file.Name)
		writeReadinessFingerprint(h, file.Path)
		writeReadinessFingerprint(h, fmt.Sprintf("%d", file.Size))
		if file.ByteRange != nil {
			writeReadinessFingerprint(h, fmt.Sprintf("%d:%d", file.ByteRange[0], file.ByteRange[1]))
		}
	}

	providerFileNames := make([]string, 0, len(placement.Files))
	for name := range placement.Files {
		providerFileNames = append(providerFileNames, name)
	}
	sort.Strings(providerFileNames)
	for _, name := range providerFileNames {
		writeReadinessFingerprint(h, name)
		providerFile := placement.Files[name]
		if providerFile == nil {
			writeReadinessFingerprint(h, "<nil>")
			continue
		}
		writeReadinessFingerprint(h, providerFile.Id)
		writeReadinessFingerprint(h, providerFile.Path)
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

func writeReadinessFingerprint(h hash.Hash, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = io.WriteString(h, value)
}
