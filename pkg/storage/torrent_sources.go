package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

const (
	torrentSourceDirName  = "torrent-sources"
	torrentSourceSuffix   = ".torrent"
	torrentSourceDirMode  = 0700
	torrentSourceMode     = 0600
	torrentSourceMaxFiles = 100_000
)

// Keep the durable source cache bounded independently of queue limits. The
// per-file parser ceiling remains 64 MiB; this total ceiling prevents a long-
// lived installation from consuming storage without bound.
var torrentSourceStoreMaxBytes int64 = 1 << 30

func normalizeTorrentSourceHash(infoHash string) (string, error) {
	infoHash = strings.ToLower(strings.TrimSpace(infoHash))
	if len(infoHash) != 40 {
		return "", fmt.Errorf("torrent source infohash must be 40 hexadecimal characters")
	}
	decoded, err := hex.DecodeString(infoHash)
	if err != nil || len(decoded) != 20 {
		return "", fmt.Errorf("torrent source infohash must be 40 hexadecimal characters")
	}
	return infoHash, nil
}

func (s *Storage) torrentSourceDir() string {
	return filepath.Join(s.dir, torrentSourceDirName)
}

func (s *Storage) torrentSourcePath(infoHash string) (string, error) {
	infoHash, err := normalizeTorrentSourceHash(infoHash)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.torrentSourceDir(), infoHash+torrentSourceSuffix), nil
}

func (s *Storage) openTorrentSourceRoot(create bool) (*os.Root, error) {
	baseRoot, _, err := safepath.OpenRoot(s.dir)
	if err != nil {
		return nil, fmt.Errorf("open storage root: %w", err)
	}
	defer baseRoot.Close()

	info, statErr := baseRoot.Lstat(torrentSourceDirName)
	if os.IsNotExist(statErr) && create {
		if err := baseRoot.Mkdir(torrentSourceDirName, torrentSourceDirMode); err != nil {
			return nil, fmt.Errorf("create torrent source directory: %w", err)
		}
		info, statErr = baseRoot.Lstat(torrentSourceDirName)
	}
	if statErr != nil {
		return nil, fmt.Errorf("inspect torrent source directory: %w", statErr)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("torrent source path is not a private directory")
	}

	rooted, err := baseRoot.OpenRoot(torrentSourceDirName)
	if err != nil {
		return nil, fmt.Errorf("open torrent source directory: %w", err)
	}
	openedInfo, err := rooted.Stat(".")
	if err != nil || !openedInfo.IsDir() || !os.SameFile(info, openedInfo) {
		_ = rooted.Close()
		return nil, fmt.Errorf("torrent source directory changed while opening")
	}
	if create {
		if err := rooted.Chmod(".", torrentSourceDirMode); err != nil {
			_ = rooted.Close()
			return nil, fmt.Errorf("secure torrent source directory: %w", err)
		}
	}
	return rooted, nil
}

// SaveTorrentSource validates and durably stores the exact torrent source used
// for provider submission. A mismatched key is rejected so a corrupted or
// substituted file cannot be sent during restart recovery or repair.
func (s *Storage) SaveTorrentSource(infoHash string, data []byte) error {
	infoHash, err := normalizeTorrentSourceHash(infoHash)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("torrent source is empty")
	}
	if int64(len(data)) > utils.MaxMetadataFileBytes {
		return fmt.Errorf("torrent source exceeds %d bytes", utils.MaxMetadataFileBytes)
	}
	parsed, err := utils.GetMagnetFromBytes(data, false)
	if err != nil {
		return fmt.Errorf("validate torrent source: %w", err)
	}
	if parsed.InfoHash != infoHash {
		return fmt.Errorf("torrent source infohash mismatch")
	}

	s.torrentSourcesMu.Lock()
	defer s.torrentSourcesMu.Unlock()

	rooted, err := s.openTorrentSourceRoot(true)
	if err != nil {
		return err
	}
	defer rooted.Close()
	name := infoHash + torrentSourceSuffix
	var previousSize int64
	if info, statErr := rooted.Lstat(name); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("existing torrent source is not a regular file")
		}
		previousSize = info.Size()
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect existing torrent source: %w", statErr)
	}
	projectedSize := s.torrentSourceBytes - previousSize + int64(len(data))
	if previousSize > s.torrentSourceBytes || projectedSize > torrentSourceStoreMaxBytes {
		// A full scan is intentionally reserved for startup, explicit cleanup,
		// and quota pressure. Normal admissions stay O(1) even with thousands
		// of retained private-torrent sources.
		total, pruneErr := s.pruneTorrentSourcesLocked(infoHash)
		if pruneErr != nil {
			return pruneErr
		}
		projectedSize = total - previousSize + int64(len(data))
	}
	if projectedSize > torrentSourceStoreMaxBytes {
		return fmt.Errorf("torrent source store exceeds %d bytes", torrentSourceStoreMaxBytes)
	}

	if err := atomicWriteTorrentSource(rooted, name, data); err != nil {
		return fmt.Errorf("persist torrent source: %w", err)
	}
	s.torrentSourceBytes = projectedSize
	return nil
}

// LoadTorrentSource returns only a regular, bounded torrent file whose content
// hashes to the requested key. Callers may fall back to a magnet only when the
// source is genuinely absent; all other failures indicate unsafe state.
func (s *Storage) LoadTorrentSource(infoHash string) ([]byte, error) {
	infoHash, err := normalizeTorrentSourceHash(infoHash)
	if err != nil {
		return nil, err
	}
	s.torrentSourcesMu.Lock()
	defer s.torrentSourcesMu.Unlock()

	rooted, err := s.openTorrentSourceRoot(false)
	if err != nil {
		return nil, err
	}
	defer rooted.Close()
	name := infoHash + torrentSourceSuffix
	info, err := rooted.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect torrent source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("torrent source is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > utils.MaxMetadataFileBytes {
		return nil, fmt.Errorf("torrent source size is outside the allowed range")
	}

	file, err := rooted.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open torrent source: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened torrent source: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("torrent source changed while opening")
	}
	data, err := utils.ReadAllLimited(file, utils.MaxMetadataFileBytes)
	if err != nil {
		return nil, fmt.Errorf("read torrent source: %w", err)
	}
	parsed, err := utils.GetMagnetFromBytes(data, false)
	if err != nil {
		return nil, fmt.Errorf("validate stored torrent source: %w", err)
	}
	if parsed.InfoHash != infoHash {
		return nil, fmt.Errorf("stored torrent source infohash mismatch")
	}
	return data, nil
}

// PruneTorrentSources removes sources no longer referenced by either the main
// library or the active queue. It is opportunistic by design: queue and library
// deletion remain independent, while startup and future admissions reclaim
// orphaned files safely.
func (s *Storage) PruneTorrentSources() error {
	s.torrentSourcesMu.Lock()
	defer s.torrentSourcesMu.Unlock()
	total, err := s.pruneTorrentSourcesLocked("")
	if err == nil {
		s.torrentSourceBytes = total
	}
	return err
}

func (s *Storage) pruneTorrentSourcesLocked(keepInfoHash string) (int64, error) {
	rooted, err := s.openTorrentSourceRoot(false)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer rooted.Close()

	directory, err := rooted.Open(".")
	if err != nil {
		return 0, fmt.Errorf("list torrent sources: %w", err)
	}
	entries, readErr := directory.ReadDir(torrentSourceMaxFiles + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return 0, fmt.Errorf("list torrent sources: %w", errors.Join(readErr, closeErr))
	}
	if closeErr != nil {
		return 0, fmt.Errorf("close torrent source directory: %w", closeErr)
	}
	if len(entries) > torrentSourceMaxFiles {
		return 0, fmt.Errorf("torrent source store exceeds %d directory entries", torrentSourceMaxFiles)
	}

	var total int64
	changed := false
	for _, entry := range entries {
		name := entry.Name()
		info, infoErr := rooted.Lstat(name)
		if infoErr != nil {
			return 0, fmt.Errorf("inspect torrent source entry: %w", infoErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return 0, fmt.Errorf("torrent source entry is a symlink")
		}
		if isTorrentSourceTempName(name) {
			if removeErr := rooted.Remove(name); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return 0, fmt.Errorf("remove incomplete torrent source: %w", removeErr)
			}
			changed = true
			continue
		}

		hashName := strings.TrimSuffix(name, torrentSourceSuffix)
		validSourceName := strings.HasSuffix(name, torrentSourceSuffix)
		if _, hashErr := normalizeTorrentSourceHash(hashName); hashErr != nil {
			validSourceName = false
		}
		if validSourceName && hashName != keepInfoHash &&
			!s.entries.Exists(hashName) && !s.queue.Exists(hashName) {
			if removeErr := rooted.Remove(name); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				return 0, fmt.Errorf("remove orphaned torrent source: %w", removeErr)
			}
			changed = true
			continue
		}
		if info.Mode().IsRegular() {
			if info.Size() > torrentSourceStoreMaxBytes-total {
				return 0, fmt.Errorf("torrent source store exceeds %d bytes", torrentSourceStoreMaxBytes)
			}
			total += info.Size()
		}
	}
	if changed {
		if err := syncTorrentSourceDirectory(rooted); err != nil {
			return 0, fmt.Errorf("sync pruned torrent sources: %w", err)
		}
	}
	return total, nil
}

func isTorrentSourceTempName(name string) bool {
	if !strings.HasPrefix(name, ".") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(name, "."), torrentSourceSuffix+".tmp-")
	if len(parts) != 2 || len(parts[1]) != 32 {
		return false
	}
	if _, err := normalizeTorrentSourceHash(parts[0]); err != nil {
		return false
	}
	_, err := hex.DecodeString(parts[1])
	return err == nil
}

func newTorrentSourceTempName(destination string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate torrent source temporary name: %w", err)
	}
	return "." + destination + ".tmp-" + hex.EncodeToString(random[:]), nil
}

func atomicWriteTorrentSource(rooted *os.Root, destination string, data []byte) (err error) {
	tempName, err := newTorrentSourceTempName(destination)
	if err != nil {
		return err
	}
	temp, err := rooted.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, torrentSourceMode)
	if err != nil {
		return err
	}
	defer func() {
		_ = temp.Close()
		if err != nil {
			_ = rooted.Remove(tempName)
		}
	}()

	if err = temp.Chmod(torrentSourceMode); err != nil {
		return err
	}
	if written, writeErr := temp.Write(data); writeErr != nil {
		err = writeErr
		return err
	} else if written != len(data) {
		err = io.ErrShortWrite
		return err
	}
	if err = temp.Sync(); err != nil {
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = replaceTorrentSource(rooted, tempName, destination); err != nil {
		return err
	}
	return syncTorrentSourceDirectory(rooted)
}
