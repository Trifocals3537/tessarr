package vfs

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"unicode/utf16"
)

const cacheFilenameMaxBytes = 255

// Keep ordinary sidecars compatible with existing caches. Only names whose
// .json suffix would exceed the native component limit need an opaque leaf;
// their exact data filename is retained in ItemInfo for scans and eviction.
func cacheMetadataPath(directory, filename string) string {
	leaf := filename + ".json"
	length := len(leaf)
	if runtime.GOOS == "windows" {
		// Preserve existing Unicode sidecars on NTFS, whose limit counts
		// UTF-16 code units rather than UTF-8 bytes.
		length = len(utf16.Encode([]rune(leaf)))
	}
	if length > cacheFilenameMaxBytes {
		leaf = fmt.Sprintf(".tessarr-meta-%x.json", sha256.Sum256([]byte(filename)))
	}
	return filepath.Join(directory, leaf)
}

func validCacheFilename(filename string) bool {
	return filename != "" && filename != "." && filename != ".." &&
		!filepath.IsAbs(filename) && filepath.VolumeName(filename) == "" &&
		filepath.Base(filename) == filename
}

// Publish atomically without extending the final leaf. The former .json.tmp
// suffix could overflow even when both the data and .json names fitted.
func writeCacheMetadata(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".tessarr-meta-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
