package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

const (
	privateFileMode  = 0600
	privateDirMode   = 0700
	maxConfigBackups = 10
)

var persistenceMu sync.Mutex
var backupSequence atomic.Uint64

// persistConfig writes config.json without exposing a partially-written file.
// Before replacing an existing configuration, it stores the last known
// on-disk version in a small, timestamped backup set.
func persistConfig(path string, data []byte) error {
	if len(data) > maxConfigurationFileBytes {
		return fmt.Errorf("configuration exceeds %d-byte persistence limit", maxConfigurationFileBytes)
	}
	persistenceMu.Lock()
	defer persistenceMu.Unlock()

	current, err := safepath.ReadRegularFile(path, maxConfigurationFileBytes)
	switch {
	case err == nil && bytes.Equal(current, data):
		if err := safepath.ChmodRegularFile(path, privateFileMode); err != nil {
			return fmt.Errorf("secure config permissions: %w", err)
		}
		return nil
	case err == nil:
		if err := writeConfigBackup(path, current); err != nil {
			return fmt.Errorf("back up existing config: %w", err)
		}
	case errors.Is(err, os.ErrNotExist):
		// First write; there is nothing to back up.
	default:
		return fmt.Errorf("read existing config: %w", err)
	}

	if err := atomicWriteFile(path, data, privateFileMode); err != nil {
		return fmt.Errorf("atomically replace config: %w", err)
	}
	return nil
}

// persistAuth applies the same crash-safe replacement as config.json. Auth
// changes are intentionally not copied into the configuration backup set.
func persistAuth(path string, data []byte) error {
	if len(data) > maxConfigurationFileBytes {
		return fmt.Errorf("authentication configuration exceeds %d-byte persistence limit", maxConfigurationFileBytes)
	}
	persistenceMu.Lock()
	defer persistenceMu.Unlock()

	if err := atomicWriteFile(path, data, privateFileMode); err != nil {
		return fmt.Errorf("atomically replace auth config: %w", err)
	}
	return nil
}

func writeConfigBackup(configPath string, data []byte) (returnErr error) {
	backupDir := filepath.Join(filepath.Dir(configPath), "backups")
	backupRoot, _, err := safepath.EnsureOpenRoot(backupDir, privateDirMode)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, backupRoot.Close()) }()
	if err := backupRoot.Chmod(".", privateDirMode); err != nil {
		return err
	}

	name := fmt.Sprintf(
		"config-%s-%06d.json",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		backupSequence.Add(1),
	)
	if err := safepath.AtomicWriteRootFile(backupRoot, name, data, privateFileMode); err != nil {
		return err
	}

	return pruneConfigBackups(backupRoot)
}

func pruneConfigBackups(backupRoot *os.Root) error {
	directory, err := backupRoot.Open(".")
	if err != nil {
		return err
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "config-") && strings.HasSuffix(name, ".json") {
			names = append(names, name)
		}
	}

	slices.Sort(names)
	if len(names) <= maxConfigBackups {
		return nil
	}

	for _, name := range names[:len(names)-maxConfigBackups] {
		if err := safepath.RemoveRootRegularFile(backupRoot, name); err != nil {
			return err
		}
	}
	return safepath.SyncRoot(backupRoot)
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	if _, err := safepath.EnsureRoot(filepath.Dir(path), privateDirMode); err != nil {
		return err
	}
	return safepath.AtomicWriteFile(path, data, mode)
}
