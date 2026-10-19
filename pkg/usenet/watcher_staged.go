package usenet

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
)

var (
	// ErrWatchedNZBStagedConflict means durable staged state exists for the
	// deterministic ID but cannot be proven to contain the submitted bytes.
	ErrWatchedNZBStagedConflict = errors.New("watched NZB staged state conflicts with submitted content")

	// ErrWatchedNZBStageLinkUnavailable means the metadata filesystem cannot
	// perform the no-overwrite hard-link commit used for crash-safe staging.
	ErrWatchedNZBStageLinkUnavailable = errors.New("watched NZB staging requires hard-link support")
)

func stageNZBAt(metadataRoot, id string, content []byte) (string, error) {
	if len(content) == 0 {
		return "", fmt.Errorf("staged NZB content is empty")
	}
	maxBytes := int64(len(content))
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return "", err
	}
	expectedDigest := sha256.Sum256(content)
	stagedPath, err := metadataFilePath(metadataRoot, id, nzbStagedSuffix)
	if err != nil {
		return "", err
	}
	tempPath, err := metadataFilePath(metadataRoot, id, nzbStagedTempSuffix)
	if err != nil {
		return "", err
	}
	absoluteRoot, stagedLeaf, err := metadataDirectLeaf(metadataRoot, stagedPath)
	if err != nil {
		return "", err
	}
	_, tempLeaf, err := metadataDirectLeaf(metadataRoot, tempPath)
	if err != nil {
		return "", err
	}

	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("open staged NZB metadata root: %w", err)
	}
	defer rooted.Close()

	stagedExists, err := matchingStagedNZBLeaf(
		rooted,
		stagedLeaf,
		expectedDigest,
		maxBytes,
	)
	if err != nil {
		return stagedPath, err
	}
	if stagedExists {
		if err := syncWatchedNZBLeaf(rooted, stagedLeaf, maxBytes); err != nil {
			return stagedPath, fmt.Errorf("sync recovered staged NZB %q: %w", stagedLeaf, err)
		}
		if err := syncWatchedNZBRoot(rooted); err != nil {
			return stagedPath, fmt.Errorf("sync recovered staged NZB directory: %w", err)
		}
		if err := cleanupMatchingStagedTemp(
			rooted,
			tempLeaf,
			expectedDigest,
			maxBytes,
		); err != nil {
			return stagedPath, err
		}
		return stagedPath, nil
	}

	tempExists, err := matchingStagedNZBLeaf(
		rooted,
		tempLeaf,
		expectedDigest,
		maxBytes,
	)
	if err != nil {
		return stagedPath, err
	}
	if !tempExists {
		file, openErr := rooted.OpenFile(
			tempLeaf,
			os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			0o600,
		)
		if openErr != nil {
			return stagedPath, fmt.Errorf("create staged NZB temporary file: %w", openErr)
		}
		writeErr := writeFull(file, content)
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			_ = rooted.Remove(tempLeaf)
			_ = syncWatchedNZBRoot(rooted)
			return stagedPath, fmt.Errorf("persist staged NZB temporary file: %w", err)
		}
	}
	if err := syncWatchedNZBLeaf(rooted, tempLeaf, maxBytes); err != nil {
		return stagedPath, fmt.Errorf("sync staged NZB temporary file %q: %w", tempLeaf, err)
	}

	if err := rooted.Link(tempLeaf, stagedLeaf); err != nil {
		if os.IsExist(err) {
			matches, matchErr := matchingStagedNZBLeaf(
				rooted,
				stagedLeaf,
				expectedDigest,
				maxBytes,
			)
			if matchErr != nil {
				return stagedPath, matchErr
			}
			if !matches {
				return stagedPath, fmt.Errorf(
					"%w: staged target %q appeared with different content",
					ErrWatchedNZBStagedConflict,
					stagedLeaf,
				)
			}
		} else {
			return stagedPath, errors.Join(
				ErrWatchedNZBStageLinkUnavailable,
				fmt.Errorf("commit staged NZB %q: %w", stagedLeaf, err),
			)
		}
	}
	if matches, err := matchingStagedNZBLeaf(
		rooted,
		stagedLeaf,
		expectedDigest,
		maxBytes,
	); err != nil {
		return stagedPath, err
	} else if !matches {
		return stagedPath, fmt.Errorf(
			"%w: committed staged target %q disappeared",
			ErrWatchedNZBStagedConflict,
			stagedLeaf,
		)
	}
	if err := syncWatchedNZBRoot(rooted); err != nil {
		return stagedPath, fmt.Errorf("sync committed staged NZB %q: %w", stagedLeaf, err)
	}
	if err := rooted.Remove(tempLeaf); err != nil && !os.IsNotExist(err) {
		return stagedPath, fmt.Errorf("remove staged NZB temporary file %q: %w", tempLeaf, err)
	}
	if err := syncWatchedNZBRoot(rooted); err != nil {
		return stagedPath, fmt.Errorf("sync staged NZB temporary cleanup %q: %w", tempLeaf, err)
	}
	return stagedPath, nil
}

func ReadStagedNZBAt(
	metadataRoot, id, persistedPath string,
	maxBytes int64,
) ([]byte, error) {
	path, err := validatePersistedMetadataPath(
		metadataRoot,
		id,
		persistedPath,
		nzbStagedSuffix,
	)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("staged NZB path is empty")
	}
	return readStableMetadataContent(metadataRoot, path, maxBytes)
}

func ReadCanonicalStagedNZBAt(
	metadataRoot, id string,
	maxBytes int64,
) ([]byte, error) {
	path, err := metadataFilePath(metadataRoot, id, nzbStagedSuffix)
	if err != nil {
		return nil, err
	}
	return ReadStagedNZBAt(metadataRoot, id, path, maxBytes)
}

func ReadNZBSourceAt(
	metadataRoot, id, persistedPath string,
	maxBytes int64,
) ([]byte, error) {
	path, err := validatePersistedMetadataPath(
		metadataRoot,
		id,
		persistedPath,
		nzbSourceSuffix,
	)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("NZB source path is empty")
	}
	return readStableMetadataContent(metadataRoot, path, maxBytes)
}

func readStableMetadataContent(root, path string, maxBytes int64) ([]byte, error) {
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return nil, err
	}
	absoluteRoot, leaf, err := metadataDirectLeaf(root, path)
	if err != nil {
		return nil, err
	}
	file, err := openMetadataFile(absoluteRoot, path)
	if err != nil {
		return nil, err
	}
	beforeInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect stable metadata file %q: %w", leaf, err)
	}
	if err := validateWatchedNZBFileInfo(leaf, beforeInfo, maxBytes); err != nil {
		_ = file.Close()
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	afterInfo, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read stable metadata file %q: %w", leaf, readErr)
	}
	if statErr != nil {
		return nil, fmt.Errorf("reinspect stable metadata file %q: %w", leaf, statErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close stable metadata file %q: %w", leaf, closeErr)
	}
	if int64(len(content)) > maxBytes {
		return nil, fmt.Errorf("metadata file %q exceeds byte limit %d", leaf, maxBytes)
	}
	pathInfo, err := statMetadataFile(absoluteRoot, path)
	if err != nil {
		return nil, fmt.Errorf("reinspect stable metadata path %q: %w", leaf, err)
	}
	if !os.SameFile(beforeInfo, afterInfo) ||
		!os.SameFile(beforeInfo, pathInfo) ||
		beforeInfo.Size() != afterInfo.Size() ||
		beforeInfo.Size() != pathInfo.Size() ||
		beforeInfo.Size() != int64(len(content)) ||
		!beforeInfo.ModTime().Equal(afterInfo.ModTime()) ||
		!beforeInfo.ModTime().Equal(pathInfo.ModTime()) {
		return nil, fmt.Errorf("metadata file %q changed while reading", leaf)
	}
	return content, nil
}

func matchingStagedNZBLeaf(
	rooted *os.Root,
	leaf string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
) (bool, error) {
	info, err := rooted.Lstat(leaf)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect staged NZB %q: %w", leaf, err)
	}
	if err := validateWatchedNZBFileInfo(leaf, info, maxBytes); err != nil {
		return false, fmt.Errorf("%w: %v", ErrWatchedNZBStagedConflict, err)
	}
	digest, err := digestWatchedNZBLeaf(rooted, leaf, info, maxBytes)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrWatchedNZBStagedConflict, err)
	}
	if digest != expectedDigest {
		return false, fmt.Errorf(
			"%w: staged file %q has different content",
			ErrWatchedNZBStagedConflict,
			leaf,
		)
	}
	return true, nil
}

func cleanupMatchingStagedTemp(
	rooted *os.Root,
	tempLeaf string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
) error {
	exists, err := matchingStagedNZBLeaf(rooted, tempLeaf, expectedDigest, maxBytes)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := rooted.Remove(tempLeaf); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove recovered staged NZB temporary file %q: %w", tempLeaf, err)
	}
	if err := syncWatchedNZBRoot(rooted); err != nil {
		return fmt.Errorf("sync recovered staged NZB temporary cleanup %q: %w", tempLeaf, err)
	}
	return nil
}

func writeFull(file *os.File, content []byte) error {
	for len(content) > 0 {
		written, err := file.Write(content)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(content) {
			return io.ErrShortWrite
		}
		content = content[written:]
	}
	return nil
}

func syncWatchedNZBLeaf(rooted *os.Root, leaf string, maxBytes int64) error {
	expectedInfo, err := rooted.Lstat(leaf)
	if err != nil {
		return fmt.Errorf("inspect watched NZB %q for sync: %w", leaf, err)
	}
	if err := validateWatchedNZBFileInfo(leaf, expectedInfo, maxBytes); err != nil {
		return err
	}
	flags := os.O_RDONLY
	if runtime.GOOS == "windows" {
		// FlushFileBuffers requires a handle opened with write access.
		flags = os.O_RDWR
	}
	file, err := rooted.OpenFile(leaf, flags, 0)
	if err != nil {
		return fmt.Errorf("open watched NZB %q for sync: %w", leaf, err)
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return fmt.Errorf("inspect opened watched NZB %q for sync: %w", leaf, statErr)
	}
	if !os.SameFile(expectedInfo, openedInfo) {
		_ = file.Close()
		return fmt.Errorf("watched NZB %q changed while opening for sync", leaf)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(syncErr, closeErr)
}
