package usenet

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

const (
	DefaultWatchedNZBMaxFileBytes  int64 = 64 << 20
	DefaultWatchedNZBMaxTotalBytes int64 = 128 << 20
	DefaultWatchedNZBMaxEntries          = 256
	DefaultWatchedNZBMaxFiles            = 16
	watchedNZBClaimReadBatch             = 64
)

var (
	ErrWatchedNZBClaimConflict        = errors.New("watched NZB claim target already exists")
	ErrWatchedNZBClaimLinkUnavailable = errors.New(
		"watched NZB claiming requires hard-link support",
	)
)

type ClaimNewNZBLimits struct {
	MaxEntries    int
	MaxFiles      int
	MaxFileBytes  int64
	MaxTotalBytes int64
}

type ClaimNewNZBResult struct {
	Pending   []PendingNZB
	Scanned   int
	Matched   int
	Attempted int
	Failed    int
	BytesRead int64
	More      bool
}

type NZBClaimScanner struct {
	mu sync.Mutex

	absoluteRoot string
	rooted       *os.Root
	directory    *os.File
	pending      []os.DirEntry
	reachedEOF   bool
	closed       bool
}

func DefaultClaimNewNZBLimits() ClaimNewNZBLimits {
	return ClaimNewNZBLimits{
		MaxEntries:    DefaultWatchedNZBMaxEntries,
		MaxFiles:      DefaultWatchedNZBMaxFiles,
		MaxFileBytes:  DefaultWatchedNZBMaxFileBytes,
		MaxTotalBytes: DefaultWatchedNZBMaxTotalBytes,
	}
}

func NewNZBClaimScanner(metadataRoot string) (*NZBClaimScanner, error) {
	absoluteRoot, err := filepath.Abs(metadataRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve watched NZB metadata root: %w", err)
	}
	absoluteRoot = filepath.Clean(absoluteRoot)
	if _, _, err := metadataDirectLeaf(
		absoluteRoot,
		filepath.Join(absoluteRoot, ".probe"),
	); err != nil {
		return nil, err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("open watched NZB metadata root: %w", err)
	}
	return &NZBClaimScanner{
		absoluteRoot: absoluteRoot,
		rooted:       rooted,
		pending:      make([]os.DirEntry, 0, watchedNZBClaimReadBatch),
	}, nil
}

func (scanner *NZBClaimScanner) Close() error {
	if scanner == nil {
		return nil
	}
	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	if scanner.closed {
		return nil
	}
	scanner.closed = true
	var errs []error
	if scanner.directory != nil {
		errs = append(errs, scanner.directory.Close())
		scanner.directory = nil
	}
	if scanner.rooted != nil {
		errs = append(errs, scanner.rooted.Close())
		scanner.rooted = nil
	}
	scanner.pending = nil
	return errors.Join(errs...)
}

func (scanner *NZBClaimScanner) Scan(
	limits ClaimNewNZBLimits,
) (ClaimNewNZBResult, error) {
	var result ClaimNewNZBResult
	if scanner == nil {
		return result, fmt.Errorf("watched NZB claim scanner is nil")
	}
	if err := validateClaimNewNZBLimits(limits); err != nil {
		return result, err
	}

	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	if scanner.closed || scanner.rooted == nil {
		return result, fmt.Errorf("watched NZB claim scanner is closed")
	}
	if scanner.directory == nil {
		directory, err := scanner.rooted.Open(".")
		if err != nil {
			return result, fmt.Errorf("open watched NZB claim directory: %w", err)
		}
		scanner.directory = directory
		scanner.reachedEOF = false
	}

	var errs []error
	seenPaths := make(map[string]struct{})
	for {
		for len(scanner.pending) > 0 {
			entry := scanner.pending[0]
			name := entry.Name()
			kind, claimedName := classifyWatchedNZBClaimCandidate(name)
			if kind == watchedNZBClaimIgnore {
				scanner.pending = scanner.pending[1:]
				continue
			}
			if result.Attempted >= limits.MaxFiles {
				result.More = true
				return result, errors.Join(errs...)
			}
			result.Matched++

			info, err := scanner.rooted.Lstat(name)
			if err != nil {
				scanner.pending = scanner.pending[1:]
				result.Attempted++
				if os.IsNotExist(err) {
					continue
				}
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf("inspect watched NZB candidate %q: %w", name, err))
				continue
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				scanner.pending = scanner.pending[1:]
				result.Attempted++
				continue
			}
			if info.Size() < 0 || info.Size() > limits.MaxFileBytes {
				scanner.pending = scanner.pending[1:]
				result.Attempted++
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf(
					"watched NZB candidate %q size %d exceeds limit %d",
					name,
					info.Size(),
					limits.MaxFileBytes,
				))
				continue
			}
			if info.Size() > limits.MaxTotalBytes-result.BytesRead {
				result.More = true
				return result, errors.Join(errs...)
			}

			scanner.pending = scanner.pending[1:]
			result.Attempted++
			claimedLeaf := name
			if kind == watchedNZBClaimRaw {
				skip, markerErr := hasWatchedNZBManagedMarker(scanner.rooted, name)
				if markerErr != nil {
					result.Failed++
					result.More = true
					errs = append(errs, markerErr)
					continue
				}
				if skip {
					continue
				}
				if err := syncWatchedNZBLeaf(
					scanner.rooted,
					name,
					limits.MaxFileBytes,
				); err != nil {
					result.Failed++
					result.More = true
					errs = append(errs, fmt.Errorf(
						"sync watched NZB candidate %q before claim: %w",
						name,
						err,
					))
					continue
				}
				claimedLeaf = name + ".importing"
				if err := claimWatchedNZBInRoot(
					scanner.rooted,
					name,
					claimedLeaf,
					info,
					syncWatchedNZBRoot,
				); err != nil {
					result.Failed++
					result.More = true
					errs = append(errs, err)
					continue
				}
			} else if err := syncWatchedNZBLeaf(
				scanner.rooted,
				name,
				limits.MaxFileBytes,
			); err != nil {
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf(
					"sync recovered watched NZB claim %q: %w",
					name,
					err,
				))
				continue
			}

			claimedPath := filepath.Join(scanner.absoluteRoot, claimedLeaf)
			snapshot, err := ReadClaimedNZBSnapshotAt(
				scanner.absoluteRoot,
				claimedPath,
				limits.MaxFileBytes,
			)
			if err != nil {
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf("snapshot claimed watched NZB %q: %w", claimedLeaf, err))
				continue
			}
			if snapshot.Size > limits.MaxTotalBytes-result.BytesRead {
				result.More = true
				continue
			}
			if _, duplicate := seenPaths[snapshot.Path]; duplicate {
				continue
			}
			seenPaths[snapshot.Path] = struct{}{}
			result.BytesRead += snapshot.Size
			result.Pending = append(result.Pending, PendingNZB{
				Name:          claimedName,
				Path:          snapshot.Path,
				Content:       snapshot.Content,
				ContentDigest: snapshot.ContentDigest,
				Size:          snapshot.Size,
				ModTime:       snapshot.ModTime,
			})
		}

		if scanner.reachedEOF {
			if err := scanner.directory.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close watched NZB claim directory: %w", err))
			}
			scanner.directory = nil
			scanner.reachedEOF = false
			result.More = result.More || result.Failed > 0
			return result, errors.Join(errs...)
		}
		if result.Scanned >= limits.MaxEntries {
			result.More = true
			return result, errors.Join(errs...)
		}

		batchSize := min(watchedNZBClaimReadBatch, limits.MaxEntries-result.Scanned)
		entries, readErr := scanner.directory.ReadDir(batchSize)
		result.Scanned += len(entries)
		scanner.pending = append(scanner.pending, entries...)
		if errors.Is(readErr, io.EOF) {
			scanner.reachedEOF = true
		} else if readErr != nil {
			return result, errors.Join(
				errors.Join(errs...),
				fmt.Errorf("scan watched NZB claims: %w", readErr),
			)
		}
		if len(entries) == 0 && readErr == nil {
			return result, errors.Join(
				errors.Join(errs...),
				fmt.Errorf("scan watched NZB claims made no progress"),
			)
		}
	}
}

type watchedNZBClaimKind uint8

const (
	watchedNZBClaimIgnore watchedNZBClaimKind = iota
	watchedNZBClaimRaw
	watchedNZBClaimImporting
)

func classifyWatchedNZBClaimCandidate(name string) (watchedNZBClaimKind, string) {
	if strings.HasSuffix(name, watchedNZBImportingSuffix) {
		claimedName := strings.TrimSuffix(name, ".importing")
		if validateWatchedNZBLeafName(claimedName) == nil &&
			!isCanonicalManagedNZBLeaf(claimedName) {
			return watchedNZBClaimImporting, claimedName
		}
		return watchedNZBClaimIgnore, ""
	}
	if !strings.HasSuffix(name, ".nzb") || validateWatchedNZBLeafName(name) != nil {
		return watchedNZBClaimIgnore, ""
	}
	// ParseWithID stores raw source as <canonical UUID>.nzb. It is managed
	// internal state, never user intake, even if a crash removed its marker.
	if isCanonicalManagedNZBLeaf(name) {
		return watchedNZBClaimIgnore, ""
	}
	return watchedNZBClaimRaw, name
}

func isCanonicalManagedNZBLeaf(name string) bool {
	id := strings.TrimSuffix(name, ".nzb")
	canonical, err := canonicalNZBID(id)
	return err == nil && canonical == id
}

func validateWatchedNZBLeafName(name string) error {
	if err := safepath.ValidateIdentifier(name); err != nil {
		return err
	}
	if !strings.HasSuffix(name, ".nzb") {
		return fmt.Errorf("watched NZB leaf %q does not end in .nzb", name)
	}
	return nil
}

func hasWatchedNZBManagedMarker(rooted *os.Root, rawLeaf string) (bool, error) {
	for _, suffix := range []string{".processed", ".processing", ".failed"} {
		marker := rawLeaf + suffix
		info, err := rooted.Lstat(marker)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, fmt.Errorf("inspect managed marker %q: %w", marker, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return false, fmt.Errorf("managed marker %q is not a regular no-follow file", marker)
		}
		return true, nil
	}
	return false, nil
}

func claimWatchedNZBInRoot(
	rooted *os.Root,
	rawLeaf, importingLeaf string,
	originalInfo os.FileInfo,
	syncDirectory watchedNZBDirSyncFunc,
) error {
	if syncDirectory == nil {
		return fmt.Errorf("watched NZB claim directory-sync operation is nil")
	}
	if _, err := rooted.Lstat(importingLeaf); err == nil {
		return fmt.Errorf(
			"%w: %q cannot replace %q",
			ErrWatchedNZBClaimConflict,
			rawLeaf,
			importingLeaf,
		)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect watched NZB claim target %q: %w", importingLeaf, err)
	}
	linkErr := rooted.Link(rawLeaf, importingLeaf)
	if linkErr != nil {
		if os.IsExist(linkErr) {
			return fmt.Errorf(
				"%w: %q cannot replace %q",
				ErrWatchedNZBClaimConflict,
				rawLeaf,
				importingLeaf,
			)
		}
		// A normal rename can overwrite an importing name created after the
		// check above. Hard links give this claim the required atomic,
		// no-replace behavior on every supported platform, so fail closed when
		// the watched filesystem cannot provide them.
		return errors.Join(
			ErrWatchedNZBClaimLinkUnavailable,
			fmt.Errorf(
				"hard-link %q as %q: %w",
				rawLeaf,
				importingLeaf,
				linkErr,
			),
		)
	}
	claimedInfo, err := rooted.Lstat(importingLeaf)
	if err != nil {
		return fmt.Errorf("inspect claimed watched NZB %q: %w", importingLeaf, err)
	}
	if claimedInfo.Mode()&os.ModeSymlink != 0 ||
		!claimedInfo.Mode().IsRegular() ||
		!os.SameFile(originalInfo, claimedInfo) {
		return fmt.Errorf(
			"%w: claimed path %q does not identify original source",
			ErrWatchedNZBClaimChanged,
			importingLeaf,
		)
	}
	if err := syncDirectory(rooted); err != nil {
		return fmt.Errorf(
			"sync watched NZB claim %q before removing %q: %w",
			importingLeaf,
			rawLeaf,
			err,
		)
	}
	if err := rooted.Remove(rawLeaf); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf(
			"remove raw watched NZB name %q after claim: %w",
			rawLeaf,
			err,
		)
	}
	if err := syncDirectory(rooted); err != nil {
		return fmt.Errorf(
			"sync watched NZB claim after removing %q: %w",
			rawLeaf,
			err,
		)
	}
	return nil
}

func validateClaimNewNZBLimits(limits ClaimNewNZBLimits) error {
	if limits.MaxEntries <= 0 {
		return fmt.Errorf("watched NZB claim MaxEntries must be positive")
	}
	if limits.MaxFiles <= 0 {
		return fmt.Errorf("watched NZB claim MaxFiles must be positive")
	}
	if limits.MaxFiles > limits.MaxEntries {
		return fmt.Errorf(
			"watched NZB claim MaxFiles %d exceeds MaxEntries %d",
			limits.MaxFiles,
			limits.MaxEntries,
		)
	}
	if err := validateWatchedNZBMaxBytes(limits.MaxFileBytes); err != nil {
		return err
	}
	if limits.MaxTotalBytes < limits.MaxFileBytes {
		return fmt.Errorf(
			"watched NZB claim MaxTotalBytes %d is below MaxFileBytes %d",
			limits.MaxTotalBytes,
			limits.MaxFileBytes,
		)
	}
	return nil
}
