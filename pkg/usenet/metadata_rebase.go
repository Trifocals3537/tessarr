package usenet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const maxNZBMetadataRebaseFiles = 1_000_000

// RebaseNZBMetadataPaths rewrites source-NZB paths in an offline migration
// staging tree. It leaves external paths untouched and rejects a contained
// path unless it is the exact source file belonging to that NZB ID.
func RebaseNZBMetadataPaths(metaDir, sourceRoot, targetRoot string) (int, error) {
	changed := 0
	sourceNZBRoot := filepath.Join(sourceRoot, "usenet", "nzbs")
	targetNZBRoot := filepath.Join(targetRoot, "usenet", "nzbs")
	// Finish enumeration before replacing any directory entries. Renames during
	// a batched ReadDir walk can skip or revisit records on some filesystems.
	// Only retain bounded IDs, not decoded metadata or article mappings.
	ids := make([]string, 0)
	err := scanMetadataDirectory(metaDir, metaReadBatchSize, func(entry os.DirEntry) error {
		if entry.IsDir() || filepath.Ext(entry.Name()) != metaFileExtension {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("NZB metadata %q is a symlink", entry.Name())
		}
		id := strings.TrimSuffix(entry.Name(), metaFileExtension)
		if _, err := metadataFilePath(metaDir, id, nzbMetaSuffix); err != nil {
			return err
		}
		if len(ids) >= maxNZBMetadataRebaseFiles {
			return fmt.Errorf("NZB metadata migration exceeds %d records", maxNZBMetadataRebaseFiles)
		}
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		rebased, err := rebaseNZBMetadataPath(metaDir, id, sourceRoot, sourceNZBRoot, targetNZBRoot)
		if err != nil {
			return changed, err
		}
		if rebased {
			changed++
		}
	}
	return changed, nil
}

func rebaseNZBMetadataPath(metaDir, id, sourceRoot, sourceNZBRoot, targetNZBRoot string) (bool, error) {
	path, err := metadataFilePath(metaDir, id, nzbMetaSuffix)
	if err != nil {
		return false, err
	}
	info, err := statMetadataFile(metaDir, path)
	if err != nil {
		return false, fmt.Errorf("inspect NZB metadata %q: %w", id, err)
	}
	data, err := readMetadataFile(metaDir, path)
	if err != nil {
		return false, fmt.Errorf("read NZB metadata %q: %w", id, err)
	}
	nzb, err := decodeNZB(data)
	if err != nil {
		return false, fmt.Errorf("decode NZB metadata %q: %w", id, err)
	}
	if err := validateStoredNZBIdentity(id, nzb); err != nil {
		return false, err
	}
	if nzb.Path == "" || !filepath.IsAbs(nzb.Path) {
		return false, nil
	}
	relative, err := filepath.Rel(sourceRoot, filepath.Clean(nzb.Path))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false, nil
	}
	if _, err := validatePersistedMetadataPath(sourceNZBRoot, id, nzb.Path, nzbSourceSuffix); err != nil {
		return false, fmt.Errorf("unsafe stored source path for NZB %q: %w", id, err)
	}
	nzb.Path = filepath.Join(targetNZBRoot, id+string(nzbSourceSuffix))
	encoded, err := encodeNZBV2(nzb)
	if err != nil {
		return false, fmt.Errorf("encode NZB metadata %q: %w", id, err)
	}
	temporary, err := metadataFilePath(metaDir, id, nzbMetaV2TempSuffix)
	if err != nil {
		return false, err
	}
	if err := writeMetadataFilePreservingMode(metaDir, temporary, encoded, info.Mode().Perm()); err != nil {
		return false, fmt.Errorf("write staged NZB metadata %q: %w", id, err)
	}
	if err := renameMetadataFile(metaDir, temporary, path); err != nil {
		return false, fmt.Errorf("replace staged NZB metadata %q: %w", id, err)
	}
	return true, nil
}
