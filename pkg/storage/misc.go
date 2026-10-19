package storage

import (
	"fmt"
	"maps"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

// HandleExistingEntryMerge merges an incoming entry with an existing one that
// shares the same infohash. This preserves placements, files, and tags from
// the existing entry that the incoming entry may not know about.
func HandleExistingEntryMerge(existing, incoming *Entry) *Entry {
	if existing != nil && incoming != nil && incoming.MainGeneration == 0 {
		incoming.MainGeneration = existing.MainGeneration
	}
	// If NZB entry, ignore merging - just return incoming
	if incoming.Protocol == config.ProtocolNZB {
		return incoming
	}
	PreserveTorrentOutputPath(existing, incoming)
	incoming.Files = mergeFiles(existing.Files, incoming.Files)
	incoming.ActiveProvider = selectActivePlacement(existing, incoming)
	incoming.Providers = mergeProviders(existing.Providers, incoming.Providers)
	incoming.Tags = mergeTags(existing.Tags, incoming.Tags)

	return incoming
}

// ReconcileCompletedTorrentEntry maps a completed queue snapshot onto the
// canonical identities already published by the main entry. Queue entries are
// intentionally built in isolation, so this must run before their maps are
// merged; otherwise a naming-rule upgrade can union old and new identities and
// expose duplicate or unresolvable files.
func ReconcileCompletedTorrentEntry(existing, incoming *Entry) error {
	if existing == nil || incoming == nil || !existing.IsTorrent() || !incoming.IsTorrent() {
		return nil
	}
	placement := incoming.GetActiveProvider()
	if placement == nil {
		return fmt.Errorf("completed queue entry has no active provider placement")
	}
	remoteFiles := make(map[string]debridTypes.File, len(placement.Files))
	for name, providerFile := range placement.Files {
		if providerFile == nil {
			return fmt.Errorf("completed provider file %q is nil", name)
		}
		canonical := incoming.Files[name]
		if canonical == nil {
			return fmt.Errorf("completed provider file %q has no queue file metadata", name)
		}
		remoteFiles[name] = debridTypes.File{
			Id:         providerFile.Id,
			Name:       name,
			Path:       providerFile.Path,
			OutputPath: canonical.Path,
			Size:       canonical.Size,
			ByteRange:  canonical.ByteRange,
			Deleted:    canonical.Deleted,
			Link:       providerFile.Link,
		}
	}
	remote := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         incoming.InfoHash,
		Name:             incoming.Name,
		OriginalFilename: incoming.OriginalFilename,
		Size:             incoming.Size,
		Bytes:            incoming.Bytes,
		Files:            remoteFiles,
		Status:           placement.Status,
		Progress:         placement.Progress,
		Debrid:           placement.Provider,
	}
	preserveTitle := !existing.CanApplyTorrentTitle(remote)
	if preserveTitle {
		remote.Name = existing.Name
		remote.OriginalFilename = existing.OriginalFilename
	}
	reconciled, err := existing.AddTorrentProvider(remote)
	if err != nil {
		return fmt.Errorf("reconcile completed queue placement: %w", err)
	}
	// AddTorrentProvider rebuilds the file mapping, but the queue placement is
	// authoritative for the transfer lifecycle. Do not replace its timestamps
	// and progress with the new placement's defaults during the main merge.
	reconciled.AddedAt = placement.AddedAt
	reconciled.RemovedAt = placement.RemovedAt
	reconciled.Progress = placement.Progress
	reconciled.DownloadedAt = placement.DownloadedAt
	if preserveTitle {
		incoming.Name = existing.Name
		incoming.OriginalFilename = existing.OriginalFilename
	}
	existing.ActiveProvider = incoming.ActiveProvider
	incoming.Files = existing.Files
	incoming.Providers = existing.Providers
	return nil
}

// mergeProviders merges two placement maps, preferring newer data for same debrid
func mergeProviders(existing, incoming map[string]*ProviderEntry) map[string]*ProviderEntry {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := make(map[string]*ProviderEntry)

	// Copy existing placements
	maps.Copy(merged, existing)

	// Merge incoming placements (overwrites if same key)
	for k, v := range incoming {
		if existingPlacement, exists := merged[k]; exists {
			// Keep placement with more recent UpdatedAt
			if v.AddedAt.After(existingPlacement.AddedAt) {
				merged[k] = v
			}
		} else {
			merged[k] = v
		}
	}

	return merged
}

// mergeFiles merges two file maps, preferring files with newer AddedOn
func mergeFiles(existing, incoming map[string]*File) map[string]*File {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := make(map[string]*File)

	// Copy existing files
	maps.Copy(merged, existing)

	// Merge incoming files
	for k, v := range incoming {
		if existingFile, exists := merged[k]; exists {
			preferred, fallback := existingFile, v
			// Prefer file with newer AddedOn timestamp. On a tie, prefer the
			// record that carries provider path provenance.
			if v.AddedOn.After(existingFile.AddedOn) ||
				(v.AddedOn.Equal(existingFile.AddedOn) && existingFile.Path == "" && v.Path != "") {
				preferred, fallback = v, existingFile
			}
			if preferred.Path == "" && fallback.Path != "" {
				enriched := *preferred
				enriched.Path = fallback.Path
				preferred = &enriched
			}
			merged[k] = preferred
		} else {
			merged[k] = v
		}
	}

	return merged
}

// selectActivePlacement selects the active debrid placement
func selectActivePlacement(existing, incoming *Entry) string {
	// Prefer incoming if it has an active placement
	if incoming.ActiveProvider != "" {
		return incoming.ActiveProvider
	}
	return existing.ActiveProvider
}

// mergeTags merges two tag slices, removing duplicates
func mergeTags(existing, incoming []string) []string {
	if len(existing) == 0 {
		return incoming
	}
	if len(incoming) == 0 {
		return existing
	}

	tagSet := make(map[string]bool)
	for _, tag := range existing {
		tagSet[tag] = true
	}
	for _, tag := range incoming {
		tagSet[tag] = true
	}

	merged := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		merged = append(merged, tag)
	}
	return merged
}
