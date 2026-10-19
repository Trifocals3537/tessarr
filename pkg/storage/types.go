package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

type (
	SwitcherStatus string
	TorrentState   string
)

const (
	SwitcherStatusPending    SwitcherStatus = "pending"
	SwitcherStatusInProgress SwitcherStatus = "in_progress"
	SwitcherStatusCompleted  SwitcherStatus = "completed"
	SwitcherStatusFailed     SwitcherStatus = "failed"
	SwitcherStatusCancelled  SwitcherStatus = "cancelled"

	EntryStateDownloading TorrentState = "downloading"
	EntryStatePausedDL    TorrentState = "pausedDL"
	EntryStatePausedUP    TorrentState = "pausedUP"
	EntryStateError       TorrentState = "error"
	EntryStateStalledDL   TorrentState = "stalledDL"
)

// Common errors
var (
	ErrPlacementNotFound     = fmt.Errorf("providerEntry not found")
	ErrAlreadyOnDebrid       = fmt.Errorf("torrent already on this debrid")
	ErrPlacementNotCompleted = fmt.Errorf("providerEntry not completed")
	ErrNoActivePlacement     = fmt.Errorf("no active providerEntry")
)

// Entry is the unified model across debrids and nzbs
type Entry struct {
	// MainGeneration is an in-process optimistic mutation token for the durable
	// main-entry row. It is deliberately not serialized. Every successful main
	// mutation advances the token, invalidating stale read-to-write snapshots.
	MainGeneration uint64 `msgpack:"-" json:"-"`

	// MainProviderSnapshot and MainMutationProvider form a transient
	// authorization for provider rediscovery after explicit deletion. They are
	// populated only after a full provider snapshot observed the key absent.
	MainProviderSnapshot uint64 `msgpack:"-" json:"-"`
	MainMutationProvider string `msgpack:"-" json:"-"`

	// QueueGeneration is an in-process lifecycle token. It is deliberately not
	// serialized: workers cannot survive a restart, and stale workers from an
	// earlier incarnation of the same queue key must never mutate a later one.
	QueueGeneration uint64 `msgpack:"-" json:"-"`

	// QueueIncarnation is a durable, opaque identity assigned when a queue row
	// is first created. It lets a completed queue item explicitly re-import a
	// same-key main entry after deletion without granting that authority to an
	// older worker or to provider refresh.
	QueueIncarnation string `msgpack:"-" json:"-"`

	// MainReimportIncarnation is a transient capability bound only after
	// PrepareQueuedReplacement verifies that QueueIncarnation still owns the
	// current durable queue row. It is never serialized.
	MainReimportIncarnation string `msgpack:"-" json:"-"`

	// OutputName pins the local output component independently of its display
	// title. Empty means legacy name-derived layout; never auto-rename.
	OutputName string `msgpack:"output_name,omitempty" json:"output_name,omitempty"`

	Protocol         config.Protocol `msgpack:"protocol" json:"protocol"`                   // torrent or nzb
	InfoHash         string          `msgpack:"info_hash" json:"info_hash"`                 // Primary key - torrent hash
	Name             string          `msgpack:"name" json:"name"`                           // Entry name
	OriginalFilename string          `msgpack:"original_filename" json:"original_filename"` // Original filename from debrid
	Size             int64           `msgpack:"size" json:"size"`                           // Total size in bytes (for QBit compat)
	Bytes            int64           `msgpack:"bytes" json:"bytes"`                         // Actual bytes (debrid uses this)
	Magnet           string          `msgpack:"magnet,omitempty" json:"magnet,omitempty"`   // Magnet link

	IsDownloading  bool  `msgpack:"is_downloading,omitempty" json:"is_downloading,omitempty"`   // Whether currently downloading(this is for local download)
	SizeDownloaded int64 `msgpack:"size_downloaded,omitempty" json:"size_downloaded,omitempty"` // Actual downloaded bytes

	// Multi-Provider ProviderEntry Strategy
	ActiveProvider string                    `msgpack:"active_provider" json:"active_provider"` // Current active debrid
	Providers      map[string]*ProviderEntry `msgpack:"providers" json:"providers"`             // debrid -> ProviderEntry details

	// Files (from debrid cache)
	Files map[string]*File `msgpack:"files" json:"files"` // filename -> File details

	State TorrentState `msgpack:"state" json:"state"` // This is for QBitTorrent compatibility
	// Provider State (from active providerEntry)
	Status   debridTypes.TorrentStatus `msgpack:"status" json:"status"`     // downloaded, downloading, queued, error
	Progress float64                   `msgpack:"progress" json:"progress"` // Download progress (0-100)
	Speed    int64                     `msgpack:"speed" json:"speed"`       // Download speed
	Seeders  int                       `msgpack:"seeders" json:"seeders"`   // Number of seeders

	IsComplete bool `msgpack:"is_complete" json:"is_complete"` // Ready for use
	Bad        bool `msgpack:"bad" json:"bad"`                 // Marked as bad/corrupted

	// Metadata
	Category       string   `msgpack:"category,omitempty" json:"category,omitempty"`               // Category (e.g., sonarr, radarr)
	Tags           []string `msgpack:"tags,omitempty" json:"tags,omitempty"`                       // User-defined tags
	MountPath      string   `msgpack:"mount_path" json:"mount_path"`                               // Mount path for this torrent
	SavePath       string   `msgpack:"save_path,omitempty" json:"save_path,omitempty"`             // Download/symlink folder
	ContentPath    string   `msgpack:"content_path,omitempty" json:"content_path,omitempty"`       // Final content path
	ClientEndpoint string   `msgpack:"client_endpoint,omitempty" json:"client_endpoint,omitempty"` // qBittorrent Host used by the submitting Arr
	TerminalChecks int      `msgpack:"terminal_checks,omitempty" json:"terminal_checks,omitempty"`
	HandoffReason  string   `msgpack:"handoff_reason,omitempty" json:"handoff_reason,omitempty"`

	// Timestamps
	AddedOn     time.Time  `msgpack:"added_on" json:"added_on"`                             // When first added (from debrid)
	CreatedAt   time.Time  `msgpack:"created_at" json:"created_at"`                         // When created in manager
	UpdatedAt   time.Time  `msgpack:"updated_at" json:"updated_at"`                         // Last update time
	CompletedAt *time.Time `msgpack:"completed_at,omitempty" json:"completed_at,omitempty"` // When completed
	ImportedAt  *time.Time `msgpack:"imported_at,omitempty" json:"imported_at,omitempty"`   // When imported by Arr
	// LastObservedAt records provider visibility while LastProgressAt advances
	// only when the reported transfer progress changes. UpdatedAt cannot serve
	// either purpose because storage writes update it automatically.
	LastObservedAt *time.Time `msgpack:"last_observed_at,omitempty" json:"last_observed_at,omitempty"`
	LastProgressAt *time.Time `msgpack:"last_progress_at,omitempty" json:"last_progress_at,omitempty"`

	// Import Request Data (for processing)
	Action           config.DownloadAction `msgpack:"action,omitempty" json:"action,omitempty"`                       // symlink, download, strm none
	DownloadUncached bool                  `msgpack:"download_uncached,omitempty" json:"download_uncached,omitempty"` // Force uncached download
	CallbackURL      string                `msgpack:"callback_url,omitempty" json:"callback_url,omitempty"`           // Callback URL for completion
	SkipMultiSeason  bool                  `msgpack:"skip_multi_season,omitempty" json:"skip_multi_season,omitempty"` // Skip multi-season detection

	// Error tracking
	LastError     string     `msgpack:"last_error,omitempty" json:"last_error,omitempty"`           // Last error message
	ErrorCount    int        `msgpack:"error_count,omitempty" json:"error_count,omitempty"`         // Number of errors
	LastErrorTime *time.Time `msgpack:"last_error_time,omitempty" json:"last_error_time,omitempty"` // Last error time
}

func (e *Entry) IsTorrent() bool {
	return e.Protocol == config.ProtocolTorrent
}

func (e *Entry) IsNZB() bool {
	return e.Protocol == config.ProtocolNZB
}

// ObserveTransfer records a successful provider poll without conflating it
// with real transfer progress. The first observation starts the progress grace
// period for legacy rows that predate these timestamps.
func (e *Entry) ObserveTransfer(progress float64, observedAt time.Time) {
	observedAt = observedAt.UTC()
	if e.LastProgressAt == nil || math.Abs(progress-e.Progress) > 1e-9 {
		progressAt := observedAt
		e.LastProgressAt = &progressAt
	}
	lastObservedAt := observedAt
	e.LastObservedAt = &lastObservedAt
}

func (e *Entry) Validate() error {
	activeProvider := e.GetActiveProvider()
	if activeProvider == nil {
		return fmt.Errorf("no active providerEntry")
	}
	// Check if active providerEntry is completed
	if len(activeProvider.Files) == 0 {
		return fmt.Errorf("no files in active providerEntry")
	}
	return nil
}

// Sanitize replaces any non-finite float64 values with 0 so the entry can be
// safely JSON-encoded. This guards against NaN/Inf produced by division-by-zero
// when a debrid provider reports size=0 for an in-progress torrent.
func (e *Entry) Sanitize() {
	if math.IsNaN(e.Progress) || math.IsInf(e.Progress, 0) {
		e.Progress = 0
	}
	for _, p := range e.Providers {
		if math.IsNaN(p.Progress) || math.IsInf(p.Progress, 0) {
			p.Progress = 0
		}
	}
}

// CanBeFixed checks if the entry can be repaired
// TODO: Add more checks later. This will be done when we add other NZB source like TB(nzb)
func (e *Entry) CanBeFixed() bool {
	return e.IsTorrent()
}

func (e *Entry) CanBeMoved() bool {
	return e.IsTorrent()
}

// EntryItem These are torrents by names.
// This keeps track of multiple torrents with the same folder name
// Comprises only files(which has their respective infohashes) and placements
type EntryItem struct {
	Name  string           `msgpack:"name" json:"name"`   // Folder name
	Files map[string]*File `msgpack:"files" json:"files"` // filename -> File details
	Size  int64            `msgpack:"size" json:"size"`   // Total size of all files
}

func (e *EntryItem) GetFile(filename string) (*File, error) {
	if e.Files == nil {
		return nil, fmt.Errorf("failed to get entry item, file is nil")
	}
	f, exists := e.Files[filename]
	if !exists {
		return nil, fmt.Errorf("failed to get entry item, file does not exist")
	}
	if f.Deleted {
		return nil, fmt.Errorf("failed to get entry item, file is deleted")
	}
	return f, nil
}

func (e *EntryItem) GetSize() int64 {
	size := int64(0)
	for _, f := range e.Files {
		if !f.Deleted {
			size += f.Size
		}
	}
	return size
}

func (e *EntryItem) GetFirstFile() (*File, error) {
	for _, f := range e.Files {
		return f, nil
	}
	return nil, fmt.Errorf("no active files found")
}

func (e *EntryItem) GetActiveFiles() []*File {
	files := make([]*File, 0, len(e.Files))
	for _, f := range e.Files {
		if !f.Deleted {
			files = append(files, f)
		}
	}
	return files
}

type File struct {
	// ID is a stable, opaque per-file identity used by long-lived stream URLs.
	// It is assigned once by storage and retained when provider data refreshes.
	ID        string    `msgpack:"id,omitempty" json:"id,omitempty"`
	Name      string    `msgpack:"name" json:"name"`
	Path      string    `msgpack:"path,omitempty" json:"path,omitempty"`
	AddedOn   time.Time `msgpack:"added_on" json:"added_on"`
	Size      int64     `msgpack:"size" json:"size"`
	ByteRange *[2]int64 `msgpack:"byte_range,omitempty" json:"byte_range,omitempty"`
	Deleted   bool      `msgpack:"deleted" json:"deleted"`
	InfoHash  string    `msgpack:"infohash,omitempty" json:"infohash,omitempty"` // Parent infohash(might be an nzb or torrent)
}

// ProviderFile represents debrid-specific file information
type ProviderFile struct {
	Id   string `msgpack:"id,omitempty" json:"id,omitempty"`     // For TorBox-style providers (file_id)
	Link string `msgpack:"link,omitempty" json:"link,omitempty"` // For RealDebrid/AllDebrid-style providers (restricted URL)
	Path string `msgpack:"path,omitempty" json:"path,omitempty"` // Path within the debrid's filesystem
}

// ProviderEntry represents a torrent's providerEntry on a specific debrid service
type ProviderEntry struct {
	Provider  string                    `msgpack:"provider,omitempty" json:"provider,omitempty"`
	ID        string                    `msgpack:"debrid_id" json:"id"`                              // ID in that debrid service (e.g., L3734BKKKSBA6)
	AddedAt   time.Time                 `msgpack:"added_at" json:"added_at"`                         // When added to this debrid
	RemovedAt *time.Time                `msgpack:"removed_at,omitempty" json:"removed_at,omitempty"` // When removed (if archived)
	Status    debridTypes.TorrentStatus `msgpack:"status" json:"status"`                             // ProviderEntry status
	Progress  float64                   `msgpack:"progress" json:"progress"`                         // Download progress on this debrid (0-100)

	// Provider-specific file information
	Files map[string]*ProviderFile `msgpack:"files" json:"files"` // filename -> debrid-specific file info

	// Cached data from debrid (avoid re-fetching)
	DownloadedAt *time.Time `msgpack:"downloaded_at,omitempty" json:"downloaded_at,omitempty"` // When download completed on debrid
}

// NeedsUpdate checks if this placement is stale compared to the remote torrent.
// Returns true if the stored placement should be refreshed.
func (p *ProviderEntry) NeedsUpdate(remote *debridTypes.Torrent) bool {
	if p.ID != remote.Id {
		return true // Re-added on debrid with a different ID
	}
	if p.Status != remote.Status {
		return true // Status changed (e.g., downloading → downloaded)
	}

	if len(p.Files) == 0 {
		return true
	}
	return false
}

func (p *ProviderEntry) IsValid() bool {
	if p.ID == "" || p.Provider == "" {
		return false
	}
	// Check if all files have necessary info
	for _, pf := range p.Files {
		if pf.Id == "" || pf.Link == "" {
			return false
		}
	}
	return true
}

// GetActiveProvider returns the active providerEntry
func (e *Entry) GetActiveProvider() *ProviderEntry {
	if e.Providers == nil || e.ActiveProvider == "" {
		return nil
	}
	providerEntry, exists := e.Providers[e.ActiveProvider]
	if !exists {
		return nil
	}
	return providerEntry
}

func (e *Entry) AddUsenetProvider(metadata *NZB) *ProviderEntry {
	if e.Providers == nil {
		e.Providers = make(map[string]*ProviderEntry)
	}
	providerEntry := &ProviderEntry{
		Provider: "usenet",
		ID:       metadata.ID,
		AddedAt:  time.Now(),
		Status:   debridTypes.TorrentStatusDownloaded,
		Files:    make(map[string]*ProviderFile),
	}
	for _, f := range metadata.Files {
		providerEntry.Files[f.Name] = &ProviderFile{
			Id:   f.Name,
			Link: path.Join(e.MountPath, f.Name),
			Path: path.Join(e.MountPath, f.Name),
		}
	}
	e.Providers["usenet"] = providerEntry
	return providerEntry
}

// AddTorrentProvider adds or updates a providerEntry for a debrid. Before a
// refreshed placement is published, canonical file keys are reconciled by the
// provider's stable ID or path so naming-rule upgrades cannot strand persisted
// files behind stale placement keys.
func (e *Entry) AddTorrentProvider(debridTorrent *debridTypes.Torrent) (*ProviderEntry, error) {
	if e.Providers == nil {
		e.Providers = make(map[string]*ProviderEntry)
	}
	canonicalNames, err := e.reconcileTorrentFiles(debridTorrent)
	if err != nil {
		return nil, err
	}

	providerEntry := &ProviderEntry{
		Provider: debridTorrent.Debrid,
		ID:       debridTorrent.Id,
		AddedAt:  time.Now(),
		Status:   debridTorrent.Status,
		Files:    make(map[string]*ProviderFile),
	}

	for _, f := range debridTorrent.GetFiles() {
		canonicalName := canonicalNames[f.Name]
		if canonicalName == "" {
			return nil, fmt.Errorf("provider file %q has no canonical identity", f.Name)
		}
		if _, exists := providerEntry.Files[canonicalName]; exists {
			return nil, fmt.Errorf("provider files collide at canonical name %q", canonicalName)
		}
		providerEntry.Files[canonicalName] = &ProviderFile{
			Id:   f.Id,
			Link: f.Link,
			Path: f.Path,
		}
	}
	e.Providers[debridTorrent.Debrid] = providerEntry
	return providerEntry, nil
}

// UpdateTorrentProviderState publishes transfer metadata without treating an
// incomplete provider file snapshot as authoritative. Existing placement file
// identities remain available for a later complete refresh; a new placement
// starts empty and cannot leak partial files into the canonical tree.
func (e *Entry) UpdateTorrentProviderState(remote *debridTypes.Torrent) (*ProviderEntry, error) {
	if remote == nil {
		return nil, fmt.Errorf("provider torrent is nil")
	}
	if e.Providers == nil {
		e.Providers = make(map[string]*ProviderEntry)
	}
	previous := e.Providers[remote.Debrid]
	files := make(map[string]*ProviderFile)
	var addedAt time.Time
	var downloadedAt *time.Time
	if previous != nil {
		addedAt = previous.AddedAt
		downloadedAt = previous.DownloadedAt
		for name, file := range previous.Files {
			files[name] = file
		}
	}
	if addedAt.IsZero() {
		addedAt = time.Now()
	}
	placement := &ProviderEntry{
		Provider:     remote.Debrid,
		ID:           remote.Id,
		AddedAt:      addedAt,
		Status:       remote.Status,
		Progress:     remote.Progress,
		Files:        files,
		DownloadedAt: downloadedAt,
	}
	e.Providers[remote.Debrid] = placement
	return placement, nil
}

func (e *Entry) reconcileTorrentFiles(remote *debridTypes.Torrent) (map[string]string, error) {
	if remote == nil {
		return nil, fmt.Errorf("provider torrent is nil")
	}
	if e.Files == nil {
		e.Files = make(map[string]*File)
	}
	remoteFiles := make(map[string]debridTypes.File, len(remote.Files))
	canonicalNames := make(map[string]string, len(remote.Files))
	for _, file := range remote.GetFiles() {
		remoteFiles[file.Name] = file
		canonicalNames[file.Name] = file.Name
	}
	if len(e.Files) == 0 {
		return canonicalNames, nil
	}

	idIndex := make(map[string][]string, len(remoteFiles))
	nativePathIndex := make(map[string][]string, len(remoteFiles))
	outputPathIndex := make(map[string][]string, len(remoteFiles))
	for name, file := range remoteFiles {
		if file.Id != "" {
			idIndex[file.Id] = append(idIndex[file.Id], name)
		}
		if key := providerPathIdentity(file.Path); key != "" {
			nativePathIndex[key] = append(nativePathIndex[key], name)
		}
		if key := providerPathIdentity(file.LocalPath()); key != "" {
			outputPathIndex[key] = append(outputPathIndex[key], name)
		}
	}

	canonicalToRemote := make(map[string]string)
	remoteToCanonical := make(map[string]string)
	addMatch := func(canonicalName, remoteName string) error {
		if canonicalName == "" || remoteName == "" {
			return nil
		}
		if previous := canonicalToRemote[canonicalName]; previous != "" && previous != remoteName {
			return fmt.Errorf("canonical file %q matches multiple refreshed files", canonicalName)
		}
		if previous := remoteToCanonical[remoteName]; previous != "" && previous != canonicalName {
			return fmt.Errorf("refreshed provider file %q matches multiple canonical files", remoteName)
		}
		canonicalToRemote[canonicalName] = remoteName
		remoteToCanonical[remoteName] = canonicalName
		return nil
	}

	previous := e.Providers[remote.Debrid]
	if previous != nil {
		for oldName, oldFile := range previous.Files {
			if oldFile == nil || e.Files[oldName] == nil {
				continue
			}
			newName, matched, err := matchRefreshedProviderFileName(
				oldFile,
				e.Files[oldName],
				idIndex,
				nativePathIndex,
				outputPathIndex,
				remoteFiles,
			)
			if err != nil {
				return nil, fmt.Errorf("reconcile provider file %q: %w", oldName, err)
			}
			if matched {
				if err := addMatch(oldName, newName); err != nil {
					return nil, err
				}
			}
		}
	}

	// A newly added provider has no prior placement. Reuse provider-relative
	// paths from every existing placement so the new placement maps onto the
	// same canonical files instead of publishing a duplicate set.
	for _, placement := range e.Providers {
		if placement == nil {
			continue
		}
		for oldName, oldFile := range placement.Files {
			if oldFile == nil || e.Files[oldName] == nil || canonicalToRemote[oldName] != "" {
				continue
			}
			match, matched, err := matchProviderFilePath(
				oldFile.Path,
				e.Files[oldName],
				nativePathIndex,
				outputPathIndex,
				remoteFiles,
			)
			if err != nil {
				return nil, fmt.Errorf("reconcile provider file %q: %w", oldName, err)
			}
			if matched {
				if err := addMatch(oldName, match); err != nil {
					return nil, err
				}
			}
		}
	}

	// Provider-only legacy rows may not have retained placement paths. Their
	// canonical local path is still a safe final fallback when it identifies
	// exactly one refreshed file.
	for oldName, file := range e.Files {
		if file == nil || canonicalToRemote[oldName] != "" {
			continue
		}
		match, matched, err := matchProviderOutputPath(
			file.Path,
			file,
			outputPathIndex,
			remoteFiles,
		)
		if err != nil {
			return nil, fmt.Errorf("reconcile canonical file %q: %w", oldName, err)
		}
		if matched {
			if err := addMatch(oldName, match); err != nil {
				return nil, err
			}
		}
	}

	canChangeLocalIdentity := !torrentArtifactsMayExist(e)
	renames := make(map[string]string)
	for oldName, newName := range canonicalToRemote {
		if canChangeLocalIdentity && oldName != newName {
			renames[oldName] = newName
			canonicalNames[newName] = newName
		} else {
			canonicalNames[newName] = oldName
		}
	}

	canonical := make(map[string]*File, len(e.Files))
	for oldName, file := range e.Files {
		newName := oldName
		if renamed := renames[oldName]; renamed != "" {
			newName = renamed
		}
		if _, exists := canonical[newName]; exists {
			return nil, fmt.Errorf("canonical file rename %q to %q collides with an existing file", oldName, newName)
		}
		canonical[newName] = file
	}
	unmatchedRemoteNames := make([]string, 0, len(remoteFiles))
	for remoteName := range remoteFiles {
		if remoteToCanonical[remoteName] != "" {
			continue
		}
		unmatchedRemoteNames = append(unmatchedRemoteNames, remoteName)
	}
	sort.Slice(unmatchedRemoteNames, func(i, j int) bool {
		left := remoteFiles[unmatchedRemoteNames[i]]
		right := remoteFiles[unmatchedRemoteNames[j]]
		leftPath := providerPathIdentity(left.Path)
		rightPath := providerPathIdentity(right.Path)
		if leftPath != rightPath {
			return leftPath < rightPath
		}
		return unmatchedRemoteNames[i] < unmatchedRemoteNames[j]
	})
	if !canChangeLocalIdentity && len(unmatchedRemoteNames) > 0 {
		// Manager applies non-empty provider titles after this reconciliation.
		// Reserve against the identity that the final output layout will see.
		outputEntry := *e
		if e.CanApplyTorrentTitle(remote) {
			if remote.Name != "" {
				outputEntry.Name = remote.Name
			}
			if remote.OriginalFilename != "" {
				outputEntry.OriginalFilename = remote.OriginalFilename
			}
		}
		if err := reserveMaterializedTorrentOutputPaths(
			&outputEntry,
			canonical,
			remoteFiles,
			canonicalNames,
			canonicalToRemote,
			unmatchedRemoteNames,
		); err != nil {
			return nil, err
		}
	}
	for _, remoteName := range unmatchedRemoteNames {
		remoteFile := remoteFiles[remoteName]
		canonicalName := canonicalNames[remoteName]
		if _, exists := canonical[canonicalName]; exists {
			return nil, fmt.Errorf("new provider file %q collides with canonical file %q", remoteName, canonicalName)
		}
		canonical[canonicalName] = newCanonicalTorrentFile(e, remoteFile)
	}

	providerMaps := make(map[string]map[string]*ProviderFile, len(e.Providers))
	for providerName, placement := range e.Providers {
		if placement == nil {
			continue
		}
		files := make(map[string]*ProviderFile, len(placement.Files))
		for oldName, file := range placement.Files {
			newName := oldName
			if renamed := renames[oldName]; renamed != "" {
				// Provider IDs are provider-local, and a shared old map key is
				// not enough to prove that a fallback link is the same media.
				// Carry a non-refreshed placement across a canonical rename only
				// when its provider-relative path establishes that association.
				// Otherwise omit it until that placement is refreshed and can be
				// reconciled independently.
				if providerName != remote.Debrid && !providerFileMatchesRemotePath(file, remoteFiles[renamed]) {
					continue
				}
				newName = renamed
			}
			if _, exists := files[newName]; exists {
				return nil, fmt.Errorf("provider %q file rename %q to %q collides with an existing file", providerName, oldName, newName)
			}
			files[newName] = file
		}
		providerMaps[providerName] = files
	}

	for _, remoteName := range canonicalToRemote {
		canonicalName := canonicalNames[remoteName]
		file := canonical[canonicalName]
		remoteFile := remoteFiles[remoteName]
		if canChangeLocalIdentity {
			file.Name = canonicalName
			file.Path = remoteFile.LocalPath()
		} else if file.Path == "" {
			file.Path = remoteFile.LocalPath()
		}
		if remoteFile.Size > 0 || remoteFile.ByteRange != nil || (file.Size <= 0 && file.ByteRange == nil) {
			file.Size = remoteFile.Size
			file.ByteRange = remoteFile.ByteRange
		}
		file.InfoHash = e.InfoHash
		if file.AddedOn.IsZero() {
			file.AddedOn = e.AddedOn
		}
	}
	e.Files = canonical
	for providerName, files := range providerMaps {
		e.Providers[providerName].Files = files
	}
	return canonicalNames, nil
}

func newCanonicalTorrentFile(entry *Entry, file debridTypes.File) *File {
	return &File{
		Name:      file.Name,
		Path:      file.LocalPath(),
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
		InfoHash:  entry.InfoHash,
		AddedOn:   entry.AddedOn,
	}
}

func torrentArtifactsMayExist(entry *Entry) bool {
	if entry == nil {
		return false
	}
	if entry.CompletedAt != nil || entry.IsDownloading || entry.SizeDownloaded > 0 {
		return true
	}
	// IsDownloading is process-local work state and is intentionally cleared
	// during restart recovery. The owned output directory is durable evidence
	// that a symlink, STRM, or downloaded file may already exist. Any inspection
	// error other than nonexistence is handled conservatively to avoid stranding
	// an artifact behind a renamed logical path.
	if !filepath.IsAbs(entry.SavePath) {
		// Relative and empty legacy roots cannot be pinned without consulting
		// process working-directory state. Treat them as potentially materialized
		// instead of authorizing an irreversible identity change.
		return true
	}
	candidate := entry.DownloadPath()
	relative, err := filepath.Rel(entry.SavePath, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		// An invalid legacy output identity is never safe to rename implicitly.
		return true
	}
	rooted, _, err := safepath.OpenRoot(entry.SavePath)
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	defer rooted.Close()
	if _, err := rooted.Lstat(relative); err == nil || !errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

func torrentFileSizesCompatible(canonical *File, remote debridTypes.File) bool {
	canonicalSize := boundedTorrentTransferSize(canonical.Size, canonical.ByteRange)
	remoteSize := boundedTorrentTransferSize(remote.Size, remote.ByteRange)
	return canonicalSize <= 0 || remoteSize <= 0 || canonicalSize == remoteSize
}

func boundedTorrentTransferSize(size int64, byteRange *[2]int64) int64 {
	if byteRange == nil || byteRange[0] < 0 || byteRange[1] < byteRange[0] {
		return size
	}
	span := byteRange[1] - byteRange[0]
	if span == math.MaxInt64 {
		return 0
	}
	return span + 1
}

func matchRefreshedProviderFileName(
	oldFile *ProviderFile,
	canonicalFile *File,
	idIndex, nativePathIndex, outputPathIndex map[string][]string,
	remoteFiles map[string]debridTypes.File,
) (string, bool, error) {
	if oldFile.Id != "" {
		matches := idIndex[oldFile.Id]
		if len(matches) > 1 {
			return "", false, fmt.Errorf("provider ID %q is ambiguous", oldFile.Id)
		}
		if len(matches) == 1 {
			// A unique provider-local ID is stronger identity evidence than a
			// provider path. Output-path aliases can legitimately equal another
			// file's native path after portable sanitization, so consulting the
			// combined path index here would turn a proven ID match into a false
			// ambiguity.
			return matches[0], true, nil
		}
	}
	if oldFile.Path != "" {
		pathName, matched, err := matchProviderFilePath(
			oldFile.Path,
			canonicalFile,
			nativePathIndex,
			outputPathIndex,
			remoteFiles,
		)
		if err != nil {
			return "", false, err
		}
		if matched {
			return pathName, true, nil
		}
	}
	return "", false, nil
}

func matchProviderFilePath(
	value string,
	canonicalFile *File,
	nativePathIndex, outputPathIndex map[string][]string,
	remoteFiles map[string]debridTypes.File,
) (string, bool, error) {
	key := providerPathIdentity(value)
	if key == "" {
		return "", false, nil
	}
	if matches := nativePathIndex[key]; len(matches) > 1 {
		return "", false, fmt.Errorf("provider path %q is ambiguous", value)
	} else if len(matches) == 1 {
		return matches[0], true, nil
	}
	var sizeMatches []string
	hadSizeConflict := false
	for name, remoteFile := range remoteFiles {
		remoteKey := providerPathIdentity(remoteFile.Path)
		if !providerPathsDifferOnlyByLeadingRoot(key, remoteKey) {
			continue
		}
		if torrentFileSizesCompatible(canonicalFile, remoteFile) {
			sizeMatches = append(sizeMatches, name)
		} else {
			hadSizeConflict = true
		}
	}
	if len(sizeMatches) > 1 {
		return "", false, fmt.Errorf("provider path %q is ambiguous without its release root", value)
	}
	if len(sizeMatches) == 1 {
		return sizeMatches[0], true, nil
	}
	if hadSizeConflict {
		return "", false, fmt.Errorf("provider path %q conflicts with refreshed file sizes", value)
	}
	// Portable output paths are weaker evidence than all forms of a provider's
	// native path. A source-native path can equal an unrelated file's sanitized
	// output while the intended file differs only by a release-root prefix, so
	// consult output paths only after native exact and rooted matches are absent.
	if name, matched, err := matchProviderOutputPath(value, canonicalFile, outputPathIndex, remoteFiles); err != nil || matched {
		return name, matched, err
	}
	return "", false, nil
}

func matchProviderOutputPath(
	value string,
	canonicalFile *File,
	outputPathIndex map[string][]string,
	remoteFiles map[string]debridTypes.File,
) (string, bool, error) {
	key := providerPathIdentity(value)
	if key == "" {
		return "", false, nil
	}
	matches := outputPathIndex[key]
	if len(matches) > 1 {
		return "", false, fmt.Errorf("provider output path %q is ambiguous", value)
	}
	if len(matches) == 0 {
		return "", false, nil
	}
	name := matches[0]
	if !torrentFileSizesCompatible(canonicalFile, remoteFiles[name]) {
		return "", false, fmt.Errorf("provider output path %q conflicts with refreshed file sizes", value)
	}
	return name, true, nil
}

func providerPathsDifferOnlyByLeadingRoot(left, right string) bool {
	if left == "" || right == "" || left == right ||
		!strings.Contains(left, "/") || !strings.Contains(right, "/") {
		return false
	}
	return strings.HasSuffix(left, "/"+right) || strings.HasSuffix(right, "/"+left)
}

func providerPathIdentity(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, `\`, "/"))
	if value == "" {
		return ""
	}
	return strings.ToLower(path.Clean(value))
}

func providerFileMatchesRemotePath(providerFile *ProviderFile, remoteFile debridTypes.File) bool {
	if providerFile == nil {
		return false
	}
	key := providerPathIdentity(providerFile.Path)
	if key == "" {
		return false
	}
	return key == providerPathIdentity(remoteFile.Path) || key == providerPathIdentity(remoteFile.LocalPath())
}

// ActivatePlacement switches the active debrid
func (e *Entry) ActivatePlacement(debridName string) error {
	if e.Providers == nil {
		return ErrPlacementNotFound
	}

	// The map key is the configured account identity. Prefer it so two accounts
	// of the same provider type can never activate one another by accident.
	foundPlacement := e.Providers[debridName]
	if foundPlacement == nil {
		// Retain compatibility with older rows whose key and provider name differ.
		for _, providerEntry := range e.Providers {
			if providerEntry != nil && providerEntry.Provider == debridName {
				foundPlacement = providerEntry
				break
			}
		}
	}

	if foundPlacement == nil {
		return ErrPlacementNotFound
	}

	if foundPlacement.Status != debridTypes.TorrentStatusDownloaded {
		return ErrPlacementNotCompleted
	}

	e.ActiveProvider = debridName
	e.UpdatedAt = time.Now()

	return nil
}

// RemoveProvider removes local placements for one configured provider. Remote
// cleanup belongs to the manager layer so provider errors cannot be discarded
// while durable state is removed.
func (e *Entry) RemoveProvider(debridName string) {
	if e.Providers == nil {
		return
	}

	// Find and remove all placements with this debrid name
	var keysToDelete []string

	for key, providerEntry := range e.Providers {
		// Current rows use the configured provider name as both key and value.
		// Accept either identity so older rows can still be cleaned up safely.
		if key == debridName || (providerEntry != nil && providerEntry.Provider == debridName) {
			keysToDelete = append(keysToDelete, key)
		}
	}

	// Delete the placements
	for _, key := range keysToDelete {
		delete(e.Providers, key)
	}

	// If the providerEntry is the active providerEntry, find a new active providerEntry
	if e.ActiveProvider == debridName {
		e.SwitchToNextProvider()
	}
}

// HasProvider checks if torrent exists on a debrid
func (e *Entry) HasProvider(provider string) bool {
	if e.Providers == nil {
		return false
	}
	_, exists := e.Providers[provider]
	return exists
}

// SwitchToNextProvider switches to the next completed providerEntry if available
func (e *Entry) SwitchToNextProvider() {
	if e.Providers == nil {
		return
	}
	for _, providerEntry := range e.Providers {
		if providerEntry.Status == debridTypes.TorrentStatusDownloaded {
			_ = e.ActivatePlacement(providerEntry.Provider)
			return
		}
	}
}

// MarkAsCompleted marks the torrent as completed
func (e *Entry) MarkAsCompleted(contentPath string) {
	e.State = EntryStatePausedUP
	e.IsDownloading = false
	e.IsComplete = true
	e.Progress = 1.0
	e.ContentPath = contentPath
	now := time.Now()
	e.CompletedAt = &now
	e.UpdatedAt = now
}

// MarkAsError marks the torrent as errored
func (e *Entry) MarkAsError(err error) {
	e.State = EntryStateError
	e.Status = debridTypes.TorrentStatusError
	e.IsDownloading = false
	e.LastError = err.Error()
	e.ErrorCount++
	now := time.Now()
	e.LastErrorTime = &now
	e.UpdatedAt = now
}

// MarkAsStalled records a terminal transfer stall while preserving the
// qBittorrent stalledDL state that Servarr understands. Keeping this distinct
// from a generic provider error lets the Arr handoff target only watchdog
// decisions, not transient provider or network failures.
func (e *Entry) MarkAsStalled(err error) {
	e.MarkAsError(err)
	e.State = EntryStateStalledDL
}

func (e *Entry) GetFile(filename string) (*File, error) {
	if e.Files == nil {
		return nil, fmt.Errorf("failed to get entry file, files is nil")
	}
	f, exists := e.Files[filename]
	if !exists {
		return nil, fmt.Errorf("failed to get entry file, file not found")
	}
	if f.Deleted {
		return nil, fmt.Errorf("file deleted")
	}
	return f, nil
}

// GetFileByID resolves a non-deleted file by its stable identity.
func (e *Entry) GetFileByID(id string) (*File, error) {
	if id == "" {
		return nil, fmt.Errorf("file id is required")
	}
	for _, file := range e.Files {
		if file != nil && file.ID == id && !file.Deleted {
			return file, nil
		}
	}
	return nil, fmt.Errorf("file id %q not found", id)
}

// RunChecks performs integrity checks on the Entry
// Returns whether to refresh and any error encountered
func (e *Entry) RunChecks() (bool, error) {
	if e.Bad {
		return false, fmt.Errorf("entry marked as bad")
	}
	activeProvider := e.GetActiveProvider()
	if activeProvider == nil {
		return true, fmt.Errorf("no active providerEntry") // need to refresh
	}
	if activeProvider.Status != debridTypes.TorrentStatusDownloaded {
		return true, fmt.Errorf("active providerEntry not completed") // need to refresh
	}

	if len(activeProvider.Files) == 0 {
		return true, fmt.Errorf("no files in active providerEntry") // need to refresh
	}

	// Then check all the files exists and files not deleted have links
	for _, f := range e.Files {
		if f.Deleted {
			continue
		}
		pf, exists := activeProvider.Files[f.Name]
		if !exists {
			return true, fmt.Errorf("file %s missing in active providerEntry", f.Name) // need to refresh
		}
		if pf.Link == "" {
			return true, fmt.Errorf("file %s has no link in active providerEntry", f.Name) // need to refresh
		}
	}
	return false, nil
}

func (e *Entry) GetActiveFiles() []*File {
	files := make([]*File, 0, len(e.Files))
	for _, f := range e.Files {
		if !f.Deleted {
			files = append(files, f)
		}
	}
	return files
}
func (e *Entry) GetFolder() string {
	// CHeck if the mount folder is empty or .
	return GetTorrentFolder(config.Get().FolderNaming, e)
}

// IsValid checks if the torrent has essential fields
func (e *Entry) IsValid() bool {
	// Check infohash
	if e.InfoHash == "" || e.Name == "" {
		return false
	}
	// Check if there is at least one providerEntry
	if len(e.Providers) == 0 {
		return false
	}
	activePlacement := e.GetActiveProvider()
	if activePlacement == nil {
		return false
	}
	// Check validity of active providerEntry
	if activePlacement.ID == "" || activePlacement.Provider == "" {
		return false
	}
	return activePlacement.IsValid()
}

// DownloadPath returns the expected download/symlink path for this entry
func (e *Entry) DownloadPath() string {
	return filepath.Join(e.SavePath, e.OutputComponent())
}

// SwitcherJob tracks the progress of a migration operation
type SwitcherJob struct {
	ID             string         `msgpack:"id" json:"id"`
	InfoHash       string         `msgpack:"infohash" json:"info_hash"`                            // Entry being migrated
	SourceProvider string         `msgpack:"source_provider" json:"source_provider"`               // Source provider
	TargetProvider string         `msgpack:"target_provider" json:"target_provider"`               // Target provider
	Status         SwitcherStatus `msgpack:"status" json:"status"`                                 // Job status
	Progress       float64        `msgpack:"progress" json:"progress"`                             // Progress (0-100)
	Error          string         `msgpack:"error,omitempty" json:"error,omitempty"`               // Error message if failed
	CreatedAt      time.Time      `msgpack:"created_at" json:"created_at"`                         // When job started
	CompletedAt    *time.Time     `msgpack:"completed_at,omitempty" json:"completed_at,omitempty"` // When completed
	KeepOld        bool           `msgpack:"keep_old" json:"keep_old"`                             // Whether to keep old providerEntry(or remove it)
	WaitComplete   bool           `msgpack:"wait_complete" json:"wait_complete"`                   // Whether to wait for download
}

// SystemMigrationStatus tracks overall system migration from legacy to unified
type SystemMigrationStatus struct {
	Running   bool      `msgpack:"running" json:"running"`                           // Whether migration is running
	Total     int       `msgpack:"total" json:"total"`                               // Total torrents to migrate
	Completed int       `msgpack:"completed" json:"completed"`                       // Completed migrations
	Errors    int       `msgpack:"errors" json:"errors"`                             // Number of errors
	StartedAt time.Time `msgpack:"started_at" json:"started_at"`                     // When migration started
	UpdatedAt time.Time `msgpack:"updated_at" json:"updated_at"`                     // Last update
	ErrorList []string  `msgpack:"error_list,omitempty" json:"error_list,omitempty"` // List of errors
}

// CachedTorrent represents the debrid cache JSON format for migration
type CachedTorrent struct {
	ID               string                       `json:"id"`                // Debrid torrent ID
	InfoHash         string                       `json:"info_hash"`         // Entry info hash
	Name             string                       `json:"name"`              // Entry name
	Folder           string                       `json:"folder"`            // Folder name
	Filename         string                       `json:"filename"`          // Filename
	OriginalFilename string                       `json:"original_filename"` // Original filename
	Size             int64                        `json:"size"`              // Size (legacy)
	Bytes            int64                        `json:"bytes"`             // Actual bytes
	Magnet           any                          `json:"magnet"`            // Magnet (can be nil)
	Files            map[string]*debridTypes.File `json:"files"`             // Files map
	Status           string                       `json:"status"`            // Status from debrid
	Added            string                       `json:"added"`             // Added timestamp
	Progress         float64                      `json:"progress"`          // Progress 0-100
	Speed            int64                        `json:"speed"`             // Speed
	Seeders          int                          `json:"seeders"`           // Seeders
	Links            []string                     `json:"links"`             // Download links
	MountPath        string                       `json:"mount_path"`        // Mount path
	DeletedFiles     []string                     `json:"deleted_files"`     // Deleted files
	Debrid           string                       `json:"debrid"`            // Debrid name
	Arr              *arr.Arr                     `json:"arr"`               // Arr association
	AddedOn          string                       `json:"added_on"`          // Added on timestamp
	IsComplete       bool                         `json:"is_complete"`       // Is complete
	Bad              bool                         `json:"bad"`               // Is bad
}

// ToManagedTorrent converts a cached torrent to managed format
func (ct *CachedTorrent) ToManagedTorrent() *Entry {
	now := time.Now()
	// Parse timestamps
	var addedOn, createdAt time.Time
	if ct.AddedOn != "" {
		addedOn, _ = time.Parse(time.RFC3339, ct.AddedOn)
	}
	if addedOn.IsZero() && ct.Added != "" {
		addedOn, _ = time.Parse(time.RFC3339, ct.Added)
	}
	if addedOn.IsZero() {
		addedOn = now
	}
	createdAt = addedOn
	// GetReader category from arr
	var category string
	if ct.Arr != nil {
		category = ct.Arr.Name
	}

	mt := &Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         ct.InfoHash,
		Name:             ct.Name,
		OriginalFilename: ct.OriginalFilename,
		Size:             ct.Size,
		Bytes:            ct.Bytes,
		Magnet:           "",
		ActiveProvider:   ct.Debrid,
		Providers:        make(map[string]*ProviderEntry),
		Status:           debridTypes.TorrentStatus(ct.Status),
		Progress:         ct.Progress,
		Speed:            ct.Speed,
		Seeders:          ct.Seeders,
		IsComplete:       ct.IsComplete,
		Bad:              ct.Bad,
		Category:         category,
		Tags:             []string{},
		MountPath:        ct.MountPath,
		AddedOn:          addedOn,
		CreatedAt:        createdAt,
		UpdatedAt:        now,
		Files:            make(map[string]*File),
	}

	for name, f := range ct.Files {
		mt.Files[name] = &File{
			Name:      f.Name,
			Path:      f.LocalPath(),
			Size:      f.Size,
			ByteRange: f.ByteRange,
			InfoHash:  ct.InfoHash, // Track which torrent this file came from
			Deleted:   f.Deleted,
			AddedOn:   addedOn,
		}
	}

	// Set magnet if present
	if ct.Magnet != nil {
		if mag, ok := ct.Magnet.(string); ok {
			mt.Magnet = mag
		}
	}

	// Create providerEntry for this debrid
	if ct.Debrid != "" && ct.ID != "" {
		var downloadedAt *time.Time
		if ct.IsComplete {
			downloadedAt = &addedOn
		}

		providerEntry := &ProviderEntry{
			Provider:     ct.Debrid,
			ID:           ct.ID,
			AddedAt:      addedOn,
			Status:       debridTypes.TorrentStatus(ct.Status),
			Progress:     ct.Progress,
			DownloadedAt: downloadedAt,
			Files:        make(map[string]*ProviderFile),
		}

		// Populate providerEntry files from cached torrent
		for _, f := range ct.Files {
			providerEntry.Files[f.Name] = &ProviderFile{
				Id:   f.Id,
				Link: f.Link,
				Path: f.Path,
			}
		}

		// Use composite key for providerEntry
		mt.Providers[ct.Debrid] = providerEntry
	}

	// Set completion timestamp if complete
	if ct.IsComplete {
		mt.CompletedAt = &addedOn
	}

	return mt
}

// GetTorrentFolder returns the folder name for a torrent by debrid ID
func GetTorrentFolder(folderNaming config.WebDavFolderNaming, entry *Entry) string {
	var folder string
	switch folderNaming {
	case config.WebDavUseFileName:
		folder = path.Clean(entry.Name)
	case config.WebDavUseOriginalName:
		folder = path.Clean(entry.OriginalFilename)
	case config.WebDavUseFileNameNoExt:
		folder = path.Clean(utils.RemoveExtension(entry.Name))
	case config.WebDavUseOriginalNameNoExt:
		folder = path.Clean(utils.RemoveExtension(entry.OriginalFilename))
	case config.WebdavUseHash:
		folder = entry.InfoHash
	default:
		folder = path.Clean(entry.Name)
	}
	return folder
}
