package manager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	strmurl "github.com/Trifocals3537/tessarr/pkg/strm"
	"github.com/rs/zerolog"
)

const (
	strmRootMarker  = ".tessarr-strm-root"
	strmEntryMarker = ".tessarr-strm-entry"
	maxStrmRead     = 4096
	maxStrmWalk     = 250000
)

var (
	strmRootMarkerNames  = []string{strmRootMarker, legacyStrmRootMarker}
	strmEntryMarkerNames = []string{strmEntryMarker, legacyStrmEntryMarker}
)

// Strm maintains a signed, mountless media library derived from durable
// entries. It owns only files it can cryptographically recognize.
type Strm struct {
	manager *Manager
	logger  zerolog.Logger
	mu      sync.Mutex
	jobsMu  sync.Mutex
	// entryJobs stores whether another pass became necessary while a pass for
	// the same entry was already running. This coalesces frequent refreshes.
	entryJobs   map[string]bool
	sweepActive bool
	sweepDirty  bool
	sweepReason string
}

type StrmReport struct {
	Entries  int      `json:"entries"`
	Verified int      `json:"verified"`
	Written  int      `json:"written"`
	Deleted  int      `json:"deleted"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors,omitempty"`
}

type strmRootState struct {
	Version        int    `json:"version"`
	KeyFingerprint string `json:"key_fingerprint"`
}

type strmEntryState struct {
	Version   int    `json:"version"`
	InfoHash  string `json:"info_hash"`
	Directory string `json:"directory"`
	Signature string `json:"signature"`
}

type strmTarget struct {
	path    string
	content string
	entryID string
	fileID  string
}

func NewStrm(manager *Manager) *Strm {
	return &Strm{
		manager:   manager,
		logger:    manager.logger.With().Str("component", "strm").Logger(),
		entryJobs: make(map[string]bool),
	}
}

func (r *StrmReport) addError(err error) {
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
}

func strmKeyFingerprint(secret string) string {
	// The legacy domain is a stable on-disk signature namespace. The migration
	// renames its marker but deliberately keeps the signature valid.
	sum := sha256.Sum256([]byte("decypharr-strm-root\x00" + secret))
	return hex.EncodeToString(sum[:])
}

func (s *Strm) ensureRoot(cfg *config.Config) (string, error) {
	root, err := safepath.ValidateRoot(cfg.Strm.Path)
	if err != nil {
		return "", err
	}
	root, err = safepath.EnsureRoot(root, 0755)
	if err != nil {
		return "", fmt.Errorf("create STRM root: %w", err)
	}
	if err := safepath.RejectSymlinks(root); err != nil {
		return "", err
	}

	markerPath := filepath.Join(root, strmRootMarker)
	data, err := readCompatibleStrmMarker(root, strmRootMarkerNames)
	if errors.Is(err, os.ErrNotExist) {
		children, readErr := os.ReadDir(root)
		if readErr != nil {
			return "", fmt.Errorf("inspect STRM root: %w", readErr)
		}
		if len(children) != 0 {
			return "", fmt.Errorf("STRM root %q is non-empty and is not owned by Tessarr", root)
		}
		state := strmRootState{Version: 1, KeyFingerprint: strmKeyFingerprint(cfg.Strm.Secret)}
		encoded, marshalErr := json.Marshal(state)
		if marshalErr != nil {
			return "", marshalErr
		}
		if err := atomicWrite(root, markerPath, encoded); err != nil {
			return "", fmt.Errorf("write STRM root marker: %w", err)
		}
		return root, nil
	}
	if err != nil {
		return "", fmt.Errorf("read STRM root marker: %w", err)
	}
	var state strmRootState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 ||
		state.KeyFingerprint != strmKeyFingerprint(cfg.Strm.Secret) {
		return "", fmt.Errorf("STRM root %q has an invalid ownership marker or signing key", root)
	}
	return root, nil
}

func (s *Strm) ensureEntryDir(root string, cfg *config.Config, entry *storage.Entry) (string, error) {
	directory := entryDirectoryName(entry)
	dir, err := safepath.JoinIdentifiers(root, directory)
	if err != nil {
		return "", err
	}
	if _, err := safepath.EnsureDir(root, dir, 0755); err != nil {
		return "", err
	}
	markerPath := filepath.Join(dir, strmEntryMarker)
	data, err := readCompatibleStrmMarker(dir, strmEntryMarkerNames)
	if errors.Is(err, os.ErrNotExist) {
		children, readErr := os.ReadDir(dir)
		if readErr != nil {
			return "", readErr
		}
		if len(children) != 0 {
			return "", fmt.Errorf("STRM entry directory %q is non-empty and unowned", dir)
		}
		signature, signErr := strmurl.Sign(cfg.Strm.Secret, "entry-marker", entry.InfoHash, directory)
		if signErr != nil {
			return "", signErr
		}
		encoded, marshalErr := json.Marshal(strmEntryState{
			Version: 1, InfoHash: entry.InfoHash, Directory: directory, Signature: signature,
		})
		if marshalErr != nil {
			return "", marshalErr
		}
		if err := atomicWrite(root, markerPath, encoded); err != nil {
			return "", err
		}
		return dir, nil
	}
	if err != nil {
		return "", err
	}
	var state strmEntryState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 ||
		state.InfoHash != entry.InfoHash || state.Directory != directory ||
		!strmurl.Verify(cfg.Strm.Secret, "entry-marker", state.Signature, entry.InfoHash, directory) {
		return "", fmt.Errorf("STRM entry directory %q has an invalid ownership marker", dir)
	}
	return dir, nil
}

func entryDirectoryName(entry *storage.Entry) string {
	suffix := " [" + identitySlug(entry.InfoHash) + "]"
	name := compactPortableComponent(
		entry.GetFolder(),
		"entry",
		safepath.PortableIdentifierMaxBytes-len(suffix),
	)
	return name + suffix
}

func identitySlug(identity string) string {
	clean := strings.ToLower(strings.TrimSpace(identity))
	if len(clean) >= 8 {
		return clean[:8]
	}
	return digestSlug(identity)
}

func digestSlug(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func portableComponent(value, fallback string) string {
	value = strings.TrimSpace(filepath.Base(strings.ReplaceAll(value, "\\", "/")))
	var builder strings.Builder
	for _, r := range value {
		switch {
		case unicode.IsControl(r), strings.ContainsRune(`<>:"/\\|?*`, r):
			builder.WriteRune('_')
		default:
			builder.WriteRune(r)
		}
	}
	value = strings.TrimRight(strings.TrimSpace(builder.String()), ". ")
	if value == "" {
		value = fallback
	}
	if err := safepath.ValidateIdentifier(value); err != nil {
		value = fallback + "-" + digestSlug(value)
	}
	return value
}

func compactPortableComponent(value, fallback string, maxBytes int) string {
	value = portableComponent(value, fallback)
	compacted, err := safepath.CompactIdentifier(value, maxBytes)
	if err == nil {
		return compacted
	}

	// portableComponent already rejects unsafe values. This fallback only
	// protects callers from an unexpected compaction failure while retaining a
	// deterministic digest rather than silently aliasing another component.
	fallback = fallback + "-" + digestSlug(value)
	compacted, err = safepath.CompactIdentifier(fallback, maxBytes)
	if err == nil {
		return compacted
	}
	return "item"
}

func mediaRelativePath(name, fileID string, keepExtension bool) string {
	parts := strings.FieldsFunc(strings.ReplaceAll(name, "\\", "/"), func(r rune) bool { return r == '/' })
	if len(parts) == 0 {
		parts = []string{"media"}
	}
	for index := range parts {
		parts[index] = compactPortableComponent(
			parts[index],
			"media",
			safepath.PortableIdentifierMaxBytes,
		)
	}
	parts[len(parts)-1] = strmurl.FileName(parts[len(parts)-1], keepExtension)
	compacted, err := safepath.CompactIdentifier(
		parts[len(parts)-1],
		safepath.PortableIdentifierMaxBytes,
	)
	if err != nil {
		parts[len(parts)-1] = "media-" + identitySlug(fileID) + ".strm"
	} else {
		parts[len(parts)-1] = compacted
	}
	return filepath.Join(parts...)
}

func (s *Strm) desired(root string, cfg *config.Config, entry *storage.Entry) ([]strmTarget, error) {
	base, err := strmurl.BaseURL(cfg)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entry.Files))
	for name := range entry.Files {
		names = append(names, name)
	}
	sort.Strings(names)

	used := make(map[string]struct{}, len(names))
	type plannedTarget struct {
		relative string
		content  string
		fileID   string
	}
	planned := make([]plannedTarget, 0, len(names))
	for _, name := range names {
		file := entry.Files[name]
		if file == nil || file.Deleted || file.ID == "" ||
			strings.EqualFold(filepath.Ext(file.Name), ".strm") || cfg.IsFileAllowed(file.Name, file.Size) != nil {
			continue
		}
		relative := mediaRelativePath(file.Name, file.ID, cfg.Strm.KeepMediaExtension)
		portableKey := strings.ToLower(filepath.ToSlash(relative))
		if _, collision := used[portableKey]; collision {
			relative, err = disambiguateStrmPath(relative, file.ID)
			if err != nil {
				return nil, fmt.Errorf("disambiguate colliding STRM path %q: %w", file.Name, err)
			}
			portableKey = strings.ToLower(filepath.ToSlash(relative))
		}
		if _, collision := used[portableKey]; collision {
			return nil, fmt.Errorf("STRM path collision for %q", file.Name)
		}
		used[portableKey] = struct{}{}
		content, err := strmurl.FileURL(base, cfg.Strm.Secret, entry.InfoHash, file.ID, file.Name)
		if err != nil {
			return nil, err
		}
		planned = append(planned, plannedTarget{
			relative: relative, content: content, fileID: file.ID,
		})
	}
	if len(planned) == 0 {
		return nil, nil
	}
	dir, err := s.ensureEntryDir(root, cfg, entry)
	if err != nil {
		return nil, err
	}
	targets := make([]strmTarget, 0, len(planned))
	for _, plan := range planned {
		targetPath := filepath.Join(dir, plan.relative)
		if _, err := safepath.ValidateUnderRoot(root, targetPath); err != nil {
			return nil, err
		}
		targets = append(targets, strmTarget{
			path: targetPath, content: plan.content, entryID: entry.InfoHash, fileID: plan.fileID,
		})
	}
	return targets, nil
}

func disambiguateStrmPath(relative, fileID string) (string, error) {
	directory := filepath.Dir(relative)
	if directory == "." {
		directory = ""
	}
	base := filepath.Base(relative)
	extension := filepath.Ext(base)
	base = strings.TrimSuffix(base, extension) + " [" + identitySlug(fileID) + "]" + extension
	base, err := safepath.CompactIdentifier(base, safepath.PortableIdentifierMaxBytes)
	if err != nil {
		return "", err
	}
	return filepath.Join(directory, base), nil
}

func (s *Strm) syncEntryLocked(ctx context.Context, root string, cfg *config.Config, entry *storage.Entry, report *StrmReport) []strmTarget {
	if entry == nil || !entry.IsComplete {
		return nil
	}
	if err := ctx.Err(); err != nil {
		report.addError(err)
		return nil
	}
	for _, file := range entry.Files {
		if file != nil && file.ID == "" {
			if err := s.manager.storage.AddOrUpdateDurable(entry); err != nil {
				report.addError(fmt.Errorf("assign file IDs for %s: %w", entry.Name, err))
				return nil
			}
			break
		}
	}
	targets, err := s.desired(root, cfg, entry)
	if err != nil {
		report.addError(fmt.Errorf("prepare STRM entry %s: %w", entry.Name, err))
		return nil
	}
	report.Entries++
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			report.addError(err)
			break
		}
		verified, err := writeOwnedStrm(root, cfg.Strm.Secret, target)
		if err != nil {
			report.Skipped++
			report.addError(err)
			continue
		}
		if verified {
			report.Verified++
		} else {
			report.Written++
		}
	}
	s.removeEntryStaleLocked(ctx, root, cfg.Strm.Secret, entry, targets, report)
	return targets
}

func (s *Strm) removeEntryStaleLocked(ctx context.Context, root, secret string, entry *storage.Entry, targets []strmTarget, report *StrmReport) {
	dir := filepath.Join(root, entryDirectoryName(entry))
	if err := verifyEntryMarker(dir, secret, entry.InfoHash); err != nil {
		return
	}
	keep := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		keep[filepath.Clean(target.path)] = struct{}{}
	}
	visited := 0
	_ = filepath.WalkDir(dir, func(path string, item fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		visited++
		if visited > maxStrmWalk || ctx.Err() != nil {
			return fs.SkipAll
		}
		if item.IsDir() || !strings.EqualFold(filepath.Ext(path), ".strm") {
			return nil
		}
		if _, wanted := keep[filepath.Clean(path)]; wanted {
			return nil
		}
		data, err := readRegularFile(path, maxStrmRead)
		if err != nil {
			return nil
		}
		infohash, _, owned := strmurl.ParseOwnedURL(strings.TrimSpace(string(data)), secret)
		if owned && infohash == entry.InfoHash {
			if err := safepath.Remove(root, path); err != nil {
				report.addError(err)
			} else {
				report.Deleted++
				pruneEmptyStrmDirs(dir, filepath.Dir(path))
			}
		}
		return nil
	})
	if len(targets) == 0 {
		if err := removeStrmMarkers(root, dir, strmEntryMarkerNames); err == nil {
			pruneEmptyStrmDirs(root, dir)
		}
	}
}

func writeOwnedStrm(root, secret string, target strmTarget) (bool, error) {
	data, err := readRegularFile(target.path, maxStrmRead)
	if err == nil {
		current := strings.TrimSpace(string(data))
		if current == target.content {
			return true, nil
		}
		entryID, _, owned := strmurl.ParseOwnedURL(current, secret)
		if !owned || entryID != target.entryID {
			return false, fmt.Errorf("preserving foreign STRM file %q", target.path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := atomicWrite(root, target.path, []byte(target.content+"\n")); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Strm) Sweep(ctx context.Context) (*StrmReport, error) {
	cfg := config.Get()
	if !cfg.Strm.Active() {
		return nil, fmt.Errorf("STRM is disabled or has no export path")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	root, err := s.ensureRoot(cfg)
	if err != nil {
		return nil, err
	}
	entries, err := s.manager.storage.List(nil)
	if err != nil {
		return nil, err
	}
	report := &StrmReport{}
	desired := make(map[string]struct{})
	desiredDirs := make(map[string]struct{})
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		targets := s.syncEntryLocked(ctx, root, cfg, entry, report)
		if len(targets) > 0 {
			desiredDirs[filepath.Clean(filepath.Join(root, entryDirectoryName(entry)))] = struct{}{}
		}
		for _, target := range targets {
			desired[filepath.Clean(target.path)] = struct{}{}
		}
	}
	if err := s.removeStale(ctx, root, cfg.Strm.Secret, desired, desiredDirs, report); err != nil {
		return report, err
	}
	return report, nil
}

func (s *Strm) removeStale(ctx context.Context, root, secret string, desired, desiredDirs map[string]struct{}, report *StrmReport) error {
	visited := 0
	var stale []string
	var staleMarkers []strmEntryState
	seenMarkerDirs := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		visited++
		if visited > maxStrmWalk {
			return fmt.Errorf("STRM tree exceeds %d entries", maxStrmWalk)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if isStrmMarkerName(filepath.Base(path), strmEntryMarkerNames) {
			dir := filepath.Clean(filepath.Dir(path))
			if _, seen := seenMarkerDirs[dir]; seen {
				return nil
			}
			seenMarkerDirs[dir] = struct{}{}
			state, err := readEntryMarker(dir, secret)
			if err == nil {
				if _, keep := desiredDirs[dir]; !keep {
					staleMarkers = append(staleMarkers, state)
				}
			}
			return nil
		}
		if !strings.EqualFold(filepath.Ext(path), ".strm") {
			return nil
		}
		if _, keep := desired[filepath.Clean(path)]; keep {
			return nil
		}
		data, err := readRegularFile(path, maxStrmRead)
		if err != nil {
			return nil
		}
		if _, _, owned := strmurl.ParseOwnedURL(strings.TrimSpace(string(data)), secret); owned {
			stale = append(stale, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, path := range stale {
		if err := safepath.Remove(root, path); err != nil {
			report.addError(err)
			continue
		}
		report.Deleted++
		pruneEmptyStrmDirs(root, filepath.Dir(path))
	}
	for _, state := range staleMarkers {
		dir := filepath.Join(root, state.Directory)
		if err := verifyEntryMarker(dir, secret, state.InfoHash); err != nil {
			continue
		}
		if err := removeStrmMarkers(root, dir, strmEntryMarkerNames); err != nil {
			report.addError(err)
			continue
		}
		pruneEmptyStrmDirs(root, dir)
	}
	return nil
}

func (s *Strm) SweepAsync(reason string) bool {
	if !config.Get().Strm.Active() {
		return false
	}
	s.jobsMu.Lock()
	if s.sweepActive {
		s.sweepDirty = true
		s.sweepReason = reason
		s.jobsMu.Unlock()
		return true
	}
	s.sweepActive = true
	s.sweepReason = reason
	s.jobsMu.Unlock()
	if s.manager.startBackground("STRM sweep", func() {
		for {
			s.jobsMu.Lock()
			currentReason := s.sweepReason
			s.sweepDirty = false
			s.jobsMu.Unlock()
			report, err := s.Sweep(s.manager.ctx)
			if err != nil {
				s.logger.Warn().Err(err).Str("reason", currentReason).Msg("STRM sweep failed")
			} else {
				s.logger.Info().Str("reason", currentReason).
					Int("entries", report.Entries).Int("written", report.Written).
					Int("verified", report.Verified).Int("deleted", report.Deleted).
					Int("skipped", report.Skipped).Int("errors", len(report.Errors)).
					Msg("STRM sweep complete")
			}
			s.jobsMu.Lock()
			if s.sweepDirty && s.manager.ctx.Err() == nil {
				s.jobsMu.Unlock()
				continue
			}
			s.sweepActive = false
			s.jobsMu.Unlock()
			return
		}
	}) {
		return true
	}
	s.jobsMu.Lock()
	s.sweepActive = false
	s.jobsMu.Unlock()
	return false
}

func (s *Strm) SyncEntryAsync(infohash string) bool {
	if !config.Get().Strm.Active() || infohash == "" {
		return false
	}
	s.jobsMu.Lock()
	if _, running := s.entryJobs[infohash]; running {
		s.entryJobs[infohash] = true
		s.jobsMu.Unlock()
		return true
	}
	s.entryJobs[infohash] = false
	s.jobsMu.Unlock()
	if s.manager.startBackground("STRM entry sync", func() {
		for {
			s.syncEntry(infohash)
			s.jobsMu.Lock()
			if s.entryJobs[infohash] && s.manager.ctx.Err() == nil {
				s.entryJobs[infohash] = false
				s.jobsMu.Unlock()
				continue
			}
			delete(s.entryJobs, infohash)
			s.jobsMu.Unlock()
			return
		}
	}) {
		return true
	}
	s.jobsMu.Lock()
	delete(s.entryJobs, infohash)
	s.jobsMu.Unlock()
	return false
}

func (s *Strm) syncEntry(infohash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg := config.Get()
	if !cfg.Strm.Active() {
		return
	}
	root, err := s.ensureRoot(cfg)
	if err != nil {
		s.logger.Warn().Err(err).Str("entry", infohash).Msg("STRM entry sync failed")
		return
	}
	entry, err := s.manager.storage.Get(infohash)
	if err != nil {
		if !storage.IsEntryNotFound(err) {
			s.logger.Warn().Err(err).Str("entry", infohash).Msg("STRM entry lookup failed")
		}
		return
	}
	report := &StrmReport{}
	s.syncEntryLocked(s.manager.ctx, root, cfg, entry, report)
	for _, message := range report.Errors {
		s.logger.Warn().Str("entry", infohash).Msg(message)
	}
}

func (s *Strm) RemoveEntryAsync(entry *storage.Entry) bool {
	if !config.Get().Strm.Active() || entry == nil {
		return false
	}
	return s.manager.startBackground("STRM entry removal", func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		cfg := config.Get()
		root, err := s.ensureRoot(cfg)
		if err != nil {
			s.logger.Warn().Err(err).Str("entry", entry.InfoHash).Msg("STRM removal refused")
			return
		}
		dir := filepath.Join(root, entryDirectoryName(entry))
		if err := verifyEntryMarker(dir, cfg.Strm.Secret, entry.InfoHash); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				s.logger.Warn().Err(err).Str("entry", entry.InfoHash).Msg("STRM removal refused")
			}
			return
		}
		removedParents := make(map[string]struct{})
		_ = filepath.WalkDir(dir, func(path string, item fs.DirEntry, walkErr error) error {
			if walkErr != nil || item.IsDir() || !strings.EqualFold(filepath.Ext(path), ".strm") {
				return nil
			}
			data, readErr := readRegularFile(path, maxStrmRead)
			if readErr != nil {
				return nil
			}
			infohash, _, owned := strmurl.ParseOwnedURL(strings.TrimSpace(string(data)), cfg.Strm.Secret)
			if owned && infohash == entry.InfoHash {
				if err := safepath.Remove(root, path); err == nil {
					removedParents[filepath.Dir(path)] = struct{}{}
				}
			}
			return nil
		})
		parents := make([]string, 0, len(removedParents))
		for parent := range removedParents {
			parents = append(parents, parent)
		}
		sort.Slice(parents, func(i, j int) bool {
			return len(parents[i]) > len(parents[j])
		})
		for _, parent := range parents {
			pruneEmptyStrmDirs(root, parent)
		}
		_ = removeStrmMarkers(root, dir, strmEntryMarkerNames)
		pruneEmptyStrmDirs(root, dir)
	})
}

func verifyEntryMarker(dir, secret, infohash string) error {
	state, err := readEntryMarker(dir, secret)
	if err != nil {
		return err
	}
	if state.InfoHash != infohash {
		return fmt.Errorf("invalid STRM entry ownership marker in %q", dir)
	}
	return nil
}

func readEntryMarker(dir, secret string) (strmEntryState, error) {
	directory := filepath.Base(dir)
	data, err := readCompatibleStrmMarker(dir, strmEntryMarkerNames)
	if err != nil {
		return strmEntryState{}, err
	}
	var state strmEntryState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 ||
		state.Directory != directory ||
		!strmurl.Verify(secret, "entry-marker", state.Signature, state.InfoHash, directory) {
		return strmEntryState{}, fmt.Errorf("invalid STRM entry ownership marker in %q", dir)
	}
	return state, nil
}

func readCompatibleStrmMarker(dir string, names []string) ([]byte, error) {
	var contents []byte
	found := false
	for _, name := range names {
		data, err := readRegularFile(filepath.Join(dir, name), maxStrmRead)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if found && !bytes.Equal(contents, data) {
			return nil, fmt.Errorf("STRM ownership markers disagree in %q", dir)
		}
		contents = data
		found = true
	}
	if !found {
		return nil, os.ErrNotExist
	}
	return contents, nil
}

func isStrmMarkerName(name string, names []string) bool {
	for _, candidate := range names {
		if name == candidate {
			return true
		}
	}
	return false
}

func removeStrmMarkers(root, dir string, names []string) error {
	var removeErr error
	for _, name := range names {
		if err := safepath.Remove(root, filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return removeErr
}

func readRegularFile(path string, limit int64) ([]byte, error) {
	return safepath.ReadRegularFile(path, limit)
}

func atomicWrite(root, target string, content []byte) error {
	target, err := safepath.ValidateUnderRoot(root, target)
	if err != nil {
		return err
	}
	parent := filepath.Dir(target)
	if filepath.Clean(parent) != filepath.Clean(root) {
		if _, err := safepath.EnsureDir(root, parent, 0755); err != nil {
			return err
		}
	}
	id, err := storage.NewFileID()
	if err != nil {
		return err
	}
	temporary := filepath.Join(parent, ".tessarr-strm-"+id+".tmp")
	file, err := safepath.OpenFile(root, temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = safepath.Remove(root, temporary)
		}
	}()
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0644); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := safepath.Rename(root, temporary, target); err != nil {
		return fmt.Errorf("publish STRM file %q: %w", target, err)
	}
	removeTemporary = false
	return nil
}

func pruneEmptyStrmDirs(root, dir string) {
	root = filepath.Clean(root)
	for dir = filepath.Clean(dir); dir != root; dir = filepath.Dir(dir) {
		if _, err := safepath.ValidateUnderRoot(root, dir); err != nil {
			return
		}
		if err := safepath.Remove(root, dir); err != nil {
			return
		}
	}
}
