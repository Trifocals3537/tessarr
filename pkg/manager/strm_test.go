package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	strmurl "github.com/Trifocals3537/tessarr/pkg/strm"
)

const managerTestStrmSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestStrmRootOwnershipFailsClosed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "keep.txt"), []byte("foreign"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Strm: config.Strm{Enabled: true, Path: root, Secret: managerTestStrmSecret}}
	reconciler := &Strm{}
	if _, err := reconciler.ensureRoot(cfg); err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("ensureRoot error = %v, want unowned-root refusal", err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "keep.txt")); err != nil || string(data) != "foreign" {
		t.Fatalf("foreign file changed: %q, %v", data, err)
	}
}

func TestStrmRootMarkerPinsSigningKey(t *testing.T) {
	root := filepath.Join(t.TempDir(), "library")
	reconciler := &Strm{}
	cfg := &config.Config{Strm: config.Strm{Enabled: true, Path: root, Secret: managerTestStrmSecret}}
	if _, err := reconciler.ensureRoot(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.ensureRoot(cfg); err != nil {
		t.Fatalf("same key did not reopen owned root: %v", err)
	}
	cfg.Strm.Secret = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if _, err := reconciler.ensureRoot(cfg); err == nil {
		t.Fatal("different signing key accepted an existing STRM root")
	}
}

func TestStrmAcceptsPreCutoverRootAndEntryMarkers(t *testing.T) {
	manager, cfg := newStrmSweepManager(t)
	entry := addStrmSweepEntry(t, manager)
	if _, err := manager.strm.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	entryDir := filepath.Join(cfg.Strm.Path, entryDirectoryName(entry))
	if err := os.Rename(
		filepath.Join(cfg.Strm.Path, strmRootMarker),
		filepath.Join(cfg.Strm.Path, legacyStrmRootMarker),
	); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(
		filepath.Join(entryDir, strmEntryMarker),
		filepath.Join(entryDir, legacyStrmEntryMarker),
	); err != nil {
		t.Fatal(err)
	}

	report, err := manager.strm.Sweep(context.Background())
	if err != nil {
		t.Fatalf("pre-cutover STRM markers were not accepted: %v", err)
	}
	if report.Written != 0 || report.Deleted != 0 || report.Verified != 1 || len(report.Errors) != 0 {
		t.Fatalf("pre-cutover marker sweep did not converge: %+v", report)
	}
	for _, path := range []string{
		filepath.Join(cfg.Strm.Path, legacyStrmRootMarker),
		filepath.Join(entryDir, legacyStrmEntryMarker),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("legacy marker %q was not preserved: %v", path, err)
		}
	}
}

func TestStrmRejectsConflictingMarkerAliases(t *testing.T) {
	root := filepath.Join(t.TempDir(), "library")
	reconciler := &Strm{}
	cfg := &config.Config{Strm: config.Strm{Enabled: true, Path: root, Secret: managerTestStrmSecret}}
	if _, err := reconciler.ensureRoot(cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, legacyStrmRootMarker), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.ensureRoot(cfg); err == nil || !strings.Contains(err.Error(), "markers disagree") {
		t.Fatalf("conflicting STRM markers error = %v", err)
	}
}

func TestWriteOwnedStrmPreservesForeignFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(root, "Movie.strm")
	if err := os.WriteFile(targetPath, []byte("https://foreign.example/movie"), 0644); err != nil {
		t.Fatal(err)
	}
	base, err := strmurl.BaseURL(&config.Config{AppURL: "https://media.example"})
	if err != nil {
		t.Fatal(err)
	}
	content, err := strmurl.FileURL(base, managerTestStrmSecret, "entry", "file", "Movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	_, err = writeOwnedStrm(root, managerTestStrmSecret, strmTarget{
		path: targetPath, content: content, entryID: "entry", fileID: "file",
	})
	if err == nil || !strings.Contains(err.Error(), "preserving foreign") {
		t.Fatalf("writeOwnedStrm error = %v", err)
	}
	data, readErr := os.ReadFile(targetPath)
	if readErr != nil || string(data) != "https://foreign.example/movie" {
		t.Fatalf("foreign STRM changed: %q, %v", data, readErr)
	}
}

func TestRemoveStaleDeletesOnlyAuthenticatedStrm(t *testing.T) {
	root := filepath.Join(t.TempDir(), "library")
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	base, err := strmurl.BaseURL(&config.Config{AppURL: "https://media.example"})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := strmurl.FileURL(base, managerTestStrmSecret, "entry", "file", "Movie.mkv")
	if err != nil {
		t.Fatal(err)
	}
	ownedPath := filepath.Join(root, "owned.strm")
	foreignPath := filepath.Join(root, "foreign.strm")
	if err := os.WriteFile(ownedPath, []byte(owned), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(foreignPath, []byte("https://foreign.example/movie"), 0644); err != nil {
		t.Fatal(err)
	}
	report := &StrmReport{}
	if err := (&Strm{}).removeStale(context.Background(), root, managerTestStrmSecret, nil, nil, report); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownedPath); !os.IsNotExist(err) {
		t.Fatalf("owned stale file still exists: %v", err)
	}
	if data, err := os.ReadFile(foreignPath); err != nil || string(data) != "https://foreign.example/movie" {
		t.Fatalf("foreign file changed: %q, %v", data, err)
	}
}

func newStrmSweepManager(t *testing.T) (*Manager, *config.Config) {
	t.Helper()
	oldPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(oldPath)
	})

	manager := New()
	t.Cleanup(func() {
		if err := manager.Stop(); err != nil {
			t.Errorf("stop manager: %v", err)
		}
	})
	cfg := config.Get()
	cfg.AppURL = "https://media.example/tessarr"
	cfg.Strm = config.Strm{
		Enabled: true,
		Path:    filepath.Join(t.TempDir(), "library"),
		Secret:  managerTestStrmSecret,
	}
	return manager, cfg
}

func addStrmSweepEntry(t *testing.T, manager *Manager) *storage.Entry {
	t.Helper()
	entry := &storage.Entry{
		Protocol:   config.ProtocolTorrent,
		InfoHash:   "aabbccddeeff00112233445566778899aabbccdd",
		Name:       "Movie.2023.1080p",
		IsComplete: true,
		Files: map[string]*storage.File{
			"Movie.2023.1080p.mkv": {
				Name: "Movie.2023.1080p.mkv", Size: 100,
			},
		},
	}
	if err := manager.storage.AddOrUpdateDurable(entry); err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestStrmSweepConvergesAndPreservesForeignFiles(t *testing.T) {
	manager, cfg := newStrmSweepManager(t)
	entry := addStrmSweepEntry(t, manager)

	if _, err := manager.strm.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(cfg.Strm.Path, entryDirectoryName(entry), "Movie.2023.1080p.strm")
	oldBase, err := strmurl.BaseURL(&config.Config{AppURL: "https://old.example"})
	if err != nil {
		t.Fatal(err)
	}
	staleContent, err := strmurl.FileURL(
		oldBase, cfg.Strm.Secret, entry.InfoHash, entry.Files["Movie.2023.1080p.mkv"].ID, "Movie.2023.1080p.mkv",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetPath, []byte(staleContent), 0o644); err != nil {
		t.Fatal(err)
	}

	orphanDir := filepath.Join(cfg.Strm.Path, "orphan")
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	orphanBase, err := strmurl.BaseURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	orphanContent, err := strmurl.FileURL(orphanBase, cfg.Strm.Secret, "deleted-entry", "deleted-file", "Gone.mkv")
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(orphanDir, "Gone.strm")
	if err := os.WriteFile(orphanPath, []byte(orphanContent), 0o644); err != nil {
		t.Fatal(err)
	}
	foreignPath := filepath.Join(cfg.Strm.Path, "foreign.strm")
	if err := os.WriteFile(foreignPath, []byte("plex://movie/12345"), 0o644); err != nil {
		t.Fatal(err)
	}

	report, err := manager.strm.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 0 || report.Written != 1 || report.Deleted != 1 {
		t.Fatalf("sweep report = %+v", report)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists: %v", err)
	}
	if data, err := os.ReadFile(foreignPath); err != nil || string(data) != "plex://movie/12345" {
		t.Fatalf("foreign file changed: %q, %v", data, err)
	}

	wantBase, err := strmurl.BaseURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want, err := strmurl.FileURL(
		wantBase, cfg.Strm.Secret, entry.InfoHash, entry.Files["Movie.2023.1080p.mkv"].ID, "Movie.2023.1080p.mkv",
	)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(targetPath); err != nil || strings.TrimSpace(string(data)) != want {
		t.Fatalf("generated STRM = %q, %v", data, err)
	}

	report, err = manager.strm.Sweep(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Written != 0 || report.Deleted != 0 || report.Verified != 1 || len(report.Errors) != 0 {
		t.Fatalf("second sweep did not converge: %+v", report)
	}
}

func TestStrmRemoveEntryPreservesForeignFiles(t *testing.T) {
	manager, cfg := newStrmSweepManager(t)
	entry := addStrmSweepEntry(t, manager)
	if _, err := manager.strm.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(cfg.Strm.Path, entryDirectoryName(entry))
	targetPath := filepath.Join(dir, "Movie.2023.1080p.strm")
	foreignPath := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(foreignPath, []byte("foreign"), 0o644); err != nil {
		t.Fatal(err)
	}

	if !manager.strm.RemoveEntryAsync(entry) {
		t.Fatal("entry removal was not scheduled")
	}
	manager.background.Wait()
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("owned STRM still exists: %v", err)
	}
	if data, err := os.ReadFile(foreignPath); err != nil || string(data) != "foreign" {
		t.Fatalf("foreign file changed: %q, %v", data, err)
	}
}

func TestStrmRemoveNestedEntryPrunesAndRegenerates(t *testing.T) {
	manager, cfg := newStrmSweepManager(t)
	entry := addStrmSweepEntry(t, manager)
	entry.Files = map[string]*storage.File{
		"Season 01/Episode 01.mkv": {
			Name: "Season 01/Episode 01.mkv",
			Size: 100,
		},
	}
	if err := manager.storage.AddOrUpdateDurable(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.strm.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(cfg.Strm.Path, entryDirectoryName(entry))
	file := entry.Files["Season 01/Episode 01.mkv"]
	target := filepath.Join(dir, mediaRelativePath(file.Name, file.ID, false))
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("nested STRM was not generated: %v", err)
	}

	if !manager.strm.RemoveEntryAsync(entry) {
		t.Fatal("entry removal was not scheduled")
	}
	manager.background.Wait()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("empty nested entry directory was retained: %v", err)
	}

	if _, err := manager.strm.Sweep(context.Background()); err != nil {
		t.Fatalf("regenerate removed nested entry: %v", err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("nested STRM was not regenerated: %v", err)
	}
}

func TestStrmGeneratedComponentsStayWithinPortableLimit(t *testing.T) {
	manager, _ := newStrmSweepManager(t)
	entry := addStrmSweepEntry(t, manager)
	entry.Name = strings.Repeat("Pokémon-", 50)

	entryName := entryDirectoryName(entry)
	if len(entryName) > safepath.PortableIdentifierMaxBytes {
		t.Fatalf("entry component length = %d, want <= %d", len(entryName), safepath.PortableIdentifierMaxBytes)
	}
	if err := safepath.ValidateIdentifier(entryName); err != nil {
		t.Fatalf("entry component is not portable: %v", err)
	}
	if entryName != entryDirectoryName(entry) {
		t.Fatal("entry component compaction is not deterministic")
	}

	relative := mediaRelativePath(
		strings.Repeat("Season-", 50)+"/"+strings.Repeat("Episode-", 50)+".mkv",
		"file-identity",
		false,
	)
	for _, component := range strings.Split(filepath.ToSlash(relative), "/") {
		if len(component) > safepath.PortableIdentifierMaxBytes {
			t.Fatalf("media component length = %d, want <= %d", len(component), safepath.PortableIdentifierMaxBytes)
		}
		if err := safepath.ValidateIdentifier(component); err != nil {
			t.Fatalf("media component %q is not portable: %v", component, err)
		}
	}
	if filepath.Ext(relative) != ".strm" {
		t.Fatalf("media path extension = %q, want .strm", filepath.Ext(relative))
	}

	first, err := disambiguateStrmPath(relative, "aaaaaaaa11111111")
	if err != nil {
		t.Fatal(err)
	}
	second, err := disambiguateStrmPath(relative, "bbbbbbbb22222222")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("collision disambiguation aliased %q", first)
	}
	for _, candidate := range []string{first, second} {
		base := filepath.Base(candidate)
		if len(base) > safepath.PortableIdentifierMaxBytes {
			t.Fatalf("disambiguated component length = %d, want <= %d", len(base), safepath.PortableIdentifierMaxBytes)
		}
		if filepath.Ext(base) != ".strm" {
			t.Fatalf("disambiguated extension = %q, want .strm", filepath.Ext(base))
		}
	}
}
