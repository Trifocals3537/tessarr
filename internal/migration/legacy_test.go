package migration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"google.golang.org/protobuf/proto"
)

func TestMigrateCopiesVerifiesAndRenamesState(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, ".decypharr")
	target := filepath.Join(root, ".tessarr")
	mustWriteFile(t, filepath.Join(source, "config.json"), []byte(`{"name":"Decypharr remains valid user data"}`), 0o600)
	mustWriteFile(t, filepath.Join(source, "logs", "decypharr.log"), []byte("ready\n"), 0o640)
	mustWriteFile(t, filepath.Join(source, ".decypharr-cache-owner"), []byte("owner"), 0o600)
	mustWriteFile(t, filepath.Join(source, ".decypharr-torrent-quarantine-123"), []byte("held"), 0o600)
	mustWriteFile(t, filepath.Join(source, ".decypharr-strm-root"), []byte("root"), 0o600)

	result, err := Migrate(Options{Source: source, Target: target})
	if err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if result.Files != 5 || result.RenamedEntries != 4 || result.AlreadyMigrated || result.DryRun {
		t.Fatalf("Migrate() result = %+v", result)
	}
	assertFileContent(t, filepath.Join(source, "logs", "decypharr.log"), "ready\n")
	assertFileContent(t, filepath.Join(target, "logs", "tessarr.log"), "ready\n")
	assertFileContent(t, filepath.Join(target, ".tessarr-cache-owner"), "owner")
	assertFileContent(t, filepath.Join(target, ".tessarr-torrent-quarantine-123"), "held")
	assertFileContent(t, filepath.Join(target, ".tessarr-strm-root"), "root")
	assertFileContent(t, filepath.Join(target, "config.json"), `{"name":"Decypharr remains valid user data"}`)
	if _, err := os.Stat(filepath.Join(target, receiptName)); err != nil {
		t.Fatalf("migration receipt missing: %v", err)
	}

	again, err := Migrate(Options{Source: source, Target: target})
	if err != nil {
		t.Fatalf("idempotent Migrate() error = %v", err)
	}
	if !again.AlreadyMigrated {
		t.Fatalf("idempotent Migrate() result = %+v", again)
	}
}

func TestPromoteStagedStateSyncsBeforeAndAfterRename(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	var synced []string
	promoted, err := promoteStagedState(stage, target, func(path string) error {
		synced = append(synced, path)
		if path == stage {
			if _, err := os.Lstat(target); !os.IsNotExist(err) {
				t.Fatalf("target existed before staging directory sync: %v", err)
			}
		} else if _, err := os.Stat(target); err != nil {
			t.Fatalf("target missing before parent directory sync: %v", err)
		}
		return nil
	})
	if err != nil || !promoted || len(synced) != 2 || synced[0] != stage || synced[1] != root {
		t.Fatalf("promotion = %t, %v; sync order = %v", promoted, err, synced)
	}

	stage = filepath.Join(root, "stage-failure")
	target = filepath.Join(root, "target-failure")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	promoted, err = promoteStagedState(stage, target, func(string) error { return os.ErrPermission })
	if promoted || err == nil {
		t.Fatalf("failed pre-rename sync promoted state: promoted=%t err=%v", promoted, err)
	}
	if _, err := os.Lstat(stage); err != nil {
		t.Fatalf("stage removed on pre-rename sync failure: %v", err)
	}

	promoted, err = promoteStagedState(stage, target, func(path string) error {
		if path == root {
			return os.ErrPermission
		}
		return nil
	})
	if !promoted || err == nil {
		t.Fatalf("post-rename sync failure hid promoted state: promoted=%t err=%v", promoted, err)
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target removed on post-rename sync failure: %v", err)
	}
}

func TestMigrateDryRunMakesNoChanges(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(source, "decypharr.log"), []byte("log"), 0o600)

	result, err := Migrate(Options{Source: source, Target: target, DryRun: true})
	if err != nil {
		t.Fatalf("Migrate(dry-run) error = %v", err)
	}
	if !result.DryRun || result.Files != 1 || result.RenamedEntries != 1 {
		t.Fatalf("Migrate(dry-run) result = %+v", result)
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatalf("dry run created target: %v", err)
	}
}

func TestVerifyTreeIgnoresDirectoryAllocationSize(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "nested", "episode.mkv"), []byte("media"), 0o600)
	expected, err := scanTarget(root)
	if err != nil {
		t.Fatal(err)
	}
	changedDirectory := false
	for index := range expected {
		if expected[index].Mode.IsDir() {
			expected[index].Size += 4096
			changedDirectory = true
		}
	}
	if !changedDirectory {
		t.Fatal("test tree did not contain a directory manifest entry")
	}
	if err := verifyTree(root, expected); err != nil {
		t.Fatalf("verifyTree() rejected equivalent directory contents: %v", err)
	}
	actual, err := scanTarget(root)
	if err != nil {
		t.Fatal(err)
	}
	if digestManifest(actual) != digestManifest(expected) {
		t.Fatal("stable manifest digest changed with directory allocation size")
	}
	if digestManifestV2(actual) == digestManifestV2(expected) {
		t.Fatal("legacy digest fixture did not capture the directory-size difference")
	}
}

func TestMigrateAcceptsExactVersionTwoReceipt(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(source, "nested", "config.json"), []byte("{}"), 0o600)

	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	sourceEntries, err := buildManifest(source)
	if err != nil {
		t.Fatal(err)
	}
	targetEntries, err := scanTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	legacy := receipt{
		SchemaVersion: legacyReceiptVersionV2,
		SourceDigest:  digestManifestV2(sourceEntries),
		TargetDigest:  digestManifestV2(targetEntries),
	}
	data, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(target, receiptName), data, 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := Migrate(Options{Source: source, Target: target})
	if err != nil {
		t.Fatalf("Migrate() rejected a valid version-two receipt: %v", err)
	}
	if !result.AlreadyMigrated {
		t.Fatalf("Migrate() result = %+v, want existing migration", result)
	}
	if err := os.WriteFile(filepath.Join(target, "nested", "config.json"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(Options{Source: source, Target: target}); err == nil ||
		!strings.Contains(err.Error(), "cannot be safely verified") {
		t.Fatalf("tampered version-two target error = %v", err)
	}
}

func TestMigrateRejectsDriftedVersionTwoReceiptWithoutDryRunWrites(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(source, "nested", "config.json"), []byte("{}"), 0o600)

	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	sourceEntries, err := buildManifest(source)
	if err != nil {
		t.Fatal(err)
	}
	targetEntries, err := scanTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	for index := range sourceEntries {
		if sourceEntries[index].Mode.IsDir() {
			sourceEntries[index].Size += 4096
		}
	}
	for index := range targetEntries {
		if targetEntries[index].Mode.IsDir() {
			targetEntries[index].Size += 8192
		}
	}
	legacy := receipt{
		SchemaVersion: legacyReceiptVersionV2,
		SourceDigest:  digestManifestV2(sourceEntries),
		TargetDigest:  digestManifestV2(targetEntries),
	}
	data, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(target, receiptName), data, 0o600); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(Options{Source: source, Target: target, DryRun: true}); err == nil ||
		!strings.Contains(err.Error(), "cannot be safely verified") {
		t.Fatalf("drifted version-two receipt error = %v", err)
	}
	after, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("dry run changed migration parent entries: before=%d after=%d", len(before), len(after))
	}
	for index := range before {
		if before[index].Name() != after[index].Name() {
			t.Fatalf("dry run changed migration parent entries: before=%q after=%q", before[index].Name(), after[index].Name())
		}
	}
}

func TestMigrateRebasesOnlyContainedConfigurationPaths(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	external := filepath.Join(root, "external-media")
	configuration := map[string]any{
		"download_folder": filepath.Join(source, "downloads"),
		"usenet": map[string]any{
			"disk_buffer_path": filepath.Join(source, "buffer"),
		},
		"mount": map[string]any{
			"mount_path": external,
		},
	}
	data, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(source, "config.json"), data, 0o600)

	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	migrated, err := os.ReadFile(filepath.Join(target, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(migrated, &got); err != nil {
		t.Fatal(err)
	}
	if got["download_folder"] != filepath.Join(target, "downloads") {
		t.Fatalf("download folder = %q", got["download_folder"])
	}
	usenet := got["usenet"].(map[string]any)
	if usenet["disk_buffer_path"] != filepath.Join(target, "buffer") {
		t.Fatalf("disk buffer path = %q", usenet["disk_buffer_path"])
	}
	mount := got["mount"].(map[string]any)
	if mount["mount_path"] != external {
		t.Fatalf("external mount path changed to %q", mount["mount_path"])
	}

	if err := os.WriteFile(filepath.Join(target, "config.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(Options{Source: source, Target: target}); err == nil ||
		!strings.Contains(err.Error(), "receipt verification") {
		t.Fatalf("tampered rebased target error = %v", err)
	}
}

func TestMigrateRebasesPersistedEntryPaths(t *testing.T) {
	root := t.TempDir()
	previousConfigPath := config.GetMainPath()
	config.Reset()
	if err := config.SetConfigPath(filepath.Join(root, "runtime-config")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		config.Reset()
		if err := config.SetConfigPath(previousConfigPath); err != nil {
			t.Errorf("restore config path: %v", err)
		}
	})

	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(source, "config.json"), []byte("{}"), 0o600)
	store, err := storage.NewStorage(filepath.Join(source, "db"))
	if err != nil {
		t.Fatal(err)
	}
	entry := &storage.Entry{
		Protocol:    config.ProtocolTorrent,
		InfoHash:    "migration-entry",
		Name:        "Migration Entry",
		SavePath:    filepath.Join(source, "downloads"),
		ContentPath: filepath.Join(source, "downloads", "Migration Entry"),
		Files:       map[string]*storage.File{},
		Providers:   map[string]*storage.ProviderEntry{},
	}
	if err := store.AddOrUpdateDurable(entry); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	migrated, err := storage.NewStorage(filepath.Join(target, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	got, err := migrated.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if got.SavePath != filepath.Join(target, "downloads") ||
		got.ContentPath != filepath.Join(target, "downloads", "Migration Entry") {
		t.Fatalf("migrated entry paths = %#v", got)
	}
}

func TestMigrateRebasesPersistedNZBSourcePath(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	id := "123e4567-e89b-12d3-a456-426614174000"
	sourceNZB := filepath.Join(source, "usenet", "nzbs", id+".nzb")
	mustWriteFile(t, sourceNZB, []byte("nzb"), 0o600)
	encoded, err := proto.Marshal(&usenet.NZBProto{Id: id, Name: "Episode", Path: sourceNZB})
	if err != nil {
		t.Fatal(err)
	}
	mustWriteFile(t, filepath.Join(source, "usenet", "meta", id+".meta"), encoded, 0o644)
	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	previous := config.GetMainPath()
	if err := config.SetConfigPath(target); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := config.SetConfigPath(previous); err != nil {
			t.Errorf("restore configuration path: %v", err)
		}
	})
	metaStore, err := usenet.NewNZBStorage()
	if err != nil {
		t.Fatal(err)
	}
	nzb, err := metaStore.GetNZB(id)
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(target, "usenet", "nzbs", id+".nzb"); nzb.Path != want {
		t.Fatalf("migrated NZB path = %q, want %q", nzb.Path, want)
	}
}

func TestMigrateRejectsTransformedCollision(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	mustWriteFile(t, filepath.Join(source, ".decypharr-cache-owner"), []byte("old"), 0o600)
	mustWriteFile(t, filepath.Join(source, ".tessarr-cache-owner"), []byte("new"), 0o600)

	_, err := Migrate(Options{Source: source, Target: filepath.Join(root, "new")})
	if err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("Migrate() error = %v, want collision", err)
	}
}

func TestMigrateRejectsReservedReceiptSourceName(t *testing.T) {
	t.Parallel()

	for _, name := range []string{receiptName, ".TESSARR-MIGRATION.JSON"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "old")
			target := filepath.Join(root, "new")
			mustWriteFile(t, filepath.Join(source, name), []byte("user data"), 0o600)

			_, err := Migrate(Options{Source: source, Target: target})
			if err == nil || !strings.Contains(err.Error(), "reserves target name") {
				t.Fatalf("Migrate() error = %v, want reserved receipt rejection", err)
			}
			if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
				t.Fatalf("reserved-name rejection created target: %v", statErr)
			}
		})
	}
}

func TestMigrateRejectsSymlinkedExistingReceipt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows test users may not have symlink permission")
	}
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(source, "config.json"), []byte("{}"), 0o600)
	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	receiptPath := filepath.Join(target, receiptName)
	receiptData, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "outside-receipt.json")
	mustWriteFile(t, outside, receiptData, 0o600)
	if err := os.Remove(receiptPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, receiptPath); err != nil {
		t.Fatal(err)
	}

	_, err = Migrate(Options{Source: source, Target: target})
	if err == nil || !strings.Contains(err.Error(), "receipt is not a regular file") {
		t.Fatalf("Migrate() error = %v, want symlinked receipt rejection", err)
	}
}

func TestMigrateRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows test users may not have symlink permission")
	}
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	mustWriteFile(t, filepath.Join(source, "outside"), []byte("secret"), 0o600)
	if err := os.Symlink(filepath.Join(source, "outside"), filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}

	_, err := Migrate(Options{Source: source, Target: filepath.Join(root, "new")})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Migrate() error = %v, want symlink rejection", err)
	}
}

func TestMigratePopulatesReadOnlyDirectoriesBeforeRestoringModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not preserve Unix permission bits")
	}
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	target := filepath.Join(root, "new")
	t.Cleanup(func() {
		for _, path := range []string{
			filepath.Join(source, "readonly"),
			filepath.Join(source, "readonly", "nested"),
			filepath.Join(target, "readonly"),
			filepath.Join(target, "readonly", "nested"),
		} {
			_ = os.Chmod(path, 0o700)
		}
	})
	nested := filepath.Join(source, "readonly", "nested")
	mustWriteFile(t, filepath.Join(nested, "state.bin"), []byte("state"), 0o640)
	if err := os.Chmod(nested, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Dir(nested), 0o555); err != nil {
		t.Fatal(err)
	}

	if _, err := Migrate(Options{Source: source, Target: target}); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"readonly", filepath.Join("readonly", "nested")} {
		info, err := os.Stat(filepath.Join(target, relative))
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o555 {
			t.Fatalf("mode for %q = %o, want 555", relative, got)
		}
	}
	info, err := os.Stat(filepath.Join(target, "readonly", "nested", "state.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("file mode = %o, want 640", got)
	}
}

func TestMigrateRejectsSymlinkedTargetParent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows test users may not have symlink permission")
	}
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	realParent := filepath.Join(root, "real")
	mustWriteFile(t, filepath.Join(source, "config.json"), []byte("{}"), 0o600)
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedParent := filepath.Join(root, "linked")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		t.Fatal(err)
	}

	_, err := Migrate(Options{Source: source, Target: filepath.Join(linkedParent, "new")})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Migrate() error = %v, want target-parent symlink rejection", err)
	}
}

func TestMigrateRejectsNestedRootsAndUnverifiedTarget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	mustWriteFile(t, filepath.Join(source, "config.json"), []byte("{}"), 0o600)

	if _, err := Migrate(Options{Source: source, Target: filepath.Join(source, "new")}); err == nil {
		t.Fatal("Migrate() accepted nested target")
	}

	target := filepath.Join(root, "new")
	mustWriteFile(t, filepath.Join(target, "existing"), []byte("preserve"), 0o600)
	if _, err := Migrate(Options{Source: source, Target: target}); err == nil {
		t.Fatal("Migrate() accepted unverified existing target")
	}
	assertFileContent(t, filepath.Join(target, "existing"), "preserve")
}

func mustWriteFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal(err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(content) != want {
		t.Fatalf("ReadFile(%q) = %q, want %q", path, content, want)
	}
}
