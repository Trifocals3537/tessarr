package manager

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
)

func TestValidateTorrentDownloadFolder(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "api")
	outside := t.TempDir()

	for _, test := range []struct {
		name      string
		requested string
		wantErr   bool
	}{
		{name: "configured root", requested: root},
		{name: "strict descendant", requested: child},
		{name: "outside root", requested: outside, wantErr: true},
		{name: "traversal escape", requested: filepath.Join(root, "..", filepath.Base(outside)), wantErr: true},
		{name: "empty", requested: "", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateTorrentDownloadFolder(root, test.requested)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateTorrentDownloadFolder() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateTorrentDownloadFolderRejectsSymlinkComponent(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated Windows privileges")
	}
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "redirect")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := validateTorrentDownloadFolder(root, filepath.Join(link, "child")); err == nil {
		t.Fatal("symlinked descendant was accepted")
	}
}

func TestValidateTorrentRootName(t *testing.T) {
	for _, test := range []struct {
		name       string
		value      string
		allowEmpty bool
		wantErr    bool
	}{
		{name: "release", value: "Movie.2026.1080p"},
		{name: "media extension", value: "Movie.mkv"},
		{name: "magnet can omit display name", allowEmpty: true},
		{name: "resolved name required", wantErr: true},
		{name: "slash", value: "../escape", wantErr: true},
		{name: "backslash", value: `season\episode`, wantErr: true},
		{name: "display punctuation", value: "Movie: Part Two"},
		{name: "display device name", value: "CON"},
		{name: "display trailing space", value: " Movie.mkv "},
		{name: "control", value: "Movie\n", wantErr: true},
		{name: "invalid utf8", value: "Movie\xff", wantErr: true},
		{name: "drive path", value: "C:movie", wantErr: true},
		{name: "dot traversal", value: "..", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateTorrentRootName(test.value, test.allowEmpty)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateTorrentRootName() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestValidateTorrentImportRequestCanonicalizesAndRejectsCategoryTraversal(t *testing.T) {
	root := t.TempDir()
	manager := &Manager{config: &config.Config{DownloadFolder: root}}
	req := &ImportRequest{
		DownloadFolder: filepath.Join(root, "api"),
		Magnet:         &utils.Magnet{InfoHash: "hash", Name: ""},
		Arr:            &arr.Arr{Name: "radarr"},
	}
	if err := manager.validateTorrentImportRequest(req); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(req.DownloadFolder) {
		t.Fatalf("DownloadFolder = %q, want canonical absolute path", req.DownloadFolder)
	}

	req.Arr.Name = ""
	if err := manager.validateTorrentImportRequest(req); err != nil {
		t.Fatalf("uncategorized request was rejected: %v", err)
	}
	if req.Arr.Name != "uncategorized" {
		t.Fatalf("empty category normalized to %q, want uncategorized", req.Arr.Name)
	}

	req.Arr.Name = "../escape"
	if err := manager.validateTorrentImportRequest(req); err == nil {
		t.Fatal("category traversal was accepted")
	}
}
