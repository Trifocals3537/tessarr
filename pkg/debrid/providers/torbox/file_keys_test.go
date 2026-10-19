package torbox

import (
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestTorboxFilesByLogicalNamePreservesNestedDuplicateBasenames(t *testing.T) {
	files, err := torboxFilesByLogicalName([]types.File{
		{Id: "11", Path: "Release/Season 01/Episode.mkv"},
		{Id: "12", Path: "Release/Season 02/Episode.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 2 {
		t.Fatalf("file count = %d, want 2", len(files))
	}
	wantPaths := map[string]bool{"Release/Season 01/Episode.mkv": true, "Release/Season 02/Episode.mkv": true}
	for name, file := range files {
		if file.Name != name || strings.ContainsAny(name, `/\`) || !wantPaths[file.Path] {
			t.Fatalf("logical file %q = %#v", name, file)
		}
		delete(wantPaths, file.Path)
	}
	if len(wantPaths) != 0 {
		t.Fatalf("missing provider paths: %#v", wantPaths)
	}
}

func TestTorboxFilesByLogicalNameKeepsUniqueBasenameCompatibility(t *testing.T) {
	files, err := torboxFilesByLogicalName([]types.File{
		{Id: "21", Path: `Release\Season 01\Unique.mkv`},
	})
	if err != nil {
		t.Fatal(err)
	}

	file, ok := files["Unique.mkv"]
	if !ok {
		t.Fatalf("unique basename key missing in %#v", files)
	}
	if file.Name != "Unique.mkv" || file.Path != "Release/Season 01/Unique.mkv" {
		t.Fatalf("unique file = %#v", file)
	}
}

func TestTorboxFilesByLogicalNameRejectsDuplicateProviderPath(t *testing.T) {
	_, err := torboxFilesByLogicalName([]types.File{
		{Id: "31", Path: "Release/Episode.mkv"},
		{Id: "32", Path: "Release/Episode.mkv"},
	})
	if err == nil {
		t.Fatal("duplicate provider path was accepted")
	}
}
