package realdebrid

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/common/rar"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func storedRARFile(path string, offset, size int64) *rar.File {
	return &rar.File{
		Path:           path,
		Size:           size,
		CompressedSize: size,
		Method:         rar.MethodStore,
		DataOffset:     offset,
	}
}

func TestHandleRarFallbackPlansPortableOutputPath(t *testing.T) {
	provider := &RealDebrid{}
	files, err := provider.handleRarFallback(
		&types.Torrent{Id: "torrent", Name: "Movie: Part?"},
		torrentInfo{Bytes: 123, Links: []string{"restricted"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("fallback files = %#v", files)
	}
	for _, file := range files {
		if file.Path != "Movie: Part?.rar" || file.OutputPath == "" || file.OutputPath == file.Path {
			t.Fatalf("fallback path planning = %#v", file)
		}
		if !strings.HasSuffix(file.OutputPath, ".rar") {
			t.Fatalf("fallback extension was not preserved: %q", file.OutputPath)
		}
	}
}

func TestMapStoredRARFilesUsesValidatedArchiveMetadata(t *testing.T) {
	generated := time.Unix(100, 0)
	selected := []types.File{{
		TorrentId: "torrent", Id: "7", Name: "Video?.mkv", Path: "Release/Video?.mkv", Size: 999,
	}}

	files, err := mapStoredRARFiles(
		selected,
		[]*rar.File{storedRARFile("Archive/Video_.mkv", 10, 4)},
		"https://restricted.example/archive",
		generated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("mapped files = %#v, want one", files)
	}
	var name string
	var file types.File
	for name, file = range files {
	}
	if strings.ContainsAny(name, `/\?*`) || !strings.HasSuffix(name, ".mkv") ||
		file.Name != name || file.Path != "Release/Video?.mkv" ||
		!strings.HasSuffix(file.OutputPath, "/"+name) ||
		file.Size != 4 || !file.IsRar || file.ByteRange == nil ||
		*file.ByteRange != [2]int64{10, 13} || file.Link != "https://restricted.example/archive" ||
		!file.Generated.Equal(generated) {
		t.Fatalf("mapped file %q = %#v", name, file)
	}
	if selected[0].Size != 999 || selected[0].ByteRange != nil || selected[0].IsRar {
		t.Fatalf("input selection was mutated: %#v", selected[0])
	}
}

func TestMapStoredRARFilesFailsClosedOnUnsafeMappings(t *testing.T) {
	tests := []struct {
		name          string
		selected      []types.File
		rarFiles      []*rar.File
		wantError     error
		wantSubstring string
	}{
		{
			name:     "compressed entry",
			selected: []types.File{{Name: "video.mkv", Size: 8}},
			rarFiles: []*rar.File{{
				Path: "video.mkv", Size: 8, CompressedSize: 4, Method: 0x33, DataOffset: 10,
			}},
			wantError: rar.ErrCompressionNotSupported,
		},
		{
			name:          "incomplete match",
			selected:      []types.File{{Name: "one.mkv"}, {Name: "two.mkv"}},
			rarFiles:      []*rar.File{storedRARFile("one.mkv", 10, 4)},
			wantSubstring: "matched 1 of 2",
		},
		{
			name:          "duplicate archive basename",
			selected:      []types.File{{Name: "video.mkv"}},
			rarFiles:      []*rar.File{storedRARFile("one/video.mkv", 10, 4), storedRARFile("two/video.mkv", 20, 4)},
			wantSubstring: "ambiguous archive path match",
		},
		{
			name:          "ambiguous selected safe basename",
			selected:      []types.File{{Name: "Video?.mkv"}, {Name: "Video*.mkv"}},
			rarFiles:      []*rar.File{storedRARFile("Video_.mkv", 10, 4)},
			wantSubstring: "same archive entry",
		},
		{
			name:          "no selected match",
			selected:      []types.File{{Name: "video.mkv"}},
			rarFiles:      []*rar.File{storedRARFile("other.mkv", 10, 4)},
			wantSubstring: "no directly streamable selected files",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files, err := mapStoredRARFiles(test.selected, test.rarFiles, "restricted", time.Time{})
			if err == nil || files != nil {
				t.Fatalf("mapStoredRARFiles() = %#v, %v; want failure", files, err)
			}
			if test.wantError != nil && !errors.Is(err, test.wantError) {
				t.Fatalf("error = %v, want %v", err, test.wantError)
			}
			if test.wantSubstring != "" && !strings.Contains(err.Error(), test.wantSubstring) {
				t.Fatalf("error = %v, want substring %q", err, test.wantSubstring)
			}
		})
	}
}

func TestMapStoredRARFilesUsesDeepestUniquePathSuffix(t *testing.T) {
	selected := []types.File{
		{
			TorrentId: "torrent", Id: "1", Name: "Episode.mkv",
			Path: "Release/Season 01/Episode.mkv", Size: 999,
		},
		{
			TorrentId: "torrent", Id: "2", Name: "Episode.mkv",
			Path: "Release/Season 02/Episode.mkv", Size: 999,
		},
	}
	rars := []*rar.File{
		storedRARFile("Archive/Season 02/Episode.mkv", 20, 4),
		storedRARFile("Archive/Season 01/Episode.mkv", 10, 4),
		storedRARFile("Extras/Episode.mkv", 30, 4),
	}

	files, err := mapStoredRARFiles(selected, rars, "restricted", time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	wants := map[string][2]int64{
		"Release/Season 01/Episode.mkv": {10, 13},
		"Release/Season 02/Episode.mkv": {20, 23},
	}
	if len(files) != len(wants) {
		t.Fatalf("mapped files = %#v, want %d", files, len(wants))
	}
	for name, file := range files {
		wantRange, exists := wants[file.Path]
		if !exists || strings.ContainsAny(name, `/\`) || file.ByteRange == nil ||
			*file.ByteRange != wantRange || file.Name != name {
			t.Fatalf("mapped file %q = %#v, pathExists=%v", name, file, exists)
		}
		delete(wants, file.Path)
	}
	if len(wants) != 0 {
		t.Fatalf("missing mapped provider paths: %#v", wants)
	}
}

func TestMapStoredRARFilesRejectsUnsafeOrConflictingPaths(t *testing.T) {
	tests := []struct {
		name      string
		selected  []types.File
		rarFiles  []*rar.File
		wantError string
	}{
		{
			name:      "selected traversal",
			selected:  []types.File{{Name: "video.mkv", Path: "../video.mkv"}},
			rarFiles:  []*rar.File{storedRARFile("video.mkv", 10, 4)},
			wantError: "traverses outside",
		},
		{
			name:      "archive traversal",
			selected:  []types.File{{Name: "video.mkv"}},
			rarFiles:  []*rar.File{storedRARFile("../video.mkv", 10, 4)},
			wantError: "traverses outside",
		},
		{
			name: "duplicate selected route",
			selected: []types.File{
				{Name: "video.mkv", Path: "Release/video.mkv"},
				{Name: "video.mkv", Path: "release/VIDEO.MKV"},
			},
			rarFiles:  []*rar.File{storedRARFile("release/video.mkv", 10, 4)},
			wantError: "same archive entry",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			files, err := mapStoredRARFiles(test.selected, test.rarFiles, "restricted", time.Time{})
			if err == nil || files != nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("mapStoredRARFiles() = %#v, %v; want %q", files, err, test.wantError)
			}
		})
	}
}
