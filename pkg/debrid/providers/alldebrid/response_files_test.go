package alldebrid

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/request"
)

func TestDoRequestRejectsTrailingJSON(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{} {}`))
	}))
	defer server.Close()

	client := &AllDebrid{Host: server.URL, client: request.New()}
	var result map[string]any
	if _, err := client.doRequest("/response", nil, &result); err == nil ||
		!strings.Contains(err.Error(), "multiple values") {
		t.Fatalf("doRequest error = %v, want trailing JSON rejection", err)
	}
}

func TestFlattenFilesPreservesNestedDuplicateBasenames(t *testing.T) {
	files, err := (&AllDebrid{}).flattenFiles("ad", []MagnetFile{
		{
			Name: "Season 01",
			Elements: []MagnetFile{{
				Name: "Episode.mkv",
				Size: 1024,
				Link: "https://download.invalid/1",
			}},
		},
		{
			Name: "Season 02",
			Elements: []MagnetFile{{
				Name: "Episode.mkv",
				Size: 2048,
				Link: "https://download.invalid/2",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := map[string]bool{"Season 01/Episode.mkv": true, "Season 02/Episode.mkv": true}
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

func TestFlattenFilesUsesStablePathIdentityAcrossOrdering(t *testing.T) {
	first := []MagnetFile{
		{Name: "Season 01", Elements: []MagnetFile{{Name: "Episode.mkv", Size: 1024, Link: "https://download.invalid/1"}}},
		{Name: "Season 02", Elements: []MagnetFile{{Name: "Episode.mkv", Size: 2048, Link: "https://download.invalid/2"}}},
	}
	second := []MagnetFile{first[1], first[0]}
	firstFiles, err := (&AllDebrid{}).flattenFiles("ad", first)
	if err != nil {
		t.Fatal(err)
	}
	secondFiles, err := (&AllDebrid{}).flattenFiles("ad", second)
	if err != nil {
		t.Fatal(err)
	}
	firstIDs := make(map[string]string)
	for _, file := range firstFiles {
		firstIDs[file.Path] = file.Id
		if !strings.HasPrefix(file.Id, "path-") {
			t.Fatalf("file %q has unstable identity %q", file.Path, file.Id)
		}
	}
	for _, file := range secondFiles {
		if file.Id != firstIDs[file.Path] {
			t.Fatalf("file %q identity changed with API order: first=%q second=%q", file.Path, firstIDs[file.Path], file.Id)
		}
	}
}

func TestFlattenFilesRejectsExcessiveNesting(t *testing.T) {
	tree := MagnetFile{Name: "Episode.mkv", Size: 1, Link: "https://download.invalid"}
	for depth := 0; depth <= allDebridFileTreeMaxDepth; depth++ {
		tree = MagnetFile{Name: "folder", Elements: []MagnetFile{tree}}
	}
	if _, err := (&AllDebrid{}).flattenFiles("ad", []MagnetFile{tree}); err == nil {
		t.Fatal("expected excessive AllDebrid nesting to be rejected")
	}
}
