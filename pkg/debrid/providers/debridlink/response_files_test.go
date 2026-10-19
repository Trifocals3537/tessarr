package debridlink

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/request"
)

func TestDoGetRejectsTrailingJSON(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{} {}`))
	}))
	defer server.Close()

	client := &DebridLink{Host: server.URL, client: request.New()}
	var result map[string]any
	if _, err := client.doGet("/response", nil, &result); err == nil ||
		!strings.Contains(err.Error(), "multiple values") {
		t.Fatalf("doGet error = %v, want trailing JSON rejection", err)
	}
}

func TestFilesByLogicalNamePreservesNestedDuplicateBasenames(t *testing.T) {
	files, err := (&DebridLink{}).filesByLogicalName("dl", []torrentFile{
		{
			ID:          "1",
			Name:        "Release/Season 01/Episode.mkv",
			DownloadURL: "https://download.invalid/1",
			Size:        1024,
		},
		{
			ID:          "2",
			Name:        "Release/Season 02/Episode.mkv",
			DownloadURL: "https://download.invalid/2",
			Size:        2048,
		},
	})
	if err != nil {
		t.Fatal(err)
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
