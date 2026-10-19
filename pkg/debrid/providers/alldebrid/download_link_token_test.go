package alldebrid

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/rs/zerolog"
)

func TestFetchDownloadLinkTracksDownloadAccountToken(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer download-token" {
			t.Errorf("Authorization = %q, want download account token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"link":"https://cdn.example/video","id":"download-id"}}`))
	}))
	defer server.Close()

	accounts := account.NewManager(config.Debrid{
		Name:            "alldebrid",
		DownloadAPIKeys: []string{"download-token"},
	}, nil, zerolog.Nop())
	provider := &AllDebrid{
		Host:                  server.URL,
		APIKey:                "main-token",
		autoExpiresLinksAfter: time.Hour,
		config:                config.Debrid{Name: "alldebrid"},
	}
	got, err := provider.fetchDownloadLink(
		accounts.Current(),
		"42",
		&types.File{Link: "https://restricted.example/video", Name: "video.mkv", Size: 4},
	)
	if err != nil {
		t.Fatalf("fetchDownloadLink() error = %v", err)
	}
	if got.Token != "download-token" {
		t.Fatalf("link token = %q, want owning download token", got.Token)
	}
}
