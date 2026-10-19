package torbox

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/rs/zerolog"
)

func TestDeleteTorrentUsesOfficialControlContract(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/api/torrents/controltorrent" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var body struct {
			TorrentID int    `json:"torrent_id"`
			Operation string `json:"operation"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if body.TorrentID != 42 || body.Operation != "delete" {
			t.Errorf("body = %+v", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &Torbox{
		Host:   server.URL + "/v1",
		client: request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
		logger: zerolog.Nop(),
	}
	if err := client.DeleteTorrent("42"); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteTorrent("not-a-number"); err == nil {
		t.Fatal("nonnumeric ID was accepted")
	}
	if calls.Load() != 1 {
		t.Fatalf("request calls = %d, want 1", calls.Load())
	}
}

func TestDeleteTorrentDistinguishesNotFoundFromUnauthorized(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	for _, test := range []struct {
		name      string
		status    int
		wantGone  bool
		wantError bool
	}{
		{name: "not found", status: http.StatusNotFound, wantGone: true, wantError: true},
		{name: "unauthorized", status: http.StatusUnauthorized, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			client := &Torbox{
				Host:   server.URL,
				client: request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
				logger: zerolog.Nop(),
			}
			err := client.DeleteTorrent("42")
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
			if errors.Is(err, customerror.TorrentNotFoundError) != test.wantGone {
				t.Fatalf("error = %v, typed not-found = %v", err, test.wantGone)
			}
		})
	}
}

func TestUpdateTorrentPreservesNestedFilePath(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/api/torrents/mylist" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("id"); got != "42" {
			t.Errorf("id = %q, want 42", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"id":                42,
				"name":              "Example",
				"hash":              "abc",
				"download_state":    "completed",
				"download_finished": true,
				"files": []map[string]any{
					{
						"id":            7,
						"name":          "Season 01/Episode 01.mkv",
						"absolute_path": "Season 01/Episode 01.mkv",
						"size":          1024,
					},
				},
			},
		})
	}))
	defer server.Close()

	client := &Torbox{
		Host:   server.URL + "/v1",
		client: request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "torbox"},
	}
	torrent := &types.Torrent{Id: "42"}
	if err := client.UpdateTorrent(torrent); err != nil {
		t.Fatal(err)
	}
	file, ok := torrent.Files["Episode 01.mkv"]
	if !ok {
		t.Fatalf("files = %#v", torrent.Files)
	}
	if file.Path != "Season 01/Episode 01.mkv" {
		t.Fatalf("path = %q, want nested provider path", file.Path)
	}
}
