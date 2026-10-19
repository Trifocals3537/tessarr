package torbox

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	json "github.com/bytedance/sonic"
	"github.com/rs/zerolog"
)

func TestDoGetRejectsTrailingJSON(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{} {}`))
	}))
	defer server.Close()

	client := &Torbox{Host: server.URL, client: request.New()}
	var result map[string]any
	if _, err := client.doGet("/response", nil, &result); err == nil ||
		!strings.Contains(err.Error(), "multiple values") {
		t.Fatalf("doGet error = %v, want trailing JSON rejection", err)
	}
}

func TestUpdateTorrentRejectsUnsuccessfulEnvelopeWithoutExposingDetail(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"success":false,
			"error":"https://download.invalid/object?token=do-not-log",
			"data":null
		}`))
	}))
	defer server.Close()

	client := &Torbox{Host: server.URL, client: request.New(request.WithMaxRetries(0))}
	err := client.UpdateTorrent(&types.Torrent{Id: "1"})
	if err == nil || strings.Contains(err.Error(), "do-not-log") {
		t.Fatalf("UpdateTorrent error = %v", err)
	}
}

func TestGetTorrentsReturnsOnlyCompleteBoundedSnapshots(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	newClient := func(handler http.HandlerFunc) (*Torbox, func()) {
		server := httptest.NewServer(handler)
		client := &Torbox{
			Host:   server.URL,
			client: request.New(request.WithMaxRetries(0)),
			logger: zerolog.Nop(),
		}
		return client, server.Close
	}

	t.Run("complete pages", func(t *testing.T) {
		client, closeServer := newClient(func(w http.ResponseWriter, r *http.Request) {
			if got := r.URL.Query().Get("bypass_cache"); got != "true" {
				t.Errorf("bypass_cache = %q, want true", got)
			}
			if got := r.URL.Query().Get("limit"); got != "1" {
				t.Errorf("limit = %q, want 1", got)
			}
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			switch offset {
			case 0:
				writeTorboxTorrentPage(t, w, 1)
			case 1:
				writeTorboxTorrentPage(t, w, 2)
			default:
				writeTorboxTorrentPage(t, w)
			}
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(4, 4, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(torrents) != 2 || torrents[0].Id != "1" || torrents[1].Id != "2" {
			t.Fatalf("torrents = %#v", torrents)
		}
	})

	t.Run("later page failure discards partial snapshot", func(t *testing.T) {
		client, closeServer := newClient(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("offset") == "0" {
				writeTorboxTorrentPage(t, w, 1)
				return
			}
			http.Error(w, "do-not-expose", http.StatusTeapot)
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(4, 4, 1)
		if err == nil || len(torrents) != 0 || strings.Contains(err.Error(), "do-not-expose") {
			t.Fatalf("torrents = %#v, error = %v", torrents, err)
		}
	})

	t.Run("ignored offset is detected", func(t *testing.T) {
		client, closeServer := newClient(func(w http.ResponseWriter, _ *http.Request) {
			writeTorboxTorrentPage(t, w, 1)
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(4, 4, 1)
		if err == nil || len(torrents) != 0 || !strings.Contains(err.Error(), "repeated torrent ID") {
			t.Fatalf("torrents = %#v, error = %v", torrents, err)
		}
	})

	t.Run("page ceiling", func(t *testing.T) {
		client, closeServer := newClient(func(w http.ResponseWriter, r *http.Request) {
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			writeTorboxTorrentPage(t, w, offset+1)
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(2, 10, 1)
		if err == nil || len(torrents) != 0 || !strings.Contains(err.Error(), "2 non-empty pages") {
			t.Fatalf("torrents = %#v, error = %v", torrents, err)
		}
	})

	t.Run("item ceiling", func(t *testing.T) {
		client, closeServer := newClient(func(w http.ResponseWriter, r *http.Request) {
			offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
			writeTorboxTorrentPage(t, w, offset+1)
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(4, 1, 1)
		if err == nil || len(torrents) != 0 || !strings.Contains(err.Error(), "exceeds 1 items") {
			t.Fatalf("torrents = %#v, error = %v", torrents, err)
		}
	})

	t.Run("short page is terminal", func(t *testing.T) {
		calls := 0
		client, closeServer := newClient(func(w http.ResponseWriter, r *http.Request) {
			calls++
			if got := r.URL.Query().Get("offset"); got != "0" {
				t.Errorf("offset = %q, want 0", got)
			}
			if got := r.URL.Query().Get("limit"); got != "1000" {
				t.Errorf("limit = %q, want 1000", got)
			}
			ids := make([]int, 267)
			for i := range ids {
				ids[i] = i + 1
			}
			writeTorboxTorrentPage(t, w, ids...)
		})
		defer closeServer()

		torrents, err := client.getTorrentsBounded(4, 1_000, 1_000)
		if err != nil {
			t.Fatal(err)
		}
		if len(torrents) != 267 {
			t.Fatalf("torrent count = %d, want 267", len(torrents))
		}
		if calls != 1 {
			t.Fatalf("request count = %d, want 1", calls)
		}
	})
}

func TestInfoResponseAcceptsObjectAndArrayShapes(t *testing.T) {
	tests := []struct {
		name string
		body string
		id   string
		want string
	}{
		{
			name: "documented object",
			body: `{"success":true,"data":{"id":2,"name":"wanted"}}`,
			id:   "2",
			want: "wanted",
		},
		{
			name: "relay array",
			body: `{"success":true,"data":[{"id":1,"name":"other"},{"id":2,"name":"wanted"}]}`,
			id:   "2",
			want: "wanted",
		},
		{
			name: "array does not guess",
			body: `{"success":true,"data":[{"id":1,"name":"other"}]}`,
			id:   "2",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var response InfoResponse
			if err := json.Unmarshal([]byte(test.body), &response); err != nil {
				t.Fatal(err)
			}
			item := response.torrent(test.id)
			if test.want == "" {
				if item != nil {
					t.Fatalf("torrent = %#v, want nil", item)
				}
				return
			}
			if item == nil || item.Name != test.want {
				t.Fatalf("torrent = %#v, want name %q", item, test.want)
			}
		})
	}
}

func TestGetTorrentSelectsRequestedItemFromArrayResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"success":true,
			"data":[
				{"id":1,"name":"other","size":1,"download_state":"completed","download_finished":true,"files":[]},
				{"id":2,"name":"wanted","size":2,"download_state":"completed","download_finished":true,"files":[]}
			]
		}`))
	}))
	defer server.Close()

	client := &Torbox{
		Host:   server.URL,
		client: request.New(request.WithMaxRetries(0)),
		config: config.Debrid{Name: "torbox"},
	}
	torrent, err := client.GetTorrent("2")
	if err != nil {
		t.Fatal(err)
	}
	if torrent.Id != "2" || torrent.Name != "wanted" {
		t.Fatalf("torrent = %#v, want requested ID 2", torrent)
	}
}

func writeTorboxTorrentPage(t *testing.T, w http.ResponseWriter, ids ...int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprint(w, `{"success":true,"data":[`)
	for index, id := range ids {
		if index != 0 {
			_, _ = fmt.Fprint(w, ",")
		}
		_, _ = fmt.Fprintf(
			w,
			`{"id":%d,"name":"torrent-%d","size":1,"download_state":"completed","download_finished":true,"files":[]}`,
			id,
			id,
		)
	}
	_, _ = fmt.Fprint(w, `]}`)
}
