package arr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBlocklistAndResearchDownloadTargetsExactQueueRow(t *testing.T) {
	var removed []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/queue":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"page":1,"pageSize":200,"totalRecords":2,"records":[{"id":17,"downloadId":"ABC123","protocol":"torrent"},{"id":18,"downloadId":"other","protocol":"torrent"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/queue/bulk":
			if r.URL.Query().Get("removeFromClient") != "true" ||
				r.URL.Query().Get("blocklist") != "true" ||
				r.URL.Query().Get("skipRedownload") != "false" {
				t.Errorf("unexpected handoff query: %s", r.URL.RawQuery)
			}
			var body struct {
				Ids []int `json:"ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode delete body: %v", err)
			}
			removed = body.Ids
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	handedOff, err := a.BlocklistAndResearchDownloadCtx(context.Background(), "abc123")
	if err != nil || !handedOff {
		t.Fatalf("handoff = %t, %v", handedOff, err)
	}
	if len(removed) != 1 || removed[0] != 17 {
		t.Fatalf("removed queue IDs = %v, want [17]", removed)
	}
}

func TestBlocklistAndResearchDownloadRejectsAmbiguousHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"page":1,"pageSize":200,"totalRecords":2,"records":[{"id":17,"downloadId":"abc123","protocol":"torrent"},{"id":18,"downloadId":"ABC123","protocol":"torrent"}]}`))
	}))
	defer server.Close()

	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	if handedOff, err := a.BlocklistAndResearchDownloadCtx(context.Background(), "abc123"); err == nil || handedOff {
		t.Fatalf("ambiguous handoff = %t, %v; want refusal", handedOff, err)
	}
}

func TestEndpointHandoffRejectsWrongDownloadClient(t *testing.T) {
	var deletes int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v3/downloadclient":
			_, _ = w.Write([]byte(`[{"name":"Our Tessarr","implementation":"QBittorrent","fields":[{"name":"host","value":"tessarr.example"},{"name":"port","value":8282}]}]`))
		case "/api/v3/queue":
			_, _ = w.Write([]byte(`{"page":1,"pageSize":200,"totalRecords":1,"records":[{"id":17,"downloadId":"abc123","protocol":"torrent","downloadClient":"Other qBittorrent"}]}`))
		case "/api/v3/queue/bulk":
			deletes++
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	a := New("sonarr", server.URL, "token", false, nil, "", "manual")
	if done, err := a.BlocklistAndResearchDownloadForEndpointCtx(context.Background(), "abc123", "tessarr.example:8282"); err == nil || done {
		t.Fatalf("wrong-client handoff = %t, %v", done, err)
	}
	if deletes != 0 {
		t.Fatalf("deleted %d wrong-client jobs", deletes)
	}
}
