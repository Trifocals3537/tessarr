package arr

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestEndpointHandoffBeforeDeleteValidatesOwnership(t *testing.T) {
	for _, mode := range []string{"owned", "wrong-client", "ambiguous", "persistence-failure"} {
		t.Run(mode, func(t *testing.T) {
			var deleted, authorized atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v3/downloadclient":
					w.Write([]byte(`[{"name":"Tessarr","implementation":"QBittorrent","fields":[{"name":"host","value":"host"},{"name":"port","value":8282}]}]`))
				case "/api/v3/queue":
					if mode == "wrong-client" {
						w.Write([]byte(`{"page":1,"totalRecords":1,"records":[{"id":17,"protocol":"torrent","downloadId":"hash","downloadClient":"Other"}]}`))
						return
					}
					if mode == "ambiguous" {
						w.Write([]byte(`{"page":1,"totalRecords":2,"records":[{"id":17,"protocol":"torrent","downloadId":"hash","downloadClient":"Tessarr"},{"id":18,"protocol":"torrent","downloadId":"hash","downloadClient":"Tessarr"}]}`))
						return
					}
					w.Write([]byte(`{"page":1,"totalRecords":1,"records":[{"id":17,"protocol":"torrent","downloadId":"hash","downloadClient":"Tessarr"}]}`))
				case "/api/v3/queue/bulk":
					if authorized.Load() != 1 {
						t.Error("Arr DELETE outran ownership proof")
					}
					deleted.Add(1)
					w.WriteHeader(200)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			a := New("sonarr", server.URL, "token", false, nil, "", "manual")
			accepted, err := a.BlocklistAndResearchDownloadForEndpointBeforeDeleteCtx(context.Background(), "hash", "host:8282", func() error {
				authorized.Add(1)
				if mode == "persistence-failure" {
					return errors.New("sync failure")
				}
				return nil
			})
			if mode == "owned" {
				if err != nil || !accepted || deleted.Load() != 1 {
					t.Fatalf("handoff=%v,%v deletes=%d", accepted, err, deleted.Load())
				}
			} else {
				if err == nil || accepted || deleted.Load() != 0 {
					t.Fatalf("unsafe handoff=%v,%v deletes=%d", accepted, err, deleted.Load())
				}
				if mode != "persistence-failure" && authorized.Load() != 0 {
					t.Fatal("unowned queue granted proof")
				}
			}
		})
	}
}
