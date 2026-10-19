package arr

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func resetSharedArrClientForTest(t *testing.T) {
	t.Helper()
	sharedClient = nil
	sharedOnce = sync.Once{}
}

func TestRepairHTTPDoesNotExhaustConnections(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	for _, kind := range []Type{Sonarr, Radarr} {
		for _, operation := range []string{"delete", "search", "history"} {
			t.Run(string(kind)+"/"+operation, func(t *testing.T) {
				resetSharedArrClientForTest(t)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprint(w, `{"accepted":true}`)
				}))
				t.Cleanup(server.Close)

				a := New(string(kind), server.URL, "test-token", false, nil, "", "manual")
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				for i := 1; i <= 40; i++ {
					files := []ContentFile{{FileId: i, Id: i, EpisodeId: i, SeasonNumber: 1}}
					var err error
					switch operation {
					case "delete":
						err = a.DeleteFiles(ctx, files)
					case "search":
						err = a.SearchMissing(ctx, files)
					case "history":
						err = a.MarkHistoryFailedCtx(ctx, i)
					}
					if err != nil {
						t.Fatalf("request %d stalled or failed: %v", i, err)
					}
				}
			})
		}
	}
}

func TestRequestCtxClosesDecodedErrorResponses(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"error":"bad request"}`)
	}))
	t.Cleanup(server.Close)

	a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 1; i <= 40; i++ {
		var out map[string]any
		resp, err := a.RequestCtx(ctx, http.MethodGet, "api/v3/test", nil, &out)
		if err != nil {
			t.Fatalf("request %d returned error: %v", i, err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("request %d status = %s, want 400 Bad Request", i, resp.Status)
		}
	}
}

func TestRepairDeletePreservesFilesOnFailure(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	for _, kind := range []Type{Sonarr, Radarr} {
		for _, mode := range []string{"http-error", "cancelled", "success"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				resetSharedArrClientForTest(t)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if mode == "http-error" {
						w.WriteHeader(http.StatusBadRequest)
					}
					_, _ = fmt.Fprint(w, `{}`)
				}))
				t.Cleanup(server.Close)

				a := New(string(kind), server.URL, "test-token", false, nil, "", "manual")
				path := filepath.Join(t.TempDir(), "dummy-media.mkv")
				if err := os.WriteFile(path, []byte("test fixture only"), 0600); err != nil {
					t.Fatal(err)
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "cancelled" {
					cancel()
				}

				err := a.DeleteFiles(ctx, []ContentFile{{FileId: 1, Path: path}})
				_, statErr := os.Stat(path)
				switch mode {
				case "success":
					if err != nil || !os.IsNotExist(statErr) {
						t.Fatalf("successful delete: err=%v stat=%v", err, statErr)
					}
				default:
					if err == nil || statErr != nil {
						t.Fatalf("failed delete must return error and preserve file: err=%v stat=%v", err, statErr)
					}
				}
			})
		}
	}
}

func TestRepairDeleteReportsLocalCleanupFailure(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(server.Close)

	a := New(string(Sonarr), server.URL, "test-token", false, nil, "", "manual")
	dir := filepath.Join(t.TempDir(), "non-empty-dir")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "child"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}

	err := a.DeleteFiles(context.Background(), []ContentFile{{FileId: 1, Path: dir}})
	if err == nil {
		t.Fatal("expected local cleanup failure to be returned")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "child")); statErr != nil {
		t.Fatalf("expected failed local cleanup to leave directory contents: %v", statErr)
	}
}
