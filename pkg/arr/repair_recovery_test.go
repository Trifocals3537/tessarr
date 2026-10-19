package arr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestExactRepairHistoryDoesNotUseNewestUnrelatedGrab(t *testing.T) {
	var pages atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages.Add(1)
		if r.URL.Query().Get("downloadId") != "old-download" {
			t.Error("missing exact download filter")
		}
		if r.URL.Query().Get("page") == "1" {
			fmt.Fprint(w, `{"totalRecords":101,"records":[{"id":99,"downloadId":"new-download","eventType":"grabbed","movieId":7},{"id":98,"downloadId":"old-download","eventType":"grabbed","movieId":8}]}`)
		} else {
			fmt.Fprint(w, `{"totalRecords":101,"records":[{"id":42,"downloadId":"OLD-DOWNLOAD","eventType":"grabbed","movieId":7}]}`)
		}
	}))
	defer server.Close()
	a := &Arr{Type: Radarr, Host: server.URL, Token: "test-token"}
	grab, failed, err := a.ExactRepairHistory(context.Background(), "old-download", RepairFile{MovieID: 7})
	if err != nil || grab == nil || grab.ID != 42 || failed != nil || pages.Load() != 2 {
		t.Fatalf("grab=%+v failed=%+v pages=%d err=%v", grab, failed, pages.Load(), err)
	}
}

func TestRecoveryMutationsNeverRetryOrFollowRedirects(t *testing.T) {
	for _, status := range []int{500, 503, 307, 403, 429} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(status)
			}))
			defer server.Close()
			a := &Arr{Type: Radarr, Host: server.URL, Token: "test-token"}
			err := a.FailRepairHistory(context.Background(), 42)
			if err == nil || calls.Load() != 1 {
				t.Fatalf("calls=%d err=%v", calls.Load(), err)
			}
			if errors.Is(err, ErrRepairRejected) != (status == 403 || status == 429) {
				t.Fatalf("incorrect rejection classification: %v", err)
			}
		})
	}
}

func TestRecoveryDeleteRevalidatesIdentityAndNeverUnlinksLocalPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replacement.mkv")
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	var deletes atomic.Int32
	file := RepairFile{ID: 10, MovieID: 7, Path: path, Size: 11}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			w.WriteHeader(204)
			return
		}
		json.NewEncoder(w).Encode(file)
	}))
	defer server.Close()
	a := &Arr{Type: Radarr, Host: server.URL, Token: "test-token"}
	if err := a.DeleteRepairFile(context.Background(), file); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("local replacement was unlinked: %v", err)
	}
	wrong := file
	wrong.MovieID++
	if err := a.DeleteRepairFile(context.Background(), wrong); err == nil {
		t.Fatal("changed media identity was accepted")
	}
	if deletes.Load() != 1 {
		t.Fatalf("deletes=%d", deletes.Load())
	}
}

func TestSonarrRecoveryPreservesAllEpisodesInFile(t *testing.T) {
	var searched []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/episodefile/10":
			fmt.Fprint(w, `{"id":10,"seriesId":7,"path":"/tv/double.mkv","size":100}`)
		case "/api/v3/episode":
			fmt.Fprint(w, `[{"id":3,"episodeFileId":10},{"id":2,"episodeFileId":10},{"id":8,"episodeFileId":19}]`)
		case "/api/v3/command":
			var payload struct {
				Name       string
				EpisodeIDs []int
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
			}
			if payload.Name != "EpisodeSearch" {
				t.Errorf("search=%s", payload.Name)
			}
			searched = payload.EpisodeIDs
			fmt.Fprint(w, `{"id":123}`)
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	a := &Arr{Type: Sonarr, Host: server.URL, Token: "test-token"}
	file, err := a.ReadRepairFile(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.SearchRepairFile(context.Background(), *file); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(searched, []int{2, 3}) {
		t.Fatalf("search targets=%v", searched)
	}
}

func TestRepairSearchRequiresReceiptAndReconcilesExactTargets(t *testing.T) {
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			fmt.Fprint(w, "{}")
			return
		}
		json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "name": "MoviesSearch", "queued": now.Add(-time.Hour), "body": map[string]any{"movieIds": []int{7}}},
			{"id": 2, "name": "MoviesSearch", "queued": now, "body": map[string]any{"movieIds": []int{8}}},
			{"id": 3, "name": "MoviesSearch", "queued": now, "body": map[string]any{"movieIds": []int{7}}},
		})
	}))
	defer server.Close()
	a := &Arr{Type: Radarr, Host: server.URL, Token: "test-token"}
	file := RepairFile{MovieID: 7}
	if _, err := a.SearchRepairFile(context.Background(), file); err == nil {
		t.Fatal("missing receipt accepted")
	}
	id, err := a.FindRepairSearch(context.Background(), file, now)
	if err != nil || id != 3 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestRecoveryReadsRejectIncompleteSuccessBodies(t *testing.T) {
	for _, body := range []string{"", "null", "{}"} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			a := &Arr{Type: Radarr, Host: server.URL, Token: "test-token"}
			if _, err := a.ReadRepairFile(context.Background(), 10); err == nil {
				t.Fatal("invalid file accepted")
			}
			if _, err := a.RepairConfig(context.Background()); err == nil {
				t.Fatal("invalid config accepted")
			}
			if _, _, err := a.ExactRepairHistory(context.Background(), "download", RepairFile{MovieID: 7}); err == nil {
				t.Fatal("incomplete history accepted as absent history")
			}
		})
	}
}
