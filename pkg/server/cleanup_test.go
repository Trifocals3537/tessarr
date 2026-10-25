package server

import (
	"bytes"
	"encoding/json"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCleanupRoutesAuthenticateFenceAndAuditResolution(t *testing.T) {
	old := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(old) })
	cfg := config.Get()
	cfg.DownloadFolder = t.TempDir()
	cfg.Debrids = nil
	cfg.UseAuth = true
	cfg.Auth = &config.Auth{Username: "admin", Password: "hash", APIToken: "secret-api-token", SessionVersion: 1}
	m := manager.New()
	t.Cleanup(func() { m.Stop(); logger.Close() })
	s := New(m)
	e := &storage.Entry{InfoHash: "operator-work", Name: "<img src=x onerror=alert(1)>", Protocol: config.ProtocolTorrent, State: storage.EntryStatePausedUP, SavePath: cfg.DownloadFolder, IsComplete: true}
	if err := m.Storage().AddQueue(e); err != nil {
		t.Fatal(err)
	}
	i, err := m.Storage().PrepareQueuedDeletionPreservingFiles(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	i, err = m.Storage().ClaimCleanup(i.InfoHash, i.QueueIncarnation, "private-owner", map[string]string{"fake": "secret-account-tag"}, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Storage().StartCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := m.Storage().RetireCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := m.Storage().ReleaseCleanup(i.InfoHash, i.QueueIncarnation, i.Work.Token, "local_cleanup_requires_review", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	router := chi.NewRouter()
	router.Use(s.authMiddleware)
	router.Get("/api/cleanup/workflows", s.handleCleanupWorkflows)
	router.Post("/api/cleanup/workflows/{key}/resolve", s.handleResolveCleanup)
	request := func(method, path string, body []byte, token bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://tessarr.example"+path, bytes.NewReader(body))
		if token {
			req.Header.Set("Authorization", "Bearer secret-api-token")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	if r := request("GET", "/api/cleanup/workflows", nil, false); r.Code != 401 {
		t.Fatalf("anonymous read: %d", r.Code)
	}
	r := request("GET", "/api/cleanup/workflows", nil, true)
	if r.Code != 200 {
		t.Fatalf("GET: %d %s", r.Code, r.Body.String())
	}
	for _, secret := range []string{"secret-api-token", "secret-account-tag", "private-owner", i.Work.Token} {
		if strings.Contains(r.Body.String(), secret) {
			t.Fatal("private claim/account leaked")
		}
	}
	var view struct {
		Active []cleanupView `json:"active"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &view); err != nil || len(view.Active) != 1 {
		t.Fatal("missing work view", err)
	}
	current := view.Active[0]
	id := uuid.NewString()
	path := "/api/cleanup/workflows/operator-work/resolve"
	payload := func(inc string, rev uint64, action string) []byte {
		b, _ := json.Marshal(map[string]any{"incarnation": inc, "revision": rev, "request_id": id, "action": action})
		return b
	}
	for _, tc := range []struct {
		body  []byte
		token bool
		code  int
	}{
		{payload(current.Incarnation, current.Revision, "reconcile"), false, 401},
		{payload("replacement", current.Revision, "reconcile"), true, 409},
		{payload(current.Incarnation, current.Revision-1, "reconcile"), true, 409},
		{payload(current.Incarnation, current.Revision, "force-complete"), true, 400},
		{[]byte(`{"incarnation":"x","revision":1,"request_id":"x","action":"retry","preserve_files":false}`), true, 400},
		{payload(current.Incarnation, current.Revision, "reconcile"), true, 202},
		{payload(current.Incarnation, current.Revision, "reconcile"), true, 202},
	} {
		r := request("POST", path, tc.body, tc.token)
		if r.Code != tc.code {
			t.Fatalf("resolution: %d want %d: %s", r.Code, tc.code, r.Body.String())
		}
	}
	rows, _ := m.Storage().QueuedDeletionIntents()
	if len(rows) != 1 || len(rows[0].Work.Decisions) != 1 || !rows[0].PreserveFiles {
		t.Fatal("resolution changed immutable policy or duplicated audit")
	}
	cookie := authenticatedTestCookie(t, s, cfg.Auth.SessionVersion)
	req := httptest.NewRequest("POST", "http://tessarr.example"+path, bytes.NewReader(payload(current.Incarnation, current.Revision, "reconcile")))
	req.AddCookie(cookie)
	req.Header.Set("Origin", "http://attacker.example")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != 403 {
		t.Fatalf("cross-origin resolution accepted: %d", response.Code)
	}
	// A replay acknowledged by the old receipt cannot act on a new queue row
	// with the same hash. Completion history is scoped to the old incarnation.
	claimed, err := m.Storage().ClaimCleanup(i.InfoHash, i.QueueIncarnation, "operator-worker", nil, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Storage().CompleteCleanup(claimed, nil); err != nil {
		t.Fatal(err)
	}
	replacement := &storage.Entry{InfoHash: e.InfoHash, Name: "replacement", State: storage.EntryStatePausedUP}
	if err := m.Storage().AddQueue(replacement); err != nil {
		t.Fatal(err)
	}
	r = request("POST", path, payload(current.Incarnation, current.Revision, "reconcile"), true)
	if r.Code != 202 {
		t.Fatalf("completed decision replay: %d %s", r.Code, r.Body.String())
	}
	loaded, err := m.Storage().GetQueued(e.InfoHash)
	if err != nil || loaded.QueueIncarnation != replacement.QueueIncarnation {
		t.Fatal("old resolution affected a replacement row")
	}
}
