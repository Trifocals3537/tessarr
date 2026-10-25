package manager

import (
	"context"
	"encoding/json"

	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/logger"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestHealthRegressionClearStateMustRetainUnrecoveredRange(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		var start, end int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 900 || end != 999 {
			t.Errorf("unexpected range %s", request.Header.Get("Range"))
		}
		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/1024", start, end))
		w.Header().Set("Content-Length", fmt.Sprint(length))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(make([]byte, length/2)) // every exact range, including retries, is genuinely truncated
	}))
	defer server.Close()
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{"primary": {DownloadLink: server.URL, Size: 1024}}}
	m := newStreamFailoverTestManager(links, server.Client(), "primary")
	m.storage = r.manager.storage
	m.clients = r.manager.clients
	r = NewRepair(m)
	if err := m.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:unexpected_eof", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 900, Length: 100}); err != nil {
		t.Fatal(err)
	}
	probe := func() *storage.EntryHealth {
		return r.probeEntry(context.Background(), "review", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	}
	h := probe()
	if h == nil || !h.Dirty || len(h.FailedReads) != 1 || h.Status != storage.HealthUnknown {
		t.Fatalf("fixture did not retain failed range: %+v", h)
	}
	before := requests.Load()
	cleared, err := r.ClearStates([]storage.HealthStatus{storage.HealthUnknown})
	if err != nil {
		t.Fatal(err)
	}
	h = probe()
	t.Logf("cleared=%d requests=%d->%d status=%s dirty=%t ranges=%d", cleared.Cleared, before, requests.Load(), h.Status, h.Dirty, len(h.FailedReads))
	if h.Status == storage.HealthHealthy && !h.Dirty {
		t.Fatal("Clear State erased required evidence and falsely declared unchanged truncated content healthy")
	}
}

func TestHealthRegressionBrokenVerdictMustRejectChangedSource(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 1)
	r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "old-account", check: func(context.Context, string) error {
		r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "new-account", check: func(context.Context, string) error { return nil }})
		return customerror.HosterUnavailableError
	}})
	h := r.probeEntry(context.Background(), "review", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	eligible, _ := r.collectBrokenHealths([]string{item.Name}, false)
	t.Logf("status=%s dirty=%t broken=%d clear_eligible=%d", h.Status, h.Dirty, len(h.BrokenFiles), eligible.Size())
	if h.Status != storage.HealthUnknown || !h.Dirty || len(h.BrokenFiles) != 0 || eligible.Size() != 0 {
		t.Fatal("old account's broken verdict survives changed source fingerprint and authorizes clearing the current file")
	}
}

func TestHealthRegressionLaterSourceChangeInvalidatesEarlierProof(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 2)
	r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "old-account", check: func(ctx context.Context, name string) error {
		if name == "video-000.mkv" {
			return nil
		}
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			h, err := r.manager.storage.GetEntryHealth(item.Name)
			if err != nil {
				return err
			}
			if h.ProbeProgress != nil && h.ProbeProgress.Verified["video-000.mkv"] != "" {
				r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "new-account", check: func(context.Context, string) error { return nil }})
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				t.Error("first file proof was not checkpointed")
				return context.DeadlineExceeded
			case <-ticker.C:
			}
		}
	}})
	h := r.probeEntry(context.Background(), "later-source", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || h.ProbeProgress == nil || len(h.ProbeProgress.Verified) != 0 || len(h.IncompleteFiles) != 2 {
		t.Fatalf("earlier account proof survived a later change: %+v", h)
	}
}

func TestHealthRegressionUnavailableSourceCannotAuthorizeClear(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 1)
	r.manager.clients.Delete("primary")
	h := r.probeEntry(context.Background(), "missing-source", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	eligible, _ := r.collectBrokenHealths([]string{item.Name}, false)
	if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || len(h.BrokenFiles) != 0 || eligible.Size() != 0 {
		t.Fatalf("unverifiable source authorized removal: %+v", h)
	}
}

func TestHealthRegressionAuthorizedFileRetirementMustRetireItsFailedRanges(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 2)
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, name string) error {
		if name == "video-000.mkv" {
			return customerror.HosterUnavailableError
		}
		return nil
	}})
	if err := r.manager.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:content_missing", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 100, Length: 20}); err != nil {
		t.Fatal(err)
	}
	h := r.probeEntry(context.Background(), "review", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthBroken || h.Dirty || len(h.FailedReads) != 1 {
		t.Fatalf("fixture not confirmed broken: %+v", h)
	}
	if _, err := r.ClearBroken(context.Background(), []string{item.Name}); err != nil {
		t.Fatal(err)
	}
	r.runWG.Wait()
	current, err := r.manager.storage.GetEntryItem(item.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Files["video-000.mkv"].Deleted {
		t.Fatal("operator did not retire exact broken file")
	}
	h = r.probeEntry(context.Background(), "review-after-clear", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	t.Logf("authorized retired file deleted=true remaining=1 status=%s dirty=%t ranges=%d incomplete=%v", h.Status, h.Dirty, len(h.FailedReads), h.IncompleteFiles)
	if h.Status != storage.HealthHealthy || h.Dirty {
		t.Fatal("authorized removal leaves failed ranges targeting a deleted file, so remaining healthy media stays incomplete forever")
	}
}

func TestHealthRegressionOldBrokenVerdictCannotRetireReplacement(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 2)
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error { return customerror.HosterUnavailableError }})
	h := r.probeEntry(context.Background(), "old-file", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthBroken || len(h.BrokenFiles) != 2 || h.BrokenFiles[0].FileID == "" {
		t.Fatalf("broken identity fixture: %+v", h)
	}
	bf := h.BrokenFiles[0]
	current, _ := r.manager.storage.GetEntryItem(item.Name)
	current.Files[bf.FileName].ID = "new-incarnation"
	if err := r.manager.storage.UpdateItem(current); err != nil {
		t.Fatal(err)
	}
	if err := r.manager.removeTorrentFile(item.Name, bf.FileName, &bf); err == nil {
		t.Fatal("old broken verdict authorized a replacement's retirement")
	}
	current, _ = r.manager.storage.GetEntryItem(item.Name)
	if current.Files[bf.FileName].Deleted {
		t.Fatal("replacement was retired")
	}
}

func TestHealthRegressionAliasTracksCanonicalRename(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHealthRegressionAliasRenameHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "TESSARR_HEALTH_REGRESSION_ALIAS=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical rename producer: %v\n%s", err, out)
	}
}
func TestHealthRegressionAliasRenameHelper(t *testing.T) {
	if os.Getenv("TESSARR_HEALTH_REGRESSION_ALIAS") != "1" {
		return
	}
	defer logger.Close()
	root := t.TempDir()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"folder_naming": "original", "debrids": []map[string]any{{"name": "realdebrid", "api_key": "fixture"}}})
	if err := os.WriteFile(filepath.Join(root, "config.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.Get()
	r, e, _ := newEvidenceRepair(t, 1)
	e.Name = "Runtime.Release"
	e.OriginalFilename = "Canonical.A"
	if err := r.manager.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	failed := storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 100, Length: 20}
	if err := r.manager.storage.RecordFailedRead(e.Name, e.Protocol, "read_failure:timeout", failed); err != nil {
		t.Fatal(err)
	}
	c := &candidate{name: e.Name}
	if err := r.resolveHealthCandidate(c); err != nil {
		t.Fatal(err)
	}
	if c.name != "Canonical.A" {
		t.Fatalf("wrong initial canonical name %s", c.name)
	}
	current, err := r.manager.storage.Get(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.OriginalFilename = "Canonical.B"
	if err := r.manager.storage.AddOrUpdate(current); err != nil {
		t.Fatal(err)
	}
	if _, err := r.manager.storage.GetEntryItem("Canonical.B"); err != nil {
		t.Fatalf("producer did not move canonical item: %v", err)
	}
	if _, err := r.manager.storage.GetEntryItem("Canonical.A"); err == nil {
		t.Fatal("old canonical item unexpectedly remains")
	}
	if err := r.manager.storage.RecordFailedRead(e.Name, e.Protocol, "read_failure:timeout", failed); err != nil {
		t.Fatal(err)
	}
	h, _ := r.manager.storage.GetEntryHealth(e.Name)
	c = &candidate{name: e.Name}
	err = r.resolveHealthCandidate(c)
	t.Logf("actual index=Canonical.B failure stored=%s resolved=%s error=%v", h.EntryName, c.name, err)
	if err != nil || c.name != "Canonical.B" {
		t.Fatal("persisted name-only alias strands new failures at obsolete canonical key")
	}
	oldEvidence, _ := r.manager.storage.GetEntryHealth("Canonical.B")
	current, err = r.manager.storage.Get(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Name = "Previous.Runtime"
	if err := r.manager.storage.AddOrUpdate(current); err != nil {
		t.Fatal(err)
	}
	other := &storage.Entry{InfoHash: "other-health-owner", Name: e.Name, OriginalFilename: "Canonical.C", Protocol: e.Protocol,
		Files: map[string]*storage.File{"video-000.mkv": {InfoHash: "other-health-owner", Name: "video-000.mkv", Size: 1024}}}
	if err := r.manager.storage.AddOrUpdate(other); err != nil {
		t.Fatal(err)
	}
	otherFailure := failed
	otherFailure.InfoHash = other.InfoHash
	if err := r.manager.storage.RecordFailedRead(other.Name, other.Protocol, "read_failure:timeout", otherFailure); err != nil {
		t.Fatal(err)
	}
	c = &candidate{name: other.Name}
	if err := r.resolveHealthCandidate(c); err != nil || c.name != "Canonical.C" {
		t.Fatalf("reused runtime name was not rebound: %s %v", c.name, err)
	}
	newEvidence, _ := r.manager.storage.GetEntryHealth(other.Name)
	if len(newEvidence.FailedReads) != 1 || newEvidence.FailedReads[0].InfoHash != other.InfoHash {
		t.Fatalf("previous owner's evidence crossed name reuse: %+v", newEvidence)
	}
	previous, _ := r.manager.storage.GetEntryHealth("Canonical.B")
	if len(previous.FailedReads) != len(oldEvidence.FailedReads) || previous.OwnerInfoHash != e.InfoHash {
		t.Fatalf("name reuse discarded previous owner's evidence: %+v", previous)
	}
}
