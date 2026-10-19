package link

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestRecoveryRetainsLogicalFilename(t *testing.T) {
	for _, providerName := range []string{"bundle.rar", "provider-renamed-video.mkv"} {
		t.Run(providerName, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/expired" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				writeValidLinkProbe(w)
			}))
			defer server.Close()
			old := lifecycleDownloadLink(server.URL + "/expired")
			fresh := lifecycleDownloadLink(server.URL + "/fresh")
			old.Filename, fresh.Filename = providerName, providerName
			client := &lifecycleTestClient{links: []types.DownloadLink{old, fresh}, cacheLinks: true}
			service := newLifecycleService(client, server.Client(), 0)
			entry := lifecycleTestEntry()
			if providerName == "bundle.rar" {
				entry.Files["video.mkv"].Size = 2
				entry.Files["video.mkv"].ByteRange = &[2]int64{1, 2}
			}
			got, err := service.GetLink(context.Background(), entry, "video.mkv")
			if err != nil || got.DownloadLink != fresh.DownloadLink {
				t.Fatalf("GetLink() = %+v, %v; want replacement", got, err)
			}
			if got.Filename != providerName || got.Size != 4 {
				t.Fatalf("provider object identity changed: %+v", got)
			}
			if len(entry.Files) != 1 || entry.Files["video.mkv"].Name != "video.mkv" {
				t.Fatal("logical file map changed")
			}
			if providerName == "bundle.rar" && *entry.Files["video.mkv"].ByteRange != [2]int64{1, 2} {
				t.Fatal("archive offsets changed")
			}
			if fetches, invalidations := client.counts(); fetches != 2 || invalidations != 1 {
				t.Fatalf("fetches/invalidations = %d/%d, want 2/1", fetches, invalidations)
			}
		})
	}
}

func TestRefreshUsesRequestedFilename(t *testing.T) {
	for _, providerName := range []string{"", "bundle.rar", "provider-renamed-video.mkv"} {
		t.Run("provider_filename="+providerName, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeValidLinkProbe(w)
			}))
			defer server.Close()
			fresh := lifecycleDownloadLink(server.URL)
			fresh.Filename = providerName
			client := &lifecycleTestClient{links: []types.DownloadLink{fresh}}
			service := newLifecycleService(client, server.Client(), 0)
			rejected := lifecycleDownloadLink("https://cdn.example/expired")
			rejected.Filename = providerName
			got, err := service.Refresh(context.Background(), lifecycleTestEntry(), "video.mkv", rejected)
			if err != nil || got.DownloadLink != fresh.DownloadLink || got.Filename != providerName {
				t.Fatalf("Refresh() = %+v, %v; want replacement with unchanged provider identity", got, err)
			}
			if fetches, invalidations := client.counts(); fetches != 1 || invalidations != 1 {
				t.Fatalf("fetches/invalidations = %d/%d, want 1/1", fetches, invalidations)
			}
		})
	}
}

func TestRefreshRejectsUnknownLogicalFileBeforeInvalidation(t *testing.T) {
	for _, filename := range []string{"", "bundle.rar"} {
		t.Run("requested_filename="+filename, func(t *testing.T) {
			client := &lifecycleTestClient{}
			service := newLifecycleService(client, http.DefaultClient, 0)
			rejected := lifecycleDownloadLink("https://cdn.example/expired")
			if _, err := service.Refresh(context.Background(), lifecycleTestEntry(), filename, rejected); err == nil {
				t.Fatal("Refresh() succeeded for a missing logical file")
			}
			if fetches, invalidations := client.counts(); fetches != 0 || invalidations != 0 {
				t.Fatalf("fetches/invalidations = %d/%d, want 0/0", fetches, invalidations)
			}
		})
	}
}

func sharedArchiveEntry() *storage.Entry {
	entry := lifecycleTestEntry()
	entry.Files["video.mkv"].Size = 2
	entry.Files["video.mkv"].ByteRange = &[2]int64{0, 1}
	entry.Files["second.mkv"] = &storage.File{Name: "second.mkv", Size: 2, ByteRange: &[2]int64{2, 3}}
	entry.Providers["test"].Files["second.mkv"] = &storage.ProviderFile{Link: "restricted-link"}
	return entry
}

func TestArchiveRefreshCooldownIsScopedToLogicalFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	rejected := lifecycleDownloadLink(server.URL)
	rejected.Filename = "bundle.rar"
	client := &lifecycleTestClient{links: []types.DownloadLink{rejected}, cacheLinks: true}
	service := newLifecycleService(client, server.Client(), 0)
	entry := sharedArchiveEntry()

	for _, filename := range []string{"video.mkv", "second.mkv"} {
		_, err := service.Refresh(context.Background(), entry, filename, rejected)
		if linkErr := GetLinkError(err); linkErr == nil || linkErr.Code != "403" {
			t.Fatalf("first Refresh(%q) error = %v, want original CDN rejection", filename, err)
		}
		_, err = service.Refresh(context.Background(), entry, filename, rejected)
		if linkErr := GetLinkError(err); linkErr == nil || linkErr.Code != CodeLinkRefreshCooldown {
			t.Fatalf("second Refresh(%q) error = %v, want per-file cooldown", filename, err)
		}
	}
	if fetches, invalidations := client.counts(); fetches != 2 || invalidations != 2 {
		t.Fatalf("fetches/invalidations = %d/%d, want 2/2", fetches, invalidations)
	}
}

func TestDifferentLogicalFilesDoNotShareArchiveRefreshFlight(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
			writeValidLinkProbe(w)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	// Always release handlers on assertion failure, before closing the server.
	defer close(release)
	fresh := lifecycleDownloadLink(server.URL)
	fresh.Filename = "bundle.rar"
	client := &lifecycleTestClient{links: []types.DownloadLink{fresh}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := sharedArchiveEntry()
	results := make(chan error, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, filename := range []string{"video.mkv", "second.mkv"} {
		go func() {
			_, err := service.Refresh(ctx, entry, filename, fresh)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("different logical files incorrectly shared one refresh flight")
		}
	}
	// Send one release per request; the deferred close also handles early failure.
	for range 2 {
		select {
		case release <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("refresh probe did not finish")
		}
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("Refresh() error = %v", err)
		}
	}
	if fetches, invalidations := client.counts(); fetches != 2 || invalidations != 2 {
		t.Fatalf("fetches/invalidations = %d/%d, want independent 2/2", fetches, invalidations)
	}
}

func TestHosterRepairRetainsLogicalFilename(t *testing.T) {
	for _, errorFilename := range []string{"", "bundle.rar"} {
		t.Run("provider_error_filename="+errorFilename, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeValidLinkProbe(w)
			}))
			defer server.Close()
			fresh := lifecycleDownloadLink(server.URL)
			fresh.Filename = "bundle.rar"
			client := &lifecycleTestClient{
				fetchErr:     customerror.HosterUnavailableError,
				fetchErrLink: types.DownloadLink{Filename: errorFilename},
				links:        []types.DownloadLink{fresh},
			}
			service := newLifecycleService(client, server.Client(), 0)
			repairs := 0
			service.repairer = func(context.Context, *storage.Entry) error {
				repairs++
				client.mu.Lock()
				client.fetchErr = nil
				client.mu.Unlock()
				return nil
			}
			got, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv")
			if err != nil || got.DownloadLink != fresh.DownloadLink || repairs != 1 {
				t.Fatalf("repaired link = %+v, err=%v, repairs=%d", got, err, repairs)
			}
		})
	}
}
