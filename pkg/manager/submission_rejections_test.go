package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestSubmissionRejectionCacheIsProviderScopedBoundedAndExpiring(t *testing.T) {
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	cache := newSubmissionRejectionCache(time.Hour, 2)
	cache.now = func() time.Time { return now }

	cache.put("rd", "ABC", "Release")
	if _, ok := cache.get("rd", "abc"); !ok {
		t.Fatal("normalized provider/hash lookup missed cached rejection")
	}
	if _, ok := cache.get("torbox", "abc"); ok {
		t.Fatal("provider-scoped rejection leaked to another provider")
	}

	now = now.Add(time.Minute)
	cache.put("torbox", "abc", "Release")
	now = now.Add(time.Minute)
	cache.put("rd", "def", "Other")
	if got := cache.len(); got != 2 {
		t.Fatalf("cache length = %d, want bounded length 2", got)
	}

	now = now.Add(2 * time.Hour)
	if got := cache.len(); got != 0 {
		t.Fatalf("expired cache length = %d, want 0", got)
	}
}

func TestSendToDebridSuppressesRepeatedContentRejection(t *testing.T) {
	root := t.TempDir()
	attempts := 0
	client := &routingTestClient{
		cfg: config.Debrid{Name: "rd"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			attempts++
			return nil, customerror.NewTorrentContentRejectedError(torrent.Name)
		},
	}
	manager := rejectionTestManager(root, client)
	request := rejectionTestImport(root, "rd")

	for i := 0; i < 2; i++ {
		_, err := manager.SendToDebrid(context.Background(), request)
		var customErr *customerror.Error
		if !errors.As(err, &customErr) || customErr.Code != "torrent_content_rejected" {
			t.Fatalf("attempt %d error = %v, want typed content rejection", i+1, err)
		}
	}

	if attempts != 1 {
		t.Fatalf("provider attempts = %d, want 1 after cooldown suppression", attempts)
	}
	stats := manager.TorrentAdmissionStats()
	if stats.ContentRejections != 1 || stats.SuppressedSubmissions != 1 || stats.ActiveCooldowns != 1 {
		t.Fatalf("admission stats = %#v, want one rejection, suppression, and cooldown", stats)
	}
}

func TestSendToDebridDoesNotSuppressRetryableOrOperationalFailures(t *testing.T) {
	tests := []struct {
		name string
		err  func(string) error
	}{
		{
			name: "cache miss",
			err: func(name string) error {
				return customerror.NewTorrentNotCachedError(name)
			},
		},
		{
			name: "provider outage",
			err: func(string) error {
				return errors.New("provider unavailable")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			attempts := 0
			client := &routingTestClient{
				cfg: config.Debrid{Name: "rd"},
				submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
					attempts++
					return nil, test.err(torrent.Name)
				},
			}
			manager := rejectionTestManager(root, client)
			request := rejectionTestImport(root, "rd")

			for range 2 {
				if _, err := manager.SendToDebrid(context.Background(), request); err == nil {
					t.Fatal("SendToDebrid() error = nil")
				}
			}
			if attempts != 2 {
				t.Fatalf("provider attempts = %d, want 2 without suppression", attempts)
			}
			if stats := manager.TorrentAdmissionStats(); stats.ContentRejections != 0 ||
				stats.SuppressedSubmissions != 0 || stats.ActiveCooldowns != 0 {
				t.Fatalf("admission stats = %#v, want no content cooldown", stats)
			}
		})
	}
}

func TestSendToDebridContentRejectionStillFallsBackToAnotherProvider(t *testing.T) {
	root := t.TempDir()
	rdAttempts := 0
	torboxAttempts := 0
	rd := &routingTestClient{
		cfg: config.Debrid{Name: "rd"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			rdAttempts++
			return nil, customerror.NewTorrentContentRejectedError(torrent.Name)
		},
	}
	torbox := &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torboxAttempts++
			torrent.Id = "torbox-id"
			torrent.Debrid = "torbox"
			torrent.Status = debridTypes.TorrentStatusDownloaded
			return torrent, nil
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("rd", rd)
	clients.Store("torbox", torbox)
	manager := &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: root,
			Debrids:        []config.Debrid{{Name: "rd"}, {Name: "torbox"}},
		},
		logger:               zerolog.Nop(),
		submissionRejections: newSubmissionRejectionCache(time.Hour, 10),
	}
	request := rejectionTestImport(root, "")

	for i := 0; i < 2; i++ {
		result, err := manager.SendToDebrid(context.Background(), request)
		if err != nil || result == nil || result.Debrid != "torbox" {
			t.Fatalf("attempt %d result = %#v, error = %v", i+1, result, err)
		}
	}
	if rdAttempts != 1 || torboxAttempts != 2 {
		t.Fatalf("provider attempts = rd:%d torbox:%d, want rd:1 torbox:2", rdAttempts, torboxAttempts)
	}
}

func rejectionTestManager(root string, client debrid.Client) *Manager {
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("rd", client)
	return &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: root,
			Debrids:        []config.Debrid{{Name: "rd"}},
		},
		logger:               zerolog.Nop(),
		submissionRejections: newSubmissionRejectionCache(time.Hour, 10),
	}
}

func rejectionTestImport(root, selectedDebrid string) *ImportRequest {
	return &ImportRequest{
		DownloadFolder: root,
		SelectedDebrid: selectedDebrid,
		Magnet: &utils.Magnet{
			InfoHash: "0123456789abcdef0123456789abcdef01234567",
			Name:     "Release",
			Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
		},
		Arr: &arr.Arr{Name: "sonarr"},
	}
}
