package manager

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

// policyTestMagnet returns the minimal magnet accepted by import validation.
func policyTestMagnet() *utils.Magnet {
	return &utils.Magnet{
		InfoHash: "0123456789abcdef0123456789abcdef01234567",
		Name:     "Release",
		Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
	}
}

// recordingPolicyClient records every submission attempt as "provider:pass"
// where pass is "cached" or "uncached". A cached attempt that finds the torrent
// uncached reports a torrent_not_cached error, exactly like the real providers.
type recordingPolicyClient struct {
	debrid.Client
	cfg      config.Debrid
	cached   bool
	attempts *[]string
}

func (c *recordingPolicyClient) Config() config.Debrid  { return c.cfg }
func (c *recordingPolicyClient) Logger() zerolog.Logger { return zerolog.Nop() }

func (c *recordingPolicyClient) SubmitMagnet(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	mode := "cached"
	if torrent.DownloadUncached {
		mode = "uncached"
	}
	*c.attempts = append(*c.attempts, c.cfg.Name+":"+mode)
	if mode == "cached" && !c.cached {
		return nil, customerror.NewTorrentNotCachedError(torrent.Name)
	}
	torrent.Id = c.cfg.Name + "-id"
	torrent.Debrid = c.cfg.Name
	torrent.Status = debridTypes.TorrentStatusDownloaded
	return torrent, nil
}

func (c *recordingPolicyClient) CheckStatus(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	return torrent, nil
}

func (c *recordingPolicyClient) DeleteTorrent(string) error { return nil }

// newPolicyManager wires a Manager whose provider set is fully mocked.
func newPolicyManager(t *testing.T, attempts *[]string, providers []config.Debrid) *Manager {
	t.Helper()
	clients := xsync.NewMap[string, debrid.Client]()
	for _, provider := range providers {
		clients.Store(provider.Name, &recordingPolicyClient{
			cfg:      provider,
			cached:   false, // every provider reports a cache miss when asked cached-only
			attempts: attempts,
		})
	}
	return &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: t.TempDir(),
			Debrids:        providers,
		},
		logger: zerolog.Nop(),
	}
}

func policyProviders(realDebridUncached, torboxUncached bool) []config.Debrid {
	return []config.Debrid{
		{Name: "realdebrid", DownloadUncached: realDebridUncached},
		{Name: "torbox", DownloadUncached: torboxUncached},
	}
}

func sendPolicyRequest(t *testing.T, m *Manager, downloadUncached *bool) error {
	t.Helper()
	_, err := m.SendToDebrid(context.Background(), &ImportRequest{
		DownloadFolder:   m.config.DownloadFolder,
		DownloadUncached: downloadUncached,
		Magnet:           policyTestMagnet(),
		Arr:              &arr.Arr{Name: "sonarr"},
	})
	return err
}

// TestSendToDebridNilArrPolicyInheritsProviderUncachedPolicy is the regression
// test for the operational footgun this change fixes: an Arr entry with no
// explicit download_uncached must inherit provider policy, so a TorBox
// configured for uncached fallback keeps working after every cached miss.
func TestSendToDebridNilArrPolicyInheritsProviderUncachedPolicy(t *testing.T) {
	var attempts []string
	m := newPolicyManager(t, &attempts, policyProviders(false, true))

	if err := sendPolicyRequest(t, m, nil); err != nil {
		t.Fatalf("SendToDebrid() error = %v", err)
	}
	want := []string{"realdebrid:cached", "torbox:cached", "torbox:uncached"}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want %v", attempts, want)
	}
}

// TestSendToDebridArrFalseStaysStrictlyCachedOnly proves an explicit false on
// the Arr wins over every provider's own uncached setting.
func TestSendToDebridArrFalseStaysStrictlyCachedOnly(t *testing.T) {
	var attempts []string
	m := newPolicyManager(t, &attempts, policyProviders(true, true))

	err := sendPolicyRequest(t, m, boolPointer(false))
	if err == nil {
		t.Fatal("SendToDebrid() error = nil, want not-cached failure")
	}
	if !strings.Contains(err.Error(), "not cached") {
		t.Fatalf("error = %v, want not-cached failure", err)
	}
	want := []string{"realdebrid:cached", "torbox:cached"}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want cached-only attempts %v", attempts, want)
	}
}

// TestSendToDebridArrTrueOverridesProviderCachedOnly proves an explicit true on
// the Arr unlocks the uncached pass even for providers configured cached-only.
func TestSendToDebridArrTrueOverridesProviderCachedOnly(t *testing.T) {
	var attempts []string
	m := newPolicyManager(t, &attempts, policyProviders(false, false))

	if err := sendPolicyRequest(t, m, boolPointer(true)); err != nil {
		t.Fatalf("SendToDebrid() error = %v", err)
	}
	want := []string{"realdebrid:cached", "torbox:cached", "realdebrid:uncached"}
	if !reflect.DeepEqual(attempts, want) {
		t.Fatalf("attempts = %v, want %v", attempts, want)
	}
}

// TestSendToDebridNeverAttemptsUncachedBeforeAllCachedExhausted proves the
// two-pass ordering invariant regardless of the effective policy source.
func TestSendToDebridNeverAttemptsUncachedBeforeAllCachedExhausted(t *testing.T) {
	for _, tc := range []struct {
		name             string
		downloadUncached *bool
	}{
		{name: "nil inherit policy", downloadUncached: nil},
		{name: "arr true policy", downloadUncached: boolPointer(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts []string
			m := newPolicyManager(t, &attempts, policyProviders(true, true))

			_ = sendPolicyRequest(t, m, tc.downloadUncached) // success or failure is irrelevant here

			seenUncached := false
			for _, attempt := range attempts {
				if strings.HasSuffix(attempt, ":uncached") {
					seenUncached = true
					continue
				}
				if seenUncached && strings.HasSuffix(attempt, ":cached") {
					t.Fatalf("cached attempt %q after first uncached attempt in %v", attempt, attempts)
				}
			}
		})
	}
}

// TestNewTorrentRequestPreservesDownloadUncachedPointer guards the qBittorrent
// import path: the Arr's tri-state pointer must reach the ImportRequest
// untouched so nil can keep meaning "inherit provider policy".
func TestNewTorrentRequestPreservesDownloadUncachedPointer(t *testing.T) {
	dummyArr := &arr.Arr{Name: "sonarr"}

	for _, tc := range []struct {
		name string
		in   *bool
	}{
		{name: "nil", in: nil},
		{name: "true", in: boolPointer(true)},
		{name: "false", in: boolPointer(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := NewTorrentRequest("torbox", "/downloads", policyTestMagnet(), dummyArr, config.DownloadActionSymlink, tc.in, "", ImportTypeQBit, false)
			if (req.DownloadUncached == nil) != (tc.in == nil) {
				t.Fatalf("DownloadUncached = %v, want nil=%v", req.DownloadUncached, tc.in == nil)
			}
			if tc.in != nil && *req.DownloadUncached != *tc.in {
				t.Fatalf("DownloadUncached = %v, want %v", *req.DownloadUncached, *tc.in)
			}
		})
	}
}

func TestUncachedPolicySourceDistinguishesProviderArrAndRequest(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *ImportRequest
		want string
	}{
		{name: "nil request", req: nil, want: "provider"},
		{name: "provider inheritance", req: &ImportRequest{Type: ImportTypeQBit}, want: "provider"},
		{name: "arr override", req: &ImportRequest{Type: ImportTypeQBit, DownloadUncached: boolPointer(true)}, want: "arr"},
		{name: "runtime API override", req: &ImportRequest{Type: ImportTypeAPI, DownloadUncached: boolPointer(true)}, want: "request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := uncachedPolicySource(tc.req); got != tc.want {
				t.Fatalf("uncachedPolicySource() = %q, want %q", got, tc.want)
			}
		})
	}
}
