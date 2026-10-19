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

type routingTestClient struct {
	debrid.Client
	cfg    config.Debrid
	submit func(*debridTypes.Torrent) (*debridTypes.Torrent, error)
	check  func(*debridTypes.Torrent) (*debridTypes.Torrent, error)
	fresh  func(*debridTypes.Torrent) (*debridTypes.Torrent, error)
	get    func(string) (*debridTypes.Torrent, error)
	delete func(string) error
}

func boolPointer(value bool) *bool { return &value }

func (c *routingTestClient) Config() config.Debrid  { return c.cfg }
func (c *routingTestClient) Logger() zerolog.Logger { return zerolog.Nop() }

func (c *routingTestClient) SubmitMagnet(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if c.submit == nil {
		return torrent, nil
	}
	return c.submit(torrent)
}

func (c *routingTestClient) CheckStatus(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if c.check == nil {
		return torrent, nil
	}
	return c.check(torrent)
}

func (c *routingTestClient) CheckStatusFreshContext(_ context.Context, torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if c.fresh != nil {
		return c.fresh(torrent)
	}
	return c.CheckStatus(torrent)
}

func (c *routingTestClient) GetTorrent(id string) (*debridTypes.Torrent, error) {
	if c.get == nil {
		return nil, customerror.TorrentNotFoundError
	}
	return c.get(id)
}

func (c *routingTestClient) DeleteTorrent(id string) error {
	if c.delete == nil {
		return nil
	}
	return c.delete(id)
}

func TestFilterDebridUsesConfigurationOrder(t *testing.T) {
	clients := xsync.NewMap[string, debrid.Client]()
	for _, name := range []string{"zeta", "second", "alpha", "first"} {
		clients.Store(name, &routingTestClient{cfg: config.Debrid{Name: name}})
	}
	manager := &Manager{
		clients: clients,
		config: &config.Config{Debrids: []config.Debrid{
			{Name: "first"},
			{Name: "second"},
		}},
	}
	want := []string{"first", "second", "alpha", "zeta"}

	for range 100 {
		gotClients := manager.FilterDebrid(nil)
		got := make([]string, 0, len(gotClients))
		for _, client := range gotClients {
			got = append(got, client.Config().Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("provider order = %v, want %v", got, want)
		}
	}
}

func TestGetEntriesUsesDeterministicNamespaceOrder(t *testing.T) {
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("second", &routingTestClient{cfg: config.Debrid{Name: "second"}})
	clients.Store("first", &routingTestClient{cfg: config.Debrid{Name: "first"}})
	manager := &Manager{
		clients: clients,
		config: &config.Config{Debrids: []config.Debrid{
			{Name: "first"},
			{Name: "second"},
		}},
	}

	entries := manager.GetEntries()
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	want := []string{
		EntryAllFolder,
		EntryBadFolder,
		EntryTorrentFolder,
		EntryNZBFolder,
		"first",
		"second",
		"version.txt",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mount namespace order = %v, want %v", got, want)
	}
}

func TestCustomFoldersUseDeterministicOrder(t *testing.T) {
	manager := &Manager{config: &config.Config{CustomFolders: map[string]config.CustomFolders{
		"zeta":  {},
		"alpha": {},
	}}}
	manager.initCustomFolders()
	if got, want := manager.GetCustomFolders(), []string{"alpha", "zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("custom folder order = %v, want %v", got, want)
	}
}

func TestSendToDebridFallsBackInOrderWithFreshCandidate(t *testing.T) {
	root := t.TempDir()
	var attempts []string
	first := &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			attempts = append(attempts, "torbox")
			torrent.Id = "must-not-leak"
			torrent.Debrid = "must-not-leak"
			torrent.Files["must-not-leak"] = debridTypes.File{Name: "must-not-leak"}
			return nil, customerror.NewTorrentNotCachedError(torrent.Name)
		},
	}
	second := &routingTestClient{
		cfg: config.Debrid{Name: "realdebrid"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			attempts = append(attempts, "realdebrid")
			if torrent.Id != "" || torrent.Debrid != "" || len(torrent.Files) != 0 {
				t.Fatalf("fallback candidate retained failed-provider state: %#v", torrent)
			}
			torrent.Id = "rd-id"
			torrent.Debrid = "realdebrid"
			torrent.Status = debridTypes.TorrentStatusDownloaded
			return torrent, nil
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	// Store in the opposite order to prove the map cannot choose routing order.
	clients.Store("realdebrid", second)
	clients.Store("torbox", first)
	manager := &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: root,
			Debrids: []config.Debrid{
				{Name: "torbox"},
				{Name: "realdebrid"},
			},
		},
		logger: zerolog.Nop(),
	}

	result, err := manager.SendToDebrid(context.Background(), &ImportRequest{
		DownloadFolder: root,
		Magnet: &utils.Magnet{
			InfoHash: "0123456789abcdef0123456789abcdef01234567",
			Name:     "Release",
			Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
		},
		Arr: &arr.Arr{Name: "sonarr"},
	})
	if err != nil {
		t.Fatalf("SendToDebrid() error = %v", err)
	}
	if !reflect.DeepEqual(attempts, []string{"torbox", "realdebrid"}) {
		t.Fatalf("attempts = %v", attempts)
	}
	if result == nil || result.Debrid != "realdebrid" || result.Id != "rd-id" {
		t.Fatalf("result = %#v", result)
	}
}

func TestSendToDebridReportsIncompleteProviderResponse(t *testing.T) {
	root := t.TempDir()
	client := &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
			return nil, nil
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox", client)
	manager := &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: root,
			Debrids:        []config.Debrid{{Name: "torbox"}},
		},
		logger: zerolog.Nop(),
	}

	_, err := manager.SendToDebrid(context.Background(), &ImportRequest{
		DownloadFolder: root,
		Magnet: &utils.Magnet{
			InfoHash: "0123456789abcdef0123456789abcdef01234567",
			Name:     "Release",
			Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
		},
		Arr: &arr.Arr{Name: "sonarr"},
	})
	if err == nil {
		t.Fatal("SendToDebrid() error = nil")
	}
	if !strings.Contains(err.Error(), "torbox submission") ||
		!strings.Contains(err.Error(), "incomplete submission response") {
		t.Fatalf("error = %q, want provider-scoped incomplete-response detail", err)
	}
	if strings.Contains(err.Error(), "%!w") {
		t.Fatalf("error contains a nil-wrap formatting artifact: %q", err)
	}
}

func TestSendToDebridTriesEveryProviderCachedBeforeUncached(t *testing.T) {
	root := t.TempDir()
	var attempts []string
	client := func(name string, cached bool) *routingTestClient {
		return &routingTestClient{
			cfg: config.Debrid{Name: name, DownloadUncached: true},
			submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				mode := "cached"
				if torrent.DownloadUncached {
					mode = "uncached"
				}
				attempts = append(attempts, name+":"+mode)
				if !torrent.DownloadUncached && !cached {
					return nil, customerror.NewTorrentNotCachedError(torrent.Name)
				}
				torrent.Id = name + "-id"
				torrent.Debrid = name
				torrent.Status = debridTypes.TorrentStatusDownloaded
				return torrent, nil
			},
		}
	}

	t.Run("later cached provider wins before earlier uncached provider", func(t *testing.T) {
		attempts = nil
		clients := xsync.NewMap[string, debrid.Client]()
		clients.Store("torbox", client("torbox", false))
		clients.Store("realdebrid", client("realdebrid", true))
		manager := &Manager{
			clients: clients,
			config: &config.Config{DownloadFolder: root, Debrids: []config.Debrid{
				{Name: "torbox", DownloadUncached: true},
				{Name: "realdebrid", DownloadUncached: true},
			}},
			logger: zerolog.Nop(),
		}

		result, err := manager.SendToDebrid(context.Background(), &ImportRequest{
			DownloadFolder: root,
			Magnet: &utils.Magnet{
				InfoHash: "0123456789abcdef0123456789abcdef01234567",
				Name:     "Release",
				Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
			},
			Arr: &arr.Arr{Name: "sonarr"},
		})
		if err != nil {
			t.Fatalf("SendToDebrid() error = %v", err)
		}
		if got, want := attempts, []string{"torbox:cached", "realdebrid:cached"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("attempts = %v, want %v", got, want)
		}
		if result.Debrid != "realdebrid" || result.DownloadUncached {
			t.Fatalf("result = %#v, want cached realdebrid placement", result)
		}
	})

	t.Run("uncached pass starts only after every cached miss", func(t *testing.T) {
		attempts = nil
		clients := xsync.NewMap[string, debrid.Client]()
		clients.Store("torbox", client("torbox", false))
		clients.Store("realdebrid", client("realdebrid", false))
		manager := &Manager{
			clients: clients,
			config: &config.Config{DownloadFolder: root, Debrids: []config.Debrid{
				{Name: "torbox", DownloadUncached: true},
				{Name: "realdebrid", DownloadUncached: true},
			}},
			logger: zerolog.Nop(),
		}

		result, err := manager.SendToDebrid(context.Background(), &ImportRequest{
			DownloadFolder:   root,
			DownloadUncached: boolPointer(true),
			Magnet: &utils.Magnet{
				InfoHash: "0123456789abcdef0123456789abcdef01234567",
				Name:     "Release",
				Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
			},
			Arr: &arr.Arr{Name: "sonarr"},
		})
		if err != nil {
			t.Fatalf("SendToDebrid() error = %v", err)
		}
		if got, want := attempts, []string{"torbox:cached", "realdebrid:cached", "torbox:uncached"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("attempts = %v, want %v", got, want)
		}
		if result.Debrid != "torbox" || !result.DownloadUncached {
			t.Fatalf("result = %#v, want uncached torbox placement", result)
		}
	})
}

func TestSendToDebridDoesNotTreatProviderFailureAsCacheMiss(t *testing.T) {
	root := t.TempDir()
	var attempts []bool
	client := &routingTestClient{
		cfg: config.Debrid{Name: "torbox", DownloadUncached: true},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			attempts = append(attempts, torrent.DownloadUncached)
			return nil, context.DeadlineExceeded
		},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("torbox", client)
	manager := &Manager{
		clients: clients,
		config: &config.Config{
			DownloadFolder: root,
			Debrids:        []config.Debrid{{Name: "torbox", DownloadUncached: true}},
		},
		logger: zerolog.Nop(),
	}

	_, err := manager.SendToDebrid(context.Background(), &ImportRequest{
		DownloadFolder: root,
		Magnet: &utils.Magnet{
			InfoHash: "0123456789abcdef0123456789abcdef01234567",
			Name:     "Release",
			Link:     "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
		},
		Arr: &arr.Arr{Name: "sonarr"},
	})
	if err == nil {
		t.Fatal("SendToDebrid() error = nil")
	}
	if got, want := attempts, []bool{false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("DownloadUncached attempts = %v, want %v", got, want)
	}
}
