package manager

import (
	"cmp"
	"errors"
	"sort"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/alldebrid"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/debridlink"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/premiumize"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/realdebrid"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/torbox"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"go.uber.org/ratelimit"
)

var (
	ErrUnsupportedDebridProvider = errors.New("unsupported debrid provider")
)

func (m *Manager) ProviderClient(name string) debrid.Client {
	client, ok := m.clients.Load(name)
	if !ok {
		return nil
	}
	return client
}

func (m *Manager) initDebridClients() {
	cfg := config.Get()
	for _, dc := range cfg.Debrids {
		client, err := m.createClient(dc)
		if err != nil {
			m.logger.Error().Err(err).Str("debrid", dc.Name).Msg("Failed to create debrid client")
			continue
		}
		m.clients.Store(dc.Name, client)
	}
}

// createClient creates a debrid client based on configuration
func (m *Manager) createClient(dc config.Debrid) (debrid.Client, error) {
	var client debrid.Client
	var err error

	rateLimits := map[string]ratelimit.Limiter{}

	mainRL := utils.ParseRateLimit(dc.RateLimit)
	repairRL := utils.ParseRateLimit(cmp.Or(dc.RepairRateLimit, dc.RateLimit))
	downloadRL := utils.ParseRateLimit(cmp.Or(dc.DownloadRateLimit, dc.RateLimit))

	rateLimits["main"] = mainRL
	rateLimits["repair"] = repairRL
	rateLimits["download"] = downloadRL

	switch dc.Provider {
	case "realdebrid":
		client, err = realdebrid.New(dc, rateLimits, m.providerTraffic)
	case "alldebrid":
		client, err = alldebrid.New(dc, rateLimits)
	case "torbox":
		client, err = torbox.New(dc, rateLimits, m.providerTraffic)
	case "debridlink":
		client, err = debridlink.New(dc, rateLimits)
	case "premiumize":
		client, err = premiumize.New(dc, rateLimits)
	default:
		return nil, ErrUnsupportedDebridProvider
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}

// FilterDebrid returns matching clients in configuration order. The client
// registry is a concurrent map and its iteration order is intentionally
// undefined, so iterating it directly makes multi-provider fallback random.
// Clients that are not present in the current configuration are retained as a
// deterministic, lexically sorted fallback for tests and transitional reloads.
func (m *Manager) FilterDebrid(filter func(debrid.Client) bool) []debrid.Client {
	if m == nil || m.clients == nil {
		return nil
	}

	filtered := make([]debrid.Client, 0)
	seen := make(map[string]struct{})
	appendClient := func(name string) {
		if _, exists := seen[name]; exists {
			return
		}
		seen[name] = struct{}{}
		client, ok := m.clients.Load(name)
		if !ok || client == nil {
			return
		}
		if filter == nil || filter(client) {
			filtered = append(filtered, client)
		}
	}

	if m.config != nil {
		for _, configured := range m.config.Debrids {
			appendClient(configured.Name)
		}
	}

	extraNames := make([]string, 0)
	m.clients.Range(func(name string, _ debrid.Client) bool {
		if _, exists := seen[name]; !exists {
			extraNames = append(extraNames, name)
		}
		return true
	})
	sort.Strings(extraNames)
	for _, name := range extraNames {
		appendClient(name)
	}
	return filtered
}

func (m *Manager) GetIngests() ([]types.IngestData, error) {
	// Use streaming to avoid loading all torrents into memory
	var ingests []types.IngestData
	err := m.storage.ForEach(func(torrent *storage.Entry) error {
		ingests = append(ingests, types.IngestData{
			Debrid: torrent.ActiveProvider,
			Name:   torrent.OriginalFilename,
			Hash:   torrent.InfoHash,
			Size:   torrent.Bytes,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ingests, nil
}

func (m *Manager) GetIngestsByDebrid(debridName string) ([]types.IngestData, error) {
	// Use streaming to avoid loading all torrents into memory
	var ingests []types.IngestData
	err := m.storage.ForEach(func(torrent *storage.Entry) error {
		if !torrent.HasProvider(debridName) {
			return nil
		}
		ingests = append(ingests, types.IngestData{
			Debrid: torrent.ActiveProvider,
			Name:   torrent.OriginalFilename,
			Hash:   torrent.InfoHash,
			Size:   torrent.Bytes,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ingests, nil
}
