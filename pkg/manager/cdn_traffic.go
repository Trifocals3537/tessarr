package manager

import (
	"context"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func (m *Manager) withCDNIdentity(ctx context.Context, link debridTypes.DownloadLink, fallbackProvider string) context.Context {
	provider := strings.TrimSpace(link.Debrid)
	if provider == "" {
		provider = strings.TrimSpace(fallbackProvider)
	}
	providerType := m.cdnProviderType(provider)
	return cdntraffic.WithIdentity(ctx, cdntraffic.Identity{
		Provider:     provider,
		ProviderType: providerType,
		AccountToken: link.Token,
		LinkKey:      cdnLinkKey(link),
	})
}

func cdnLinkKey(link debridTypes.DownloadLink) string {
	if link.Link != "" {
		return link.Link
	}
	if link.Id != "" || link.Filename != "" {
		return link.Id + "\x00" + link.Filename
	}
	// Some providers expose only the final URL. It may be signed, so the
	// governor hashes it immediately and never includes it in snapshots.
	return link.DownloadLink
}

func (m *Manager) cdnProviderType(provider string) string {
	if m == nil || m.clients == nil || provider == "" {
		return ""
	}
	client, ok := m.clients.Load(provider)
	if !ok || client == nil {
		return ""
	}
	return client.Config().Provider
}

// CDNTrafficStats returns secret-free adaptive admission statistics.
func (m *Manager) CDNTrafficStats() cdntraffic.Stats {
	if m == nil || m.cdnTraffic == nil {
		return cdntraffic.Stats{}
	}
	return m.cdnTraffic.Snapshot()
}
