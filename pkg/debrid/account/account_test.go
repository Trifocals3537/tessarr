package account

import (
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
)

func newLinkCacheTestAccount() *Account {
	return &Account{
		Debrid: "test",
		Token:  "token",
		links:  xsync.NewMap[string, types.DownloadLink](),
	}
}

func TestGetDownloadLinkReusesHealthyCachedLink(t *testing.T) {
	account := newLinkCacheTestAccount()
	now := time.Now()
	cached := types.DownloadLink{
		Debrid:       "test",
		Token:        "token",
		Link:         "restricted-link",
		DownloadLink: "https://cdn.example/healthy",
		Generated:    now.Add(-time.Hour),
		ExpiresAt:    now.Add(time.Hour),
	}
	account.storeLink(cached)

	file := &types.File{Link: cached.Link}
	fetches := 0
	got, err := account.GetDownloadLink("torrent", file, func(*Account, string, *types.File) (types.DownloadLink, error) {
		fetches++
		return types.DownloadLink{}, nil
	})
	if err != nil {
		t.Fatalf("GetDownloadLink() error = %v", err)
	}
	if fetches != 0 {
		t.Fatalf("fetches = %d, want 0", fetches)
	}
	if got.DownloadLink != cached.DownloadLink {
		t.Fatalf("download link = %q, want %q", got.DownloadLink, cached.DownloadLink)
	}
}

func TestGetDownloadLinkRefreshesExpiringCachedLink(t *testing.T) {
	account := newLinkCacheTestAccount()
	now := time.Now()
	stale := types.DownloadLink{
		Debrid:       "test",
		Token:        "token",
		Link:         "restricted-link",
		DownloadLink: "https://cdn.example/stale",
		Generated:    now.Add(-time.Hour),
		ExpiresAt:    now.Add(30 * time.Second),
	}
	fresh := stale
	fresh.DownloadLink = "https://cdn.example/fresh"
	fresh.Generated = now
	fresh.ExpiresAt = now.Add(time.Hour)
	account.storeLink(stale)

	file := &types.File{Link: stale.Link}
	fetches := 0
	got, err := account.GetDownloadLink("torrent", file, func(*Account, string, *types.File) (types.DownloadLink, error) {
		fetches++
		return fresh, nil
	})
	if err != nil {
		t.Fatalf("GetDownloadLink() error = %v", err)
	}
	if fetches != 1 {
		t.Fatalf("fetches = %d, want 1", fetches)
	}
	if got.DownloadLink != fresh.DownloadLink {
		t.Fatalf("download link = %q, want %q", got.DownloadLink, fresh.DownloadLink)
	}
}

func TestInvalidateLinkOnlyEvictsLocalCache(t *testing.T) {
	account := newLinkCacheTestAccount()
	link := types.DownloadLink{
		Debrid:       "test",
		Token:        "token",
		Link:         "restricted-link",
		DownloadLink: "https://cdn.example/cached",
	}
	account.storeLink(link)
	account.InvalidateLink(link)

	if account.DownloadLinksCount() != 0 {
		t.Fatalf("cached links = %d, want 0", account.DownloadLinksCount())
	}
}

func TestStoreLinkDoesNotCacheRecoveryProbeOwnership(t *testing.T) {
	account := newLinkCacheTestAccount()
	link := types.DownloadLink{
		Link:            "restricted-link",
		DownloadLink:    "https://cdn.example/cached",
		RecoveryProbeID: 42,
	}
	account.storeLink(link)

	cached, ok := account.GetRandomLink()
	if !ok {
		t.Fatal("cached link not found")
	}
	if cached.RecoveryProbeID != 0 {
		t.Fatalf("cached recovery probe ID = %d, want 0", cached.RecoveryProbeID)
	}
}

func TestManagerInvalidateDownloadLinkUsesOwningAccount(t *testing.T) {
	first := newLinkCacheTestAccount()
	first.Token = "first"
	second := newLinkCacheTestAccount()
	second.Token = "second"
	link := types.DownloadLink{
		Debrid:       "test",
		Token:        second.Token,
		Link:         "restricted-link",
		DownloadLink: "https://cdn.example/cached",
	}
	second.storeLink(link)
	manager := &Manager{accounts: xsync.NewMap[string, *Account]()}
	manager.accounts.Store(first.Token, first)
	manager.accounts.Store(second.Token, second)

	if err := manager.InvalidateDownloadLink(link); err != nil {
		t.Fatalf("InvalidateDownloadLink() error = %v", err)
	}
	if second.DownloadLinksCount() != 0 {
		t.Fatalf("owning account cached links = %d, want 0", second.DownloadLinksCount())
	}
}
