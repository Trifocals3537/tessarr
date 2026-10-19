package torbox

import (
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestFetchDownloadLinkTracksDownloadAccountToken(t *testing.T) {
	provider := &Torbox{
		Host:                  "https://api.torbox.app/v1",
		APIKey:                "main-token",
		autoExpiresLinksAfter: time.Hour,
		config:                config.Debrid{Name: "torbox"},
	}
	got, err := provider.fetchDownloadLink(
		&account.Account{Token: "download-token"},
		"42",
		&types.File{Id: "7", Link: "torbox://42/7", Name: "video.mkv", Size: 4},
	)
	if err != nil {
		t.Fatalf("fetchDownloadLink() error = %v", err)
	}
	if got.Token != "download-token" {
		t.Fatalf("link token = %q, want owning download token", got.Token)
	}
}
