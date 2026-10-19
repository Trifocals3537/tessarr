package debridlink

import (
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestFetchDownloadLinkTracksDownloadAccountToken(t *testing.T) {
	provider := &DebridLink{
		APIKey:                "main-token",
		autoExpiresLinksAfter: time.Hour,
		config:                config.Debrid{Name: "debridlink"},
	}
	got, err := provider.fetchDownloadLink(
		&account.Account{Token: "download-token"},
		"42",
		&types.File{Link: "https://cdn.example/video", Name: "video.mkv", Size: 4},
	)
	if err != nil {
		t.Fatalf("fetchDownloadLink() error = %v", err)
	}
	if got.Token != "download-token" || got.Size != 4 {
		t.Fatalf("link token/size = %q/%d, want owning download token and size 4", got.Token, got.Size)
	}
}
