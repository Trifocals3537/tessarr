package debridlink

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestGetTorrentUsesDocumentedListEndpointAndExactID(t *testing.T) {
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/seedbox/list" || r.URL.Query().Get("ids") != "wanted" {
			t.Errorf("request = %s?%s, want /seedbox/list?ids=wanted", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{
			"success":true,
			"value":[
				{"id":"other","name":"Wrong","status":100,"files":[]},
				{"id":"wanted","name":"Right","hashString":"hash","status":4,"downloadPercent":35,"totalSize":20,"created":1700000000,"files":[{"id":"file-1","name":"Right/video.mkv","size":20,"downloadUrl":"https://cdn.example/video","downloadPercent":35}]}
			]
		}`)
	})

	torrent, err := provider.GetTorrent("wanted")
	if err != nil {
		t.Fatal(err)
	}
	if torrent.Id != "wanted" || torrent.Name != "Right" || torrent.InfoHash != "hash" {
		t.Fatalf("torrent = %#v, want exact requested torrent", torrent)
	}
	if torrent.Status != types.TorrentStatusDownloading || torrent.Progress != 35 {
		t.Fatalf("status/progress = %q/%v, want downloading/35", torrent.Status, torrent.Progress)
	}
	file, ok := torrent.Files["video.mkv"]
	if !ok || file.DownloadLink.DownloadLink != "https://cdn.example/video" {
		t.Fatalf("file = %#v, exists=%v, want attached provider download link", file, ok)
	}
}

func TestUpdateTorrentSelectsRequestedID(t *testing.T) {
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/seedbox/list" || r.URL.Query().Get("ids") != "wanted" {
			t.Errorf("request = %s?%s, want /seedbox/list?ids=wanted", r.URL.Path, r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"success":true,"value":[{"id":"other","name":"Wrong","status":100,"files":[]},{"id":"wanted","name":"Right","status":100,"downloadPercent":100,"files":[]}]}`)
	})

	torrent := &types.Torrent{Id: "wanted"}
	if err := provider.UpdateTorrent(torrent); err != nil {
		t.Fatal(err)
	}
	if torrent.Id != "wanted" || torrent.Name != "Right" || torrent.Status != types.TorrentStatusDownloaded {
		t.Fatalf("torrent = %#v, want exact requested completed torrent", torrent)
	}
}

func TestGetProfileTreatsPremiumLeftAsDuration(t *testing.T) {
	const premiumLeft = int64(3_600)
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/account/infos" {
			t.Errorf("path = %q, want /account/infos", r.URL.Path)
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"value":{"username":"user","email":"user@example.com","accountType":1,"premiumLeft":%d,"pts":7}}`, premiumLeft)
	})

	before := time.Now()
	profile, err := provider.GetProfile()
	after := time.Now()
	if err != nil {
		t.Fatal(err)
	}
	wantMin := before.Add(time.Duration(premiumLeft) * time.Second)
	wantMax := after.Add(time.Duration(premiumLeft) * time.Second)
	if profile.Expiration.Before(wantMin) || profile.Expiration.After(wantMax) {
		t.Fatalf("expiration = %s, want between %s and %s", profile.Expiration, wantMin, wantMax)
	}
	if profile.Type != "premium" || profile.Premium != profile.Expiration.Unix() {
		t.Fatalf("profile = %#v, want premium with absolute expiration", profile)
	}
}

func TestGetProfileLeavesFreeAccountExpirationUnknown(t *testing.T) {
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"success":true,"value":{"username":"free","premiumLeft":0}}`)
	})
	profile, err := provider.GetProfile()
	if err != nil {
		t.Fatal(err)
	}
	if profile.Type != "free" || !profile.Expiration.IsZero() || profile.Premium != 0 {
		t.Fatalf("profile = %#v, want free account with unknown expiration", profile)
	}
}

func TestPremiumExpirationRejectsDurationOverflow(t *testing.T) {
	_, err := debridLinkPremiumExpiration(time.Now(), debridLinkMaxPremiumSeconds+1)
	if err == nil || !strings.Contains(err.Error(), "invalid premium duration") {
		t.Fatalf("debridLinkPremiumExpiration() error = %v, want overflow rejection", err)
	}
}

func TestGetTorrentsRejectsMissingValue(t *testing.T) {
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"success":false}`)
	})
	_, _, err := provider.getTorrents(0, 100)
	if err == nil || !strings.Contains(err.Error(), "error getting torrents") {
		t.Fatalf("getTorrents() error = %v, want invalid envelope error", err)
	}
}

func TestTorrentLookupsRejectMissingIDs(t *testing.T) {
	provider := &DebridLink{}
	if _, err := provider.GetTorrent("   "); err == nil || !strings.Contains(err.Error(), "ID is empty") {
		t.Fatalf("GetTorrent() error = %v, want empty ID rejection", err)
	}
	if err := provider.UpdateTorrent(&types.Torrent{}); err == nil || !strings.Contains(err.Error(), "ID is missing") {
		t.Fatalf("UpdateTorrent() error = %v, want missing ID rejection", err)
	}
}

func TestGetTorrentsFollowsProviderPaginationPastActiveOnlyPage(t *testing.T) {
	requests := make([]string, 0, 2)
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		requests = append(requests, page)
		switch page {
		case "0":
			_, _ = io.WriteString(w, `{"success":true,"value":[{"id":"active","name":"Active","status":4,"files":[]}],"pagination":{"page":0,"pages":2,"next":1,"previous":-1}}`)
		case "1":
			_, _ = io.WriteString(w, `{"success":true,"value":[{"id":"done","name":"Done","status":100,"files":[]}],"pagination":{"page":1,"pages":2,"next":-1,"previous":0}}`)
		default:
			t.Fatalf("unexpected page %q", page)
		}
	})

	torrents, err := provider.GetTorrents()
	if err != nil {
		t.Fatal(err)
	}
	if len(torrents) != 1 || torrents[0].Id != "done" {
		t.Fatalf("torrents = %#v, want completed torrent from second page", torrents)
	}
	if strings.Join(requests, ",") != "0,1" {
		t.Fatalf("requested pages = %v, want [0 1]", requests)
	}
}

func TestGetTorrentsRejectsPaginationCycles(t *testing.T) {
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "0" {
			_, _ = io.WriteString(w, `{"success":true,"value":[],"pagination":{"page":0,"pages":2,"next":1,"previous":-1}}`)
			return
		}
		_, _ = io.WriteString(w, `{"success":true,"value":[],"pagination":{"page":1,"pages":2,"next":0,"previous":0}}`)
	})

	_, err := provider.GetTorrents()
	if err == nil || !strings.Contains(err.Error(), "repeated page 0") {
		t.Fatalf("GetTorrents() error = %v, want pagination cycle rejection", err)
	}
}

func TestFetchDownloadLinksUsesOwningAccountAndProviderPagination(t *testing.T) {
	now := time.Now().Unix()
	requests := make([]string, 0, 2)
	provider := newDebridLinkTestProvider(t, config.Debrid{
		Name:                 "debridlink",
		APIKey:               "main-token",
		DownloadAPIKeys:      []string{"download-token"},
		AutoExpireLinksAfter: "1h",
	}, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer download-token" {
			t.Errorf("Authorization = %q, want owning download account", got)
		}
		page := r.URL.Query().Get("page")
		requests = append(requests, page)
		switch page {
		case "0":
			_, _ = fmt.Fprintf(w, `{"success":true,"value":[{"created":%d,"id":"first","name":"first.mkv","url":"https://source.example/first","downloadUrl":"https://cdn.example/first","expired":false,"size":1},{"created":%d,"id":"expired","name":"expired.mkv","url":"https://source.example/expired","downloadUrl":"https://cdn.example/expired","expired":true,"size":1},{"created":0,"id":"unknown","name":"unknown.mkv","url":"https://source.example/unknown","downloadUrl":"https://cdn.example/unknown","expired":false,"size":1}],"pagination":{"page":0,"pages":2,"next":1,"previous":-1}}`, now, now)
		case "1":
			_, _ = fmt.Fprintf(w, `{"success":true,"value":[{"created":%d,"id":"second","name":"second.mkv","url":"https://source.example/second","downloadUrl":"https://cdn.example/second","expired":false,"size":2}],"pagination":{"page":1,"pages":2,"next":-1,"previous":0}}`, now)
		default:
			t.Fatalf("unexpected page %q", page)
		}
	})

	account := provider.accountsManager.Current()
	links, err := provider.fetchDownloadLinks(account)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].Id != "first" || links[1].Id != "second" {
		t.Fatalf("links = %#v, want active links from both pages", links)
	}
	for _, link := range links {
		if link.Token != "download-token" {
			t.Fatalf("link %q token = %q, want owning account token", link.Id, link.Token)
		}
	}
	if strings.Join(requests, ",") != "0,1" {
		t.Fatalf("requested pages = %v, want [0 1]", requests)
	}
}

func TestDebridLinkNextPageValidatesProviderMetadata(t *testing.T) {
	for _, test := range []struct {
		name       string
		pagination *debridLinkPagination
		page       int
		items      int
		want       string
	}{
		{name: "page mismatch", pagination: &debridLinkPagination{Page: 1, Pages: 2, Next: -1}, page: 0, want: "returned page 1"},
		{name: "self cycle", pagination: &debridLinkPagination{Page: 0, Pages: 2, Next: 0}, page: 0, want: "invalid next page 0"},
		{name: "next outside pages", pagination: &debridLinkPagination{Page: 0, Pages: 2, Next: 2}, page: 0, want: "invalid next page 2"},
		{name: "oversized page", page: 0, items: debridLinkListPageSize + 1, want: "returned 101 items"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := debridLinkNextPage(test.pagination, test.page, test.items, debridLinkListPageSize)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("debridLinkNextPage() error = %v, want %q", err, test.want)
			}
		})
	}
}
