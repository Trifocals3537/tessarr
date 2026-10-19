package providertraffic

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestClassifyTorBoxTraffic(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Operation
	}{
		{name: "torrent resolver", raw: "https://api.torbox.app/v1/api/torrents/requestdl?token=secret", want: OperationResolveLink},
		{name: "usenet resolver", raw: "https://api.torbox.app/v1/api/usenet/requestdl", want: OperationResolveLink},
		{name: "web resolver", raw: "https://api.torbox.app/v1/api/webdl/requestdl", want: OperationResolveLink},
		{name: "usenet create", raw: "https://api.torbox.app/v1/api/usenet/createusenetdownload", want: OperationCreateUsenet},
		{name: "web create", raw: "https://api.torbox.app/v1/api/webdl/createwebdownload", want: OperationCreateWebDownload},
		{name: "general api", raw: "https://api.torbox.app/v1/api/torrents/mylist", want: OperationAPI},
		{name: "cdn", raw: "https://nexus-001.torbox.app/signed/file", want: OperationNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := url.Parse(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := ClassifyURL("torbox", parsed); got != test.want {
				t.Fatalf("ClassifyURL() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestTorBoxCapabilitiesMatchDocumentedContracts(t *testing.T) {
	capabilities := For("torbox")
	if capabilities.APIBudget.Requests != 300 || capabilities.APIBudget.Period != time.Minute {
		t.Fatalf("general API budget = %+v", capabilities.APIBudget)
	}
	for name, budget := range map[string]RateBudget{
		"uncached torrent": capabilities.UncachedTorrentCreateBudget,
		"usenet":           capabilities.UsenetCreateBudget,
		"web download":     capabilities.WebDownloadCreateBudget,
	} {
		if budget.Requests != 60 || budget.Period != time.Hour {
			t.Fatalf("%s create budget = %+v", name, budget)
		}
	}
	if capabilities.CDNLinkConcurrency != 4 || capabilities.CDNHostConcurrency < capabilities.CDNLinkConcurrency {
		t.Fatalf("TorBox CDN capabilities = %+v", capabilities)
	}
}

func TestRealDebridCapabilitiesMatchDocumentedAccountContract(t *testing.T) {
	capabilities := For("realdebrid")
	if capabilities.AccountAPIBudget.Requests != 250 ||
		capabilities.AccountAPIBudget.Period != time.Minute ||
		capabilities.AccountAPIBudget.Burst != 20 {
		t.Fatalf("account API budget = %+v", capabilities.AccountAPIBudget)
	}
	if capabilities.APIBudget.valid() {
		t.Fatalf("Real-Debrid API budget must be account-wide, got endpoint budget %+v", capabilities.APIBudget)
	}
}

func TestClassifyRealDebridAPIWithoutCountingCDNTraffic(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Operation
	}{
		{name: "api", raw: "https://api.real-debrid.com/rest/1.0/torrents?auth_token=secret", want: OperationAPI},
		{name: "restricted link", raw: "https://real-debrid.com/d/ABCDEFGHIJKLM", want: OperationNone},
		{name: "cdn", raw: "https://example.download.real-debrid.com/d/private", want: OperationNone},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := url.Parse(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := ClassifyURL("realdebrid", parsed); got != test.want {
				t.Fatalf("ClassifyURL() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRequestOperationCannotReclassifyTorBoxCDN(t *testing.T) {
	ctx := WithOperation(context.Background(), OperationCreateTorrentUncached)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://nexus-001.torbox.app/signed/file",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := ClassifyRequest("torbox", request); got != OperationNone {
		t.Fatalf("ClassifyRequest() = %v, want CDN OperationNone", got)
	}
}

func TestExplicitUncachedTorrentClassification(t *testing.T) {
	ctx := WithOperation(context.Background(), OperationCreateTorrentUncached)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"https://api.torbox.app/v1/api/torrents/createtorrent",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := ClassifyRequest("torbox", request); got != OperationCreateTorrentUncached {
		t.Fatalf("ClassifyRequest() = %v, want uncached create", got)
	}
}

func TestEndpointKeyIsQueryFreeAndEndpointSpecific(t *testing.T) {
	first, err := url.Parse("https://api.torbox.app/v1/api/torrents/mylist?token=secret&id=1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := url.Parse("https://api.torbox.app/v1/api/torrents/checkcached?hash=private")
	if err != nil {
		t.Fatal(err)
	}
	firstKey := EndpointKey("torbox", http.MethodGet, first)
	secondKey := EndpointKey("torbox", http.MethodGet, second)
	if firstKey == secondKey {
		t.Fatalf("different endpoints shared key %q", firstKey)
	}
	if strings.Contains(firstKey, "secret") || strings.Contains(firstKey, "id=1") {
		t.Fatalf("endpoint key retained query data: %q", firstKey)
	}
	if headKey := EndpointKey("torbox", http.MethodHead, first); headKey != firstKey {
		t.Fatalf("HEAD key = %q, want GET key %q", headKey, firstKey)
	}
}
