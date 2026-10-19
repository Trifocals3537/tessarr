package providertraffic

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Operation identifies a provider request budget. Every non-zero operation
// also spends from the provider's general API budget.
type Operation uint8

const (
	OperationNone Operation = iota
	OperationAPI
	OperationResolveLink
	OperationCreateTorrentUncached
	OperationCreateUsenet
	OperationCreateWebDownload
)

// RateBudget is a provider-advertised request allowance.
type RateBudget struct {
	Requests int
	Period   time.Duration
	Burst    int
}

func (b RateBudget) valid() bool {
	return b.Requests > 0 && b.Period > 0
}

// Capabilities describes provider-side traffic contracts that are independent
// of an individual Tessarr installation. Zero values mean unknown/unlimited.
// User-configured rate limits remain additional, potentially tighter guards.
type Capabilities struct {
	AccountAPIBudget            RateBudget // shared by every API client using one account token
	APIBudget                   RateBudget // applied independently per endpoint
	UncachedTorrentCreateBudget RateBudget
	UsenetCreateBudget          RateBudget
	WebDownloadCreateBudget     RateBudget
	ResolverConcurrency         int
	CDNHostConcurrency          int
	CDNLinkConcurrency          int
}

// For returns the conservative built-in contract for a provider type.
func For(providerType string) Capabilities {
	switch strings.ToLower(strings.TrimSpace(providerType)) {
	case "realdebrid":
		// Real-Debrid documents one 250 request/minute allowance for the whole
		// API. Keep a modest burst below the short addMagnet bursts observed to
		// trigger misleading provider errors while preserving over four requests
		// per second of sustained throughput.
		// https://api.real-debrid.com/
		return Capabilities{
			AccountAPIBudget: RateBudget{Requests: 250, Period: time.Minute, Burst: 20},
		}
	case "torbox":
		// API budgets: https://support.torbox.app/en/articles/13726368-api-rate-limits
		// Link guidance: https://support.torbox.app/en/articles/15315517-why-are-my-download-links-not-working
		return Capabilities{
			APIBudget:                   RateBudget{Requests: 300, Period: time.Minute, Burst: 30},
			UncachedTorrentCreateBudget: RateBudget{Requests: 60, Period: time.Hour, Burst: 60},
			UsenetCreateBudget:          RateBudget{Requests: 60, Period: time.Hour, Burst: 60},
			WebDownloadCreateBudget:     RateBudget{Requests: 60, Period: time.Hour, Burst: 60},
			ResolverConcurrency:         4,
			// TorBox recommends no more than four connections to one signed
			// link. A separate 16-request host ceiling lets unrelated playback
			// progress while still bounding this process's aggregate fan-out.
			CDNHostConcurrency: 16,
			CDNLinkConcurrency: 4,
		}
	default:
		return Capabilities{}
	}
}

func (c Capabilities) budgetFor(operation Operation) RateBudget {
	switch operation {
	case OperationCreateTorrentUncached:
		return c.UncachedTorrentCreateBudget
	case OperationCreateUsenet:
		return c.UsenetCreateBudget
	case OperationCreateWebDownload:
		return c.WebDownloadCreateBudget
	default:
		return RateBudget{}
	}
}

type operationContextKey struct{}

// WithOperation marks a request that consumes a narrower endpoint budget.
// It is intentionally context-scoped so retries retain the classification.
func WithOperation(ctx context.Context, operation Operation) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, operationContextKey{}, operation)
}

func operationFromContext(ctx context.Context) Operation {
	if ctx == nil {
		return OperationNone
	}
	operation, _ := ctx.Value(operationContextKey{}).(Operation)
	return operation
}

// ClassifyRequest separates provider API calls from CDN transfers and detects
// endpoints whose documented budget is narrower than the general API budget.
func ClassifyRequest(providerType string, request *http.Request) Operation {
	if request == nil || request.URL == nil {
		return OperationNone
	}
	classified := ClassifyURL(providerType, request.URL)
	if classified == OperationNone {
		return OperationNone
	}
	if operation := operationFromContext(request.Context()); operation != OperationNone {
		return operation
	}
	return classified
}

// ClassifyURL is the URL-only form used by the streaming transport.
func ClassifyURL(providerType string, requestURL *url.URL) Operation {
	if requestURL == nil {
		return OperationNone
	}
	providerType = strings.ToLower(strings.TrimSpace(providerType))
	switch providerType {
	case "realdebrid":
		if strings.EqualFold(requestURL.Hostname(), "api.real-debrid.com") {
			return OperationAPI
		}
		return OperationNone
	case "torbox":
		if !strings.EqualFold(requestURL.Hostname(), "api.torbox.app") {
			return OperationNone
		}
	default:
		return OperationAPI
	}

	requestPath := strings.ToLower(strings.TrimSuffix(requestURL.EscapedPath(), "/"))
	switch {
	case strings.HasSuffix(requestPath, "/api/torrents/requestdl"),
		strings.HasSuffix(requestPath, "/api/usenet/requestdl"),
		strings.HasSuffix(requestPath, "/api/webdl/requestdl"):
		return OperationResolveLink
	case strings.HasSuffix(requestPath, "/api/usenet/createusenetdownload"):
		return OperationCreateUsenet
	case strings.HasSuffix(requestPath, "/api/webdl/createwebdownload"):
		return OperationCreateWebDownload
	default:
		// Cached-only torrent creation belongs to the general budget. The
		// uncached form is explicitly tagged by the TorBox provider because
		// the URL alone cannot distinguish the two contracts.
		return OperationAPI
	}
}

// EndpointKey returns a query-free key for a provider endpoint. TorBox's
// general allowance is documented per endpoint, so unrelated API methods must
// not consume one shared 300/minute bucket. Queries are excluded because they
// can contain API tokens and resource identifiers.
func EndpointKey(providerType, method string, requestURL *url.URL) string {
	if requestURL == nil || ClassifyURL(providerType, requestURL) == OperationNone {
		return ""
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodGet
	}
	if method == http.MethodHead {
		// HEAD is served by the GET route and should not manufacture a second
		// allowance for link-validation probes.
		method = http.MethodGet
	}
	requestPath := strings.ToLower(strings.TrimSuffix(requestURL.EscapedPath(), "/"))
	return method + " " + requestPath
}
