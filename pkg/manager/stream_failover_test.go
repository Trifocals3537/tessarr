package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type failoverLinkService struct {
	mu    sync.Mutex
	links map[string]debridTypes.DownloadLink
	errs  map[string]error
	calls []string
}

func (s *failoverLinkService) GetLink(_ context.Context, entry *storage.Entry, _ string) (debridTypes.DownloadLink, error) {
	provider := entry.ActiveProvider
	s.mu.Lock()
	s.calls = append(s.calls, provider)
	linkValue := s.links[provider]
	err := s.errs[provider]
	s.mu.Unlock()
	if err != nil {
		return debridTypes.DownloadLink{}, err
	}
	if linkValue.Debrid == "" {
		linkValue.Debrid = provider
	}
	if linkValue.Filename == "" {
		linkValue.Filename = "video.mkv"
	}
	if linkValue.Link == "" {
		linkValue.Link = provider + "://restricted"
	}
	return linkValue, nil
}

func (s *failoverLinkService) Refresh(ctx context.Context, entry *storage.Entry, filename string, _ debridTypes.DownloadLink) (debridTypes.DownloadLink, error) {
	return s.GetLink(ctx, entry, filename)
}

func (*failoverLinkService) Clear() {}

func (s *failoverLinkService) callOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func streamFailoverEntry(providers ...string) *storage.Entry {
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "stream-failover-hash",
		Name:           "stream-failover-entry",
		ActiveProvider: providers[0],
		Files:          map[string]*storage.File{"video.mkv": {Name: "video.mkv", Size: 4}},
		Providers:      make(map[string]*storage.ProviderEntry, len(providers)),
	}
	for _, provider := range providers {
		entry.Providers[provider] = &storage.ProviderEntry{
			Provider: provider,
			ID:       "torrent-" + provider,
			Status:   debridTypes.TorrentStatusDownloaded,
			Files: map[string]*storage.ProviderFile{
				"video.mkv": {Link: provider + "://restricted"},
			},
		}
	}
	return entry
}

func newStreamFailoverTestManager(
	service downloadLinkService,
	client *http.Client,
	providers ...string,
) *Manager {
	configured := make([]config.Debrid, 0, len(providers))
	for _, provider := range providers {
		configured = append(configured, config.Debrid{Name: provider})
	}
	return &Manager{
		linkService:               service,
		streamClient:              client,
		config:                    &config.Config{Retries: 3, Debrids: configured},
		logger:                    zerolog.Nop(),
		activeStreams:             xsync.NewMap[string, *ActiveStream](),
		streamProviderPreferences: xsync.NewMap[string, streamProviderPreference](),
		streamProviderWeather:     newStreamProviderWeather(),
		streamFileCircuits:        newStreamFileCircuitBreaker(),
	}
}

func TestStreamCandidatesAreDeterministicAndIsolated(t *testing.T) {
	entry := streamFailoverEntry("primary", "zeta", "beta", "incomplete", "removed")
	entry.Providers["incomplete"].Status = debridTypes.TorrentStatusDownloading
	removedAt := time.Now()
	entry.Providers["removed"].RemovedAt = &removedAt
	manager := newStreamFailoverTestManager(&failoverLinkService{}, http.DefaultClient, "incomplete", "beta")

	candidates := manager.streamCandidates(entry, "video.mkv")
	providers := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		providers = append(providers, candidate.provider)
	}
	if want := []string{"primary", "beta", "zeta"}; !slices.Equal(providers, want) {
		t.Fatalf("candidate order = %v, want %v", providers, want)
	}
	if candidates[0].entryForAttempt(entry, "video.mkv") != entry {
		t.Fatal("active candidate should preserve the original entry lifecycle semantics")
	}

	alternate := candidates[1].entryForAttempt(entry, "video.mkv")
	alternate.ActiveProvider = "mutated"
	alternate.Providers["beta"].Files["video.mkv"].Link = "mutated"
	if entry.ActiveProvider != "primary" || entry.Providers["beta"].Files["video.mkv"].Link == "mutated" {
		t.Fatal("candidate mutation leaked into the caller's entry")
	}
}

func TestAlternateCandidateSnapshotOnlyCopiesRequestedFile(t *testing.T) {
	entry := streamFailoverEntry("primary", "fallback")
	for index := range 2_000 {
		name := fmt.Sprintf("extra-%04d.mkv", index)
		entry.Files[name] = &storage.File{Name: name, Size: 4}
		for _, placement := range entry.Providers {
			placement.Files[name] = &storage.ProviderFile{Link: placement.Provider + "://restricted/" + name}
		}
	}
	manager := newStreamFailoverTestManager(&failoverLinkService{}, http.DefaultClient, "primary", "fallback")

	candidates := manager.streamCandidates(entry, "video.mkv")
	if len(candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(candidates))
	}
	alternate := candidates[1].entryForAttempt(entry, "video.mkv")
	if len(alternate.Files) != 1 || len(alternate.Providers) != 1 ||
		len(alternate.Providers["fallback"].Files) != 1 {
		t.Fatalf("alternate snapshot copied unrelated files: files/providers/provider-files = %d/%d/%d",
			len(alternate.Files), len(alternate.Providers), len(alternate.Providers["fallback"].Files))
	}
}

func TestStreamFailsOverBeforeReadyAndReusesSuccessfulPreference(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/primary":
			primaryRequests.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/fallback":
			fallbackRequests.Add(1)
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 0-3/4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("data"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	var waits atomic.Int32
	manager.streamWait = func(context.Context, time.Duration) error {
		waits.Add(1)
		return nil
	}
	entry := streamFailoverEntry("primary", "fallback")
	streamID := manager.TrackStream(entry, "video.mkv", "test")
	defer manager.UntrackStream(streamID)
	otherStreamID := manager.TrackStream(entry, "video.mkv", "other-client")
	defer manager.UntrackStream(otherStreamID)
	streamCtx := WithActiveStreamID(context.Background(), streamID)

	for attempt := 0; attempt < 2; attempt++ {
		var output bytes.Buffer
		readyCalls := 0
		if err := manager.Stream(streamCtx, entry, "video.mkv", 0, 3, &output, func(*StreamMetadata) error {
			readyCalls++
			return nil
		}, "test"); err != nil {
			t.Fatalf("Stream() attempt %d error = %v", attempt+1, err)
		}
		if output.String() != "data" || readyCalls != 1 {
			t.Fatalf("attempt %d output/ready = %q/%d, want data/1", attempt+1, output.String(), readyCalls)
		}
	}

	if got, want := links.callOrder(), []string{"primary", "fallback", "fallback"}; !slices.Equal(got, want) {
		t.Fatalf("provider calls = %v, want %v", got, want)
	}
	if primaryRequests.Load() != 1 || fallbackRequests.Load() != 2 || waits.Load() != 0 {
		t.Fatalf("primary/fallback/waits = %d/%d/%d, want 1/2/0", primaryRequests.Load(), fallbackRequests.Load(), waits.Load())
	}
	if entry.ActiveProvider != "primary" {
		t.Fatalf("durable active provider changed to %q", entry.ActiveProvider)
	}
	active := manager.GetActiveStreams()
	activeProviders := make(map[string]string, len(active))
	for _, stream := range active {
		activeProviders[stream.ID] = stream.Debrid
	}
	if len(active) != 2 || activeProviders[streamID] != "fallback" || activeProviders[otherStreamID] != "primary" {
		t.Fatalf("active stream providers = %+v, want only initiating consumer on fallback", activeProviders)
	}
	stats := manager.StreamFailoverStats()
	if stats.Attempts != 1 || stats.Successes != 2 || stats.PreferredHits != 1 || stats.Exhausted != 0 {
		t.Fatalf("failover stats = %+v, want attempts=1 successes=2 preferred=1 exhausted=0", stats)
	}
}

func TestExpiredStreamPreferenceReturnsToActiveProvider(t *testing.T) {
	manager := newStreamFailoverTestManager(&failoverLinkService{}, http.DefaultClient, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")
	manager.streamProviderPreferences.Store(streamPreferenceKey(entry, "video.mkv"), streamProviderPreference{
		provider:  "fallback",
		expiresAt: time.Now().Add(-time.Second),
	})

	candidates := manager.streamCandidates(entry, "video.mkv")
	if len(candidates) != 2 || candidates[0].provider != "primary" || candidates[0].preferred {
		t.Fatalf("expired preference candidates = %+v, want active provider first", candidates)
	}
	if manager.streamProviderPreferences.Size() != 0 {
		t.Fatal("expired preference was not evicted")
	}
}

func TestStreamAttemptsDeferFullActiveRecoveryUntilAlternatesFail(t *testing.T) {
	candidates := []streamCandidate{
		{provider: "primary"},
		{provider: "fallback"},
	}
	attempts := streamAttemptCandidates(candidates, "primary")
	providers := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		providers = append(providers, attempt.provider)
	}
	if want := []string{"primary", "fallback", "primary"}; !slices.Equal(providers, want) {
		t.Fatalf("attempt providers = %v, want %v", providers, want)
	}
	if attempts[0].recovery || attempts[1].recovery || !attempts[2].recovery {
		t.Fatalf("recovery flags = %v/%v/%v, want false/false/true",
			attempts[0].recovery, attempts[1].recovery, attempts[2].recovery)
	}

	preferredFirst := []streamCandidate{
		{provider: "fallback", preferred: true},
		{provider: "primary"},
	}
	attempts = streamAttemptCandidates(preferredFirst, "primary")
	if len(attempts) != 2 {
		t.Fatalf("preferred-first attempt count = %d, want 2 without duplicate active recovery", len(attempts))
	}
}

func TestStreamPreservesRangeAcrossProviderFailover(t *testing.T) {
	var ranges []string
	var rangeMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeMu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		rangeMu.Unlock()
		if r.URL.Path == "/primary" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 1-2/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("at"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")
	var output bytes.Buffer
	var metadata *StreamMetadata
	err := manager.Stream(context.Background(), entry, "video.mkv", 1, 2, &output, func(got *StreamMetadata) error {
		metadata = got
		return nil
	}, "test")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if output.String() != "at" || metadata == nil || metadata.StatusCode != http.StatusPartialContent ||
		metadata.ContentLength != 2 || metadata.Header.Get("Content-Range") != "bytes 1-2/4" {
		t.Fatalf("output/metadata = %q/%+v, want ranged fallback response", output.String(), metadata)
	}
	if want := []string{"bytes=1-2", "bytes=1-2"}; !slices.Equal(ranges, want) {
		t.Fatalf("range headers = %v, want %v", ranges, want)
	}
}

func TestStreamFailsOverBeforeCommitOnInvalidUpstreamRange(t *testing.T) {
	tests := []struct {
		name     string
		response func(http.ResponseWriter)
	}{
		{
			name: "ignored range",
			response: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data"))
			},
		},
		{
			name: "missing content range",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "2")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("at"))
			},
		},
		{
			name: "mismatched bounds",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Range", "bytes 0-1/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("da"))
			},
		},
		{
			name: "mismatched total",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Range", "bytes 1-2/99")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("at"))
			},
		},
		{
			name: "encoded representation",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Range", "bytes 1-2/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("at"))
			},
		},
		{
			name: "known short body",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Content-Range", "bytes 1-2/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("a"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var primaryRequests atomic.Int32
			var fallbackRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/primary":
					primaryRequests.Add(1)
					test.response(w)
				case "/fallback":
					fallbackRequests.Add(1)
					w.Header().Set("Content-Length", "2")
					w.Header().Set("Content-Range", "bytes 1-2/4")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write([]byte("at"))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
				"primary":  {DownloadLink: server.URL + "/primary"},
				"fallback": {DownloadLink: server.URL + "/fallback"},
			}}
			manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
			entry := streamFailoverEntry("primary", "fallback")
			var output bytes.Buffer
			var metadata *StreamMetadata
			readyCalls := 0
			err := manager.Stream(context.Background(), entry, "video.mkv", 1, 2, &output, func(got *StreamMetadata) error {
				readyCalls++
				metadata = got
				return nil
			}, "test")
			if err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			if output.String() != "at" || readyCalls != 1 || metadata == nil ||
				metadata.Header.Get("Content-Range") != "bytes 1-2/4" {
				t.Fatalf("output/ready/metadata = %q/%d/%+v, want clean ranged fallback", output.String(), readyCalls, metadata)
			}
			if got, want := links.callOrder(), []string{"primary", "fallback"}; !slices.Equal(got, want) {
				t.Fatalf("provider calls = %v, want %v", got, want)
			}
			if primaryRequests.Load() != 1 || fallbackRequests.Load() != 1 {
				t.Fatalf("primary/fallback requests = %d/%d, want 1/1", primaryRequests.Load(), fallbackRequests.Load())
			}
		})
	}
}

func TestStreamValidatesFullResponseFramingBeforeCommit(t *testing.T) {
	tests := []struct {
		name     string
		response func(http.ResponseWriter)
	}{
		{
			name: "mismatched full range",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Range", "bytes 0-3/99")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("data"))
			},
		},
		{
			name: "missing full range",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("data"))
			},
		},
		{
			name: "known short full body",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "3")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("dat"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/primary":
					test.response(w)
				case "/fallback":
					w.Header().Set("Content-Length", "4")
					w.Header().Set("Content-Range", "bytes 0-3/4")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write([]byte("data"))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()

			links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
				"primary":  {DownloadLink: server.URL + "/primary"},
				"fallback": {DownloadLink: server.URL + "/fallback"},
			}}
			manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
			entry := streamFailoverEntry("primary", "fallback")
			var output bytes.Buffer
			readyCalls := 0
			if err := manager.Stream(context.Background(), entry, "video.mkv", 0, -1, &output, func(*StreamMetadata) error {
				readyCalls++
				return nil
			}, "test"); err != nil {
				t.Fatalf("Stream() error = %v", err)
			}
			if output.String() != "data" || readyCalls != 1 {
				t.Fatalf("output/ready = %q/%d, want clean full fallback", output.String(), readyCalls)
			}
			if got, want := links.callOrder(), []string{"primary", "fallback"}; !slices.Equal(got, want) {
				t.Fatalf("provider calls = %v, want %v", got, want)
			}
		})
	}
}

func TestStreamPreservesFirstByteRange(t *testing.T) {
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", "1")
		w.Header().Set("Content-Range", "bytes 0-0/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("d"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: server.URL},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary")
	entry := streamFailoverEntry("primary")
	var output bytes.Buffer
	var metadata *StreamMetadata
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 0, &output, func(got *StreamMetadata) error {
		metadata = got
		return nil
	}, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if gotRange != "bytes=0-0" || output.String() != "d" || metadata == nil ||
		metadata.Header.Get("Content-Range") != "bytes 0-0/4" {
		t.Fatalf("range/output/metadata = %q/%q/%+v, want first-byte partial response", gotRange, output.String(), metadata)
	}
}

func TestRootedStreamTranslatesLogicalPartialRange(t *testing.T) {
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 11-12/100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("at"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: server.URL, Size: 100},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary")
	entry := streamFailoverEntry("primary")
	entry.Files["video.mkv"].ByteRange = &[2]int64{10, 13}

	var output bytes.Buffer
	var metadata *StreamMetadata
	if err := manager.Stream(context.Background(), entry, "video.mkv", 1, 2, &output, func(got *StreamMetadata) error {
		metadata = got
		return nil
	}, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if gotRange != "bytes=11-12" || output.String() != "at" {
		t.Fatalf("upstream range/output = %q/%q, want bytes=11-12/at", gotRange, output.String())
	}
	if metadata == nil || metadata.StatusCode != http.StatusPartialContent || metadata.ContentLength != 2 ||
		metadata.Header.Get("Content-Range") != "bytes 1-2/4" {
		t.Fatalf("client metadata = %+v, want logical partial response", metadata)
	}
}

func TestRootedStreamMapsFullLogicalFileToPartialUpstream(t *testing.T) {
	var gotRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 10-13/100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: server.URL, Size: 100},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary")
	entry := streamFailoverEntry("primary")
	entry.Files["video.mkv"].ByteRange = &[2]int64{10, 13}

	var output bytes.Buffer
	var metadata *StreamMetadata
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, -1, &output, func(got *StreamMetadata) error {
		metadata = got
		return nil
	}, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if gotRange != "bytes=10-13" || output.String() != "data" {
		t.Fatalf("upstream range/output = %q/%q, want bytes=10-13/data", gotRange, output.String())
	}
	if metadata == nil || metadata.StatusCode != http.StatusOK || metadata.ContentLength != 4 ||
		metadata.Header.Get("Content-Range") != "" {
		t.Fatalf("client metadata = %+v, want full logical response without archive offsets", metadata)
	}
}

func TestRootedStreamAcceptsWildcardTotalWhenUpstreamSizeIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 11-12/*")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("at"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: server.URL},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary")
	entry := streamFailoverEntry("primary")
	entry.Files["video.mkv"].ByteRange = &[2]int64{10, 13}

	var output bytes.Buffer
	if err := manager.Stream(context.Background(), entry, "video.mkv", 1, 2, &output, nil, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if output.String() != "at" {
		t.Fatalf("output = %q, want at", output.String())
	}
}

func TestRootedStreamRejectsInconsistentStoredRangeBeforeLinkLookup(t *testing.T) {
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: "https://primary.example/file", Size: 100},
	}}
	manager := newStreamFailoverTestManager(links, http.DefaultClient, "primary")
	entry := streamFailoverEntry("primary")
	entry.Files["video.mkv"].ByteRange = &[2]int64{10, 12}

	var output bytes.Buffer
	readyCalls := 0
	err := manager.Stream(context.Background(), entry, "video.mkv", 0, -1, &output, func(*StreamMetadata) error {
		readyCalls++
		return nil
	}, "test")
	if err == nil || !strings.Contains(err.Error(), "rooted byte range size 3 does not match logical file size 4") {
		t.Fatalf("Stream() error = %v, want rooted size mismatch", err)
	}
	if output.Len() != 0 || readyCalls != 0 || len(links.callOrder()) != 0 {
		t.Fatalf("output/ready/link calls = %d/%d/%v, want 0/0/none", output.Len(), readyCalls, links.callOrder())
	}
}

func TestStreamUsesFullActiveRecoveryOnlyAfterAlternateFailure(t *testing.T) {
	var primaryRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/primary":
			if primaryRequests.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Content-Range", "bytes 0-3/4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("data"))
		case "/fallback":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	manager.config.Retries = 0
	entry := streamFailoverEntry("primary", "fallback")
	var output bytes.Buffer
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if output.String() != "data" {
		t.Fatalf("output = %q, want data", output.String())
	}
	if got, want := links.callOrder(), []string{"primary", "fallback", "primary"}; !slices.Equal(got, want) {
		t.Fatalf("provider calls = %v, want %v", got, want)
	}
	stats := manager.StreamFailoverStats()
	if stats.Attempts != 2 || stats.Successes != 0 || stats.Exhausted != 0 {
		t.Fatalf("failover stats = %+v, want attempts=2 successes=0 exhausted=0", stats)
	}
}

type immediateFailureBody struct{}

func (*immediateFailureBody) Read([]byte) (int, error) { return 0, errors.New("upstream reset") }
func (*immediateFailureBody) Close() error             { return nil }

func TestStreamFailsOverWhenBodyFailsBeforeFirstByte(t *testing.T) {
	var requests []string
	var requestMu sync.Mutex
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestMu.Lock()
		requests = append(requests, req.URL.Hostname())
		requestMu.Unlock()
		body := io.ReadCloser(&immediateFailureBody{})
		if req.URL.Hostname() == "fallback.example" {
			body = io.NopCloser(strings.NewReader("data"))
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{"bytes 0-3/4"}},
			Body:          body,
			ContentLength: 4,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	var output bytes.Buffer
	readyCalls := 0
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, func(*StreamMetadata) error {
		readyCalls++
		return nil
	}, "test"); err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if output.String() != "data" || readyCalls != 1 ||
		!slices.Equal(requests, []string{"primary.example", "fallback.example"}) {
		t.Fatalf("output/ready/requests = %q/%d/%v", output.String(), readyCalls, requests)
	}
}

type partialFailureBody struct {
	offset int
}

func (b *partialFailureBody) Read(p []byte) (int, error) {
	const data = "ab"
	if b.offset < len(data) {
		n := copy(p, data[b.offset:])
		b.offset += n
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func (*partialFailureBody) Close() error { return nil }

func TestStreamRejectsInvalidAlternateAfterResponseCommitment(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body := io.ReadCloser(&partialFailureBody{})
		if req.URL.Hostname() == "fallback.example" {
			fallbackRequests.Add(1)
			body = io.NopCloser(strings.NewReader("data"))
		} else {
			primaryRequests.Add(1)
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{"bytes 0-3/4"}},
			Body:          body,
			ContentLength: 4,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	var output bytes.Buffer
	readyCalls := 0
	err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, func(*StreamMetadata) error {
		readyCalls++
		return nil
	}, "test")
	if err == nil {
		t.Fatal("Stream() succeeded, want committed body failure")
	}
	if output.String() != "ab" || readyCalls != 1 || primaryRequests.Load() != 4 || fallbackRequests.Load() != 4 {
		t.Fatalf("output/ready/primary/fallback = %q/%d/%d/%d, want ab/1/4/4", output.String(), readyCalls, primaryRequests.Load(), fallbackRequests.Load())
	}
	stats := manager.StreamFailoverStats()
	if stats.CommittedHandoffs != 1 || stats.HandoffSuccesses != 0 || stats.Exhausted != 1 {
		t.Fatalf("stream failover stats = %+v, want one exhausted committed handoff", stats)
	}
}

func TestStreamResumesInterruptedBodyFromExactNextByte(t *testing.T) {
	var requestMu sync.Mutex
	var ranges []string
	var fallbackRequests atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Hostname() == "fallback.example" {
			fallbackRequests.Add(1)
		}
		requestRange := req.Header.Get("Range")
		requestMu.Lock()
		ranges = append(ranges, requestRange)
		requestMu.Unlock()

		body := io.ReadCloser(&partialFailureBody{})
		contentRange := "bytes 0-3/4"
		contentLength := int64(4)
		if requestRange == "bytes=2-3" {
			body = io.NopCloser(strings.NewReader("cd"))
			contentRange = "bytes 2-3/4"
			contentLength = 2
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{contentRange}},
			Body:          body,
			ContentLength: contentLength,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	var output bytes.Buffer
	readyCalls := 0
	err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, func(*StreamMetadata) error {
		readyCalls++
		return nil
	}, "test")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	requestMu.Lock()
	gotRanges := append([]string(nil), ranges...)
	requestMu.Unlock()
	if output.String() != "abcd" || readyCalls != 1 || fallbackRequests.Load() != 0 {
		t.Fatalf(
			"output/ready/fallback = %q/%d/%d, want abcd/1/0",
			output.String(),
			readyCalls,
			fallbackRequests.Load(),
		)
	}
	if want := []string{"bytes=0-3", "bytes=2-3"}; !slices.Equal(gotRanges, want) {
		t.Fatalf("request ranges = %v, want %v", gotRanges, want)
	}
	sessionStats := manager.StreamSessionStats()
	if sessionStats.HTTPResumeAttempts != 1 || sessionStats.HTTPResumeSuccesses != 1 {
		t.Fatalf("HTTP resume stats = %+v, want one successful resume", sessionStats)
	}
}

func TestStreamHandsOffUnreadSuffixToIdenticalHashProvider(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestRange := req.Header.Get("Range")
		if req.URL.Hostname() == "fallback.example" {
			fallbackRequests.Add(1)
			return &http.Response{
				StatusCode:    http.StatusPartialContent,
				Header:        http.Header{"Content-Range": []string{"bytes 2-3/4"}},
				Body:          io.NopCloser(strings.NewReader("cd")),
				ContentLength: 2,
				Request:       req,
			}, nil
		}

		requestNumber := primaryRequests.Add(1)
		body := io.ReadCloser(&partialFailureBody{})
		contentRange := "bytes 0-3/4"
		contentLength := int64(4)
		if requestNumber > 1 {
			body = &immediateFailureBody{}
			contentRange = "bytes 2-3/4"
			contentLength = 2
		}
		if requestNumber == 1 && requestRange != "bytes=0-3" {
			t.Errorf("initial Range = %q, want bytes=0-3", requestRange)
		}
		if requestNumber > 1 && requestRange != "bytes=2-3" {
			t.Errorf("resume Range = %q, want bytes=2-3", requestRange)
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{contentRange}},
			Body:          body,
			ContentLength: contentLength,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	var output bytes.Buffer
	readyCalls := 0
	err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, func(*StreamMetadata) error {
		readyCalls++
		return nil
	}, "test")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	if output.String() != "abcd" || readyCalls != 1 ||
		primaryRequests.Load() != 2 || fallbackRequests.Load() != 1 {
		t.Fatalf(
			"output/ready/primary/fallback = %q/%d/%d/%d, want abcd/1/2/1",
			output.String(),
			readyCalls,
			primaryRequests.Load(),
			fallbackRequests.Load(),
		)
	}
	if got := links.callOrder(); !slices.Equal(got, []string{"primary", "fallback"}) {
		t.Fatalf("provider link calls = %v, want primary then fallback", got)
	}
	stats := manager.StreamFailoverStats()
	if stats.CommittedHandoffs != 1 || stats.HandoffSuccesses != 1 ||
		stats.Attempts != 1 || stats.Successes != 1 ||
		stats.FileCircuitOpens != 1 || stats.OpenFileCircuits != 1 {
		t.Fatalf("stream failover stats = %+v, want one successful handoff with failed primary isolated", stats)
	}
	sessionStats := manager.StreamSessionStats()
	if sessionStats.HTTPResumeAttempts != 2 || sessionStats.HTTPResumeSuccesses != 1 {
		t.Fatalf("HTTP resume stats = %+v, want failed primary resume and successful handoff resume", sessionStats)
	}
	candidates := manager.streamCandidates(entry, "video.mkv")
	if len(candidates) != 2 || candidates[0].provider != "fallback" || candidates[1].provider != "primary" {
		t.Fatalf("post-handoff candidates = %+v, want fallback before isolated primary", candidates)
	}
}

func TestStreamCarriesConfirmedCursorAcrossMultipleProviders(t *testing.T) {
	var requestMu sync.Mutex
	var requests []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		host := req.URL.Hostname()
		requestRange := req.Header.Get("Range")
		requestMu.Lock()
		requests = append(requests, host+" "+requestRange)
		requestMu.Unlock()

		var body io.ReadCloser
		var contentRange string
		var contentLength int64
		switch host + " " + requestRange {
		case "primary.example bytes=0-3":
			body = &partialFailureBody{}
			contentRange = "bytes 0-3/4"
			contentLength = 4
		case "primary.example bytes=2-3":
			body = &immediateFailureBody{}
			contentRange = "bytes 2-3/4"
			contentLength = 2
		case "beta.example bytes=2-3":
			body = &terminalErrorBody{data: "c", err: io.ErrUnexpectedEOF}
			contentRange = "bytes 2-3/4"
			contentLength = 2
		case "beta.example bytes=3-3":
			body = &immediateFailureBody{}
			contentRange = "bytes 3-3/4"
			contentLength = 1
		case "gamma.example bytes=3-3":
			body = io.NopCloser(strings.NewReader("d"))
			contentRange = "bytes 3-3/4"
			contentLength = 1
		default:
			t.Fatalf("unexpected request %s %s", host, requestRange)
			return nil, errors.New("unexpected request")
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{contentRange}},
			Body:          body,
			ContentLength: contentLength,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {DownloadLink: "https://primary.example/file"},
		"beta":    {DownloadLink: "https://beta.example/file"},
		"gamma":   {DownloadLink: "https://gamma.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "beta", "gamma")

	var output bytes.Buffer
	err := manager.Stream(
		context.Background(),
		streamFailoverEntry("primary", "beta", "gamma"),
		"video.mkv",
		0,
		3,
		&output,
		nil,
		"test",
	)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	requestMu.Lock()
	gotRequests := append([]string(nil), requests...)
	requestMu.Unlock()
	wantRequests := []string{
		"primary.example bytes=0-3",
		"primary.example bytes=2-3",
		"beta.example bytes=2-3",
		"beta.example bytes=3-3",
		"gamma.example bytes=3-3",
	}
	if output.String() != "abcd" || !slices.Equal(gotRequests, wantRequests) {
		t.Fatalf("output/requests = %q/%v, want abcd/%v", output.String(), gotRequests, wantRequests)
	}
	stats := manager.StreamFailoverStats()
	if stats.CommittedHandoffs != 2 || stats.HandoffSuccesses != 1 {
		t.Fatalf("stream failover stats = %+v, want two handoffs and one success", stats)
	}
}

type terminalErrorBody struct {
	data string
	err  error
	done bool
}

func (b *terminalErrorBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, b.err
	}
	b.done = true
	return copy(p, b.data), b.err
}

func (*terminalErrorBody) Close() error { return nil }

func TestStreamResumesRootedArchiveBodyFromExactUpstreamByte(t *testing.T) {
	var requestMu sync.Mutex
	var ranges []string
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestRange := req.Header.Get("Range")
		requestMu.Lock()
		ranges = append(ranges, requestRange)
		requestMu.Unlock()

		body := io.ReadCloser(&partialFailureBody{})
		contentRange := "bytes 10-13/100"
		contentLength := int64(4)
		if requestRange == "bytes=12-13" {
			body = io.NopCloser(strings.NewReader("cd"))
			contentRange = "bytes 12-13/100"
			contentLength = 2
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{contentRange}},
			Body:          body,
			ContentLength: contentLength,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary": {
			DownloadLink: "https://primary.example/archive",
			Size:         100,
		},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary")
	entry := streamFailoverEntry("primary")
	entry.Files["video.mkv"].ByteRange = &[2]int64{10, 13}

	var output bytes.Buffer
	var metadata *StreamMetadata
	err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, func(got *StreamMetadata) error {
		metadata = got
		return nil
	}, "test")
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	requestMu.Lock()
	gotRanges := append([]string(nil), ranges...)
	requestMu.Unlock()
	if output.String() != "abcd" {
		t.Fatalf("output = %q, want abcd", output.String())
	}
	if want := []string{"bytes=10-13", "bytes=12-13"}; !slices.Equal(gotRanges, want) {
		t.Fatalf("request ranges = %v, want %v", gotRanges, want)
	}
	if metadata == nil || metadata.StatusCode != http.StatusOK || metadata.ContentLength != 4 ||
		metadata.Header.Get("Content-Range") != "" {
		t.Fatalf("logical metadata = %+v, want full four-byte logical response", metadata)
	}
}

func TestStreamBoundsInvalidSameProviderResume(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Hostname() == "fallback.example" {
			fallbackRequests.Add(1)
		} else {
			primaryRequests.Add(1)
		}
		body := io.ReadCloser(&partialFailureBody{})
		contentRange := "bytes 0-3/4"
		contentLength := int64(4)
		if req.Header.Get("Range") == "bytes=2-3" {
			body = io.NopCloser(strings.NewReader("cd"))
			contentRange = "bytes 0-1/4"
			contentLength = 2
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{contentRange}},
			Body:          body,
			ContentLength: contentLength,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")

	var output bytes.Buffer
	err := manager.Stream(
		context.Background(),
		streamFailoverEntry("primary", "fallback"),
		"video.mkv",
		0,
		3,
		&output,
		nil,
		"test",
	)
	if err == nil || !strings.Contains(err.Error(), "does not match requested range 2-3") {
		t.Fatalf("Stream() error = %v, want invalid resumed Content-Range", err)
	}
	if output.String() != "ab" || primaryRequests.Load() != 4 || fallbackRequests.Load() != 4 {
		t.Fatalf(
			"output/primary/fallback = %q/%d/%d, want ab/4/4",
			output.String(),
			primaryRequests.Load(),
			fallbackRequests.Load(),
		)
	}
}

func TestStreamSinkFailureNeverFallsThroughOrDegradesProvider(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Hostname() == "fallback.example" {
			fallbackRequests.Add(1)
		} else {
			primaryRequests.Add(1)
		}
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{"bytes 0-3/4"}},
			Body:          io.NopCloser(strings.NewReader("data")),
			ContentLength: 4,
			Request:       req,
		}, nil
	})}
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: "https://primary.example/file"},
		"fallback": {DownloadLink: "https://fallback.example/file"},
	}}
	manager := newStreamFailoverTestManager(links, client, "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")
	writer := &failingStreamWriter{limit: 2, err: errTestSinkClosed}

	err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, writer, nil, "test")
	if !errors.Is(err, errTestSinkClosed) {
		t.Fatalf("Stream() error = %v, want sink failure", err)
	}
	if writer.written != 2 || primaryRequests.Load() != 1 || fallbackRequests.Load() != 0 {
		t.Fatalf(
			"written/primary/fallback = %d/%d/%d, want 2/1/0",
			writer.written,
			primaryRequests.Load(),
			fallbackRequests.Load(),
		)
	}
	if manager.streamProviderWeather.candidateDegraded("primary") {
		t.Fatal("client sink failure degraded the upstream provider")
	}
	if stats := manager.StreamFailoverStats(); stats.FileCircuitOpens != 0 || stats.OpenFileCircuits != 0 {
		t.Fatalf("client sink failure opened file/provider circuit: %+v", stats)
	}
}

func TestStreamCancellationNeverFallsThroughToAnotherProvider(t *testing.T) {
	links := &failoverLinkService{
		links: map[string]debridTypes.DownloadLink{
			"fallback": {DownloadLink: "https://fallback.example/file"},
		},
		errs: map[string]error{"primary": context.Canceled},
	}
	manager := newStreamFailoverTestManager(links, http.DefaultClient, "primary", "fallback")
	err := manager.Stream(context.Background(), streamFailoverEntry("primary", "fallback"), "video.mkv", 0, 3, io.Discard, nil, "test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Stream() error = %v, want context cancellation", err)
	}
	if got := links.callOrder(); !slices.Equal(got, []string{"primary"}) {
		t.Fatalf("provider calls = %v, want primary only", got)
	}
}

func TestConcurrentStreamFailoverIsRaceSafe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/primary" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	const streams = 24
	start := make(chan struct{})
	errorsByStream := make(chan error, streams)
	var group sync.WaitGroup
	group.Add(streams)
	for range streams {
		go func() {
			defer group.Done()
			<-start
			var output bytes.Buffer
			if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
				errorsByStream <- err
				return
			}
			if output.String() != "data" {
				errorsByStream <- fmt.Errorf("output = %q, want data", output.String())
			}
		}()
	}
	close(start)
	group.Wait()
	close(errorsByStream)
	for err := range errorsByStream {
		t.Error(err)
	}
	if manager.StreamFailoverStats().Successes == 0 {
		t.Fatal("concurrent requests never recorded a successful alternate provider")
	}
}

func TestStreamReportsExhaustedEligibleProviders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	manager.config.Retries = 0
	err := manager.Stream(context.Background(), streamFailoverEntry("primary", "fallback"), "video.mkv", 0, 3, io.Discard, nil, "test")
	if err == nil || !strings.Contains(err.Error(), "all eligible stream providers failed") {
		t.Fatalf("Stream() error = %v, want exhausted-provider error", err)
	}
	stats := manager.StreamFailoverStats()
	if stats.Attempts != 2 || stats.Successes != 0 || stats.Exhausted != 1 {
		t.Fatalf("failover stats = %+v, want attempts=2 successes=0 exhausted=1", stats)
	}
}

func BenchmarkStreamCandidatesLargeTorrent(b *testing.B) {
	entry := streamFailoverEntry("primary", "fallback", "third")
	for index := range 2_000 {
		name := fmt.Sprintf("extra-%04d.mkv", index)
		entry.Files[name] = &storage.File{Name: name, Size: 4}
		for _, placement := range entry.Providers {
			placement.Files[name] = &storage.ProviderFile{Link: placement.Provider + "://restricted/" + name}
		}
	}
	manager := newStreamFailoverTestManager(&failoverLinkService{}, http.DefaultClient, "primary", "fallback", "third")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		candidates := manager.streamCandidates(entry, "video.mkv")
		if len(candidates) != 3 {
			b.Fatalf("candidate count = %d, want 3", len(candidates))
		}
	}
}
