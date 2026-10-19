package link

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

func TestDoLinkFlightBoundsSharedWork(t *testing.T) {
	t.Parallel()
	var group singleflight.Group
	before := time.Now()
	_, err := doLinkFlight(context.Background(), &group, "bounded", func(ctx context.Context) (types.DownloadLink, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return types.DownloadLink{}, errors.New("shared link context has no deadline")
		}
		if deadline.After(before.Add(sharedLinkTimeout + time.Second)) {
			return types.DownloadLink{}, fmt.Errorf("shared link deadline %s exceeds timeout", deadline)
		}
		return types.DownloadLink{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type lifecycleTestClient struct {
	debrid.Client
	mu            sync.Mutex
	links         []types.DownloadLink
	fetchErr      error
	fetchErrLink  types.DownloadLink
	cacheLinks    bool
	cached        map[string]types.DownloadLink
	fetches       int
	invalidations int
	accounts      *account.Manager
}

type doneObservedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newDoneObservedContext(ctx context.Context) *doneObservedContext {
	return &doneObservedContext{Context: ctx, observed: make(chan struct{})}
}

func (c *doneObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func (c *lifecycleTestClient) GetDownloadLink(_ string, file *types.File) (types.DownloadLink, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cacheLinks {
		if cached, ok := c.cached[file.Link]; ok {
			return cached, nil
		}
	}
	c.fetches++
	if c.fetchErr != nil {
		return c.fetchErrLink, c.fetchErr
	}
	if len(c.links) == 0 {
		return types.DownloadLink{}, fmt.Errorf("no test links configured")
	}
	index := min(c.fetches-1, len(c.links)-1)
	link := c.links[index]
	if c.cacheLinks {
		if c.cached == nil {
			c.cached = make(map[string]types.DownloadLink)
		}
		c.cached[file.Link] = link
	}
	return link, nil
}

func (c *lifecycleTestClient) InvalidateCachedLink(link types.DownloadLink) error {
	c.mu.Lock()
	c.invalidations++
	delete(c.cached, link.Link)
	c.mu.Unlock()
	return nil
}

func (c *lifecycleTestClient) AccountManager() *account.Manager {
	return c.accounts
}

func (c *lifecycleTestClient) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetches, c.invalidations
}

func lifecycleTestEntry() *storage.Entry {
	const filename = "video.mkv"
	return &storage.Entry{
		InfoHash:       "hash",
		Name:           "entry",
		ActiveProvider: "test",
		Files: map[string]*storage.File{
			filename: {Name: filename, Size: 4},
		},
		Providers: map[string]*storage.ProviderEntry{
			"test": {
				Provider: "test",
				ID:       "torrent",
				Files: map[string]*storage.ProviderFile{
					filename: {Link: "restricted-link"},
				},
			},
		},
	}
}

func lifecycleDownloadLink(url string) types.DownloadLink {
	return types.DownloadLink{
		Debrid:       "test",
		Token:        "token",
		Filename:     "video.mkv",
		Link:         "restricted-link",
		DownloadLink: url,
		Size:         4,
		Generated:    time.Now(),
		ExpiresAt:    time.Now().Add(time.Hour),
	}
}

func writeValidLinkProbe(w http.ResponseWriter) {
	w.Header().Set("Content-Length", "2")
	w.Header().Set("Content-Range", "bytes 0-1/4")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write([]byte("da"))
}

func newLifecycleService(client *lifecycleTestClient, httpClient *http.Client, retries int) *Service {
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("test", client)
	return New(clients, nil, nil, nil, httpClient, retries, zerolog.Nop())
}

func TestGetLinkOwnerCancellationDoesNotCancelSharedResolution(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseRequest) })
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if probes.Add(1) == 1 {
			close(requestStarted)
		}
		<-releaseRequest
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() {
		_, err := service.GetLink(ownerCtx, entry, "video.mkv")
		ownerResult <- err
	}()
	<-requestStarted

	cancelOwner()
	if err := <-ownerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner GetLink() error = %v, want context cancellation", err)
	}

	type result struct {
		link types.DownloadLink
		err  error
	}
	waiterCtx := newDoneObservedContext(context.Background())
	waiterResult := make(chan result, 1)
	go func() {
		link, err := service.GetLink(waiterCtx, entry, "video.mkv")
		waiterResult <- result{link: link, err: err}
	}()
	<-waiterCtx.observed

	select {
	case result := <-waiterResult:
		t.Fatalf("joined GetLink() completed before shared request was released: %+v", result)
	default:
	}
	if fetches, _ := client.counts(); fetches != 1 || probes.Load() != 1 {
		t.Fatalf("provider fetches/range probes = %d/%d, want one shared operation", fetches, probes.Load())
	}

	releaseOnce.Do(func() { close(releaseRequest) })
	waiter := <-waiterResult
	if waiter.err != nil || waiter.link.DownloadLink != server.URL {
		t.Fatalf("joined GetLink() = %q, %v; want shared success", waiter.link.DownloadLink, waiter.err)
	}
}

func TestRefreshOwnerCancellationDoesNotCancelSharedRegeneration(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseRequest) })
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if probes.Add(1) == 1 {
			close(requestStarted)
		}
		<-releaseRequest
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	rejected := lifecycleDownloadLink("https://cdn.example/rejected")
	replacement := lifecycleDownloadLink(server.URL)
	client := &lifecycleTestClient{cacheLinks: true, links: []types.DownloadLink{replacement}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerResult := make(chan error, 1)
	go func() {
		_, err := service.Refresh(ownerCtx, entry, "video.mkv", rejected)
		ownerResult <- err
	}()
	<-requestStarted

	cancelOwner()
	if err := <-ownerResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner Refresh() error = %v, want context cancellation", err)
	}

	type result struct {
		link types.DownloadLink
		err  error
	}
	waiterCtx := newDoneObservedContext(context.Background())
	waiterResult := make(chan result, 1)
	go func() {
		link, err := service.Refresh(waiterCtx, entry, "video.mkv", rejected)
		waiterResult <- result{link: link, err: err}
	}()
	<-waiterCtx.observed

	select {
	case result := <-waiterResult:
		t.Fatalf("joined Refresh() completed before shared request was released: %+v", result)
	default:
	}
	fetches, invalidations := client.counts()
	if fetches != 1 || invalidations != 1 || probes.Load() != 1 {
		t.Fatalf("provider fetches/invalidations/range probes = %d/%d/%d, want one shared regeneration", fetches, invalidations, probes.Load())
	}

	releaseOnce.Do(func() { close(releaseRequest) })
	waiter := <-waiterResult
	if waiter.err != nil || waiter.link.DownloadLink != replacement.DownloadLink {
		t.Fatalf("joined Refresh() = %q, %v; want shared replacement", waiter.link.DownloadLink, waiter.err)
	}
}

func TestWithoutRepairNeverMutatesAlternatePlacement(t *testing.T) {
	client := &lifecycleTestClient{fetchErr: customerror.HosterUnavailableError}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("test", client)
	var repairs atomic.Int32
	var saves atomic.Int32
	service := New(
		clients,
		nil,
		func(context.Context, *storage.Entry) error {
			repairs.Add(1)
			return nil
		},
		func(*storage.Entry) error {
			saves.Add(1)
			return nil
		},
		http.DefaultClient,
		0,
		zerolog.Nop(),
	)

	entry := lifecycleTestEntry()
	_, err := service.GetLink(WithoutRepair(context.Background()), entry, "video.mkv")
	if !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("GetLink() error = %v, want hoster unavailable", err)
	}
	if repairs.Load() != 0 || saves.Load() != 0 || entry.Bad {
		t.Fatalf("read-only alternate repairs/saves/bad = %d/%d/%v, want 0/0/false", repairs.Load(), saves.Load(), entry.Bad)
	}
}

func TestReadOnlyProbePreservesFullActiveRepairRecovery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{
		fetchErr:     customerror.HosterUnavailableError,
		fetchErrLink: lifecycleDownloadLink(server.URL),
		links:        []types.DownloadLink{lifecycleDownloadLink(server.URL)},
	}
	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store("test", client)
	var repairs atomic.Int32
	service := New(
		clients,
		nil,
		func(context.Context, *storage.Entry) error {
			repairs.Add(1)
			client.mu.Lock()
			client.fetchErr = nil
			client.mu.Unlock()
			return nil
		},
		nil,
		server.Client(),
		0,
		zerolog.Nop(),
	)
	entry := lifecycleTestEntry()

	if _, err := service.GetLink(WithoutRepair(WithFailFast(context.Background())), entry, "video.mkv"); !errors.Is(err, customerror.HosterUnavailableError) {
		t.Fatalf("read-only GetLink() error = %v, want hoster unavailable", err)
	}
	if repairs.Load() != 0 {
		t.Fatalf("read-only repairs = %d, want 0", repairs.Load())
	}
	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err != nil {
		t.Fatalf("full recovery GetLink() error = %v", err)
	}
	if repairs.Load() != 1 {
		t.Fatalf("full recovery repairs = %d, want 1", repairs.Load())
	}
}

func TestFailFastValidationSkipsRetryDelay(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if probes.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 3)
	var waits atomic.Int32
	service.wait = func(context.Context, time.Duration) error {
		waits.Add(1)
		return nil
	}

	_, err := service.GetLink(WithFailFast(context.Background()), lifecycleTestEntry(), "video.mkv")
	if err == nil {
		t.Fatal("GetLink() succeeded, want first-attempt transient failure")
	}
	if probes.Load() != 1 || waits.Load() != 0 {
		t.Fatalf("range probes/waits = %d/%d, want 1/0", probes.Load(), waits.Load())
	}
}

func TestTransientValidationFailureIsNotSticky(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probes.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()

	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err == nil {
		t.Fatal("first GetLink() succeeded, want transient failure")
	}
	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err != nil {
		t.Fatalf("second GetLink() error = %v", err)
	}
	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err != nil {
		t.Fatalf("memoized GetLink() error = %v", err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("range probes = %d, want 2", got)
	}
}

func TestThrottleBacksOffWithoutGeneratingAnotherLink(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probes.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 1)
	var waits []time.Duration
	service.wait = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}

	if _, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv"); err != nil {
		t.Fatalf("GetLink() error = %v", err)
	}
	fetches, invalidations := client.counts()
	if fetches != 1 || invalidations != 0 {
		t.Fatalf("fetches/invalidations = %d/%d, want 1/0", fetches, invalidations)
	}
	if len(waits) != 1 || waits[0] != 3*time.Second {
		t.Fatalf("waits = %v, want [3s]", waits)
	}
}

func TestRejectedLinkRefreshesOnceAndValidatesReplacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/old" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{
		lifecycleDownloadLink(server.URL + "/old"),
		lifecycleDownloadLink(server.URL + "/new"),
	}}
	service := newLifecycleService(client, server.Client(), 0)
	got, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv")
	if err != nil {
		t.Fatalf("GetLink() error = %v", err)
	}
	if got.DownloadLink != server.URL+"/new" {
		t.Fatalf("download link = %q, want replacement", got.DownloadLink)
	}
	fetches, invalidations := client.counts()
	if fetches != 2 || invalidations != 1 {
		t.Fatalf("fetches/invalidations = %d/%d, want 2/1", fetches, invalidations)
	}
}

func TestRejectedReplacementStartsRefreshCooldown(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	client := &lifecycleTestClient{cacheLinks: true, links: []types.DownloadLink{
		lifecycleDownloadLink(server.URL + "/old"),
		lifecycleDownloadLink(server.URL + "/new"),
		lifecycleDownloadLink(server.URL + "/unexpected"),
	}}
	service := newLifecycleService(client, server.Client(), 0)
	var logs bytes.Buffer
	service.logger = zerolog.New(&logs)
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	entry := lifecycleTestEntry()

	_, err := service.GetLink(context.Background(), entry, "video.mkv")
	if err == nil {
		t.Fatal("GetLink() succeeded, want rejected replacement error")
	}
	if linkErr := GetLinkError(err); linkErr == nil || !linkErr.ShouldRefetch() || linkErr.Code != "403" {
		t.Fatalf("error = %v, want original refetchable rejection", err)
	}

	for attempt := range 2 {
		_, err = service.GetLink(context.Background(), entry, "video.mkv")
		linkErr := GetLinkError(err)
		if linkErr == nil || !linkErr.ShouldBackoff() || linkErr.Code != "link_refresh_cooldown" {
			t.Fatalf("cooldown attempt %d error = %v, want throttled refresh cooldown", attempt+1, err)
		}
		if linkErr.RetryAfter != refreshBackoffBase {
			t.Fatalf("cooldown attempt %d RetryAfter = %s, want %s", attempt+1, linkErr.RetryAfter, refreshBackoffBase)
		}
	}
	fetches, invalidations := client.counts()
	if fetches != 2 || invalidations != 1 {
		t.Fatalf("provider fetches/invalidations = %d/%d, want bounded 2/1", fetches, invalidations)
	}
	if got := probes.Load(); got != 4 {
		t.Fatalf("range probes = %d, want 4 so the cached CDN URL remains probeable", got)
	}
	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &event); err != nil {
		t.Fatalf("decode replacement warning: %v", err)
	}
	if event["error_code"] != "403" || event["error_category"] != "refetchable" || event["failure_scope"] != "download_link" {
		t.Fatalf("replacement warning lacks scoped rejection cause: %v", event)
	}
	for _, sensitive := range []string{server.URL, "restricted-link", "token"} {
		if strings.Contains(logs.String(), sensitive) {
			t.Fatalf("replacement warning exposed link credentials or URL: %s", logs.String())
		}
	}
}

func TestRefreshCooldownClearsWhenCachedLinkRecovers(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusForbidden)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := int(status.Load()); got != http.StatusOK {
			w.WriteHeader(got)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{cacheLinks: true, links: []types.DownloadLink{
		lifecycleDownloadLink(server.URL + "/old"),
		lifecycleDownloadLink(server.URL + "/replacement"),
	}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()

	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err == nil {
		t.Fatal("first GetLink() succeeded, want rejected replacement")
	}
	status.Store(http.StatusOK)
	got, err := service.GetLink(context.Background(), entry, "video.mkv")
	if err != nil {
		t.Fatalf("recovery GetLink() error = %v", err)
	}
	if got.DownloadLink != server.URL+"/replacement" {
		t.Fatalf("recovered link = %q, want cached replacement", got.DownloadLink)
	}
	fetches, invalidations := client.counts()
	if fetches != 2 || invalidations != 1 {
		t.Fatalf("provider fetches/invalidations = %d/%d, want 2/1 without another regeneration", fetches, invalidations)
	}
	service.refreshMu.Lock()
	remaining := len(service.refreshBackoffs)
	service.refreshMu.Unlock()
	if remaining != 0 {
		t.Fatalf("refresh backoffs = %d, want recovery to clear state", remaining)
	}
}

func TestRefreshCooldownExpiresAndBacksOffExponentially(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	client := &lifecycleTestClient{cacheLinks: true, links: []types.DownloadLink{
		lifecycleDownloadLink(server.URL + "/old"),
		lifecycleDownloadLink(server.URL + "/replacement-1"),
		lifecycleDownloadLink(server.URL + "/replacement-2"),
	}}
	service := newLifecycleService(client, server.Client(), 0)
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	entry := lifecycleTestEntry()

	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err == nil {
		t.Fatal("first GetLink() succeeded, want rejected replacement")
	}
	now = now.Add(refreshBackoffBase)
	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err == nil {
		t.Fatal("post-cooldown GetLink() succeeded, want second rejected replacement")
	}

	_, err := service.GetLink(context.Background(), entry, "video.mkv")
	linkErr := GetLinkError(err)
	if linkErr == nil || linkErr.Code != "link_refresh_cooldown" || linkErr.RetryAfter != 2*refreshBackoffBase {
		t.Fatalf("error = %v (RetryAfter %v), want second 1m cooldown", err, linkErrRetryAfter(linkErr))
	}
	fetches, invalidations := client.counts()
	if fetches != 3 || invalidations != 2 {
		t.Fatalf("provider fetches/invalidations = %d/%d, want one new refresh after expiry (3/2)", fetches, invalidations)
	}
}

func linkErrRetryAfter(err *Error) time.Duration {
	if err == nil {
		return 0
	}
	return err.RetryAfter
}

func TestGetLinkAndRefreshShareOneProviderRegeneration(t *testing.T) {
	replacementStarted := make(chan struct{})
	releaseReplacement := make(chan struct{})
	var startOnce sync.Once
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		switch r.URL.Path {
		case "/old":
			w.WriteHeader(http.StatusForbidden)
		case "/replacement":
			startOnce.Do(func() { close(replacementStarted) })
			<-releaseReplacement
			writeValidLinkProbe(w)
		default:
			writeValidLinkProbe(w)
		}
	}))
	defer server.Close()

	old := lifecycleDownloadLink(server.URL + "/old")
	replacement := lifecycleDownloadLink(server.URL + "/replacement")
	unexpected := lifecycleDownloadLink(server.URL + "/unexpected")
	old.Filename, replacement.Filename, unexpected.Filename = "bundle.rar", "bundle.rar", "bundle.rar"
	client := &lifecycleTestClient{cacheLinks: true, links: []types.DownloadLink{old, replacement, unexpected}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()

	type refreshResult struct {
		link types.DownloadLink
		err  error
	}
	firstResult := make(chan refreshResult, 1)
	go func() {
		link, err := service.GetLink(context.Background(), entry, "video.mkv")
		firstResult <- refreshResult{link: link, err: err}
	}()
	<-replacementStarted

	releaseTimer := time.AfterFunc(100*time.Millisecond, func() { close(releaseReplacement) })
	secondLink, secondErr := service.Refresh(context.Background(), entry, "video.mkv", old)
	first := <-firstResult
	_ = releaseTimer.Stop()
	if first.err != nil || secondErr != nil {
		t.Fatalf("concurrent GetLink()/Refresh() errors = %v / %v", first.err, secondErr)
	}
	if first.link.DownloadLink != replacement.DownloadLink || secondLink.DownloadLink != replacement.DownloadLink {
		t.Fatalf("concurrent links = %q / %q, want shared replacement", first.link.DownloadLink, secondLink.DownloadLink)
	}
	fetches, invalidations := client.counts()
	if fetches != 2 || invalidations != 1 || probes.Load() != 2 {
		t.Fatalf("provider fetches/invalidations/range probes = %d/%d/%d, want 2/1/2", fetches, invalidations, probes.Load())
	}
}

func TestCanceledRefreshDoesNotCreateBackoff(t *testing.T) {
	client := &lifecycleTestClient{}
	service := newLifecycleService(client, http.DefaultClient, 0)
	entry := lifecycleTestEntry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.Refresh(ctx, entry, "video.mkv", lifecycleDownloadLink("https://cdn.example/rejected"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Refresh() error = %v, want context cancellation", err)
	}
	fetches, invalidations := client.counts()
	if fetches != 0 || invalidations != 0 {
		t.Fatalf("provider fetches/invalidations = %d/%d, want 0/0", fetches, invalidations)
	}
	service.refreshMu.Lock()
	states := len(service.refreshBackoffs)
	service.refreshMu.Unlock()
	if states != 0 {
		t.Fatalf("refresh backoffs = %d, want none after cancellation", states)
	}
}

func TestRefreshBackoffIsScopedByProviderPlacementAndFile(t *testing.T) {
	service := newLifecycleService(&lifecycleTestClient{}, http.DefaultClient, 0)
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }

	entry := lifecycleTestEntry()
	baseKey := linkLifecycleKey(entry, "video.mkv")
	fileKey := linkLifecycleKey(entry, "extras/trailer.mkv")
	otherPlacement := lifecycleTestEntry()
	otherPlacement.Providers["test"].ID = "replacement-torrent"
	placementKey := linkLifecycleKey(otherPlacement, "video.mkv")
	otherProvider := lifecycleTestEntry()
	otherProvider.ActiveProvider = "other"
	otherProvider.Providers["other"] = &storage.ProviderEntry{Provider: "other", ID: "torrent"}
	providerKey := linkLifecycleKey(otherProvider, "video.mkv")

	service.recordRefreshFailure(baseKey)
	if _, blocked := service.refreshDelay(baseKey); !blocked {
		t.Fatal("base file is not in cooldown")
	}
	for label, key := range map[string]string{
		"file":      fileKey,
		"placement": placementKey,
		"provider":  providerKey,
	} {
		if _, blocked := service.refreshDelay(key); blocked {
			t.Fatalf("%s key unexpectedly inherited another file's cooldown", label)
		}
	}
}

func TestRefreshBackoffStateIsBoundedAndClearable(t *testing.T) {
	service := newLifecycleService(&lifecycleTestClient{}, http.DefaultClient, 0)
	service.now = func() time.Time {
		return time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	}

	for index := range maxRefreshBackoffs + 17 {
		service.recordRefreshFailure(fmt.Sprintf("key-%d", index))
	}
	service.refreshMu.Lock()
	states := len(service.refreshBackoffs)
	service.refreshMu.Unlock()
	if states != maxRefreshBackoffs {
		t.Fatalf("refresh backoffs = %d, want bounded %d", states, maxRefreshBackoffs)
	}

	service.validated.Store("validated", struct{}{})
	service.Clear()
	service.refreshMu.Lock()
	states = len(service.refreshBackoffs)
	service.refreshMu.Unlock()
	if states != 0 || service.validated.Size() != 0 {
		t.Fatalf("state after Clear() = %d backoffs/%d validations, want 0/0", states, service.validated.Size())
	}
}

func TestRefreshBackoffDelayCapsAtMaximum(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 1, want: 30 * time.Second},
		{failures: 2, want: time.Minute},
		{failures: 3, want: 2 * time.Minute},
		{failures: 4, want: 4 * time.Minute},
		{failures: 5, want: 5 * time.Minute},
		{failures: 20, want: 5 * time.Minute},
	}
	for _, test := range tests {
		if got := refreshBackoffDelay(test.failures); got != test.want {
			t.Errorf("refreshBackoffDelay(%d) = %s, want %s", test.failures, got, test.want)
		}
	}
}

func TestValidationUsesTwoByteRangeProbe(t *testing.T) {
	var methods []string
	var encoding string
	var cacheControl string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+":"+r.Header.Get("Range"))
		encoding = r.Header.Get("Accept-Encoding")
		cacheControl = r.Header.Get("Cache-Control")
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 0)
	if _, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv"); err != nil {
		t.Fatalf("GetLink() error = %v", err)
	}
	want := []string{"GET:bytes=0-1"}
	if fmt.Sprint(methods) != fmt.Sprint(want) {
		t.Fatalf("requests = %v, want %v", methods, want)
	}
	if encoding != "identity" {
		t.Fatalf("Accept-Encoding = %q, want identity", encoding)
	}
	if cacheControl != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", cacheControl)
	}
}

func TestValidationRejectsMalformedRangeProbe(t *testing.T) {
	tests := []struct {
		name     string
		response func(http.ResponseWriter)
		code     string
	}{
		{
			name: "ignored range",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "4")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("data"))
			},
			code: "range_probe_status",
		},
		{
			name: "missing content range",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "1")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("d"))
			},
			code: "range_probe_content_range",
		},
		{
			name: "wrong bounds",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Content-Range", "bytes 1-1/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("a"))
			},
			code: "range_probe_content_range",
		},
		{
			name: "wrong total",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Content-Range", "bytes 0-1/99")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("d"))
			},
			code: "range_probe_content_range",
		},
		{
			name: "wrong content length",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "3")
				w.Header().Set("Content-Range", "bytes 0-1/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("ddd"))
			},
			code: "range_probe_content_length",
		},
		{
			name: "empty chunked body",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Range", "bytes 0-1/4")
				w.WriteHeader(http.StatusPartialContent)
				w.(http.Flusher).Flush()
			},
			code: "range_probe_body_length",
		},
		{
			name: "overlong chunked body",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Range", "bytes 0-1/4")
				w.WriteHeader(http.StatusPartialContent)
				w.(http.Flusher).Flush()
				_, _ = w.Write([]byte("ddd"))
			},
			code: "range_probe_body_length",
		},
		{
			name: "encoded representation",
			response: func(w http.ResponseWriter) {
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Set("Content-Length", "1")
				w.Header().Set("Content-Range", "bytes 0-1/4")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("d"))
			},
			code: "range_probe_encoding",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				test.response(w)
			}))
			defer server.Close()

			service := newLifecycleService(&lifecycleTestClient{}, server.Client(), 0)
			link := lifecycleDownloadLink(server.URL)
			err := service.validateLink(context.Background(), &link)
			linkErr := GetLinkError(err)
			if linkErr == nil || linkErr.Code != test.code || !linkErr.ShouldRefetch() {
				t.Fatalf("validateLink() error = %v, want refetchable %s", err, test.code)
			}
		})
	}
}

func TestRegeneratedLinkWithSameURLIsRevalidated(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	first := lifecycleDownloadLink(server.URL)
	second := first
	second.Generated = first.Generated.Add(time.Minute)
	second.ExpiresAt = first.ExpiresAt.Add(time.Minute)
	client := &lifecycleTestClient{links: []types.DownloadLink{first, second}}
	service := newLifecycleService(client, server.Client(), 0)
	entry := lifecycleTestEntry()

	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err != nil {
		t.Fatalf("first GetLink() error = %v", err)
	}
	if _, err := service.GetLink(context.Background(), entry, "video.mkv"); err != nil {
		t.Fatalf("second GetLink() error = %v", err)
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("range probes = %d, want 2 for two link generations", got)
	}
}

func TestAccountFailureStartsRetryableCooldownWhenNoAlternateExists(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.Header().Set("X-Error", "bandwidth_exceeded")
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	accounts := account.NewManager(config.Debrid{
		Name:            "test",
		DownloadAPIKeys: []string{"token"},
	}, nil, zerolog.Nop())
	client := &lifecycleTestClient{
		links:    []types.DownloadLink{lifecycleDownloadLink(server.URL)},
		accounts: accounts,
	}
	service := newLifecycleService(client, server.Client(), 0)
	_, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv")
	if err == nil {
		t.Fatal("GetLink() succeeded, want account cooldown error")
	}
	if linkErr := GetLinkError(err); linkErr == nil || linkErr.Code != "account_cooldown" || !linkErr.ShouldBackoff() || linkErr.RetryAfter < 119*time.Second {
		t.Fatalf("error = %v, want throttled account cooldown honoring Retry-After", err)
	}
	if probes.Load() != 1 {
		t.Fatalf("range probes = %d, want 1 without recursive fallback", probes.Load())
	}
	accountState := accounts.All()[0].RecoveryStatus(time.Now())
	if accountState.State != account.StateTemporarilySuspended || accounts.All()[0].Disabled.Load() {
		t.Fatalf("account state/disabled = %s/%v, want temporary/false", accountState.State, accounts.All()[0].Disabled.Load())
	}
}

func TestAccountFailureMovesToAlternateAccount(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		if r.URL.Path == "/first" {
			w.Header().Set("X-Error", "bandwidth_exceeded")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	accounts := account.NewManager(config.Debrid{
		Name:            "test",
		DownloadAPIKeys: []string{"first-token", "second-token"},
	}, nil, zerolog.Nop())
	first := lifecycleDownloadLink(server.URL + "/first")
	first.Token = "first-token"
	second := lifecycleDownloadLink(server.URL + "/second")
	second.Token = "second-token"
	client := &lifecycleTestClient{
		links:    []types.DownloadLink{first, second},
		accounts: accounts,
	}
	service := newLifecycleService(client, server.Client(), 0)
	got, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv")
	if err != nil {
		t.Fatalf("GetLink() error = %v", err)
	}
	if got.Token != "second-token" {
		t.Fatalf("link token = %q, want alternate account", got.Token)
	}
	if probes.Load() != 2 || len(accounts.Active()) != 1 || accounts.Active()[0].Token != "second-token" {
		t.Fatalf("range probes/active accounts = %d/%v, want 2/[second-token]", probes.Load(), accounts.Active())
	}
	firstAccount, _ := accounts.GetAccount("first-token")
	if status := firstAccount.RecoveryStatus(time.Now()); status.State != account.StateTemporarilySuspended || firstAccount.Disabled.Load() {
		t.Fatalf("first account state/disabled = %s/%v, want temporary/false", status.State, firstAccount.Disabled.Load())
	}
}

func TestSuccessfulValidatedProbeRecoversSuspendedAccount(t *testing.T) {
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		writeValidLinkProbe(w)
	}))
	defer server.Close()

	accounts := account.NewManager(config.Debrid{
		Name:            "test",
		DownloadAPIKeys: []string{"token"},
	}, nil, zerolog.Nop())
	testAccount := accounts.All()[0]
	status, changed := accounts.SuspendTemporary(testAccount, 0, 0, "bytes_limit_reached")
	if !changed {
		t.Fatal("account was not initially suspended")
	}
	acquired, probeID, _ := testAccount.TryAcquire(status.RetryAt)
	if !acquired || probeID == 0 {
		t.Fatal("recovery probe was not acquired")
	}
	probeLink := lifecycleDownloadLink(server.URL)
	probeLink.RecoveryProbeID = probeID
	client := &lifecycleTestClient{
		links:    []types.DownloadLink{probeLink},
		accounts: accounts,
	}
	service := newLifecycleService(client, server.Client(), 0)
	// Simulate a late success from a request that started before suspension.
	// The half-open probe must still perform its own validation.
	service.validated.Store(validationKey(probeLink), struct{}{})

	if _, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv"); err != nil {
		t.Fatalf("GetLink() error = %v", err)
	}
	if probes.Load() != 1 {
		t.Fatalf("range probes = %d, want 1 fresh half-open validation", probes.Load())
	}
	if recovered := testAccount.RecoveryStatus(time.Now()); recovered.State != account.StateActive || len(accounts.Active()) != 1 {
		t.Fatalf("recovered account status/active = %+v/%d", recovered, len(accounts.Active()))
	}
}
