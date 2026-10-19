package request

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/providertraffic"
	"github.com/Trifocals3537/tessarr/internal/tlsconfig"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
	"golang.org/x/net/proxy"
)

var (
	once     sync.Once
	instance *Client

	// ErrInvalidProxy is returned before any request is sent when an explicit
	// proxy setting cannot be parsed or uses an unsupported scheme. Explicit
	// proxy configuration is fail-closed so credentials and traffic never fall
	// back to the host's direct connection by accident.
	ErrInvalidProxy = errors.New("request: invalid proxy configuration")
)

const defaultMaxConnsPerHost = 32

type ClientOption func(*Client)

// Client represents an HTTP client with additional capabilities
type Client struct {
	client           *retryablehttp.Client
	httpClient       *http.Client // underlying http client
	rateLimiter      ratelimit.Limiter
	headers          map[string]string
	headersMu        sync.RWMutex
	maxRetries       int
	singleAttempt    bool
	timeout          time.Duration
	retryableStatus  map[int]struct{}
	logger           zerolog.Logger
	proxy            string
	configurationErr error
	traffic          *providertraffic.Controller
	trafficIdentity  providertraffic.Identity
}

// WithMaxRetries sets the maximum number of retry attempts
func WithMaxRetries(maxRetries int) ClientOption {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

// WithSingleAttempt returns the first HTTP response as-is, including 429/5xx,
// so durable mutation callers can distinguish rejection from an unknown result.
func WithSingleAttempt() ClientOption {
	return func(c *Client) {
		c.maxRetries = 0
		c.singleAttempt = true
	}
}

// WithNoRedirects keeps a mutation on its configured endpoint. A redirect is
// returned to the caller rather than silently replaying a non-idempotent POST.
func WithNoRedirects() ClientOption {
	return func(c *Client) {
		c.httpClient.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
}

// WithTimeout sets the request timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithRateLimiter sets a rate limiter
func WithRateLimiter(rl ratelimit.Limiter) ClientOption {
	return func(c *Client) {
		c.rateLimiter = rl
	}
}

// WithHeaders sets default headers
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *Client) {
		c.headersMu.Lock()
		c.headers = headers
		c.headersMu.Unlock()
	}
}

func (c *Client) SetHeader(key, value string) {
	c.headersMu.Lock()
	c.headers[key] = value
	c.headersMu.Unlock()
}

func WithLogger(logger zerolog.Logger) ClientOption {
	return func(c *Client) {
		c.logger = logger
	}
}

func WithTransport(transport *http.Transport) ClientOption {
	return func(c *Client) {
		if transport == nil {
			c.httpClient.Transport = nil
			return
		}

		secured := transport.Clone()
		secured.TLSClientConfig = tlsconfig.Harden(secured.TLSClientConfig)
		// Custom TLS dial hooks bypass TLSClientConfig entirely. Clear them so
		// WithTransport cannot silently opt out of certificate verification.
		//lint:ignore SA1019 DialTLS must be cleared alongside DialTLSContext to secure caller-owned transports.
		secured.DialTLS = nil
		secured.DialTLSContext = nil
		c.httpClient.Transport = secured
	}
}

// WithRetryableStatus replaces the default status set with the provider's
// explicit retry contract.
func WithRetryableStatus(statusCodes ...int) ClientOption {
	return func(c *Client) {
		c.retryableStatus = make(map[int]struct{}) // reset the map
		for _, code := range statusCodes {
			c.retryableStatus[code] = struct{}{}
		}
	}
}

func WithProxy(proxyURL string) ClientOption {
	return func(c *Client) {
		c.proxy = proxyURL
	}
}

// WithProviderTraffic applies the provider's built-in API contract to every
// physical attempt, including retries. User-configured rate limits remain an
// additional guard at the logical-request layer.
func WithProviderTraffic(
	controller *providertraffic.Controller,
	providerType string,
	accountToken string,
) ClientOption {
	return func(c *Client) {
		c.traffic = controller
		c.trafficIdentity = providertraffic.Identity{
			ProviderType: providerType,
			AccountToken: accountToken,
		}
	}
}

// Do performs an HTTP request with retries for certain status codes.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	return c.do(req, nil)
}

// DoWithoutDefaultHeaders performs a request without applying the named
// client-level headers. Request-specific headers are left intact. This is
// useful when an API client follows a provider-generated URL whose own
// signature or token authorizes the download and API credentials must not be
// sent to that host.
func (c *Client) DoWithoutDefaultHeaders(req *http.Request, excluded ...string) (*http.Response, error) {
	excludedHeaders := make(map[string]struct{}, len(excluded))
	for _, key := range excluded {
		excludedHeaders[http.CanonicalHeaderKey(key)] = struct{}{}
	}
	return c.do(req, excludedHeaders)
}

func (c *Client) do(req *http.Request, excludedHeaders map[string]struct{}) (*http.Response, error) {
	if c.configurationErr != nil {
		return nil, c.configurationErr
	}
	// Apply headers
	c.headersMu.RLock()
	if c.headers != nil {
		for key, value := range c.headers {
			if _, excluded := excludedHeaders[http.CanonicalHeaderKey(key)]; excluded {
				continue
			}
			req.Header.Set(key, value)
		}
	}
	c.headersMu.RUnlock()

	// Apply rate limiting
	if c.rateLimiter != nil {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		default:
			c.rateLimiter.Take()
		}
	}

	// Convert to a retryable request without copying replayable in-memory
	// bodies. net/http supplies GetBody for bytes.Buffer/Reader and strings.Reader
	// requests; using that factory avoids a second full allocation for large
	// multipart torrent uploads while preserving exact retry behavior.
	var retryReq *retryablehttp.Request
	var err error
	if req.GetBody != nil {
		requestWithoutBody := req.Clone(req.Context())
		requestWithoutBody.Body = nil
		retryReq, err = retryablehttp.FromRequest(requestWithoutBody)
		if err == nil {
			err = retryReq.SetBody(retryablehttp.ReaderFunc(func() (io.Reader, error) {
				return req.GetBody()
			}))
			retryReq.ContentLength = req.ContentLength
		}
		// The retry client owns the replacement readers returned by GetBody;
		// close the unused original body to retain net/http's ownership contract.
		if req.Body != nil {
			_ = req.Body.Close()
		}
	} else {
		retryReq, err = retryablehttp.FromRequest(req)
	}
	if err != nil {
		return nil, fmt.Errorf("creating retryable request: %w", err)
	}

	return c.client.Do(retryReq)
}

// MakeRequest performs an HTTP request and returns the response body as bytes
func (c *Client) MakeRequest(req *http.Request) ([]byte, error) {
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := res.Body.Close(); err != nil {
			c.logger.Printf("Failed to close response body: %v", err)
		}
	}()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		// Response bodies can contain provider diagnostics, signed URLs, or
		// credentials. Drain only a small bounded prefix for connection reuse
		// and report the status without reflecting the body.
		_, _ = io.CopyN(io.Discard, res.Body, 64<<10)
		return nil, fmt.Errorf("HTTP error %d", res.StatusCode)
	}

	bodyBytes, err := utils.ReadAllLimited(res.Body, utils.MaxJSONResponseBytes)
	if err != nil {
		return nil, fmt.Errorf("reading bounded response body: %w", err)
	}

	return bodyBytes, nil
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request: %w", err)
	}

	return c.Do(req)
}

// retryAfterBackoff extends DefaultBackoff with bounded Retry-After support.
// Providers commonly return the header with 429 and 503 responses. Honor the
// requested delay without allowing it to exceed the client's retry ceiling.
func retryAfterBackoff(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
	if resp != nil && (resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode == http.StatusServiceUnavailable) {
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				wait := time.Duration(secs) * time.Second
				if wait > max {
					return max
				}
				return wait
			}
			if t, err := http.ParseTime(ra); err == nil {
				if wait := time.Until(t); wait > 0 {
					if wait > max {
						return max
					}
					return wait
				}
			}
		}
	}
	return retryablehttp.DefaultBackoff(min, max, attemptNum, resp)
}

// New creates a new HTTP client with the specified options
func New(options ...ClientOption) *Client {
	client := &Client{
		maxRetries: 5,
		retryableStatus: map[int]struct{}{
			http.StatusTooManyRequests:     {},
			http.StatusInternalServerError: {},
			http.StatusBadGateway:          {},
			http.StatusServiceUnavailable:  {},
			http.StatusGatewayTimeout:      {},
		},
		logger:  logger.New("request"),
		timeout: 60 * time.Second,
		proxy:   "",
		headers: make(map[string]string),
	}

	// Create default http client
	client.httpClient = &http.Client{
		Timeout:       client.timeout,
		CheckRedirect: NoRefererRedirectPolicy,
	}

	// Apply options before configuring transport
	for _, option := range options {
		option(client)
	}

	client.httpClient.Timeout = client.timeout

	// Check if transport was set by WithTransport option
	if client.httpClient.Transport == nil {
		client.httpClient.Transport = &http.Transport{
			TLSClientConfig: tlsconfig.Verified(""),
			Proxy:           http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       defaultMaxConnsPerHost,
			IdleConnTimeout:       30 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		}
	}
	transport := client.httpClient.Transport.(*http.Transport)
	if transport.MaxConnsPerHost <= 0 {
		transport.MaxConnsPerHost = defaultMaxConnsPerHost
	}
	if client.proxy != "" {
		client.configurationErr = setProxy(transport, client.proxy)
	}
	if client.traffic != nil {
		client.httpClient.Transport = &providerTrafficTransport{
			base:       client.httpClient.Transport,
			controller: client.traffic,
			identity:   client.trafficIdentity,
		}
	}

	// Create retryablehttp client
	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient = client.httpClient
	retryClient.RetryMax = client.maxRetries
	retryClient.RetryWaitMin = 1 * time.Second
	retryClient.RetryWaitMax = 30 * time.Second
	retryClient.Logger = nil
	retryClient.Backoff = retryAfterBackoff

	// Custom retry policy based on retryable status codes
	retryClient.CheckRetry = func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// Don't retry on context errors
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if client.singleAttempt {
			return false, nil
		}

		// Use the library policy only for transport failures. Applying it to
		// provider responses would retry every 5xx and silently override the
		// provider-specific status contract configured by each client.
		if err != nil {
			return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
		}

		if resp == nil {
			return false, nil
		}
		if _, ok := client.retryableStatus[resp.StatusCode]; ok {
			return true, nil
		}

		return false, nil
	}

	client.client = retryClient

	return client
}

func Default() *Client {
	once.Do(func() {
		instance = New()
	})
	return instance
}

type contextDialerAdapter struct {
	dialContext func(context.Context, string, string) (net.Conn, error)
}

func (d contextDialerAdapter) Dial(network, address string) (net.Conn, error) {
	return d.dialContext(context.Background(), network, address)
}

func (d contextDialerAdapter) DialContext(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	return d.dialContext(ctx, network, address)
}

func setProxy(transport *http.Transport, rawURL string) error {
	proxyURL, err := url.Parse(rawURL)
	if err != nil || proxyURL.Host == "" {
		return ErrInvalidProxy
	}

	switch strings.ToLower(proxyURL.Scheme) {
	case "http", "https":
		transport.Proxy = http.ProxyURL(proxyURL)
		return nil
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if proxyURL.User != nil {
			password, _ := proxyURL.User.Password()
			auth = &proxy.Auth{
				User:     proxyURL.User.Username(),
				Password: password,
			}
		}

		forwardDialContext := transport.DialContext
		if forwardDialContext == nil {
			forwardDialContext = (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext
		}
		dialer, err := proxy.SOCKS5(
			"tcp",
			proxyURL.Host,
			auth,
			contextDialerAdapter{dialContext: forwardDialContext},
		)
		if err != nil {
			return ErrInvalidProxy
		}
		contextDialer, ok := dialer.(proxy.ContextDialer)
		if !ok {
			return ErrInvalidProxy
		}
		transport.Proxy = nil
		transport.DialContext = contextDialer.DialContext
		return nil
	default:
		return ErrInvalidProxy
	}
}
