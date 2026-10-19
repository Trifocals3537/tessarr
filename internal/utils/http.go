package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/tlsconfig"
	"go.uber.org/ratelimit"
)

const (
	// MaxMetadataFileBytes is the largest NZB or torrent metadata document
	// accepted from an upload or remote URL. Metadata should be small; the
	// comparatively generous ceiling preserves compatibility with unusually
	// large NZBs while keeping allocations bounded.
	MaxMetadataFileBytes int64 = 64 << 20

	// MaxImportRequestBytes bounds a complete multipart import request and the
	// aggregate metadata retained for one import operation.
	MaxImportRequestBytes int64 = 256 << 20

	// MaxMultipartMemoryBytes limits the portion of multipart uploads retained
	// in memory. net/http spills larger file parts to temporary files.
	MaxMultipartMemoryBytes int64 = 16 << 20

	// MaxImportItems prevents one request from creating an unbounded amount of
	// parsing and provider work.
	MaxImportItems = 64

	// MaxMultipartFormParts bounds control fields plus file parts. It leaves
	// room for the import item ceiling and normal qBit/API option fields.
	MaxMultipartFormParts = 128

	// MaxMagnetTextBytes bounds magnet links and .magnet files independently
	// from binary torrent/NZB metadata.
	MaxMagnetTextBytes int64 = 1 << 20

	// MaxJSONResponseBytes bounds decoded provider/API response documents.
	// Provider metadata should be far smaller than this in normal operation.
	MaxJSONResponseBytes int64 = 32 << 20

	// MaxControlRequestBytes bounds small JSON/form control-plane requests such
	// as login, registration, authentication changes, and initial setup.
	MaxControlRequestBytes int64 = 64 << 10

	metadataDownloadTimeout = 30 * time.Second
)

var (
	// ErrContentTooLarge allows HTTP handlers to distinguish a resource-limit
	// rejection from malformed input or a transient network failure.
	ErrContentTooLarge = errors.New("content exceeds byte limit")

	metadataHTTPClient = newMetadataHTTPClient()
)

func newMetadataHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsconfig.Harden(transport.TLSClientConfig)
	return &http.Client{
		Transport: transport,
		Timeout:   metadataDownloadTimeout,
	}
}

func ParseRateLimit(rateStr string) ratelimit.Limiter {
	if rateStr == "" {
		return nil
	}
	parts := strings.SplitN(rateStr, "/", 2)
	if len(parts) != 2 {
		return nil
	}

	// parse count
	count, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || count <= 0 {
		return nil
	}

	// Set slack size to 10%
	slackSize := count / 10

	// normalize unit
	unit := strings.ToLower(strings.TrimSpace(parts[1]))
	unit = strings.TrimSuffix(unit, "s")
	switch unit {
	case "minute", "min":
		return ratelimit.New(count, ratelimit.Per(time.Minute), ratelimit.WithSlack(slackSize))
	case "second", "sec":
		return ratelimit.New(count, ratelimit.Per(time.Second), ratelimit.WithSlack(slackSize))
	case "hour", "hr":
		return ratelimit.New(count, ratelimit.Per(time.Hour), ratelimit.WithSlack(slackSize))
	case "day", "d":
		return ratelimit.New(count, ratelimit.Per(24*time.Hour), ratelimit.WithSlack(slackSize))
	default:
		return nil
	}
}

func JSONResponse(w http.ResponseWriter, data any, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if data != nil {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		_ = encoder.Encode(data)
	}
}

func ValidateURL(urlStr string) error {
	if urlStr == "" {
		return fmt.Errorf("URL cannot be empty")
	}

	// Try parsing as full URL first
	u, err := url.Parse(urlStr)
	if err == nil && u.Scheme != "" && u.Host != "" {
		// It's a full URL, validate scheme
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("URL scheme must be http or https")
		}
		return nil
	}

	// Check if it's a host:port format (no scheme)
	if strings.Contains(urlStr, ":") && !strings.Contains(urlStr, "://") {
		// Try parsing with http:// prefix
		testURL := "http://" + urlStr
		u, err := url.Parse(testURL)
		if err != nil {
			return fmt.Errorf("invalid host:port format: %w", err)
		}

		if u.Host == "" {
			return fmt.Errorf("host is required in host:port format")
		}

		// Validate port number
		if u.Port() == "" {
			return fmt.Errorf("port is required in host:port format")
		}

		return nil
	}

	return fmt.Errorf("invalid URL format: %s", urlStr)
}

func JoinURL(base string, paths ...string) (string, error) {
	// Split the last path component to separate query parameters
	lastPath := paths[len(paths)-1]
	parts := strings.Split(lastPath, "?")
	paths[len(paths)-1] = parts[0]

	joined, err := url.JoinPath(base, paths...)
	if err != nil {
		return "", err
	}

	// AddOrUpdate back query parameters if they exist
	if len(parts) > 1 {
		return joined + "?" + parts[1], nil
	}

	return joined, nil
}

type DownloadOptions func(r *http.Request)

func WithHeader(key, value string) DownloadOptions {
	return func(r *http.Request) {
		r.Header.Set(key, value)
	}
}

// ReadAllLimited reads at most maxBytes from r. It consumes one additional
// byte only to distinguish an exact-size document from an oversized one.
func ReadAllLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("%w: maximum must be positive", ErrContentTooLarge)
	}
	if maxBytes == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("%w: maximum is too large", ErrContentTooLarge)
	}

	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return data, err
	}
	if int64(len(data)) > maxBytes {
		return data, fmt.Errorf("%w: maximum is %d bytes", ErrContentTooLarge, maxBytes)
	}
	return data, nil
}

// DecodeJSONResponse decodes exactly one JSON value from a bounded response
// body. Trailing whitespace is accepted; a second value or trailing garbage is
// rejected so callers cannot accidentally accept an ambiguous response.
func DecodeJSONResponse(body io.Reader, destination any) error {
	return DecodeJSONResponseBounded(body, destination, MaxJSONResponseBytes)
}

// DecodeJSONResponseBounded is the configurable form used by focused callers
// and tests. Limits above the standard response ceiling are rejected.
func DecodeJSONResponseBounded(
	body io.Reader,
	destination any,
	maxBytes int64,
) error {
	if body == nil {
		return fmt.Errorf("JSON response body is required")
	}
	if maxBytes <= 0 || maxBytes > MaxJSONResponseBytes {
		return fmt.Errorf(
			"JSON response byte limit must be between 1 and %d",
			MaxJSONResponseBytes,
		)
	}

	limited := &io.LimitedReader{R: body, N: maxBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(destination); err != nil {
		if limited.N == 0 {
			return fmt.Errorf(
				"%w: JSON response maximum is %d bytes",
				ErrContentTooLarge,
				maxBytes,
			)
		}
		return fmt.Errorf("failed to decode JSON response: %w", err)
	}

	var trailing any
	err := decoder.Decode(&trailing)
	if limited.N == 0 {
		return fmt.Errorf(
			"%w: JSON response maximum is %d bytes",
			ErrContentTooLarge,
			maxBytes,
		)
	}
	switch {
	case err == nil:
		return fmt.Errorf("JSON response contains multiple values")
	case errors.Is(err, io.EOF):
		return nil
	default:
		return fmt.Errorf("JSON response contains invalid trailing data: %w", err)
	}
}

// DecodeJSONRequestBounded decodes exactly one JSON value from an HTTP request
// and enforces a hard total-body limit for both fixed-length and chunked
// bodies.
func DecodeJSONRequestBounded(
	w http.ResponseWriter,
	r *http.Request,
	destination any,
	maxRequestBytes int64,
) error {
	if r == nil || r.Body == nil {
		return fmt.Errorf("JSON request body is required")
	}
	if maxRequestBytes <= 0 || maxRequestBytes > MaxJSONResponseBytes {
		return fmt.Errorf(
			"JSON request byte limit must be between 1 and %d",
			MaxJSONResponseBytes,
		)
	}
	if err := limitRequestBody(w, r, maxRequestBytes); err != nil {
		return err
	}

	data, err := ReadAllLimited(r.Body, maxRequestBytes)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("failed to decode JSON request: %w", err)
	}
	return nil
}

// ParseMultipartFormBounded applies a hard limit to the complete request body
// before parsing it. ParseMultipartForm's maxMemory argument alone is not a
// request-size limit; file parts beyond it are otherwise written to disk
// without a total bound.
func ParseMultipartFormBounded(
	w http.ResponseWriter,
	r *http.Request,
	maxRequestBytes int64,
	maxMemoryBytes int64,
) error {
	if maxRequestBytes <= 0 || maxMemoryBytes <= 0 {
		return fmt.Errorf("multipart limits must be positive")
	}
	if err := limitRequestBody(w, r, maxRequestBytes); err != nil {
		return err
	}
	return r.ParseMultipartForm(maxMemoryBytes)
}

// MultipartFormPartCount counts both repeated value fields and file parts.
// Callers use it after parsing to reject excessive unknown/control fields in
// addition to their stricter import-item limit.
func MultipartFormPartCount(form *multipart.Form) int {
	if form == nil {
		return 0
	}
	total := 0
	for _, values := range form.Value {
		total += len(values)
		if total > MaxMultipartFormParts {
			return total
		}
	}
	for _, files := range form.File {
		total += len(files)
		if total > MaxMultipartFormParts {
			return total
		}
	}
	return total
}

// ParseFormBounded applies a hard total-body limit before parsing regular URL
// encoded forms. It is safe to call when an earlier middleware already parsed
// the form; net/http will reuse the existing parsed values.
func ParseFormBounded(
	w http.ResponseWriter,
	r *http.Request,
	maxRequestBytes int64,
) error {
	if err := limitRequestBody(w, r, maxRequestBytes); err != nil {
		return err
	}
	return r.ParseForm()
}

// ParseAnyFormBounded parses either URL-encoded or multipart form data under a
// hard total-body limit. Multipart fields remain in memory because this helper
// is intended only for small control-plane forms.
func ParseAnyFormBounded(
	w http.ResponseWriter,
	r *http.Request,
	maxRequestBytes int64,
) error {
	if err := limitRequestBody(w, r, maxRequestBytes); err != nil {
		return err
	}
	err := r.ParseMultipartForm(maxRequestBytes)
	if errors.Is(err, http.ErrNotMultipart) {
		return nil
	}
	return err
}

func limitRequestBody(w http.ResponseWriter, r *http.Request, maxRequestBytes int64) error {
	if maxRequestBytes <= 0 {
		return fmt.Errorf("request byte limit must be positive")
	}
	if r.ContentLength > maxRequestBytes {
		return &http.MaxBytesError{Limit: maxRequestBytes}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	return nil
}

// IsRequestTooLarge reports whether a multipart/request read failed because it
// exceeded a configured hard limit.
func IsRequestTooLarge(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr) || errors.Is(err, ErrContentTooLarge)
}

// RedactedURL returns only the scheme and host of an HTTP(S) URL. Paths,
// userinfo, fragments, and queries may carry signed tokens and are never
// suitable for logs or error responses.
func RedactedURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u == nil {
		return "<invalid-url>"
	}
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "http" && scheme != "https") || u.Host == "" {
		return "<invalid-url>"
	}
	return scheme + "://" + u.Host
}

func DownloadFile(rawURL string, options ...DownloadOptions) (string, []byte, error) {
	return DownloadFileBounded(context.Background(), rawURL, MaxMetadataFileBytes, options...)
}

// DownloadFileBounded downloads a small metadata document with a total request
// timeout and a strict response-body limit. Errors intentionally identify only
// the remote origin so signed URLs and embedded credentials are not exposed.
func DownloadFileBounded(
	ctx context.Context,
	rawURL string,
	maxBytes int64,
	options ...DownloadOptions,
) (string, []byte, error) {
	safeURL := RedactedURL(rawURL)
	if safeURL == "<invalid-url>" {
		return "", nil, fmt.Errorf("failed to create request for %s: invalid HTTP URL", safeURL)
	}
	if maxBytes <= 0 || maxBytes > MaxMetadataFileBytes {
		return "", nil, fmt.Errorf("download byte limit must be between 1 and %d", MaxMetadataFileBytes)
	}
	if ctx == nil {
		return "", nil, fmt.Errorf("failed to create request for %s: missing context", safeURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request for %s", safeURL)
	}

	// Apply options to the request
	for _, opt := range options {
		opt(req)
	}

	resp, err := metadataHTTPClient.Do(req)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return "", nil, fmt.Errorf("failed to download file from %s: request timed out", safeURL)
		case errors.Is(err, context.Canceled):
			return "", nil, fmt.Errorf("failed to download file from %s: request canceled", safeURL)
		default:
			// net/url errors include the complete requested URL. Do not wrap or
			// stringify them here.
			return "", nil, fmt.Errorf("failed to download file from %s: network request failed", safeURL)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf(
			"failed to download file from %s: status code %d",
			safeURL,
			resp.StatusCode,
		)
	}
	if resp.ContentLength > maxBytes {
		return "", nil, fmt.Errorf(
			"failed to download file from %s: %w",
			safeURL,
			fmt.Errorf("%w: maximum is %d bytes", ErrContentTooLarge, maxBytes),
		)
	}

	filename := getFilenameFromResponse(resp, rawURL)

	data, err := ReadAllLimited(resp.Body, maxBytes)
	if err != nil {
		if errors.Is(err, ErrContentTooLarge) {
			return "", data, fmt.Errorf("failed to download file from %s: %w", safeURL, err)
		}
		return "", data, fmt.Errorf("failed to read response body from %s", safeURL)
	}

	return filename, data, nil
}

func getFilenameFromResponse(resp *http.Response, originalURL string) string {
	// 1. Try Content-Disposition header
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		// First try standard MIME parsing
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			// RFC 5987: filename* takes precedence
			if filename := params["filename*"]; filename != "" {
				return filename
			}
			if filename := params["filename"]; filename != "" {
				return filename
			}
		}

		// Manual fallback for non-compliant headers (unquoted filenames with special chars)
		if filename := extractFilenameManual(cd); filename != "" {
			return filename
		}
	}

	// 2. Fall back to URL path
	if parsedURL, err := url.Parse(originalURL); err == nil {
		if filename := filepath.Base(parsedURL.Path); filename != "." && filename != "/" {
			// URL decode the filename
			if decoded, err := url.QueryUnescape(filename); err == nil {
				return decoded
			}
			return filename
		}
	}

	// 3. Default filename
	return "downloaded_file"
}

// extractFilenameManual handles non-compliant Content-Disposition headers
// where filename is not properly quoted (e.g., filename=[Erai-raws]...nzb)
func extractFilenameManual(cd string) string {
	// Try filename*= first (RFC 5987)
	if _, after, ok := strings.Cut(cd, "filename*="); ok {
		value := after
		// Handle UTF-8'' prefix
		if strings.HasPrefix(value, "UTF-8''") || strings.HasPrefix(value, "utf-8''") {
			value = value[7:]
		}
		// Take until semicolon or end
		if semi := strings.Index(value, ";"); semi != -1 {
			value = value[:semi]
		}
		value = strings.Trim(value, `"' `)
		if decoded, err := url.QueryUnescape(value); err == nil {
			return decoded
		}
		return value
	}

	// Try filename= (simple case)
	if _, after, ok := strings.Cut(cd, "filename="); ok {
		value := after
		// Take until semicolon or end
		if semi := strings.Index(value, ";"); semi != -1 {
			value = value[:semi]
		}
		// Remove surrounding quotes if present
		value = strings.Trim(value, `"' `)
		if value != "" {
			return value
		}
	}

	return ""
}

func GetContentType(fileName string) string {
	contentType := mime.TypeByExtension(filepath.Ext(fileName))
	if contentType == "" {
		return "application/octet-stream"
	}
	return contentType
}

// IsValidURL checks if a string is a valid HTTP/HTTPS URL.
// Optimized for speed with early exits before calling url.Parse.
func IsValidURL(s string) bool {
	n := len(s)
	if n < 10 { // minimum: "http://a.b"
		return false
	}

	// Fast scheme check without allocation
	var schemeEnd int
	if s[0] == 'h' && s[1] == 't' && s[2] == 't' && s[3] == 'p' {
		if s[4] == ':' && s[5] == '/' && s[6] == '/' {
			schemeEnd = 7 // http://
		} else if s[4] == 's' && s[5] == ':' && s[6] == '/' && s[7] == '/' {
			schemeEnd = 8 // https://
		} else {
			return false
		}
	} else {
		return false
	}

	// Check host portion is non-empty
	host := s[schemeEnd:]
	if slashIdx := strings.IndexByte(host, '/'); slashIdx != -1 {
		host = host[:slashIdx]
	}
	if len(host) == 0 {
		return false
	}

	// Full parse for edge cases (ports, userinfo, IPv6, etc.)
	u, err := url.Parse(s)
	return err == nil && u.Host != ""
}
