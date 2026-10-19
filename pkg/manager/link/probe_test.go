package link

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

type probeRoundTripFunc func(*http.Request) (*http.Response, error)

func (f probeRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type countedProbeBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countedProbeBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}

func (b *countedProbeBody) Close() error {
	b.closed = true
	return nil
}

func TestProbeBoundsBodyReadAndClosesRejectedResponse(t *testing.T) {
	body := &countedProbeBody{Reader: strings.NewReader(strings.Repeat("x", 32768))}
	client := &http.Client{Transport: probeRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusPartialContent,
			Header:        http.Header{"Content-Range": []string{"bytes 0-1/4"}},
			ContentLength: -1,
			Body:          body,
		}, nil
	})}
	service := newLifecycleService(&lifecycleTestClient{}, client, 0)
	download := lifecycleDownloadLink("https://cdn.example/probe")
	err := service.validateLink(context.Background(), &download)
	if got := GetLinkError(err); got == nil || got.Code != "range_probe_body_length" {
		t.Fatalf("error = %v, want overlong body rejection", err)
	}
	if body.read != 3 || !body.closed {
		t.Fatalf("read/closed = %d/%v, want two bytes plus one sentinel and a closed body", body.read, body.closed)
	}
}

func TestProbeAcceptsExactSizeAwareRanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		size    int64
		total   int64
		wantEnd int64
		body    string
		chunked bool
	}{
		{name: "ordinary object", size: 4, total: 4, wantEnd: 1, body: "da"},
		{name: "two byte object", size: 2, total: 2, wantEnd: 1, body: "da"},
		{name: "one byte object", size: 1, total: 1, wantEnd: 0, body: "d"},
		{name: "unknown size", total: 4, wantEnd: 1, body: "da"},
		{name: "negative unknown size", size: -1, total: 4, wantEnd: 1, body: "da"},
		{name: "unknown size clamped at EOF", total: 1, wantEnd: 1, body: "d"},
		{name: "chunked range", size: 4, total: 4, wantEnd: 1, body: "da", chunked: true},
		{name: "chunked EOF", total: 1, wantEnd: 1, body: "d", chunked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got, want := r.Header.Get("Range"), fmt.Sprintf("bytes=0-%d", tc.wantEnd); got != want {
					t.Errorf("Range = %q, want %q", got, want)
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(tc.body)-1, tc.total))
				if !tc.chunked {
					w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)))
				}
				w.WriteHeader(http.StatusPartialContent)
				if tc.chunked {
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			service := newLifecycleService(&lifecycleTestClient{}, server.Client(), 0)
			download := lifecycleDownloadLink(server.URL)
			download.Size = tc.size
			if err := service.validateLink(context.Background(), &download); err != nil {
				t.Fatalf("valid range rejected: %v", err)
			}
		})
	}
}

func TestProbeAvoidsCDNZeroEndQuirk(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Range") {
		case "bytes=0-0":
			// Some CDNs report the requested bounds but send the whole object
			// when the requested end is zero. This response must stay invalid.
			w.Header().Set("Content-Range", "bytes 0-0/4")
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("data"))
		case "bytes=0-1":
			w.Header().Set("Content-Range", "bytes 0-1/4")
			w.Header().Set("Content-Length", "2")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("da"))
		default:
			http.Error(w, "unexpected probe", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := &lifecycleTestClient{links: []types.DownloadLink{lifecycleDownloadLink(server.URL)}}
	service := newLifecycleService(client, server.Client(), 0)
	if _, err := service.GetLink(context.Background(), lifecycleTestEntry(), "video.mkv"); err != nil {
		t.Fatalf("playable object rejected: %v", err)
	}
	if fetches, invalidations := client.counts(); fetches != 1 || invalidations != 0 {
		t.Fatalf("unexpected provider churn: fetches=%d invalidations=%d", fetches, invalidations)
	}
}

func TestProbeRejectsInvalidSizeAwareRanges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		size       int64
		rangeValue string
		length     string
		body       string
		code       string
	}{
		{name: "zero total", rangeValue: "bytes 0-1/0", length: "2", body: "da", code: "range_probe_content_range"},
		{name: "unknown total", rangeValue: "bytes 0-1/*", length: "2", body: "da", code: "range_probe_content_range"},
		{name: "overflow total", rangeValue: "bytes 0-1/9223372036854775808", length: "2", body: "da", code: "range_probe_content_range"},
		{name: "unjustified clamp", rangeValue: "bytes 0-0/4", length: "1", body: "d", code: "range_probe_content_range"},
		{name: "range exceeds total", rangeValue: "bytes 0-1/1", length: "2", body: "da", code: "range_probe_content_range"},
		{name: "one byte size mismatch", size: 1, rangeValue: "bytes 0-0/4", length: "1", body: "d", code: "range_probe_content_range"},
		{name: "oversized body", size: 4, rangeValue: "bytes 0-1/4", length: "4", body: "data", code: "range_probe_content_length"},
		{name: "short declared body", size: 4, rangeValue: "bytes 0-1/4", length: "1", body: "d", code: "range_probe_content_length"},
		{name: "truncated body", size: 4, rangeValue: "bytes 0-1/4", length: "2", body: "d", code: "range_probe_body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Range", tc.rangeValue)
				w.Header().Set("Content-Length", tc.length)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			service := newLifecycleService(&lifecycleTestClient{}, server.Client(), 0)
			download := lifecycleDownloadLink(server.URL)
			download.Size = tc.size
			err := service.validateLink(context.Background(), &download)
			if got := GetLinkError(err); got == nil || got.Code != tc.code {
				t.Fatalf("error = %v, want %s", err, tc.code)
			}
		})
	}
}
