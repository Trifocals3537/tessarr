package rar

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPFileReadAtRequiresExactContentRange(t *testing.T) {
	const contents = "0123456789"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=3-6" {
			t.Errorf("Range = %q, want bytes=3-6", got)
		}
		if got := r.Header.Get("Accept-Encoding"); got != "identity" {
			t.Errorf("Accept-Encoding = %q, want identity", got)
		}
		w.Header().Set("Content-Range", "Bytes 3-6/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, contents[3:7])
	}))
	defer server.Close()

	file := &HttpFile{
		URL:        server.URL,
		client:     &http.Client{Timeout: time.Second},
		FileSize:   int64(len(contents)),
		MaxRetries: 0,
	}
	buffer := make([]byte, 4)
	n, err := file.ReadAt(buffer, 3)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buffer) || string(buffer) != "3456" {
		t.Fatalf("ReadAt = %d, %q", n, buffer)
	}
}

func TestHTTPFileSizeFallsBackToStrictRangeProbe(t *testing.T) {
	for _, test := range []struct {
		name         string
		headStatus   int
		headLength   string
		headEncoding string
	}{
		{name: "HEAD rejected", headStatus: http.StatusMethodNotAllowed},
		{name: "HEAD length missing", headStatus: http.StatusOK},
		{name: "HEAD encoded", headStatus: http.StatusOK, headLength: "7", headEncoding: "gzip"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var headRequests atomic.Int32
			var getRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodHead:
					headRequests.Add(1)
					if got := r.Header.Get("Accept-Encoding"); got != "identity" {
						t.Errorf("HEAD Accept-Encoding = %q, want identity", got)
					}
					if test.headLength != "" {
						w.Header().Set("Content-Length", test.headLength)
					}
					if test.headEncoding != "" {
						w.Header().Set("Content-Encoding", test.headEncoding)
					}
					w.WriteHeader(test.headStatus)
				case http.MethodGet:
					getRequests.Add(1)
					if got := r.Header.Get("Range"); got != "bytes=0-0" {
						t.Errorf("Range = %q, want bytes=0-0", got)
					}
					if got := r.Header.Get("Accept-Encoding"); got != "identity" {
						t.Errorf("Accept-Encoding = %q, want identity", got)
					}
					w.Header().Set("Content-Length", "1")
					w.Header().Set("Content-Range", "Bytes 0-0/10")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = io.WriteString(w, "0")
				default:
					t.Errorf("method = %s, want HEAD or GET", r.Method)
				}
			}))
			defer server.Close()

			file := &HttpFile{
				URL:        server.URL,
				client:     server.Client(),
				MaxRetries: 0,
			}
			size, err := file.getFileSize()
			if err != nil {
				t.Fatal(err)
			}
			if size != 10 {
				t.Fatalf("getFileSize() = %d, want 10", size)
			}
			if headRequests.Load() != 1 || getRequests.Load() != 1 {
				t.Fatalf("HEAD/GET requests = %d/%d, want 1/1", headRequests.Load(), getRequests.Load())
			}
		})
	}
}

func TestHTTPFileSizeKeepsSuccessfulHEADFastPath(t *testing.T) {
	var getRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			if got := r.Header.Get("Accept-Encoding"); got != "identity" {
				t.Errorf("HEAD Accept-Encoding = %q, want identity", got)
			}
			w.Header().Set("Content-Length", "10")
			return
		}
		getRequests.Add(1)
		http.Error(w, "unexpected GET", http.StatusInternalServerError)
	}))
	defer server.Close()

	file := &HttpFile{URL: server.URL, client: server.Client(), MaxRetries: 0}
	size, err := file.getFileSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 10 || getRequests.Load() != 0 {
		t.Fatalf("size/GET requests = %d/%d, want 10/0", size, getRequests.Load())
	}
}

func TestHTTPFileReadAtRejectsEncodedWholeFileResponse(t *testing.T) {
	const contents = "small"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, contents)
	}))
	defer server.Close()

	file := &HttpFile{
		URL:        server.URL,
		client:     server.Client(),
		FileSize:   int64(len(contents)),
		MaxRetries: 0,
	}
	if _, err := file.ReadAt(make([]byte, len(contents)), 0); !errors.Is(err, ErrNetworkError) {
		t.Fatalf("ReadAt() error = %v, want encoded-response rejection", err)
	}
}

func TestHTTPFileSizeRejectsInvalidRangeProbe(t *testing.T) {
	for _, contentRange := range []string{
		"bytes 0-1/10",
		"bytes 0-0/*",
		"bytes 0-0/0",
	} {
		t.Run(contentRange, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Range", contentRange)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, "0")
			}))
			defer server.Close()

			file := &HttpFile{URL: server.URL, client: server.Client(), MaxRetries: 0}
			if _, err := file.getFileSize(); !errors.Is(err, ErrNetworkError) {
				t.Fatalf("getFileSize() error = %v, want network integrity error", err)
			}
		})
	}
}

func TestHTTPFileSizeRejectsIgnoredRangeWithoutReadingArchive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, strings.Repeat("x", 1<<20))
	}))
	defer server.Close()

	file := &HttpFile{URL: server.URL, client: server.Client(), MaxRetries: 0}
	if _, err := file.getFileSize(); !errors.Is(err, ErrRangeRequestsNotSupported) {
		t.Fatalf("getFileSize() error = %v, want unsupported ranges", err)
	}
}

func TestHTTPFileReadAtRejectsWrongContentRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-3/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, "0123")
	}))
	defer server.Close()

	file := &HttpFile{
		URL:        server.URL,
		client:     &http.Client{Timeout: time.Second},
		FileSize:   10,
		MaxRetries: 0,
	}
	buffer := make([]byte, 4)
	if _, err := file.ReadAt(buffer, 3); !errors.Is(err, ErrNetworkError) {
		t.Fatalf("error = %v, want network integrity error", err)
	}
}

func TestHTTPFileReadAtRejectsIgnoredRangeWithoutReadingWholeBody(t *testing.T) {
	var written atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		for range 1024 {
			n, err := io.WriteString(w, strings.Repeat("x", 1024))
			written.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	defer server.Close()

	file := &HttpFile{
		URL:        server.URL,
		client:     &http.Client{Timeout: time.Second},
		FileSize:   1 << 20,
		MaxRetries: 0,
	}
	if _, err := file.ReadAt(make([]byte, 4096), 0); !errors.Is(err, ErrRangeRequestsNotSupported) {
		t.Fatalf("error = %v, want unsupported ranges", err)
	}
	if written.Load() == 0 {
		t.Fatal("test server did not start its response")
	}
}

func TestHTTPFileReadAtAcceptsExactWholeFileResponse(t *testing.T) {
	const contents = "small"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, contents)
	}))
	defer server.Close()

	file := &HttpFile{
		URL:        server.URL,
		client:     &http.Client{Timeout: time.Second},
		FileSize:   int64(len(contents)),
		MaxRetries: 0,
	}
	buffer := make([]byte, len(contents))
	n, err := file.ReadAt(buffer, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buffer) || string(buffer) != contents {
		t.Fatalf("ReadAt = %d, %q", n, buffer)
	}
}

type secretErrorRoundTripper struct{}

func (secretErrorRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, errors.New("transport exposed " + req.URL.String())
}

func TestHTTPFileNetworkErrorsRedactSignedURL(t *testing.T) {
	const secret = "signed-secret-token"
	file := &HttpFile{
		URL: "https://downloads.example.test/media.rar?token=" + secret,
		client: &http.Client{
			Transport: secretErrorRoundTripper{},
			Timeout:   time.Second,
		},
		FileSize:   10,
		MaxRetries: 0,
	}

	if _, err := file.getFileSize(); err == nil {
		t.Fatal("getFileSize() unexpectedly succeeded")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("getFileSize() leaked signed URL secret: %v", err)
	}

	if _, err := file.ReadAt(make([]byte, 4), 0); err == nil {
		t.Fatal("ReadAt() unexpectedly succeeded")
	} else if strings.Contains(err.Error(), secret) {
		t.Fatalf("ReadAt() leaked signed URL secret: %v", err)
	}
}
