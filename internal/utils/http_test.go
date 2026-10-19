package utils

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetadataHTTPClientUsesVerifiedModernTLS(t *testing.T) {
	t.Parallel()

	transport, ok := metadataHTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected metadata transport type %T", metadataHTTPClient.Transport)
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("metadata transport has no TLS policy")
	}
	if transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("metadata transport disables certificate verification")
	}
	if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("TLS minimum = %#x, want TLS 1.2+", transport.TLSClientConfig.MinVersion)
	}
}

func TestReadAllLimitedExactAndOversized(t *testing.T) {
	t.Parallel()

	exact, err := ReadAllLimited(strings.NewReader("12345678"), 8)
	if err != nil {
		t.Fatalf("exact-size read failed: %v", err)
	}
	if string(exact) != "12345678" {
		t.Fatalf("unexpected exact-size content %q", exact)
	}

	oversized, err := ReadAllLimited(strings.NewReader("123456789"), 8)
	if !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("expected ErrContentTooLarge, got %v", err)
	}
	if len(oversized) != 9 {
		t.Fatalf("expected the limit probe byte to be returned, got %d bytes", len(oversized))
	}
}

func TestDecodeJSONResponseBoundedRequiresOneCompleteValue(t *testing.T) {
	t.Parallel()

	t.Run("one value", func(t *testing.T) {
		var decoded struct {
			OK bool `json:"ok"`
		}
		if err := DecodeJSONResponseBounded(
			strings.NewReader(" \n {\"ok\":true}\t "),
			&decoded,
			32,
		); err != nil {
			t.Fatalf("valid response failed: %v", err)
		}
		if !decoded.OK {
			t.Fatal("valid response was not decoded")
		}
	})

	t.Run("multiple values", func(t *testing.T) {
		var decoded map[string]any
		err := DecodeJSONResponseBounded(
			strings.NewReader(`{"ok":true} {"unexpected":true}`),
			&decoded,
			64,
		)
		if err == nil || !strings.Contains(err.Error(), "multiple values") {
			t.Fatalf("expected multiple-value rejection, got %v", err)
		}
	})

	t.Run("trailing garbage", func(t *testing.T) {
		var decoded map[string]any
		err := DecodeJSONResponseBounded(
			strings.NewReader(`{"ok":true} not-json`),
			&decoded,
			64,
		)
		if err == nil || !strings.Contains(err.Error(), "invalid trailing data") {
			t.Fatalf("expected trailing-data rejection, got %v", err)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		var decoded string
		err := DecodeJSONResponseBounded(
			strings.NewReader(`"123456789"`),
			&decoded,
			8,
		)
		if !errors.Is(err, ErrContentTooLarge) {
			t.Fatalf("expected ErrContentTooLarge, got %v", err)
		}
	})

	t.Run("exact limit", func(t *testing.T) {
		var decoded int
		if err := DecodeJSONResponseBounded(strings.NewReader("12345"), &decoded, 5); err != nil {
			t.Fatalf("exact-limit response failed: %v", err)
		}
		if decoded != 12345 {
			t.Fatalf("decoded %d, want 12345", decoded)
		}
	})
}

func TestDecodeJSONRequestBoundedRejectsAmbiguousAndOversizedBodies(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		body       string
		limit      int64
		wantStatus int
		wantLarge  bool
	}{
		{
			name:       "one value",
			body:       `{"ok":true}`,
			limit:      32,
			wantStatus: http.StatusOK,
		},
		{
			name:       "multiple values",
			body:       `{"ok":true} {"extra":true}`,
			limit:      64,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "oversized",
			body:       `{"value":"123456789"}`,
			limit:      8,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantLarge:  true,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := httptest.NewRequest(
				http.MethodPost,
				"/",
				strings.NewReader(test.body),
			)
			response := httptest.NewRecorder()
			var decoded map[string]any
			err := DecodeJSONRequestBounded(
				response,
				request,
				&decoded,
				test.limit,
			)
			if test.wantStatus == http.StatusOK {
				if err != nil {
					t.Fatalf("valid request failed: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid request succeeded")
			}
			if IsRequestTooLarge(err) != test.wantLarge {
				t.Fatalf("IsRequestTooLarge(%v) = %t", err, !test.wantLarge)
			}
		})
	}
}

func TestDownloadFileBoundedRejectsAdvertisedAndChunkedOversize(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "advertised",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "9")
				_, _ = w.Write([]byte("123456789"))
			},
		},
		{
			name: "chunked",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				_, _ = w.Write([]byte("123456789"))
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()

			_, content, err := DownloadFileBounded(
				context.Background(),
				server.URL+"/metadata?token=do-not-expose",
				8,
			)
			if !errors.Is(err, ErrContentTooLarge) {
				t.Fatalf("expected ErrContentTooLarge, got %v", err)
			}
			if strings.Contains(err.Error(), "do-not-expose") ||
				strings.Contains(err.Error(), "/metadata") {
				t.Fatalf("download error exposed signed URL data: %v", err)
			}
			if test.name == "chunked" && len(content) != 9 {
				t.Fatalf("expected bounded chunked probe, got %d bytes", len(content))
			}
		})
	}
}

func TestDownloadFileBoundedStatusTimeoutAndSecretRedaction(t *testing.T) {
	t.Parallel()

	t.Run("status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "upstream failure", http.StatusBadGateway)
		}))
		defer server.Close()

		rawURL := strings.Replace(server.URL, "http://", "http://user:password@", 1) +
			"/private/passkey?token=super-secret"
		_, _, err := DownloadFileBounded(context.Background(), rawURL, 32)
		if err == nil || !strings.Contains(err.Error(), "status code 502") {
			t.Fatalf("expected status error, got %v", err)
		}
		for _, secret := range []string{"user", "password", "passkey", "super-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("download error exposed %q: %v", secret, err)
			}
		}
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(
			_ http.ResponseWriter,
			r *http.Request,
		) {
			<-r.Context().Done()
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		_, _, err := DownloadFileBounded(ctx, server.URL+"/slow?token=hidden", 32)
		if err == nil || !strings.Contains(err.Error(), "request timed out") {
			t.Fatalf("expected timeout error, got %v", err)
		}
		if strings.Contains(err.Error(), "hidden") || strings.Contains(err.Error(), "/slow") {
			t.Fatalf("timeout error exposed signed URL data: %v", err)
		}
	})
}

func TestParseMultipartFormBoundedRejectsChunkedOversize(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("name", "sample.nzb")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(bytes.Repeat([]byte("x"), 128)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body.Bytes()))
	request.ContentLength = -1 // exercise streaming/chunked request handling
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()

	err = ParseMultipartFormBounded(response, request, 64, 32)
	if !IsRequestTooLarge(err) {
		t.Fatalf("expected bounded multipart rejection, got %v", err)
	}
	if request.MultipartForm != nil {
		_ = request.MultipartForm.RemoveAll()
	}
}

func TestParseFormBoundedRejectsChunkedOversize(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader("urls="+strings.Repeat("x", 128)),
	)
	request.ContentLength = -1
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	err := ParseFormBounded(response, request, 32)
	if !IsRequestTooLarge(err) {
		t.Fatalf("expected bounded form rejection, got %v", err)
	}
}

func TestMultipartFormPartCountIncludesValuesAndFiles(t *testing.T) {
	t.Parallel()

	form := &multipart.Form{
		Value: map[string][]string{
			"category": {"one"},
			"tag":      {"a", "b"},
		},
		File: map[string][]*multipart.FileHeader{
			"files": {{Filename: "one.nzb"}, {Filename: "two.nzb"}},
		},
	}
	if got, want := MultipartFormPartCount(form), 5; got != want {
		t.Fatalf("part count = %d, want %d", got, want)
	}
}
