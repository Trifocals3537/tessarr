package request

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoWithoutDefaultHeadersPreservesNonExcludedHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want excluded default header omitted", got)
		}
		if got := r.Header.Get("User-Agent"); got != "Tessarr-Test" {
			t.Errorf("User-Agent = %q, want retained default header", got)
		}
		if got := r.Header.Get("Range"); got != "bytes=0-7" {
			t.Errorf("Range = %q, want request-specific header retained", got)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	client := New(
		WithHeaders(map[string]string{
			"Authorization": "Bearer secret",
			"User-Agent":    "Tessarr-Test",
		}),
		WithMaxRetries(0),
	)
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-7")

	resp, err := client.DoWithoutDefaultHeaders(req, "authorization")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestDoStillAppliesAllDefaultHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q, want normal default header", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := New(WithHeaders(map[string]string{"Authorization": "Bearer secret"}), WithMaxRetries(0))
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}
