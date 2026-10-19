package request

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientRedirectOmitsRefererAndPreservesOtherHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/signed":
			http.Redirect(w, r, "/cdn", http.StatusTemporaryRedirect)
		case "/cdn":
			if got := r.Header.Get("Referer"); got != "" {
				t.Errorf("Referer = %q, want signed source URL omitted", got)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer api-token" {
				t.Errorf("Authorization = %q, want same-origin API header preserved", got)
			}
			if got := r.Header.Get("Range"); got != "bytes=0-7" {
				t.Errorf("Range = %q, want request header preserved", got)
			}
			_, _ = io.WriteString(w, "payload")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := New(
		WithHeaders(map[string]string{"Authorization": "Bearer api-token"}),
		WithMaxRetries(0),
	)
	req, err := http.NewRequest(http.MethodGet, server.URL+"/signed?token=top-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-7")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestNoRefererRedirectPolicyRetainsRedirectLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "https://cdn.example/file", nil)
	req.Header.Set("Referer", "https://api.example/file?token=top-secret")
	via := make([]*http.Request, maxRedirects)

	if err := NoRefererRedirectPolicy(req, via); err == nil {
		t.Fatal("redirect policy accepted more than the default redirect limit")
	}
	if got := req.Header.Get("Referer"); got != "" {
		t.Fatalf("Referer = %q, want removed even on rejected redirect", got)
	}
}
