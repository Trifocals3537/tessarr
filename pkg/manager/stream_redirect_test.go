package manager

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStreamHTTPClientDoesNotForwardSignedURLAsReferer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/requestdl":
			http.Redirect(w, r, "/cdn/video", http.StatusTemporaryRedirect)
		case "/cdn/video":
			if got := r.Header.Get("Referer"); got != "" {
				t.Errorf("Referer = %q, want tokenized source URL omitted", got)
			}
			if got := r.Header.Get("Range"); got != "bytes=0-3" {
				t.Errorf("Range = %q, want playback range preserved", got)
			}
			_, _ = io.WriteString(w, "data")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newStreamHTTPClient(nil)
	req, err := http.NewRequest(http.MethodGet, server.URL+"/requestdl?token=top-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-3")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}
