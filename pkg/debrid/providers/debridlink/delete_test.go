package debridlink

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/rs/zerolog"
)

func TestDeleteTorrentDistinguishesNotFoundFromUnauthorized(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	for _, test := range []struct {
		name     string
		status   int
		wantGone bool
	}{
		{name: "not found", status: http.StatusNotFound, wantGone: true},
		{name: "unauthorized", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.URL.Path != "/seedbox/42/remove" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			client := &DebridLink{
				Host:   server.URL,
				client: request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
				logger: zerolog.Nop(),
			}
			err := client.DeleteTorrent("42")
			if err == nil {
				t.Fatal("DeleteTorrent() error = nil")
			}
			if errors.Is(err, customerror.TorrentNotFoundError) != test.wantGone {
				t.Fatalf("error = %v, typed not-found = %v", err, test.wantGone)
			}
		})
	}
}
