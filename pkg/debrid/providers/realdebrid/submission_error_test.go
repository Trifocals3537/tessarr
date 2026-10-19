package realdebrid

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestSubmitMagnetTypesContentPolicyRejections(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
	}))
	defer server.Close()

	client := &RealDebrid{
		Host:   server.URL,
		client: request.New(request.WithMaxRetries(0)),
		config: config.Debrid{Name: "rd"},
	}

	tests := []struct {
		name   string
		magnet *utils.Magnet
	}{
		{
			name: "magnet",
			magnet: &utils.Magnet{
				Name: "Release",
				Link: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
			},
		},
		{
			name: "torrent file",
			magnet: &utils.Magnet{
				Name: "Release",
				File: []byte("torrent metadata"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.SubmitMagnet(&types.Torrent{Magnet: test.magnet, Name: test.magnet.Name})
			var customErr *customerror.Error
			if !errors.As(err, &customErr) {
				t.Fatalf("SubmitMagnet() error = %v, want typed custom error", err)
			}
			if customErr.Code != "torrent_content_rejected" || !customErr.IsPermanent() {
				t.Fatalf("SubmitMagnet() error = %#v, want permanent content rejection", customErr)
			}
		})
	}
}
