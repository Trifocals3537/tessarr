package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestTautulliWebhookRejectsUntargetedPayloadBeforeRepairLookup(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing media id", body: `{"topic":"tautulli"}`},
		{name: "blank media id", body: `{"topic":"tautulli","media_id":"  "}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &Server{logger: zerolog.Nop()}
			request := httptest.NewRequest(http.MethodPost, "/webhooks/tautulli", strings.NewReader(test.body))
			response := httptest.NewRecorder()

			server.handleTautulli(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
			if !strings.Contains(response.Body.String(), "media ID") {
				t.Fatalf("response = %q, want a media ID error", response.Body.String())
			}
		})
	}
}
