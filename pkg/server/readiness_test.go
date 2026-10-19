package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/manager"
)

func TestLivenessAndReadinessHandlers(t *testing.T) {
	s := &Server{manager: &manager.Manager{}}

	live := httptest.NewRecorder()
	s.handleLiveness(live, httptest.NewRequest(http.MethodGet, "/live", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("liveness status = %d, want %d", live.Code, http.StatusOK)
	}
	if got := live.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("liveness Cache-Control = %q, want no-store", got)
	}

	ready := httptest.NewRecorder()
	s.handleReadiness(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status = %d, want %d", ready.Code, http.StatusServiceUnavailable)
	}
	if got := ready.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	if got := ready.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("readiness Cache-Control = %q, want no-store", got)
	}
	var status manager.RuntimeStatus
	if err := json.Unmarshal(ready.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode readiness response: %v", err)
	}
	if status.Ready {
		t.Fatalf("readiness body = %+v, want unready", status)
	}
}
