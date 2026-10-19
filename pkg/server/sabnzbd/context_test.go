package sabnzbd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/arr"
)

func TestCategoryContextAcceptsSABCatParameter(t *testing.T) {
	s := &SABnzbd{}
	handler := s.categoryContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := getCategory(r.Context()); got != "sonarr" {
			t.Fatalf("category = %q, want sonarr", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/sabnzbd/api?cat=sonarr", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}

func TestModeContextPreservesAuthenticatedArr(t *testing.T) {
	s := &SABnzbd{}
	downloadUncached := true
	authenticated := arr.New(
		"sonarr",
		"http://sonarr:8989",
		"configured-token",
		false,
		&downloadUncached,
		"torbox",
		"config",
	)
	handler := s.modeContext(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := getArrFromContext(r.Context()); got != authenticated {
			t.Fatalf("authenticated Arr was replaced: got %#v, want %#v", got, authenticated)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/sabnzbd/api?mode=queue&cat=sonarr", nil)
	request = request.WithContext(context.WithValue(request.Context(), arrKey, authenticated))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
}
