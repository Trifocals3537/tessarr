package request

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

func TestMakeRequestBoundsSuccessBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes.Repeat([]byte("x"), int(utils.MaxJSONResponseBytes)+1))
	}))
	defer server.Close()

	client := New(WithMaxRetries(0))
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.MakeRequest(req); err == nil {
		t.Fatal("MakeRequest() accepted an oversized response")
	}
}

func TestMakeRequestDoesNotReflectErrorBody(t *testing.T) {
	const secret = "provider-secret-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "signed URL "+secret, http.StatusBadRequest)
	}))
	defer server.Close()

	client := New(WithMaxRetries(0))
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.MakeRequest(req)
	if err == nil {
		t.Fatal("MakeRequest() unexpectedly accepted an error response")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("MakeRequest() reflected a sensitive response body: %v", err)
	}
}
