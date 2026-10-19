package request

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type failingReadCloser struct {
	closed bool
}

func (*failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("original body was read") }
func (body *failingReadCloser) Close() error {
	body.closed = true
	return nil
}

func TestDoUsesReplayableBodyFactoryWithoutReadingOriginal(t *testing.T) {
	payload := []byte("replayable multipart payload")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("body = %q, want %q", got, payload)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	originalBody := &failingReadCloser{}
	req, err := http.NewRequest(http.MethodPost, server.URL, originalBody)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(payload))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(payload)), nil
	}

	client := New(WithMaxRetries(0))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	_ = resp.Body.Close()
	if !originalBody.closed {
		t.Fatal("Do() did not close the unused original body")
	}
}
