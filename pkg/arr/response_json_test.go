package arr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/request"
)

func TestRequestCtxJSONResponses(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)
	for _, test := range []struct {
		name    string
		body    string
		status  int
		want    string
		wantErr bool
	}{
		{name: "object", body: `{"value":"decoded"}`, want: "decoded"},
		{name: "whitespace", body: "{\"value\":\"decoded\"}\n\t ", want: "decoded"},
		{name: "empty", want: "original"},
		{name: "blank", body: "\n\t ", want: "original"},
		{name: "no content", status: http.StatusNoContent, want: "original"},
		{name: "null", body: "null", want: "original"},
		{name: "truncated", body: `{"value":`, wantErr: true},
		{name: "malformed", body: `not-json`, wantErr: true},
		{name: "second value", body: `{"value":"decoded"} {}`, wantErr: true},
		{name: "trailing junk", body: `{"value":"decoded"} garbage`, wantErr: true},
		{name: "HTTP error is not decoded", status: http.StatusBadRequest, body: `not-json`, want: "original"},
	} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/chunked=%v", test.name, chunked), func(t *testing.T) {
				status := test.status
				if status == 0 {
					status = http.StatusOK
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !chunked {
						w.Header().Set("Content-Length", fmt.Sprint(len(test.body)))
					}
					w.WriteHeader(status)
					if chunked {
						w.(http.Flusher).Flush()
					}
					_, _ = io.WriteString(w, test.body)
				}))
				defer server.Close()
				a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
				out := struct {
					Value string `json:"value"`
				}{Value: "original"}
				resp, err := a.RequestCtx(context.Background(), http.MethodGet, "/test", nil, &out)
				if (err != nil) != test.wantErr {
					t.Fatalf("decode error = %v, want error=%v", err, test.wantErr)
				}
				if resp == nil || resp.StatusCode != status {
					t.Fatalf("response status not preserved: %v", resp)
				}
				if chunked && status != http.StatusNoContent && (len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked") {
					t.Fatalf("response was not actually chunked: %v", resp.TransferEncoding)
				}
				if !test.wantErr && out.Value != test.want {
					t.Fatalf("decoded value = %q, want %q", out.Value, test.want)
				}
			})
		}
	}
}

func TestRequestCtxJSONBodyCancellation(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)
	for _, prefix := range []string{`{"value":`, `{"value":"decoded"}`} {
		t.Run(prefix, func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, prefix)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
			done := make(chan error, 1)
			go func() {
				var out map[string]string
				_, err := a.RequestCtx(ctx, http.MethodGet, "/test", nil, &out)
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("response body did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("decode cancellation = %v, want context.Canceled", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("response decoding did not stop after cancellation")
			}
		})
	}
}

func TestRequestCtxResponseOnlyPreservesBodyOwnership(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)
	const body = `{"accepted":true}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
	resp, err := a.RequestCtx(context.Background(), http.MethodGet, "/test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != body {
		t.Fatalf("response-only body was consumed or closed: %q, %v", data, err)
	}
}

func TestRequestCtxJSONHonorsClientTimeout(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)
	sharedOnce.Do(func() {
		sharedClient = request.New(request.WithTimeout(50*time.Millisecond), request.WithMaxRetries(0))
	})
	t.Cleanup(func() { resetSharedArrClientForTest(t) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"value":`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var out map[string]string
	_, err := a.RequestCtx(ctx, http.MethodGet, "/test", nil, &out)
	if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("client timeout did not bound response decoding: err=%v, parent=%v", err, ctx.Err())
	}
}

func TestRequestCtxJSONFailuresReleaseConnections(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	resetSharedArrClientForTest(t)
	for _, body := range []string{`{"value":`, `{"value":"decoded"} []`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for attempt := range 40 {
				var out map[string]string
				resp, err := a.RequestCtx(ctx, http.MethodGet, "/test", nil, &out)
				if err == nil || resp == nil || ctx.Err() != nil {
					t.Fatalf("request %d did not return a bounded decode failure: resp=%v err=%v", attempt+1, resp, err)
				}
			}
		})
	}
}

type arrJSONRecord struct {
	ID   int    `json:"id"`
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func arrJSONPayload(tb testing.TB, records int) []byte {
	tb.Helper()
	rows := make([]arrJSONRecord, records)
	for i := range rows {
		rows[i] = arrJSONRecord{ID: i + 1, Path: fmt.Sprintf("/media/Series/Season %02d/Episode %05d %s.mkv", i%20+1, i, strings.Repeat("x", 64)), Size: 3_500_000_000}
	}
	data, err := json.Marshal(rows)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func BenchmarkArrResponseJSON(b *testing.B) {
	config.SetConfigPath(b.TempDir())
	for _, records := range []int{500, 5000, 20000} {
		data := arrJSONPayload(b, records)
		for _, chunked := range []bool{false, true} {
			b.Run(fmt.Sprintf("records=%d/chunked=%v", records, chunked), func(b *testing.B) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !chunked {
						w.Header().Set("Content-Length", fmt.Sprint(len(data)))
					}
					if chunked {
						w.(http.Flusher).Flush()
					}
					reader := bytes.NewReader(data)
					buffer := make([]byte, 16<<10)
					for reader.Len() > 0 {
						n, _ := reader.Read(buffer)
						if _, err := w.Write(buffer[:n]); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
				}))
				defer server.Close()
				a := New("sonarr", server.URL, "test-token", false, nil, "", "manual")
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var out []arrJSONRecord
					_, err := a.RequestCtx(context.Background(), http.MethodGet, "/test", nil, &out)
					if err != nil || len(out) != records {
						b.Fatalf("decoded %d records: %v", len(out), err)
					}
				}
			})
		}
	}
}
