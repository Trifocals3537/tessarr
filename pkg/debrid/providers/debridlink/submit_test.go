package debridlink

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func newSubmitTestProvider(t *testing.T, handler http.HandlerFunc) *DebridLink {
	return newDebridLinkTestProvider(t, config.Debrid{
		Name:   "debridlink",
		APIKey: "secret-token",
	}, handler)
}

func newDebridLinkTestProvider(
	t *testing.T,
	debridConfig config.Debrid,
	handler http.HandlerFunc,
) *DebridLink {
	t.Helper()
	previousPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	provider, err := New(debridConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider.Host = server.URL
	return provider
}

func writeSubmitSuccess(t *testing.T, w http.ResponseWriter, value string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"success":true,"value":`+value+`}`)
}

func TestSubmitMagnetUploadsTorrentFileAsMultipart(t *testing.T) {
	wantFile := []byte("d4:infod4:name12:release.mkvee")
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/seedbox/add" {
			t.Errorf("request = %s %s, want POST /seedbox/add", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Errorf("Authorization = %q, want configured bearer token", got)
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			t.Fatalf("Content-Type = %q, want multipart/form-data: %v", r.Header.Get("Content-Type"), err)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		gotFile, err := io.ReadAll(file)
		if err != nil {
			t.Fatal(err)
		}
		if header.Filename != "upload.torrent" || string(gotFile) != string(wantFile) {
			t.Errorf("upload = (%q, %q), want (%q, %q)", header.Filename, gotFile, "upload.torrent", wantFile)
		}
		if got := r.FormValue("url"); got != "" {
			t.Errorf("url field = %q, want empty for file upload", got)
		}
		// Debrid Link can return the ID before torrent metadata has been resolved.
		writeSubmitSuccess(t, w, `{"id":"torrent-42"}`)
	})

	torrent, err := provider.SubmitMagnet(&types.Torrent{
		Magnet: &utils.Magnet{File: wantFile},
	})
	if err != nil {
		t.Fatal(err)
	}
	if torrent.Id != "torrent-42" || torrent.Debrid != "debridlink" {
		t.Fatalf("torrent = %#v, want accepted incomplete metadata with provider ID", torrent)
	}
	if torrent.Added.IsZero() {
		t.Fatal("Added is zero, want a useful local submission timestamp")
	}
}

func TestSubmitMagnetSendsJSONURL(t *testing.T) {
	const magnetLink = "magnet:?xt=urn:btih:0123456789012345678901234567890123456789"
	provider := newSubmitTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json: %v", r.Header.Get("Content-Type"), err)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if got := payload["url"]; got != magnetLink {
			t.Errorf("url = %q, want %q", got, magnetLink)
		}
		writeSubmitSuccess(t, w, `{"id":"torrent-43","hashString":"provider-hash"}`)
	})

	torrent, err := provider.SubmitMagnet(&types.Torrent{
		Magnet: &utils.Magnet{Link: magnetLink},
	})
	if err != nil {
		t.Fatal(err)
	}
	if torrent.Id != "torrent-43" || torrent.InfoHash != "provider-hash" {
		t.Fatalf("torrent = %#v, want submitted ID and provider hash", torrent)
	}
}

func TestSubmitMagnetValidatesSourcesBeforeRequest(t *testing.T) {
	provider := &DebridLink{Host: "http://127.0.0.1:1"}
	tests := []struct {
		name    string
		torrent *types.Torrent
		want    string
	}{
		{name: "missing torrent", want: "source is missing"},
		{name: "missing magnet", torrent: &types.Torrent{}, want: "source is missing"},
		{name: "empty magnet", torrent: &types.Torrent{Magnet: &utils.Magnet{}}, want: "magnet link is empty"},
		{name: "empty torrent file", torrent: &types.Torrent{Magnet: &utils.Magnet{File: []byte{}}}, want: "torrent file is empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := provider.newSubmitRequest(test.torrent)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newSubmitRequest() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSubmitMagnetRejectsInvalidSuccessEnvelope(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing value", body: `{"success":true}`, want: "error adding torrent"},
		{name: "missing id", body: `{"success":true,"value":{"name":"unresolved"}}`, want: "empty torrent ID"},
		{name: "trailing json", body: `{"success":true,"value":{"id":"42"}} {}`, want: "multiple values"},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := newSubmitTestProvider(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, test.body)
			})
			_, err := provider.SubmitMagnet(&types.Torrent{Magnet: &utils.Magnet{Link: "magnet:?xt=urn:btih:test"}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("SubmitMagnet() error = %v, want %q", err, test.want)
			}
		})
	}
}
