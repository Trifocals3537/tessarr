package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestTorrentDownloadResolvesEveryLinkBeforeCreatingAnyPartial(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "all-links", config.DownloadActionDownload)
	entry.Files = map[string]*storage.File{
		"a.mkv": {Name: "a.mkv", Path: "Release/a.mkv", Size: 1},
		"b.mkv": {Name: "b.mkv", Path: "Release/b.mkv", Size: 1},
	}
	downloader := &Downloader{
		manager: &Manager{},
		dest:    root,
		logger:  zerolog.Nop(),
		torrentLink: func(_ context.Context, _ *storage.Entry, name string) (string, error) {
			if name == "b.mkv" {
				return "", errors.New("missing link")
			}
			return "https://example.invalid/a", nil
		},
	}
	err := downloader.processTorrentDownload(context.Background(), entry)
	if err == nil || !strings.Contains(err.Error(), "resolve all torrent download links") {
		t.Fatalf("partial link resolution error = %v", err)
	}
	if entry.IsComplete {
		t.Fatal("entry was marked complete after partial link resolution")
	}
	entries, err := os.ReadDir(entry.DownloadPath())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range entries {
		if item.Name() != torrentOwnerMarkerName {
			t.Fatalf("link resolution failure created artifact %q", item.Name())
		}
	}
}

func TestRootedTorrentDownloadRestartsOn200AndPublishesAtomically(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "http-restart", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.file.Write([]byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=3-4" {
			t.Errorf("Range = %q, want bytes=3-4", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("fresh"))
	}))
	defer server.Close()
	downloader := rootedTorrentTestDownloader(server.Client())
	if err := downloader.localDownloader(context.Background(), server.URL+"/file", part, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := part.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTorrentTestContents(t, filepath.Join(entry.DownloadPath(), "movie.mkv"), "fresh")
}

func TestRootedTorrentDownloadRejectsMismatchedContentRangeAndTotal(t *testing.T) {
	tests := []struct {
		name         string
		contentRange string
	}{
		{name: "bounds", contentRange: "bytes 1-4/5"},
		{name: "full total", contentRange: "bytes 2-4/99"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			entry := torrentOwnershipTestEntry(root, "range-"+test.name, config.DownloadActionDownload)
			entry.InfoHash = strings.ReplaceAll(entry.InfoHash, " ", "-")
			if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
				t.Fatal(err)
			}
			part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 5)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.file.Write([]byte("ab")); err != nil {
				t.Fatal(err)
			}
			if err := part.Close(); err != nil {
				t.Fatal(err)
			}
			part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
			if err != nil {
				t.Fatal(err)
			}
			defer part.Close()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Range", test.contentRange)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte("cde"))
			}))
			defer server.Close()
			err = rootedTorrentTestDownloader(server.Client()).localDownloader(
				context.Background(),
				server.URL+"/file",
				part,
				nil,
				nil,
			)
			if err == nil {
				t.Fatal("mismatched Content-Range was accepted")
			}
			size, statErr := part.Size()
			if statErr != nil {
				t.Fatal(statErr)
			}
			if size != 2 {
				t.Fatalf("partial size after rejected response = %d, want 2", size)
			}
		})
	}
}

func TestRootedTorrentDownload416RequiresCompletePart(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "range-416", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.file.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()
	downloader := rootedTorrentTestDownloader(server.Client())
	if err := downloader.localDownloader(context.Background(), server.URL+"/file", part, nil, nil); err == nil {
		t.Fatal("416 was accepted for an incomplete partial file")
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}

	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.file.Write([]byte("cde")); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	if err := downloader.localDownloader(context.Background(), server.URL+"/file", part, nil, nil); err != nil {
		t.Fatalf("complete partial file was not accepted: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("complete partial issued an unnecessary request; requests=%d", got)
	}
}

func TestRootedTorrentDownloadCancellationPreservesSafeResume(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "cancel-resume", config.DownloadActionDownload)
	entry.Files["movie.mkv"].Size = 5
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	firstBodyRead := make(chan struct{})
	client := &http.Client{Transport: torrentRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestNumber := requests.Add(1)
		if requestNumber == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: &torrentCancellationBody{
					ctx:       r.Context(),
					firstRead: firstBodyRead,
				},
				Request: r,
			}, nil
		}
		if got := r.Header.Get("Range"); got != "bytes=2-4" {
			t.Errorf("resume Range = %q, want bytes=2-4", got)
		}
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header: http.Header{
				"Content-Range": []string{"bytes 2-4/5"},
			},
			Body:    io.NopCloser(strings.NewReader("cde")),
			Request: r,
		}, nil
	})}
	downloader := rootedTorrentTestDownloader(client)

	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- downloader.localDownloader(ctx, "https://downloads.example/file", part, nil, nil)
	}()
	<-firstBodyRead
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled download error = %v", err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}

	part, err = openOwnedTorrentPart(root, entry, "movie.mkv", 5)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	if size, err := part.Size(); err != nil || size != 2 {
		t.Fatalf("resumable part size = %d, error=%v, want 2", size, err)
	}
	if err := downloader.localDownloader(context.Background(), "https://downloads.example/file", part, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := part.Commit(); err != nil {
		t.Fatal(err)
	}
	assertTorrentTestContents(t, filepath.Join(entry.DownloadPath(), "movie.mkv"), "abcde")
}

func TestRootedTorrentDownloadSupportsRARRangeSizeDifferentFromLogicalSize(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "rar-range", config.DownloadActionDownload)
	file := entry.Files["movie.mkv"]
	file.Size = 1_000
	file.ByteRange = &[2]int64{10, 13}
	if got := torrentTransferSize(file); got != 4 {
		t.Fatalf("torrentTransferSize() = %d, want 4", got)
	}
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", torrentTransferSize(file))
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=10-13" {
			t.Errorf("Range = %q, want bytes=10-13", got)
		}
		w.Header().Set("Content-Range", "bytes 10-13/9999")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	if err := rootedTorrentTestDownloader(server.Client()).localDownloader(
		context.Background(),
		server.URL+"/archive",
		part,
		file.ByteRange,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := part.Commit(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(entry.DownloadPath(), "movie.mkv"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 4 {
		t.Fatalf("ranged output size = %d, want 4", info.Size())
	}
}

func TestRootedTorrentDownloadErrorsRedactSignedURL(t *testing.T) {
	root := t.TempDir()
	entry := torrentOwnershipTestEntry(root, "redacted-url", config.DownloadActionDownload)
	if _, _, err := claimTorrentEntryDirectory(root, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	part, err := openOwnedTorrentPart(root, entry, "movie.mkv", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusInternalServerError)
	}))
	defer server.Close()
	const secret = "super-secret-signed-token"
	err = rootedTorrentTestDownloader(server.Client()).localDownloader(
		context.Background(),
		fmt.Sprintf("%s/file?token=%s", server.URL, secret),
		part,
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("unexpected status was accepted")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "token=") {
		t.Fatalf("download error leaked signed URL: %v", err)
	}
}

func rootedTorrentTestDownloader(client *http.Client) *Downloader {
	return &Downloader{
		manager: &Manager{streamClient: client},
	}
}

type torrentRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn torrentRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type torrentCancellationBody struct {
	ctx       context.Context
	firstRead chan struct{}
	returned  bool
}

func (body *torrentCancellationBody) Read(buffer []byte) (int, error) {
	if !body.returned {
		body.returned = true
		written := copy(buffer, "ab")
		close(body.firstRead)
		return written, nil
	}
	<-body.ctx.Done()
	return 0, body.ctx.Err()
}

func (*torrentCancellationBody) Close() error {
	return nil
}
