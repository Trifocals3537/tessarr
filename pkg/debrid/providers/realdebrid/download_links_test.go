package realdebrid

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestResolveRealDebridFileDownloadLinksBoundsConcurrency(t *testing.T) {
	const (
		fileCount = 200
		limit     = 5
	)
	files := make(map[string]types.File, fileCount)
	for i := range fileCount {
		name := fmt.Sprintf("file-%03d.mkv", i)
		files[name] = types.File{Name: name}
	}
	torrent := &types.Torrent{Id: "torrent-id", Files: files}

	var active atomic.Int32
	var peak atomic.Int32
	links, err := resolveRealDebridFileDownloadLinks(
		torrent,
		limit,
		func(_ string, file *types.File) (types.DownloadLink, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				observed := peak.Load()
				if current <= observed || peak.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			return types.DownloadLink{
				Id:           file.Name,
				Filename:     file.Name,
				DownloadLink: "https://example.invalid/" + file.Name,
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrency = %d, want at most %d", got, limit)
	}
	if len(links) != fileCount || len(torrent.Files) != fileCount {
		t.Fatalf(
			"resolved links/files = %d/%d, want %d/%d",
			len(links),
			len(torrent.Files),
			fileCount,
			fileCount,
		)
	}
}

func TestResolveRealDebridFileDownloadLinksWaitsForBoundedBatchOnError(t *testing.T) {
	files := make(map[string]types.File, 25)
	for i := range 25 {
		name := fmt.Sprintf("file-%02d.mkv", i)
		files[name] = types.File{Name: name}
	}
	torrent := &types.Torrent{Id: "torrent-id", Files: files}
	var attempted atomic.Int32

	links, err := resolveRealDebridFileDownloadLinks(
		torrent,
		3,
		func(_ string, file *types.File) (types.DownloadLink, error) {
			attempted.Add(1)
			if file.Name == "file-07.mkv" {
				return types.DownloadLink{}, errors.New("injected link failure")
			}
			return types.DownloadLink{
				Id:           file.Name,
				Filename:     file.Name,
				DownloadLink: "https://example.invalid/" + file.Name,
			}, nil
		},
	)
	if err == nil {
		t.Fatal("resolver error = nil")
	}
	if links != nil {
		t.Fatalf("links = %v, want nil on partial failure", links)
	}
	if got := attempted.Load(); got != int32(len(files)) {
		t.Fatalf("attempted files = %d, want %d", got, len(files))
	}
	if len(torrent.Files) != len(files) {
		t.Fatal("partial result replaced the torrent's original file map")
	}
}

func TestResolveRealDebridFileDownloadLinksRejectsInvalidInputs(t *testing.T) {
	resolver := func(string, *types.File) (types.DownloadLink, error) {
		return types.DownloadLink{}, nil
	}
	if _, err := resolveRealDebridFileDownloadLinks(nil, 1, resolver); err == nil {
		t.Fatal("nil torrent was accepted")
	}
	if _, err := resolveRealDebridFileDownloadLinks(&types.Torrent{}, 0, resolver); err == nil {
		t.Fatal("zero concurrency was accepted")
	}
	if _, err := resolveRealDebridFileDownloadLinks(&types.Torrent{}, 1, nil); err == nil {
		t.Fatal("nil resolver was accepted")
	}
}
