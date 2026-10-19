package realdebrid

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestCollectRealDebridDownloadLinksRequiresCompleteProgressingPages(t *testing.T) {
	t.Run("complete", func(t *testing.T) {
		calls := 0
		links, err := collectRealDebridDownloadLinks(func(offset, limit int) ([]types.DownloadLink, error) {
			calls++
			if limit != realDebridDownloadListPageSize {
				t.Fatalf("limit = %d", limit)
			}
			if offset != 0 {
				t.Fatalf("offset = %d, want 0", offset)
			}
			return []types.DownloadLink{{Id: "one"}, {Id: "two"}}, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if calls != 1 || len(links) != 2 {
			t.Fatalf("calls = %d, links = %#v", calls, links)
		}
	})

	t.Run("later page error discards partial result", func(t *testing.T) {
		wantErr := errors.New("page failed")
		calls := 0
		links, err := collectRealDebridDownloadLinks(func(offset, _ int) ([]types.DownloadLink, error) {
			calls++
			if calls == 1 {
				page := make([]types.DownloadLink, realDebridDownloadListPageSize)
				for index := range page {
					page[index].Id = fmt.Sprintf("id-%d", index)
				}
				return page, nil
			}
			if offset != realDebridDownloadListPageSize {
				t.Fatalf("second offset = %d", offset)
			}
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) || links != nil {
			t.Fatalf("links = %#v, err = %v", links, err)
		}
	})

	t.Run("repeated page is rejected", func(t *testing.T) {
		page := make([]types.DownloadLink, realDebridDownloadListPageSize)
		for index := range page {
			page[index].Id = fmt.Sprintf("id-%d", index)
		}
		_, err := collectRealDebridDownloadLinks(func(_, _ int) ([]types.DownloadLink, error) {
			return page, nil
		})
		if err == nil || !strings.Contains(err.Error(), "repeated ID") {
			t.Fatalf("error = %v, want repeated ID", err)
		}
	})
}
