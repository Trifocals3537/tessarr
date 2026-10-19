package manager

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestCommittedHandoffIdentityValid(t *testing.T) {
	entry := streamFailoverEntry("primary", "fallback")
	plan, err := buildStreamRangePlan(entry.Files["video.mkv"], 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !committedHandoffIdentityValid(entry, "video.mkv", plan) {
		t.Fatal("valid identical-hash placement was rejected")
	}

	t.Run("missing entry hash", func(t *testing.T) {
		copy := *entry
		copy.InfoHash = ""
		if committedHandoffIdentityValid(&copy, "video.mkv", plan) {
			t.Fatal("handoff accepted without a stable entry hash")
		}
	})

	t.Run("mismatched file hash", func(t *testing.T) {
		copy := *entry
		copy.Files = map[string]*storage.File{"video.mkv": {
			Name:     "video.mkv",
			Size:     4,
			InfoHash: "different-hash",
		}}
		if committedHandoffIdentityValid(&copy, "video.mkv", plan) {
			t.Fatal("handoff accepted mismatched file identity")
		}
	})

	t.Run("changed logical geometry", func(t *testing.T) {
		copy := *entry
		copy.Files = map[string]*storage.File{"video.mkv": {
			Name: "video.mkv",
			Size: 5,
		}}
		if committedHandoffIdentityValid(&copy, "video.mkv", plan) {
			t.Fatal("handoff accepted changed logical geometry")
		}
	})
}

func TestCommittedHandoffRejectsCanceledReads(t *testing.T) {
	for _, canceledErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(canceledErr.Error(), func(t *testing.T) {
			links := &failoverLinkService{}
			manager := newStreamFailoverTestManager(links, http.DefaultClient, "primary", "fallback")
			entry := streamFailoverEntry("primary", "fallback")
			plan, err := buildStreamRangePlan(entry.Files["video.mkv"], 0, 3)
			if err != nil {
				t.Fatal(err)
			}
			candidates := manager.streamCandidates(entry, "video.mkv")
			result := manager.handoffHTTPStream(
				context.Background(),
				entry,
				candidates[0],
				candidates[1:],
				"video.mkv",
				plan,
				io.Discard,
				make([]byte, 16),
				streamTransferResult{written: 2, sourceErr: canceledErr},
			)
			if result.sourceErr != canceledErr {
				t.Fatalf("source error = %v, want %v", result.sourceErr, canceledErr)
			}
			if got := links.callOrder(); len(got) != 0 {
				t.Fatalf("provider link calls = %v, want none", got)
			}
			if stats := manager.StreamFailoverStats(); stats.CommittedHandoffs != 0 {
				t.Fatalf("stream failover stats = %+v, want no handoff", stats)
			}
		})
	}
}
