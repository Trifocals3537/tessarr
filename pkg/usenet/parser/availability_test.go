package parser

import (
	"errors"
	"strings"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/Trifocals3537/tessarr/internal/nntp"
)

func TestArticleProbeFailuresClassifiesOnlyUniformMissingArticles(t *testing.T) {
	t.Parallel()

	missingFirst := &nntp.Error{
		Type:    nntp.ErrorTypeArticleNotFound,
		Code:    430,
		Message: "first article missing",
	}
	missingSecond := &nntp.Error{
		Type:    nntp.ErrorTypeArticleNotFound,
		Code:    430,
		Message: "second article missing",
	}

	failures := articleProbeFailures{}
	failures.add(errors.Join(errors.New("wrapped"), missingFirst))
	failures.add(missingSecond)
	err := failures.unavailableError()
	if !errors.Is(err, ErrNZBArticlesUnavailable) {
		t.Fatalf("expected ErrNZBArticlesUnavailable, got %v", err)
	}
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("expected the first NNTP cause to remain discoverable, got %v", err)
	}
	if !nntp.IsArticleNotFoundError(errors.Unwrap(err)) {
		t.Fatalf("expected standard unwrapping to expose the NNTP cause, got %v", err)
	}
	if message := err.Error(); !strings.Contains(message, ErrNZBArticlesUnavailable.Error()) ||
		strings.Contains(message, "%!") {
		t.Fatalf("malformed unavailable-article error: %q", message)
	}
}

func TestProbeFileGroupConnectivityAcceptsPartialAvailability(t *testing.T) {
	t.Parallel()

	groups := map[string]*FileGroup{
		"missing": testConnectivityGroup("missing-id"),
		"present": testConnectivityGroup("present-id"),
	}
	err := probeFileGroupConnectivity(groups, func(messageID string) error {
		if messageID == "missing-id" {
			return &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("partially available NZB was rejected: %v", err)
	}
}

func TestProbeFileGroupConnectivityRejectsOnlyUniformMissingArticles(t *testing.T) {
	t.Parallel()

	groups := map[string]*FileGroup{
		"first":  testConnectivityGroup("first-id"),
		"second": testConnectivityGroup("second-id"),
	}
	err := probeFileGroupConnectivity(groups, func(string) error {
		return &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430}
	})
	if !errors.Is(err, ErrNZBArticlesUnavailable) {
		t.Fatalf("uniformly missing NZB was not rejected: %v", err)
	}
}

func testConnectivityGroup(messageID string) *FileGroup {
	return &FileGroup{
		ActualFilename: messageID,
		Files: []nzbparser.NzbFile{{
			Segments: nzbparser.NzbSegments{{Id: messageID}},
		}},
	}
}

func TestArticleProbeFailuresPreservesTransientAndMixedFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		errs []error
	}{
		{
			name: "timeout",
			errs: []error{nntp.NewTimeoutError(errors.New("deadline"))},
		},
		{
			name: "missing and timeout",
			errs: []error{
				&nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430},
				nntp.NewTimeoutError(errors.New("deadline")),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			failures := articleProbeFailures{}
			for _, err := range test.errs {
				failures.add(err)
			}
			if err := failures.unavailableError(); err != nil {
				t.Fatalf("transient or mixed failures were classified as unavailable: %v", err)
			}
		})
	}
}

func TestArticleProbeFailuresMergePreservesMixedClassification(t *testing.T) {
	t.Parallel()

	missing := articleProbeFailures{}
	missing.add(&nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430})
	other := articleProbeFailures{}
	other.add(errFileTypeUndetected)
	missing.merge(other)

	if err := missing.unavailableError(); err != nil {
		t.Fatalf("merged mixed failures were classified as unavailable: %v", err)
	}
}
