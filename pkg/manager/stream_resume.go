package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// resumeHTTPStream retries only the unread suffix of a committed response.
// It never calls the response-ready callback again and never changes provider;
// alternate-provider handoff is a separate policy layer.
func (m *Manager) resumeHTTPStream(
	ctx context.Context,
	candidateEntry *storage.Entry,
	candidate streamCandidate,
	filename string,
	rangePlan streamRangePlan,
	writer io.Writer,
	buf []byte,
	current streamTransferResult,
	downloadLink types.DownloadLink,
	expectedUpstreamTotal int64,
	linkRefreshes int,
	maxAttempts int,
) streamTransferResult {
	if maxAttempts <= 0 {
		return current
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			current.sourceErr = ctxErr
			return current
		}
		if current.sinkErr != nil || current.written >= rangePlan.expectedLen {
			return current
		}
		if attempt > 1 && !retryableCommittedSourceError(current.sourceErr) {
			return current
		}

		remaining := rangePlan.expectedLen - current.written
		resumeStart := rangePlan.upstreamStart + current.written
		m.streamSessionMetrics.httpResumeAttempts.Add(1)
		resp, requestErr := m.doRequest(
			ctx,
			downloadLink,
			candidate.provider,
			resumeStart,
			rangePlan.upstreamEnd,
			1,
		)
		if requestErr != nil {
			current.sourceErr = requestErr
			continue
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			linkErr := link.ClassifyHTTPStatus(resp.StatusCode, resp.Header)
			resp.Body.Close()
			if linkErr.ShouldRefetch() && linkRefreshes < maxLinkRefreshesPerStream {
				refreshed, refreshErr := m.linkService.Refresh(
					ctx,
					candidateEntry,
					filename,
					downloadLink,
				)
				if refreshErr != nil {
					current.sourceErr = fmt.Errorf("failed to refresh interrupted stream link: %w", refreshErr)
					return current
				}
				downloadLink = refreshed
				linkRefreshes++
				current.sourceErr = StreamError{Err: linkErr, Retryable: true, LinkError: true}
				continue
			}

			current.sourceErr = StreamError{
				Err:       linkErr,
				Retryable: linkErr.IsRetryable(),
				LinkError: linkErr.ShouldRefetch(),
			}
			continue
		}

		if integrityErr := validateStreamResponseIntegrity(
			resp,
			resumeStart,
			rangePlan.upstreamEnd,
			expectedUpstreamTotal,
			remaining,
			true,
		); integrityErr != nil {
			resp.Body.Close()
			current.sourceErr = StreamError{Err: integrityErr, Retryable: true}
			continue
		}

		reader := io.Reader(io.LimitReader(resp.Body, remaining))
		var firstByte [1]byte
		firstN, firstErr := io.ReadFull(reader, firstByte[:])
		if firstN == 0 {
			resp.Body.Close()
			if firstErr == nil || firstErr == io.EOF {
				firstErr = io.ErrUnexpectedEOF
			}
			current.sourceErr = fmt.Errorf("resumed upstream body failed before first byte: %w", firstErr)
			continue
		}

		resumed := transferStreamBody(
			writer,
			reader,
			firstByte[:firstN],
			remaining,
			buf,
		)
		resp.Body.Close()
		current.written += resumed.written
		current.sourceErr = resumed.sourceErr
		current.sinkErr = resumed.sinkErr
		if current.err() == nil {
			m.streamSessionMetrics.httpResumeSuccesses.Add(1)
			return current
		}
	}

	return current
}

func retryableCommittedSourceError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var streamErr StreamError
	if errors.As(err, &streamErr) {
		return streamErr.Retryable
	}
	return isConnectionError(err) || strings.Contains(strings.ToLower(err.Error()), "timeout")
}
