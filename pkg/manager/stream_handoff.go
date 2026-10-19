package manager

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// handoffHTTPStream continues only the unread suffix on another placement of
// the same torrent. The original response is already committed, so this path
// cannot change client-visible headers or replay bytes.
func (m *Manager) handoffHTTPStream(
	ctx context.Context,
	original *storage.Entry,
	failedCandidate streamCandidate,
	fallbackCandidates []streamCandidate,
	filename string,
	rangePlan streamRangePlan,
	writer io.Writer,
	buf []byte,
	current streamTransferResult,
) streamTransferResult {
	if !committedHandoffIdentityValid(original, filename, rangePlan) ||
		current.sinkErr != nil || current.sourceErr == nil ||
		current.written >= rangePlan.expectedLen || ctx.Err() != nil ||
		errors.Is(current.sourceErr, context.Canceled) ||
		errors.Is(current.sourceErr, context.DeadlineExceeded) {
		return current
	}

	seen := map[string]struct{}{strings.ToLower(strings.TrimSpace(failedCandidate.provider)): {}}
	consideredAlternate := false
	for _, candidate := range fallbackCandidates {
		providerKey := strings.ToLower(strings.TrimSpace(candidate.provider))
		if providerKey == "" {
			continue
		}
		if _, duplicate := seen[providerKey]; duplicate {
			continue
		}
		seen[providerKey] = struct{}{}
		if !streamPlacementReady(candidate.placement, filename) {
			continue
		}
		consideredAlternate = true
		if ctxErr := ctx.Err(); ctxErr != nil {
			current.sourceErr = ctxErr
			return current
		}

		candidateEntry := candidate.entryForAttempt(original, filename)
		candidateCtx := link.WithFailFast(link.WithoutRepair(ctx))
		downloadLink, linkErr := m.linkService.GetLink(candidateCtx, candidateEntry, filename)
		if linkErr != nil {
			m.recordStreamFileCircuitFailure(original, filename, candidate.provider, linkErr)
			failureClass, _ := classifyStreamProviderFailure(linkErr)
			if failureClass == "" {
				failureClass = "unknown"
			}
			m.logger.Debug().
				Str("event", "stream.handoff").
				Str("outcome", "link_failed").
				Str("stream_request_id", streamRequestID(ctx)).
				Str("failed_provider", failedCandidate.provider).
				Str("provider", candidate.provider).
				Str("file", filename).
				Str("failure_class", failureClass).
				Msg("Committed stream handoff could not obtain alternate link")
			continue
		}

		expectedUpstreamTotal := downloadLink.Size
		if expectedUpstreamTotal <= 0 && !rangePlan.rooted {
			expectedUpstreamTotal = rangePlan.logicalSize
		}
		m.streamHandoffAttempts.Add(1)
		m.streamFailoverAttempts.Add(1)

		current = m.resumeHTTPStream(
			candidateCtx,
			candidateEntry,
			candidate,
			filename,
			rangePlan,
			writer,
			buf,
			current,
			downloadLink,
			expectedUpstreamTotal,
			0,
			m.streamConnectionAttempts(),
		)
		if current.err() == nil {
			m.streamHandoffSuccesses.Add(1)
			m.markStreamFileCircuitSuccess(original, filename, candidate.provider)
			m.markStreamProviderReady(ctx, original, filename, candidate)
			m.logger.Info().
				Str("event", "stream.handoff").
				Str("outcome", "succeeded").
				Str("stream_request_id", streamRequestID(ctx)).
				Str("failed_provider", failedCandidate.provider).
				Str("provider", candidate.provider).
				Str("file", filename).
				Int64("bytes_delivered", current.written).
				Msg("Committed stream handoff completed")
			return current
		}
		if current.sinkErr != nil || ctx.Err() != nil ||
			errors.Is(current.sourceErr, context.Canceled) ||
			errors.Is(current.sourceErr, context.DeadlineExceeded) {
			return current
		}

		failureClass, _ := classifyStreamProviderFailure(current.sourceErr)
		m.recordStreamFileCircuitFailure(original, filename, candidate.provider, StreamError{
			Err:       current.sourceErr,
			Retryable: true,
		})
		if failureClass == "" {
			failureClass = "unknown"
		}
		m.logger.Debug().
			Str("event", "stream.handoff").
			Str("outcome", "failed").
			Str("stream_request_id", streamRequestID(ctx)).
			Str("provider", candidate.provider).
			Str("file", filename).
			Int64("bytes_delivered", current.written).
			Str("failure_class", failureClass).
			Msg("Committed stream handoff provider failed")
		failedCandidate = candidate
	}

	if consideredAlternate {
		m.streamFailoverExhausted.Add(1)
	}
	return current
}

func committedHandoffIdentityValid(original *storage.Entry, filename string, rangePlan streamRangePlan) bool {
	if original == nil || strings.TrimSpace(original.InfoHash) == "" {
		return false
	}
	file := original.Files[filename]
	if file == nil || file.Size != rangePlan.logicalSize || rangePlan.expectedLen <= 0 {
		return false
	}
	if fileHash := strings.TrimSpace(file.InfoHash); fileHash != "" &&
		!strings.EqualFold(fileHash, strings.TrimSpace(original.InfoHash)) {
		return false
	}
	if rangePlan.upstreamEnd < rangePlan.upstreamStart ||
		rangePlan.expectedLen != rangePlan.logicalEnd-rangePlan.logicalStart+1 {
		return false
	}
	return true
}
