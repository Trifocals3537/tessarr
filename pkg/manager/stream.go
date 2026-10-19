package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/retry"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	streamBufferSize = 256 * 1024
)

// streamBufPool provides reusable buffers for streaming to reduce GC pressure.
// Each buffer is 256KB - prevents per-request allocations.
var streamBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, streamBufferSize)
		return &buf
	},
}

// ActiveStream represents a currently active streaming file
type ActiveStream struct {
	ID         string `json:"id"`
	EntryName  string `json:"entry_name"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	Source     string `json:"source"` // "torrent" or "nzb"
	StartedAt  int64  `json:"started_at"`
	LastActive int64  `json:"last_active"` // Last activity timestamp (for observability)
	Debrid     string `json:"debrid,omitempty"`
	Client     string `json:"client,omitempty"` // Client identifier (User-Agent for WebDAV, "DFS" for DFS)
}

// === Active Streams Tracking ===

// registerStream registers an active stream for observability.
// Returns the stream ID so the caller can remove it when streaming completes.
func (m *Manager) registerStream(entryName, fileName string, fileSize int64, source, debrid, client string) string {
	// Each consumer gets its own identity. A deterministic file key caused two
	// clients playing the same file to overwrite each other, and the first close
	// then removed the remaining live stream from observability.
	streamID := fmt.Sprintf("s%016x", m.activeStreamSequence.Add(1))
	now := utils.NowUnix()

	stream := &ActiveStream{
		ID:         streamID,
		EntryName:  entryName,
		FileName:   fileName,
		FileSize:   fileSize,
		Source:     source,
		StartedAt:  now,
		LastActive: now,
		Debrid:     debrid,
		Client:     client,
	}

	m.activeStreams.Store(streamID, stream)
	return streamID
}

// unregisterStream removes an active stream entry if it exists.
func (m *Manager) unregisterStream(streamID string) {
	if streamID == "" {
		return
	}
	m.activeStreams.Delete(streamID)
}

// GetActiveStreams returns all currently active streams.
func (m *Manager) GetActiveStreams() []*ActiveStream {
	var streams []*ActiveStream
	m.activeStreams.Range(func(_ string, stream *ActiveStream) bool {
		streams = append(streams, stream)
		return true
	})
	return streams
}

// GetActiveStreamsCount returns the number of active streams.
func (m *Manager) GetActiveStreamsCount() int {
	return m.activeStreams.Size()
}

type StreamError struct {
	Err             error
	Retryable       bool
	LinkError       bool // true if we should try a new link
	StreamRequestID string
}

func (e StreamError) Error() string {
	return e.Err.Error()
}

func (e StreamError) Unwrap() error {
	return e.Err
}

func (e StreamError) IsRetryable() bool {
	return e.Retryable
}

func (e StreamError) RequestID() string {
	return e.StreamRequestID
}

// StreamMetadata describes the headers/status for a streaming response before data flows.
type StreamMetadata struct {
	Header        http.Header
	StatusCode    int
	ContentLength int64
}

// streamRangePlan keeps the client's logical file range separate from the
// upstream resource range. They differ for stored files exposed from inside a
// RAR archive: clients address the extracted file from zero, while the CDN must
// receive the file's absolute archive offsets.
type streamRangePlan struct {
	logicalStart  int64
	logicalEnd    int64
	logicalSize   int64
	upstreamStart int64
	upstreamEnd   int64
	expectedLen   int64
	rooted        bool
}

// StreamReadyFunc allows callers to copy headers/status before streaming begins.
type StreamReadyFunc func(*StreamMetadata) error

// isConnectionError checks if the error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// Check for common connection errors
	if strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "connection refused") {
		return true
	}

	// Check for net.Error types
	var netErr net.Error
	return errors.As(err, &netErr)
}

// Stream streams a file from an entry to the provided writer within the specified byte range.
// client identifies the caller (e.g., User-Agent for WebDAV, "DFS" for DFS mount).
func (m *Manager) Stream(ctx context.Context, entry *storage.Entry, filename string, start, end int64, writer io.Writer, onReady StreamReadyFunc, client string) (resultErr error) {
	ctx, requestID := m.beginStreamRequest(ctx)
	defer func() {
		resultErr = wrapStreamRequestError(requestID, resultErr)
	}()

	if writer == nil {
		return fmt.Errorf("writer is nil")
	}

	// get file info for size
	file, ok := entry.Files[filename]
	if !ok {
		return retry.Unrecoverable(fmt.Errorf("file %s not found", filename))
	}
	start, end, err := normalizeStreamRange(file.Size, start, end)

	if err != nil {
		return retry.Unrecoverable(fmt.Errorf("invalid stream range for file %s: %w", filename, err))
	}

	// Route based on protocol
	if entry.Protocol == config.ProtocolNZB {
		return m.streamUsenet(ctx, entry, filename, start, end, writer, onReady)
	}

	// Default to HTTP streaming for torrents
	return m.streamHTTP(ctx, entry, filename, start, end, writer, onReady)
}

// TrackStream registers an active stream for observability and returns the stream ID.
// Call UntrackStream with the returned ID when streaming completes.
func (m *Manager) TrackStream(entry *storage.Entry, filename, client string) string {
	if m == nil || m.activeStreams == nil || entry == nil {
		return ""
	}
	file, ok := entry.Files[filename]
	if !ok {
		return ""
	}

	var source, debrid string
	if entry.Protocol == config.ProtocolNZB {
		source = "nzb"
	} else {
		source = "torrent"
		debrid = entry.ActiveProvider
	}

	return m.registerStream(entry.Name, filename, file.Size, source, debrid, client)
}

// UntrackStream removes a previously-registered active stream if the ID is non-empty.
func (m *Manager) UntrackStream(streamID string) {
	m.unregisterStream(streamID)
}

// streamHTTP handles streaming for torrent files via HTTP
func (m *Manager) streamHTTP(ctx context.Context, torrent *storage.Entry, filename string, start, end int64, writer io.Writer, onReady StreamReadyFunc) error {
	file, ok := torrent.Files[filename]
	if !ok {
		return fmt.Errorf("file not found in entry: %s", filename)
	}

	rangePlan, err := buildStreamRangePlan(file, start, end)
	if err != nil {
		return retry.Unrecoverable(fmt.Errorf("invalid stream mapping for %s: %w", filename, err))
	}

	// Link validation and body transfer both inherit playback priority. This
	// lets a seek get ahead of queued bulk downloads without bypassing the
	// provider's shared concurrency budget.
	ctx = cdntraffic.WithPriority(ctx, cdntraffic.PriorityInteractive)

	// Get buffer from pool - reduces GC pressure significantly
	bufPtr := streamBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer streamBufPool.Put(bufPtr)

	candidates := m.streamCandidates(torrent, filename)
	if len(candidates) == 0 {
		return fmt.Errorf("no eligible stream providers for %s", filename)
	}
	candidates = streamAttemptCandidates(candidates, torrent.ActiveProvider)

	failures := make([]error, 0, len(candidates))
	fileCircuitDecisions := make(map[string]bool, len(candidates))
	fileCircuitProbeKeys := make(map[string]string, len(candidates))
	fileCircuitFailures := make(map[string]bool, len(candidates))
	defer func() {
		if m.streamFileCircuits == nil {
			return
		}
		for _, key := range fileCircuitProbeKeys {
			m.streamFileCircuits.releaseProbe(key)
		}
	}()
	recordFileCircuitFailure := func(provider string, failure error) {
		providerKey := strings.ToLower(strings.TrimSpace(provider))
		if fileCircuitFailures[providerKey] {
			return
		}
		if result := m.recordStreamFileCircuitFailure(torrent, filename, provider, failure); result.NewlyOpen {
			fileCircuitFailures[providerKey] = true
		}
	}
	for index, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return retry.Unrecoverable(err)
		}
		provider := candidate.provider
		if provider == "" {
			provider = torrent.ActiveProvider
		}
		providerKey := strings.ToLower(strings.TrimSpace(provider))
		allowed, decided := fileCircuitDecisions[providerKey]
		if !decided {
			circuitKey, probe, circuitAllowed, retryAfter := m.beginStreamFileCircuitAttempt(torrent, filename, provider)
			allowed = circuitAllowed
			fileCircuitDecisions[providerKey] = circuitAllowed
			if probe {
				fileCircuitProbeKeys[providerKey] = circuitKey
			}
			if !circuitAllowed {
				m.streamFileCircuitDefers.Add(1)
				failures = append(failures, fmt.Errorf("provider %s: %w", provider, StreamError{
					Err:       streamFileCircuitOpenError{retryAfter: retryAfter},
					Retryable: true,
				}))
			}
		}
		if !allowed {
			continue
		}
		weatherProbe := false
		if m.streamProviderWeather != nil {
			probe, allowed := m.streamProviderWeather.beginAttempt(candidate.provider)
			if !allowed {
				failures = append(failures, fmt.Errorf("provider %s: %w", candidate.provider, StreamError{
					Err:       errors.New("provider recovery probe already in progress"),
					Retryable: true,
				}))
				continue
			}
			weatherProbe = probe
		}

		candidateCtx := ctx
		fallbackCandidates := candidates[index+1:]
		hasFallback := len(fallbackCandidates) > 0
		isAlternate := !strings.EqualFold(candidate.provider, torrent.ActiveProvider)
		if isAlternate || (hasFallback && !candidate.recovery) {
			candidateCtx = link.WithoutRepair(candidateCtx)
		}
		if hasFallback {
			candidateCtx = link.WithFailFast(candidateCtx)
		}
		ready, err := m.streamHTTPFromCandidate(
			candidateCtx,
			torrent,
			candidate,
			filename,
			rangePlan,
			writer,
			onReady,
			buf,
			fallbackCandidates,
		)
		if weatherProbe {
			m.streamProviderWeather.releaseProbe(candidate.provider)
		}
		if err == nil {
			return nil
		}
		if ready || ctx.Err() != nil || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		providerLabel := provider
		if providerLabel == "" {
			providerLabel = "active"
		}
		recordFileCircuitFailure(provider, err)
		weather := m.recordStreamProviderFailure(providerLabel, streamPreferenceKey(torrent, filename), err)
		if !hasFallback && len(failures) == 0 {
			// Preserve the exact historical error type/semantics when an entry
			// has no alternate placement.
			return err
		}

		failures = append(failures, fmt.Errorf("provider %s: %w", providerLabel, err))
		if !hasFallback {
			break
		}

		m.streamFailoverAttempts.Add(1)
		nextProvider := candidates[index+1].provider
		if nextProvider == "" {
			nextProvider = "active"
		}
		m.logger.Debug().
			Str("event", "stream.provider_failover").
			Str("outcome", "retrying").
			Str("stream_request_id", streamRequestID(ctx)).
			Str("failed_provider", providerLabel).
			Str("next_provider", nextProvider).
			Str("failure_class", weather.Class).
			Int("distinct_files", weather.DistinctFiles).
			Dur("cooldown", weather.Cooldown).
			Msg("Stream provider failed before response commitment; trying next placement")
	}

	if len(failures) == 1 {
		return failures[0]
	}
	if len(failures) > 1 {
		m.streamFailoverExhausted.Add(1)
		return fmt.Errorf("all eligible stream providers failed: %w", errors.Join(failures...))
	}
	return fmt.Errorf("no stream provider produced a response for %s", filename)
}

func (m *Manager) streamHTTPFromCandidate(
	ctx context.Context,
	original *storage.Entry,
	candidate streamCandidate,
	filename string,
	rangePlan streamRangePlan,
	writer io.Writer,
	onReady StreamReadyFunc,
	buf []byte,
	fallbackCandidates []streamCandidate,
) (bool, error) {
	hasFallback := len(fallbackCandidates) > 0
	candidateEntry := candidate.entryForAttempt(original, filename)
	downloadLink, err := m.linkService.GetLink(ctx, candidateEntry, filename)
	if err != nil {
		return false, fmt.Errorf("failed to get download link: %w", err)
	}

	statusRetries := 0
	linkRefreshes := 0
	remainingWait := config.DefaultRetryDelayMax
	for {
		connectionAttempts := m.streamConnectionAttempts()
		if hasFallback {
			connectionAttempts = 1
		}
		resp, reqErr := m.doRequest(
			ctx,
			downloadLink,
			candidate.provider,
			rangePlan.upstreamStart,
			rangePlan.upstreamEnd,
			connectionAttempts,
		)
		if reqErr != nil {
			// Connection retries happen inside doRequest and never receive body
			// bytes, so returning here cannot replay caller-visible data.
			return false, reqErr
		}

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			linkErr := link.ClassifyHTTPStatus(resp.StatusCode, resp.Header)
			resp.Body.Close()

			if linkErr.ShouldRefetch() {
				if linkRefreshes >= maxLinkRefreshesPerStream {
					return false, StreamError{
						Err:       fmt.Errorf("replacement download link was also rejected: %w", linkErr),
						Retryable: false,
						LinkError: true,
					}
				}
				downloadLink, err = m.linkService.Refresh(ctx, candidateEntry, filename, downloadLink)
				if err != nil {
					return false, fmt.Errorf("failed to refresh rejected download link: %w", err)
				}
				linkRefreshes++
				continue
			}

			if (linkErr.ShouldRetry() || linkErr.ShouldBackoff()) && statusRetries < m.streamStatusRetries() {
				if hasFallback {
					return false, StreamError{Err: linkErr, Retryable: true}
				}
				delay := streamRetryDelay(linkErr, statusRetries, remainingWait)
				if delay <= 0 {
					return false, StreamError{Err: linkErr, Retryable: true}
				}
				if waitErr := m.waitForStreamRetry(ctx, delay); waitErr != nil {
					return false, retry.Unrecoverable(waitErr)
				}
				remainingWait -= delay
				statusRetries++
				continue
			}

			return false, StreamError{
				Err:       linkErr,
				Retryable: linkErr.IsRetryable(),
				LinkError: false,
			}
		}

		expectedUpstreamTotal := downloadLink.Size
		if expectedUpstreamTotal <= 0 && !rangePlan.rooted {
			expectedUpstreamTotal = rangePlan.logicalSize
		}
		logicalPartial := rangePlan.logicalStart > 0 || rangePlan.logicalEnd < rangePlan.logicalSize-1
		requirePartial := logicalPartial || rangePlan.rooted
		if expectedUpstreamTotal > 0 {
			requirePartial = logicalPartial ||
				rangePlan.upstreamStart > 0 || rangePlan.upstreamEnd < expectedUpstreamTotal-1
		}
		if integrityErr := validateStreamResponseIntegrity(
			resp,
			rangePlan.upstreamStart,
			rangePlan.upstreamEnd,
			expectedUpstreamTotal,
			rangePlan.expectedLen,
			requirePartial,
		); integrityErr != nil {
			resp.Body.Close()
			return false, StreamError{Err: integrityErr, Retryable: true}
		}

		var header http.Header
		if onReady != nil {
			header = resp.Header.Clone()
		}
		meta := &StreamMetadata{
			Header:        header,
			StatusCode:    resp.StatusCode,
			ContentLength: rangePlan.expectedLen,
		}

		if rangePlan.expectedLen > 0 {
			if header != nil {
				header["Content-Length"] = []string{strconv.FormatInt(rangePlan.expectedLen, 10)}
			}
		}
		if logicalPartial {
			meta.StatusCode = http.StatusPartialContent
			if header != nil {
				header["Content-Range"] = []string{buildContentRange(
					rangePlan.logicalStart,
					rangePlan.logicalEnd,
					rangePlan.logicalSize,
				)}
			}
		} else {
			meta.StatusCode = http.StatusOK
			if header != nil {
				header.Del("Content-Range")
			}
		}

		// Prove that the upstream body can produce at least one byte before the
		// HTTP caller commits status/headers. This is the last point where a
		// provider switch is safe. Once ready is reported or a writer is touched,
		// committed bytes are never replayed; recovery can only request the exact
		// unread suffix.
		reader := io.Reader(resp.Body)
		if rangePlan.expectedLen > 0 {
			reader = io.LimitReader(resp.Body, rangePlan.expectedLen)
		}
		var firstByte [1]byte
		firstN, firstErr := io.ReadFull(reader, firstByte[:])
		if firstN == 0 {
			resp.Body.Close()
			if firstErr == nil || firstErr == io.EOF {
				firstErr = io.ErrUnexpectedEOF
			}
			return false, StreamError{
				Err:       fmt.Errorf("upstream body failed before first byte: %w", firstErr),
				Retryable: true,
			}
		}

		m.markStreamProviderReady(ctx, original, filename, candidate)

		if onReady != nil {
			if readyErr := onReady(meta); readyErr != nil {
				resp.Body.Close()
				return true, retry.Unrecoverable(readyErr)
			}
		}

		transfer := transferStreamBody(
			writer,
			reader,
			firstByte[:firstN],
			rangePlan.expectedLen,
			buf,
		)
		resp.Body.Close()
		if transfer.sinkErr == nil && retryableCommittedSourceError(transfer.sourceErr) {
			transfer = m.resumeHTTPStream(
				ctx,
				candidateEntry,
				candidate,
				filename,
				rangePlan,
				writer,
				buf,
				transfer,
				downloadLink,
				expectedUpstreamTotal,
				linkRefreshes,
				m.streamStatusRetries(),
			)
		}
		if transfer.sinkErr == nil && transfer.sourceErr != nil && transfer.written < rangePlan.expectedLen {
			m.recordStreamFileCircuitFailure(original, filename, candidate.provider, StreamError{
				Err:       transfer.sourceErr,
				Retryable: true,
			})
		}
		handoffAttempted := false
		if transfer.sinkErr == nil && transfer.sourceErr != nil &&
			transfer.written < rangePlan.expectedLen && len(fallbackCandidates) > 0 {
			handoffAttempted = true
			transfer = m.handoffHTTPStream(
				ctx,
				original,
				candidate,
				fallbackCandidates,
				filename,
				rangePlan,
				writer,
				buf,
				transfer,
			)
		}
		copyErr := transfer.err()

		if copyErr != nil && copyErr != io.EOF {
			// Check if this is a retriable error (timeout, network issue)
			// vs a permanent error (context cancelled by user)
			if ctx.Err() != nil {
				// User/system cancelled - don't retry
				return true, retry.Unrecoverable(ctx.Err())
			}
			if isConnectionError(copyErr) || strings.Contains(copyErr.Error(), "timeout") {
				// Network/timeout error - retriable
				return true, copyErr
			}
			// Unknown error - don't retry to avoid infinite loops
			return true, retry.Unrecoverable(copyErr)
		}
		if !handoffAttempted && copyErr == nil && transfer.written == rangePlan.expectedLen {
			m.markStreamFileCircuitSuccess(original, filename, candidate.provider)
		}
		return true, nil
	}
}

func (m *Manager) markStreamProviderReady(ctx context.Context, original *storage.Entry, filename string, candidate streamCandidate) {
	if original == nil {
		return
	}
	if m.streamProviderWeather != nil && m.streamProviderWeather.recordSuccess(candidate.provider) {
		m.streamProviderRecoveries.Add(1)
		m.logger.Info().
			Str("event", "stream.provider_recovered").
			Str("outcome", "healthy").
			Str("provider", candidate.provider).
			Msg("Stream provider recovered")
	}
	m.rememberStreamProvider(original, filename, candidate.provider)
	m.updateActiveStreamProvider(activeStreamID(ctx), candidate.provider)
	if candidate.provider == "" || strings.EqualFold(candidate.provider, original.ActiveProvider) {
		return
	}
	m.streamFailoverSuccesses.Add(1)
	if candidate.preferred {
		m.streamPreferredHits.Add(1)
	}
}

const maxLinkRefreshesPerStream = 1

func (m *Manager) streamStatusRetries() int {
	if m.config == nil || m.config.Retries < 0 {
		return 0
	}
	return m.config.Retries
}

func (m *Manager) streamConnectionAttempts() int {
	return m.streamStatusRetries() + 1
}

func streamRetryDelay(linkErr *link.Error, attempt int, remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	delay := config.DefaultRetryDelay
	if linkErr != nil && linkErr.ShouldBackoff() && linkErr.RetryAfter > 0 {
		delay = linkErr.RetryAfter
	} else {
		for range attempt {
			if delay >= config.DefaultRetryDelayMax/2 {
				delay = config.DefaultRetryDelayMax
				break
			}
			delay *= 2
		}
	}
	if delay > config.DefaultRetryDelayMax {
		delay = config.DefaultRetryDelayMax
	}
	if delay > remaining {
		delay = remaining
	}
	return delay
}

func (m *Manager) waitForStreamRetry(ctx context.Context, delay time.Duration) error {
	if m.streamWait != nil {
		return m.streamWait(ctx, delay)
	}
	return waitForStreamRetry(ctx, delay)
}

func waitForStreamRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// streamUsenet handles streaming for NZB files via usenet
func (m *Manager) streamUsenet(ctx context.Context, entry *storage.Entry, filename string, start, end int64, writer io.Writer, onReady StreamReadyFunc) error {
	if m.usenet == nil {
		return retry.Unrecoverable(fmt.Errorf("usenet client not configured"))
	}

	file, ok := entry.Files[filename]
	if !ok {
		return retry.Unrecoverable(fmt.Errorf("file not found in entry: %s", filename))
	}
	// A previous read may already have proven that an article is permanently
	// missing across every configured provider. Check before onReady writes a
	// 200/206 response so WebDAV clients receive a bounded complete error on
	// subsequent attempts rather than another truncated success response.
	if err := m.usenet.EnsureStreamReady(ctx, entry.InfoHash, filename); err != nil {
		return err
	}

	contentLength := end - start + 1

	// Only build headers if onReady callback is provided (avoids allocations for DFS streaming)
	if onReady != nil {
		statusCode := http.StatusOK
		header := make(http.Header, 4) // Pre-size to avoid rehashing
		header["Accept-Ranges"] = []string{"bytes"}
		header["Content-Length"] = []string{strconv.FormatInt(contentLength, 10)}
		if start > 0 || end < file.Size-1 {
			statusCode = http.StatusPartialContent
			header["Content-Range"] = []string{buildContentRange(start, end, file.Size)}
		}
		header["Content-Type"] = []string{utils.GetContentType(filename)}

		if err := onReady(&StreamMetadata{
			Header:        header,
			StatusCode:    statusCode,
			ContentLength: contentLength,
		}); err != nil {
			return err
		}
	}

	// Stream NZB content directly into writer
	return m.usenet.Stream(ctx, entry.InfoHash, filename, start, end, writer)
}

func normalizeStreamRange(size, start, end int64) (int64, int64, error) {
	if size <= 0 {
		return 0, 0, fmt.Errorf("invalid file size %d", size)
	}

	if start < 0 {
		start = 0
	}
	if end == -1 || end >= size {
		end = size - 1
	}
	if start >= size {
		return 0, 0, fmt.Errorf("requested start %d beyond file size %d", start, size)
	}
	if end < start {
		return 0, 0, fmt.Errorf("invalid byte range %d-%d", start, end)
	}
	return start, end, nil
}

func buildStreamRangePlan(file *storage.File, start, end int64) (streamRangePlan, error) {
	if file == nil {
		return streamRangePlan{}, errors.New("file metadata is missing")
	}
	plan := streamRangePlan{
		logicalStart:  start,
		logicalEnd:    end,
		logicalSize:   file.Size,
		upstreamStart: start,
		upstreamEnd:   end,
		expectedLen:   end - start + 1,
	}
	if file.ByteRange == nil {
		return plan, nil
	}

	rangeStart, rangeEnd := file.ByteRange[0], file.ByteRange[1]
	if rangeStart < 0 || rangeEnd < rangeStart || rangeEnd-rangeStart == math.MaxInt64 {
		return streamRangePlan{}, fmt.Errorf("invalid rooted byte range %d-%d", rangeStart, rangeEnd)
	}
	rangeSize := rangeEnd - rangeStart + 1
	if rangeSize != file.Size {
		return streamRangePlan{}, fmt.Errorf(
			"rooted byte range size %d does not match logical file size %d",
			rangeSize,
			file.Size,
		)
	}

	plan.upstreamStart = rangeStart + start
	plan.upstreamEnd = rangeStart + end
	plan.rooted = true
	return plan, nil
}

// validateStreamResponseIntegrity rejects upstream representations that cannot
// safely satisfy the exact bytes requested by a media client. This runs before
// response headers or body bytes are committed, leaving provider failover safe.
func validateStreamResponseIntegrity(resp *http.Response, start, end, expectedTotal, expectedLen int64, partial bool) error {
	if resp == nil {
		return fmt.Errorf("upstream returned no response")
	}
	if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return fmt.Errorf("upstream returned unexpected content encoding %q", encoding)
	}
	if partial && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("upstream ignored required byte range %d-%d with status %d", start, end, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusPartialContent {
		actualStart, actualEnd, actualTotal, err := parseTorrentContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return fmt.Errorf("invalid upstream Content-Range: %w", err)
		}
		if actualStart != start || actualEnd != end {
			return fmt.Errorf(
				"upstream Content-Range %d-%d does not match requested range %d-%d",
				actualStart,
				actualEnd,
				start,
				end,
			)
		}
		if expectedTotal > 0 && actualTotal != expectedTotal {
			return fmt.Errorf("upstream Content-Range total %d does not match expected size %d", actualTotal, expectedTotal)
		}
		if expectedTotal <= 0 && actualTotal >= 0 && actualTotal <= end {
			return fmt.Errorf("upstream Content-Range total %d does not contain requested end %d", actualTotal, end)
		}
	}
	if expectedLen > 0 && resp.ContentLength >= 0 && resp.ContentLength != expectedLen {
		return fmt.Errorf("upstream Content-Length %d does not match expected response length %d", resp.ContentLength, expectedLen)
	}
	return nil
}

func (m *Manager) doRequest(
	ctx context.Context,
	downloadLink debridTypes.DownloadLink,
	fallbackProvider string,
	start, end int64,
	attempts int,
) (*http.Response, error) {
	var resp *http.Response
	ctx = m.withCDNIdentity(ctx, downloadLink, fallbackProvider)
	attempts = max(1, attempts)

	err := retry.Do(
		func() error {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, downloadLink.DownloadLink, nil)
			if reqErr != nil {
				return retry.Unrecoverable(StreamError{Err: reqErr, Retryable: false})
			}

			// Every stream request carries its normalized range. Besides keeping
			// response validation uniform, this preserves the important bytes=0-0
			// probe that cannot be inferred from start/end being non-zero.
			req.Header.Set("Range", buildHTTPRange(start, end))

			// Set optimized headers for streaming
			req.Header.Set("Connection", "keep-alive")
			req.Header.Set("Accept-Encoding", "identity") // Disable compression for streaming
			req.Header.Set("Cache-Control", "no-cache")

			var doErr error
			resp, doErr = m.streamClient.Do(req)
			if doErr != nil {
				// Check if it's a connection error that we should retry
				if isConnectionError(doErr) {
					return doErr
				}
				return retry.Unrecoverable(StreamError{Err: doErr, Retryable: true})
			}
			return nil
		},
		retry.Context(ctx),
		retry.Attempts(uint(attempts)),
		retry.Delay(config.DefaultRetryDelay),
		retry.MaxDelay(config.DefaultRetryDelayMax),
		retry.DelayType(retry.FixedDelay),
		retry.LastErrorOnly(true),
	)

	if err != nil {
		return nil, StreamError{Err: fmt.Errorf("connection retry exhausted: %w", err), Retryable: true}
	}
	return resp, nil
}

func buildContentRange(start, end, total int64) string {
	var b strings.Builder
	b.Grow(64)
	b.WriteString("bytes ")
	b.WriteString(strconv.FormatInt(start, 10))
	b.WriteByte('-')
	if end >= start {
		b.WriteString(strconv.FormatInt(end, 10))
	} else {
		b.WriteByte('*')
	}
	b.WriteByte('/')
	b.WriteString(strconv.FormatInt(total, 10))
	return b.String()
}

func buildHTTPRange(start, end int64) string {
	var b strings.Builder
	b.Grow(32)
	b.WriteString("bytes=")
	b.WriteString(strconv.FormatInt(start, 10))
	b.WriteByte('-')
	if end >= start {
		b.WriteString(strconv.FormatInt(end, 10))
	}
	return b.String()
}
