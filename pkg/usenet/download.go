package usenet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/sourcegraph/conc/pool"
)

// segmentResult holds a fetched segment and its index for ordered writing
type segmentResult struct {
	index int
	data  []byte
	err   error
}

// ProgressCallback is called periodically during download with progress info
// downloaded: total bytes written so far, speed: bytes per second (estimated)
type ProgressCallback func(downloaded int64, speed int64)

// Download downloads a file by fetching segments in parallel and streaming to writer in order.
// Bytes flow to the writer progressively as in-order segments complete - no waiting for all segments.
// If progressCallback is provided, it will be called after each segment write with current progress.
func (u *Usenet) Download(ctx context.Context, nzoID, filename string, writer io.Writer, progressCallback ProgressCallback) error {
	// get file metadata
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no segments: %s", file.Name)
	}
	downloadCtx, cancelDownload := context.WithCancel(ctx)
	defer cancelDownload()

	// Track progress
	var completedSegments atomic.Int64
	var downloadedBytes atomic.Int64

	// Channel for segment results - buffered to allow parallel fetching ahead
	maxConnections := max(u.processingMaxConnections, 1)
	reorderWindow := maxConnections * 2
	resultChan := make(chan segmentResult, reorderWindow)
	// A token remains held while a fetched payload is waiting for its turn to
	// be written. This bounds out-of-order memory if an early segment is slow.
	reorderSlots := make(chan struct{}, reorderWindow)

	// Map to hold out-of-order segments waiting to be written
	pendingSegments := make(map[int][]byte)
	nextToWrite := 0

	// Error tracking
	var writeErr error
	var writeErrMu sync.Mutex

	// Writer goroutine - writes segments in order as they arrive
	var writerWg sync.WaitGroup
	writerWg.Go(func() {
		for result := range resultChan {
			if result.err != nil {
				<-reorderSlots
				writeErrMu.Lock()
				if writeErr == nil {
					writeErr = result.err
					cancelDownload()
					for range pendingSegments {
						<-reorderSlots
					}
					clear(pendingSegments)
				}
				writeErrMu.Unlock()
				continue
			}

			writeErrMu.Lock()
			stopped := writeErr != nil
			writeErrMu.Unlock()
			if stopped {
				<-reorderSlots
				continue
			}

			pendingSegments[result.index] = result.data

			// Write all consecutive segments starting from nextToWrite
			for {
				data, exists := pendingSegments[nextToWrite]
				if !exists {
					break
				}
				delete(pendingSegments, nextToWrite)

				// io.Copy turns a short write with no explicit writer error into
				// io.ErrShortWrite, preventing a silently truncated output file.
				n, err := io.Copy(writer, bytes.NewReader(data))
				<-reorderSlots
				if err != nil {
					writeErrMu.Lock()
					if writeErr == nil {
						writeErr = fmt.Errorf("write failed at segment %d: %w", nextToWrite, err)
						cancelDownload()
					}
					writeErrMu.Unlock()
					for range pendingSegments {
						<-reorderSlots
					}
					clear(pendingSegments)
					break
				}

				completedSegments.Add(1)
				downloaded := downloadedBytes.Add(n)
				nextToWrite++

				// Call progress callback if provided
				if progressCallback != nil {
					// Estimate speed (rough: assume ~1s per segment batch)
					completed := completedSegments.Load()
					speed := downloaded / max(1, completed) * int64(max(u.processingMaxConnections, 1))
					progressCallback(downloaded, speed)
				}

			}
		}
	})

	// Fetch segments in parallel
	p := pool.New().WithContext(downloadCtx).WithMaxGoroutines(maxConnections)

	for idx, segment := range file.Segments {
		segIdx := idx
		seg := segment

		p.Go(func(ctx context.Context) error {
			select {
			case reorderSlots <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			resultSent := false
			defer func() {
				if !resultSent {
					<-reorderSlots
				}
			}()
			sendResult := func(result segmentResult) {
				resultChan <- result
				resultSent = true
			}

			// Check for write errors
			writeErrMu.Lock()
			if writeErr != nil {
				writeErrMu.Unlock()
				return writeErr
			}
			writeErrMu.Unlock()

			// Check context
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Fetch segment using manager with failover
			var data []byte
			err := u.nntp.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
				d, e := conn.GetDecodedBody(seg.MessageID)
				data = d
				return e
			})
			if err != nil {
				sendResult(segmentResult{index: segIdx, err: fmt.Errorf("segment %d: %w", segIdx, err)})
				return nil // Don't stop other workers
			}

			data, err = prepareSegmentPayload(data, seg.SegmentDataStart, seg.Bytes)
			if err != nil {
				sendResult(segmentResult{index: segIdx, err: fmt.Errorf("segment %d: %w", segIdx, err)})
				return nil
			}

			sendResult(segmentResult{index: segIdx, data: data})
			return nil
		})
	}

	// Wait for all fetches to complete, then close result channel
	fetchErr := p.Wait()
	close(resultChan)

	// Wait for writer to finish
	writerWg.Wait()

	// Check for errors
	if writeErr != nil {
		return writeErr
	}
	if fetchErr != nil {
		return fetchErr
	}

	u.logger.Info().
		Str("file", filename).
		Int64("bytes", downloadedBytes.Load()).
		Msg("Download complete")

	return nil
}

func prepareSegmentPayload(data []byte, dataStart, expectedBytes int64) ([]byte, error) {
	if dataStart < 0 {
		return nil, fmt.Errorf("negative data offset %d", dataStart)
	}
	if expectedBytes <= 0 {
		return nil, fmt.Errorf("invalid expected size %d", expectedBytes)
	}
	if dataStart > int64(len(data)) {
		return nil, fmt.Errorf("data offset %d exceeds decoded size %d", dataStart, len(data))
	}
	available := int64(len(data)) - dataStart
	if available < expectedBytes {
		return nil, fmt.Errorf("incomplete decoded data: got %d of %d bytes after offset %d", available, expectedBytes, dataStart)
	}
	return data[dataStart : dataStart+expectedBytes], nil
}
