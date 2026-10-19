package manager

import "io"

// streamTransferResult keeps failures from the upstream source separate from
// failures writing to the media client. Provider recovery must never treat a
// disconnected client, canceled seek, or failed FUSE write as provider-health
// evidence.
type streamTransferResult struct {
	written   int64
	sourceErr error
	sinkErr   error
}

// err preserves the historical transfer error precedence: a write failure
// wins over a read failure observed in the same iteration.
func (r streamTransferResult) err() error {
	if r.sinkErr != nil {
		return r.sinkErr
	}
	return r.sourceErr
}

// transferStreamBody copies a prefetched first byte and the remaining source
// into the client writer. It intentionally does not retry or switch sources;
// it only establishes the failure boundary needed by the resumable streaming
// coordinator.
func transferStreamBody(
	writer io.Writer,
	reader io.Reader,
	prefetched []byte,
	expectedLen int64,
	buf []byte,
) streamTransferResult {
	var result streamTransferResult

	writeChunk := func(chunk []byte) bool {
		written, err := writer.Write(chunk)
		result.written += int64(written)
		if err != nil {
			result.sinkErr = err
			return false
		}
		if written != len(chunk) {
			result.sinkErr = io.ErrShortWrite
			return false
		}
		return true
	}

	if len(prefetched) > 0 && !writeChunk(prefetched) {
		return result
	}

	for {
		read, readErr := reader.Read(buf)
		if read > 0 && !writeChunk(buf[:read]) {
			// Match io.CopyBuffer semantics: once the sink rejects bytes, the
			// source cannot be blamed for the failed transfer.
			return result
		}
		if readErr != nil {
			if readErr != io.EOF {
				result.sourceErr = readErr
			}
			break
		}
	}

	if result.sourceErr == nil && expectedLen > 0 && result.written < expectedLen {
		result.sourceErr = io.ErrUnexpectedEOF
	}
	return result
}
