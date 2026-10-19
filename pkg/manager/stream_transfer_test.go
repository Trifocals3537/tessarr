package manager

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

var errTestSourceReset = errors.New("test source reset")
var errTestSinkClosed = errors.New("test sink closed")

type terminalErrorReader struct {
	data []byte
	err  error
	done bool
}

func (r *terminalErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(p, r.data), r.err
}

type failingStreamWriter struct {
	written int
	limit   int
	err     error
}

func (w *failingStreamWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return 0, w.err
	}
	if remaining < len(p) {
		w.written += remaining
		return remaining, w.err
	}
	w.written += len(p)
	return len(p), nil
}

type shortStreamWriter struct{}

func (shortStreamWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func TestTransferStreamBodyClassifiesFailureBoundary(t *testing.T) {
	tests := []struct {
		name       string
		writer     io.Writer
		reader     io.Reader
		prefetched string
		expected   int64
		wantData   string
		wantBytes  int64
		wantSource error
		wantSink   error
	}{
		{
			name:       "complete",
			writer:     &strings.Builder{},
			reader:     strings.NewReader("bcd"),
			prefetched: "a",
			expected:   4,
			wantData:   "abcd",
			wantBytes:  4,
		},
		{
			name:       "clean eof before expected length is source truncation",
			writer:     &strings.Builder{},
			reader:     strings.NewReader("b"),
			prefetched: "a",
			expected:   4,
			wantData:   "ab",
			wantBytes:  2,
			wantSource: io.ErrUnexpectedEOF,
		},
		{
			name:       "explicit mid-stream source failure",
			writer:     &strings.Builder{},
			reader:     &terminalErrorReader{data: []byte("bc"), err: errTestSourceReset},
			prefetched: "a",
			expected:   4,
			wantData:   "abc",
			wantBytes:  3,
			wantSource: errTestSourceReset,
		},
		{
			name:       "sink failure is not a source failure",
			writer:     &failingStreamWriter{limit: 2, err: errTestSinkClosed},
			reader:     strings.NewReader("bcd"),
			prefetched: "a",
			expected:   4,
			wantBytes:  2,
			wantSink:   errTestSinkClosed,
		},
		{
			name:       "short sink write",
			writer:     shortStreamWriter{},
			reader:     strings.NewReader("bcd"),
			prefetched: "a",
			expected:   4,
			wantBytes:  0,
			wantSink:   io.ErrShortWrite,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := transferStreamBody(
				test.writer,
				test.reader,
				[]byte(test.prefetched),
				test.expected,
				make([]byte, 2),
			)
			if result.written != test.wantBytes {
				t.Fatalf("written = %d, want %d", result.written, test.wantBytes)
			}
			if !errors.Is(result.sourceErr, test.wantSource) ||
				(test.wantSource == nil && result.sourceErr != nil) {
				t.Fatalf("source error = %v, want %v", result.sourceErr, test.wantSource)
			}
			if !errors.Is(result.sinkErr, test.wantSink) ||
				(test.wantSink == nil && result.sinkErr != nil) {
				t.Fatalf("sink error = %v, want %v", result.sinkErr, test.wantSink)
			}
			if builder, ok := test.writer.(*strings.Builder); ok && builder.String() != test.wantData {
				t.Fatalf("output = %q, want %q", builder.String(), test.wantData)
			}
		})
	}
}

func TestTransferStreamBodySinkFailureWinsSameReadIteration(t *testing.T) {
	writer := &failingStreamWriter{limit: 1, err: errTestSinkClosed}
	reader := &terminalErrorReader{data: []byte("bc"), err: errTestSourceReset}
	result := transferStreamBody(writer, reader, []byte("a"), 3, make([]byte, 4))

	if !errors.Is(result.sinkErr, errTestSinkClosed) {
		t.Fatalf("sink error = %v, want %v", result.sinkErr, errTestSinkClosed)
	}
	if result.sourceErr != nil {
		t.Fatalf("source error = %v, want nil when sink rejects the same read", result.sourceErr)
	}
	if !errors.Is(result.err(), errTestSinkClosed) {
		t.Fatalf("transfer error = %v, want sink error", result.err())
	}
}

func TestRetryableCommittedSourceError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "connection reset", err: errors.New("connection reset by peer"), want: true},
		{name: "idle timeout", err: errors.New("upstream read timeout"), want: true},
		{name: "classified retryable", err: StreamError{Err: errors.New("temporary"), Retryable: true}, want: true},
		{name: "classified permanent", err: StreamError{Err: errors.New("permanent")}, want: false},
		{name: "client cancellation", err: context.Canceled, want: false},
		{name: "request deadline", err: context.DeadlineExceeded, want: false},
		{name: "unknown source error", err: errors.New("malformed source"), want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := retryableCommittedSourceError(test.err); got != test.want {
				t.Fatalf("retryableCommittedSourceError(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}
