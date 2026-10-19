package manager

import (
	"net/http"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
)

// StreamErrorHTTPStatus maps a stream failure that occurred before response
// commitment to client-safe HTTP semantics. Provider/link weather is retryable
// and must not look like a client 4xx; a genuine range refusal remains 416.
func StreamErrorHTTPStatus(err error) (status int, retryable bool) {
	if err == nil {
		return 0, false
	}

	sawStreamError := false
	allRangeErrors := true
	sawRetryable := false
	walkStreamErrors(err, func(streamErr StreamError) {
		sawStreamError = true
		linkErr := link.GetLinkError(streamErr.Err)
		isRangeError := linkErr != nil && linkErr.Code == "416"
		allRangeErrors = allRangeErrors && isRangeError
		if streamErr.Retryable || streamErr.LinkError || (linkErr != nil && linkErr.IsRetryable()) {
			sawRetryable = true
		}
	})
	if sawRetryable {
		return http.StatusServiceUnavailable, true
	}
	if sawStreamError && allRangeErrors {
		return http.StatusRequestedRangeNotSatisfiable, false
	}

	// Link resolution can fail before the HTTP stream layer creates a
	// StreamError. Preserve the same semantics for that path.
	if linkErr := link.GetLinkError(err); linkErr != nil {
		if linkErr.Code == "416" {
			return http.StatusRequestedRangeNotSatisfiable, false
		}
		if linkErr.IsRetryable() {
			return http.StatusServiceUnavailable, true
		}
	}
	if isConnectionError(err) {
		return http.StatusServiceUnavailable, true
	}
	return http.StatusInternalServerError, false
}

func walkStreamErrors(err error, visit func(StreamError)) {
	if err == nil {
		return
	}
	if streamErr, ok := directStreamError(err); ok {
		visit(streamErr)
		return
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			walkStreamErrors(child, visit)
		}
		return
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		walkStreamErrors(wrapped.Unwrap(), visit)
	}
}

func directStreamError(err error) (StreamError, bool) {
	switch streamErr := err.(type) {
	case StreamError:
		return streamErr, true
	case *StreamError:
		if streamErr != nil {
			return *streamErr, true
		}
	}
	return StreamError{}, false
}
