package webdav

import (
	"errors"
	"net/http"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/manager"
)

func normalizeStreamError(err error, headersWritten bool) *customerror.Error {
	var correlated interface{ RequestID() string }
	requestID := ""
	if errors.As(err, &correlated) {
		requestID = correlated.RequestID()
	}
	var existing *customerror.Error
	if errors.As(err, &existing) {
		requestError := existing.Clone()
		requestError.HeadersWritten = headersWritten
		requestError.WithRequestID(requestID)
		return requestError
	}

	status, retryable := manager.StreamErrorHTTPStatus(err)
	code := "server.internal_error"
	if status == http.StatusServiceUnavailable {
		code = "stream.provider_unavailable"
	} else if status == http.StatusRequestedRangeNotSatisfiable {
		code = "stream.invalid_range"
	}
	streamErr := customerror.NewError(err, status, code, customerror.IsSilentError(err), headersWritten)
	streamErr.WithRequestID(requestID)
	if retryable {
		streamErr.Retryable()
	}
	if status == http.StatusRequestedRangeNotSatisfiable {
		streamErr.Permanent()
	}
	return streamErr
}
