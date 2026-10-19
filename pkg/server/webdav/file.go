package webdav

import (
	"fmt"
	"net/http"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (h *Handler) StreamResponse(entry *storage.Entry, info *manager.FileInfo, w http.ResponseWriter, r *http.Request) error {
	start, end, err := getRange(info.Size(), r)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size()))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return customerror.NewError(
			fmt.Errorf("invalid or unsupported byte range: %w", err),
			http.StatusRequestedRangeNotSatisfiable,
			"stream.invalid_range",
			true,
			true,
		).Permanent()
	}

	// Extract client identifier from User-Agent header
	client := r.UserAgent()
	if client == "" {
		client = "Unknown"
	}

	streamID := h.manager.TrackStream(entry, info.Name(), client)
	if streamID != "" {
		defer h.manager.UntrackStream(streamID)
	}
	streamCtx := manager.WithActiveStreamID(r.Context(), streamID)

	headersWritten := false
	err = h.manager.Stream(streamCtx, entry, info.Name(), start, end, w, func(meta *manager.StreamMetadata) error {
		if err := h.handleSuccessfulResponse(w, meta, start, end); err != nil {
			return err
		}
		headersWritten = true
		return nil
	}, client)
	if err != nil {
		return normalizeStreamError(err, headersWritten)
	}
	return nil
}

func (h *Handler) handleSuccessfulResponse(w http.ResponseWriter, meta *manager.StreamMetadata, start, end int64) error {
	statusCode := http.StatusOK
	if meta != nil {
		if meta.Header != nil {
			if contentLength := meta.Header.Get("Content-Length"); contentLength != "" {
				w.Header().Set("Content-Length", contentLength)
			} else if meta.ContentLength > 0 {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.ContentLength))
			}

			if contentRange := meta.Header.Get("Content-Range"); contentRange != "" {
				w.Header().Set("Content-Range", contentRange)
			}

			if contentType := meta.Header.Get("Content-Type"); contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
		}
		if meta.StatusCode != 0 {
			statusCode = meta.StatusCode
		} else if start > 0 || end > 0 {
			statusCode = http.StatusPartialContent
		}
	} else if start > 0 || end > 0 {
		statusCode = http.StatusPartialContent
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(statusCode)
	return nil
}

func getRange(size int64, r *http.Request) (int64, int64, error) {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		// Signal downstream streaming code to serve the entire file
		return 0, -1, nil
	}

	ranges, err := parseRange(rangeHeader, size)
	if err != nil {
		return 0, 0, err
	}
	if len(ranges) == 0 {
		return 0, 0, fmt.Errorf("range is not satisfiable for a %d-byte file", size)
	}
	if len(ranges) > 1 {
		return 0, 0, fmt.Errorf("multiple ranges are not supported")
	}
	return ranges[0].start, ranges[0].end, nil
}
