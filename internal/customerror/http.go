package customerror

import (
	"errors"
	"fmt"
	"net/http"
)

var HosterUnavailableError = (&Error{
	statusCode: 503,
	err:        errors.New("hoster is unavailable"),
	Code:       "hoster_unavailable",
}).Retryable() // 503 Service Unavailable is transient

var UsenetSegmentMissingError = &Error{
	statusCode: 404,
	err:        errors.New("usenet segment is missing"),
	Code:       "usenet_segment_missing",
}

var UsenetCorruptContentError = &Error{
	statusCode: 422,
	err:        errors.New("usenet file head does not match a recognized media container"),
	Code:       "usenet_corrupt_content",
}

var TrafficExceededError = &Error{
	statusCode: 503,
	err:        errors.New("traffic limit exceeded"),
	Code:       "traffic_exceeded",
}

var TorrentNotFoundError = &Error{
	statusCode: 404,
	err:        errors.New("torrent not found"),
	Code:       "torrent_not_found",
}

var TooManyActiveDownloadsError = (&Error{
	statusCode: 509,
	err:        errors.New("too many active downloads"),
	Code:       "too_many_active_downloads",
}).Retryable() // slot exhaustion is transient — retry after backoff

// NewTorrentNotCachedError returns a fresh error because retry and permanence
// are mutable flags on Error. A cache miss is transient: another provider may
// already have the torrent, or this provider may cache it later.
func NewTorrentNotCachedError(name string) *Error {
	return NewError(
		fmt.Errorf("torrent %q is not cached", name),
		http.StatusConflict,
		"torrent_not_cached",
		false,
		false,
	).Retryable()
}

// NewTorrentContentRejectedError marks a provider content-policy rejection.
// It is permanent for this submission, unlike a cache miss or an operational
// provider failure, and can therefore be cooled down safely by admission.
func NewTorrentContentRejectedError(name string) *Error {
	return NewError(
		fmt.Errorf("torrent %q was rejected by provider content policy", name),
		http.StatusUnprocessableEntity,
		"torrent_content_rejected",
		false,
		false,
	).Permanent()
}
