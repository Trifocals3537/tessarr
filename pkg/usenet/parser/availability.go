package parser

import (
	"errors"
	"fmt"

	"github.com/Trifocals3537/tessarr/internal/nntp"
)

// ErrNZBArticlesUnavailable identifies an NZB whose required probe articles
// are definitively absent from every configured provider. Callers may treat
// this as a release-specific rejection; connection, authentication, timeout,
// and parser failures deliberately remain distinguishable.
var ErrNZBArticlesUnavailable = errors.New("NZB articles unavailable")

var errFileTypeUndetected = errors.New("file type could not be detected")

type articleProbeFailures struct {
	count              int
	allArticlesMissing bool
	first              error
}

func (f *articleProbeFailures) add(err error) {
	if err == nil {
		return
	}
	if f.count == 0 {
		f.allArticlesMissing = true
		f.first = err
	}
	f.count++
	if !nntp.IsArticleNotFoundError(err) {
		f.allArticlesMissing = false
	}
}

func (f *articleProbeFailures) merge(other articleProbeFailures) {
	if other.count == 0 {
		return
	}
	if f.count == 0 {
		*f = other
		return
	}
	f.count += other.count
	f.allArticlesMissing = f.allArticlesMissing && other.allArticlesMissing
}

func (f articleProbeFailures) unavailableError() error {
	if f.count == 0 || !f.allArticlesMissing {
		return nil
	}
	return &articlesUnavailableError{count: f.count, cause: f.first}
}

type articlesUnavailableError struct {
	count int
	cause error
}

func (e *articlesUnavailableError) Error() string {
	return fmt.Sprintf(
		"%v: %d required article probe(s) failed; first failure: %v",
		ErrNZBArticlesUnavailable,
		e.count,
		e.cause,
	)
}

func (e *articlesUnavailableError) Is(target error) bool {
	return target == ErrNZBArticlesUnavailable
}

func (e *articlesUnavailableError) Unwrap() error {
	return e.cause
}
