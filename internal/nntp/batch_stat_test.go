package nntp

import (
	"errors"
	"testing"
)

func TestShouldStopBatchStatHonorsCompletionMode(t *testing.T) {
	t.Parallel()

	missing := []StatResult{{
		Available: false,
		Error: &Error{
			Type: ErrorTypeArticleNotFound,
			Code: 430,
		},
	}}
	if !shouldStopBatchStat(true, missing) {
		t.Fatal("fail-fast BatchStat did not stop on a definitive missing article")
	}
	if shouldStopBatchStat(false, missing) {
		t.Fatal("complete BatchStatAll stopped on a missing article")
	}

	transient := []StatResult{{
		Available: false,
		Error:     NewTimeoutError(errors.New("deadline")),
	}}
	if shouldStopBatchStat(true, transient) {
		t.Fatal("fail-fast BatchStat stopped on a transient error")
	}
}
