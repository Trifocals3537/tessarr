package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestRecordClaimsRestartAndRetry(t *testing.T) {
	now := time.Unix(1000, 0)
	r := NewRecord(now)
	if err := r.Claim("one", "first", now, false); err != nil {
		t.Fatal(err)
	}
	r.EffectStarted = true
	if err := r.Claim("two", "second", now.Add(time.Hour), false); !errors.Is(err, ErrClaim) {
		t.Fatalf("live claim stolen: %v", err)
	}
	if err := r.Claim("two", "second", now.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	if !r.EffectStarted || r.Attempt != 2 {
		t.Fatal("recovery discarded uncertainty or attempt identity")
	}
	if err := r.Release("first", "stale", false, now); !errors.Is(err, ErrClaim) {
		t.Fatal("stale outcome accepted")
	}
	if err := r.Release("second", "temporary", false, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Claim("two", "third", now, false); !errors.Is(err, ErrNotDue) {
		t.Fatal("retry deadline ignored")
	}
	if err := r.Claim("two", "third", r.NextAt, false); err != nil {
		t.Fatal(err)
	}
}
func TestRecordOperatorDecisionsAreFencedAndIdempotent(t *testing.T) {
	now := time.Now()
	r := NewRecord(now)
	r.Claim("owner", "token", now, false)
	if err := r.Resolve(r.Revision, "request", "reconcile", now); !errors.Is(err, ErrClaim) {
		t.Fatal("operator stole running claim")
	}
	r.EffectStarted = true
	r.Release("token", "uncertain", true, now)
	revision := r.Revision
	if err := r.Resolve(revision-1, "request", "retry", now); !errors.Is(err, ErrClaim) {
		t.Fatal("stale UI revision accepted")
	}
	if err := r.Resolve(revision, "request", "reconcile", now); err != nil {
		t.Fatal(err)
	}
	if err := r.Resolve(revision, "request", "reconcile", now); err != nil {
		t.Fatal(err)
	}
	if len(r.Decisions) != 1 || r.Mode != "reconcile" || !r.EffectStarted {
		t.Fatal("duplicate decision or destructive scope change")
	}
	if err := r.Resolve(r.Revision, "other", "force-complete", now); err == nil {
		t.Fatal("force completion accepted")
	}
}
