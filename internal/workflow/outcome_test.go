package workflow

import (
	"context"
	"errors"
	"testing"
)

type panickingStringer struct{}

func (panickingStringer) String() string { panic("must not format recovered data") }

func TestRunClassifiesExecutionWithoutExposingPanicPayload(t *testing.T) {
	failure := errors.New("operation failed")
	for _, tc := range []struct {
		name string
		work func(context.Context) error
		kind Kind
		err  error
	}{
		{"success", func(context.Context) error { return nil }, Succeeded, nil},
		{"failure", func(context.Context) error { return failure }, Failed, failure},
		{"string panic", func(context.Context) error { panic("secret-signed-url") }, Panicked, ErrPanicked},
		{"non-string panic", func(context.Context) error { panic(panickingStringer{}) }, Panicked, ErrPanicked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := Run(context.Background(), tc.work)
			if out.Kind != tc.kind || !errors.Is(out.Err, tc.err) {
				t.Fatalf("outcome: %+v", out)
			}
		})
	}
}

func TestRunPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	out := Run(ctx, func(context.Context) error { called = true; return nil })
	if called || out.Kind != Canceled || !errors.Is(out.Err, context.Canceled) {
		t.Fatalf("canceled admission: %+v", out)
	}
	ctx, cancel = context.WithCancel(context.Background())
	out = Run(ctx, func(context.Context) error { cancel(); return errors.New("request interrupted") })
	if out.Kind != Canceled || !errors.Is(out.Err, context.Canceled) {
		t.Fatalf("canceled work: %+v", out)
	}
}
