// Package workflow defines execution outcomes independently of a storage engine
// or provider. Persistence and retry policy belong to the application boundary.
package workflow

import (
	"context"
	"errors"
	"runtime/debug"
)

type Kind string

const (
	Succeeded Kind = "succeeded"
	Failed    Kind = "failed"
	Canceled  Kind = "canceled"
	Panicked  Kind = "panicked"
)

// ErrPanicked deliberately omits the recovered payload, which may contain
// provider credentials, signed URLs, or a value whose String method panics.
var ErrPanicked = errors.New("workflow worker panicked; inspect service diagnostics")

type Outcome struct {
	Kind  Kind
	Err   error
	Stack []byte
}

// Run contains a panic before the caller releases its ownership/fencing lease.
// Succeeded means the invocation returned successfully, not that a media import
// is complete; completion still requires its own durable checkpoint.
func Run(ctx context.Context, work func(context.Context) error) (out Outcome) {
	defer func() {
		if recover() != nil {
			out = Outcome{Kind: Panicked, Err: ErrPanicked, Stack: debug.Stack()}
		}
	}()
	if err := ctx.Err(); err != nil {
		return Outcome{Kind: Canceled, Err: err}
	}
	err := work(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return Outcome{Kind: Canceled, Err: ctxErr}
	}
	if err != nil {
		return Outcome{Kind: Failed, Err: err}
	}
	return Outcome{Kind: Succeeded}
}
