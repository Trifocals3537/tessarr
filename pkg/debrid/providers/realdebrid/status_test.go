package realdebrid

import (
	"errors"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestRealDebridStatusErrorClassifiesOnlyDocumentedTerminalStates(t *testing.T) {
	for _, status := range []string{"magnet_error", "error", "virus", "dead"} {
		t.Run(status, func(t *testing.T) {
			if err := realDebridStatusError("Release", status); !errors.Is(err, types.ErrTerminalProviderTorrent) {
				t.Fatalf("realDebridStatusError(%q) = %v, want terminal provider marker", status, err)
			}
		})
	}

	for _, status := range []string{"queued", "future_provider_state", ""} {
		t.Run("nonterminal_"+status, func(t *testing.T) {
			if err := realDebridStatusError("Release", status); errors.Is(err, types.ErrTerminalProviderTorrent) {
				t.Fatalf("realDebridStatusError(%q) = %v, unknown or active state must remain retryable", status, err)
			}
		})
	}
}
