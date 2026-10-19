package manager

import (
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestQueueAllHashSentinelDoesNotCreateLiteralFilter(t *testing.T) {
	queue := &Queue{}
	for _, hashes := range [][]string{{"all"}, {" ALL "}} {
		if filter := queue.ListFilterFunc("", config.ProtocolAll, "", hashes); filter != nil {
			t.Fatalf("ListFilterFunc(%q) created a literal hash filter", hashes)
		}
	}
}
