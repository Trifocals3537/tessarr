package nntp

import (
	"os"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

func TestMain(m *testing.M) {
	// Match application startup: NNTP deadlines use the shared cached clock.
	// Without its updater, retries can leave later tests with expired deadlines.
	utils.StartGlobalCachedTime()
	code := m.Run()
	utils.StopGlobalCachedTime()
	os.Exit(code)
}
