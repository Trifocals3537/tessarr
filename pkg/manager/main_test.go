package manager

import (
	"fmt"
	"os"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
)

func TestMain(m *testing.M) {
	configRoot, err := os.MkdirTemp("", "tessarr-manager-test-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create manager test config root: %v\n", err)
		os.Exit(1)
	}
	config.SetConfigPath(configRoot)
	_ = logger.Default()

	code := m.Run()
	if err := logger.Close(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "close manager test logger: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	_ = os.RemoveAll(configRoot)
	os.Exit(code)
}
