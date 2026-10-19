package utils

import (
	"os"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "tessarr-utils-test-")
	if err != nil {
		panic(err)
	}

	config.SetConfigPath(configDir)
	code := m.Run()
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}
