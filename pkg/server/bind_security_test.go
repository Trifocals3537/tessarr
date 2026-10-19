package server

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestIsLoopbackBindAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bindAddress string
		want        bool
	}{
		{name: "IPv4 loopback", bindAddress: "127.0.0.1", want: true},
		{name: "IPv6 loopback", bindAddress: "::1", want: true},
		{name: "bracketed IPv6 loopback", bindAddress: "[::1]", want: true},
		{name: "zoned IPv6 loopback", bindAddress: "::1%lo", want: true},
		{name: "localhost", bindAddress: "LOCALHOST", want: true},
		{name: "all IPv4 interfaces", bindAddress: "0.0.0.0"},
		{name: "all IPv6 interfaces", bindAddress: "::"},
		{name: "private interface", bindAddress: "10.0.0.4"},
		{name: "empty address", bindAddress: ""},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := isLoopbackBindAddress(test.bindAddress); got != test.want {
				t.Fatalf(
					"isLoopbackBindAddress(%q) = %t, want %t",
					test.bindAddress,
					got,
					test.want,
				)
			}
		})
	}
}

func TestServerStartRejectsUnsafeDeploymentBeforeListen(t *testing.T) {
	previousPath := config.GetMainPath()
	configDir := t.TempDir()
	config.Reset()
	config.SetConfigPath(configDir)
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	if err := os.WriteFile(
		filepath.Join(configDir, "config.json"),
		[]byte(`{
			"bind_address":"192.0.2.10",
			"port":"8282",
			"disable_webdav":true
		}`),
		0600,
	); err != nil {
		t.Fatalf("write config: %v", err)
	}

	err := (&Server{}).Start(context.Background())
	if err == nil {
		t.Fatal("Start() succeeded for an unauthenticated subnet listener")
	}
	if !strings.Contains(err.Error(), "deployment safety check") {
		t.Fatalf("Start() error = %q, want deployment safety context", err)
	}
}

func TestServerBindFailureDoesNotSignalReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	previousPath := config.GetMainPath()
	config.Reset()
	configDir := t.TempDir()
	if err := config.SetConfigPath(configDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		config.Reset()
		_ = config.SetConfigPath(previousPath)
	})
	data, err := json.Marshal(&config.Config{
		BindAddress: "127.0.0.1",
		Port:        strconv.Itoa(listener.Addr().(*net.TCPAddr).Port),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	server := &Server{ready: make(chan struct{})}
	if err := server.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("Start() error = %v, want occupied-listener failure", err)
	}
	select {
	case <-server.Ready():
		t.Fatal("failed HTTP bind signaled readiness")
	default:
	}
}
