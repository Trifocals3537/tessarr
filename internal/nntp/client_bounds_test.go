package nntp

import (
	"reflect"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestNewClientDefensivelyRejectsUnsafeProviderCapacity(t *testing.T) {
	for _, capacity := range []int{-1, config.UsenetConnectionLimit + 1} {
		cfg := &config.Config{Usenet: config.Usenet{Providers: []config.UsenetProvider{{
			Host:           "news.example.test",
			MaxConnections: capacity,
		}}}}
		if client, err := NewClient(cfg); err == nil || client != nil {
			t.Fatalf("NewClient(max_connections=%d) = (%v, %v), want rejection", capacity, client, err)
		}
	}
}

func TestNewClientDoesNotMutateConfigProviders(t *testing.T) {
	cfg := &config.Config{Usenet: config.Usenet{Providers: []config.UsenetProvider{
		{Host: "secondary.example.test", Priority: 20, Backbone: "  Secondary  ", MaxConnections: 1},
		{Host: "primary.example.test", Priority: 10, Backbone: "  Primary  ", MaxConnections: 1},
	}}}
	original := append([]config.UsenetProvider(nil), cfg.Usenet.Providers...)
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if !reflect.DeepEqual(cfg.Usenet.Providers, original) {
		t.Fatalf("NewClient mutated config providers:\n got: %#v\nwant: %#v", cfg.Usenet.Providers, original)
	}
}
