package config

import (
	"strings"
	"testing"
)

func TestValidateUsenetRejectsUnsafeConnectionCounts(t *testing.T) {
	validProvider := UsenetProvider{
		Host:           "news.example.test",
		Port:           563,
		Username:       "user",
		Password:       "secret",
		MaxConnections: 20,
	}
	valid := Usenet{
		Providers:                []UsenetProvider{validProvider},
		MaxConnections:           15,
		ProcessingMaxConnections: 15,
	}

	tests := []struct {
		name   string
		mutate func(*Usenet)
	}{
		{name: "negative provider", mutate: func(u *Usenet) { u.Providers[0].MaxConnections = -1 }},
		{name: "excessive provider", mutate: func(u *Usenet) { u.Providers[0].MaxConnections = UsenetConnectionLimit + 1 }},
		{name: "negative stream", mutate: func(u *Usenet) { u.MaxConnections = -1 }},
		{name: "excessive stream", mutate: func(u *Usenet) { u.MaxConnections = UsenetConnectionLimit + 1 }},
		{name: "negative processing", mutate: func(u *Usenet) { u.ProcessingMaxConnections = -1 }},
		{name: "excessive processing", mutate: func(u *Usenet) { u.ProcessingMaxConnections = UsenetConnectionLimit + 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Providers = append([]UsenetProvider(nil), valid.Providers...)
			test.mutate(&candidate)
			if err := validateUsenet(candidate); err == nil || !strings.Contains(err.Error(), "between 1 and") {
				t.Fatalf("validateUsenet() error = %v, want bounded-connection error", err)
			}
		})
	}
}

func TestValidateUsenetAcceptsConnectionLimit(t *testing.T) {
	candidate := Usenet{
		Providers: []UsenetProvider{{
			Host:           "news.example.test",
			Port:           563,
			Username:       "user",
			Password:       "secret",
			MaxConnections: UsenetConnectionLimit,
		}},
		MaxConnections:           UsenetConnectionLimit,
		ProcessingMaxConnections: UsenetConnectionLimit,
	}
	if err := validateUsenet(candidate); err != nil {
		t.Fatalf("validateUsenet() rejected the documented limit: %v", err)
	}
}
