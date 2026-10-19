package arr

import (
	"strings"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
)

func TestConstantTimeStringEqual(t *testing.T) {
	tests := []struct {
		name        string
		left, right string
		want        bool
	}{
		{name: "equal", left: "token", right: "token", want: true},
		{name: "different", left: "token", right: "other", want: false},
		{name: "different lengths", left: "token", right: "token-extra", want: false},
		{name: "empty", left: "", right: "", want: true},
		{
			name:  "oversized",
			left:  strings.Repeat("a", maxCredentialCompareBytes+1),
			right: strings.Repeat("a", maxCredentialCompareBytes+1),
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := constantTimeStringEqual(test.left, test.right); got != test.want {
				t.Fatalf("constantTimeStringEqual() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestMatchCredentialsOnlyReturnsConfiguredArrs(t *testing.T) {
	storage := &Storage{arrs: xsync.NewMap[string, *Arr]()}
	sonarr := New("sonarr", "http://sonarr:8989", "sonarr-token", false, nil, "", "manual")
	radarr := New("radarr", "http://radarr:7878", "radarr-token", false, nil, "", "manual")
	storage.arrs.Store(sonarr.Name, sonarr)
	storage.arrs.Store(radarr.Name, radarr)

	if got := storage.MatchCredentials("sonarr", sonarr.Host, sonarr.Token); got != sonarr {
		t.Fatalf("category match = %#v, want sonarr", got)
	}
	if got := storage.MatchCredentials("", radarr.Host, radarr.Token); got != radarr {
		t.Fatalf("credential search = %#v, want radarr", got)
	}
	if got := storage.MatchCredentials("sonarr", "http://attacker.invalid", "secret"); got != nil {
		t.Fatalf("unconfigured host matched %#v", got)
	}
	if got := storage.MatchCredentials("sonarr", sonarr.Host, "wrong"); got != nil {
		t.Fatalf("wrong token matched %#v", got)
	}
}
