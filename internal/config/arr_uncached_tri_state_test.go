package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func boolPtr(value bool) *bool { return &value }

func TestConfigArrDownloadUncachedTriStateRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		json string
		want *bool
	}{
		{name: "omitted inherits", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok"}`, want: nil},
		{name: "explicit null inherits", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":null}`, want: nil},
		{name: "explicit true allows", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":true}`, want: boolPtr(true)},
		{name: "explicit false blocks", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":false}`, want: boolPtr(false)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var decoded Arr
			if err := json.Unmarshal([]byte(tc.json), &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if (decoded.DownloadUncached == nil) != (tc.want == nil) {
				t.Fatalf("DownloadUncached = %v, want nil=%v", decoded.DownloadUncached, tc.want == nil)
			}
			if tc.want != nil && *decoded.DownloadUncached != *tc.want {
				t.Fatalf("DownloadUncached = %v, want %v", *decoded.DownloadUncached, *tc.want)
			}

			// omitempty: nil must not render the key; explicit values must.
			remarshaled, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if tc.want == nil && strings.Contains(string(remarshaled), "download_uncached") {
				t.Fatalf("nil must omit download_uncached, got %s", remarshaled)
			}
			if tc.want != nil {
				want := `"download_uncached":` + jsonBool(*tc.want)
				if !strings.Contains(string(remarshaled), want) {
					t.Fatalf("explicit value must render %s, got %s", want, remarshaled)
				}
			}

			var again Arr
			if err := json.Unmarshal(remarshaled, &again); err != nil {
				t.Fatalf("re-unmarshal: %v", err)
			}
			if (again.DownloadUncached == nil) != (tc.want == nil) {
				t.Fatalf("round trip lost tri-state: %s", remarshaled)
			}
			if tc.want != nil && *again.DownloadUncached != *tc.want {
				t.Fatalf("round trip changed value: want %v got %v", *tc.want, *again.DownloadUncached)
			}
		})
	}
}

func TestConfigArrIsZeroDistinguishesExplicitFalseFromNil(t *testing.T) {
	if !(Arr{}).IsZero() {
		t.Fatal("zero Arr must be empty")
	}
	if (Arr{DownloadUncached: boolPtr(false)}).IsZero() {
		t.Fatal("explicit false DownloadUncached must keep the Arr non-empty")
	}
	if (Arr{DownloadUncached: boolPtr(true)}).IsZero() {
		t.Fatal("explicit true DownloadUncached must keep the Arr non-empty")
	}
}

func jsonBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
