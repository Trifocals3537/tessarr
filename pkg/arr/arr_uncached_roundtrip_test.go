package arr

import (
	"encoding/json"
	"strings"
	"testing"
)

func marshalArr(t *testing.T, a Arr) string {
	t.Helper()
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal Arr: %v", err)
	}
	return string(data)
}

func TestArrDownloadUncachedNilOmitsJSONKey(t *testing.T) {
	rendered := marshalArr(t, Arr{Name: "sonarr", Host: "http://sonarr:8989", Token: "tok"})
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rendered), &raw); err != nil {
		t.Fatalf("unmarshal rendered arr: %v", err)
	}
	if _, ok := raw["download_uncached"]; ok {
		t.Fatalf("nil DownloadUncached must omit download_uncached, got %s", rendered)
	}
}

func TestArrDownloadUncachedExplicitValuesMarshal(t *testing.T) {
	rendered := marshalArr(t, Arr{Name: "sonarr", DownloadUncached: downloadUncachedPtr(t, true)})
	if !strings.Contains(rendered, `"download_uncached":true`) {
		t.Fatalf("explicit true must render download_uncached:true, got %s", rendered)
	}

	rendered = marshalArr(t, Arr{Name: "sonarr", DownloadUncached: downloadUncachedPtr(t, false)})
	if !strings.Contains(rendered, `"download_uncached":false`) {
		t.Fatalf("explicit false must render download_uncached:false, got %s", rendered)
	}
}

func TestArrDownloadUncachedJSONRoundTripPreservesTriState(t *testing.T) {
	cases := []struct {
		name string
		json string
		want *bool // nil means the pointer must be nil
	}{
		{name: "omitted", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok"}`, want: nil},
		{name: "explicit null", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":null}`, want: nil},
		{name: "explicit true", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":true}`, want: downloadUncachedPtr(t, true)},
		{name: "explicit false", json: `{"name":"sonarr","host":"http://sonarr:8989","token":"tok","download_uncached":false}`, want: downloadUncachedPtr(t, false)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var decoded Arr
			if err := json.Unmarshal([]byte(tc.json), &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if (decoded.DownloadUncached == nil) != (tc.want == nil) {
				t.Fatalf("DownloadUncached pointer = %v, want nil=%v", decoded.DownloadUncached, tc.want == nil)
			}
			if tc.want != nil && *decoded.DownloadUncached != *tc.want {
				t.Fatalf("DownloadUncached = %v, want %v", *decoded.DownloadUncached, *tc.want)
			}

			// Re-marshal and unmarshal again: the state must survive a round trip.
			remarshaled := marshalArr(t, decoded)
			var again Arr
			if err := json.Unmarshal([]byte(remarshaled), &again); err != nil {
				t.Fatalf("re-unmarshal: %v", err)
			}
			if (again.DownloadUncached == nil) != (tc.want == nil) {
				t.Fatalf("round trip lost tri-state: %s -> %v", remarshaled, again.DownloadUncached)
			}
			if tc.want != nil && *again.DownloadUncached != *tc.want {
				t.Fatalf("round trip changed value: want %v got %v", *tc.want, *again.DownloadUncached)
			}
		})
	}
}

func TestArrNewPreservesNilDownloadUncached(t *testing.T) {
	a := New("sonarr", "http://sonarr:8989", "tok", false, nil, "", "")
	if a.DownloadUncached != nil {
		t.Fatalf("New(nil) must keep DownloadUncached nil, got %v", *a.DownloadUncached)
	}
	b := New("sonarr", "http://sonarr:8989", "tok", false, downloadUncachedPtr(t, true), "", "")
	if b.DownloadUncached == nil || !*b.DownloadUncached {
		t.Fatal("New(true) must preserve explicit true")
	}
	c := New("sonarr", "http://sonarr:8989", "tok", false, downloadUncachedPtr(t, false), "", "")
	if c.DownloadUncached == nil || *c.DownloadUncached {
		t.Fatal("New(false) must preserve explicit false")
	}
}

func downloadUncachedPtr(t *testing.T, value bool) *bool {
	t.Helper()
	return &value
}
