package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func currentConfigWithArrs() *config.Config {
	current := representativeConfig()
	current.Arrs = []config.Arr{
		{Name: "sonarr", Host: "http://sonarr:8989", Token: "tok-a"},
		{Name: "radarr", Host: "http://radarr:7878", Token: "tok-b", DownloadUncached: boolPtrForServerTest(false)},
	}
	return current
}

func boolPtrForServerTest(value bool) *bool { return &value }

func arrDownloadUncachedByName(t *testing.T, cfg *config.Config, name string) (present bool, value bool) {
	t.Helper()
	for _, a := range cfg.Arrs {
		if a.Name != name {
			continue
		}
		if a.DownloadUncached == nil {
			return false, false
		}
		return true, *a.DownloadUncached
	}
	t.Fatalf("arr %q missing from decoded config", name)
	return false, false
}

// TestConfigUpdatePreservesArrDownloadUncachedTriState proves the config API
// round-trips all three Arr policy states: inherit (nil), allow (true), and
// cached-only (false), for both PATCH and PUT.
func TestConfigUpdatePreservesArrDownloadUncachedTriState(t *testing.T) {
	body := `{"arrs":[
		{"name":"sonarr","host":"http://sonarr:8989","token":"tok-a"},
		{"name":"radarr","host":"http://radarr:7878","token":"tok-b","download_uncached":null},
		{"name":"lidarr","host":"http://lidarr:8686","token":"tok-c","download_uncached":true},
		{"name":"readarr","host":"http://readarr:8787","token":"tok-d","download_uncached":false}
	]}`

	for _, method := range []string{http.MethodPatch, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			updated, err := decodeConfigUpdate(method, strings.NewReader(body), currentConfigWithArrs())
			if err != nil {
				t.Fatalf("decode %s: %v", method, err)
			}

			if present, _ := arrDownloadUncachedByName(t, updated, "sonarr"); present {
				t.Fatal("omitted download_uncached must decode as nil (inherit)")
			}
			if present, _ := arrDownloadUncachedByName(t, updated, "radarr"); present {
				t.Fatal("explicit null download_uncached must decode as nil (inherit)")
			}
			if present, value := arrDownloadUncachedByName(t, updated, "lidarr"); !present || !value {
				t.Fatalf("explicit true must survive %s, got present=%v value=%v", method, present, value)
			}
			if present, value := arrDownloadUncachedByName(t, updated, "readarr"); !present || value {
				t.Fatalf("explicit false must survive %s, got present=%v value=%v", method, present, value)
			}
		})
	}
}

// TestConfigPatchInheritClearsExplicitOverride proves an operator can move an
// Arr from an explicit override back to inherit by sending null.
func TestConfigPatchInheritClearsExplicitOverride(t *testing.T) {
	patch := `{"arrs":[{"name":"radarr","host":"http://radarr:7878","token":"tok-b","download_uncached":null}]}`
	updated, err := decodeConfigUpdate(http.MethodPatch, strings.NewReader(patch), currentConfigWithArrs())
	if err != nil {
		t.Fatalf("decode PATCH: %v", err)
	}
	if present, _ := arrDownloadUncachedByName(t, updated, "radarr"); present {
		t.Fatal("PATCH null must clear the explicit radarr override to inherit")
	}
}

// TestConfigMarshalOmitsNilArrDownloadUncached proves API responses omit the
// key for inheriting Arrs so the UI can distinguish inherit from cached-only.
func TestConfigMarshalOmitsNilArrDownloadUncached(t *testing.T) {
	current := currentConfigWithArrs()
	rendered, err := json.Marshal(current)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if !strings.Contains(string(rendered), `"name":"sonarr"`) {
		t.Fatalf("sonarr arr missing from %s", rendered)
	}

	decodeArrObjects := func(data []byte) []map[string]json.RawMessage {
		t.Helper()
		var topLevel map[string]json.RawMessage
		if err := json.Unmarshal(data, &topLevel); err != nil {
			t.Fatalf("decode config: %v", err)
		}
		var arrObjects []map[string]json.RawMessage
		if err := json.Unmarshal(topLevel["arrs"], &arrObjects); err != nil {
			t.Fatalf("decode arrs: %v", err)
		}
		return arrObjects
	}

	// The explicit radarr override must render inside its own arr object.
	radarrRendered := false
	for _, arrObject := range decodeArrObjects(rendered) {
		if string(arrObject["name"]) == `"radarr"` {
			value, ok := arrObject["download_uncached"]
			if !ok || string(value) != "false" {
				t.Fatalf("radarr override must render download_uncached:false, object = %v", arrObject)
			}
			radarrRendered = true
		}
		if string(arrObject["name"]) == `"sonarr"` {
			if _, ok := arrObject["download_uncached"]; ok {
				t.Fatalf("sonarr inherit state must omit download_uncached, object = %v", arrObject)
			}
		}
	}
	if !radarrRendered {
		t.Fatal("radarr arr missing from rendered config")
	}

	// Clearing the override must remove the key from every arr object (the
	// debrid provider's own download_uncached is a different field and keeps
	// rendering).
	current.Arrs[1].DownloadUncached = nil
	renderedUpdated, err := json.Marshal(current)
	if err != nil {
		t.Fatalf("marshal updated config: %v", err)
	}
	for _, arrObject := range decodeArrObjects(renderedUpdated) {
		if _, ok := arrObject["download_uncached"]; ok {
			t.Fatalf("nil override must omit download_uncached entirely, arr object = %v", arrObject)
		}
	}
}
