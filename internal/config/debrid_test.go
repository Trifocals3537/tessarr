package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDebridMarshalPreservesExplicitDownloadUncachedFalse(t *testing.T) {
	data, err := json.Marshal(Debrid{Name: "realdebrid", DownloadUncached: false})
	if err != nil {
		t.Fatalf("marshal debrid: %v", err)
	}
	if !strings.Contains(string(data), `"download_uncached":false`) {
		t.Fatalf("marshaled debrid omitted explicit false: %s", data)
	}
}
