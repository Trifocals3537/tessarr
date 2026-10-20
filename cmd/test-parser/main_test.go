package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestDiagnosticLoggerEscapesNamesAndErrors(t *testing.T) {
	var output bytes.Buffer
	logger := diagnosticLogger(&output)
	value := "release\r\nFORGED\x1b[2J"
	logger.Error().Str("name", value).Err(errors.New(value)).Msg("parse failed")
	text := output.String()
	if strings.Count(text, "\n") != 1 || strings.ContainsAny(text, "\r\x1b") {
		t.Fatalf("log must be a single JSON line with escaped controls: %q", text)
	}
	var event map[string]any
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["name"] != value || event["error"] != value {
		t.Fatalf("escaped fields must retain their original values: %#v", event)
	}
}

func TestSummaryEscapesUntrustedText(t *testing.T) {
	fields := []string{
		"id\nFORGED ID",
		"name\rFORGED NAME",
		"file\x1b[2J\nFORGED FILE.mkv",
		"internal\x00\u202e/entry",
		"group\t\nFORGED GROUP",
	}
	nzb := &storage.NZB{
		ID: fields[0], Name: fields[1], TotalSize: 1024,
		Files: []storage.NZBFile{{
			Name: fields[2], InternalPath: fields[3], Groups: []string{fields[4]},
			Password: "never-print-this-secret", Size: 1024,
		}},
	}
	var output bytes.Buffer
	printSummary(&output, nzb)
	text := output.String()
	for _, field := range fields {
		if strings.Contains(text, field) {
			t.Errorf("unescaped input appears in summary: %q", field)
		}
	}
	for _, control := range []string{"\r", "\x1b", "\x00", "\t", "\u202e"} {
		if strings.Contains(text, control) {
			t.Errorf("raw control character in summary: %q", control)
		}
	}
	for _, escaped := range []string{`id\nFORGED ID`, `name\rFORGED NAME`, `file\x1b[2J\nFORGED FILE.mkv`, `internal\x00\u202e/entry`, `group\t\nFORGED GROUP`} {
		if !strings.Contains(text, escaped) {
			t.Errorf("missing escaped display value %q", escaped)
		}
	}
	if strings.Contains(text, "never-print-this-secret") || !strings.Contains(text, "Protected (***)") {
		t.Fatal("password must remain redacted")
	}
}

func TestSummaryPreservesReadableNamesAndInput(t *testing.T) {
	nzb := &storage.NZB{ID: "id", Name: "Release 日本語", Files: []storage.NZBFile{{Name: "movie.mkv", IsStored: true, Groups: []string{"alt.binaries.test"}}}}
	var output bytes.Buffer
	printSummary(&output, nzb)
	for _, expected := range []string{`Name:       "Release 日本語"`, `[1] "movie.mkv"`, `Groups:       ["alt.binaries.test"]`, "None", "Stored (seekable)"} {
		if !strings.Contains(output.String(), expected) {
			t.Errorf("missing readable output %q", expected)
		}
	}
	if nzb.Name != "Release 日本語" || nzb.Files[0].Name != "movie.mkv" {
		t.Fatal("display escaping must not change parsed identifiers")
	}
}
