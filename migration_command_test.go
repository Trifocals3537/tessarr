package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunLegacyMigrationDryRun(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	source := filepath.Join(root, "old")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "config.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runLegacyMigration([]string{
		"--source", source,
		"--target", filepath.Join(root, "new"),
		"--dry-run",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runLegacyMigration() error = %v, stderr = %q", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "no files were written") {
		t.Fatalf("runLegacyMigration() output = %q", stdout.String())
	}
}
