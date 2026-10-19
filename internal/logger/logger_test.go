package logger

import (
	"bytes"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

func TestSetLevelUpdatesExistingLogger(t *testing.T) {
	previousLevel := zerolog.GlobalLevel()
	t.Cleanup(func() { zerolog.SetGlobalLevel(previousLevel) })

	var output bytes.Buffer
	log := zerolog.New(&output).Level(zerolog.TraceLevel)

	if err := SetLevel("info"); err != nil {
		t.Fatal(err)
	}
	log.Debug().Msg("hidden-debug")
	log.Info().Msg("visible-info")
	if got := output.String(); strings.Contains(got, "hidden-debug") || !strings.Contains(got, "visible-info") {
		t.Fatalf("initial output = %q", got)
	}

	output.Reset()
	if err := SetLevel("debug"); err != nil {
		t.Fatal(err)
	}
	log.Debug().Msg("visible-debug")
	if got := output.String(); !strings.Contains(got, "visible-debug") {
		t.Fatalf("updated output = %q", got)
	}

	output.Reset()
	if err := SetLevel("error"); err != nil {
		t.Fatal(err)
	}
	log.Warn().Msg("hidden-warning")
	log.Error().Msg("visible-error")
	if got := output.String(); strings.Contains(got, "hidden-warning") || !strings.Contains(got, "visible-error") {
		t.Fatalf("raised-threshold output = %q", got)
	}
}

func TestConsoleWriterPreservesReadableComponentFormatWithoutANSI(t *testing.T) {
	var output bytes.Buffer
	log := zerolog.New(newConsoleWriter(&output, false)).
		With().
		Str(subsystemField, "manager").
		Logger()

	log.Info().Str("file", "movie.mkv").Msg("Playback ready")
	got := output.String()
	if !strings.Contains(got, "[manager] Playback ready") {
		t.Fatalf("component prefix missing from %q", got)
	}
	if !strings.Contains(got, "file=movie.mkv") {
		t.Fatalf("structured field missing from %q", got)
	}
	if strings.Contains(got, subsystemField+"=") {
		t.Fatalf("component field duplicated in %q", got)
	}
	if strings.Contains(got, "\x1b[") {
		t.Fatalf("non-color output contains ANSI escapes: %q", got)
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input   string
		want    zerolog.Level
		wantErr bool
	}{
		{input: "", want: zerolog.InfoLevel},
		{input: "DEBUG", want: zerolog.DebugLevel},
		{input: "warning", want: zerolog.WarnLevel},
		{input: "trace", want: zerolog.TraceLevel},
		{input: "verbose", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := parseLevel(test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseLevel() error = %v, wantErr %t", err, test.wantErr)
			}
			if !test.wantErr && got != test.want {
				t.Fatalf("parseLevel() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGetLogPathUsesPrivateDirectoryPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ACLs are not represented by Unix permission bits")
	}
	previousPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(previousPath) })

	path := GetLogPath()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0700 {
		t.Fatalf("log directory permissions = %o, want 700", got)
	}
}
