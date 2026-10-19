package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/rs/zerolog"
	"golang.org/x/term"
	"gopkg.in/natefinch/lumberjack.v2"
)

const subsystemField = "subsystem"

var (
	coreOnce sync.Once
	core     *loggingCore

	defaultOnce sync.Once
	defaultLog  zerolog.Logger

	levelMu         sync.Mutex
	levelConfigured bool
)

type loggingCore struct {
	writer   zerolog.LevelWriter
	rotating *lumberjack.Logger
}

func GetLogPath() string {
	logsDir := filepath.Join(config.GetMainPath(), "logs")
	rooted, logsDir, err := safepath.EnsureOpenRoot(logsDir, 0o700)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logs directory: %v", err))
	}
	defer rooted.Close()
	if err := rooted.Chmod(".", 0o700); err != nil {
		panic(fmt.Sprintf("Failed to secure logs directory: %v", err))
	}
	return logsDir
}

func getCore() *loggingCore {
	coreOnce.Do(func() {
		rotating := &lumberjack.Logger{
			Filename:   filepath.Join(GetLogPath(), "tessarr.log"),
			MaxSize:    10,
			MaxAge:     15,
			MaxBackups: 10,
			Compress:   true,
		}

		color := term.IsTerminal(int(os.Stdout.Fd())) &&
			os.Getenv("NO_COLOR") == "" &&
			!strings.EqualFold(os.Getenv("TERM"), "dumb")
		console := newConsoleWriter(os.Stdout, color)
		file := newConsoleWriter(rotating, false)
		multi := zerolog.MultiLevelWriter(
			zerolog.SyncWriter(console),
			zerolog.SyncWriter(file),
		)

		core = &loggingCore{
			writer:   multi,
			rotating: rotating,
		}
	})
	return core
}

func newConsoleWriter(out io.Writer, color bool) zerolog.ConsoleWriter {
	return zerolog.ConsoleWriter{
		Out:           out,
		TimeFormat:    "2006-01-02 15:04:05",
		NoColor:       !color,
		FieldsExclude: []string{subsystemField},
		FormatLevel:   formatLevel(color),
		FormatPrepare: func(event map[string]any) error {
			subsystem, _ := event[subsystemField].(string)
			if subsystem == "" {
				return nil
			}
			message := fmt.Sprint(event[zerolog.MessageFieldName])
			event[zerolog.MessageFieldName] = fmt.Sprintf("[%s] %s", subsystem, message)
			return nil
		},
	}
}

func formatLevel(color bool) zerolog.Formatter {
	return func(value any) string {
		level := strings.ToUpper(fmt.Sprint(value))
		formatted := fmt.Sprintf("| %-6s|", level)
		if !color {
			return formatted
		}

		code := "\033[37m"
		switch strings.ToLower(fmt.Sprint(value)) {
		case "debug":
			code = "\033[36m"
		case "info":
			code = "\033[32m"
		case "warn":
			code = "\033[33m"
		case "error":
			code = "\033[31m"
		case "fatal":
			code = "\033[35m"
		case "panic":
			code = "\033[41m"
		}
		return code + formatted + "\033[0m"
	}
}

func parseLevel(value string) (zerolog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return zerolog.InfoLevel, nil
	case "trace":
		return zerolog.TraceLevel, nil
	case "debug":
		return zerolog.DebugLevel, nil
	case "warn", "warning":
		return zerolog.WarnLevel, nil
	case "error":
		return zerolog.ErrorLevel, nil
	default:
		return zerolog.InfoLevel, fmt.Errorf("unsupported log level %q", value)
	}
}

// SetLevel changes the process-wide threshold. Component loggers created
// before this call observe the new level without being rebuilt.
func SetLevel(value string) error {
	level, err := parseLevel(value)
	if err != nil {
		return err
	}
	levelMu.Lock()
	defer levelMu.Unlock()

	// zerolog checks this atomic threshold before allocating or formatting an
	// event, which keeps disabled playback-path debug logs effectively free.
	zerolog.SetGlobalLevel(level)
	levelConfigured = true
	return nil
}

func New(subsystem string) zerolog.Logger {
	levelMu.Lock()
	if !levelConfigured {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		levelConfigured = true
	}
	levelMu.Unlock()

	return zerolog.New(getCore().writer).
		With().
		Timestamp().
		Str(subsystemField, subsystem).
		Logger()
}

func Default() zerolog.Logger {
	defaultOnce.Do(func() {
		defaultLog = New("tessarr")
	})
	return defaultLog
}

// Close flushes and closes the process-wide rotating log file.
func Close() error {
	if core == nil || core.rotating == nil {
		return nil
	}
	return core.rotating.Close()
}
