package manager

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	readFailureLogInterval = time.Minute
	readFailureLogCapacity = 4096
)

func classifyTerminalReadFailure(err error) (class string, retryable bool) {
	switch {
	case err == nil:
		return "none", false
	case errors.Is(err, context.Canceled):
		return "canceled", false
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout", true
	case nntp.IsArticleNotFoundError(err):
		return "content_missing", false
	case nntp.IsAuthenticationError(err):
		return "provider_auth", false
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof", true
	}

	var customErr *customerror.Error
	if errors.As(err, &customErr) {
		switch customErr.HTTPStatus() {
		case 401, 403:
			return "provider_auth", false
		case 404, 410:
			return "content_missing", false
		case 429:
			return "rate_limited", true
		}
		if customErr.IsPermanent() {
			return "permanent", false
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout", true
		}
		return "transport", true
	}
	if customerror.IsRetriableError(err) {
		return "transient", true
	}
	return "other", false
}

func (m *Manager) allowReadFailureLog(key string, now time.Time) bool {
	m.readFailureLogMu.Lock()
	defer m.readFailureLogMu.Unlock()
	if m.readFailureLogLast == nil {
		m.readFailureLogLast = make(map[string]time.Time)
	}
	if last, ok := m.readFailureLogLast[key]; ok && now.Sub(last) < readFailureLogInterval {
		return false
	}
	if len(m.readFailureLogLast) >= readFailureLogCapacity {
		for existing, last := range m.readFailureLogLast {
			if now.Sub(last) >= readFailureLogInterval {
				delete(m.readFailureLogLast, existing)
			}
		}
		if len(m.readFailureLogLast) >= readFailureLogCapacity {
			return false
		}
	}
	m.readFailureLogLast[key] = now
	return true
}

// RecordTerminalReadFailure feeds exhausted DFS reads into the persistent
// health reconciler. It deliberately excludes the raw error from logs because
// provider errors can contain signed URLs, credentials, or article IDs.
func (m *Manager) RecordTerminalReadFailure(entry *storage.Entry, filename string, offset, length int64, err error) {
	if m == nil || entry == nil || err == nil || errors.Is(err, context.Canceled) {
		return
	}
	class, retryable := classifyTerminalReadFailure(err)
	m.terminalReadFailures.Add(1)
	key := strings.Join([]string{entry.InfoHash, filename, class}, "\x00")
	shouldLog := m.allowReadFailureLog(key, time.Now())

	persisted := false
	if m.storage != nil && entry.Name != "" {
		persisted = m.storage.MarkEntryDirtyChecked(entry.Name, entry.Protocol, "read_failure:"+class) == nil
	}
	if persisted && m.repair != nil {
		m.repair.QueueDirtyEntry(entry.Name)
	}
	if !shouldLog {
		return
	}

	provider := entry.ActiveProvider
	if provider == "" && entry.Protocol == "nzb" {
		provider = "usenet"
	}
	m.logger.Warn().
		Str("event", "stream.read_terminal_failure").
		Str("outcome", "failed").
		Str("entry", entry.Name).
		Str("hash", entry.InfoHash).
		Str("file", filename).
		Str("protocol", string(entry.Protocol)).
		Str("provider", provider).
		Int64("offset", offset).
		Int64("length", length).
		Str("failure_class", class).
		Bool("retryable", retryable).
		Bool("health_dirty_persisted", persisted).
		Msg("Terminal media read failure")
}
