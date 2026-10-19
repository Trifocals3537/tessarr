package nntp

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

// requestBody positions the shared reader at a dot-terminated article body.
// Some servers return 430 for BODY while serving the exact same message with
// ARTICLE. Try that command once, on this provider, before declaring it missing.
// The normal BODY path adds no requests; auth/transient/transfer errors never
// trigger this fallback. All existing yEnc decoding and CRC checks still apply.
func (c *Connection) requestBody(messageID string) error {
	if err := c.sendCommandArg("BODY", messageID); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send BODY command: %w", err))
	}
	code, message, err := c.readResponseCodeWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read body response: %w", err))
	}
	if code == 222 {
		return nil
	}
	missing := classifyNNTPError(code, string(message))
	if code != 430 {
		return missing
	}
	if err := c.sendCommandArg("ARTICLE", messageID); err != nil {
		return NewConnectionError(fmt.Errorf("failed to send ARTICLE command: %w", err))
	}
	code, message, err = c.readResponseCodeWithDeadline(timeouts.StreamBodyTimeout)
	if err != nil {
		return NewConnectionError(fmt.Errorf("failed to read article response: %w", err))
	}
	if code == 500 || code == 501 {
		return missing
	} // Unsupported on this server.
	if code != 220 {
		return classifyNNTPError(code, string(message))
	}
	invalid := func(reason string) error {
		_ = c.Close() // Never reuse a connection with an unconsumed/malformed article.
		return NewConnectionError(fmt.Errorf("invalid ARTICLE fallback: %s", reason))
	}
	// RFC 3977: response arguments are article-number message-id [text].
	fields := strings.Fields(string(message))
	if len(fields) < 2 || fields[1] != messageID {
		return invalid("response message-id mismatch")
	}
	_ = c.conn.SetReadDeadline(utils.Now().Add(timeouts.StreamBodyTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	const maxHeaderBytes = 64 << 10
	total := 0
	foundID, haveHeader, lastWasID := false, false, false
	for {
		line, err := c.reader.ReadSlice('\n')
		total += len(line)
		if err != nil || total > maxHeaderBytes {
			return invalid("truncated or oversized headers")
		}
		line = bytes.TrimSuffix(line, []byte("\n"))
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) == 0 {
			if !foundID {
				return invalid("missing message-id header")
			}
			return nil
		}
		if bytes.Equal(line, []byte(".")) {
			return invalid("missing header/body separator")
		}
		if line[0] == ' ' || line[0] == '\t' {
			if !haveHeader || lastWasID {
				return invalid("invalid folded header")
			}
			continue
		}
		name, value, ok := bytes.Cut(line, []byte(":"))
		if !ok {
			return invalid("malformed header")
		}
		haveHeader = true
		lastWasID = bytes.EqualFold(name, []byte("Message-ID"))
		if lastWasID {
			if foundID || strings.TrimSpace(string(value)) != messageID {
				return invalid("message-id header mismatch")
			}
			foundID = true
		}
	}
}
