// Package logsafe neutralizes line-oriented log values before they reach
// console or plain-text log sinks.
package logsafe

import "strings"

// Text removes both newline forms so an untrusted value cannot forge a second
// log record. Callers should still place the result after a fixed label.
func Text(value string) string {
	value = strings.ReplaceAll(value, "\r", "")
	return strings.ReplaceAll(value, "\n", "")
}
