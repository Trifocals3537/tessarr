package common

import (
	"context"
	"errors"
)

// TorrentPresence is evidence from a read-only, account-scoped exact-ID query.
// Its zero value is unknown: an ordinary listing miss is never absence proof.
type TorrentPresence uint8

const (
	TorrentPresenceUnknown TorrentPresence = iota
	TorrentPresent
	TorrentAbsent
)

var ErrPresenceUnverifiable = errors.New("provider cannot establish authoritative transfer absence")

// ContextTorrentPresenceChecker is stronger than GetTorrent: implementations
// must distinguish an exact-ID absence from list omissions, malformed responses,
// authentication errors and generic proxy failures, without mutating state.
type ContextTorrentPresenceChecker interface {
	TorrentPresenceContext(context.Context, string) (TorrentPresence, error)
}
