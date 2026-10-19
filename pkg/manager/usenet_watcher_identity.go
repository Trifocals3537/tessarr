package manager

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"github.com/google/uuid"
)

const (
	// Keep the original domain forever: it is part of the persisted UUID
	// algorithm, not a display name. Changing it would duplicate accepted NZBs.
	watchedNZBIdentityDomain = "decypharr/watched-nzb/v1"
	watchedNZBProvider       = "usenet"
	watchedNZBCategory       = "uncategorized"
)

var errWatchedNZBStateAmbiguous = errors.New("watched NZB state is ambiguous")

// watchedNZBIdentity is stable across retries and process restarts. Its UUIDv8
// version nibble keeps the deterministic namespace disjoint from Tessarr's
// ordinary random UUIDv4 import IDs.
type watchedNZBIdentity struct {
	ID            string
	ClaimedName   string
	ContentDigest [sha256.Size]byte
}

func newWatchedNZBIdentity(claimedName string, content []byte) (watchedNZBIdentity, error) {
	if err := validateWatchedNZBClaimedName(claimedName); err != nil {
		return watchedNZBIdentity{}, err
	}
	if len(content) == 0 {
		return watchedNZBIdentity{}, fmt.Errorf("watched NZB content is empty")
	}

	hash := sha256.New()
	_, _ = hash.Write([]byte(watchedNZBIdentityDomain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(claimedName))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(content)
	sum := hash.Sum(nil)

	var id uuid.UUID
	copy(id[:], sum[:len(id)])
	id[6] = (id[6] & 0x0f) | 0x80 // RFC 9562 UUIDv8
	id[8] = (id[8] & 0x3f) | 0x80 // RFC variant

	identity := watchedNZBIdentity{
		ID:            id.String(),
		ClaimedName:   claimedName,
		ContentDigest: sha256.Sum256(content),
	}
	if err := identity.validate(); err != nil {
		return watchedNZBIdentity{}, err
	}
	return identity, nil
}

func validateWatchedNZBClaimedName(claimedName string) error {
	if err := safepath.ValidateIdentifier(claimedName); err != nil {
		return fmt.Errorf("invalid watched NZB claimed name %q: %w", claimedName, err)
	}
	if !strings.HasSuffix(claimedName, ".nzb") {
		return fmt.Errorf("watched NZB claimed name %q does not end in .nzb", claimedName)
	}
	return nil
}

func (identity watchedNZBIdentity) validate() error {
	if err := validateWatchedNZBClaimedName(identity.ClaimedName); err != nil {
		return err
	}
	parsed, err := uuid.Parse(identity.ID)
	if err != nil {
		return fmt.Errorf("invalid watched NZB ID %q: %w", identity.ID, err)
	}
	if parsed.String() != identity.ID ||
		parsed.Version() != uuid.Version(8) ||
		parsed.Variant() != uuid.RFC4122 {
		return fmt.Errorf("watched NZB ID %q is not a canonical RFC UUIDv8", identity.ID)
	}
	if identity.ContentDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("watched NZB content digest is empty")
	}
	return nil
}

// matchWatchedNZBEntry validates only immutable watcher provenance. Mutable
// progress/status/file fields deliberately do not participate in reconciliation.
func matchWatchedNZBEntry(identity watchedNZBIdentity, entry *storage.Entry) error {
	if err := identity.validate(); err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("watched NZB entry is nil")
	}
	if entry.InfoHash != identity.ID {
		return fmt.Errorf("entry ID %q does not match watched NZB ID %q", entry.InfoHash, identity.ID)
	}
	if entry.Protocol != config.ProtocolNZB {
		return fmt.Errorf("entry protocol %q is not NZB", entry.Protocol)
	}
	// AddNewNZB deliberately uses the parser's release name for both display
	// fields. The persisted deterministic ID is the immutable commitment to the
	// distinct claimed leaf and content digest.
	if entry.Name == "" {
		return fmt.Errorf("watched NZB parsed entry name is empty")
	}
	if entry.OriginalFilename != entry.Name {
		return fmt.Errorf(
			"entry original filename %q does not match parsed name %q",
			entry.OriginalFilename,
			entry.Name,
		)
	}
	if entry.Category != watchedNZBCategory {
		return fmt.Errorf(
			"watched NZB entry category %q is not %q",
			entry.Category,
			watchedNZBCategory,
		)
	}
	if entry.Action != config.DownloadActionNone {
		return fmt.Errorf("watched NZB entry action %q is not none", entry.Action)
	}
	if entry.ActiveProvider != watchedNZBProvider {
		return fmt.Errorf("watched NZB active provider %q is not usenet", entry.ActiveProvider)
	}
	if len(entry.Providers) != 1 {
		return fmt.Errorf("watched NZB entry has %d provider records, want exactly one", len(entry.Providers))
	}
	provider, ok := entry.Providers[watchedNZBProvider]
	if !ok || provider == nil {
		return fmt.Errorf("watched NZB usenet provider record is missing")
	}
	if provider.Provider != watchedNZBProvider {
		return fmt.Errorf("provider provenance %q is not usenet", provider.Provider)
	}
	if provider.ID != identity.ID {
		return fmt.Errorf("provider ID %q does not match watched NZB ID %q", provider.ID, identity.ID)
	}
	if provider.RemovedAt != nil {
		return fmt.Errorf("watched NZB provider record is removed")
	}
	return nil
}

func matchWatchedNZBMetadata(identity watchedNZBIdentity, metadata *storage.NZB) error {
	if err := identity.validate(); err != nil {
		return err
	}
	if metadata == nil {
		return fmt.Errorf("watched NZB metadata is nil")
	}
	if metadata.ID != identity.ID {
		return fmt.Errorf("metadata ID %q does not match watched NZB ID %q", metadata.ID, identity.ID)
	}
	if metadata.Category != watchedNZBCategory {
		return fmt.Errorf(
			"watched NZB metadata category %q is not %q",
			metadata.Category,
			watchedNZBCategory,
		)
	}
	if metadata.Name == "" {
		return fmt.Errorf("watched NZB metadata name is empty")
	}
	return nil
}

func matchWatchedNZBEntryMetadata(
	identity watchedNZBIdentity,
	entry *storage.Entry,
	metadata *storage.NZB,
) error {
	if err := matchWatchedNZBEntry(identity, entry); err != nil {
		return err
	}
	if err := matchWatchedNZBMetadata(identity, metadata); err != nil {
		return err
	}
	if entry.Name != metadata.Name || entry.OriginalFilename != metadata.Name {
		return fmt.Errorf(
			"queue parsed names %q/%q do not match metadata name %q",
			entry.Name,
			entry.OriginalFilename,
			metadata.Name,
		)
	}
	return nil
}

// matchWatchedNZBDurableState proves that a visible queue/main record and its
// metadata are backed by this identity's exact ID-bound staged source. The
// staged bytes, not just the row key, are part of watcher provenance.
func matchWatchedNZBDurableState(
	identity watchedNZBIdentity,
	entry *storage.Entry,
	metadata *storage.NZB,
	metadataRoot string,
	maxBytes int64,
) error {
	if err := matchWatchedNZBEntryMetadata(identity, entry, metadata); err != nil {
		return err
	}
	if err := matchWatchedNZBStagedSource(
		identity,
		metadataRoot,
		entry.Magnet,
		maxBytes,
	); err != nil {
		return err
	}
	if metadata.Path == "" {
		if watchedNZBMetadataIsTerminal(metadata) {
			return nil
		}
		return fmt.Errorf("nonterminal watched NZB metadata source path is empty")
	}
	source, err := usenet.ReadNZBSourceAt(
		metadataRoot,
		identity.ID,
		metadata.Path,
		maxBytes,
	)
	if err != nil {
		if os.IsNotExist(err) && watchedNZBMetadataIsTerminal(metadata) {
			return nil
		}
		return fmt.Errorf("read watched NZB managed source: %w", err)
	}
	if digest := sha256.Sum256(source); digest != identity.ContentDigest {
		return fmt.Errorf(
			"watched NZB managed source digest %x does not match identity digest %x",
			digest,
			identity.ContentDigest,
		)
	}
	return nil
}

func matchWatchedNZBMainTerminalState(
	identity watchedNZBIdentity,
	entry *storage.Entry,
	metadata *storage.NZB,
	metadataRoot string,
	maxBytes int64,
) error {
	if err := matchWatchedNZBEntryMetadata(identity, entry, metadata); err != nil {
		return err
	}
	if metadata.Status != usenet.NZBStatusCompleted {
		return fmt.Errorf(
			"main-only watched NZB metadata status %q is not completed",
			metadata.Status,
		)
	}
	if entry.Magnet != "" {
		staged, err := usenet.ReadStagedNZBAt(
			metadataRoot,
			identity.ID,
			entry.Magnet,
			maxBytes,
		)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("validate retained main staged path: %w", err)
			}
		} else if digest := sha256.Sum256(staged); digest != identity.ContentDigest {
			return fmt.Errorf(
				"retained main staged source digest %x does not match identity digest %x",
				digest,
				identity.ContentDigest,
			)
		}
	}
	if metadata.Path != "" {
		source, err := usenet.ReadNZBSourceAt(
			metadataRoot,
			identity.ID,
			metadata.Path,
			maxBytes,
		)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("validate retained main managed path: %w", err)
			}
		} else if digest := sha256.Sum256(source); digest != identity.ContentDigest {
			return fmt.Errorf(
				"retained main managed source digest %x does not match identity digest %x",
				digest,
				identity.ContentDigest,
			)
		}
	}
	return nil
}

func watchedNZBMetadataIsTerminal(metadata *storage.NZB) bool {
	return metadata != nil &&
		(metadata.Status == usenet.NZBStatusCompleted ||
			metadata.Status == usenet.NZBStatusFailed)
}

func matchWatchedNZBStagedSource(
	identity watchedNZBIdentity,
	metadataRoot, stagedPath string,
	maxBytes int64,
) error {
	if err := identity.validate(); err != nil {
		return err
	}
	if stagedPath == "" {
		return fmt.Errorf("watched NZB staged source path is empty")
	}
	content, err := usenet.ReadStagedNZBAt(
		metadataRoot,
		identity.ID,
		stagedPath,
		maxBytes,
	)
	if err != nil {
		return fmt.Errorf("read watched NZB staged source: %w", err)
	}
	if digest := sha256.Sum256(content); digest != identity.ContentDigest {
		return fmt.Errorf(
			"watched NZB staged source digest %x does not match identity digest %x",
			digest,
			identity.ContentDigest,
		)
	}
	return nil
}

func matchWatchedNZBPartialMetadata(
	identity watchedNZBIdentity,
	metadata *storage.NZB,
	metadataRoot string,
	maxBytes int64,
) error {
	if err := matchWatchedNZBMetadata(identity, metadata); err != nil {
		return err
	}
	staged, err := usenet.ReadCanonicalStagedNZBAt(
		metadataRoot,
		identity.ID,
		maxBytes,
	)
	if err != nil {
		return fmt.Errorf("read canonical watched NZB staged source: %w", err)
	}
	if digest := sha256.Sum256(staged); digest != identity.ContentDigest {
		return fmt.Errorf(
			"canonical staged source digest %x does not match identity digest %x",
			digest,
			identity.ContentDigest,
		)
	}
	source, err := usenet.ReadNZBSourceAt(
		metadataRoot,
		identity.ID,
		metadata.Path,
		maxBytes,
	)
	if err != nil {
		return fmt.Errorf("read partial watched NZB managed source: %w", err)
	}
	if digest := sha256.Sum256(source); digest != identity.ContentDigest {
		return fmt.Errorf(
			"partial managed source digest %x does not match identity digest %x",
			digest,
			identity.ContentDigest,
		)
	}
	return nil
}

type watchedNZBEntryLookup func(string) (*storage.Entry, error)
type watchedNZBMetadataLookup func(string) (*storage.NZB, error)
type watchedNZBNotFound func(error) bool

// inspectWatchedNZBEntry treats only the caller's typed not-found predicate as
// authoritative absence. Every lookup failure, nil record, or identity mismatch
// is ambiguous so the importing source remains available for recovery.
func inspectWatchedNZBEntry(
	identity watchedNZBIdentity,
	lookup watchedNZBEntryLookup,
	isNotFound watchedNZBNotFound,
) (bool, error) {
	if err := identity.validate(); err != nil {
		return false, fmt.Errorf("%w: %v", errWatchedNZBStateAmbiguous, err)
	}
	if lookup == nil || isNotFound == nil {
		return false, fmt.Errorf("%w: entry lookup input is nil", errWatchedNZBStateAmbiguous)
	}
	entry, err := lookup(identity.ID)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("%w: inspect entry: %v", errWatchedNZBStateAmbiguous, err)
	}
	if err := matchWatchedNZBEntry(identity, entry); err != nil {
		return false, fmt.Errorf("%w: %v", errWatchedNZBStateAmbiguous, err)
	}
	return true, nil
}

// inspectWatchedNZBMetadata applies the same typed-absence rule to the
// ID-bound NZB metadata needed by a visible active queue record.
func inspectWatchedNZBMetadata(
	identity watchedNZBIdentity,
	lookup watchedNZBMetadataLookup,
	isNotFound watchedNZBNotFound,
) (bool, error) {
	if err := identity.validate(); err != nil {
		return false, fmt.Errorf("%w: %v", errWatchedNZBStateAmbiguous, err)
	}
	if lookup == nil || isNotFound == nil {
		return false, fmt.Errorf("%w: metadata lookup input is nil", errWatchedNZBStateAmbiguous)
	}
	metadata, err := lookup(identity.ID)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("%w: inspect metadata: %v", errWatchedNZBStateAmbiguous, err)
	}
	if err := matchWatchedNZBMetadata(identity, metadata); err != nil {
		return false, fmt.Errorf("%w: %v", errWatchedNZBStateAmbiguous, err)
	}
	return true, nil
}
