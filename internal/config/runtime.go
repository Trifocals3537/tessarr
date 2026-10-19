package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// The active configuration is an immutable snapshot. Writers serialize through
// updateMu, edit a private deep copy, persist it, and publish the copy with one
// atomic pointer swap. Readers therefore never observe a partly-applied update.
var (
	activeConfig  atomic.Pointer[Config]
	desiredConfig atomic.Pointer[Config]
	updateMu      sync.Mutex
	pathMu        sync.RWMutex
)

// UpdateResult describes the durable configuration and whether the running
// process adopted it. Restart-required updates are saved but deliberately not
// published; the replacement process loads the desired configuration from disk.
type UpdateResult struct {
	Previous        *Config
	Desired         *Config
	Active          *Config
	RestartRequired bool
}

func SetConfigPath(path string) error {
	if path != "" {
		validated, err := safepath.ValidateRoot(path)
		if err != nil {
			return fmt.Errorf("invalid configuration root: %w", err)
		}
		path = validated
	}
	pathMu.Lock()
	configPath = path
	pathMu.Unlock()
	return nil
}

func GetMainPath() string {
	pathMu.RLock()
	path := configPath
	pathMu.RUnlock()
	return path
}

// Get returns the current immutable process snapshot. Callers must not mutate
// the returned Config or anything reachable from it; use Update for changes.
func Get() *Config {
	if cfg := activeConfig.Load(); cfg != nil {
		return cfg
	}

	updateMu.Lock()
	defer updateMu.Unlock()
	if cfg := activeConfig.Load(); cfg != nil {
		return cfg
	}

	cfg := &Config{}
	if err := cfg.loadConfig(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "configuration Error: %v\n", err)
		os.Exit(1)
	}
	activeConfig.Store(cfg)
	desiredConfig.Store(cfg)
	return cfg
}

// Clone returns a deep copy that callers may safely edit. Auth is excluded from
// Config's JSON representation, so it is copied explicitly.
func Clone(cfg *Config) (*Config, error) {
	if cfg == nil {
		return nil, nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal configuration clone: %w", err)
	}
	clone := &Config{}
	if err := json.Unmarshal(data, clone); err != nil {
		return nil, fmt.Errorf("unmarshal configuration clone: %w", err)
	}
	clone.Auth = cloneAuth(cfg.Auth)
	return clone, nil
}

func cloneAuth(auth *Auth) *Auth {
	if auth == nil {
		return nil
	}
	clone := *auth
	return &clone
}

// Update applies edit to a private draft, persists the complete desired state,
// and atomically publishes it when all changed fields are runtime-safe. The
// callback must not retain or publish the draft pointer.
func Update(edit func(*Config) error) (UpdateResult, error) {
	// Initialize outside updateMu; Get performs its own serialized first load.
	Get()

	updateMu.Lock()
	defer updateMu.Unlock()

	current := activeConfig.Load()
	base := desiredConfig.Load()
	if base == nil {
		base = current
	}
	draft, err := Clone(base)
	if err != nil {
		return UpdateResult{}, err
	}
	if err := edit(draft); err != nil {
		return UpdateResult{}, err
	}
	// Normalize before deciding whether the update is hot. This also makes the
	// rollback capsule's desired digest match the document Save will persist.
	if err := draft.setDefaultsForPath(GetMainPath(), false); err != nil {
		return UpdateResult{}, err
	}
	// Save materializes generated defaults. Do that before cloning the desired
	// snapshot or hashing a restart transaction so the persisted document and
	// the transaction always describe the same STRM signing identity.
	if err := draft.setStrmDefaults(true); err != nil {
		return UpdateResult{}, err
	}
	// Save also fills missing auth credentials. Materialize them before the
	// desired snapshot and restart digest are computed, without writing auth.json
	// outside Update's rollback boundary.
	if _, err := draft.materializeAuthDefaults(); err != nil {
		return UpdateResult{}, err
	}
	published, err := Clone(draft)
	if err != nil {
		return UpdateResult{}, err
	}
	restartRequired := current.RequiresRestart(published)
	var restartPrepared *restartPreparation
	if restartRequired {
		restartPrepared, err = prepareRestartTransaction(current, published)
		if err != nil {
			return UpdateResult{}, err
		}
	}
	abortRestart := func(updateErr error) error {
		if restartPrepared == nil {
			return updateErr
		}
		if abortErr := restartPrepared.Rollback(); abortErr != nil {
			return errors.Join(updateErr, fmt.Errorf("remove prepared restart rollback: %w", abortErr))
		}
		return updateErr
	}

	authChanged := !reflect.DeepEqual(base.Auth, draft.Auth)
	var previousAuth []byte
	previousAuthExists := false
	if authChanged {
		previousAuth, err = safepath.ReadRegularFile(draft.AuthFile(), maxConfigurationFileBytes)
		switch {
		case err == nil:
			previousAuthExists = true
		case errors.Is(err, os.ErrNotExist):
			err = nil
		default:
			return UpdateResult{}, abortRestart(fmt.Errorf("read existing authentication before update: %w", err))
		}
		if err := draft.saveAuth(draft.Auth); err != nil {
			return UpdateResult{}, abortRestart(fmt.Errorf("persist authentication update: %w", err))
		}
	}

	if err := draft.Save(); err != nil {
		if authChanged {
			rollbackErr := restoreAuthFile(draft.AuthFile(), previousAuth, previousAuthExists)
			if rollbackErr != nil {
				return UpdateResult{}, abortRestart(errors.Join(err, fmt.Errorf("roll back authentication update: %w", rollbackErr)))
			}
		}
		return UpdateResult{}, abortRestart(err)
	}

	desiredConfig.Store(published)
	active := current
	if !restartRequired {
		activeConfig.Store(published)
		active = published
	}

	previousCopy, err := Clone(base)
	if err != nil {
		return UpdateResult{}, err
	}
	desiredCopy, err := Clone(published)
	if err != nil {
		return UpdateResult{}, err
	}
	activeCopy, err := Clone(active)
	if err != nil {
		return UpdateResult{}, err
	}
	return UpdateResult{
		Previous:        previousCopy,
		Desired:         desiredCopy,
		Active:          activeCopy,
		RestartRequired: restartRequired,
	}, nil
}

func restoreAuthFile(path string, data []byte, existed bool) error {
	if existed {
		return persistAuth(path, data)
	}
	if err := safepath.RemoveRegularFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return safepath.SyncDirectory(filepath.Dir(path))
}

// Reset clears only the in-memory snapshot. The next Get reloads the desired
// configuration from disk, matching the behavior of a process restart.
func Reset() {
	updateMu.Lock()
	activeConfig.Store(nil)
	desiredConfig.Store(nil)
	updateMu.Unlock()
}
