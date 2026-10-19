package config

import (
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

var restartTransactionMu sync.Mutex

const (
	restartTransactionVersion = 1
	restartStatePrepared      = "prepared"
	restartStateApplying      = "applying"
	restartTransactionName    = "config-restart-transaction.json"
)

type restartTransaction struct {
	Version        int                `json:"version"`
	State          string             `json:"state"`
	CreatedAt      time.Time          `json:"created_at"`
	DesiredDigest  string             `json:"desired_digest"`
	PreviousConfig stdjson.RawMessage `json:"previous_config"`
	PreviousAuth   stdjson.RawMessage `json:"previous_auth"`
}

// restartPreparation remembers the transaction record that existed before a
// configuration update. If persistence fails, Rollback restores that exact
// record so an earlier pending update keeps its original recovery point.
type restartPreparation struct {
	previous *restartTransaction
}

func restartTransactionPath() string {
	return filepath.Join(GetMainPath(), restartTransactionName)
}

func prepareRestartTransaction(previous, desired *Config) (*restartPreparation, error) {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	digest, err := restartSnapshotDigest(desired)
	if err != nil {
		return nil, fmt.Errorf("digest desired configuration: %w", err)
	}

	tx, err := loadRestartTransaction()
	if err != nil {
		return nil, err
	}
	preparation := &restartPreparation{}
	if tx != nil {
		if tx.State != restartStatePrepared {
			return nil, errors.New("a configuration restart is already being applied")
		}
		copy := *tx
		copy.PreviousConfig = append(stdjson.RawMessage(nil), tx.PreviousConfig...)
		copy.PreviousAuth = append(stdjson.RawMessage(nil), tx.PreviousAuth...)
		preparation.previous = &copy
		tx.DesiredDigest = digest
	} else {
		previousConfig, previousAuth, err := marshalRestartSnapshot(previous)
		if err != nil {
			return nil, fmt.Errorf("capture last-known-good configuration: %w", err)
		}
		tx = &restartTransaction{
			Version:        restartTransactionVersion,
			State:          restartStatePrepared,
			CreatedAt:      time.Now().UTC(),
			DesiredDigest:  digest,
			PreviousConfig: previousConfig,
			PreviousAuth:   previousAuth,
		}
	}
	if err := saveRestartTransaction(tx); err != nil {
		return nil, fmt.Errorf("persist restart transaction: %w", err)
	}
	return preparation, nil
}

func (p *restartPreparation) Rollback() error {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	if p != nil && p.previous != nil {
		return saveRestartTransaction(p.previous)
	}
	return removeRestartTransaction()
}

func marshalRestartSnapshot(cfg *Config) ([]byte, []byte, error) {
	if cfg == nil {
		return nil, nil, errors.New("configuration snapshot is unavailable")
	}
	configData, err := stdjson.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	auth := cfg.Auth
	if auth == nil {
		auth = &Auth{}
	}
	authData, err := stdjson.MarshalIndent(auth, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	return configData, authData, nil
}

func restartSnapshotDigest(cfg *Config) (string, error) {
	configData, authData, err := marshalRestartSnapshot(cfg)
	if err != nil {
		return "", err
	}
	return digestRestartBytes(configData, authData)
}

func digestRestartBytes(configData, authData []byte) (string, error) {
	var cfg Config
	if err := stdjson.Unmarshal(configData, &cfg); err != nil {
		return "", fmt.Errorf("decode configuration for digest: %w", err)
	}
	var auth Auth
	if err := stdjson.Unmarshal(authData, &auth); err != nil {
		return "", fmt.Errorf("decode authentication for digest: %w", err)
	}
	canonicalConfig, err := stdjson.Marshal(cfg)
	if err != nil {
		return "", err
	}
	canonicalAuth, err := stdjson.Marshal(auth)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write(canonicalConfig)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(canonicalAuth)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func loadRestartTransaction() (*restartTransaction, error) {
	data, err := safepath.ReadRegularFile(restartTransactionPath(), 3*maxConfigurationFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read restart transaction: %w", err)
	}
	var tx restartTransaction
	if err := stdjson.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("decode restart transaction: %w", err)
	}
	if tx.Version != restartTransactionVersion {
		return nil, fmt.Errorf("unsupported restart transaction version %d", tx.Version)
	}
	if tx.State != restartStatePrepared && tx.State != restartStateApplying {
		return nil, fmt.Errorf("invalid restart transaction state %q", tx.State)
	}
	if tx.DesiredDigest == "" || len(tx.PreviousConfig) == 0 || len(tx.PreviousAuth) == 0 {
		return nil, errors.New("restart transaction is incomplete")
	}
	if _, err := digestRestartBytes(tx.PreviousConfig, tx.PreviousAuth); err != nil {
		return nil, fmt.Errorf("validate rollback snapshot: %w", err)
	}
	return &tx, nil
}

func saveRestartTransaction(tx *restartTransaction) error {
	data, err := stdjson.MarshalIndent(tx, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(restartTransactionPath(), data, privateFileMode)
}

// PrepareRestartStartup advances a prepared transaction before loading the
// desired config. If a prior process already reached applying but never proved
// ready, the last-known-good snapshot is restored instead.
func PrepareRestartStartup() (rolledBack bool, err error) {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	tx, err := loadRestartTransaction()
	if err != nil || tx == nil {
		return false, err
	}
	if tx.State == restartStateApplying {
		if err := restoreRestartTransaction(tx); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := verifyDesiredRestartDigest(tx); err != nil {
		if restoreErr := restoreRestartTransaction(tx); restoreErr != nil {
			return false, errors.Join(err, fmt.Errorf("restore last-known-good configuration: %w", restoreErr))
		}
		return true, nil
	}
	tx.State = restartStateApplying
	if err := saveRestartTransaction(tx); err != nil {
		return false, fmt.Errorf("mark restart transaction applying: %w", err)
	}
	return false, nil
}

// MarkRestartApplying advances an in-process restart immediately before the
// active snapshot and manager are reset.
func MarkRestartApplying() error {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	tx, err := loadRestartTransaction()
	if err != nil || tx == nil {
		return err
	}
	if tx.State == restartStateApplying {
		return nil
	}
	if err := verifyDesiredRestartDigest(tx); err != nil {
		if restoreErr := restoreRestartTransaction(tx); restoreErr != nil {
			return errors.Join(err, fmt.Errorf("restore last-known-good configuration: %w", restoreErr))
		}
		return fmt.Errorf("desired configuration could not be verified; restored last-known-good configuration: %w", err)
	}
	tx.State = restartStateApplying
	if err := saveRestartTransaction(tx); err != nil {
		return fmt.Errorf("mark restart transaction applying: %w", err)
	}
	return nil
}

func verifyDesiredRestartDigest(tx *restartTransaction) error {
	configData, err := safepath.ReadRegularFile(filepath.Join(GetMainPath(), "config.json"), maxConfigurationFileBytes)
	if err != nil {
		return fmt.Errorf("read desired configuration: %w", err)
	}
	authData, err := safepath.ReadRegularFile(filepath.Join(GetMainPath(), "auth.json"), maxConfigurationFileBytes)
	if err != nil {
		return fmt.Errorf("read desired authentication: %w", err)
	}
	digest, err := digestRestartBytes(configData, authData)
	if err != nil {
		return err
	}
	if digest != tx.DesiredDigest {
		return errors.New("desired configuration changed after the restart transaction was prepared")
	}
	return nil
}

// CommitRestart removes the rollback capsule only after manager and mount
// readiness have both been published.
func CommitRestart() error {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	tx, err := loadRestartTransaction()
	if err != nil || tx == nil {
		return err
	}
	if tx.State != restartStateApplying {
		return fmt.Errorf("cannot commit restart transaction in state %q", tx.State)
	}
	return removeRestartTransaction()
}

// RollbackApplyingRestart restores the prior files after an attempted config
// failed before readiness. Prepared transactions are left for their first try.
func RollbackApplyingRestart() (bool, error) {
	restartTransactionMu.Lock()
	defer restartTransactionMu.Unlock()

	tx, err := loadRestartTransaction()
	if err != nil || tx == nil {
		return false, err
	}
	if tx.State != restartStateApplying {
		return false, nil
	}
	if err := restoreRestartTransaction(tx); err != nil {
		return false, err
	}
	return true, nil
}

func restoreRestartTransaction(tx *restartTransaction) error {
	if _, err := digestRestartBytes(tx.PreviousConfig, tx.PreviousAuth); err != nil {
		return fmt.Errorf("validate last-known-good snapshot: %w", err)
	}
	if err := persistConfig(filepath.Join(GetMainPath(), "config.json"), tx.PreviousConfig); err != nil {
		return fmt.Errorf("restore last-known-good configuration: %w", err)
	}
	if err := persistAuth(filepath.Join(GetMainPath(), "auth.json"), tx.PreviousAuth); err != nil {
		return fmt.Errorf("restore last-known-good authentication: %w", err)
	}
	if err := removeRestartTransaction(); err != nil {
		return fmt.Errorf("remove restored restart transaction: %w", err)
	}
	return nil
}

func removeRestartTransaction() error {
	path := restartTransactionPath()
	if err := safepath.RemoveRegularFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return safepath.SyncDirectory(filepath.Dir(path))
}
