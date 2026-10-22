package manager

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

var ErrProviderSubmissionPending = errors.New("provider submission outcome uncertain; retry reconciles the existing attempt before submitting again")

func submissionAccount(cfg config.Debrid) string {
	provider := cfg.Provider
	if provider == "" {
		provider = cfg.Name
	}
	// Authenticate a domain-separated account identity with the primary API
	// key. This is an account-binding MAC, not a password verifier. Download
	// keys and tuning changes do not change the account used for creation.
	mac := hmac.New(sha256.New, []byte(cfg.APIKey))
	_, _ = mac.Write([]byte("tessarr/provider-submission-account/v1\x00" + provider))
	return fmt.Sprintf("hmac-sha256-v1:%x", mac.Sum(nil))
}

func pendingSubmission(err error) error {
	return errors.Join(ErrProviderSubmissionPending, err)
}

func definiteSubmissionRejection(err error) bool {
	return isTorrentNotCachedError(err) || isTooManyActiveDownloads(err) || isTorrentContentRejection(err)
}

func (m *Manager) verifySubmissionAccount(entry *storage.Entry) error {
	if placement := entry.GetActiveProvider(); placement != nil && m.storage != nil {
		account := ""
		if m.clients != nil {
			if client := m.ProviderClient(entry.ActiveProvider); client != nil {
				account = submissionAccount(client.Config())
			}
		}
		if err := m.storage.CheckProviderPlacementAccount(entry.ActiveProvider, placement.ID, account); err != nil {
			return pendingSubmission(err)
		}
	}
	if entry.QueueIncarnation == "" {
		return nil // Legacy work predates the submission protocol.
	}
	intent, err := m.queue.storage.GetProviderSubmission(entry)
	if err != nil {
		return err
	}
	if intent == nil || intent.Phase == storage.SubmissionRejected {
		return nil
	}
	client := m.ProviderClient(intent.Provider)
	if client == nil || client.Config().Name != intent.Provider || submissionAccount(client.Config()) != intent.Account {
		return pendingSubmission(fmt.Errorf("the original provider account is unavailable or has changed"))
	}
	return nil
}

func (m *Manager) verifyPlacementAccount(client debrid.Client, id string) error {
	if m.storage == nil {
		return nil
	}
	if err := m.storage.CheckProviderPlacementAccount(client.Config().Name, id, submissionAccount(client.Config())); err != nil {
		return pendingSubmission(err)
	}
	return nil
}

// A recovered, acknowledged refusal has completed its durable rollback. Feed
// the result into cached-first routing without creating the same attempt again.
type resolvedSubmissionRejection struct {
	provider string
	uncached bool
	cause    error
}

func (r *resolvedSubmissionRejection) Error() string { return r.cause.Error() }
func (r *resolvedSubmissionRejection) Unwrap() error { return r.cause }

func (m *Manager) rejectAcknowledgedSubmission(entry *storage.Entry, intent *storage.ProviderSubmission, client debrid.Client, cause error) error {
	if err := m.rollbackQueuedProviderTorrent(entry, client, intent.ProviderID); err != nil {
		return pendingSubmission(errors.Join(cause, err))
	}
	return &resolvedSubmissionRejection{provider: intent.Provider, uncached: intent.DownloadUncached, cause: cause}
}

func (m *Manager) submissionStep(entry *storage.Entry, change func() error) error {
	if m.queue == nil || m.storage == nil {
		return fmt.Errorf("provider submission storage is unavailable")
	}
	err := m.queue.lifecycle.withUpdate(entry, change)
	if errors.Is(err, storage.ErrProviderSubmissionConflict) {
		return pendingSubmission(err)
	}
	if err != nil {
		m.recordWorkflowFault(err)
	}
	return err
}

func (m *Manager) bindSubmission(entry *storage.Entry, intent *storage.ProviderSubmission, remote *debridTypes.Torrent) error {
	return m.submissionStep(entry, func() error {
		updated, err := m.storage.BindProviderSubmission(entry, intent.Attempt, remote)
		if err != nil {
			return err
		}
		refreshSubmissionEntry(entry, updated)
		return nil
	})
}

func refreshSubmissionEntry(entry, updated *storage.Entry) {
	updated.QueueGeneration = entry.QueueGeneration
	updated.MainGeneration = entry.MainGeneration
	updated.MainProviderSnapshot = entry.MainProviderSnapshot
	updated.MainMutationProvider = entry.MainMutationProvider
	updated.MainReimportIncarnation = entry.MainReimportIncarnation
	*entry = *updated
}

// Normal provider polling may also request rollback after binding. Preserve the
// same account/ID scope and never convert a recovered hash match into deletion
// authority. Legacy queue placements keep their existing cleanup behavior.
func (m *Manager) rollbackQueuedProviderTorrent(entry *storage.Entry, client debrid.Client, id string) error {
	if entry.QueueIncarnation == "" {
		return m.deleteProviderTorrent(client, id)
	}
	intent, err := m.queue.storage.GetProviderSubmission(entry)
	if err != nil {
		return err
	}
	if intent == nil {
		return m.deleteProviderTorrent(client, id)
	}
	if intent.Recovered || client == nil || intent.Provider != client.Config().Name || intent.ProviderID != id || intent.Account != submissionAccount(client.Config()) {
		return pendingSubmission(fmt.Errorf("automatic rollback lacks acknowledged provider ownership"))
	}
	if err := m.submissionStep(entry, func() error {
		return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionCleanup)
	}); err != nil {
		return err
	}
	if err := m.deleteProviderTorrent(client, id); err != nil {
		return pendingSubmission(err)
	}
	return m.submissionStep(entry, func() error {
		return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionRejected)
	})
}

func submissionIdentity(remote *debridTypes.Torrent, hash, provider, id string) error {
	if remote == nil || remote.Id == "" || remote.Id != id || (remote.InfoHash != "" && !strings.EqualFold(remote.InfoHash, hash)) || (remote.Debrid != "" && remote.Debrid != provider) {
		return pendingSubmission(fmt.Errorf("provider returned a different or incomplete transfer identity"))
	}
	remote.InfoHash, remote.Debrid = hash, provider
	return nil
}

func getProviderTorrent(ctx context.Context, client debrid.Client, id string) (*debridTypes.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if contextual, ok := client.(debrid.ContextTorrentGetter); ok {
		return contextual.GetTorrentContext(ctx, id)
	}
	// Legacy providers may not support cancellation. Only a read is allowed
	// here; a late result cannot mutate manager state after cancellation.
	type result struct {
		torrent *debridTypes.Torrent
		err     error
	}
	done := make(chan result, 1)
	go func() {
		var torrent *debridTypes.Torrent
		outcome := workflow.Run(ctx, func(context.Context) (err error) {
			torrent, err = client.GetTorrent(id)
			return err
		})
		done <- result{torrent, outcome.Err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-done:
		return r.torrent, r.err
	}
}

// reconcileSubmission never creates remote work or deletes hash matches. An
// empty/eventually-consistent listing is not proof that a timed-out POST failed.
func (m *Manager) reconcileSubmission(ctx context.Context, req *ImportRequest, entry *storage.Entry, intent *storage.ProviderSubmission) (*debridTypes.Torrent, error) {
	client := m.ProviderClient(intent.Provider)
	if client == nil || client.Config().Name != intent.Provider || submissionAccount(client.Config()) != intent.Account {
		return nil, pendingSubmission(fmt.Errorf("the original provider account is unavailable or has changed"))
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if intent.ProviderID != "" {
		if err := m.verifyPlacementAccount(client, intent.ProviderID); err != nil {
			return nil, err
		}
	}
	if intent.Phase == storage.SubmissionCleanup {
		// A cleanup can have succeeded before a crash lost its acknowledgement.
		// Do not replay DELETE against a potentially reused ID. Only an explicit
		// typed absence from the original account permits routing to continue.
		_, err := getProviderTorrent(ctx, client, intent.ProviderID)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !errors.Is(err, customerror.TorrentNotFoundError) {
			return nil, pendingSubmission(fmt.Errorf("the prior rollback requires confirmed provider absence"))
		}
		if err := m.submissionStep(entry, func() error {
			return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionRejected)
		}); err != nil {
			return nil, err
		}
		return m.sendToDebrid(ctx, req, entry)
	}
	var observed *debridTypes.Torrent
	var err error
	if intent.ProviderID != "" && !intent.Recovered {
		// Library listings omit active transfers on several providers. Read
		// the acknowledged ID before CheckStatus, which can select files or
		// restart a transfer. Missing hashes must not inherit our expected hash.
		observed, err = getProviderTorrent(ctx, client, intent.ProviderID)
		if err != nil {
			return nil, pendingSubmission(err)
		}
		if observed == nil || !strings.EqualFold(observed.InfoHash, entry.InfoHash) {
			return nil, pendingSubmission(fmt.Errorf("provider lookup did not confirm the expected content hash"))
		}
		if err := submissionIdentity(observed, entry.InfoHash, intent.Provider, intent.ProviderID); err != nil {
			return nil, err
		}
	} else {
		listed, err := getProviderTorrents(ctx, client)
		if err != nil {
			return nil, pendingSubmission(err)
		}
		matches := make(map[string]*debridTypes.Torrent)
		for _, torrent := range listed {
			if torrent == nil || !strings.EqualFold(torrent.InfoHash, entry.InfoHash) || strings.TrimSpace(torrent.Id) == "" || (intent.ProviderID != "" && torrent.Id != intent.ProviderID) {
				continue
			}
			if torrent.Debrid != "" && torrent.Debrid != intent.Provider {
				return nil, pendingSubmission(fmt.Errorf("provider listing changed account identity"))
			}
			matches[torrent.Id] = torrent
		}
		if len(matches) != 1 {
			return nil, pendingSubmission(fmt.Errorf("provider reconciliation found %d matching IDs; inspect the original provider account", len(matches)))
		}
		for _, torrent := range matches {
			observed = torrent.Copy()
		}
		if intent.ProviderID == "" {
			intent.ProviderID, intent.Recovered = observed.Id, true
			if err := m.submissionStep(entry, func() error {
				return m.storage.RecordProviderSubmissionID(entry, intent.Attempt, intent.ProviderID, true)
			}); err != nil {
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stub := &debridTypes.Torrent{Id: intent.ProviderID, InfoHash: entry.InfoHash, Debrid: intent.Provider,
		Magnet: req.Magnet, Arr: req.Arr, Name: entry.Name, DownloadUncached: intent.DownloadUncached, Files: make(map[string]debridTypes.File)}
	remote := observed
	if intent.Recovered {
		// A hash match proves content, not ownership of an in-progress create.
		// Reuse only completed content; never select files or roll it back.
		if remote.Status != debridTypes.TorrentStatusDownloaded {
			return nil, pendingSubmission(fmt.Errorf("the matching provider transfer is not complete"))
		}
	} else {
		remote, err = checkProviderStatus(ctx, client, stub)
		if ctx.Err() != nil {
			return nil, pendingSubmission(ctx.Err())
		}
		if remote != nil {
			if identityErr := submissionIdentity(remote, entry.InfoHash, intent.Provider, intent.ProviderID); identityErr != nil {
				return nil, identityErr
			}
		}
		if err != nil {
			if definiteSubmissionRejection(err) || errors.Is(err, debridTypes.ErrTerminalProviderTorrent) {
				return nil, m.rejectAcknowledgedSubmission(entry, intent, client, err)
			}
			return nil, pendingSubmission(err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := submissionIdentity(remote, entry.InfoHash, intent.Provider, intent.ProviderID); err != nil {
		return nil, err
	}
	remote.DownloadUncached = intent.DownloadUncached
	remote.Arr = req.Arr
	if err := m.validateResolvedTorrentNames(remote, req.Action); err != nil {
		if !intent.Recovered {
			return nil, m.rejectAcknowledgedSubmission(entry, intent, client, err)
		}
		return nil, pendingSubmission(err)
	}
	if err := m.bindSubmission(entry, intent, remote); err != nil {
		return nil, err
	}
	return remote, nil
}

// submitRoutedCandidate shares normal routing validation with the low-level
// router, while queue-backed callers checkpoint each external-effect boundary.
func (m *Manager) submitRoutedCandidate(ctx context.Context, client debrid.Client, candidate *debridTypes.Torrent, req *ImportRequest, entry *storage.Entry) (*debridTypes.Torrent, error) {
	var intent *storage.ProviderSubmission
	if entry != nil {
		var created bool
		if err := m.submissionStep(entry, func() (err error) {
			intent, created, err = m.storage.BeginProviderSubmission(entry, client.Config().Name, submissionAccount(client.Config()), candidate.DownloadUncached)
			return err
		}); err != nil {
			return nil, err
		}
		if !created {
			return m.reconcileSubmission(ctx, req, entry, intent)
		}
	}
	createCtx := ctx
	if intent != nil {
		createCtx = request.WithMutationAttempt(ctx)
	}
	ack, err := submitProviderMagnet(createCtx, client, candidate)
	if intent != nil {
		id := candidate.Id
		if ack != nil && ack.Id != "" {
			id = ack.Id
		}
		if id != "" {
			if saveErr := m.submissionStep(entry, func() error {
				return m.storage.RecordProviderSubmissionID(entry, intent.Attempt, id, false)
			}); saveErr != nil {
				return nil, saveErr
			}
			intent.ProviderID = id
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if intent != nil {
			return nil, pendingSubmission(ctxErr)
		}
		if ack != nil {
			ctxErr = errors.Join(ctxErr, m.deleteProviderTorrent(client, ack.Id))
		}
		return nil, ctxErr
	}
	if err != nil || ack == nil || ack.Id == "" {
		if err == nil {
			err = fmt.Errorf("provider returned an incomplete submission response")
		}
		if intent != nil {
			if intent.ProviderID != "" || !definiteSubmissionRejection(err) {
				return nil, pendingSubmission(err)
			}
			if saveErr := m.submissionStep(entry, func() error {
				return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionRejected)
			}); saveErr != nil {
				return nil, saveErr
			}
		}
		return nil, err
	}
	if intent != nil {
		if err := submissionIdentity(ack, entry.InfoHash, intent.Provider, intent.ProviderID); err != nil {
			return nil, err
		}
	}
	ack.Arr = req.Arr
	remote, statusErr := checkProviderStatus(ctx, client, ack)
	rollbackID := ack.Id
	if intent == nil && remote != nil && remote.Id != "" {
		rollbackID = remote.Id
	}
	rollback := func(cause error) error {
		if intent != nil {
			if saveErr := m.submissionStep(entry, func() error {
				return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionCleanup)
			}); saveErr != nil {
				return saveErr
			}
		}
		cleanupErr := m.deleteProviderTorrent(client, rollbackID)
		if intent != nil {
			if cleanupErr != nil {
				return pendingSubmission(errors.Join(cause, cleanupErr))
			}
			if saveErr := m.submissionStep(entry, func() error {
				return m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionRejected)
			}); saveErr != nil {
				return saveErr
			}
		}
		return errors.Join(cause, cleanupErr)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if intent != nil {
			return nil, pendingSubmission(ctxErr)
		}
		return nil, rollback(ctxErr)
	}
	if intent != nil && remote != nil {
		if err := submissionIdentity(remote, entry.InfoHash, intent.Provider, intent.ProviderID); err != nil {
			return nil, err
		}
	}
	if statusErr != nil {
		if intent != nil && !definiteSubmissionRejection(statusErr) && !errors.Is(statusErr, debridTypes.ErrTerminalProviderTorrent) {
			return nil, pendingSubmission(statusErr)
		}
		return nil, rollback(statusErr)
	}
	if remote == nil {
		if intent != nil {
			return nil, pendingSubmission(fmt.Errorf("provider returned no status snapshot"))
		}
		return nil, rollback(fmt.Errorf("provider returned nil after checking torrent %s status", ack.Name))
	}
	if err := m.validateResolvedTorrentNames(remote, req.Action); err != nil {
		return nil, rollback(fmt.Errorf("provider returned an unsafe torrent name: %w", err))
	}
	if intent != nil {
		remote.DownloadUncached = intent.DownloadUncached
		if err := m.bindSubmission(entry, intent, remote); err != nil {
			return nil, err
		}
	}
	return remote, nil
}
