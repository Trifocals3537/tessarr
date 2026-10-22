package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

var ErrProviderPlacementAccount = errors.New("provider placement belongs to a different or unresolved account")

// A placement's account fence outlives its queue row and follows every copy of
// the provider ID. It is a restriction, never a grant of deletion authority.
type providerPlacementAccount struct {
	Version  int    `json:"version"`
	Provider string `json:"provider"`
	ID       string `json:"id"`
	Account  string `json:"account"`
	Conflict bool   `json:"conflict,omitempty"`
}

func placementAccountKey(provider, id string) string {
	return "provider_placement_account/" + hex.EncodeToString([]byte(normalizeMainEntryProvider(provider))) + "/" + hex.EncodeToString([]byte(id))
}

func (s *Storage) placementAccount(provider, id string) (*providerPlacementAccount, error) {
	data, err := s.storageState.Get(placementAccountKey(provider, id))
	if hybrid.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var binding providerPlacementAccount
	if err := json.Unmarshal(data, &binding); err != nil {
		return nil, fmt.Errorf("decode provider placement account: %w", err)
	}
	if binding.Version != 1 || binding.Provider != normalizeMainEntryProvider(provider) || binding.ID != id || binding.Account == "" {
		return nil, fmt.Errorf("invalid provider placement account")
	}
	return &binding, nil
}

// CheckProviderPlacementAccount also protects season children and administrative
// cleanup after queue retirement. Legacy placements without submission evidence
// retain their existing behavior; no current credential is adopted as evidence.
func (s *Storage) CheckProviderPlacementAccount(provider, id, account string) error {
	binding, err := s.placementAccount(provider, id)
	if err != nil || binding == nil {
		return err
	}
	if binding.Conflict || binding.Account != account {
		return ErrProviderPlacementAccount
	}
	return nil
}

// bindPlacementAccount is called under the queue lifecycle gate, before an ID
// can be published. Serialize different hashes sharing a provider ID as well.
func (s *Storage) bindPlacementAccount(provider, id, account string) error {
	s.providerPlacementMu.Lock()
	defer s.providerPlacementMu.Unlock()
	binding, err := s.placementAccount(provider, id)
	if err != nil {
		return err
	}
	if binding != nil {
		if binding.Conflict || binding.Account != account {
			return ErrProviderSubmissionConflict
		}
		// A prior failed sync may have made this restriction readable. Flush it
		// again before treating an identical retry as durably acknowledged.
		return s.storageState.Sync()
	}
	return s.savePlacementAccount(&providerPlacementAccount{Version: 1, Provider: normalizeMainEntryProvider(provider), ID: id, Account: account})
}

func (s *Storage) savePlacementAccount(binding *providerPlacementAccount) error {
	data, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if err := s.storageState.Put(placementAccountKey(binding.Provider, binding.ID), data, nil); err != nil {
		return err
	}
	return s.storageState.Sync()
}

// Earlier submission journals already contain account evidence but did not
// attach it to copied placements. Recover only that recorded evidence before
// startup cleanup can run. Conflicting histories stay fenced for every account.
func (s *Storage) recoverProviderPlacementAccounts() error {
	bindings := make(map[string]*providerPlacementAccount)
	if err := s.storageState.ForEach(func(key string, data []byte) error {
		if !strings.HasPrefix(key, "provider_submission/") {
			return nil
		}
		var intent ProviderSubmission
		if err := json.Unmarshal(data, &intent); err != nil {
			return fmt.Errorf("decode submission account evidence: %w", err)
		}
		if submissionKey(intent.InfoHash) != key {
			return fmt.Errorf("submission account evidence has a different identity")
		}
		verified, err := s.submissionRaw(intent.InfoHash)
		if err != nil {
			return err
		}
		if verified == nil || verified.ProviderID == "" {
			return nil
		}
		bindingKey := placementAccountKey(verified.Provider, verified.ProviderID)
		binding := bindings[bindingKey]
		if binding == nil {
			binding, err = s.placementAccount(verified.Provider, verified.ProviderID)
			if err != nil {
				return err
			}
			if binding == nil {
				binding = &providerPlacementAccount{Version: 1, Provider: normalizeMainEntryProvider(verified.Provider), ID: verified.ProviderID, Account: verified.Account}
			}
			bindings[bindingKey] = binding
		}
		if binding.Account != verified.Account {
			binding.Conflict = true
		}
		return nil
	}); err != nil {
		return err
	}
	for _, binding := range bindings {
		current, err := s.placementAccount(binding.Provider, binding.ID)
		if err != nil {
			return err
		}
		if current != nil && *current == *binding {
			continue
		}
		if err := s.savePlacementAccount(binding); err != nil {
			return err
		}
	}
	return nil
}
