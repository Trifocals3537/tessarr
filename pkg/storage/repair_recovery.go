package storage

import (
	"errors"
	"sort"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	json "github.com/bytedance/sonic"
)

type RecoveryState string

const (
	RecoveryPending   RecoveryState = "pending"
	RecoveryWaiting   RecoveryState = "waiting_for_import"
	RecoveryAttention RecoveryState = "needs_attention"
	RecoveryComplete  RecoveryState = "import_confirmed"
)

// RepairRecovery survives both run-history pruning and Arr library deletion.
// It stores identities and receipts, never API tokens or response bodies.
type RepairRecovery struct {
	ID               string         `json:"id"`
	EndpointBinding  string         `json:"endpoint_binding"`
	Original         BrokenFile     `json:"original"`
	File             arr.RepairFile `json:"file"`
	Prepared         bool           `json:"prepared"`
	Deleted          bool           `json:"deleted"`
	HistoryID        int            `json:"history_id,omitempty"`
	FailureIntentAt  time.Time      `json:"failure_intent_at,omitempty"`
	FailureConfirmed bool           `json:"failure_confirmed"`
	AutoSearch       bool           `json:"auto_search"`
	SearchIntentAt   time.Time      `json:"search_intent_at,omitempty"`
	SearchReceiptID  int            `json:"search_receipt_id,omitempty"`
	State            RecoveryState  `json:"state"`
	ErrorCode        string         `json:"error_code,omitempty"`
	LastRunID        string         `json:"last_run_id,omitempty"`
	NextCheckAt      time.Time      `json:"next_check_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

func (s *Storage) SaveRepairRecovery(job *RepairRecovery) error {
	if job == nil || job.ID == "" || job.Original.ArrName == "" || job.Original.ArrFileID <= 0 {
		return errors.New("repair recovery identity is incomplete")
	}
	job.UpdatedAt = time.Now()
	// Probe diagnostic text is not needed for identity and could contain a
	// provider URL or response. Recovery persists only its own fixed phase codes.
	job.Original.Reason = ""
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	// This store uses SyncInterval=0: a failed sync is a failed save, and no
	// subsequent external mutation is allowed by the caller.
	return s.repairRecoveries.Put(job.ID, data, &hybrid.EntryMeta{
		Status: string(job.State), Name: job.Original.EntryName, AddedOn: job.UpdatedAt.Unix(),
	})
}

func (s *Storage) GetRepairRecovery(id string) (*RepairRecovery, error) {
	data, err := s.repairRecoveries.Get(id)
	if hybrid.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var job RepairRecovery
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	if job.ID != id {
		return nil, errors.New("repair recovery identity is corrupt")
	}
	return &job, nil
}

// PendingRepairRecoveryIDs uses the metadata index so replay doesn't retain all
// job bodies. Oldest-updated work goes first; callers bound each replay pass.
func (s *Storage) PendingRepairRecoveryIDs(names []string) ([]string, error) {
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		wanted[name] = true
	}
	type entry struct {
		id      string
		updated int64
	}
	var entries []entry
	err := s.repairRecoveries.ForEachMeta(func(id string, meta *hybrid.IndexEntry) error {
		if meta.Status != string(RecoveryComplete) && (len(wanted) == 0 || wanted[meta.Name]) {
			entries = append(entries, entry{id, meta.AddedOn})
		}
		return nil
	})
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].updated == entries[j].updated {
			return entries[i].id < entries[j].id
		}
		return entries[i].updated < entries[j].updated
	})
	ids := make([]string, len(entries))
	for i := range entries {
		ids[i] = entries[i].id
	}
	return ids, err
}

func (s *Storage) RepairRecoveryCounts() (map[RecoveryState]int, error) {
	counts := make(map[RecoveryState]int)
	err := s.repairRecoveries.ForEachMeta(func(_ string, meta *hybrid.IndexEntry) error {
		counts[RecoveryState(meta.Status)]++
		return nil
	})
	return counts, err
}

// Only completed receipts expire. Unfinished intent is never discarded just
// because a run finished, health was cleared, or the Arr record disappeared.
func (s *Storage) PruneRepairRecoveries(before time.Time) error {
	var ids []string
	err := s.repairRecoveries.ForEachMeta(func(id string, meta *hybrid.IndexEntry) error {
		if meta.Status == string(RecoveryComplete) && meta.AddedOn < before.Unix() {
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.repairRecoveries.Delete(id); err != nil {
			return err
		}
	}
	return nil
}
