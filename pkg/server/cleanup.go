package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type cleanupView struct {
	Key             string              `json:"key"`
	Incarnation     string              `json:"incarnation"`
	Name            string              `json:"name,omitempty"`
	State           string              `json:"state"`
	Revision        uint64              `json:"revision"`
	Attempts        uint64              `json:"attempts"`
	Reason          string              `json:"reason,omitempty"`
	NextAt          time.Time           `json:"next_at,omitempty"`
	UpdatedAt       time.Time           `json:"updated_at,omitempty"`
	Scope           string              `json:"scope"`
	PreserveFiles   bool                `json:"preserve_files"`
	ProviderPending bool                `json:"provider_pending"`
	SavedPath       string              `json:"saved_path,omitempty"`
	Decisions       []workflow.Decision `json:"decisions,omitempty"`
}

func cleanupRecordView(key, incarnation string, r *workflow.Record) cleanupView {
	v := cleanupView{Key: key, Incarnation: incarnation, State: "pending"}
	if r != nil {
		v.State = string(r.State)
		v.Revision = r.Revision
		v.Attempts = r.Attempt
		v.Reason = r.Reason
		v.NextAt = r.NextAt
		v.UpdatedAt = r.UpdatedAt
		v.Decisions = r.Decisions
	}
	return v
}
func (s *Server) handleCleanupWorkflows(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	intents, receipts, err := s.manager.CleanupWorkflows()
	if err != nil {
		http.Error(w, "Cleanup storage unavailable", http.StatusServiceUnavailable)
		return
	}
	active := []cleanupView{}
	history := []cleanupView{}
	for _, i := range intents {
		if len(active) >= 200 {
			break
		}
		v := cleanupRecordView(i.InfoHash, i.QueueIncarnation, i.Work)
		v.Name = i.Entry.Name
		v.Scope = i.PlacementScope
		v.PreserveFiles = i.PreserveFiles
		v.ProviderPending = i.PlacementCleanupPending
		v.SavedPath = i.Entry.DownloadPath()
		active = append(active, v)
	}
	for _, receipt := range receipts {
		v := cleanupRecordView(receipt.Key, receipt.Incarnation, receipt.Work)
		v.State = "completed"
		if receipt.IntentPresent {
			v.State = "completion_pending"
		}
		history = append(history, v)
	}
	utils.JSONResponse(w, map[string]any{"active": active, "total": len(intents), "history": history}, http.StatusOK)
}
func (s *Server) handleResolveCleanup(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	var input struct {
		Incarnation string `json:"incarnation"`
		Revision    uint64 `json:"revision"`
		RequestID   string `json:"request_id"`
		Action      string `json:"action"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "Invalid resolution request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "Invalid resolution request", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(input.RequestID); err != nil || input.Incarnation == "" || (input.Action != "retry" && input.Action != "reconcile") {
		http.Error(w, "Invalid resolution identity or action", http.StatusBadRequest)
		return
	}
	err := s.manager.ResolveCleanupWorkflow(chi.URLParam(r, "key"), input.Incarnation, input.Revision, input.RequestID, input.Action)
	if err != nil {
		if errors.Is(err, workflow.ErrClaim) || errors.Is(err, storage.ErrQueuedDeletionIdentityMismatch) {
			http.Error(w, "Cleanup changed; refresh before resolving", http.StatusConflict)
			return
		}
		http.Error(w, "Resolution was not checkpointed; refresh service status", http.StatusServiceUnavailable)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "scheduled"}, http.StatusAccepted)
}
