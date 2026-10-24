package workflow

import (
	"errors"
	"fmt"
	"time"
)

var ErrClaim = errors.New("workflow claim or revision changed")
var ErrNotDue = errors.New("workflow is not due")

type State string

const (
	Pending   State = "pending"
	Running   State = "running"
	RetryWait State = "retry_wait"
	Blocked   State = "blocked"
)

type Decision struct {
	ID     string    `json:"id"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
}

// Record is embedded in the application's authoritative intent. Claims never
// expire while their process is alive: a slow external call must not overlap a
// replacement worker. Reclaim is allowed only during exclusive startup.
type Record struct {
	Version       int               `json:"version"`
	Revision      uint64            `json:"revision"`
	State         State             `json:"state"`
	Attempt       uint64            `json:"attempt"`
	Owner         string            `json:"owner,omitempty"`
	Token         string            `json:"token,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	NextAt        time.Time         `json:"next_at,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	Mode          string            `json:"mode,omitempty"`
	EffectStarted bool              `json:"effect_started,omitempty"`
	Accounts      map[string]string `json:"accounts,omitempty"`
	Decisions     []Decision        `json:"decisions,omitempty"`
}

func (r *Record) Validate() error {
	if r == nil {
		return nil
	}
	if r.Version != 1 || r.Revision == 0 {
		return fmt.Errorf("unsupported workflow record")
	}
	switch r.State {
	case Pending, Running, RetryWait, Blocked:
	default:
		return fmt.Errorf("unknown workflow state")
	}
	if (r.State == Running) != (r.Owner != "" && r.Token != "") {
		return fmt.Errorf("invalid workflow claim")
	}
	if r.State != Running && (r.Owner != "" || r.Token != "") {
		return fmt.Errorf("released workflow retains claim")
	}
	if len(r.Decisions) > 32 {
		return fmt.Errorf("workflow decision history exceeds limit")
	}
	if r.Mode != "" && r.Mode != "reconcile" {
		return fmt.Errorf("unknown workflow mode")
	}
	return nil
}

func NewRecord(now time.Time) *Record {
	return &Record{Version: 1, Revision: 1, State: Pending, CreatedAt: now, UpdatedAt: now}
}

func (r *Record) Claim(owner, token string, now time.Time, restart bool) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if owner == "" || token == "" {
		return ErrClaim
	}
	if r.State == Running {
		if !restart || r.Owner == owner {
			return ErrClaim
		}
		// The effect flag remains set so a crash cannot turn uncertainty into
		// permission to repeat an external mutation.
	} else if r.State == Blocked || (!r.NextAt.IsZero() && now.Before(r.NextAt)) {
		return ErrNotDue
	}
	r.State = Running
	r.Owner = owner
	r.Token = token
	r.Attempt++
	r.Revision++
	r.UpdatedAt = now
	return nil
}

func (r *Record) Check(token string) error {
	if r == nil || r.State != Running || r.Token == "" || r.Token != token {
		return ErrClaim
	}
	return nil
}
func (r *Record) Release(token, reason string, blocked bool, now time.Time) error {
	if err := r.Check(token); err != nil {
		return err
	}
	r.Owner = ""
	r.Token = ""
	r.Reason = reason
	r.UpdatedAt = now
	r.Revision++
	r.State = RetryWait
	delay := 5 * time.Second
	for n := uint64(1); n < r.Attempt && delay < 15*time.Minute; n++ {
		delay *= 2
	}
	if delay > 15*time.Minute {
		delay = 15 * time.Minute
	}
	r.NextAt = now.Add(delay)
	if blocked || r.Attempt >= 10 {
		r.State = Blocked
		r.NextAt = time.Time{}
	}
	return nil
}

func (r *Record) Resolve(revision uint64, id, action string, now time.Time) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if id == "" || len(id) > 128 {
		return fmt.Errorf("resolution ID is required")
	}
	for _, d := range r.Decisions {
		if d.ID == id {
			if d.Action == action {
				return nil
			}
			return ErrClaim
		}
	}
	if r.Revision != revision || r.State == Running {
		return ErrClaim
	}
	if action != "retry" && action != "reconcile" {
		return fmt.Errorf("unsupported resolution action")
	}
	if len(r.Decisions) >= 32 {
		return fmt.Errorf("workflow decision history is full")
	}
	r.Decisions = append(r.Decisions, Decision{ID: id, Action: action, At: now})
	r.Mode = ""
	if action == "reconcile" {
		r.Mode = action
	}
	// Retry preserves uncertainty. Only read-only reconciliation can acknowledge
	// an earlier provider effect; operator input cannot grant wider deletion.
	r.State = Pending
	r.NextAt = time.Time{}
	r.Reason = "operator_requested"
	r.Revision++
	r.UpdatedAt = now
	return nil
}
