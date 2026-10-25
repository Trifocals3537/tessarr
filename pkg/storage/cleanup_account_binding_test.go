package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/workflow"
)

func TestCleanupAdministrativeAccountBindingsAreScopedAndAdditive(t *testing.T) {
	s, i := cleanupFixture(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	main := *i.Entry
	main.Providers = map[string]*ProviderEntry{
		"fixed": {Provider: "fixed", ID: "fixed-id"},
		"late":  {Provider: "late", ID: "late-id"},
	}
	if err := s.AddOrUpdate(&main); err != nil {
		t.Fatal(err)
	}
	offline := errors.New("provider unavailable")
	if err := s.DeleteWithCleanup(main.InfoHash, func(e *Entry) error {
		if err := s.PrepareMainDeletionPlacements(e); err != nil {
			return err
		}
		return offline
	}); !errors.Is(err, offline) {
		t.Fatal(err)
	}
	i, err := s.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "owner", map[string]string{"fixed": "fixed-account", "queue-only": "queue-account"}, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := s.CleanupAdminIntent(i)
	if err != nil || admin == nil {
		t.Fatal("missing admin snapshot", err)
	}
	valid := map[string]string{"fixed": "fixed-account", "late": "late-account"}
	for _, tc := range []struct {
		name     string
		intent   *QueueDeletionIntent
		snapshot *CleanupAdminIntent
		accounts map[string]string
		want     error
	}{
		{"extra provider", i, admin, map[string]string{"fixed": "fixed-account", "late": "late-account", "unrelated": "other"}, workflow.ErrClaim},
		{"replace existing", i, admin, map[string]string{"fixed": "replacement", "late": "late-account"}, ErrCleanupAccountConflict},
		{"missing late provider", i, admin, map[string]string{"fixed": "fixed-account"}, ErrCleanupAccountConflict},
		{"wrong snapshot", i, &CleanupAdminIntent{Entry: admin.Entry, snapshot: []byte("different-scope")}, valid, workflow.ErrClaim},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if bound, err := s.BindCleanupAdminAccounts(tc.intent, tc.snapshot, tc.accounts, time.Now()); !errors.Is(err, tc.want) || bound != nil {
				t.Fatal("invalid binding accepted", bound, err)
			}
			rows, err := s.QueuedDeletionIntents()
			if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != "" || rows[0].Work.Accounts["fixed"] != "fixed-account" || rows[0].Work.Accounts["queue-only"] != "queue-account" {
				t.Fatal("failed batch changed account restrictions", err)
			}
		})
	}
	stale := *i
	staleWork := *i.Work
	staleWork.Token = "obsolete-token"
	stale.Work = &staleWork
	if _, err := s.BindCleanupAdminAccounts(&stale, admin, valid, time.Now()); !errors.Is(err, workflow.ErrClaim) {
		t.Fatal("stale claim bound a new account", err)
	}
	replacement := *i
	replacement.QueueIncarnation = "different-incarnation"
	if _, err := s.BindCleanupAdminAccounts(&replacement, admin, valid, time.Now()); !errors.Is(err, ErrQueuedDeletionIdentityMismatch) {
		t.Fatal("replacement incarnation bound accounts", err)
	}
	s.mainCleanupSyncForTest = func() error { return offline }
	if bound, err := s.BindCleanupAdminAccounts(i, admin, valid, time.Now()); !errors.Is(err, offline) || bound != nil {
		t.Fatal("failed durability gate granted a binding", bound, err)
	}
	s.mainCleanupSyncForTest = nil
	bound, err := s.BindCleanupAdminAccounts(i, admin, valid, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if bound.Accounts["late"] != "late-account" || bound.Accounts["fixed"] != "fixed-account" || bound.Accounts["queue-only"] != "queue-account" || bound.Token != i.Work.Token || bound.Revision <= i.Work.Revision {
		t.Fatal("binding changed an existing restriction or claim")
	}
	if i.Work.Accounts["late"] != "" {
		t.Fatal("binding mutated the caller's uncheckpointed snapshot")
	}
	rows, err := s.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != "late-account" {
		t.Fatal("successful binding was not published", err)
	}
	if _, err := s.BindCleanupAdminAccounts(i, admin, map[string]string{"fixed": "fixed-account", "late": "replacement"}, time.Now()); !errors.Is(err, ErrCleanupAccountConflict) {
		t.Fatal("saved late account was replaced", err)
	}
	if _, err := s.BindCleanupAdminAccounts(i, admin, valid, time.Now()); err != nil {
		t.Fatal("same-account binding could not be repeated", err)
	}
}
