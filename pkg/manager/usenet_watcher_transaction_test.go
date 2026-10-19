package manager

import (
	"errors"
	"testing"
)

func TestRunWatchedNZBTransactionSubmitsThenAcceptsDurableState(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	state := watchedNZBReconciliationAbsent
	submits := 0
	accepts := 0
	cleanups := 0

	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return state, nil
		},
		func() (string, error) {
			submits++
			state = watchedNZBReconciliationDurable
			return identity.ID, nil
		},
		func() error { return nil },
		func() (string, error) {
			accepts++
			return "release.nzb.accepted", nil
		},
		func(path string) {
			if path != "release.nzb.accepted" {
				t.Fatalf("cleanup path = %q", path)
			}
			cleanups++
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if submits != 1 || accepts != 1 || cleanups != 1 {
		t.Fatalf("transaction calls: submit=%d accept=%d cleanup=%d", submits, accepts, cleanups)
	}
}

func TestRunWatchedNZBTransactionDoesNotResubmitDurableDuplicate(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	submits := 0
	accepts := 0

	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return watchedNZBReconciliationDurable, nil
		},
		func() (string, error) {
			submits++
			return "", nil
		},
		func() error { return nil },
		func() (string, error) {
			accepts++
			return "release.nzb.accepted", nil
		},
		func(string) {},
	)
	if err != nil {
		t.Fatal(err)
	}
	if submits != 0 || accepts != 1 {
		t.Fatalf("duplicate calls: submit=%d accept=%d", submits, accepts)
	}
}

func TestRunWatchedNZBTransactionRecoversAmbiguousSubmitOutcome(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	state := watchedNZBReconciliationAbsent
	submitFailure := errors.New("queue write response lost")

	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return state, nil
		},
		func() (string, error) {
			state = watchedNZBReconciliationDurable
			return identity.ID, submitFailure
		},
		func() error { return nil },
		func() (string, error) {
			return "release.nzb.accepted", nil
		},
		func(string) {},
	)
	if err != nil {
		t.Fatalf("durably reconciled ambiguous submit = %v", err)
	}
}

func TestRunWatchedNZBTransactionFailsClosedBeforeSubmission(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	ambiguous := ambiguousWatchedNZBError(errors.New("corrupt row"))
	submitted := false
	accepted := false

	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return watchedNZBReconciliationAbsent, ambiguous
		},
		func() (string, error) {
			submitted = true
			return "", nil
		},
		func() error { return nil },
		func() (string, error) {
			accepted = true
			return "", nil
		},
		func(string) {},
	)
	if !errors.Is(err, errWatchedNZBStateAmbiguous) {
		t.Fatalf("ambiguous transaction error = %v", err)
	}
	if submitted || accepted {
		t.Fatalf("ambiguous transaction submitted=%v accepted=%v", submitted, accepted)
	}
}

func TestRunWatchedNZBTransactionRetainsTerminalStateWhenAcceptFails(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	acceptFailure := errors.New("directory sync failed")
	cleaned := false

	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return watchedNZBReconciliationDurable, nil
		},
		func() (string, error) { return "", nil },
		func() error { return nil },
		func() (string, error) { return "release.nzb.accepted", acceptFailure },
		func(string) { cleaned = true },
	)
	if !errors.Is(err, acceptFailure) {
		t.Fatalf("accept failure = %v", err)
	}
	if cleaned {
		t.Fatal("terminal cleanup ran after failed acceptance")
	}
}

func TestRunWatchedNZBTransactionRejectsSuccessfulSubmissionWithoutExactID(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	accepted := false
	err := runWatchedNZBTransaction(
		identity,
		func() (watchedNZBReconciliationState, error) {
			return watchedNZBReconciliationAbsent, nil
		},
		func() (string, error) {
			return "", nil
		},
		func() error { return nil },
		func() (string, error) {
			accepted = true
			return "", nil
		},
		func(string) {},
	)
	if !errors.Is(err, errWatchedNZBStateAmbiguous) {
		t.Fatalf("missing submission ID error = %v", err)
	}
	if accepted {
		t.Fatal("transaction accepted source after missing submission ID")
	}
}

func TestRunWatchedNZBTransactionRetainsSourceUntilDurabilityBarrierSucceeds(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	state := watchedNZBReconciliationAbsent
	syncFailure := errors.New("injected queue fsync failure")
	barrierFails := true
	submits := 0
	accepts := 0

	run := func() error {
		return runWatchedNZBTransaction(
			identity,
			func() (watchedNZBReconciliationState, error) {
				return state, nil
			},
			func() (string, error) {
				submits++
				state = watchedNZBReconciliationDurable
				return identity.ID, syncFailure
			},
			func() error {
				if barrierFails {
					return syncFailure
				}
				return nil
			},
			func() (string, error) {
				accepts++
				return "release.nzb.accepted", nil
			},
			func(string) {},
		)
	}

	if err := run(); !errors.Is(err, syncFailure) {
		t.Fatalf("durability barrier failure = %v, want injected sync failure", err)
	}
	if submits != 1 || accepts != 0 {
		t.Fatalf("failed barrier calls: submit=%d accept=%d", submits, accepts)
	}

	// The visible deterministic row is reconciled on retry, but still cannot
	// authorize acceptance until a real flush succeeds. No second submit occurs.
	barrierFails = false
	if err := run(); err != nil {
		t.Fatalf("retry after successful durability barrier: %v", err)
	}
	if submits != 1 || accepts != 1 {
		t.Fatalf("recovered barrier calls: submit=%d accept=%d", submits, accepts)
	}
}
