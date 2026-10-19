package tessarr

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

type serviceFunc func(context.Context) error

func (f serviceFunc) Start(ctx context.Context) error {
	return f(ctx)
}

func TestStartServicesPropagatesErrorAndCancelsPeer(t *testing.T) {
	expected := errors.New("address already in use")
	peerCanceled := make(chan struct{})

	managerService := serviceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		close(peerCanceled)
		return nil
	})
	httpService := serviceFunc(func(context.Context) error {
		return expected
	})

	err := startServices(context.Background(), managerService, httpService)
	if !errors.Is(err, expected) {
		t.Fatalf("startServices() error = %v, want %v", err, expected)
	}

	select {
	case <-peerCanceled:
	default:
		t.Fatal("peer service was not canceled after service failure")
	}
}

func TestRestartCommitWaitsForHTTPAndManagerReadiness(t *testing.T) {
	managerReady := make(chan struct{})
	httpReady := make(chan struct{})
	serviceStopped := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		result <- waitForServiceReadiness(context.Background(), managerReady, httpReady, serviceStopped)
	}()
	close(managerReady)
	select {
	case <-result:
		t.Fatal("restart committed before HTTP listener bound")
	case <-time.After(25 * time.Millisecond):
	}
	close(serviceStopped)
	if <-result {
		t.Fatal("restart committed after HTTP service failed")
	}

	managerReady = make(chan struct{})
	httpReady = make(chan struct{})
	serviceStopped = make(chan struct{})
	close(managerReady)
	close(httpReady)
	if !waitForServiceReadiness(context.Background(), managerReady, httpReady, serviceStopped) {
		t.Fatal("healthy manager and HTTP listener did not permit restart commit")
	}
}

func TestStartServicesWaitsForServiceShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)

	managerService := serviceFunc(func(context.Context) error {
		return nil
	})
	httpService := serviceFunc(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		<-release
		return nil
	})

	go func() {
		result <- startServices(ctx, managerService, httpService)
	}()

	<-started
	cancel()

	select {
	case err := <-result:
		t.Fatalf("startServices() returned before service cleanup completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("startServices() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("startServices() did not return after service cleanup completed")
	}
}

func TestStartServicesConvertsPanicToError(t *testing.T) {
	managerService := serviceFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	})
	httpService := serviceFunc(func(context.Context) error {
		panic("boom")
	})

	err := startServices(context.Background(), managerService, httpService)
	if err == nil {
		t.Fatal("startServices() error = nil, want panic error")
	}
	if !strings.Contains(err.Error(), "HTTP service panicked: boom") {
		t.Fatalf("startServices() error = %q, want named panic", err)
	}
}

func TestStartServicesRejectsUnexpectedCleanExit(t *testing.T) {
	service := serviceFunc(func(context.Context) error {
		return nil
	})

	err := startServices(context.Background(), service, service)
	if !errors.Is(err, errServicesStopped) {
		t.Fatalf("startServices() error = %v, want %v", err, errServicesStopped)
	}
}

func TestValidateDeploymentConfigFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Config
		wantErr bool
	}{
		{
			name: "private subnet is protected",
			cfg: config.Config{
				BindAddress:        "192.0.2.10",
				UseAuth:            true,
				DisableWebDav:      true,
				AllowedClientCIDRs: []string{"192.0.2.10/32"},
			},
		},
		{
			name: "private subnet without authentication",
			cfg: config.Config{
				BindAddress:   "192.0.2.10",
				DisableWebDav: true,
			},
			wantErr: true,
		},
		{
			name: "invalid allowlist",
			cfg: config.Config{
				BindAddress:        "127.0.0.1",
				AllowedClientCIDRs: []string{"invalid"},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDeploymentConfig(&test.cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf(
					"validateDeploymentConfig() error = %v, wantErr %t",
					err,
					test.wantErr,
				)
			}
			if test.wantErr &&
				!strings.Contains(err.Error(), "deployment safety check") {
				t.Fatalf("error = %q, want deployment safety context", err)
			}
		})
	}
}

func TestFinishRestartDoesNotResetAfterServiceStopError(t *testing.T) {
	expected := errors.New("graceful shutdown deadline exceeded")
	serviceDone := make(chan error, 1)
	serviceDone <- expected

	resetCalled := false
	shutdownCalled := false
	restarted, err := finishRestart(
		context.Background(),
		serviceDone,
		func() error {
			resetCalled = true
			return nil
		},
		func() { shutdownCalled = true },
	)

	if !errors.Is(err, expected) {
		t.Fatalf("finishRestart() error = %v, want %v", err, expected)
	}
	if restarted {
		t.Fatal("finishRestart() restarted after a failed service stop")
	}
	if resetCalled {
		t.Fatal("finishRestart() reset storage after a failed service stop")
	}
	if shutdownCalled {
		t.Fatal("finishRestart() ran normal shutdown cleanup after a failed service stop")
	}
}

func TestFinishRestartResetsOnlyAfterCleanServiceStop(t *testing.T) {
	serviceDone := make(chan error, 1)
	serviceDone <- nil

	resetCalled := false
	restarted, err := finishRestart(
		context.Background(),
		serviceDone,
		func() error {
			resetCalled = true
			return nil
		},
		func() { t.Fatal("finishRestart() unexpectedly ran shutdown cleanup") },
	)

	if err != nil {
		t.Fatalf("finishRestart() error = %v, want nil", err)
	}
	if !restarted {
		t.Fatal("finishRestart() did not restart after a clean service stop")
	}
	if !resetCalled {
		t.Fatal("finishRestart() did not reset storage after a clean service stop")
	}
}

func TestFinishRestartShutsDownWhenParentWasCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serviceDone := make(chan error, 1)
	serviceDone <- nil

	shutdownCalled := false
	restarted, err := finishRestart(
		ctx,
		serviceDone,
		func() error {
			t.Fatal("finishRestart() reset storage after parent cancellation")
			return nil
		},
		func() { shutdownCalled = true },
	)

	if err != nil {
		t.Fatalf("finishRestart() error = %v, want nil", err)
	}
	if restarted {
		t.Fatal("finishRestart() restarted after parent cancellation")
	}
	if !shutdownCalled {
		t.Fatal("finishRestart() did not run shutdown cleanup after parent cancellation")
	}
}

func TestFinishRestartPropagatesResetFailure(t *testing.T) {
	serviceDone := make(chan error, 1)
	serviceDone <- nil
	expected := errors.New("manager drain timed out")

	restarted, err := finishRestart(
		context.Background(),
		serviceDone,
		func() error { return expected },
		func() { t.Fatal("finishRestart() unexpectedly ran shutdown cleanup") },
	)

	if !errors.Is(err, expected) {
		t.Fatalf("finishRestart() error = %v, want %v", err, expected)
	}
	if restarted {
		t.Fatal("finishRestart() restarted after reset failed")
	}
}
