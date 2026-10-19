package notifications

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

type testNotifier struct {
	name      string
	started   chan struct{}
	release   <-chan struct{}
	delivered chan Event
	active    atomic.Int32
	maximum   atomic.Int32
}

func (n *testNotifier) Name() string {
	return n.name
}

func (n *testNotifier) Send(ctx context.Context, event Event) error {
	active := n.active.Add(1)
	defer n.active.Add(-1)
	for {
		maximum := n.maximum.Load()
		if active <= maximum || n.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	if n.started != nil {
		select {
		case n.started <- struct{}{}:
		default:
		}
	}
	if n.release != nil {
		select {
		case <-n.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if n.delivered != nil {
		n.delivered <- event
	}
	return nil
}

func enabledNotificationConfig() config.Notifications {
	return config.Notifications{
		Enabled: true,
		Events:  []config.NotificationEvent{config.EventDownloadComplete},
	}
}

func stopNotificationService(t *testing.T, service *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestNotifyUsesBoundedQueue(t *testing.T) {
	release := make(chan struct{})
	notifier := &testNotifier{
		name:      "blocking",
		started:   make(chan struct{}, 1),
		release:   release,
		delivered: make(chan Event, 3),
	}
	service := newService(enabledNotificationConfig(), []Notifier{notifier}, zerolog.Nop(), 1, 1)

	event := Event{Type: config.EventDownloadComplete}
	service.Notify(event)
	select {
	case <-notifier.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first delivery")
	}
	service.Notify(event)
	service.Notify(event)

	close(release)
	stopNotificationService(t, service)
	if got := len(notifier.delivered); got != 2 {
		t.Fatalf("delivered notifications = %d, want 2 (one active and one queued)", got)
	}
	if got := notifier.maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent sends = %d, want 1 worker", got)
	}
}

func TestStopCancelsInFlightDeliveryAndRejectsNewWork(t *testing.T) {
	notifier := &testNotifier{
		name:    "blocking",
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	service := newService(enabledNotificationConfig(), []Notifier{notifier}, zerolog.Nop(), 1, 4)
	service.Notify(Event{Type: config.EventDownloadComplete})
	select {
	case <-notifier.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start delivery")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := service.Stop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want deadline exceeded", err)
	}
	select {
	case <-service.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after service cancellation")
	}

	service.Notify(Event{Type: config.EventDownloadComplete})
	select {
	case <-notifier.started:
		t.Fatal("notification was admitted after Stop")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestStopDrainsAcceptedDeliveries(t *testing.T) {
	notifier := &testNotifier{
		name:      "recording",
		delivered: make(chan Event, 4),
	}
	service := newService(enabledNotificationConfig(), []Notifier{notifier}, zerolog.Nop(), 1, 4)
	for range 3 {
		service.Notify(Event{Type: config.EventDownloadComplete})
	}

	stopNotificationService(t, service)
	if got := len(notifier.delivered); got != 3 {
		t.Fatalf("delivered notifications = %d, want 3", got)
	}
}

func TestNotifySnapshotsMutableEntry(t *testing.T) {
	release := make(chan struct{})
	notifier := &testNotifier{
		name:      "snapshot",
		started:   make(chan struct{}, 1),
		release:   release,
		delivered: make(chan Event, 1),
	}
	service := newService(enabledNotificationConfig(), []Notifier{notifier}, zerolog.Nop(), 1, 1)
	entry := &storage.Entry{
		InfoHash:       "original-hash",
		Name:           "original-name",
		Category:       "original-category",
		ActiveProvider: "original-provider",
		ContentPath:    "/original/path",
	}
	service.Notify(Event{Type: config.EventDownloadComplete, Entry: entry})
	select {
	case <-notifier.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start delivery")
	}

	entry.InfoHash = "mutated-hash"
	entry.Name = "mutated-name"
	entry.Category = "mutated-category"
	entry.ActiveProvider = "mutated-provider"
	entry.ContentPath = "/mutated/path"
	close(release)
	stopNotificationService(t, service)

	delivered := <-notifier.delivered
	if delivered.Entry == nil {
		t.Fatal("delivered entry snapshot is nil")
	}
	if delivered.Entry.InfoHash != "original-hash" ||
		delivered.Entry.Name != "original-name" ||
		delivered.Entry.Category != "original-category" ||
		delivered.Entry.ActiveProvider != "original-provider" ||
		delivered.Entry.ContentPath != "/original/path" {
		t.Fatalf("delivered mutable entry instead of snapshot: %#v", delivered.Entry)
	}
}

func TestConcurrentNotifyAndStopIsSafe(t *testing.T) {
	notifier := &testNotifier{name: "concurrent"}
	service := newService(enabledNotificationConfig(), []Notifier{notifier}, zerolog.Nop(), 2, 8)

	start := make(chan struct{})
	var callers sync.WaitGroup
	callers.Add(32)
	for range 32 {
		go func() {
			defer callers.Done()
			<-start
			for range 100 {
				service.Notify(Event{Type: config.EventDownloadComplete})
			}
		}()
	}
	close(start)
	stopNotificationService(t, service)
	callers.Wait()
}

func TestHTTPNotifiersHonorRequestCancellation(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func(string) Notifier
	}{
		{name: "callback", new: func(url string) Notifier { return NewCallback(url) }},
		{name: "discord", new: func(url string) Notifier { return NewDiscord(url) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				close(started)
				<-release
			}))
			defer server.Close()
			defer close(release)

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				result <- test.new(server.URL).Send(ctx, Event{Type: config.EventDownloadComplete})
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("notifier request did not reach test server")
			}
			cancel()

			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Send() error = %v, want context canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("notifier request ignored cancellation")
			}
		})
	}
}
