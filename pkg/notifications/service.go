package notifications

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

const (
	defaultQueueCapacity = 64
	defaultWorkerCount   = 2
	deliveryTimeout      = 30 * time.Second
)

type delivery struct {
	notifier Notifier
	event    Event
}

// Service manages and dispatches notifications to all configured notifiers.
// A fixed worker pool keeps slow or unavailable webhooks from creating an
// unbounded number of goroutines in download and repair paths.
type Service struct {
	config    config.Notifications
	notifiers []Notifier
	logger    zerolog.Logger

	mu        sync.RWMutex
	accepting bool
	queue     chan delivery
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	done      chan struct{}
	stopOnce  sync.Once
}

// New creates a new notification service based on the provided configuration.
func New(cfg *config.Notifications, logger zerolog.Logger) *Service {
	configuration := cloneConfig(cfg)
	return newService(
		configuration,
		configuredNotifiers(configuration),
		logger.With().Str("component", "notifications").Logger(),
		defaultWorkerCount,
		defaultQueueCapacity,
	)
}

func newService(
	cfg config.Notifications,
	notifiers []Notifier,
	logger zerolog.Logger,
	workerCount int,
	queueCapacity int,
) *Service {
	if workerCount < 1 {
		workerCount = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:    cfg,
		notifiers: append([]Notifier(nil), notifiers...),
		logger:    logger,
		accepting: true,
		queue:     make(chan delivery, queueCapacity),
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	s.workers.Add(workerCount)
	for range workerCount {
		go s.runWorker()
	}
	go func() {
		s.workers.Wait()
		close(s.done)
	}()
	return s
}

func configuredNotifiers(cfg config.Notifications) []Notifier {
	if !cfg.Enabled {
		return nil
	}

	notifiers := make([]Notifier, 0, 2)
	if cfg.WebhookURL != "" {
		notifiers = append(notifiers, NewDiscord(cfg.WebhookURL))
	}
	if cfg.CallbackURL != "" {
		notifiers = append(notifiers, NewCallback(cfg.CallbackURL))
	}
	return notifiers
}

func cloneConfig(cfg *config.Notifications) config.Notifications {
	if cfg == nil {
		return config.Notifications{}
	}
	cloned := *cfg
	cloned.Events = append([]config.NotificationEvent(nil), cfg.Events...)
	return cloned
}

// Notify admits one delivery per configured notifier without blocking the
// download or repair path. If the bounded queue is saturated, that delivery is
// dropped and recorded instead of spawning more work.
func (s *Service) Notify(event Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.accepting || !s.config.IsEventEnabled(event.Type) {
		return
	}

	event = snapshotEvent(event)
	for _, notifier := range s.notifiers {
		job := delivery{notifier: notifier, event: event}
		select {
		case s.queue <- job:
		default:
			s.logger.Warn().
				Str("notifier", notifier.Name()).
				Str("event", string(event.Type)).
				Int("queue_capacity", cap(s.queue)).
				Msg("Notification queue is full; dropping delivery")
		}
	}
}

// snapshotEvent detaches queued work from mutable manager state. Current
// notifiers only consume these entry fields, so no provider maps or file maps
// need to remain live after Notify returns.
func snapshotEvent(event Event) Event {
	if event.Entry != nil {
		event.Entry = &storage.Entry{
			InfoHash:       event.Entry.InfoHash,
			Name:           event.Entry.Name,
			Category:       event.Entry.Category,
			ActiveProvider: event.Entry.ActiveProvider,
			ContentPath:    event.Entry.ContentPath,
		}
	}
	if event.Error != nil {
		event.Error = errors.New(event.Error.Error())
	}
	return event
}

func (s *Service) runWorker() {
	defer s.workers.Done()
	for job := range s.queue {
		ctx, cancel := context.WithTimeout(s.ctx, deliveryTimeout)
		err := job.notifier.Send(ctx, job.event)
		cancel()

		if err != nil {
			logEvent := s.logger.Error()
			if errors.Is(err, context.Canceled) && s.ctx.Err() != nil {
				logEvent = s.logger.Debug()
			}
			logEvent.
				Err(err).
				Str("notifier", job.notifier.Name()).
				Str("event", string(job.event.Type)).
				Msg("Failed to send notification")
			continue
		}

		s.logger.Trace().
			Str("notifier", job.notifier.Name()).
			Str("event", string(job.event.Type)).
			Msg("Notification sent successfully")
	}
}

// Stop closes admission and drains accepted deliveries until ctx expires. On
// timeout, in-flight HTTP requests are canceled and the bounded remainder is
// discarded by workers using the canceled service context.
func (s *Service) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.accepting = false
		close(s.queue)
		s.mu.Unlock()
	})

	select {
	case <-s.done:
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel()
		return fmt.Errorf("notification delivery did not stop cleanly: %w", ctx.Err())
	}
}

// IsEventEnabled checks if a specific event type is enabled for notifications.
func (s *Service) IsEventEnabled(eventType config.NotificationEvent) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accepting && s.config.IsEventEnabled(eventType)
}

// IsEnabled returns whether notifications are globally enabled.
func (s *Service) IsEnabled() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accepting && s.config.Enabled && len(s.notifiers) > 0
}

// Reload reinitializes notifiers based on the current configuration.
func (s *Service) Reload(cfg *config.Notifications) {
	configuration := cloneConfig(cfg)
	s.mu.Lock()
	s.config = configuration
	s.notifiers = configuredNotifiers(configuration)
	s.mu.Unlock()
}
