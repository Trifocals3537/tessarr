package manager

import (
	"context"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/go-co-op/gocron/v2"
)

// runInitialCalls performs any initial calls of worker functions
// for example, call the processQueuedEntries function once
func (m *Manager) runInitialCalls(ctx context.Context) {
	m.startBackground("initial download-link refresh", func() {
		m.refreshDownloadLinks(ctx)
	})
	m.startBackground("initial queue processing", func() {
		m.processQueuedEntries(ctx)
	})
	m.startBackground("initial account sync", func() {
		m.syncAccounts(ctx)
	})
	m.startBackground("initial migration source cleanup recovery", func() {
		if err := m.processMigrationCleanups(ctx); err != nil && ctx.Err() == nil {
			m.logger.Warn().Err(err).Msg("Initial migration source cleanup recovery was incomplete")
		}
	})
}

// awaitProviderCall makes cancellation observable around legacy provider APIs
// that do not accept a context. The detached goroutine deliberately captures
// only the provider method and a buffered result channel: it cannot touch
// manager storage or mount state after its caller has been canceled.
func awaitProviderCall(ctx context.Context, call func() error) error {
	result := make(chan error, 1)
	go func() {
		result <- call()
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func submitProviderMagnet(ctx context.Context, client debrid.Client, torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return torrent, err
	}
	if contextual, ok := client.(debrid.ContextMagnetSubmitter); ok {
		return contextual.SubmitMagnetContext(ctx, torrent)
	}
	result, err := client.SubmitMagnet(torrent)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	return result, err
}

func checkProviderStatus(ctx context.Context, client debrid.Client, torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return torrent, err
	}
	if contextual, ok := client.(debrid.ContextStatusChecker); ok {
		return contextual.CheckStatusContext(ctx, torrent)
	}
	result, err := client.CheckStatus(torrent)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return result, ctxErr
	}
	return result, err
}

type freshStatusChecker interface {
	CheckStatusFreshContext(context.Context, *debridTypes.Torrent) (*debridTypes.Torrent, error)
}

func checkFreshProviderStatus(ctx context.Context, client debrid.Client, torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	checker, ok := client.(freshStatusChecker)
	if !ok {
		// Other providers perform a direct status request here. TorBox implements
		// the stronger capability above to bypass its documented list cache.
		return checkProviderStatus(ctx, client, torrent)
	}
	return checker.CheckStatusFreshContext(ctx, torrent)
}

func syncProviderAccounts(ctx context.Context, client debrid.Client) error {
	if contextual, ok := client.(debrid.ContextAccountSyncer); ok {
		return contextual.SyncAccountsContext(ctx)
	}
	return awaitProviderCall(ctx, func() error {
		client.SyncAccounts()
		return nil
	})
}

func refreshProviderDownloadLinks(ctx context.Context, client debrid.Client) error {
	if contextual, ok := client.(debrid.ContextDownloadLinkRefresher); ok {
		return contextual.RefreshDownloadLinksContext(ctx)
	}
	return awaitProviderCall(ctx, client.RefreshDownloadLinks)
}

func (m *Manager) syncDebridAccounts(ctx context.Context, debridName string, client debrid.Client) {
	if err := syncProviderAccounts(ctx, client); err != nil && ctx.Err() == nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to sync accounts")
	}
}

func (m *Manager) syncAccounts(ctx context.Context) {
	// Sync accounts for all debrids
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if ctx.Err() != nil {
			return false
		}
		if debridClient == nil {
			return true
		}
		m.syncDebridAccounts(ctx, debridName, debridClient)
		return ctx.Err() == nil
	})
}

func (m *Manager) refreshDownloadLinks(ctx context.Context) {
	// Refresh download links for all debrids
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if ctx.Err() != nil {
			return false
		}
		if debridClient == nil {
			return true
		}
		m.refreshDebridDownloadLinks(ctx, debridName, debridClient)
		return ctx.Err() == nil
	})
}

func (m *Manager) addQueueProcessorJob(ctx context.Context) error {
	// This function is responsible for starting queue processing scheduled tasks

	if jd, err := utils.ConvertToJobDef(m.config.RefreshInterval); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert queue processing interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			m.processQueuedEntries(ctx)
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create slots tracking job")
		} else {
			m.logger.Debug().Msgf("Queue processing job scheduled for every %s", m.config.RefreshInterval)
		}
	}

	if m.config.RemoveStalledAfter != "" {
		// Stalled torrents removal job
		if jd, err := utils.ConvertToJobDef("1m"); err != nil {
			m.logger.Error().Err(err).Msg("Failed to convert remove stalled torrents interval to job definition")
		} else {
			// Schedule the job
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				err := m.queue.DeleteStalled()
				if err != nil {
					m.logger.Error().Err(err).Msg("Failed to process remove stalled torrents")
				}
			}), gocron.WithContext(ctx)); err != nil {
				m.logger.Error().Err(err).Msg("Failed to create remove stalled torrents job")
			} else {
				m.logger.Debug().Msgf("Remove stalled torrents job scheduled for every %s", "1m")
			}
		}
	}

	// Recover source placements whose target migration committed but whose
	// provider deletion or local checkpoint was interrupted. Singleton mode
	// keeps a slow provider call from causing overlapping sweeps.
	if jd, err := utils.ConvertToJobDef(migrationCleanupSweepInterval.String()); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert migration cleanup interval to job definition")
	} else {
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			if err := m.processMigrationCleanups(ctx); err != nil && ctx.Err() == nil {
				m.logger.Warn().Err(err).Msg("Migration source cleanup sweep was incomplete")
			}
		}),
			gocron.WithContext(ctx),
			gocron.WithName("migration-source-cleanup"),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create migration source cleanup job")
		} else {
			m.logger.Debug().
				Dur("interval", migrationCleanupSweepInterval).
				Msg("Migration source cleanup recovery scheduled")
		}
	}

	// NZB refresh job for pending archives (every 5 minutes)
	if m.usenet != nil {
		if jd, err := utils.ConvertToJobDef("10m"); err != nil {
			m.logger.Error().Err(err).Msg("Failed to convert NZB refresh interval to job definition")
		} else {
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				if err := m.syncNZBs(ctx); err != nil {
					m.logger.Error().Err(err).Msg("Failed to refresh NZBs")
				}
			}), gocron.WithContext(ctx), gocron.WithName("nzb-refresh")); err != nil {
				m.logger.Error().Err(err).Msg("Failed to create NZB refresh job")
			} else {
				m.logger.Debug().Msg("NZB refresh job scheduled for every 5m")
			}
		}
	}
	return nil
}

func (m *Manager) StartWorker(ctx context.Context) error {
	// Stop any existing jobs before starting new ones
	m.scheduler.RemoveByTags(managerSchedulerTag)

	// Call the initial calls
	m.runInitialCalls(ctx)

	if err := m.addQueueProcessorJob(ctx); err != nil {
		return err
	}
	// Schedule per-debrid refresh jobs
	m.clients.Range(func(debridName string, debridClient debrid.Client) bool {
		if debridClient == nil {
			return true
		}

		debridConfig := debridClient.Config()

		// Schedule download link refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.DownloadLinksRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert download link refresh interval to job definition")
		} else {
			jobName := debridName + "-download-links"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				m.refreshDebridDownloadLinks(ctx, debridName, debridClient)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create download link refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Download link refresh job scheduled for every %s", debridConfig.DownloadLinksRefreshInterval)
			}
		}

		// Schedule torrent refresh job for this debrid
		if jd, err := utils.ConvertToJobDef(debridConfig.TorrentsRefreshInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert torrent refresh interval to job definition")
		} else {
			jobName := debridName + "-torrents"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				if err := m.refreshTorrents(ctx, debridName, debridClient); err != nil {
					m.logger.Error().Err(err).Str("debrid", debridName).Msg("Torrent refresh failed")
				}
				m.RefreshEntries(true)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create torrent refresh job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Torrent refresh job scheduled for every %s", debridConfig.TorrentsRefreshInterval)
			}
		}

		// Schedule account syncTorrents job for this debrid
		if jd, err := utils.ConvertToJobDef(config.DefaultAccountSyncInterval); err != nil {
			m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to convert account syncTorrents interval to job definition")
		} else {
			jobName := debridName + "-account-syncTorrents"
			if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
				m.syncDebridAccounts(ctx, debridName, debridClient)
			}), gocron.WithContext(ctx), gocron.WithName(jobName)); err != nil {
				m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to create account syncTorrents job")
			} else {
				m.logger.Debug().Str("debrid", debridName).Msgf("Account syncTorrents job scheduled for every %s", config.DefaultAccountSyncInterval)
			}
		}

		return true
	})

	// Schedule the reset invalid links job
	// This job will run every at 00:00 CET
	// and reset the invalid links in the cache
	if jd, err := utils.ConvertToJobDef("00:00"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert link reset interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.cetScheduler.NewJob(jd, gocron.NewTask(func() {
			// Reset link cache at midnight CET
			m.linkService.Clear()
			m.logger.Debug().Msg("Cleared link service cache")
		}), gocron.WithContext(ctx)); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create link reset job")
		} else {
			m.logger.Debug().Msgf("Link reset job scheduled for every midnight, CET")
		}
	}

	// Arr monitoring job
	if jd, err := utils.ConvertToJobDef("10s"); err != nil {
		m.logger.Error().Err(err).Msg("Failed to convert arr monitoring interval to job definition")
	} else {
		// Schedule the job
		if _, err := m.scheduler.NewJob(jd, gocron.NewTask(func() {
			if err := m.handoffUncachedFailures(ctx); err != nil && ctx.Err() == nil {
				m.logger.Error().Err(err).Msg("Uncached transfer handoff failed")
			}
			if err := m.arr.Monitor(ctx); err != nil && ctx.Err() == nil {
				m.logger.Error().Err(err).Msg("Arr queue monitoring failed")
			}
		}),
			gocron.WithContext(ctx),
			gocron.WithName("arr-queue-monitor"),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		); err != nil {
			m.logger.Error().Err(err).Msg("Failed to create arr monitoring job")
		} else {
			m.logger.Debug().Msgf("Arr monitoring job scheduled for every %s", "10s")
		}
	}

	// Register the health checker sweep with the scheduler if enabled.
	if m.repair != nil {
		if err := m.repair.Start(ctx); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to start repair service")
		}
	}

	// Start the scheduler
	m.scheduler.Start()
	m.cetScheduler.Start()
	return nil
}
