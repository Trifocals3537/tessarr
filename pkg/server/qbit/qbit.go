package qbit

import (
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/rs/zerolog"
)

type QBit struct {
	downloadFolder          string
	categories              []string
	alwaysRemoveTrackerURLS bool
	logger                  zerolog.Logger
	Tags                    []string
	manager                 *manager.Manager
	sessions                *qbitSessionStore
}

func New(manager *manager.Manager) *QBit {
	cfg := config.Get()
	return &QBit{
		downloadFolder:          cfg.DownloadFolder,
		categories:              cfg.Categories,
		alwaysRemoveTrackerURLS: cfg.AlwaysRmTrackerUrls,
		manager:                 manager,
		logger:                  logger.New("qbit"),
		sessions:                newQbitSessionStore(),
	}
}
