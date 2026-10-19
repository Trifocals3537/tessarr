package qbit

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// All torrent-related helpers goes here
type qbitEndpointKey struct{}

func recordQBitEndpoint(ctx context.Context, request *manager.ImportRequest) {
	if endpoint, ok := ctx.Value(qbitEndpointKey{}).(string); ok {
		request.ClientEndpoint = endpoint
	}
}

func (q *QBit) addMagnet(
	ctx context.Context,
	rawURL string,
	arr *arr.Arr,
	debrid string,
	action config.DownloadAction,
	callbackURL string,
	rmTrackerUrls, skipMultiSeason bool,
	maxBytes int64,
) (int64, error) {
	magnet, err := utils.GetMagnetFromURLContext(ctx, rawURL, rmTrackerUrls, maxBytes)
	if err != nil {
		return 0, fmt.Errorf("error parsing magnet link: %w", err)
	}
	retainedBytes := magnetMemoryBytes(magnet)
	if retainedBytes > maxBytes {
		return 0, fmt.Errorf(
			"%w: torrent metadata exceeds the remaining request budget",
			utils.ErrContentTooLarge,
		)
	}

	importReq := manager.NewTorrentRequest(debrid, q.downloadFolder, magnet, arr, action, arr.DownloadUncached, callbackURL, manager.ImportTypeQBit, skipMultiSeason)
	recordQBitEndpoint(ctx, importReq)

	err = q.manager.AdmitNewTorrent(ctx, importReq)
	if err != nil {
		return retainedBytes, fmt.Errorf("failed to process torrent: %w", err)
	}
	return retainedBytes, nil
}

func (q *QBit) addTorrent(
	ctx context.Context,
	fileHeader *multipart.FileHeader,
	arr *arr.Arr,
	debrid string,
	action config.DownloadAction,
	callbackURL string,
	rmTrackerUrls, skipMultiSeason bool,
	maxBytes int64,
) (int64, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return 0, fmt.Errorf("error opening torrent file")
	}
	var reader io.Reader = file
	magnet, parseErr := utils.GetMagnetFromFileBounded(
		reader,
		fileHeader.Filename,
		rmTrackerUrls,
		maxBytes,
	)
	closeErr := file.Close()
	if parseErr == nil && closeErr != nil {
		parseErr = closeErr
	}
	if parseErr != nil {
		return 0, fmt.Errorf("error reading torrent file %s: %w", fileHeader.Filename, parseErr)
	}
	retainedBytes := magnetMemoryBytes(magnet)
	if retainedBytes > maxBytes {
		return 0, fmt.Errorf(
			"%w: torrent metadata exceeds the remaining request budget",
			utils.ErrContentTooLarge,
		)
	}
	importReq := manager.NewTorrentRequest(debrid, q.downloadFolder, magnet, arr, action, arr.DownloadUncached, callbackURL, manager.ImportTypeQBit, skipMultiSeason)
	recordQBitEndpoint(ctx, importReq)
	err = q.manager.AdmitNewTorrent(ctx, importReq)
	if err != nil {
		return retainedBytes, fmt.Errorf("failed to process torrent: %w", err)
	}
	return retainedBytes, nil
}

func magnetMemoryBytes(magnet *utils.Magnet) int64 {
	if magnet == nil {
		return 0
	}
	return int64(len(magnet.File)) + int64(len(magnet.Link)) + int64(len(magnet.Name))
}

func (q *QBit) ResumeTorrent(t *storage.Entry) bool {
	return true
}

func (q *QBit) PauseTorrent(t *storage.Entry) bool {
	return true
}

func (q *QBit) RefreshTorrent(t *storage.Entry) bool {
	return true
}

func (q *QBit) GetTorrentProperties(t *storage.Entry) *TorrentProperties {
	return &TorrentProperties{
		AdditionDate:       t.AddedOn.Unix(),
		Comment:            "Provider Blackhole <https://github.com/Trifocals3537/tessarr>",
		CreatedBy:          "Provider Blackhole <https://github.com/Trifocals3537/tessarr>",
		CreationDate:       t.AddedOn.Unix(),
		DlLimit:            -1,
		UpLimit:            -1,
		DlSpeed:            t.Speed,
		UpSpeed:            t.Speed,
		TotalSize:          t.Size,
		TotalUploaded:      t.Bytes,
		TotalDownloaded:    t.Bytes,
		LastSeen:           time.Now().Unix(),
		NbConnectionsLimit: 100,
		Peers:              0,
		PeersTotal:         2,
		SeedingTime:        1,
		Seeds:              100,
		ShareRatio:         100,
	}
}

func (q *QBit) setTorrentTags(t *storage.Entry, tags []string) {
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		if !utils.Contains(t.Tags, tag) {
			t.Tags = append(t.Tags, tag)
		}
		if !utils.Contains(q.Tags, tag) {
			q.Tags = append(q.Tags, tag)
		}
	}
	_ = q.manager.Queue().Update(t)
}

func (q *QBit) removeTorrentTags(t *storage.Entry, tags []string) bool {
	newTorrentTags := utils.RemoveItem(t.Tags, tags...)
	q.Tags = utils.RemoveItem(q.Tags, tags...)
	t.Tags = newTorrentTags
	_ = q.manager.Queue().Update(t)
	return true
}

func (q *QBit) addTags(tags []string) bool {
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		if !utils.Contains(q.Tags, tag) {
			q.Tags = append(q.Tags, tag)
		}
	}
	return true
}
