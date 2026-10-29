package qbit

import (
	"context"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"slices"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/manager"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (q *QBit) addMagnet(ctx context.Context, url string, arr arr.Arr, debrid string, action config.DownloadAction, callbackURL string, rmTrackerUrls, skipMultiSeason bool) error {
	magnet, err := utils.GetMagnetFromUrl(url, rmTrackerUrls)
	if err != nil {
		return customerror.NewError(fmt.Errorf("error parsing magnet link: %w", err), http.StatusBadRequest, "invalid_magnet", false, false).Permanent()
	}

	importReq := manager.NewTorrentRequest(debrid, q.downloadFolder, magnet, arr, action, arr.DownloadUncached, callbackURL, manager.ImportTypeQBit, skipMultiSeason)

	err = boundedAdd(ctx, torrentAddTimeout, func(ctx context.Context) error {
		return q.manager.AddNewTorrent(ctx, importReq)
	})
	if err != nil {
		return fmt.Errorf("failed to process torrent: %w", err)
	}
	return nil
}

func (q *QBit) addTorrent(ctx context.Context, fileHeader *multipart.FileHeader, arr arr.Arr, debrid string, action config.DownloadAction, callbackURL string, rmTrackerUrls, skipMultiSeason bool) error {
	file, err := fileHeader.Open()
	if err != nil {
		return customerror.NewError(fmt.Errorf("error opening torrent file: %w", err), http.StatusBadRequest, "invalid_torrent", false, false).Permanent()
	}
	defer file.Close()
	magnet, err := utils.GetMagnetFromFile(file, fileHeader.Filename, rmTrackerUrls)
	if err != nil {
		return customerror.NewError(fmt.Errorf("error reading file %s: %w", fileHeader.Filename, err), http.StatusBadRequest, "invalid_torrent", false, false).Permanent()
	}
	importReq := manager.NewTorrentRequest(debrid, q.downloadFolder, magnet, arr, action, arr.DownloadUncached, callbackURL, manager.ImportTypeQBit, skipMultiSeason)
	err = boundedAdd(ctx, torrentAddTimeout, func(ctx context.Context) error {
		return q.manager.AddNewTorrent(ctx, importReq)
	})
	if err != nil {
		return fmt.Errorf("failed to process torrent: %w", err)
	}
	return nil
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
		Comment:            "Provider Blackhole <https://github.com/sirrobot01/decypharr>",
		CreatedBy:          "Provider Blackhole <https://github.com/sirrobot01/decypharr>",
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
		if !slices.Contains(t.Tags, tag) {
			t.Tags = append(t.Tags, tag)
		}
		if !slices.Contains(q.Tags, tag) {
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
		if !slices.Contains(q.Tags, tag) {
			q.Tags = append(q.Tags, tag)
		}
	}
	return true
}

// torrentAddTimeout bounds how long an Arr's add request waits on the debrid
// provider. Healthy TorBox adds take about 1s (p99 7s over 874 adds), but a
// degraded cache check used to hold the request until the Arr's own 100s
// HTTP timeout, once per release it tried. Failing at 30s lets the Arr move
// on to the next release, or give up and leave the item for a later search.
const torrentAddTimeout = 30 * time.Second

// boundedAdd runs add under a deadline. If the deadline expires, the result
// is a retryable 504 so the Arr treats it as a provider problem.
func boundedAdd(ctx context.Context, timeout time.Duration, add func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := add(ctx)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return customerror.NewError(
			fmt.Errorf("debrid provider did not answer within %s: %w", timeout, err),
			http.StatusGatewayTimeout, "provider_timeout", false, false,
		).Retryable()
	}
	return err
}
