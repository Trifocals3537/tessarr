package types

import (
	"errors"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
)

// ErrTerminalProviderTorrent identifies an explicit provider-reported transfer
// failure. Transport/API errors and unknown states must not trigger Arr handoff.
var ErrTerminalProviderTorrent = errors.New("terminal provider torrent failure")

type Torrent struct {
	Id               string          `json:"id"`
	InfoHash         string          `json:"info_hash"`
	Name             string          `json:"name"`
	Filename         string          `json:"filename"`
	OriginalFilename string          `json:"original_filename"`
	Size             int64           `json:"size"`
	Bytes            int64           `json:"bytes"` // Size of only the files that are downloaded
	Magnet           *utils.Magnet   `json:"magnet"`
	Files            map[string]File `json:"files"`
	Status           TorrentStatus   `json:"status"`
	ProviderState    string          `json:"-"` // Raw provider state, for conservative failure and stall decisions.
	Added            time.Time       `json:"added"`
	Progress         float64         `json:"progress"`
	Speed            int64           `json:"speed"`
	Seeders          int             `json:"seeders"`
	Links            []string        `json:"links"`
	DeletedFiles     []string        `json:"deleted_files"`

	Debrid string `json:"debrid"`

	Arr *arr.Arr `json:"arr"`

	SizeDownloaded   int64 `json:"-"` // This is used for local download
	DownloadUncached bool  `json:"-"`

	sync.Mutex
}

func (t *Torrent) GetSize() int64 {
	if t.Size == 0 {
		return t.Bytes
	}
	return t.Size
}

func (t *Torrent) Copy() *Torrent {
	t.Lock()
	defer t.Unlock()

	newFiles := make(map[string]File, len(t.Files))
	maps.Copy(newFiles, t.Files)

	return &Torrent{
		Id:               t.Id,
		InfoHash:         t.InfoHash,
		Name:             t.Name,
		Filename:         t.Filename,
		OriginalFilename: t.OriginalFilename,
		Size:             t.Size,
		Bytes:            t.Bytes,
		Magnet:           t.Magnet,
		Files:            newFiles,
		Status:           t.Status,
		ProviderState:    t.ProviderState,
		Added:            t.Added,
		Progress:         t.Progress,
		Speed:            t.Speed,
		Seeders:          t.Seeders,
		Links:            append([]string{}, t.Links...),
		Debrid:           t.Debrid,
		Arr:              t.Arr,
	}
}

func (t *Torrent) GetFile(filename string) (File, bool) {
	f, ok := t.Files[filename]
	if !ok {
		return File{}, false
	}
	return f, !f.Deleted
}

func (t *Torrent) GetFiles() []File {
	files := make([]File, 0, len(t.Files))

	for _, f := range t.Files {
		if !f.Deleted {
			files = append(files, f)
		}
	}
	return files
}

type File struct {
	TorrentId    string       `json:"torrent_id"`
	Id           string       `json:"id"`
	Name         string       `json:"name"`
	OutputPath   string       `json:"output_path,omitempty"`
	Size         int64        `json:"size"`
	IsRar        bool         `json:"is_rar"`
	ByteRange    *[2]int64    `json:"byte_range,omitempty"`
	Path         string       `json:"path"`
	Link         string       `json:"link"`
	AccountId    string       `json:"account_id"`
	Generated    time.Time    `json:"generated"`
	Deleted      bool         `json:"deleted"`
	DownloadLink DownloadLink `json:"-"`
}

// LocalPath returns the validated path Tessarr should expose in its managed
// output tree. Provider paths remain untouched for link resolution. Legacy
// records created before OutputPath was introduced retain their old layout.
func (f File) LocalPath() string {
	if f.OutputPath != "" {
		return f.OutputPath
	}
	return f.Path
}

func (t *Torrent) Cleanup(remove bool) {
	if remove {
		err := os.Remove(t.Filename)
		if err != nil {
			return
		}
	}
}

type IngestData struct {
	Debrid string `json:"debrid"`
	Name   string `json:"name"`
	Hash   string `json:"hash"`
	Size   int64  `json:"size"`
}

type LibraryStats struct {
	Total       int `json:"total"`
	Bad         int `json:"bad"`
	ActiveLinks int `json:"active_links"`
}

type Stats struct {
	Profile         *Profile         `json:"profile"`
	Library         LibraryStats     `json:"library"`
	Accounts        []map[string]any `json:"accounts"`
	SpeedTestResult *SpeedTestResult `json:"speed_test_result,omitempty"`
}

type Profile struct {
	Name       string    `json:"name"`
	Id         int64     `json:"id"`
	Username   string    `json:"username"`
	Email      string    `json:"email"`
	Points     int       `json:"points"`
	Type       string    `json:"type"`
	Premium    int64     `json:"premium"`
	Expiration time.Time `json:"expiration"`
}

type DownloadLink struct {
	Debrid       string    `json:"debrid"`
	Token        string    `json:"token"`
	Filename     string    `json:"filename"`
	Link         string    `json:"link"`
	DownloadLink string    `json:"download_link"`
	Generated    time.Time `json:"generated"`
	Size         int64     `json:"size"`
	Id           string    `json:"id"`
	ExpiresAt    time.Time

	// RecoveryProbeID ties a generated or cached URL to the one account
	// recovery probe that acquired it. It is deliberately process-local and
	// must never be serialized or exposed through the API.
	RecoveryProbeID uint64 `json:"-"`
}

const maxDownloadLinkRefreshSkew = time.Minute

// NeedsRefresh reports whether a cached download link is expired or close
// enough to expiry that starting a new stream with it would be risky. The
// skew is capped at ten percent of a known lifetime so short-lived links do
// not become immediately unusable.
func (dl *DownloadLink) NeedsRefresh(now time.Time) bool {
	if dl == nil || dl.ExpiresAt.IsZero() {
		return false
	}

	skew := maxDownloadLinkRefreshSkew
	if !dl.Generated.IsZero() && dl.ExpiresAt.After(dl.Generated) {
		if lifetimeSkew := dl.ExpiresAt.Sub(dl.Generated) / 10; lifetimeSkew < skew {
			skew = lifetimeSkew
		}
	}

	return !now.Add(skew).Before(dl.ExpiresAt)
}

func (dl *DownloadLink) Valid() error {
	if dl.Empty() {
		return EmptyDownloadLinkError
	}

	// Validate url format
	if !utils.IsValidURL(dl.DownloadLink) {
		return InvalidDownloadLinkError
	}

	return nil
}

func (dl *DownloadLink) Empty() bool {
	return dl.DownloadLink == ""
}

func (dl *DownloadLink) String() string {
	return dl.DownloadLink
}

// SpeedTestResult holds the result of a debrid provider speed test
type SpeedTestResult struct {
	Provider  string    `json:"provider"`
	SpeedMBps float64   `json:"speed_mbps"`
	LatencyMs int64     `json:"latency_ms"`
	BytesRead int64     `json:"bytes_read"`
	TestedAt  time.Time `json:"tested_at"`
	Error     string    `json:"error,omitempty"`
}
