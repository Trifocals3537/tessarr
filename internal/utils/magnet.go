package utils

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/anacrolix/torrent/metainfo"
)

var (
	hexRegex = regexp.MustCompile("^[0-9a-fA-F]{40}$")
)

type Magnet struct {
	Name     string `json:"name"`
	InfoHash string `json:"infoHash"`
	Size     int64  `json:"size"`
	Link     string `json:"link"`
	File     []byte `json:"-"`
}

func (m *Magnet) IsTorrent() bool {
	return m.File != nil
}

// stripTrackersFromMagnet removes trackers from a magnet and returns a modified copy
func stripTrackersFromMagnet(mi metainfo.Magnet, fileType string) metainfo.Magnet {
	originalTrackerCount := len(mi.Trackers)
	if len(mi.Trackers) > 0 {
		log := logger.Default()
		mi.Trackers = nil
		log.Printf("Removed %d tracker URLs from %s", originalTrackerCount, fileType)
	}
	return mi
}

// stripTrackersFromTorrentFile removes announce metadata without rebuilding the
// opaque info dictionary. That keeps the torrent's infohash stable while
// ensuring providers do not receive tracker URLs when the removal policy is on.
func stripTrackersFromTorrentFile(mi *metainfo.MetaInfo) ([]byte, error) {
	mi.Announce = ""
	mi.AnnounceList = nil

	var data bytes.Buffer
	if err := mi.Write(&data); err != nil {
		return nil, fmt.Errorf("failed to sanitize torrent metadata")
	}
	return data.Bytes(), nil
}

func GetMagnetFromFile(file io.Reader, filePath string, rmTrackerUrls bool) (*Magnet, error) {
	return GetMagnetFromFileBounded(file, filePath, rmTrackerUrls, MaxMetadataFileBytes)
}

// GetMagnetFromFileBounded parses an uploaded torrent or magnet file without
// reading more than maxBytes. The public wrapper retains the existing API and
// applies the standard metadata ceiling.
func GetMagnetFromFileBounded(
	file io.Reader,
	filePath string,
	rmTrackerUrls bool,
	maxBytes int64,
) (*Magnet, error) {
	if maxBytes <= 0 || maxBytes > MaxMetadataFileBytes {
		return nil, fmt.Errorf("metadata byte limit must be between 1 and %d", MaxMetadataFileBytes)
	}

	var (
		m   *Magnet
		err error
	)
	if strings.EqualFold(filepath.Ext(filePath), ".torrent") {
		torrentData, err := ReadAllLimited(file, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to read torrent metadata: %w", err)
		}
		m, err = GetMagnetFromBytes(torrentData, rmTrackerUrls)
		if err != nil {
			return nil, err
		}
	} else {
		// .magnet file
		magnetLimit := min(maxBytes, MaxMagnetTextBytes)
		magnetData, readErr := ReadAllLimited(file, magnetLimit)
		if readErr != nil {
			return nil, fmt.Errorf("failed to read magnet file: %w", readErr)
		}
		magnetLink, readErr := readMagnetData(magnetData)
		if readErr != nil {
			return nil, readErr
		}
		m, err = GetMagnetInfo(magnetLink, rmTrackerUrls)
		if err != nil {
			return nil, err
		}
	}
	m.Name = strings.TrimSuffix(filePath, filepath.Ext(filePath))
	return m, nil
}

func GetMagnetFromUrl(url string, rmTrackerUrls bool) (*Magnet, error) {
	return GetMagnetFromURLContext(
		context.Background(),
		url,
		rmTrackerUrls,
		MaxMetadataFileBytes,
	)
}

// GetMagnetFromURLContext resolves a magnet URI, a raw v1 infohash, or an
// HTTP(S) torrent document with caller cancellation and a strict download
// ceiling. Raw hashes are converted to tracker-free magnets; metadata and the
// display name are resolved by the selected provider after submission.
func GetMagnetFromURLContext(
	ctx context.Context,
	rawURL string,
	rmTrackerUrls bool,
	maxBytes int64,
) (*Magnet, error) {
	if maxBytes <= 0 || maxBytes > MaxMetadataFileBytes {
		return nil, fmt.Errorf("metadata byte limit must be between 1 and %d", MaxMetadataFileBytes)
	}

	rawURL = strings.TrimSpace(rawURL)
	lowerURL := strings.ToLower(rawURL)
	switch {
	case strings.HasPrefix(lowerURL, "magnet:"):
		if int64(len(rawURL)) > MaxMagnetTextBytes {
			return nil, fmt.Errorf("%w: magnet link maximum is %d bytes", ErrContentTooLarge, MaxMagnetTextBytes)
		}
		return GetMagnetInfo(rawURL, rmTrackerUrls)
	case strings.HasPrefix(lowerURL, "http://"),
		strings.HasPrefix(lowerURL, "https://"):
		return openMagnetHTTPURL(ctx, rawURL, rmTrackerUrls, maxBytes)
	default:
		infoHash, err := processInfoHash(rawURL)
		if err != nil {
			return nil, fmt.Errorf("invalid torrent URL or infohash")
		}
		return &Magnet{
			InfoHash: infoHash,
			Link:     "magnet:?xt=urn:btih:" + infoHash,
		}, nil
	}
}

func GetMagnetFromBytes(torrentData []byte, rmTrackerUrls bool) (*Magnet, error) {
	if len(torrentData) == 0 {
		return nil, fmt.Errorf("invalid torrent metadata")
	}
	if int64(len(torrentData)) > MaxMetadataFileBytes {
		return nil, fmt.Errorf("%w: torrent metadata maximum is %d bytes", ErrContentTooLarge, MaxMetadataFileBytes)
	}

	mi, err := metainfo.Load(bytes.NewReader(torrentData))
	if err != nil {
		return nil, fmt.Errorf("invalid torrent metadata")
	}

	hash := mi.HashInfoBytes()
	infoHash := hash.HexString()
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, fmt.Errorf("invalid torrent info dictionary")
	}
	finalTorrentData := torrentData
	if rmTrackerUrls {
		originalTrackerCount := len(mi.UpvertedAnnounceList())
		finalTorrentData, err = stripTrackersFromTorrentFile(mi)
		if err != nil {
			return nil, err
		}
		mi, err = metainfo.Load(bytes.NewReader(finalTorrentData))
		if err != nil || mi.HashInfoBytes().HexString() != infoHash {
			return nil, fmt.Errorf("sanitized torrent metadata changed the info dictionary")
		}
		if originalTrackerCount > 0 {
			log := logger.Default()
			log.Printf("Removed %d tracker tiers from torrent file", originalTrackerCount)
		}
	}
	magnetMeta := mi.Magnet(&hash, &info)
	magnet := &Magnet{
		InfoHash: infoHash,
		Name:     info.Name,
		Size:     info.Length,
		Link:     magnetMeta.String(),
		File:     finalTorrentData,
	}
	return magnet, nil
}

func readMagnetData(data []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// ReadAllLimited already bounds the input. Raising Scanner's token limit to
	// the same ceiling preserves long-but-valid magnet links up to that limit.
	scanner.Buffer(make([]byte, 4096), int(MaxMagnetTextBytes))
	for scanner.Scan() {
		content := strings.TrimSpace(scanner.Text())
		if content != "" {
			return content, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("failed to parse magnet file")
	}
	return "", fmt.Errorf("magnet file is empty")
}

func ReadMagnetFile(file io.Reader) string {
	data, err := ReadAllLimited(file, MaxMagnetTextBytes)
	if err != nil {
		return ""
	}
	link, err := readMagnetData(data)
	if err != nil {
		return ""
	}
	return link
}

func OpenMagnetHttpURL(magnetLink string, rmTrackerUrls bool) (*Magnet, error) {
	return openMagnetHTTPURL(
		context.Background(),
		magnetLink,
		rmTrackerUrls,
		MaxMetadataFileBytes,
	)
}

func openMagnetHTTPURL(
	ctx context.Context,
	rawURL string,
	rmTrackerUrls bool,
	maxBytes int64,
) (*Magnet, error) {
	_, torrentData, err := DownloadFileBounded(ctx, rawURL, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch torrent metadata: %w", err)
	}
	return GetMagnetFromBytes(torrentData, rmTrackerUrls)
}

func GetMagnetInfo(magnetLink string, rmTrackerUrls bool) (*Magnet, error) {
	if magnetLink == "" {
		return nil, fmt.Errorf("error getting magnet from file")
	}
	if int64(len(magnetLink)) > MaxMagnetTextBytes {
		return nil, fmt.Errorf("%w: magnet link maximum is %d bytes", ErrContentTooLarge, MaxMagnetTextBytes)
	}

	mi, err := metainfo.ParseMagnetUri(magnetLink)
	if err != nil {
		// Parser errors may embed the complete magnet URI, including private
		// tracker passkeys. Keep the external error deliberately generic.
		return nil, fmt.Errorf("error parsing magnet link")
	}

	// Strip all announce URLs if requested
	if rmTrackerUrls {
		mi = stripTrackersFromMagnet(mi, "magnet link")
	}

	btih := mi.InfoHash.HexString()
	dn := mi.DisplayName

	// Reconstruct the magnet link using the (possibly modified) spec
	finalLink := mi.String()

	magnet := &Magnet{
		InfoHash: btih,
		Name:     dn,
		Size:     0,
		Link:     finalLink,
	}
	return magnet, nil
}

func ExtractInfoHash(magnetDesc string) string {
	const prefix = "xt=urn:btih:"
	start := strings.Index(magnetDesc, prefix)
	if start == -1 {
		return ""
	}
	hash := ""
	start += len(prefix)
	end := strings.IndexAny(magnetDesc[start:], "&#")
	if end == -1 {
		hash = magnetDesc[start:]
	} else {
		hash = magnetDesc[start : start+end]
	}
	hash, _ = processInfoHash(hash) // Convert to hex if needed
	return hash
}

func processInfoHash(input string) (string, error) {
	// Regular expression for a valid 40-character hex infohash

	// If it's already a valid hex infohash, return it as is
	if hexRegex.MatchString(input) {
		return strings.ToLower(input), nil
	}

	// If it's 32 characters long, it might be Base32 encoded
	if len(input) == 32 {
		// Ensure the input is uppercase and remove any padding
		input = strings.ToUpper(strings.TrimRight(input, "="))

		// Try to decode from Base32
		decoded, err := base32.StdEncoding.DecodeString(input)
		if err == nil && len(decoded) == 20 {
			// If successful and the result is 20 bytes, encode to hex
			return hex.EncodeToString(decoded), nil
		}
	}

	// If we get here, it's not a valid infohash and we couldn't convert it
	return "", fmt.Errorf("invalid infohash: %s", input)
}

func ConstructMagnet(infoHash, name string) *Magnet {
	// Create a magnet link from the infohash and name
	name = url.QueryEscape(strings.TrimSpace(name))
	magnetUri := fmt.Sprintf("magnet:?xt=urn:btih:%s&dn=%s", infoHash, name)
	return &Magnet{
		InfoHash: infoHash,
		Name:     name,
		Size:     0,
		Link:     magnetUri,
	}
}

func GenerateInfoHash() string {
	// Generate a random 40-character hexadecimal string (20 bytes = 40 hex chars)
	b := make([]byte, 20)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}
