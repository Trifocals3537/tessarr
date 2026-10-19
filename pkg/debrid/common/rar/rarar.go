// RAR 3/4 support started as a translation of
// https://github.com/eliasbenb/RARAR.py. RAR 5 support follows RARLAB's
// published block format and intentionally exposes stored entries only.

package rar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/retry"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

// Constants from the Python code
var (
	// DefaultChunkSize Chunk sizes
	DefaultChunkSize = 4096
	HttpChunkSize    = 32768
	MaxSearchSize    = 1 << 20 // 1MB

	// RAR markers and RAR 3/4 block types.
	Rar3Marker  = []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00}
	Rar5Marker  = []byte{0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00}
	BlockFile   = byte(0x74)
	BlockHeader = byte(0x73)
	BlockMarker = byte(0x72)
	BlockEnd    = byte(0x7B)

	// FlagDirectory Header flags
	FlagSplitBefore    = 0x01
	FlagSplitAfter     = 0x02
	FlagPassword       = 0x04
	FlagDirectory      = 0xE0
	FlagHasHighSize    = 0x100
	FlagHasUnicodeName = 0x200
	FlagHasData        = 0x8000
)

// Error definitions
var (
	ErrMarkerNotFound               = errors.New("RAR marker not found within search limit")
	ErrInvalidFormat                = errors.New("invalid RAR format")
	ErrNetworkError                 = errors.New("network error")
	ErrRangeRequestsNotSupported    = errors.New("server does not support range requests")
	ErrCompressionNotSupported      = errors.New("compression method not supported")
	ErrEncryptionNotSupported       = errors.New("encrypted RAR entries are not supported")
	ErrRedirectionNotSupported      = errors.New("redirected RAR entries are not supported")
	ErrMultiVolumeNotSupported      = errors.New("multi-volume RAR entries are not supported")
	ErrDirectoryExtractNotSupported = errors.New("directory extract not supported")
)

// Name returns the base filename of the file
func (f *File) Name() string {
	if i := strings.LastIndexAny(f.Path, "\\/"); i >= 0 {
		return f.Path[i+1:]
	}
	return f.Path
}

func NewHttpFile(url string) (*HttpFile, error) {
	file := &HttpFile{
		URL:        url,
		Position:   0,
		client:     &http.Client{Timeout: 60 * time.Second},
		MaxRetries: config.Get().Retries,
	}

	// GetReader file size
	size, err := file.getFileSize()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}
	file.FileSize = size

	return file, nil
}

func (f *HttpFile) doWithRetry(operation func() (any, error)) (any, error) {
	var result any

	err := retry.Do(
		func() error {
			var opErr error
			result, opErr = operation()
			if opErr == nil {
				return nil
			}
			// Only retry on network errors
			if !errors.Is(opErr, ErrNetworkError) {
				return retry.Unrecoverable(opErr)
			}
			return opErr
		},
		retry.Attempts(uint(f.MaxRetries)+1),
		retry.Delay(config.DefaultRetryDelay),
		retry.MaxDelay(config.DefaultRetryDelayMax),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	)

	if err != nil {
		return nil, err
	}
	return result, nil
}

// getFileSize gets the total file size from the server
func (f *HttpFile) getFileSize() (int64, error) {
	result, err := f.doWithRetry(func() (any, error) {
		size, found, err := f.getFileSizeFromHEAD()
		if err != nil {
			return int64(0), err
		}
		if found {
			return size, nil
		}
		return f.getFileSizeFromRange()
	})

	if err != nil {
		return 0, err
	}

	return result.(int64), nil
}

func (f *HttpFile) getFileSizeFromHEAD() (int64, bool, error) {
	req, err := http.NewRequest(http.MethodHead, f.URL, nil)
	if err != nil {
		return 0, false, fmt.Errorf(
			"%w: create HEAD request for %s",
			ErrNetworkError,
			utils.RedactedURL(f.URL),
		)
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf(
			"%w: HEAD request to %s failed",
			ErrNetworkError,
			utils.RedactedURL(f.URL),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
			// The returned length describes a different representation than the
			// identity-encoded byte ranges used by the archive reader.
			return 0, false, nil
		}
		contentLength := strings.TrimSpace(resp.Header.Get("Content-Length"))
		if contentLength == "" {
			return 0, false, nil
		}
		size, parseErr := strconv.ParseInt(contentLength, 10, 64)
		if parseErr == nil && size > 0 {
			return size, true, nil
		}
		// A malformed or unusable HEAD length is not authoritative. A strict
		// one-byte range response can still provide the representation size.
		return 0, false, nil
	}

	if isRetryableHTTPStatus(resp.StatusCode) {
		return 0, false, fmt.Errorf(
			"%w: HEAD request returned status %d",
			ErrNetworkError,
			resp.StatusCode,
		)
	}
	// Some signed and CDN-backed URLs intentionally reject HEAD while still
	// supporting ranged GETs. Probe that capability instead of rejecting the
	// archive solely because HEAD was unavailable.
	return 0, false, nil
}

func (f *HttpFile) getFileSizeFromRange() (int64, error) {
	req, err := http.NewRequest(http.MethodGet, f.URL, nil)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: create range size probe for %s",
			ErrNetworkError,
			utils.RedactedURL(f.URL),
		)
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := f.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf(
			"%w: range size probe to %s failed",
			ErrNetworkError,
			utils.RedactedURL(f.URL),
		)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
			return 0, fmt.Errorf("%w: range size probe returned content encoding %q", ErrNetworkError, encoding)
		}
		start, end, total, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return 0, fmt.Errorf("%w: %v", ErrNetworkError, err)
		}
		if start != 0 || end != 0 || total <= 0 {
			return 0, fmt.Errorf(
				"%w: range size probe returned %d-%d/%d, want 0-0 with a positive total",
				ErrNetworkError,
				start,
				end,
				total,
			)
		}
		if resp.ContentLength >= 0 && resp.ContentLength != 1 {
			return 0, fmt.Errorf(
				"%w: range size probe body length %d, want 1",
				ErrNetworkError,
				resp.ContentLength,
			)
		}
		probe, err := io.ReadAll(io.LimitReader(resp.Body, 2))
		if err != nil {
			return 0, fmt.Errorf("%w: read range size probe: %v", ErrNetworkError, err)
		}
		if len(probe) != 1 {
			return 0, fmt.Errorf("%w: range size probe returned %d body bytes, want 1", ErrNetworkError, len(probe))
		}
		return total, nil
	case http.StatusOK:
		// The origin ignored Range. Closing immediately keeps this fallback
		// bounded instead of downloading an entire archive just to learn its size.
		return 0, ErrRangeRequestsNotSupported
	default:
		return 0, fmt.Errorf(
			"%w: range size probe returned status %d",
			ErrNetworkError,
			resp.StatusCode,
		)
	}
}

func isRetryableHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= http.StatusInternalServerError
}

// ReadAt implements the io.ReaderAt interface
func (f *HttpFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("%w: negative read offset", ErrNetworkError)
	}
	if f.FileSize <= 0 {
		return 0, fmt.Errorf("%w: invalid file size", ErrNetworkError)
	}

	// Ensure we don't read past the end of the file
	size := int64(len(p))
	remaining := f.FileSize - off
	if remaining <= 0 {
		return 0, io.EOF
	}
	if size > remaining {
		size = remaining
		p = p[:size]
	}

	result, err := f.doWithRetry(func() (any, error) {
		// Create HTTP request with Range header
		end := off + size - 1

		req, err := http.NewRequest(http.MethodGet, f.URL, nil)
		if err != nil {
			return 0, fmt.Errorf(
				"%w: create request for %s",
				ErrNetworkError,
				utils.RedactedURL(f.URL),
			)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
		req.Header.Set("Accept-Encoding", "identity")

		// Make the request
		resp, err := f.client.Do(req)
		if err != nil {
			return 0, fmt.Errorf(
				"%w: request to %s failed",
				ErrNetworkError,
				utils.RedactedURL(f.URL),
			)
		}
		defer resp.Body.Close()

		// Handle response
		switch resp.StatusCode {
		case http.StatusPartialContent:
			if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
				return 0, fmt.Errorf("%w: partial response returned content encoding %q", ErrNetworkError, encoding)
			}
			if err := validateContentRange(
				resp.Header.Get("Content-Range"),
				off,
				end,
				f.FileSize,
			); err != nil {
				return 0, fmt.Errorf("%w: %v", ErrNetworkError, err)
			}
			if resp.ContentLength >= 0 && resp.ContentLength != size {
				return 0, fmt.Errorf(
					"%w: partial response length %d does not match requested length %d",
					ErrNetworkError,
					resp.ContentLength,
					size,
				)
			}
			bytesRead, err := io.ReadFull(resp.Body, p)
			return bytesRead, err
		case http.StatusOK:
			if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
				return 0, fmt.Errorf("%w: full response returned content encoding %q", ErrNetworkError, encoding)
			}
			// A 200 response normally means the origin ignored Range. Reading
			// the whole object here can allocate the complete media file and
			// repeated ReaderAt calls would download it over and over. Accept
			// 200 only when this request already covers the exact whole file.
			if off != 0 || size != f.FileSize {
				return 0, ErrRangeRequestsNotSupported
			}
			if resp.ContentLength >= 0 && resp.ContentLength != f.FileSize {
				return 0, fmt.Errorf(
					"%w: full response length %d does not match file size %d",
					ErrNetworkError,
					resp.ContentLength,
					f.FileSize,
				)
			}
			return io.ReadFull(resp.Body, p)
		case http.StatusRequestedRangeNotSatisfiable:
			// We're at EOF
			return 0, io.EOF
		default:
			return 0, fmt.Errorf("%w: unexpected status code: %d", ErrNetworkError, resp.StatusCode)
		}
	})

	if err != nil {
		return 0, err
	}

	return result.(int), nil
}

func validateContentRange(header string, wantStart, wantEnd, wantTotal int64) error {
	start, end, total, err := parseContentRange(header)
	if err != nil {
		return err
	}
	if start != wantStart || end != wantEnd || total != wantTotal {
		return fmt.Errorf(
			"Content-Range %d-%d/%d does not match requested %d-%d/%d",
			start,
			end,
			total,
			wantStart,
			wantEnd,
			wantTotal,
		)
	}
	return nil
}

func parseContentRange(header string) (int64, int64, int64, error) {
	fields := strings.Fields(strings.TrimSpace(header))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bytes") {
		return 0, 0, 0, fmt.Errorf("missing or invalid Content-Range")
	}
	value := fields[1]
	if strings.Count(value, "/") != 1 {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range")
	}
	rangePart, totalPart, _ := strings.Cut(value, "/")
	if strings.Count(rangePart, "-") != 1 || totalPart == "*" {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range")
	}
	startPart, endPart, _ := strings.Cut(rangePart, "-")
	start, err := strconv.ParseInt(startPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range start")
	}
	end, err := strconv.ParseInt(endPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range end")
	}
	total, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range total")
	}
	if start < 0 || end < start || total <= end {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range bounds")
	}
	return start, end, total, nil
}

func validateRAR3Header(header []byte) error {
	if len(header) < 7 {
		return fmt.Errorf("%w: RAR3 header is shorter than 7 bytes", ErrInvalidFormat)
	}
	headerSize := int(binary.LittleEndian.Uint16(header[5:7]))
	if headerSize < 7 || headerSize != len(header) {
		return fmt.Errorf(
			"%w: RAR3 header size %d does not match %d bytes read",
			ErrInvalidFormat,
			headerSize,
			len(header),
		)
	}
	// Legacy authenticity/signature headers have inconsistent CRCs in real
	// archives, and old-service headers can cover bytes outside HeadSize.
	// They do not describe streamable file data, so retain UnRAR-compatible
	// tolerance while still validating their declared sizes and data bounds.
	switch header[2] {
	case 0x76, 0x77, 0x79:
		return nil
	}
	wantCRC := binary.LittleEndian.Uint16(header[:2])
	// UnRAR's RawRead::GetCRC15 finalizes the standard IEEE CRC32 over the
	// header bytes after HEAD_CRC, then compares its low 16 bits. Despite the
	// historical method name, this is not a separate CRC-16 algorithm.
	gotCRC := uint16(crc32.ChecksumIEEE(header[2:]) & 0xffff)
	if gotCRC != wantCRC {
		return fmt.Errorf(
			"%w: RAR3 header CRC mismatch (got %04x, want %04x)",
			ErrInvalidFormat,
			gotCRC,
			wantCRC,
		)
	}
	return nil
}

// NewReader creates a reader for stored entries in RAR 3/4 or RAR 5 archives.
func NewReader(url string) (*Reader, error) {
	file, err := NewHttpFile(url)
	if err != nil {
		return nil, err
	}
	return newReader(file)
}

func newReader(file *HttpFile) (*Reader, error) {
	reader := &Reader{
		File:      file,
		ChunkSize: HttpChunkSize,
		Files:     make([]*File, 0),
	}

	// Find RAR marker
	marker, version, err := reader.findMarker()
	if err != nil {
		return nil, err
	}
	reader.Marker = marker
	reader.Version = version
	if version == 5 {
		if err := reader.initializeRAR5(); err != nil {
			return nil, err
		}
		return reader, nil
	}

	pos := reader.Marker + int64(len(Rar3Marker)) // Skip marker block

	headerData, err := reader.readBytes(pos, 7)
	if err != nil {
		return nil, err
	}

	if len(headerData) < 7 {
		return nil, ErrInvalidFormat
	}

	headSize := int(binary.LittleEndian.Uint16(headerData[5:7]))
	if headSize < 13 || int64(headSize) > file.FileSize-pos {
		return nil, ErrInvalidFormat
	}
	headerData, err = reader.readBytes(pos, headSize)
	if err != nil || len(headerData) != headSize {
		return nil, ErrInvalidFormat
	}
	if err := validateRAR3Header(headerData); err != nil {
		return nil, fmt.Errorf("validate RAR3 archive header: %w", err)
	}

	if headerData[2] != BlockHeader {
		return nil, ErrInvalidFormat
	}

	// Store the position after the archive header
	reader.HeaderEndPos = pos + int64(headSize)

	return reader, nil
}

// readBytes reads a range of bytes from the file
func (r *Reader) readBytes(start int64, length int) ([]byte, error) {
	if length <= 0 {
		return []byte{}, nil
	}

	data := make([]byte, length)
	n, err := r.File.ReadAt(data, start)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n < length {
		// Partial read, return what we got
		return data[:n], nil
	}

	return data, nil
}

// findMarker finds the first RAR marker in the SFX search window.
func (r *Reader) findMarker() (int64, int, error) {
	// First try to find marker in the first chunk
	firstChunkSize := 8192 // 8KB
	chunk, err := r.readBytes(0, firstChunkSize)
	if err != nil {
		return 0, 0, err
	}

	markerPos, version := findMarkerInBytes(chunk)
	if markerPos >= 0 {
		return int64(markerPos), version, nil
	}

	// If not found, continue searching
	position := int64(firstChunkSize - len(Rar5Marker) + 1)
	maxSearch := int64(MaxSearchSize)

	for position < maxSearch {
		chunkSize := min(r.ChunkSize, int(maxSearch-position))
		chunk, err := r.readBytes(position, chunkSize)
		if err != nil || len(chunk) == 0 {
			break
		}

		markerPos, version = findMarkerInBytes(chunk)
		if markerPos >= 0 {
			return position + int64(markerPos), version, nil
		}

		// Move forward by chunk size minus the marker length
		position += int64(max(1, len(chunk)-len(Rar5Marker)+1))
	}

	return 0, 0, ErrMarkerNotFound
}

func findMarkerInBytes(data []byte) (int, int) {
	rar3 := bytes.Index(data, Rar3Marker)
	rar5 := bytes.Index(data, Rar5Marker)
	switch {
	case rar3 < 0 && rar5 < 0:
		return -1, 0
	case rar5 >= 0 && (rar3 < 0 || rar5 < rar3):
		return rar5, 5
	default:
		return rar3, 3
	}
}

// decodeUnicode decodes RAR3 Unicode encoding
func decodeUnicode(asciiStr string, unicodeData []byte) string {
	if len(unicodeData) == 0 {
		return asciiStr
	}

	var result []rune
	asciiPos := 0
	dataPos := 0
	highByte := byte(0)

	for dataPos < len(unicodeData) {
		flags := unicodeData[dataPos]
		dataPos++

		// Determine the number of character positions this flag byte controls
		var flagBits uint
		var flagCount int
		var bitCount int

		if flags&0x80 != 0 {
			// Extended flag - controls up to 32 characters (16 bit pairs)
			flagBits = uint(flags)
			bitCount = 1
			for (flagBits&(0x80>>bitCount) != 0) && dataPos < len(unicodeData) {
				flagBits = ((flagBits & ((0x80 >> bitCount) - 1)) << 8) | uint(unicodeData[dataPos])
				dataPos++
				bitCount++
			}
			flagCount = bitCount * 4
		} else {
			// Simple flag - controls 4 characters (4 bit pairs)
			flagBits = uint(flags)
			flagCount = 4
		}

		// Parse each 2-bit flag
		for i := 0; i < flagCount; i++ {
			if asciiPos >= len(asciiStr) && dataPos >= len(unicodeData) {
				break
			}

			flagValue := (flagBits >> (i * 2)) & 0x03

			switch flagValue {
			case 0:
				// Use ASCII character
				if asciiPos < len(asciiStr) {
					result = append(result, rune(asciiStr[asciiPos]))
					asciiPos++
				}
			case 1:
				// Unicode character with high byte 0
				if dataPos < len(unicodeData) {
					result = append(result, rune(unicodeData[dataPos]))
					dataPos++
				}
			case 2:
				// Unicode character with current high byte
				if dataPos < len(unicodeData) {
					lowByte := uint(unicodeData[dataPos])
					dataPos++
					result = append(result, rune(lowByte|(uint(highByte)<<8)))
				}
			case 3:
				// Set new high byte
				if dataPos < len(unicodeData) {
					highByte = unicodeData[dataPos]
					dataPos++
				}
			}
		}
	}

	// Append any remaining ASCII characters
	for asciiPos < len(asciiStr) {
		result = append(result, rune(asciiStr[asciiPos]))
		asciiPos++
	}

	return string(result)
}

func (r *Reader) readRAR3Header(position int64) ([]byte, error) {
	if r.File == nil || position < 0 || position >= r.File.FileSize {
		return nil, fmt.Errorf("%w: RAR3 header offset %d is outside archive", ErrInvalidFormat, position)
	}
	shortHeader, err := r.readBytes(position, 7)
	if err != nil {
		return nil, err
	}
	if len(shortHeader) != 7 {
		return nil, fmt.Errorf("%w: truncated RAR3 block header", ErrInvalidFormat)
	}
	headerSize := int(binary.LittleEndian.Uint16(shortHeader[5:7]))
	if headerSize < 7 || int64(headerSize) > r.File.FileSize-position {
		return nil, fmt.Errorf("%w: invalid RAR3 header size %d", ErrInvalidFormat, headerSize)
	}

	header := make([]byte, headerSize)
	copy(header, shortHeader)
	if remaining := headerSize - len(shortHeader); remaining > 0 {
		rest, err := r.readBytes(position+int64(len(shortHeader)), remaining)
		if err != nil {
			return nil, err
		}
		if len(rest) != remaining {
			return nil, fmt.Errorf("%w: truncated RAR3 block header", ErrInvalidFormat)
		}
		copy(header[len(shortHeader):], rest)
	}
	if err := validateRAR3Header(header); err != nil {
		return nil, err
	}
	return header, nil
}

// readFiles reads all file entries in the archive.
func (r *Reader) readFiles() error {
	if r.Version == 5 {
		return r.readFilesRAR5()
	}
	if r.Version != 3 {
		return fmt.Errorf("%w: unsupported RAR version %d", ErrInvalidFormat, r.Version)
	}

	// NewReader already validated the archive header and stored where it ends.
	pos := r.HeaderEndPos

	// Process validated blocks until the required end header.
	for pos < r.File.FileSize {
		headerData, err := r.readRAR3Header(pos)
		if err != nil {
			return fmt.Errorf("read RAR3 block at offset %d: %w", pos, err)
		}
		headType := headerData[2]
		headFlags := int(binary.LittleEndian.Uint16(headerData[3:5]))
		headSize := int(binary.LittleEndian.Uint16(headerData[5:7]))

		if headType == BlockEnd {
			return nil
		}

		if headType == BlockFile {
			fileInfo, err := r.parseFileHeader(headerData, pos)
			if err != nil {
				return fmt.Errorf("parse RAR3 file header at offset %d: %w", pos, err)
			}
			if fileInfo.NextOffset <= pos || fileInfo.NextOffset > r.File.FileSize {
				return fmt.Errorf("%w: invalid RAR3 next file offset %d", ErrInvalidFormat, fileInfo.NextOffset)
			}
			r.Files = append(r.Files, fileInfo)
			pos = fileInfo.NextOffset
			continue
		}

		dataSize := int64(0)
		if headFlags&FlagHasData != 0 {
			if headSize < 11 {
				return fmt.Errorf("%w: RAR3 long block header is shorter than 11 bytes", ErrInvalidFormat)
			}
			dataSize = int64(binary.LittleEndian.Uint32(headerData[7:11]))
		}
		nextOffset := pos + int64(headSize)
		if dataSize > r.File.FileSize-nextOffset {
			return fmt.Errorf("%w: RAR3 block data extends beyond archive", ErrInvalidFormat)
		}
		pos = nextOffset + dataSize
	}

	// RAR 2.x and 3.x archives are allowed to end exactly after the last
	// complete block without an explicit end-of-archive header.
	return nil
}

// parseFileHeader parses a file header and returns file info
func (r *Reader) parseFileHeader(headerData []byte, position int64) (*File, error) {
	if len(headerData) < 7 {
		return nil, fmt.Errorf("%w: RAR3 header data is too short", ErrInvalidFormat)
	}

	headType := headerData[2]
	headFlags := int(binary.LittleEndian.Uint16(headerData[3:5]))
	headSize := int(binary.LittleEndian.Uint16(headerData[5:7]))

	if headType != BlockFile {
		return nil, fmt.Errorf("%w: RAR3 block is not a file header", ErrInvalidFormat)
	}
	if headSize != len(headerData) {
		return nil, fmt.Errorf("%w: RAR3 file header size mismatch", ErrInvalidFormat)
	}

	// Check if we have enough data
	if len(headerData) < 32 {
		return nil, fmt.Errorf("%w: RAR3 file header is shorter than 32 bytes", ErrInvalidFormat)
	}

	// Parse basic file header fields
	packSize := binary.LittleEndian.Uint32(headerData[7:11])
	unpackSize := binary.LittleEndian.Uint32(headerData[11:15])
	fileOS := headerData[15]
	fileCRC := binary.LittleEndian.Uint32(headerData[16:20])
	// fileTime := binary.LittleEndian.Uint32(headerData[20:24])
	// unpVer := headerData[24]
	method := headerData[25]
	nameSize := binary.LittleEndian.Uint16(headerData[26:28])
	fileAttr := binary.LittleEndian.Uint32(headerData[28:32])

	// Handle high pack/unp sizes
	highPackSize := uint32(0)
	highUnpSize := uint32(0)

	offset := 32 // Start after basic header fields

	if headFlags&FlagHasHighSize != 0 {
		if offset+8 > len(headerData) {
			return nil, fmt.Errorf("%w: truncated RAR3 high-size fields", ErrInvalidFormat)
		}
		highPackSize = binary.LittleEndian.Uint32(headerData[offset : offset+4])
		highUnpSize = binary.LittleEndian.Uint32(headerData[offset+4 : offset+8])
		offset += 8
	}

	// Calculate actual sizes
	if unpackSize == math.MaxUint32 && (headFlags&FlagHasHighSize == 0 || highUnpSize == math.MaxUint32) {
		return nil, fmt.Errorf("%w: RAR3 unpacked size is unknown", ErrInvalidFormat)
	}
	fullPackSizeUnsigned := uint64(packSize) | uint64(highPackSize)<<32
	fullUnpSizeUnsigned := uint64(unpackSize) | uint64(highUnpSize)<<32
	if fullPackSizeUnsigned > math.MaxInt64 || fullUnpSizeUnsigned > math.MaxInt64 {
		return nil, fmt.Errorf("%w: RAR3 file size overflows", ErrInvalidFormat)
	}
	fullPackSize := int64(fullPackSizeUnsigned)
	fullUnpSize := int64(fullUnpSizeUnsigned)

	// Read filename
	if nameSize == 0 || int(nameSize) > len(headerData)-offset {
		return nil, fmt.Errorf("%w: invalid RAR3 file name size %d", ErrInvalidFormat, nameSize)
	}
	fileNameBytes := headerData[offset : offset+int(nameSize)]
	var fileName string

	if headFlags&FlagHasUnicodeName != 0 {
		before, after, ok := bytes.Cut(fileNameBytes, []byte{0})
		if ok {
			// Try UTF-8 first
			asciiPart := before
			if utf8.Valid(asciiPart) {
				fileName = string(asciiPart)
			} else {
				// Fall back to custom decoder
				asciiStr := string(asciiPart)
				unicodePart := after
				fileName = decodeUnicode(asciiStr, unicodePart)
			}
		} else {
			// No null byte
			if utf8.Valid(fileNameBytes) {
				fileName = string(fileNameBytes)
			} else {
				fileName = string(fileNameBytes) // Last resort
			}
		}
	} else {
		// Non-Unicode filename
		if utf8.Valid(fileNameBytes) {
			fileName = string(fileNameBytes)
		} else {
			fileName = string(fileNameBytes) // Fallback
		}
	}
	if fileName == "" || strings.IndexByte(fileName, 0) >= 0 {
		return nil, fmt.Errorf("%w: invalid RAR3 file name", ErrInvalidFormat)
	}

	isDirectory := (headFlags & FlagDirectory) == FlagDirectory
	isRedirected := fileOS == 3 && fileAttr&0xF000 == 0xA000

	// Calculate data offsets
	if position < 0 || int64(headSize) > math.MaxInt64-position {
		return nil, fmt.Errorf("%w: RAR3 file header offset overflows", ErrInvalidFormat)
	}
	dataOffset := position + int64(headSize)
	if !isDirectory && fullPackSize > 0 && headFlags&FlagHasData == 0 {
		return nil, fmt.Errorf("%w: RAR3 file data flag is missing", ErrInvalidFormat)
	}
	if isDirectory && fullPackSize != 0 {
		return nil, fmt.Errorf("%w: RAR3 directory has packed data", ErrInvalidFormat)
	}
	if fullPackSize > math.MaxInt64-dataOffset {
		return nil, fmt.Errorf("%w: RAR3 file data offset overflows", ErrInvalidFormat)
	}
	nextOffset := dataOffset

	if !isDirectory {
		nextOffset += fullPackSize
	}
	if r.File != nil && nextOffset > r.File.FileSize {
		return nil, fmt.Errorf("%w: RAR3 file data extends beyond archive", ErrInvalidFormat)
	}

	return &File{
		Path:           fileName,
		Size:           fullUnpSize,
		CompressedSize: fullPackSize,
		Method:         method,
		CRC:            fileCRC,
		IsDirectory:    isDirectory,
		Encrypted:      headFlags&FlagPassword != 0,
		Redirected:     isRedirected,
		SplitBefore:    headFlags&FlagSplitBefore != 0,
		SplitAfter:     headFlags&FlagSplitAfter != 0,
		DataOffset:     dataOffset,
		NextOffset:     nextOffset,
	}, nil
}

// GetFiles returns all files in the archive
func (r *Reader) GetFiles() ([]*File, error) {
	if len(r.Files) == 0 {
		err := r.readFiles()
		if err != nil {
			return nil, err
		}
	}

	return r.Files, nil
}

// ExtractFile extracts a file from the archive
func (r *Reader) ExtractFile(file *File) ([]byte, error) {
	byteRange, err := file.StreamByteRange()
	if err != nil {
		return nil, err
	}

	return r.readBytes(byteRange[0], int(byteRange[1]-byteRange[0]+1))
}
