package rar

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func encodeVInt(value uint64) []byte {
	encoded := make([]byte, 0, 10)
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value != 0 {
			b |= 0x80
		}
		encoded = append(encoded, b)
		if value == 0 {
			return encoded
		}
	}
}

func buildRAR5Block(headerType, flags uint64, body, extra, data []byte) []byte {
	headerData := append([]byte{}, encodeVInt(headerType)...)
	headerData = append(headerData, encodeVInt(flags)...)
	if flags&rar5HeaderExtra != 0 {
		headerData = append(headerData, encodeVInt(uint64(len(extra)))...)
	}
	if flags&rar5HeaderData != 0 {
		headerData = append(headerData, encodeVInt(uint64(len(data)))...)
	}
	headerData = append(headerData, body...)
	headerData = append(headerData, extra...)

	headerSize := encodeVInt(uint64(len(headerData)))
	crcData := append(append([]byte{}, headerSize...), headerData...)
	block := make([]byte, 4)
	binary.LittleEndian.PutUint32(block, crc32.ChecksumIEEE(crcData))
	block = append(block, crcData...)
	block = append(block, data...)
	return block
}

func buildRAR5File(name string, content []byte, method uint64, blockFlags, fileFlags uint64, extra []byte) []byte {
	body := append([]byte{}, encodeVInt(fileFlags)...)
	body = append(body, encodeVInt(uint64(len(content)))...)
	body = append(body, encodeVInt(0)...) // Attributes.
	body = append(body, encodeVInt(method<<7)...)
	body = append(body, encodeVInt(1)...) // Unix host OS.
	body = append(body, encodeVInt(uint64(len(name)))...)
	body = append(body, name...)
	flags := blockFlags | rar5HeaderData
	if len(extra) > 0 {
		flags |= rar5HeaderExtra
	}
	return buildRAR5Block(rar5HeaderFile, flags, body, extra, content)
}

func buildRAR5Archive(fileBlock []byte) []byte {
	archive := append([]byte("SFX!"), Rar5Marker...)
	archive = append(archive, buildRAR5Block(rar5HeaderMain, 0, encodeVInt(0), nil, nil)...)
	archive = append(archive, fileBlock...)
	archive = append(archive, buildRAR5Block(rar5HeaderEnd, 0, encodeVInt(0), nil, nil)...)
	return archive
}

func newRangeServer(t *testing.T, content []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.Itoa(len(content)))
		if request.Method == http.MethodHead {
			return
		}
		value := strings.TrimPrefix(request.Header.Get("Range"), "bytes=")
		startValue, endValue, ok := strings.Cut(value, "-")
		if !ok {
			http.Error(w, "missing range", http.StatusBadRequest)
			return
		}
		start, startErr := strconv.Atoi(startValue)
		end, endErr := strconv.Atoi(endValue)
		if startErr != nil || endErr != nil || start < 0 || end < start || end >= len(content) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestRARReader(t *testing.T, url string, size int64) *Reader {
	t.Helper()
	reader, err := newReader(
		&HttpFile{
			URL:        url,
			client:     &http.Client{Timeout: time.Second},
			FileSize:   size,
			MaxRetries: 0,
		},
	)
	if err != nil {
		t.Fatalf("newReader() error = %v", err)
	}
	return reader
}

func TestRAR5StoredArchiveIsDirectlyStreamable(t *testing.T) {
	payload := []byte("seekable-media")
	archive := buildRAR5Archive(buildRAR5File("Movies/Café.mkv", payload, 0, 0, 0, nil))
	server := newRangeServer(t, archive)

	reader := newTestRARReader(t, server.URL, int64(len(archive)))
	if reader.Version != 5 || reader.Marker != 4 {
		t.Fatalf("NewReader() version/marker = %d/%d, want 5/4", reader.Version, reader.Marker)
	}
	files, err := reader.GetFiles()
	if err != nil {
		t.Fatalf("GetFiles() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("GetFiles() count = %d, want 1", len(files))
	}
	file := files[0]
	if file.Path != "Movies/Café.mkv" || file.Size != int64(len(payload)) || file.Method != MethodStore {
		t.Fatalf("GetFiles()[0] = %#v", file)
	}
	byteRange, err := file.StreamByteRange()
	if err != nil {
		t.Fatalf("StreamByteRange() error = %v", err)
	}
	if got := byteRange[1] - byteRange[0] + 1; got != int64(len(payload)) {
		t.Fatalf("StreamByteRange() length = %d, want %d", got, len(payload))
	}
	extracted, err := reader.ExtractFile(file)
	if err != nil {
		t.Fatalf("ExtractFile() error = %v", err)
	}
	if string(extracted) != string(payload) {
		t.Fatalf("ExtractFile() = %q, want %q", extracted, payload)
	}
}

func TestRAR5UnsafeEntriesAreParsedButNotStreamable(t *testing.T) {
	encryptionRecord := append(encodeVInt(1), encodeVInt(rar5FileEncryptionExtra)...)
	redirectionRecord := append(encodeVInt(1), encodeVInt(rar5FileRedirectionExtra)...)
	tests := []struct {
		name       string
		method     uint64
		blockFlags uint64
		extra      []byte
		want       error
	}{
		{name: "compressed", method: 1, want: ErrCompressionNotSupported},
		{name: "encrypted", extra: encryptionRecord, want: ErrEncryptionNotSupported},
		{name: "redirected", extra: redirectionRecord, want: ErrRedirectionNotSupported},
		{name: "split before", blockFlags: rar5HeaderSplitBefore, want: ErrMultiVolumeNotSupported},
		{name: "split after", blockFlags: rar5HeaderSplitAfter, want: ErrMultiVolumeNotSupported},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archive := buildRAR5Archive(buildRAR5File("video.mkv", []byte("data"), test.method, test.blockFlags, 0, test.extra))
			server := newRangeServer(t, archive)
			reader := newTestRARReader(t, server.URL, int64(len(archive)))
			files, err := reader.GetFiles()
			if err != nil {
				t.Fatalf("GetFiles() error = %v", err)
			}
			if len(files) != 1 {
				t.Fatalf("GetFiles() count = %d, want 1", len(files))
			}
			if _, err := files[0].StreamByteRange(); !errors.Is(err, test.want) {
				t.Fatalf("StreamByteRange() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRAR5SkipsServiceBlocksAndSupportsMultiByteHeaderSizes(t *testing.T) {
	name := strings.Repeat("long-name-", 20) + ".mkv"
	payload := []byte("media")
	archive := append([]byte{}, Rar5Marker...)
	archive = append(archive, buildRAR5Block(rar5HeaderMain, 0, encodeVInt(0), nil, nil)...)
	archive = append(archive, buildRAR5Block(rar5HeaderService, rar5HeaderData, nil, nil, []byte("metadata"))...)
	archive = append(archive, buildRAR5File(name, payload, 0, 0, 0, nil)...)
	archive = append(archive, buildRAR5Block(rar5HeaderEnd, 0, encodeVInt(0), nil, nil)...)
	server := newRangeServer(t, archive)

	reader := newTestRARReader(t, server.URL, int64(len(archive)))
	files, err := reader.GetFiles()
	if err != nil {
		t.Fatalf("GetFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].Path != name {
		t.Fatalf("GetFiles() = %#v, want one long-name file", files)
	}
}

func TestRAR5RejectsCorruptHeaderBeforeMappingOffsets(t *testing.T) {
	archive := buildRAR5Archive(buildRAR5File("video.mkv", []byte("data"), 0, 0, 0, nil))
	mainLength := len(buildRAR5Block(rar5HeaderMain, 0, encodeVInt(0), nil, nil))
	fileOffset := len("SFX!") + len(Rar5Marker) + mainLength
	archive[fileOffset] ^= 0xff
	server := newRangeServer(t, archive)

	reader := newTestRARReader(t, server.URL, int64(len(archive)))
	if _, err := reader.GetFiles(); err == nil || !strings.Contains(err.Error(), "CRC mismatch") {
		t.Fatalf("GetFiles() error = %v, want CRC mismatch", err)
	}
}

func TestValidateRAR3HeaderMatchesUnRARCRC15Vector(t *testing.T) {
	// Common RAR 2.x/3.x archive header. Its stored 0x90cf HEAD_CRC is the
	// low 16 bits of the finalized IEEE CRC32 over the remaining 11 bytes.
	header := []byte{
		0xcf, 0x90, 0x73, 0x00, 0x00, 0x0d, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	if err := validateRAR3Header(header); err != nil {
		t.Fatalf("validateRAR3Header() error = %v", err)
	}
}

func setRAR3HeaderCRC(header []byte) {
	binary.LittleEndian.PutUint16(header[:2], uint16(crc32.ChecksumIEEE(header[2:])&0xffff))
}

func buildRAR3StoredArchive(name string, payload []byte) (archive []byte, fileHeaderOffset int) {
	mainHeader := make([]byte, 13)
	mainHeader[2] = BlockHeader
	binary.LittleEndian.PutUint16(mainHeader[5:7], uint16(len(mainHeader)))
	setRAR3HeaderCRC(mainHeader)
	fileHeader := make([]byte, 32+len(name))
	fileHeader[2] = BlockFile
	binary.LittleEndian.PutUint16(fileHeader[3:5], uint16(FlagHasData))
	binary.LittleEndian.PutUint16(fileHeader[5:7], uint16(len(fileHeader)))
	binary.LittleEndian.PutUint32(fileHeader[7:11], uint32(len(payload)))
	binary.LittleEndian.PutUint32(fileHeader[11:15], uint32(len(payload)))
	fileHeader[25] = MethodStore
	binary.LittleEndian.PutUint16(fileHeader[26:28], uint16(len(name)))
	copy(fileHeader[32:], name)
	setRAR3HeaderCRC(fileHeader)
	endHeader := make([]byte, 7)
	endHeader[2] = BlockEnd
	binary.LittleEndian.PutUint16(endHeader[5:7], uint16(len(endHeader)))
	setRAR3HeaderCRC(endHeader)

	archive = append([]byte{}, Rar3Marker...)
	archive = append(archive, mainHeader...)
	fileHeaderOffset = len(archive)
	archive = append(archive, fileHeader...)
	archive = append(archive, payload...)
	archive = append(archive, endHeader...)
	return archive, fileHeaderOffset
}

func TestReaderStillSupportsRAR3StoredArchives(t *testing.T) {
	const name = "video.mkv"
	payload := []byte("legacy-media")
	archive, _ := buildRAR3StoredArchive(name, payload)
	server := newRangeServer(t, archive)

	file := &HttpFile{
		URL:        server.URL,
		client:     &http.Client{Timeout: time.Second},
		FileSize:   int64(len(archive)),
		MaxRetries: 0,
	}
	reader, err := newReader(file)
	if err != nil {
		t.Fatalf("newReader() error = %v", err)
	}
	files, err := reader.GetFiles()
	if err != nil {
		t.Fatalf("GetFiles() error = %v", err)
	}
	if reader.Version != 3 || len(files) != 1 || files[0].Path != name {
		t.Fatalf("RAR3 reader version/files = %d/%#v", reader.Version, files)
	}
	extracted, err := reader.ExtractFile(files[0])
	if err != nil || string(extracted) != string(payload) {
		t.Fatalf("ExtractFile() = %q, %v; want %q", extracted, err, payload)
	}
}

func TestRAR3RejectsCorruptHeadersAndAcceptsExactLegacyEOF(t *testing.T) {
	t.Run("corrupt main header", func(t *testing.T) {
		archive, _ := buildRAR3StoredArchive("video.mkv", []byte("media"))
		archive[len(Rar3Marker)] ^= 0xff
		server := newRangeServer(t, archive)
		_, err := newReader(&HttpFile{
			URL: server.URL, client: &http.Client{Timeout: time.Second},
			FileSize: int64(len(archive)), MaxRetries: 0,
		})
		if !errors.Is(err, ErrInvalidFormat) || !strings.Contains(err.Error(), "CRC mismatch") {
			t.Fatalf("newReader() error = %v, want invalid CRC", err)
		}
	})

	t.Run("corrupt file header", func(t *testing.T) {
		archive, fileHeaderOffset := buildRAR3StoredArchive("video.mkv", []byte("media"))
		archive[fileHeaderOffset] ^= 0xff
		server := newRangeServer(t, archive)
		reader, err := newReader(&HttpFile{
			URL: server.URL, client: &http.Client{Timeout: time.Second},
			FileSize: int64(len(archive)), MaxRetries: 0,
		})
		if err != nil {
			t.Fatalf("newReader() error = %v", err)
		}
		if _, err := reader.GetFiles(); !errors.Is(err, ErrInvalidFormat) || !strings.Contains(err.Error(), "CRC mismatch") {
			t.Fatalf("GetFiles() error = %v, want invalid CRC", err)
		}
	})

	t.Run("exact EOF without end marker", func(t *testing.T) {
		archive, _ := buildRAR3StoredArchive("video.mkv", []byte("media"))
		archive = archive[:len(archive)-7]
		server := newRangeServer(t, archive)
		reader, err := newReader(&HttpFile{
			URL: server.URL, client: &http.Client{Timeout: time.Second},
			FileSize: int64(len(archive)), MaxRetries: 0,
		})
		if err != nil {
			t.Fatalf("newReader() error = %v", err)
		}
		files, err := reader.GetFiles()
		if err != nil || len(files) != 1 || files[0].Path != "video.mkv" {
			t.Fatalf("GetFiles() = %#v, %v; want valid legacy EOF", files, err)
		}
	})
}

func TestRAR3SkipsLongBlockDataUsingAddSizeField(t *testing.T) {
	archive, fileHeaderOffset := buildRAR3StoredArchive("video.mkv", []byte("media"))
	serviceHeader := make([]byte, 15)
	serviceHeader[2] = 0x7a
	binary.LittleEndian.PutUint16(serviceHeader[3:5], uint16(FlagHasData))
	binary.LittleEndian.PutUint16(serviceHeader[5:7], uint16(len(serviceHeader)))
	binary.LittleEndian.PutUint32(serviceHeader[7:11], 3)
	copy(serviceHeader[11:], []byte{0xff, 0xff, 0xff, 0xff})
	setRAR3HeaderCRC(serviceHeader)
	withService := append([]byte{}, archive[:fileHeaderOffset]...)
	withService = append(withService, serviceHeader...)
	withService = append(withService, []byte("svc")...)
	withService = append(withService, archive[fileHeaderOffset:]...)
	server := newRangeServer(t, withService)

	reader, err := newReader(&HttpFile{
		URL: server.URL, client: &http.Client{Timeout: time.Second},
		FileSize: int64(len(withService)), MaxRetries: 0,
	})
	if err != nil {
		t.Fatalf("newReader() error = %v", err)
	}
	files, err := reader.GetFiles()
	if err != nil {
		t.Fatalf("GetFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].Path != "video.mkv" {
		t.Fatalf("GetFiles() = %#v, want video.mkv", files)
	}
}

func TestFindMarkerInBytesChoosesEarliestSupportedVersion(t *testing.T) {
	data := append(append([]byte("prefix"), Rar5Marker...), Rar3Marker...)
	position, version := findMarkerInBytes(data)
	if position != len("prefix") || version != 5 {
		t.Fatalf("findMarkerInBytes() = %d/%d, want %d/5", position, version, len("prefix"))
	}
}
