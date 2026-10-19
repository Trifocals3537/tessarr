package hybrid

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// Log format:
// [Header: 16 bytes]
//   - Magic: 4 bytes "HYBR"
//   - Version: 4 bytes
//   - Reserved: 8 bytes
//
// [Record: variable]
//   - KeyLen: 4 bytes
//   - Key: KeyLen bytes
//   - ValueLen: 4 bytes
//   - Value: ValueLen bytes
//   - Flags: 1 byte (bit 0 = deleted)
//   - CategoryLen: 2 bytes
//   - Category: CategoryLen bytes
//   - ProviderLen: 2 bytes
//   - Provider: ProviderLen bytes
//   - StatusLen: 2 bytes
//   - Status: StatusLen bytes
//   - NameLen: 2 bytes
//   - Name: NameLen bytes
//   - TotalSize: 8 bytes
//   - Checksum: 4 bytes (CRC32)

const (
	logMagic               = "HYBR"
	logVersion             = uint32(3) // v3: added Protocol, Bad, AddedOn
	logHeaderSize          = 16
	maxLogRecordBytes      = 256 << 20
	maxLogKeyBytes         = 1 << 20
	maxUint16EncodedLength = 1<<16 - 1
	logRecordFixedBytes    = 35
)

// LogRecord represents a single record in the log
type LogRecord struct {
	Key       string
	Offset    int64 // Offset to value data in file
	Size      int32 // Size of value data
	Deleted   bool
	Category  string
	Provider  string
	Status    string
	Name      string
	TotalSize int64
	Protocol  string // "torrent" or "nzb"
	Bad       bool
	AddedOn   int64 // Unix timestamp
}

// appendLog is an append-only log file
type appendLog struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	writePos int64
	version  uint32 // File format version (for backward compatibility)
}

// openAppendLog opens an existing log or creates a new one
func openAppendLog(path string) (*appendLog, error) {
	file, created, err := openAppendLogFile(path, true)
	if err != nil {
		return nil, err
	}
	return initializeAppendLog(file, path, created)
}

// openExistingAppendLog opens an existing regular append log without creating
// it. Compaction recovery uses this so inspecting a missing artifact can never
// manufacture an empty database and accidentally make it authoritative.
func openExistingAppendLog(path string) (*appendLog, error) {
	file, _, err := openAppendLogFile(path, false)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if info.Size() == 0 {
		return nil, errors.Join(
			fmt.Errorf("existing append log is empty: %s", path),
			file.Close(),
		)
	}
	return initializeAppendLog(file, path, false)
}

// createAppendLogExclusive creates a fresh regular append log and refuses to
// follow or replace an existing path. In particular, a stale .compact symlink
// cannot redirect compaction writes into an unrelated file.
func createAppendLogExclusive(path string) (*appendLog, error) {
	file, _, err := safepath.OpenRegularFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
		closeErr := file.Close()
		removeErr := safepath.RemoveRegularFile(path)
		return nil, errors.Join(
			statErr,
			closeErr,
			removeErr,
			fmt.Errorf("append log path is not a regular file: %s", path),
		)
	}
	return initializeAppendLog(file, path, true)
}

func openAppendLogFile(path string, create bool) (*os.File, bool, error) {
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
	}
	return safepath.OpenRegularFile(path, flag, 0644)
}

func initializeAppendLog(file *os.File, path string, created bool) (*appendLog, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	log := &appendLog{
		file:    file,
		path:    path,
		version: logVersion, // Default to current version for new files
	}

	if info.Size() == 0 {
		// New file - write header
		if err := log.writeHeader(); err != nil {
			closeErr := file.Close()
			if created {
				return nil, errors.Join(err, closeErr, safepath.RemoveRegularFile(path))
			}
			return nil, errors.Join(err, closeErr)
		}
		log.writePos = logHeaderSize
	} else {
		// Existing file - validate header and find write position
		version, err := log.validateHeader()
		if err != nil {
			return nil, errors.Join(err, file.Close())
		}
		log.version = version
		log.writePos = info.Size()
	}

	return log, nil
}

func (l *appendLog) writeHeader() error {
	header := make([]byte, logHeaderSize)
	copy(header[0:4], logMagic)
	binary.LittleEndian.PutUint32(header[4:8], logVersion)
	// bytes 8-16 reserved

	_, err := l.file.WriteAt(header, 0)
	return err
}

func (l *appendLog) validateHeader() (uint32, error) {
	header := make([]byte, logHeaderSize)
	if _, err := l.file.ReadAt(header, 0); err != nil {
		return 0, fmt.Errorf("failed to read header: %w", err)
	}

	if string(header[0:4]) != logMagic {
		return 0, fmt.Errorf("invalid magic: expected %s, got %s", logMagic, string(header[0:4]))
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version > logVersion {
		return 0, fmt.Errorf("unsupported version: %d (max: %d)", version, logVersion)
	}

	return version, nil
}

// Append writes a record to the log and returns the offset and size of the value
func (l *appendLog) Append(key string, value []byte, deleted bool, category, provider, status, name string, totalSize int64, protocol string, bad bool, addedOn int64) (offset int64, size int32, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	keyBytes := []byte(key)
	catBytes := []byte(category)
	provBytes := []byte(provider)
	statusBytes := []byte(status)
	nameBytes := []byte(name)
	protocolBytes := []byte(protocol)

	recordSize, err := encodedLogRecordSize(
		len(keyBytes),
		len(value),
		len(catBytes),
		len(provBytes),
		len(statusBytes),
		len(nameBytes),
		len(protocolBytes),
	)
	if err != nil {
		return 0, 0, err
	}
	if recordSize < logRecordFixedBytes || recordSize > maxLogRecordBytes {
		return 0, 0, fmt.Errorf("invalid append log record size %d", recordSize)
	}

	buf := make([]byte, recordSize)
	pos := 0

	// Key
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(keyBytes)))
	pos += 4
	copy(buf[pos:], keyBytes)
	pos += len(keyBytes)

	// Value
	binary.LittleEndian.PutUint32(buf[pos:], uint32(len(value)))
	pos += 4
	valueOffset := l.writePos + int64(pos)
	copy(buf[pos:], value)
	pos += len(value)

	// Flags (bit 0 = deleted, bit 1 = bad)
	var flags byte
	if deleted {
		flags |= 1
	}
	if bad {
		flags |= 2
	}
	buf[pos] = flags
	pos++

	// Category
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(catBytes)))
	pos += 2
	copy(buf[pos:], catBytes)
	pos += len(catBytes)

	// Provider
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(provBytes)))
	pos += 2
	copy(buf[pos:], provBytes)
	pos += len(provBytes)

	// Status
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(statusBytes)))
	pos += 2
	copy(buf[pos:], statusBytes)
	pos += len(statusBytes)

	// Name
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(nameBytes)))
	pos += 2
	copy(buf[pos:], nameBytes)
	pos += len(nameBytes)

	// TotalSize
	binary.LittleEndian.PutUint64(buf[pos:], uint64(totalSize))
	pos += 8

	// Protocol
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(protocolBytes)))
	pos += 2
	copy(buf[pos:], protocolBytes)
	pos += len(protocolBytes)

	// AddedOn
	binary.LittleEndian.PutUint64(buf[pos:], uint64(addedOn))
	pos += 8

	// Write to file
	if _, err := l.file.WriteAt(buf, l.writePos); err != nil {
		return 0, 0, err
	}

	l.writePos += int64(recordSize)

	return valueOffset, int32(len(value)), nil
}

func encodedLogRecordSize(key, value, category, provider, status, name, protocol int) (int, error) {
	if key < 0 || key > maxLogKeyBytes {
		return 0, fmt.Errorf("append log key exceeds %d bytes", maxLogKeyBytes)
	}

	for _, field := range []struct {
		name   string
		length int
	}{
		{name: "category", length: category},
		{name: "provider", length: provider},
		{name: "status", length: status},
		{name: "name", length: name},
		{name: "protocol", length: protocol},
	} {
		if field.length < 0 || field.length > maxUint16EncodedLength {
			return 0, fmt.Errorf("append log %s exceeds %d bytes", field.name, maxUint16EncodedLength)
		}
	}

	recordSize := logRecordFixedBytes
	for _, length := range []int{key, value, category, provider, status, name, protocol} {
		if length < 0 || length > maxLogRecordBytes-recordSize {
			return 0, fmt.Errorf("append log record exceeds %d bytes", maxLogRecordBytes)
		}
		recordSize += length
	}
	return recordSize, nil
}

// ReadAt reads value data at the given offset into a freshly allocated buffer.
func (l *appendLog) ReadAt(offset int64, size int32) ([]byte, error) {
	if size < 0 || size > maxLogRecordBytes {
		return nil, fmt.Errorf("invalid append log read size %d", size)
	}
	buf := make([]byte, size)
	_, err := l.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// ReadAtInto reads size bytes at offset into buf, growing it only when it is
// too small, and returns the filled slice (which aliases buf). The result is
// valid only until buf is next reused — callers that retain the bytes must
// copy them. Used by scan paths to avoid an allocation per record.
func (l *appendLog) ReadAtInto(offset int64, size int32, buf []byte) ([]byte, error) {
	if size < 0 || size > maxLogRecordBytes {
		return nil, fmt.Errorf("invalid append log read size %d", size)
	}
	if cap(buf) < int(size) {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}
	if _, err := l.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// Iterate scans the log and calls fn for each record. It reads the file
// sequentially through a buffered reader rather than issuing a positioned read
// per field, so recovering N records costs ~one syscall per buffer-full instead
// of ~16 per record. The value payload is skipped (recovery only needs
// metadata + its on-disk offset).
func (l *appendLog) Iterate(fn func(*LogRecord) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Seek(logHeaderSize, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(l.file, 1<<20)

	pos := int64(logHeaderSize)
	fileSize := l.writePos
	var fixed [8]byte // scratch for fixed-width fields
	var sbuf []byte   // reused scratch for length-prefixed strings

	for pos < fileSize {
		record, nextPos, err := readRecordFrom(r, pos, l.version, fixed[:], &sbuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := fn(record); err != nil {
			return err
		}
		pos = nextPos
	}

	return nil
}

// ValidateComplete verifies that every byte after the header belongs to a
// complete record. Iterate intentionally tolerates a torn final append for
// normal crash recovery; compaction artifacts must be stricter because startup
// may promote one to the canonical database.
func (l *appendLog) ValidateComplete() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.file.Seek(logHeaderSize, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(l.file, 1<<20)

	pos := int64(logHeaderSize)
	fileSize := l.writePos
	var fixed [8]byte
	var sbuf []byte
	for pos < fileSize {
		_, nextPos, err := readRecordFrom(r, pos, l.version, fixed[:], &sbuf)
		if err != nil {
			return fmt.Errorf("incomplete record at offset %d: %w", pos, err)
		}
		if nextPos <= pos || nextPos > fileSize {
			return fmt.Errorf("invalid record boundary %d after offset %d (file size %d)", nextPos, pos, fileSize)
		}
		pos = nextPos
	}
	if pos != fileSize {
		return fmt.Errorf("append log ended at %d, expected %d", pos, fileSize)
	}
	return nil
}

// readRecordFrom parses one record from r, which must be positioned at the
// record starting at file offset startPos. fixed is an >=8 byte scratch for
// fixed-width fields; *sbuf is a reused growable buffer for string fields
// (string() copies out of it, so reuse across calls is safe). The value payload
// is discarded, not read. Returns the record and the next record's offset.
func readRecordFrom(r *bufio.Reader, startPos int64, version uint32, fixed []byte, sbuf *[]byte) (*LogRecord, int64, error) {
	pos := startPos

	readU32 := func() (uint32, error) {
		if _, err := io.ReadFull(r, fixed[:4]); err != nil {
			return 0, err
		}
		pos += 4
		return binary.LittleEndian.Uint32(fixed[:4]), nil
	}
	readU16 := func() (uint16, error) {
		if _, err := io.ReadFull(r, fixed[:2]); err != nil {
			return 0, err
		}
		pos += 2
		return binary.LittleEndian.Uint16(fixed[:2]), nil
	}
	readU64 := func() (int64, error) {
		if _, err := io.ReadFull(r, fixed[:8]); err != nil {
			return 0, err
		}
		pos += 8
		return int64(binary.LittleEndian.Uint64(fixed[:8])), nil
	}
	readStr := func(n int) (string, error) {
		if n == 0 {
			return "", nil
		}
		if cap(*sbuf) < n {
			*sbuf = make([]byte, n)
		}
		b := (*sbuf)[:n]
		if _, err := io.ReadFull(r, b); err != nil {
			return "", err
		}
		pos += int64(n)
		return string(b), nil
	}

	// Key
	keyLen, err := readU32()
	if err != nil {
		return nil, 0, err
	}
	if keyLen > maxLogKeyBytes {
		return nil, 0, fmt.Errorf("invalid key length: %d", keyLen)
	}
	key, err := readStr(int(keyLen))
	if err != nil {
		return nil, 0, err
	}

	// Value: record its offset, then skip the payload.
	valueLen, err := readU32()
	if err != nil {
		return nil, 0, err
	}
	valueOffset := pos
	if _, err := r.Discard(int(valueLen)); err != nil {
		return nil, 0, err
	}
	pos += int64(valueLen)

	// Flags
	if _, err := io.ReadFull(r, fixed[:1]); err != nil {
		return nil, 0, err
	}
	pos++
	deleted := fixed[0]&1 != 0
	bad := version >= 3 && fixed[0]&2 != 0

	readField := func() (string, error) {
		n, err := readU16()
		if err != nil {
			return "", err
		}
		return readStr(int(n))
	}

	category, err := readField()
	if err != nil {
		return nil, 0, err
	}
	provider, err := readField()
	if err != nil {
		return nil, 0, err
	}
	status, err := readField()
	if err != nil {
		return nil, 0, err
	}
	name, err := readField()
	if err != nil {
		return nil, 0, err
	}

	totalSize, err := readU64()
	if err != nil {
		return nil, 0, err
	}

	var protocol string
	var addedOn int64
	if version >= 3 {
		if protocol, err = readField(); err != nil {
			return nil, 0, err
		}
		if addedOn, err = readU64(); err != nil {
			return nil, 0, err
		}
	}

	return &LogRecord{
		Key:       key,
		Offset:    valueOffset,
		Size:      int32(valueLen),
		Deleted:   deleted,
		Category:  category,
		Provider:  provider,
		Status:    status,
		Name:      name,
		TotalSize: totalSize,
		Protocol:  protocol,
		Bad:       bad,
		AddedOn:   addedOn,
	}, pos, nil
}

// Sync flushes data to disk
func (l *appendLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

// Close closes the log file
func (l *appendLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// Size returns the current file size
func (l *appendLog) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.writePos
}

// Truncate truncates the log at the given position (for recovery)
func (l *appendLog) Truncate(pos int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.file.Truncate(pos); err != nil {
		return err
	}
	l.writePos = pos
	return nil
}
