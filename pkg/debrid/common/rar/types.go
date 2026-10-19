package rar

import (
	"fmt"
	"math"
	"net/http"
)

const MethodStore byte = 0x30

// File represents a file entry in a RAR archive
type File struct {
	Path           string
	Size           int64
	CompressedSize int64
	Method         byte
	CRC            uint32
	IsDirectory    bool
	Encrypted      bool
	Redirected     bool
	SplitBefore    bool
	SplitAfter     bool
	DataOffset     int64
	NextOffset     int64
}

// StreamByteRange returns the archive offsets for an entry whose packed bytes
// are already the complete logical file. Compressed entries cannot be exposed
// as seekable media without a decompression layer.
func (f *File) StreamByteRange() (*[2]int64, error) {
	if f == nil {
		return nil, fmt.Errorf("%w: file entry is nil", ErrInvalidFormat)
	}
	if f.IsDirectory {
		return nil, ErrDirectoryExtractNotSupported
	}
	if f.Encrypted {
		return nil, ErrEncryptionNotSupported
	}
	if f.Redirected {
		return nil, ErrRedirectionNotSupported
	}
	if f.SplitBefore || f.SplitAfter {
		return nil, ErrMultiVolumeNotSupported
	}
	if f.Method != MethodStore {
		return nil, fmt.Errorf("%w: method 0x%02x", ErrCompressionNotSupported, f.Method)
	}
	if f.Size <= 0 || f.CompressedSize <= 0 {
		return nil, fmt.Errorf(
			"%w: stored file has invalid unpacked/packed sizes %d/%d",
			ErrInvalidFormat,
			f.Size,
			f.CompressedSize,
		)
	}
	if f.Size != f.CompressedSize {
		return nil, fmt.Errorf(
			"%w: stored file unpacked size %d does not match packed size %d",
			ErrInvalidFormat,
			f.Size,
			f.CompressedSize,
		)
	}
	if f.DataOffset < 0 || f.CompressedSize-1 > math.MaxInt64-f.DataOffset {
		return nil, fmt.Errorf(
			"%w: stored file range %d+%d overflows",
			ErrInvalidFormat,
			f.DataOffset,
			f.CompressedSize,
		)
	}

	return &[2]int64{f.DataOffset, f.DataOffset + f.CompressedSize - 1}, nil
}

// HttpFile represents a RAR file accessible over HTTP
type HttpFile struct {
	URL        string
	Position   int64
	client     *http.Client
	FileSize   int64
	MaxRetries int
}

// Reader reads stored entries from RAR 3/4 and RAR 5 archives.
type Reader struct {
	File         *HttpFile
	ChunkSize    int
	Marker       int64
	Version      int
	HeaderEndPos int64 // Position after the archive header
	Files        []*File
}
