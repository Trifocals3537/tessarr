package rar

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestStreamByteRangeValidatesDirectlyStreamableEntries(t *testing.T) {
	tests := []struct {
		name    string
		file    *File
		want    [2]int64
		wantErr error
	}{
		{
			name: "stored file",
			file: &File{Size: 4, CompressedSize: 4, Method: MethodStore, DataOffset: 10},
			want: [2]int64{10, 13},
		},
		{
			name:    "compressed file",
			file:    &File{Size: 8, CompressedSize: 4, Method: 0x33, DataOffset: 10},
			wantErr: ErrCompressionNotSupported,
		},
		{
			name:    "stored size mismatch",
			file:    &File{Size: 8, CompressedSize: 4, Method: MethodStore, DataOffset: 10},
			wantErr: ErrInvalidFormat,
		},
		{
			name:    "empty file",
			file:    &File{Method: MethodStore, DataOffset: 10},
			wantErr: ErrInvalidFormat,
		},
		{
			name: "range overflow",
			file: &File{
				Size: 4, CompressedSize: 4, Method: MethodStore, DataOffset: math.MaxInt64 - 1,
			},
			wantErr: ErrInvalidFormat,
		},
		{
			name:    "directory",
			file:    &File{IsDirectory: true},
			wantErr: ErrDirectoryExtractNotSupported,
		},
		{
			name: "encrypted file",
			file: &File{
				Size: 4, CompressedSize: 4, Method: MethodStore, DataOffset: 10, Encrypted: true,
			},
			wantErr: ErrEncryptionNotSupported,
		},
		{
			name: "redirected file",
			file: &File{
				Size: 4, CompressedSize: 4, Method: MethodStore, DataOffset: 10, Redirected: true,
			},
			wantErr: ErrRedirectionNotSupported,
		},
		{
			name: "split file",
			file: &File{
				Size: 4, CompressedSize: 4, Method: MethodStore, DataOffset: 10, SplitAfter: true,
			},
			wantErr: ErrMultiVolumeNotSupported,
		},
		{name: "nil entry", wantErr: ErrInvalidFormat},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.file.StreamByteRange()
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("StreamByteRange() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil || got == nil || *got != test.want {
				t.Fatalf("StreamByteRange() = %v, %v; want %v", got, err, test.want)
			}
		})
	}
}

func TestParseFileHeaderPreservesUnsupportedStreamFlags(t *testing.T) {
	const name = "video.mkv"
	header := make([]byte, 32+len(name))
	header[2] = BlockFile
	binary.LittleEndian.PutUint16(
		header[3:5],
		uint16(FlagPassword|FlagSplitBefore|FlagSplitAfter|FlagHasData),
	)
	binary.LittleEndian.PutUint16(header[5:7], uint16(len(header)))
	binary.LittleEndian.PutUint32(header[7:11], 4)
	binary.LittleEndian.PutUint32(header[11:15], 4)
	header[15] = 3 // Unix host OS.
	header[25] = MethodStore
	binary.LittleEndian.PutUint16(header[26:28], uint16(len(name)))
	binary.LittleEndian.PutUint32(header[28:32], 0xA000) // Unix symbolic link.
	copy(header[32:], name)

	file, err := (&Reader{}).parseFileHeader(header, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !file.Encrypted || !file.Redirected || !file.SplitBefore || !file.SplitAfter {
		t.Fatalf(
			"parsed streamability flags = encrypted:%v redirected:%v before:%v after:%v",
			file.Encrypted,
			file.Redirected,
			file.SplitBefore,
			file.SplitAfter,
		)
	}
	if _, err := file.StreamByteRange(); !errors.Is(err, ErrEncryptionNotSupported) {
		t.Fatalf("StreamByteRange() error = %v, want encrypted-entry rejection", err)
	}
}

func TestParseFileHeaderRejectsMalformedSizesAndOffsets(t *testing.T) {
	buildHeader := func(flags int, name string) []byte {
		header := make([]byte, 32+len(name))
		header[2] = BlockFile
		binary.LittleEndian.PutUint16(header[3:5], uint16(flags))
		binary.LittleEndian.PutUint16(header[5:7], uint16(len(header)))
		binary.LittleEndian.PutUint32(header[7:11], 4)
		binary.LittleEndian.PutUint32(header[11:15], 4)
		header[25] = MethodStore
		binary.LittleEndian.PutUint16(header[26:28], uint16(len(name)))
		copy(header[32:], name)
		return header
	}

	tests := []struct {
		name     string
		header   []byte
		position int64
	}{
		{
			name:   "truncated high sizes",
			header: buildHeader(FlagHasData|FlagHasHighSize, ""),
		},
		{
			name:   "missing data flag",
			header: buildHeader(0, "video.mkv"),
		},
		{
			name: "invalid file name size",
			header: func() []byte {
				header := buildHeader(FlagHasData, "")
				binary.LittleEndian.PutUint16(header[26:28], 1)
				return header
			}(),
		},
		{
			name: "unknown unpacked size",
			header: func() []byte {
				header := buildHeader(FlagHasData, "video.mkv")
				binary.LittleEndian.PutUint32(header[11:15], math.MaxUint32)
				return header
			}(),
		},
		{
			name:     "header offset overflow",
			header:   buildHeader(FlagHasData, "video.mkv"),
			position: math.MaxInt64 - 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (&Reader{}).parseFileHeader(test.header, test.position); !errors.Is(err, ErrInvalidFormat) {
				t.Fatalf("parseFileHeader() error = %v, want invalid format", err)
			}
		})
	}
}
