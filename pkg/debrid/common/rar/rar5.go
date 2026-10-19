package rar

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"unicode/utf8"
)

const (
	rar5HeaderMain       = uint64(1)
	rar5HeaderFile       = uint64(2)
	rar5HeaderService    = uint64(3)
	rar5HeaderEncryption = uint64(4)
	rar5HeaderEnd        = uint64(5)

	rar5HeaderExtra       = uint64(0x0001)
	rar5HeaderData        = uint64(0x0002)
	rar5HeaderSplitBefore = uint64(0x0008)
	rar5HeaderSplitAfter  = uint64(0x0010)

	rar5FileDirectory   = uint64(0x0001)
	rar5FileUnixTime    = uint64(0x0002)
	rar5FileCRC32       = uint64(0x0004)
	rar5FileSizeUnknown = uint64(0x0008)

	rar5FileEncryptionExtra  = uint64(0x01)
	rar5FileRedirectionExtra = uint64(0x05)
	rar5MaxHeaderSize        = uint64((1 << 21) - 1)
)

type rar5Block struct {
	headerType uint64
	flags      uint64
	extra      []byte
	body       []byte
	dataSize   int64
	dataOffset int64
	nextOffset int64
}

type vintReader struct {
	data []byte
	pos  int
}

func (r *vintReader) remaining() int {
	return len(r.data) - r.pos
}

func (r *vintReader) readVInt() (uint64, error) {
	var value uint64
	for index := 0; index < 10; index++ {
		if r.pos >= len(r.data) {
			return 0, fmt.Errorf("%w: truncated variable integer", ErrInvalidFormat)
		}
		b := r.data[r.pos]
		r.pos++
		if index == 9 && b > 1 {
			return 0, fmt.Errorf("%w: variable integer overflows 64 bits", ErrInvalidFormat)
		}
		value |= uint64(b&0x7f) << (7 * index)
		if b&0x80 == 0 {
			return value, nil
		}
	}
	return 0, fmt.Errorf("%w: variable integer is too long", ErrInvalidFormat)
}

func (r *vintReader) readUint32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, fmt.Errorf("%w: truncated uint32", ErrInvalidFormat)
	}
	value := binary.LittleEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return value, nil
}

func (r *vintReader) readBytes(length uint64) ([]byte, error) {
	if length > uint64(r.remaining()) {
		return nil, fmt.Errorf("%w: field length %d exceeds header", ErrInvalidFormat, length)
	}
	start := r.pos
	r.pos += int(length)
	return r.data[start:r.pos], nil
}

func (r *Reader) initializeRAR5() error {
	position := r.Marker + int64(len(Rar5Marker))
	block, err := r.readRAR5Block(position)
	if err != nil {
		return fmt.Errorf("read RAR5 archive header: %w", err)
	}
	if block.headerType == rar5HeaderEncryption {
		return ErrEncryptionNotSupported
	}
	if block.headerType != rar5HeaderMain || block.dataSize != 0 {
		return ErrInvalidFormat
	}

	body := &vintReader{data: block.body}
	archiveFlags, err := body.readVInt()
	if err != nil {
		return fmt.Errorf("read RAR5 archive flags: %w", err)
	}
	if archiveFlags&0x0002 != 0 {
		if _, err := body.readVInt(); err != nil {
			return fmt.Errorf("read RAR5 volume number: %w", err)
		}
	}
	if body.remaining() != 0 {
		return fmt.Errorf("%w: unexpected RAR5 archive header data", ErrInvalidFormat)
	}

	r.HeaderEndPos = block.nextOffset
	return nil
}

func (r *Reader) readRAR5Block(position int64) (*rar5Block, error) {
	if position < 0 || position >= r.File.FileSize {
		return nil, fmt.Errorf("%w: RAR5 block offset %d is outside archive", ErrInvalidFormat, position)
	}

	prefixLength := min(int64(7), r.File.FileSize-position)
	prefix, err := r.readBytes(position, int(prefixLength))
	if err != nil {
		return nil, err
	}
	if len(prefix) < 7 {
		return nil, fmt.Errorf("%w: truncated RAR5 block header", ErrInvalidFormat)
	}

	sizeReader := &vintReader{data: prefix[4:]}
	headerDataSize, err := sizeReader.readVInt()
	if err != nil {
		return nil, err
	}
	if headerDataSize == 0 || headerDataSize > rar5MaxHeaderSize {
		return nil, fmt.Errorf("%w: invalid RAR5 header size %d", ErrInvalidFormat, headerDataSize)
	}
	sizeFieldLength := sizeReader.pos
	if sizeFieldLength > 3 {
		return nil, fmt.Errorf("%w: RAR5 header size field exceeds 3 bytes", ErrInvalidFormat)
	}
	if headerDataSize > math.MaxInt64-4-uint64(sizeFieldLength) {
		return nil, fmt.Errorf("%w: RAR5 header size overflows", ErrInvalidFormat)
	}
	totalHeaderSize := int64(4 + sizeFieldLength + int(headerDataSize))
	if totalHeaderSize > r.File.FileSize-position {
		return nil, fmt.Errorf("%w: RAR5 header extends beyond archive", ErrInvalidFormat)
	}

	header := make([]byte, totalHeaderSize)
	copy(header, prefix)
	if remaining := int(totalHeaderSize) - len(prefix); remaining > 0 {
		rest, err := r.readBytes(position+int64(len(prefix)), remaining)
		if err != nil {
			return nil, err
		}
		if len(rest) != remaining {
			return nil, fmt.Errorf("%w: truncated RAR5 block header", ErrInvalidFormat)
		}
		copy(header[len(prefix):], rest)
	}
	wantCRC := binary.LittleEndian.Uint32(header[:4])
	if gotCRC := crc32.ChecksumIEEE(header[4:]); gotCRC != wantCRC {
		return nil, fmt.Errorf(
			"%w: RAR5 header CRC mismatch (got %08x, want %08x)",
			ErrInvalidFormat,
			gotCRC,
			wantCRC,
		)
	}

	headerData := header[4+sizeFieldLength:]
	fields := &vintReader{data: headerData}
	headerType, err := fields.readVInt()
	if err != nil {
		return nil, err
	}
	flags, err := fields.readVInt()
	if err != nil {
		return nil, err
	}

	var extraSize uint64
	if flags&rar5HeaderExtra != 0 {
		extraSize, err = fields.readVInt()
		if err != nil {
			return nil, err
		}
	}
	var dataSize uint64
	if flags&rar5HeaderData != 0 {
		dataSize, err = fields.readVInt()
		if err != nil {
			return nil, err
		}
	}
	if extraSize > uint64(fields.remaining()) {
		return nil, fmt.Errorf("%w: RAR5 extra area exceeds header", ErrInvalidFormat)
	}
	if dataSize > math.MaxInt64 {
		return nil, fmt.Errorf("%w: RAR5 data size overflows", ErrInvalidFormat)
	}

	bodyEnd := len(headerData) - int(extraSize)
	if fields.pos > bodyEnd {
		return nil, fmt.Errorf("%w: RAR5 header fields overlap extra area", ErrInvalidFormat)
	}
	dataOffset := position + totalHeaderSize
	if int64(dataSize) > r.File.FileSize-dataOffset {
		return nil, fmt.Errorf("%w: RAR5 data area extends beyond archive", ErrInvalidFormat)
	}

	return &rar5Block{
		headerType: headerType,
		flags:      flags,
		body:       headerData[fields.pos:bodyEnd],
		extra:      headerData[bodyEnd:],
		dataSize:   int64(dataSize),
		dataOffset: dataOffset,
		nextOffset: dataOffset + int64(dataSize),
	}, nil
}

func (r *Reader) readFilesRAR5() error {
	for position := r.HeaderEndPos; position < r.File.FileSize; {
		block, err := r.readRAR5Block(position)
		if err != nil {
			return fmt.Errorf("read RAR5 block at offset %d: %w", position, err)
		}
		if block.nextOffset <= position {
			return fmt.Errorf("%w: RAR5 block did not advance", ErrInvalidFormat)
		}

		switch block.headerType {
		case rar5HeaderEnd:
			return nil
		case rar5HeaderEncryption:
			return ErrEncryptionNotSupported
		case rar5HeaderFile:
			file, err := parseRAR5File(block)
			if err != nil {
				return fmt.Errorf("parse RAR5 file header at offset %d: %w", position, err)
			}
			r.Files = append(r.Files, file)
		case rar5HeaderMain:
			return fmt.Errorf("%w: duplicate RAR5 archive header", ErrInvalidFormat)
		case rar5HeaderService:
			// Service blocks contain archive metadata, not user-visible files.
		default:
			// Unknown blocks are skipped using their validated header and data sizes.
		}

		position = block.nextOffset
	}
	return fmt.Errorf("%w: RAR5 end header is missing", ErrInvalidFormat)
}

func parseRAR5File(block *rar5Block) (*File, error) {
	body := &vintReader{data: block.body}
	fileFlags, err := body.readVInt()
	if err != nil {
		return nil, err
	}
	unpackedSize, err := body.readVInt()
	if err != nil {
		return nil, err
	}
	if _, err := body.readVInt(); err != nil { // File attributes.
		return nil, err
	}
	if fileFlags&rar5FileUnixTime != 0 {
		if _, err := body.readUint32(); err != nil {
			return nil, err
		}
	}
	var fileCRC uint32
	if fileFlags&rar5FileCRC32 != 0 {
		fileCRC, err = body.readUint32()
		if err != nil {
			return nil, err
		}
	}
	compressionInfo, err := body.readVInt()
	if err != nil {
		return nil, err
	}
	if _, err := body.readVInt(); err != nil { // Host OS.
		return nil, err
	}
	nameSize, err := body.readVInt()
	if err != nil {
		return nil, err
	}
	name, err := body.readBytes(nameSize)
	if err != nil {
		return nil, err
	}
	if len(name) == 0 || !utf8.Valid(name) {
		return nil, fmt.Errorf("%w: invalid RAR5 UTF-8 file name", ErrInvalidFormat)
	}
	if body.remaining() != 0 {
		return nil, fmt.Errorf("%w: unexpected RAR5 file header data", ErrInvalidFormat)
	}

	methodCode := byte((compressionInfo >> 7) & 7)
	method := methodCode
	if methodCode == 0 {
		method = MethodStore
	}
	size := int64(-1)
	if fileFlags&rar5FileSizeUnknown == 0 {
		if unpackedSize > math.MaxInt64 {
			return nil, fmt.Errorf("%w: RAR5 unpacked size overflows", ErrInvalidFormat)
		}
		size = int64(unpackedSize)
	}
	extraTypes, err := parseRAR5ExtraTypes(block.extra)
	if err != nil {
		return nil, err
	}

	return &File{
		Path:           string(name),
		Size:           size,
		CompressedSize: block.dataSize,
		Method:         method,
		CRC:            fileCRC,
		IsDirectory:    fileFlags&rar5FileDirectory != 0,
		Encrypted:      extraTypes[rar5FileEncryptionExtra],
		Redirected:     extraTypes[rar5FileRedirectionExtra],
		SplitBefore:    block.flags&rar5HeaderSplitBefore != 0,
		SplitAfter:     block.flags&rar5HeaderSplitAfter != 0,
		DataOffset:     block.dataOffset,
		NextOffset:     block.nextOffset,
	}, nil
}

func parseRAR5ExtraTypes(extra []byte) (map[uint64]bool, error) {
	types := make(map[uint64]bool)
	reader := &vintReader{data: extra}
	for reader.remaining() > 0 {
		recordSize, err := reader.readVInt()
		if err != nil {
			return nil, err
		}
		if recordSize == 0 || recordSize > uint64(reader.remaining()) {
			return nil, fmt.Errorf("%w: invalid RAR5 extra record size %d", ErrInvalidFormat, recordSize)
		}
		recordData, err := reader.readBytes(recordSize)
		if err != nil {
			return nil, err
		}
		record := &vintReader{data: recordData}
		recordType, err := record.readVInt()
		if err != nil {
			return nil, err
		}
		types[recordType] = true
	}
	return types, nil
}
