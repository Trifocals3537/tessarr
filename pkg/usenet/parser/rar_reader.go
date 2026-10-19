package parser

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/Trifocals3537/tessarr/internal/crypto"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/usenet/types"
)

// rarReader provides a continuous stream across RAR volumes
// It can efficiently skip large data sections without downloading them
type rarReader struct {
	ctx      context.Context
	manager  *nntp.Client
	volumes  []*types.Volume
	position int64 // Current absolute position in the archive

	currentVolumeIndex   int
	currentSegmentIndex  int
	currentSegmentData   []byte
	currentSegmentOffset int // Offset within current segment data
}

func newRarReader(ctx context.Context, manager *nntp.Client, volumes []*types.Volume) *rarReader {
	return &rarReader{
		ctx:                 ctx,
		manager:             manager,
		volumes:             volumes,
		position:            0,
		currentVolumeIndex:  0,
		currentSegmentIndex: 0,
	}
}

// Read implements io.Reader
func (r *rarReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	totalRead := 0

	for totalRead < len(p) {
		// Ensure we have current segment data
		if r.currentSegmentData == nil || r.currentSegmentOffset >= len(r.currentSegmentData) {
			if err := r.loadNextSegment(); err != nil {
				if totalRead > 0 {
					return totalRead, err
				}
				return 0, err
			}
		}

		// Copy from current segment
		n := copy(p[totalRead:], r.currentSegmentData[r.currentSegmentOffset:])
		r.currentSegmentOffset += n
		r.position += int64(n)
		totalRead += n
	}

	return totalRead, nil
}

// Skip efficiently skips n bytes without downloading unnecessary data
func (r *rarReader) Skip(n int64) error {
	if n < 0 {
		return fmt.Errorf("negative RAR data size")
	}
	var remaining int64
	for _, volume := range r.volumes {
		remaining += volume.Size
	}
	if r.position > remaining || n > remaining-r.position {
		return io.ErrUnexpectedEOF
	}
	if n == 0 {
		return nil
	}

	// Fast path: skip within current segment if possible
	if r.currentSegmentData != nil {
		remaining := len(r.currentSegmentData) - r.currentSegmentOffset
		if int64(remaining) >= n {
			r.currentSegmentOffset += int(n)
			r.position += n
			return nil
		}
		// Skip rest of current segment
		r.position += int64(remaining)
		n -= int64(remaining)
		r.currentSegmentData = nil
	}

	// Skip entire segments without downloading
	for n > 0 {
		if r.currentVolumeIndex >= len(r.volumes) {
			return io.EOF
		}

		volume := r.volumes[r.currentVolumeIndex]
		if r.currentSegmentIndex >= len(volume.Segments) {
			// Move to next volume
			r.currentVolumeIndex++
			r.currentSegmentIndex = 0
			continue
		}

		segment := volume.Segments[r.currentSegmentIndex]
		segmentSize := segment.Bytes

		if n >= segmentSize {
			// Skip entire segment
			r.currentSegmentIndex++
			r.position += segmentSize
			n -= segmentSize
		} else {
			// Need to load this segment and skip within it
			if err := r.loadNextSegment(); err != nil {
				return err
			}
			r.currentSegmentOffset = int(n)
			r.position += n
			n = 0
		}
	}

	return nil
}

// loadNextSegment loads the next segment's data
func (r *rarReader) loadNextSegment() error {
	for {
		if r.currentVolumeIndex >= len(r.volumes) {
			return io.EOF
		}

		volume := r.volumes[r.currentVolumeIndex]
		if r.currentSegmentIndex >= len(volume.Segments) {
			// Move to next volume
			r.currentVolumeIndex++
			r.currentSegmentIndex = 0
			continue
		}

		segment := volume.Segments[r.currentSegmentIndex]

		// Download segment using manager
		var data []byte
		err := r.manager.ExecuteWithFailover(r.ctx, func(conn *nntp.Connection) error {
			d, meta, e := conn.GetDecodedBodyWithMetadata(segment.MessageID)
			if e == nil && (meta == nil || int64(len(d)) != segment.Bytes || meta.Size != volume.Size ||
				meta.Begin != segment.StartOffset+1 || meta.End != segment.EndOffset+1) {
				return fmt.Errorf("article yEnc geometry does not match RAR volume offsets")
			}
			data = d
			return e
		})
		if err != nil {
			return fmt.Errorf("failed to fetch segment: %w", err)
		}

		r.currentSegmentData = data
		r.currentSegmentOffset = 0
		r.currentSegmentIndex++
		return nil
	}
}

// Position returns the current position in the stream
func (r *rarReader) Position() int64 {
	return r.position
}

// AbsoluteToVolumeOffset converts an absolute position in the stream to (volumeIndex, offsetWithinVolume)
func (r *rarReader) AbsoluteToVolumeOffset(absolutePos int64) (volumeIndex int, offsetInVolume int64) {
	currentPos := int64(0)

	for volIdx, volume := range r.volumes {
		// Calculate total size of this volume (sum of all segment sizes)
		volumeSize := int64(0)
		for _, segment := range volume.Segments {
			volumeSize += segment.Bytes
		}

		// Check if the position falls within this volume
		if absolutePos < currentPos+volumeSize {
			// Position is in this volume
			offsetInVol := absolutePos - currentPos
			return volIdx, offsetInVol
		}

		currentPos += volumeSize
	}

	// If we get here, position is beyond all volumes
	// Return last volume and offset
	if len(r.volumes) > 0 {
		return len(r.volumes) - 1, absolutePos
	}
	return 0, absolutePos
}

// parseRAR5StreamResult contains the result of parsing a RAR5 stream
type parseRAR5StreamResult struct {
	Files             []*RARFileEntry
	IsHeaderEncrypted bool
	EncryptionKey     []byte // AES key for file data decryption (if encrypted)
	EncryptionIV      []byte // AES IV for file data decryption (if encrypted)
}

// parseRAR5Stream parses RAR 5.0 headers from a stream reader
// This properly tracks offsets by reading headers sequentially and skipping data
// If password is provided and headers are encrypted, it will decrypt them
func (p *RARParser) parseRAR5Stream(stream *rarReader, volumeIndex int, volumeName string, password string) (*parseRAR5StreamResult, error) {
	result := &parseRAR5StreamResult{
		Files: make([]*RARFileEntry, 0),
	}

	var encryptionKey []byte // Key for decrypting file data

	// Stream position is already at 8 (after signature)
	for {
		// Record position before reading header
		headerStartPos := stream.Position()

		// Read header
		header, headerSize, dataSize, err := p.readRAR5HeaderFromStream(stream)
		if err != nil {
			return nil, fmt.Errorf("failed to read RAR5 header: %w", err)
		}

		// Check for encryption header - this means headers are encrypted
		if header.Type == RAR5HeaderTypeEncrypt {
			result.IsHeaderEncrypted = true

			// Parse encryption header to get salt and kdfCount
			encHeader, err := crypto.ParseEncryptionHeader(header.Data)
			if err != nil {
				return nil, err
			}

			// If no password provided, we can't continue
			if password == "" {
				break
			}

			// Derive key from password
			keys := crypto.DeriveKeys([]byte(password), encHeader.Salt, encHeader.KdfCount)

			// Verify password if check is present
			if encHeader.HasPwCheck {
				if !crypto.VerifyPassword(keys, encHeader.PwCheck) {
					return nil, crypto.ErrBadPassword
				}
			}

			// Store the encryption key for file data decryption
			encryptionKey = keys.Key
			result.EncryptionKey = keys.Key

			// Now we need to read encrypted headers
			// Each encrypted header is: 16-byte IV + encrypted data (aligned to 16 bytes)
			// Continue parsing with decryption enabled
			for {
				// Read IV (16 bytes)
				iv := make([]byte, crypto.BlockSize)
				if _, err := io.ReadFull(stream, iv); err != nil {
					return nil, fmt.Errorf("failed to read encrypted RAR5 IV: %w", err)
				}
				result.EncryptionIV = iv

				// Read encrypted header
				encHeader, encHeaderSize, encDataSize, err := p.readAndDecryptRAR5Header(stream, encryptionKey, iv)
				if err != nil {
					return nil, fmt.Errorf("failed to read encrypted RAR5 header: %w", err)
				}

				// Parse the decrypted header
				if encHeader.Type == RAR5HeaderTypeFile {
					headerPos := stream.Position() - int64(encHeaderSize)
					_, offsetInVol := stream.AbsoluteToVolumeOffset(headerPos + int64(encHeaderSize))

					file := p.parseRAR5FileHeader(encHeader.Data, volumeIndex, volumeName, offsetInVol, encDataSize, password)
					if file == nil {
						return nil, fmt.Errorf("invalid encrypted RAR5 file header")
					}
					file.SplitBefore = encHeader.Flags&RAR5HeaderFlagSplitBefore != 0
					file.SplitAfter = encHeader.Flags&RAR5HeaderFlagSplitAfter != 0
					// Headers being encrypted does NOT mean data is encrypted;
					// file.IsEncrypted is set from the file's extra area instead.
					result.Files = append(result.Files, file)
				}

				// Skip data section
				if encDataSize > 0 {
					// Data is also encrypted, need to account for padding
					paddedSize := ((encDataSize + crypto.BlockSize - 1) / crypto.BlockSize) * crypto.BlockSize
					if err := stream.Skip(paddedSize); err != nil {
						return nil, fmt.Errorf("failed to skip encrypted RAR5 data: %w", err)
					}
				}

				if encHeader.Type == RAR5HeaderTypeEndOfArc {
					break
				}
			}
			break
		}

		// Data offset is immediately after the header (absolute position in stream)
		dataOffsetAbsolute := headerStartPos + int64(headerSize)

		// Parse file headers
		if header.Type == RAR5HeaderTypeFile {
			_, offsetInVol := stream.AbsoluteToVolumeOffset(dataOffsetAbsolute)

			// Use the volumeIndex parameter passed to this function, not the stream's volume index
			// because each stream only contains one volume
			file := p.parseRAR5FileHeader(header.Data, volumeIndex, volumeName, offsetInVol, dataSize, password)
			if file == nil {
				return nil, fmt.Errorf("invalid RAR5 file header")
			}
			file.SplitBefore = header.Flags&RAR5HeaderFlagSplitBefore != 0
			file.SplitAfter = header.Flags&RAR5HeaderFlagSplitAfter != 0
			result.Files = append(result.Files, file)
		}

		// Skip the data section to get to the next header
		if dataSize > 0 {
			if err := stream.Skip(dataSize); err != nil {
				return nil, fmt.Errorf("failed to skip data section: %w", err)
			}
		}

		// Stop at end of archive
		if header.Type == RAR5HeaderTypeEndOfArc {
			break
		}
	}

	return result, nil
}

// readAndDecryptRAR5Header reads an encrypted RAR5 header from stream
func (p *RARParser) readAndDecryptRAR5Header(stream *rarReader, key, iv []byte) (*rar5HeaderData, int, int64, error) {
	// For encrypted headers, we need to read enough data and decrypt
	// The header starts with encrypted CRC+size+type+flags...
	// We read in blocks and decrypt

	// Read first block to get header size
	firstBlock := make([]byte, crypto.BlockSize)
	if _, err := io.ReadFull(stream, firstBlock); err != nil {
		return nil, 0, 0, err
	}

	// Save ciphertext for IV chaining
	firstBlockCipher := make([]byte, len(firstBlock))
	copy(firstBlockCipher, firstBlock)

	// Decrypt first block
	if err := crypto.DecryptBlock(firstBlock, key, iv); err != nil {
		return nil, 0, 0, err
	}

	// Now parse the decrypted data as a normal header
	// Skip CRC (4 bytes), read size
	if len(firstBlock) < 5 {
		return nil, 0, 0, fmt.Errorf("decrypted block too small")
	}

	// Read header size from decrypted data
	r := bytes.NewReader(firstBlock[4:])
	headerSize, err := readVInt(r)
	if err != nil {
		return nil, 0, 0, err
	}

	// Calculate how many more blocks we need
	// Sanity check: limit header size to 64KB to prevent OOM from corrupt decrypted data
	if headerSize > 65536 {
		return nil, 0, 0, fmt.Errorf("encrypted header size too large: %d", headerSize)
	}

	totalEncryptedSize := int(headerSize) + 4 // header + CRC
	totalEncryptedSize = ((totalEncryptedSize + crypto.BlockSize - 1) / crypto.BlockSize) * crypto.BlockSize

	if totalEncryptedSize > crypto.BlockSize {
		// Read remaining blocks
		remaining := make([]byte, totalEncryptedSize-crypto.BlockSize)
		if _, err := io.ReadFull(stream, remaining); err != nil {
			return nil, 0, 0, err
		}

		// Create new IV for CBC continuation (last ciphertext block)
		newIV := firstBlockCipher[len(firstBlockCipher)-crypto.BlockSize:]
		if err := crypto.DecryptBlock(remaining, key, newIV); err != nil {
			return nil, 0, 0, err
		}

		firstBlock = append(firstBlock, remaining...)
	}

	// Parse the decrypted header data
	r = bytes.NewReader(firstBlock[4:]) // Skip CRC

	// Read header size again
	headerSize, vintBytes, _, err := readVIntFromReaderWithBytes(r)
	if err != nil {
		return nil, 0, 0, err
	}

	headerType, n, err := readVIntFromReader(r)
	if err != nil {
		return nil, 0, 0, err
	}

	headerFlags, n2, err := readVIntFromReader(r)
	if err != nil {
		return nil, 0, 0, err
	}

	bytesConsumed := vintBytes + n + n2

	// Read extra area size if present
	if headerFlags&RAR5HeaderFlagExtraArea != 0 {
		_, n, err = readVIntFromReader(r)
		if err != nil {
			return nil, 0, 0, err
		}
		bytesConsumed += n
	}

	// Read data area size if present
	var dataAreaSize int64
	if headerFlags&RAR5HeaderFlagDataArea != 0 {
		dataSize, n, err := readVIntFromReader(r)
		if err != nil {
			return nil, 0, 0, err
		}
		bytesConsumed += n
		dataAreaSize = int64(dataSize)
	}

	// Read remaining header data
	remainingSize := int(headerSize) - bytesConsumed
	var headerData []byte
	if remainingSize > 0 {
		headerData = make([]byte, remainingSize)
		if _, err := io.ReadFull(r, headerData); err != nil {
			return nil, 0, 0, err
		}
	}

	return &rar5HeaderData{
		Type:  headerType,
		Flags: headerFlags,
		Data:  headerData,
	}, totalEncryptedSize, dataAreaSize, nil
}

// readRAR5HeaderFromStream reads a RAR5 header from the stream
// Optimized: reads header content in one call after getting header size
func (p *RARParser) readRAR5HeaderFromStream(stream *rarReader) (*rar5HeaderData, int, int64, error) {
	// Read header CRC (4 bytes) + first few bytes that contain the header size vint
	// We read a small initial buffer to get the CRC and header size
	initialBuf := make([]byte, 16)                // 4 bytes CRC + up to 10 bytes vint + some extra
	n, err := io.ReadFull(stream, initialBuf[:5]) // Read CRC + at least 1 byte of size
	if err != nil {
		return nil, 0, 0, err
	}

	// CRC covers the size vint and the entire header, including the extra area.
	pos := 4

	// Read header size vint from buffer, continuing to read more if needed
	headerSize, vintBytes := parseVIntFromBuffer(initialBuf[pos:n])
	if vintBytes == 0 {
		// Need more bytes for the vint - rare case for large headers
		for vintBytes == 0 && n < len(initialBuf) {
			extra, err := stream.Read(initialBuf[n : n+1])
			if err != nil {
				return nil, 0, 0, err
			}
			n += extra
			headerSize, vintBytes = parseVIntFromBuffer(initialBuf[pos:n])
		}
		if vintBytes == 0 {
			return nil, 0, 0, fmt.Errorf("failed to read header size vint")
		}
	}

	headerSizeVintBytes := vintBytes
	pos += vintBytes

	// Sanity check
	if headerSize > 65536 {
		return nil, 0, 0, fmt.Errorf("invalid RAR5 header size: %d (too large)", headerSize)
	}

	// Now read the entire remaining header content in one go
	// headerSize is size starting from header type field
	headerContent := make([]byte, int(headerSize))

	// Copy any bytes we already read past the size vint
	alreadyRead := n - pos
	if alreadyRead > 0 {
		copy(headerContent, initialBuf[pos:n])
	}

	// Read the rest if needed
	if alreadyRead < int(headerSize) {
		_, err := io.ReadFull(stream, headerContent[alreadyRead:])
		if err != nil {
			return nil, 0, 0, err
		}
	}
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(initialBuf[4:pos])
	_, _ = checksum.Write(headerContent)
	if checksum.Sum32() != binary.LittleEndian.Uint32(initialBuf[:4]) {
		return nil, 0, 0, fmt.Errorf("RAR5 header CRC mismatch")
	}

	// Now parse the header content from memory (no more Read calls!)
	reader := bytes.NewReader(headerContent)
	bytesConsumed := 0

	// Read header type (vint)
	headerType, err := readVInt(reader)
	if err != nil {
		return nil, 0, 0, err
	}
	_ = int(reader.Size()) - reader.Len()

	// Read header flags (vint)
	headerFlags, err := readVInt(reader)
	if err != nil {
		return nil, 0, 0, err
	}
	bytesConsumed = int(reader.Size()) - reader.Len()

	// Read extra area size if present
	if headerFlags&RAR5HeaderFlagExtraArea != 0 {
		_, err = readVInt(reader)
		if err != nil {
			return nil, 0, 0, err
		}
		bytesConsumed = int(reader.Size()) - reader.Len()
	}

	// Read data area size if present
	var dataAreaSize int64
	if headerFlags&RAR5HeaderFlagDataArea != 0 {
		dataSize, err := readVInt(reader)
		if err != nil {
			return nil, 0, 0, err
		}
		bytesConsumed = int(reader.Size()) - reader.Len()
		dataAreaSize = int64(dataSize)
	}

	// Remaining bytes are the header data
	remainingHeaderSize := int(headerSize) - bytesConsumed
	var headerData []byte
	if remainingHeaderSize > 0 {
		headerData = make([]byte, remainingHeaderSize)
		_, err := io.ReadFull(reader, headerData)
		if err != nil {
			return nil, 0, 0, err
		}
	}

	// Total header size is: CRC (4 bytes) + size vint + headerSize
	totalHeaderSize := 4 + headerSizeVintBytes + int(headerSize)

	return &rar5HeaderData{
		Type:  headerType,
		Flags: headerFlags,
		Data:  headerData,
	}, totalHeaderSize, dataAreaSize, nil
}

// parseVIntFromBuffer parses a vint from a byte slice without any Read calls
// Returns (value, bytesConsumed) - bytesConsumed is 0 if buffer doesn't contain complete vint
func parseVIntFromBuffer(buf []byte) (uint64, int) {
	var result uint64
	for i := 0; i < len(buf) && i < 10; i++ {
		b := buf[i]
		result |= uint64(b&0x7F) << (uint(i) * 7)
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	return 0, 0 // Incomplete vint
}

// readVIntFromReaderWithBytes reads a variable-length integer from a reader
// Returns the value, number of bytes read, the actual bytes read, and any error
// Optimized: uses stack-allocated array to minimize allocations
func readVIntFromReaderWithBytes(r io.Reader) (uint64, int, []byte, error) {
	// A vint can be at most 10 bytes for a 64-bit value (7 bits per byte)
	// Use stack-allocated array to avoid heap allocation
	var buf [10]byte
	var result uint64
	bytesRead := 0

	for shift := uint(0); shift < 64 && bytesRead < 10; shift += 7 {
		// Read one byte into our buffer
		n, err := r.Read(buf[bytesRead : bytesRead+1])
		if err != nil {
			if bytesRead > 0 {
				return 0, bytesRead, buf[:bytesRead], err
			}
			return 0, 0, nil, err
		}
		if n == 0 {
			if bytesRead > 0 {
				return 0, bytesRead, buf[:bytesRead], io.EOF
			}
			return 0, 0, nil, io.EOF
		}

		b := buf[bytesRead]
		bytesRead++

		result |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			// Done - return slice of what we read
			return result, bytesRead, buf[:bytesRead], nil
		}
	}

	return 0, bytesRead, buf[:bytesRead], fmt.Errorf("vint too large")
}

// readVIntFromReader reads a variable-length integer from a reader
// Returns the value, number of bytes read, and any error
func readVIntFromReader(r io.Reader) (uint64, int, error) {
	val, n, _, err := readVIntFromReaderWithBytes(r)
	return val, n, err
}

// readVInt reads a variable-length integer from bytes.Reader (keep for compatibility)
func readVInt(r *bytes.Reader) (uint64, error) {
	val, _, err := readVIntFromReader(r)
	return val, err
}

// parseRAR4Stream parses RAR 4.x headers from a stream reader
// This properly tracks offsets by reading headers sequentially and skipping data
func (p *RARParser) parseRAR4Stream(stream *rarReader, volumeIndex int, volumeName string, volumeSize int64) ([]*RARFileEntry, error) {
	var files []*RARFileEntry

	// Stream position is already at 7 (after RAR4 signature)
	// The signature is: "Rar!\x1A\x07\x00" (7 bytes)

	for {
		// Read RAR4 header
		header, err := p.readRAR4HeaderFromStream(stream)
		if err != nil {
			return nil, fmt.Errorf("failed to read RAR4 header: %w", err)
		}

		// Data offset is immediately after the header
		dataOffsetAbsolute := stream.Position()
		var dataSkipSize int64

		// Parse file headers
		if header.Type == RAR4HeaderTypeFile {
			_, offsetInVol := stream.AbsoluteToVolumeOffset(dataOffsetAbsolute)

			// Use the volumeIndex parameter passed to this function, not the stream's volume index
			// because each stream only contains one volume
			file := p.parseRAR4FileHeader(header, volumeIndex, volumeName, offsetInVol)
			if file == nil {
				return nil, fmt.Errorf("invalid RAR4 file header")
			}
			if file.PackedSize < 0 || offsetInVol > volumeSize || file.PackedSize > volumeSize-offsetInVol {
				return nil, fmt.Errorf("truncated RAR4 member: packed data extends beyond volume")
			}
			files = append(files, file)
			dataSkipSize = file.PackedSize
		} else if header.Flags&RAR4HeaderFlagLongBlock != 0 {
			if len(header.Data) < 4 {
				return nil, fmt.Errorf("invalid RAR4 long block")
			}
			dataSkipSize = int64(binary.LittleEndian.Uint32(header.Data))
		}

		// Skip the file data section (PackedSize) to get to the next header
		// CRITICAL FIX: Do NOT manually skip AddSize.
		// AddSize (if present) is part of the header structure we just read, NOT part of the file data body.
		// dataSkipSize already contains the PackedSize which is the file data body.
		skipTotal := dataSkipSize

		if skipTotal > 0 {
			if err := stream.Skip(skipTotal); err != nil {
				return nil, fmt.Errorf("failed to skip RAR4 data section: %w", err)
			}
		}

		// Stop at end of archive
		if header.Type == RAR4HeaderTypeEnd {
			break
		}
	}

	return files, nil
}

// readRAR4HeaderFromStream reads a single RAR 4.x header from stream
func (p *RARParser) readRAR4HeaderFromStream(stream *rarReader) (*rar4Header, error) {
	var raw [7]byte
	if _, err := io.ReadFull(stream, raw[:]); err != nil {
		return nil, err
	}
	header := &rar4Header{
		CRC: binary.LittleEndian.Uint16(raw[:2]), Type: raw[2],
		Flags: binary.LittleEndian.Uint16(raw[3:5]), HeadSize: binary.LittleEndian.Uint16(raw[5:7]),
	}
	if header.HeadSize < 7 {
		return nil, fmt.Errorf("invalid RAR4 header size: %d", header.HeadSize)
	}
	header.Data = make([]byte, int(header.HeadSize)-7)
	if _, err := io.ReadFull(stream, header.Data); err != nil {
		return nil, err
	}
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write(raw[2:])
	_, _ = checksum.Write(header.Data)
	if uint16(checksum.Sum32()) != header.CRC {
		return nil, fmt.Errorf("RAR4 header CRC mismatch")
	}
	return header, nil
}
