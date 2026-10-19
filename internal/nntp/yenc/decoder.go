package yenc

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
)

// Metadata contains yEnc header information and snippet bytes.
type Metadata struct {
	Name        string // filename
	Size        int64  // total file size
	Part        int64  // part number
	Total       int64  // total parts
	Begin       int64  // part start byte
	End         int64  // part end byte
	Offset      int64  // part offset within the file
	PartSize    int64  // part size (decoded)
	DecodedSize int64  // Actual decoded body length, set only after a complete body read.
	LineSize    int    // line length
	Snippet     []byte
}

// UsePureGo forces the pure-Go yEnc decoder even when CGO/rapidyenc is available.
// Set via YENC_PURE_GO=true environment variable.
var UsePureGo = os.Getenv("YENC_PURE_GO") == "true"

// Decoder wraps an io.Reader that decodes yEnc-encoded data on the fly.
// After reading, Meta contains the parsed yEnc header metadata.
type Decoder struct {
	io.Reader
	Meta DecoderMeta
}

// AcquireNNTPDecoder consumes one dot-terminated article from the connection's
// shared reader. Standalone yEnc input should use AcquireDecoder instead.
func AcquireNNTPDecoder(r *bufio.Reader) *Decoder {
	dec := AcquireDecoder(r)
	if pure, ok := dec.Reader.(*pureGoYencDecoder); ok {
		// Do not hide read-ahead for the next response in a private buffer.
		pure.r = r
		pure.nntp = true
	}
	return dec
}

// DecoderMeta holds yEnc header metadata, matching the fields from rapidyenc.Meta.
type DecoderMeta struct {
	FileName   string
	FileSize   int64
	PartNumber int64
	TotalParts int64
	Offset     int64
	PartSize   int64
}

// Begin returns the "=ypart begin" value calculated from the Offset.
func (m DecoderMeta) Begin() int64 {
	return m.Offset + 1
}

// End returns the "=ypart end" value calculated from the Offset and PartSize.
func (m DecoderMeta) End() int64 {
	return m.Offset + m.PartSize
}

// pureGoYencDecoder is a streaming yEnc decoder implemented in pure Go.
// It consumes yEnc control lines and decodes payload bytes incrementally.
type pureGoYencDecoder struct {
	r            *bufio.Reader
	meta         *DecoderMeta
	out          []byte
	outPos       int
	scratch      []byte // reused buffer for lines longer than the bufio buffer (rare)
	sawBegin     bool
	sawEnd       bool
	nntp         bool
	done         bool
	readErr      error
	decodedBytes int64
	checksum     uint32
}

func newPureGoYencDecoder(r io.Reader, meta *DecoderMeta) *pureGoYencDecoder {
	return &pureGoYencDecoder{
		r:    bufio.NewReaderSize(r, 64*1024),
		meta: meta,
		out:  make([]byte, 0, 8*1024),
	}
}

func (d *pureGoYencDecoder) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if d.readErr != nil {
		return 0, d.readErr
	}

	n := 0
	for n < len(p) {
		if d.outPos < len(d.out) {
			copied := copy(p[n:], d.out[d.outPos:])
			d.outPos += copied
			n += copied
			if d.outPos == len(d.out) {
				d.outPos = 0
				d.out = d.out[:0]
			}
			continue
		}

		if d.done {
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}

		err := d.fill()
		if err != nil {
			if err == io.EOF {
				d.done = true
				if n > 0 {
					return n, nil
				}
				return 0, io.EOF
			}
			if d.nntp {
				// A later drain must not read beyond a malformed response.
				d.readErr = err
			}
			if n > 0 {
				return n, err
			}
			return 0, err
		}
	}

	return n, nil
}

// readLine returns the next line including its trailing '\n' without allocating
// per line: ReadSlice returns a slice into the bufio buffer (valid only until
// the next read, which is fine since processLine consumes it immediately). A
// line longer than the 64 KiB bufio buffer — not expected for valid yEnc, whose
// lines are ~128 bytes — falls back to a reused scratch buffer, so steady state
// stays zero-alloc. This replaces ReadBytes, which allocated a fresh slice for
// every line (~6k allocations per article).
func (d *pureGoYencDecoder) readLine() ([]byte, error) {
	line, err := d.r.ReadSlice('\n')
	if err != bufio.ErrBufferFull {
		return line, err
	}
	d.scratch = append(d.scratch[:0], line...)
	for err == bufio.ErrBufferFull {
		line, err = d.r.ReadSlice('\n')
		d.scratch = append(d.scratch, line...)
	}
	return d.scratch, err
}

func (d *pureGoYencDecoder) fill() error {
	d.out = d.out[:0]
	d.outPos = 0

	for {
		line, err := d.readLine()
		if err != nil && err != io.EOF {
			return err
		}
		if d.nntp && err == io.EOF {
			return io.ErrUnexpectedEOF
		}

		if len(line) > 0 {
			done, lineErr := d.processLine(line)
			if lineErr != nil {
				return lineErr
			}
			if len(d.out) > 0 {
				return nil
			}
			if done {
				return io.EOF
			}
		}

		if err == io.EOF {
			return io.EOF
		}
	}
}

func (d *pureGoYencDecoder) processLine(line []byte) (bool, error) {
	line = bytes.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return false, nil
	}

	// Handle raw NNTP article termination / dot-stuffing when present.
	if line[0] == '.' {
		if len(line) == 1 {
			if d.nntp && !d.sawEnd {
				return false, io.ErrUnexpectedEOF
			}
			return true, nil
		}
		if line[1] == '.' {
			line = line[1:]
		}
	}
	if d.sawEnd {
		return false, nil
	}

	switch {
	case bytes.HasPrefix(line, []byte("=ybegin ")):
		d.sawBegin = true
		parseYBeginLine(line, d.meta)
		return false, nil
	case bytes.HasPrefix(line, []byte("=ypart ")):
		parseYPartLine(line, d.meta)
		return false, nil
	case bytes.HasPrefix(line, []byte("=yend ")):
		expected := d.meta.PartSize
		footerSize, err := parseYEndSize(line)
		if err != nil {
			return false, err
		}
		if footerSize != d.decodedBytes || (expected > 0 && expected != d.decodedBytes) {
			return false, fmt.Errorf("yEnc decoded size does not match header/footer")
		}
		d.meta.PartSize = footerSize
		for field := range strings.FieldsSeq(string(line)) {
			key, value, ok := strings.Cut(field, "=")
			if !ok || (key != "pcrc32" && (key != "crc32" || d.meta.PartNumber > 0)) {
				continue
			}
			want, err := strconv.ParseUint(value, 16, 32)
			if err != nil || uint32(want) != d.checksum {
				return false, fmt.Errorf("yEnc CRC32 mismatch")
			}
		}
		d.sawEnd = true
		// yEnc ends before the NNTP response does. Drain any epilogue and the
		// terminator before allowing this connection to return to its pool.
		return !d.nntp, nil
	}

	if !d.sawBegin {
		return false, nil
	}

	// Decode with escape handling.
	d.out = d.out[:0]
	escaped := false
	for _, b := range line {
		if escaped {
			d.out = append(d.out, b-64-42)
			escaped = false
			continue
		}
		if b == '=' {
			escaped = true
			continue
		}
		d.out = append(d.out, b-42)
	}
	if escaped {
		return false, fmt.Errorf("truncated yEnc escape")
	}
	d.decodedBytes += int64(len(d.out))
	d.checksum = crc32.Update(d.checksum, crc32.IEEETable, d.out)

	return false, nil
}

func parseYBeginLine(line []byte, meta *DecoderMeta) {
	s := strings.TrimSpace(string(line))
	rest := strings.TrimSpace(strings.TrimPrefix(s, "=ybegin "))

	if idx := strings.Index(rest, " name="); idx >= 0 {
		meta.FileName = rest[idx+6:]
		rest = strings.TrimSpace(rest[:idx])
	}

	for field := range strings.FieldsSeq(rest) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch k {
		case "size":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				meta.FileSize = n
			}
		case "part":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				meta.PartNumber = n
			}
		case "total":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				meta.TotalParts = n
			}
		}
	}

	// Single-part posts often omit =ypart; use full file size as part size.
	if meta.PartNumber == 0 {
		meta.Offset = 0
		meta.PartSize = meta.FileSize
	}
}

func parseYPartLine(line []byte, meta *DecoderMeta) {
	s := strings.TrimSpace(string(line))
	rest := strings.TrimSpace(strings.TrimPrefix(s, "=ypart "))

	var begin int64
	var end int64
	for field := range strings.FieldsSeq(rest) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch k {
		case "begin":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				begin = n
			}
		case "end":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				end = n
			}
		}
	}

	if begin > 0 {
		meta.Offset = begin - 1
	}
	if begin > 0 && end >= begin {
		meta.PartSize = end - meta.Offset
	}
}

func parseYEndSize(line []byte) (int64, error) {
	s := strings.TrimSpace(string(line))
	rest := strings.TrimSpace(strings.TrimPrefix(s, "=yend "))
	var size int64
	found := false
	for field := range strings.FieldsSeq(rest) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if k != "size" {
			continue
		}
		if found {
			return 0, fmt.Errorf("duplicate yEnc footer size")
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid yEnc footer size")
		}
		size, found = n, true
	}
	if !found {
		return 0, fmt.Errorf("missing yEnc footer size")
	}
	return size, nil
}

func acquirePureGoDecoder(r io.Reader) *Decoder {
	yd := &Decoder{}
	yd.Reader = newPureGoYencDecoder(r, &yd.Meta)
	return yd
}
