package usenet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// verifyHeadBytes covers the longest supported fixed-stride signature: three
// 204-byte transport packets plus a short leading prefix need at most byte
// 423. The range remains a small bounded head read and normally fits within
// one Usenet article.
const verifyHeadBytes = 512

var asfHeaderMagic = []byte{
	0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF, 0x11,
	0xA6, 0xD9, 0x00, 0xAA, 0x00, 0x62, 0xCE, 0x6C,
}

type mediaHeadRule struct {
	family   string
	minBytes int
	matches  func([]byte) bool
}

var (
	ebmlRule      = mediaHeadRule{"EBML", 4, func(head []byte) bool { return bytes.HasPrefix(head, []byte{0x1A, 0x45, 0xDF, 0xA3}) }}
	isoBMFFRule   = mediaHeadRule{"ISO base media", 8, matchesISOBMFF}
	aviRule       = mediaHeadRule{"RIFF AVI", 12, func(head []byte) bool { return matchesRIFFType(head, "AVI ") || matchesRIFFType(head, "AVIX") }}
	waveRule      = mediaHeadRule{"RIFF WAVE", 12, func(head []byte) bool { return matchesRIFFType(head, "WAVE") }}
	oggRule       = mediaHeadRule{"Ogg", 4, func(head []byte) bool { return bytes.HasPrefix(head, []byte("OggS")) }}
	flvRule       = mediaHeadRule{"Flash Video", 4, func(head []byte) bool { return bytes.HasPrefix(head, []byte("FLV\x01")) }}
	asfRule       = mediaHeadRule{"ASF", 16, matchesASF}
	mpegPSRule    = mediaHeadRule{"MPEG program or elementary stream", 4, matchesMPEGVideo}
	tsRule        = mediaHeadRule{"MPEG transport stream", 377, matchesTransportStream}
	m2tsRule      = mediaHeadRule{"M2TS transport stream", 389, func(head []byte) bool { return matchesPacketStride(head, 192, 4, 4) }}
	mpegAudioRule = mediaHeadRule{"MPEG audio", 3, matchesMPEGAudio}
	flacRule      = mediaHeadRule{"FLAC", 4, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("fLaC")) || bytes.HasPrefix(head, []byte("ID3"))
	}}
	aiffRule = mediaHeadRule{"AIFF", 12, matchesAIFF}
	apeRule  = mediaHeadRule{"Monkey's Audio", 4, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("MAC ")) || bytes.HasPrefix(head, []byte("ID3"))
	}}
	wavPackRule = mediaHeadRule{"WavPack", 4, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("wvpk")) || bytes.HasPrefix(head, []byte("ID3"))
	}}
	ifoRule = mediaHeadRule{"DVD IFO", 12, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("DVDVIDEO-VMG")) || bytes.HasPrefix(head, []byte("DVDVIDEO-VTS"))
	}}
	realMediaRule = mediaHeadRule{"RealMedia", 4, func(head []byte) bool { return bytes.HasPrefix(head, []byte(".RMF")) }}
	nsvRule       = mediaHeadRule{"Nullsoft Streaming Video", 4, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("NSVf")) || bytes.HasPrefix(head, []byte("NSVs"))
	}}
	h264Rule = mediaHeadRule{"raw H.264", 4, matchesH264Start}
	nuvRule  = mediaHeadRule{"NuppelVideo", 11, func(head []byte) bool {
		return bytes.HasPrefix(head, []byte("NuppelVideo")) || bytes.HasPrefix(head, []byte("MythTVVideo"))
	}}
	vivoRule = mediaHeadRule{"Vivo", 13, func(head []byte) bool { return bytes.HasPrefix(head, []byte("Version:Vivo/")) }}

	// A file with a supported extension may contain another recognized media
	// container (mislabeled releases exist and often play correctly). Extension
	// awareness decides whether verification is safe to attempt; this strong
	// signature set avoids rejecting a recognized cross-labeled container.
	supportedMediaHeadRules = []mediaHeadRule{
		ebmlRule, isoBMFFRule, aviRule, waveRule, oggRule, flvRule, asfRule,
		mpegPSRule, tsRule, m2tsRule, mpegAudioRule, flacRule, aiffRule,
		apeRule, wavPackRule, ifoRule, realMediaRule, nsvRule, h264Rule,
		nuvRule, vivoRule,
	}
)

// mediaHeadRuleFor is deliberately narrower than utils.IsMediaFile. Formats
// with ambiguous, tail-only, text, or poorly standardized signatures remain
// availability-only and pass through without a content read.
func mediaHeadRuleFor(filename string) (mediaHeadRule, bool) {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(filename)), ".")
	switch ext {
	case "mkv", "mk3d", "webm":
		return ebmlRule, true
	case "mp4", "m4v", "mov", "qt", "3gp", "m4a", "m4b", "m4p":
		return isoBMFFRule, true
	case "avi", "divx", "xvid", "bivx":
		return aviRule, true
	case "wav":
		return waveRule, true
	case "ogm", "ogv", "ogg", "oga", "opus":
		return oggRule, true
	case "flv":
		return flvRule, true
	case "wmv", "wma", "asf", "dvr-ms":
		return asfRule, true
	case "mpg", "mpeg", "m2v", "vob":
		return mpegPSRule, true
	case "ts":
		return tsRule, true
	case "m2ts":
		return m2tsRule, true
	case "mp2", "mp3":
		return mpegAudioRule, true
	case "flac":
		return flacRule, true
	case "aif", "aiff", "aifc":
		return aiffRule, true
	case "ape":
		return apeRule, true
	case "wv":
		return wavPackRule, true
	case "ifo":
		return ifoRule, true
	case "rm", "rmvb":
		return realMediaRule, true
	case "nsv":
		return nsvRule, true
	case "avc":
		return h264Rule, true
	case "nuv":
		return nuvRule, true
	case "viv":
		return vivoRule, true
	default:
		return mediaHeadRule{}, false
	}
}

func matchesRIFFType(head []byte, formType string) bool {
	return len(head) >= 12 &&
		(bytes.Equal(head[:4], []byte("RIFF")) ||
			bytes.Equal(head[:4], []byte("RIFX")) ||
			bytes.Equal(head[:4], []byte("RF64")) ||
			bytes.Equal(head[:4], []byte("BW64"))) &&
		string(head[8:12]) == formType
}

func matchesISOBMFF(head []byte) bool {
	if len(head) < 8 {
		return false
	}
	switch string(head[4:8]) {
	case "ftyp", "styp", "moov", "mdat", "free", "skip", "wide", "pnot",
		"uuid", "sidx", "ssix", "prft", "emsg":
	default:
		return false
	}
	size := binary.BigEndian.Uint32(head[:4])
	switch size {
	case 0:
		return true // box extends to EOF
	case 1:
		return len(head) >= 16 && binary.BigEndian.Uint64(head[8:16]) >= 16
	default:
		return size >= 8
	}
}

func matchesASF(head []byte) bool {
	return bytes.HasPrefix(head, asfHeaderMagic)
}

func matchesMPEGVideo(head []byte) bool {
	return bytes.HasPrefix(head, []byte{0x00, 0x00, 0x01, 0xBA}) || // pack header
		bytes.HasPrefix(head, []byte{0x00, 0x00, 0x01, 0xB3}) // sequence header
}

func matchesPacketStride(head []byte, stride, preferredOffset, search int) bool {
	for offset := preferredOffset; offset < preferredOffset+search; offset++ {
		if offset+2*stride >= len(head) {
			return false
		}
		if head[offset] == 0x47 && head[offset+stride] == 0x47 && head[offset+2*stride] == 0x47 {
			return true
		}
	}
	return false
}

func matchesTransportStream(head []byte) bool {
	return matchesPacketStride(head, 188, 0, 16) ||
		matchesPacketStride(head, 192, 4, 4) ||
		matchesPacketStride(head, 204, 0, 16)
}

func matchesMPEGAudio(head []byte) bool {
	if bytes.HasPrefix(head, []byte("ID3")) {
		return true
	}
	if len(head) < 3 || head[0] != 0xFF || head[1]&0xE0 != 0xE0 {
		return false
	}
	version := (head[1] >> 3) & 0x03
	layer := (head[1] >> 1) & 0x03
	bitrate := (head[2] >> 4) & 0x0F
	sampleRate := (head[2] >> 2) & 0x03
	return version != 1 && layer != 0 && bitrate != 0 && bitrate != 0x0F && sampleRate != 0x03
}

func matchesAIFF(head []byte) bool {
	return len(head) >= 12 && bytes.Equal(head[:4], []byte("FORM")) &&
		(string(head[8:12]) == "AIFF" || string(head[8:12]) == "AIFC")
}

func matchesH264Start(head []byte) bool {
	if len(head) < 4 {
		return false
	}
	nalOffset := 0
	switch {
	case len(head) >= 5 && bytes.Equal(head[:4], []byte{0, 0, 0, 1}):
		nalOffset = 4
	case bytes.Equal(head[:3], []byte{0, 0, 1}):
		nalOffset = 3
	default:
		return false
	}
	if nalOffset >= len(head) || head[nalOffset]&0x80 != 0 {
		return false
	}
	nalType := head[nalOffset] & 0x1F
	return nalType >= 1 && nalType <= 23
}

func matchesKnownMediaHead(head []byte) bool {
	for _, rule := range supportedMediaHeadRules {
		if len(head) >= rule.minBytes && rule.matches(head) {
			return true
		}
	}
	return false
}

func verificationError(filename string, head []byte, readErr error) error {
	rule, supported := mediaHeadRuleFor(filename)
	if !supported {
		return nil
	}
	if errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded) {
		return readErr
	}
	if nntp.IsArticleNotFoundError(readErr) {
		return fmt.Errorf("head article of %q is missing: %w", filename, customerror.UsenetSegmentMissingError)
	}
	if matchesKnownMediaHead(head) {
		return nil
	}
	// A connection or decoder failure is inconclusive. Only EOF (the complete
	// short file is known) or an error-free read may establish corruption.
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	return fmt.Errorf("head of %q does not match %s: %w", filename, rule.family, customerror.UsenetCorruptContentError)
}

func (u *Usenet) acquireContentVerifySlot(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if u.contentVerifySlots == nil {
		return func() {}, nil
	}
	select {
	case u.contentVerifySlots <- struct{}{}:
		return func() { <-u.contentVerifySlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// VerifyFileContent performs an extension-aware head verification for one
// stored NZB file. Unsupported or ambiguous formats pass through without a
// read. The caller must opt in; normal imports and scheduled sweeps never call
// this method.
func (u *Usenet) VerifyFileContent(ctx context.Context, nzoID, filename string) error {
	if _, supported := mediaHeadRuleFor(filename); !supported {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Take the global slot before decoding the complete file metadata. A repair
	// sweep can otherwise make several large NZB segment maps resident while
	// those probes wait for their turn to read.
	release, err := u.acquireContentVerifySlot(ctx)
	if err != nil {
		return err
	}
	defer release()

	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return err
	}
	return u.verifyFileContentHead(ctx, file)
}

func (u *Usenet) verifyFileContentHead(ctx context.Context, file *storage.NZBFile) error {
	if file == nil {
		return errors.New("cannot verify nil NZB file")
	}
	if _, supported := mediaHeadRuleFor(file.Name); !supported {
		return nil
	}
	entry, err := u.createEntryWithReadLimits(file, 1, 0)
	if err != nil {
		return err
	}
	defer entry.cleanup()
	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return err
	}

	readSize := verifyHeadBytes
	if file.Size > 0 && file.Size < int64(readSize) {
		readSize = int(file.Size)
	}
	head := make([]byte, readSize)
	n, readErr := readerAt.ReadAtContext(ctx, head, 0)
	if n < 0 {
		n = 0
	}
	if n > len(head) {
		n = len(head)
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return verificationError(file.Name, head[:n], readErr)
}
