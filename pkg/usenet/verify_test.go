package usenet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
)

func paddedVerificationHead(prefix []byte) []byte {
	head := make([]byte, verifyHeadBytes)
	for i := range head {
		head[i] = 0xAB
	}
	copy(head, prefix)
	return head
}

func isoVerificationHead() []byte {
	head := paddedVerificationHead(nil)
	binary.BigEndian.PutUint32(head[:4], 24)
	copy(head[4:8], "ftyp")
	return head
}

func transportVerificationHead(stride, offset int) []byte {
	head := paddedVerificationHead(nil)
	for i := range 3 {
		head[offset+i*stride] = 0x47
	}
	return head
}

func TestVerificationAcceptsSupportedContainerFamilies(t *testing.T) {
	asf := []byte{0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF, 0x11, 0xA6, 0xD9, 0x00, 0xAA, 0x00, 0x62, 0xCE, 0x6C}
	tests := []struct {
		name     string
		filename string
		head     []byte
	}{
		{"EBML", "movie.mkv", paddedVerificationHead([]byte{0x1A, 0x45, 0xDF, 0xA3})},
		{"ISO BMFF uppercase extension", "movie.MP4", isoVerificationHead()},
		{"fragmented ISO BMFF index", "movie.mp4", func() []byte {
			head := isoVerificationHead()
			copy(head[4:8], "sidx")
			return head
		}()},
		{"AVI", "movie.avi", paddedVerificationHead([]byte("RIFF\x24\x00\x00\x00AVI "))},
		{"RF64 WAVE", "audio.wav", paddedVerificationHead([]byte("RF64\x24\x00\x00\x00WAVE"))},
		{"big-endian RIFX WAVE", "audio.wav", paddedVerificationHead([]byte("RIFX\x00\x00\x00\x24WAVE"))},
		{"BW64 WAVE", "audio.wav", paddedVerificationHead([]byte("BW64\xFF\xFF\xFF\xFFWAVE"))},
		{"Ogg", "audio.opus", paddedVerificationHead([]byte("OggS\x00\x02"))},
		{"FLV", "movie.flv", paddedVerificationHead([]byte("FLV\x01"))},
		{"ASF", "movie.wmv", paddedVerificationHead(asf)},
		{"MPEG program stream", "movie.vob", paddedVerificationHead([]byte{0x00, 0x00, 0x01, 0xBA})},
		{"MPEG elementary stream", "movie.m2v", paddedVerificationHead([]byte{0x00, 0x00, 0x01, 0xB3})},
		{"transport stream", "movie.ts", transportVerificationHead(188, 0)},
		{"transport stream with short prefix", "movie.ts", transportVerificationHead(188, 7)},
		{"204-byte transport stream", "movie.ts", transportVerificationHead(204, 3)},
		{"M2TS", "movie.m2ts", transportVerificationHead(192, 4)},
		{"MP3 ID3", "audio.mp3", paddedVerificationHead([]byte("ID3\x04\x00"))},
		{"MP3 frame", "audio.mp3", paddedVerificationHead([]byte{0xFF, 0xFB, 0x90})},
		{"FLAC", "audio.flac", paddedVerificationHead([]byte("fLaC"))},
		{"AIFF", "audio.aiff", paddedVerificationHead([]byte("FORM\x00\x00\x00\x2EAIFF"))},
		{"Monkey's Audio", "audio.ape", paddedVerificationHead([]byte("MAC "))},
		{"WavPack", "audio.wv", paddedVerificationHead([]byte("wvpk"))},
		{"DVD IFO", "VIDEO_TS.IFO", paddedVerificationHead([]byte("DVDVIDEO-VMG"))},
		{"RealMedia", "movie.rmvb", paddedVerificationHead([]byte(".RMF"))},
		{"NSV", "movie.nsv", paddedVerificationHead([]byte("NSVf"))},
		{"raw H264", "movie.avc", paddedVerificationHead([]byte{0x00, 0x00, 0x00, 0x01, 0x67})},
		{"NuppelVideo", "movie.nuv", paddedVerificationHead([]byte("NuppelVideo"))},
		{"Vivo", "movie.viv", paddedVerificationHead([]byte("Version:Vivo/"))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := verificationError(tc.filename, tc.head, nil); err != nil {
				t.Fatalf("verification failed: %v", err)
			}
		})
	}
}

func TestVerificationPassesRecognizedCrossLabeledContainer(t *testing.T) {
	if err := verificationError("mislabeled.mkv", isoVerificationHead(), nil); err != nil {
		t.Fatalf("recognized media container should pass despite extension mismatch: %v", err)
	}
}

func TestVerificationRejectsDefinitiveGarbageForSupportedExtension(t *testing.T) {
	head := paddedVerificationHead([]byte("Rar!\x1a\x07\x01\x00"))
	err := verificationError("movie.mkv", head, nil)
	if !errors.Is(err, customerror.UsenetCorruptContentError) {
		t.Fatalf("error = %v, want corrupt-content marker", err)
	}
}

func TestVerificationRejectsWeakSignatureCollisions(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		head     []byte
	}{
		{"RIFF without a recognized form", "movie.avi", paddedVerificationHead([]byte("RIFF\x24\x00\x00\x00JUNK"))},
		{"invalid ISO box size", "movie.mp4", paddedVerificationHead([]byte("\x00\x00\x00\x04ftyp"))},
		{"partial ASF GUID", "movie.wmv", paddedVerificationHead([]byte{0x30, 0x26, 0xB2, 0x75, 0x8E, 0x66, 0xCF, 0x11})},
		{"invalid MPEG audio header", "audio.mp3", paddedVerificationHead([]byte{0xFF, 0xE0, 0x00})},
		{"single transport sync byte", "movie.ts", paddedVerificationHead([]byte{0x47})},
		{"forbidden H264 NAL bit", "movie.avc", paddedVerificationHead([]byte{0, 0, 0, 1, 0xE7})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := verificationError(tc.filename, tc.head, nil)
			if !errors.Is(err, customerror.UsenetCorruptContentError) {
				t.Fatalf("error = %v, want corrupt-content marker", err)
			}
		})
	}
}

func TestVerificationUnsupportedFormatsPassWithoutClassification(t *testing.T) {
	for _, filename := range []string{
		"stream.strm", "playlist.m3u", "playlist.asx", "playlist.wpl",
		"capture.wtv", "image.bin", "video.dat", "disc.nrg", "raw.dv",
		"recording.ty", "codec.vp3", "codec.svq3", "legacy.fli",
		"video.pva", "README.txt", "extensionless",
	} {
		t.Run(filename, func(t *testing.T) {
			if _, supported := mediaHeadRuleFor(filename); supported {
				t.Fatalf("%s unexpectedly has a strict verification rule", filename)
			}
			if err := verificationError(filename, []byte("Rar!"), nil); err != nil {
				t.Fatalf("unsupported format should pass through: %v", err)
			}
		})
	}
}

func TestVerifyFileContentSkipsUnsupportedBeforeStorageLookup(t *testing.T) {
	u := &Usenet{}
	if err := u.VerifyFileContent(context.Background(), "missing-nzb", "stream.strm"); err != nil {
		t.Fatalf("unsupported format touched unavailable storage: %v", err)
	}
}

func TestVerificationReadErrorsRemainInconclusive(t *testing.T) {
	transient := errors.New("temporary decoder failure")
	err := verificationError("movie.ts", make([]byte, 100), transient)
	if !errors.Is(err, transient) {
		t.Fatalf("error = %v, want original transient error", err)
	}

	err = verificationError("movie.ts", make([]byte, 100), io.EOF)
	if !errors.Is(err, customerror.UsenetCorruptContentError) {
		t.Fatalf("complete short file error = %v, want corrupt-content marker", err)
	}

	err = verificationError("movie.mp4", isoVerificationHead()[:24], io.EOF)
	if err != nil {
		t.Fatalf("valid short read ending at EOF failed: %v", err)
	}
}

func TestVerificationPropagatesCancellationAndMissingArticle(t *testing.T) {
	if err := verificationError("movie.mkv", nil, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	missing := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "gone"}
	err := verificationError("movie.mkv", nil, missing)
	if !errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("missing article error = %v, want missing-segment marker", err)
	}
}

func TestContentVerificationSlotHonorsCancellation(t *testing.T) {
	u := &Usenet{contentVerifySlots: make(chan struct{}, 1)}
	u.contentVerifySlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release, err := u.acquireContentVerifySlot(ctx)
	if release != nil {
		t.Fatal("canceled acquisition returned a release function")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("slot acquisition error = %v, want context cancellation", err)
	}
}

func TestVerifyFileContentWaitsForSlotBeforeStorageLookup(t *testing.T) {
	u := &Usenet{contentVerifySlots: make(chan struct{}, 1)}
	u.contentVerifySlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := u.VerifyFileContent(ctx, "missing-nzb", "movie.mkv")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verification error = %v, want context cancellation", err)
	}
}
