package yenc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestPureGoDecoderPreservesNextNNTPResponse(t *testing.T) {
	original := UsePureGo
	UsePureGo = true
	defer func() { UsePureGo = original }()
	payload := []byte("framed article payload")
	article := yencEncode(payload, "video.mkv", 0, 0, 0)
	const response = "223 0 <next@example.invalid>\r\n"
	for _, test := range []struct {
		name   string
		reader func() io.Reader
	}{
		{"coalesced", func() io.Reader { return strings.NewReader(article + ".\r\n" + response) }},
		{"split terminator", func() io.Reader {
			return io.MultiReader(strings.NewReader(article), strings.NewReader(".\r\n"+response))
		}},
		{"one byte reads", func() io.Reader { return iotest.OneByteReader(strings.NewReader(article + ".\r\n" + response)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire := bufio.NewReader(test.reader())
			dec := AcquireNNTPDecoder(wire)
			defer ReleaseDecoder(dec)
			decoded, err := io.ReadAll(dec)
			if err != nil || !bytes.Equal(decoded, payload) {
				t.Fatalf("decoded article = %q, %v", decoded, err)
			}
			if dec.Meta.FileName != "video.mkv" || dec.Meta.PartSize != int64(len(payload)) {
				t.Fatalf("article metadata changed: %+v", dec.Meta)
			}
			next, err := wire.ReadString('\n')
			if err != nil || next != response {
				t.Fatalf("next NNTP response = %q, %v; want %q", next, err, response)
			}
		})
	}
}

func TestPureGoNNTPDecoderRejectsTruncatedFraming(t *testing.T) {
	original := UsePureGo
	UsePureGo = true
	defer func() { UsePureGo = original }()
	article := yencEncode([]byte("payload"), "video.mkv", 0, 0, 0)
	for _, test := range []struct{ name, wire string }{
		{"missing terminator", article},
		{"partial terminator", article + "."},
		{"missing yend", "=ybegin line=128 size=1 name=video.mkv\r\nK\r\n.\r\n"},
		{"missing ybegin", ".\r\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dec := AcquireNNTPDecoder(bufio.NewReader(strings.NewReader(test.wire)))
			defer ReleaseDecoder(dec)
			if _, err := io.ReadAll(dec); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncated response error = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestPureGoNNTPDecoderPreservesDrainErrors(t *testing.T) {
	original := UsePureGo
	UsePureGo = true
	defer func() { UsePureGo = original }()
	article := yencEncode([]byte("payload"), "video.mkv", 0, 0, 0)
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, errors.New("connection reset")} {
		dec := AcquireNNTPDecoder(bufio.NewReader(io.MultiReader(strings.NewReader(article), iotest.ErrReader(want))))
		_, err := io.ReadAll(dec)
		ReleaseDecoder(dec)
		if !errors.Is(err, want) {
			t.Fatalf("drain error = %v, want %v", err, want)
		}
	}
}

func TestPureGoNNTPDecoderRetainsFramingErrorAcrossSnippetAndDrain(t *testing.T) {
	original := UsePureGo
	UsePureGo = true
	defer func() { UsePureGo = original }()
	const next = "223 0 <next@example.invalid>\r\n"
	reader := bufio.NewReader(strings.NewReader("=ybegin line=128 size=1 name=video.mkv\r\nK\r\n.\r\n" + next))
	dec := AcquireNNTPDecoder(reader)
	defer ReleaseDecoder(dec)
	// Header-prefix callers accept a short snippet, then drain the same decoder.
	if _, err := io.ReadFull(dec, make([]byte, 128)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("snippet error = %v, want io.ErrUnexpectedEOF", err)
	}
	if n, err := io.Copy(io.Discard, dec); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("drain = %d, %v; want no data and io.ErrUnexpectedEOF", n, err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != next {
		t.Fatalf("next response = %q, %v", line, err)
	}
}

func TestPureGoNNTPDecoderDrainsEpilogueAndPreservesDotStuffing(t *testing.T) {
	original := UsePureGo
	UsePureGo = true
	defer func() { UsePureGo = original }()
	// Two wire dots decode to one encoded dot, whose original byte is 4.
	article := "=ybegin line=128 size=1 name=video.mkv\r\n..\r\n=yend size=1\r\n"
	const next = "223 0 <next@example.invalid>\r\n"
	reader := bufio.NewReader(strings.NewReader(article + "poster epilogue\r\n..text\r\n\r\n.\r\n" + next))
	dec := AcquireNNTPDecoder(reader)
	defer ReleaseDecoder(dec)
	data, err := io.ReadAll(dec)
	if err != nil || !bytes.Equal(data, []byte{4}) {
		t.Fatalf("dot-stuffed payload = %v, %v", data, err)
	}
	if line, err := reader.ReadString('\n'); err != nil || line != next {
		t.Fatalf("next response = %q, %v", line, err)
	}
}
