package nntp

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/textproto"
	"strings"
	"testing"
	"time"
)

func articleFallbackConnection(t *testing.T, status, headerID string, transforms ...func(string) string) *Connection {
	t.Helper()
	client, server := net.Pipe()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	_ = server.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(client)
	conn := &Connection{conn: client, reader: br, writer: bufio.NewWriter(client), text: textproto.NewReader(br)}
	done := make(chan struct{})
	t.Cleanup(func() { _ = conn.Close(); _ = server.Close(); <-done })
	go func() {
		defer close(done)
		protocol := textproto.NewConn(server)
		for {
			command, err := protocol.ReadLine()
			if err != nil {
				return
			}
			switch command {
			case "BODY <part@fixture.invalid>":
				_ = protocol.PrintfLine("430 No such article")
			case "ARTICLE <part@fixture.invalid>":
				_ = protocol.PrintfLine("%s", status)
				if len(status) < 3 || status[:3] != "220" {
					continue
				}
				w := protocol.DotWriter()
				body := fmt.Sprintf("Message-ID: %s\r\nSubject: synthetic\r\n\r\n=ybegin part=1 line=128 size=4 name=video.mkv\r\n=ypart begin=1 end=4\r\nkkkk\r\n=yend size=4 part=1 pcrc32=%08x\r\n", headerID, crc32.ChecksumIEEE([]byte("AAAA")))
				for _, transform := range transforms {
					body = transform(body)
				}
				_, _ = io.WriteString(w, body)
				_ = w.Close()
			case "STAT <part@fixture.invalid>":
				_ = protocol.PrintfLine("223 0 <part@fixture.invalid>")
			default:
				_ = protocol.PrintfLine("500 unexpected command")
			}
		}
	}()
	return conn
}

func TestBodyMissingFallsBackToSameArticle(t *testing.T) {
	for _, mode := range []string{"decoded", "prefix", "raw", "stream"} {
		t.Run(mode, func(t *testing.T) {
			c := articleFallbackConnection(t, "220 0 <part@fixture.invalid> article follows", "<part@fixture.invalid>")
			var data []byte
			var err error
			switch mode {
			case "decoded":
				data, err = c.GetDecodedBody("part@fixture.invalid")
			case "prefix":
				var meta *YencMetadata
				meta, err = c.GetHeaderPrefix("part@fixture.invalid", 4)
				if meta != nil {
					data = meta.Snippet
				}
			case "raw":
				data, err = c.GetBody("part@fixture.invalid")
				if err == nil && bytes.Contains(data, []byte("=ybegin ")) {
					data = []byte("AAAA")
				}
			case "stream":
				var b bytes.Buffer
				_, err = c.StreamBody("part@fixture.invalid", &b)
				data = b.Bytes()
			}
			if err != nil || !bytes.Equal(data, []byte("AAAA")) {
				t.Fatalf("fallback failed: data=%q err=%v", data, err)
			}
			if _, _, err := c.Stat("part@fixture.invalid"); err != nil {
				t.Fatalf("fallback did not preserve response framing: %v", err)
			}
		})
	}
}

func TestArticleFallbackRejectsWrongIdentity(t *testing.T) {
	for _, tc := range []struct{ status, id string }{
		{"220 0 <wrong@fixture.invalid> article follows", "<part@fixture.invalid>"},
		{"220 0 <part@fixture.invalid> article follows", "<wrong@fixture.invalid>"},
		{"220 0 <part@fixture.invalid> article follows", ""},
		{"220 0 <part@fixture.invalid> article follows", "<part@fixture.invalid>\r\n <wrong@fixture.invalid>"},
		{"220 0 <part@fixture.invalid> article follows", strings.Repeat("x", 65537)},
	} {
		c := articleFallbackConnection(t, tc.status, tc.id)
		if data, err := c.GetDecodedBody("part@fixture.invalid"); err == nil || len(data) > 0 {
			t.Fatalf("wrong article accepted: err=%v", err)
		}
	}
}

func TestArticleFallbackRetainsCRCValidation(t *testing.T) {
	c := articleFallbackConnection(t, "220 0 <part@fixture.invalid> article follows", "<part@fixture.invalid>", func(body string) string { return strings.Replace(body, "kkkk", "kkkl", 1) })
	if _, err := c.GetDecodedBody("part@fixture.invalid"); err == nil {
		t.Fatal("corrupt fallback article accepted")
	}
}

func TestArticleFallbackDoesNotHideMissingOrUnsupportedResponse(t *testing.T) {
	for _, status := range []string{"430 no such article", "500 command not supported"} {
		c := articleFallbackConnection(t, status, "")
		if _, err := c.GetDecodedBody("part@fixture.invalid"); !IsArticleNotFoundError(err) {
			t.Fatalf("lost missing-article classification: %v", err)
		}
	}
}
