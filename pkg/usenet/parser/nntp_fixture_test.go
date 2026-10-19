package parser

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash/crc32"
	"net"
	"net/http/httptest"
	"net/textproto"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/rs/zerolog"
)

var contentTestTLSOnce sync.Once
var contentTestCertificate tls.Certificate

// The process-wide logger must outlive individual fixture directories.
// Windows cannot remove a directory while its log file is still open.
func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "tessarr-parser-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	config.SetConfigPath(root)
	_ = logger.New("parser-test")
	code := m.Run()
	if err := logger.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	if err := os.RemoveAll(root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

// The loopback protocol and yEnc fixture follow upstream's nntpd test helper,
// without its benchmark pacing or production-facing dependencies.
func newContentTestParser(t *testing.T, articles map[string][]byte) *NZBParser {
	t.Helper()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	_ = config.Get()
	// Trust the fixture certificate in this package's test process without
	// disabling peer verification. x509 permits installing these roots once.
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1")
	contentTestTLSOnce.Do(func() {
		server := httptest.NewTLSServer(nil)
		defer server.Close()
		contentTestCertificate = server.TLS.Certificates[0]
		roots := x509.NewCertPool()
		roots.AddCert(server.Certificate())
		x509.SetFallbackRoots(roots)
	})
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{contentTestCertificate},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	conns := make(map[net.Conn]struct{})
	closed := false
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				_ = conn.Close()
				return
			}
			conns[conn] = struct{}{}
			mu.Unlock()
			workers.Go(func() {
				defer func() {
					_ = conn.Close()
					mu.Lock()
					delete(conns, conn)
					mu.Unlock()
				}()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				protocol := textproto.NewConn(conn)
				if protocol.PrintfLine("200 fixture ready") != nil {
					return
				}
				for {
					line, err := protocol.ReadLine()
					if err != nil {
						return
					}
					fields := strings.Fields(line)
					if len(fields) == 0 {
						return
					}
					arg := fields[len(fields)-1]
					switch strings.ToUpper(fields[0]) {
					case "AUTHINFO":
						if len(fields) > 1 && strings.EqualFold(fields[1], "USER") {
							err = protocol.PrintfLine("381 password required")
						} else {
							err = protocol.PrintfLine("281 authentication accepted")
						}
					case "DATE":
						err = protocol.PrintfLine("111 20260101000000")
					case "STAT", "BODY":
						body, exists := articles[arg]
						if !exists {
							err = protocol.PrintfLine("430 no such article")
						} else if strings.EqualFold(fields[0], "STAT") {
							err = protocol.PrintfLine("223 0 %s", arg)
						} else {
							if protocol.PrintfLine("222 0 %s body", arg) != nil {
								return
							}
							writer := protocol.DotWriter()
							if _, err = writer.Write(body); err != nil {
								return
							}
							err = writer.Close()
						}
					case "QUIT":
						_ = protocol.PrintfLine("205 bye")
						return
					default:
						err = protocol.PrintfLine("500 unexpected fixture command")
					}
					if err != nil {
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		closed = true
		for conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	client, err := nntp.NewClient(&config.Config{Usenet: config.Usenet{
		Providers: []config.UsenetProvider{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 2, SSL: true}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return NewParser(client, 2, zerolog.Nop())
}

func encodeContentTestArticle(payload []byte, name string) []byte {
	var body bytes.Buffer
	fmt.Fprintf(&body, "=ybegin part=1 line=128 size=%d name=%s\r\n", len(payload), name)
	fmt.Fprintf(&body, "=ypart begin=1 end=%d\r\n", len(payload))
	column := 0
	for _, value := range payload {
		encoded := value + 42
		if encoded == 0 || encoded == '\n' || encoded == '\r' || encoded == '=' || encoded == '\t' || encoded == ' ' || encoded == '.' {
			body.WriteByte('=')
			body.WriteByte(encoded + 64)
			column += 2
		} else {
			body.WriteByte(encoded)
			column++
		}
		if column >= 128 {
			body.WriteString("\r\n")
			column = 0
		}
	}
	if column > 0 {
		body.WriteString("\r\n")
	}
	fmt.Fprintf(&body, "=yend size=%d part=1 pcrc32=%08x\r\n", len(payload), crc32.ChecksumIEEE(payload))
	return body.Bytes()
}
