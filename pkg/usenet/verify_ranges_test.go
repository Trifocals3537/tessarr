package usenet

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"net"
	"net/textproto"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

type exactRangeReaderFunc func(context.Context, []byte, int64) (int, error)

func (f exactRangeReaderFunc) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	return f(ctx, p, off)
}

func TestExactFailedRangesRejectIncompleteAndCanceledReads(t *testing.T) {
	for _, tt := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"short EOF", 3, io.EOF, io.ErrUnexpectedEOF}, {"short nil", 3, nil, io.ErrUnexpectedEOF},
		{"full EOF", 4, io.EOF, nil}, {"transport", 4, context.DeadlineExceeded, context.DeadlineExceeded},
		{"missing", 0, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound}, customerror.UsenetSegmentMissingError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyExactRange(context.Background(), exactRangeReaderFunc(func(context.Context, []byte, int64) (int, error) { return tt.n, tt.err }), 100, 96, 4)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
	reader := exactRangeReaderFunc(func(context.Context, []byte, int64) (int, error) { t.Fatal("invalid range read"); return 0, nil })
	for _, r := range [][2]int64{{-1, 1}, {0, 0}, {99, 2}, {1<<63 - 1, 2}} {
		if err := verifyExactRange(context.Background(), reader, 100, r[0], r[1]); err == nil {
			t.Fatalf("invalid range accepted: %v", r)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verifyExactRange(ctx, reader, 100, 1, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
}

func TestExactFailedRangeUsesBoundedChunks(t *testing.T) {
	var offsets []int64
	maximum := 0
	reader := exactRangeReaderFunc(func(_ context.Context, p []byte, off int64) (int, error) {
		offsets = append(offsets, off)
		maximum = max(maximum, len(p))
		return len(p), nil
	})
	if err := verifyExactRange(context.Background(), reader, 300000, 1024, 200000); err != nil {
		t.Fatal(err)
	}
	if maximum > 64*1024 || len(offsets) != 4 || offsets[0] != 1024 {
		t.Fatalf("unbounded/wrong range: %d %v", maximum, offsets)
	}
}

func TestValidHeadDoesNotHideTransportFailure(t *testing.T) {
	head := paddedVerificationHead([]byte{0x1a, 0x45, 0xdf, 0xa3})
	if err := verificationError("video.mkv", head, io.ErrUnexpectedEOF); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("transport failure hidden: %v", err)
	}
}

func TestRangeRecheckThroughProductionNNTPReader(t *testing.T) {
	// Fallback roots are process-global and set once. Isolate the fixture CA;
	// production TLS verification and authentication policy remain enabled.
	cmd := exec.Command(os.Args[0], "-test.run=^TestRangeNNTPReaderHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "TESSARR_RANGE_NNTP_HELPER=1", "GODEBUG=x509usefallbackroots=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("production reader fixture: %v %s", err, out)
	}
}

func TestRangeNNTPReaderHelper(t *testing.T) {
	if os.Getenv("TESSARR_RANGE_NNTP_HELPER") != "1" {
		return
	}
	defer logger.Close()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	x509.SetFallbackRoots(roots)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	head := bytes.Repeat([]byte{0xab}, 716800)
	copy(head, []byte{0x1a, 0x45, 0xdf, 0xa3})
	bodies := map[string][]byte{"head@fixture": head, "tail@fixture": bytes.Repeat([]byte{0xcd}, 700), "short@fixture": bytes.Repeat([]byte{0xef}, 398336), "garbage@fixture": bytes.Repeat([]byte{0xa0}, 716800)}
	var mu sync.Mutex
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
				p := textproto.NewConn(conn)
				_ = p.PrintfLine("200 fixture ready")
				for {
					command, err := p.ReadLine()
					if err != nil {
						return
					}
					parts := strings.Fields(command)
					if len(parts) == 0 {
						return
					}
					id := ""
					if len(parts) > 1 {
						id = strings.Trim(parts[1], "<>")
					}
					switch parts[0] {
					case "STAT":
						_ = p.PrintfLine("223 0 <%s>", id)
					case "BODY":
						mu.Lock()
						data := append([]byte(nil), bodies[id]...)
						mu.Unlock()
						if data == nil {
							_ = p.PrintfLine("430 missing")
							continue
						}
						_ = p.PrintfLine("222 0 <%s> body follows", id)
						w := p.DotWriter()
						_, _ = w.Write(encodeRangeFixtureYenc(data))
						_ = w.Close()
					case "ARTICLE":
						_ = p.PrintfLine("430 missing")
					case "QUIT":
						_ = p.PrintfLine("205 bye")
						return
					case "DATE":
						_ = p.PrintfLine("111 20261025000000")
					default:
						_ = p.PrintfLine("500 unsupported")
					}
				}
			}()
		}
	}()
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	root := t.TempDir()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Debrids: []config.Debrid{{Name: "realdebrid", APIKey: "fixture"}}, Usenet: config.Usenet{DiskBufferPath: filepath.Join(root, "streams"), Providers: []config.UsenetProvider{{Host: host, Port: port, SSL: true, MaxConnections: 2}}}}
	raw, _ := json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(root, "config.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.Get()
	u := newMetadataTestUsenet(t)
	client, err := nntp.NewClient(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	u.nntp = client
	defer func() { _ = u.Close(); _ = listener.Close(); <-acceptDone; connections.Wait() }()
	file := storage.NZBFile{NzbID: testNZBID, Name: "video.mkv", Size: 717500, FileType: storage.NZBFileTypeMedia, Segments: []storage.NZBSegment{{Number: 1, MessageID: "head@fixture", Bytes: 716800, StartOffset: 0, EndOffset: 716799}, {Number: 2, MessageID: "tail@fixture", Bytes: 700, StartOffset: 716800, EndOffset: 717499}}}
	save := func() {
		t.Helper()
		if err := u.nzbStorage.AddNZB(&storage.NZB{ID: testNZBID, Name: "fixture", Files: []storage.NZBFile{file}}); err != nil {
			t.Fatal(err)
		}
	}
	save()
	rangeToCheck := []storage.FailedReadRange{{InfoHash: testNZBID, FileName: file.Name, Offset: 717000, Length: 500}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := u.VerifyFileRanges(ctx, testNZBID, file.Name, rangeToCheck); err != nil {
		t.Fatalf("valid tail failed: %v", err)
	}
	mu.Lock()
	delete(bodies, "tail@fixture")
	mu.Unlock()
	if err := u.CheckFile(ctx, testNZBID, file.Name); err != nil {
		t.Fatalf("fixture STAT should succeed: %v", err)
	}
	if err := u.VerifyFileRanges(ctx, testNZBID, file.Name, rangeToCheck); !errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("missing tail accepted despite valid head/STAT: %v", err)
	}
	mu.Lock()
	bodies["tail@fixture"] = bytes.Repeat([]byte{0xcd}, 700)
	mu.Unlock()
	if err := u.VerifyFileRanges(ctx, testNZBID, file.Name, rangeToCheck); err != nil {
		t.Fatalf("restored failed range not recovered: %v", err)
	}
	before, _ := u.FileRepairFingerprint(testNZBID, file.Name)
	file.Segments[0].MessageID = "short@fixture"
	save()
	after, _ := u.FileRepairFingerprint(testNZBID, file.Name)
	if before == after {
		t.Fatal("metadata change did not invalidate progress")
	}
	if err := u.CheckFile(ctx, testNZBID, file.Name); err != nil {
		t.Fatalf("short article STAT should succeed: %v", err)
	}
	if err := u.VerifyFileRanges(ctx, testNZBID, file.Name, rangeToCheck); err == nil {
		t.Fatal("CRC-valid short mapped segment accepted")
	}
	file.Segments[0].MessageID = "garbage@fixture"
	save()
	if err := u.VerifyFileRanges(ctx, testNZBID, file.Name, rangeToCheck); !errors.Is(err, customerror.UsenetCorruptContentError) {
		t.Fatalf("wrong head accepted: %v", err)
	}
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := u.CheckFile(canceled, testNZBID, file.Name); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled availability became healthy: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closedCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := u.CheckFile(closedCtx, testNZBID, file.Name); err == nil || errors.Is(err, customerror.UsenetSegmentMissingError) {
		t.Fatalf("connection failure not inconclusive: %v", err)
	}
	if err := u.CheckFileAvailability(closedCtx, &file, 100); err != nil {
		t.Fatalf("import availability behavior changed: %v", err)
	}
}

func encodeRangeFixtureYenc(data []byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "=ybegin part=1 line=128 size=%d name=fixture.mkv\r\n=ypart begin=1 end=%d\r\n", len(data), len(data))
	column := 0
	for _, v := range data {
		v += 42
		if v == 0 || v == '\r' || v == '\n' || v == '=' {
			b.WriteByte('=')
			v += 64
			column++
		}
		b.WriteByte(v)
		column++
		if column >= 128 {
			b.WriteString("\r\n")
			column = 0
		}
	}
	b.WriteString("\r\n")
	fmt.Fprintf(&b, "=yend size=%d part=1 pcrc32=%08x\r\n", len(data), crc32.ChecksumIEEE(data))
	return b.Bytes()
}
