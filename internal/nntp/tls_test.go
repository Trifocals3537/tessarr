package nntp

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

func TestVerifiedNNTPConfigRejectsUntrustedAndUsesTrustedRoots(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)

	certificate := server.Certificate()
	serverName := certificate.Subject.CommonName
	if len(certificate.DNSNames) > 0 {
		serverName = certificate.DNSNames[0]
	}

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	connection, err := tls.DialWithDialer(
		dialer,
		"tcp",
		server.Listener.Addr().String(),
		verifiedNNTPConfig(serverName),
	)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("NNTP TLS connection with an untrusted certificate succeeded")
	}

	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	trustedConfig := verifiedNNTPConfig(serverName)
	trustedConfig.RootCAs = roots

	connection, err = tls.DialWithDialer(
		dialer,
		"tcp",
		server.Listener.Addr().String(),
		trustedConfig,
	)
	if err != nil {
		t.Fatalf("NNTP TLS connection with an explicitly trusted certificate failed: %v", err)
	}
	defer connection.Close()

	if connection.ConnectionState().Version < tls.VersionTLS12 {
		t.Fatalf("negotiated TLS version = %x, want TLS 1.2 or newer", connection.ConnectionState().Version)
	}
}

func TestStartTLSRejectsUntrustedCertificateDuringHandshake(t *testing.T) {
	certificateServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	serverCertificate := certificateServer.TLS.Certificates[0]
	leafCertificate := certificateServer.Certificate()
	certificateServer.Close()

	serverName := leafCertificate.Subject.CommonName
	if len(leafCertificate.DNSNames) > 0 {
		serverName = leafCertificate.DNSNames[0]
	}

	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() {
		_ = clientSide.Close()
		_ = serverSide.Close()
	})

	reader := bufio.NewReader(clientSide)
	connection := &Connection{
		address: serverName,
		conn:    clientSide,
		reader:  reader,
		text:    textproto.NewReader(reader),
		writer:  bufio.NewWriter(clientSide),
		logger:  zerolog.Nop(),
	}

	serverResult := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(serverSide)
		command, err := reader.ReadString('\n')
		if err != nil {
			serverResult <- err
			return
		}
		if command != "STARTTLS\r\n" {
			serverResult <- fmt.Errorf("command = %q, want STARTTLS", command)
			return
		}
		if _, err := serverSide.Write([]byte("382 Continue with TLS negotiation\r\n")); err != nil {
			serverResult <- err
			return
		}

		tlsServer := tls.Server(serverSide, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
			MinVersion:   tls.VersionTLS12,
		})
		serverResult <- tlsServer.Handshake()
	}()

	err := connection.startTLS(context.Background())
	if err == nil {
		t.Fatal("STARTTLS accepted an untrusted certificate")
	}
	if !strings.Contains(err.Error(), "STARTTLS handshake failed") {
		t.Fatalf("STARTTLS error = %q, want a handshake verification error", err)
	}

	select {
	case <-serverResult:
	case <-time.After(2 * time.Second):
		t.Fatal("STARTTLS test server did not finish its handshake")
	}
}

func localNNTPListener(t *testing.T) (net.Listener, config.UsenetProvider) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	host, portString, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}

	return listener, config.UsenetProvider{
		Host:     host,
		Port:     port,
		Username: "test-user",
		Password: "test-password",
	}
}

func localServerCertificate(t *testing.T, host string) (tls.Certificate, *x509.Certificate) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test key: %v", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create test certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse test certificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  privateKey,
		Leaf:        leaf,
	}, leaf
}

func readNNTPCommand(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	return strings.TrimSpace(line), err
}

func assertNoAUTHCommand(t *testing.T, commands []string) {
	t.Helper()
	for _, command := range commands {
		if strings.HasPrefix(command, "AUTHINFO ") {
			t.Fatalf("credentials were sent before verified TLS: %q", command)
		}
	}
}

func TestCreateConnectionRefusesAUTHWhenSTARTTLSUnsupported(t *testing.T) {
	listener, provider := localNNTPListener(t)
	serverCommands := make(chan []string, 1)

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverCommands <- nil
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))

		_, _ = io.WriteString(connection, "200 test server ready\r\n")
		reader := bufio.NewReader(connection)
		command, _ := readNNTPCommand(reader)
		commands := []string{command}
		_, _ = io.WriteString(connection, "500 STARTTLS unavailable\r\n")

		if next, err := readNNTPCommand(reader); next != "" {
			commands = append(commands, next)
		} else if err == nil {
			commands = append(commands, next)
		}
		serverCommands <- commands
	}()

	client := &Client{logger: zerolog.Nop()}
	connection, err := client.createConnection(context.Background(), provider)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("connection succeeded without STARTTLS")
	}

	commands := <-serverCommands
	if len(commands) == 0 || commands[0] != "STARTTLS" {
		t.Fatalf("commands = %q, want STARTTLS first", commands)
	}
	assertNoAUTHCommand(t, commands)
}

func TestCreateConnectionRefusesAUTHWhenSTARTTLSCertificateIsUntrusted(t *testing.T) {
	listener, provider := localNNTPListener(t)
	serverCertificate, _ := localServerCertificate(t, provider.Host)
	serverCommands := make(chan []string, 1)

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverCommands <- nil
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))

		_, _ = io.WriteString(connection, "200 test server ready\r\n")
		reader := bufio.NewReader(connection)
		command, _ := readNNTPCommand(reader)
		commands := []string{command}
		_, _ = io.WriteString(connection, "382 Continue with TLS negotiation\r\n")

		tlsServer := tls.Server(connection, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
			MinVersion:   tls.VersionTLS12,
		})
		if err := tlsServer.Handshake(); err == nil {
			tlsReader := bufio.NewReader(tlsServer)
			if next, _ := readNNTPCommand(tlsReader); next != "" {
				commands = append(commands, next)
			}
		}
		serverCommands <- commands
	}()

	client := &Client{logger: zerolog.Nop()}
	connection, err := client.createConnection(context.Background(), provider)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("connection succeeded with an untrusted STARTTLS certificate")
	}

	commands := <-serverCommands
	if len(commands) == 0 || commands[0] != "STARTTLS" {
		t.Fatalf("commands = %q, want STARTTLS first", commands)
	}
	assertNoAUTHCommand(t, commands)
}

func TestCreateConnectionAuthenticatesOnlyAfterTrustedSTARTTLS(t *testing.T) {
	listener, provider := localNNTPListener(t)
	serverCertificate, leafCertificate := localServerCertificate(t, provider.Host)
	serverCommands := make(chan []string, 1)

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverCommands <- nil
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))

		_, _ = io.WriteString(connection, "200 test server ready\r\n")
		reader := bufio.NewReader(connection)
		startTLS, _ := readNNTPCommand(reader)
		commands := []string{startTLS}
		_, _ = io.WriteString(connection, "382 Continue with TLS negotiation\r\n")

		tlsServer := tls.Server(connection, &tls.Config{
			Certificates: []tls.Certificate{serverCertificate},
			MinVersion:   tls.VersionTLS12,
		})
		if err := tlsServer.Handshake(); err != nil {
			serverCommands <- commands
			return
		}

		tlsReader := bufio.NewReader(tlsServer)
		username, _ := readNNTPCommand(tlsReader)
		commands = append(commands, username)
		_, _ = io.WriteString(tlsServer, "381 password required\r\n")

		password, _ := readNNTPCommand(tlsReader)
		commands = append(commands, password)
		_, _ = io.WriteString(tlsServer, "281 authentication accepted\r\n")
		serverCommands <- commands
	}()

	roots := x509.NewCertPool()
	roots.AddCert(leafCertificate)
	client := &Client{
		logger:    zerolog.Nop(),
		tlsConfig: &tls.Config{RootCAs: roots},
	}
	connection, err := client.createConnection(context.Background(), provider)
	if err != nil {
		t.Fatalf("trusted STARTTLS connection failed: %v", err)
	}
	defer connection.Close()

	if _, ok := connection.conn.(*tls.Conn); !ok {
		t.Fatalf("connection type = %T, want *tls.Conn", connection.conn)
	}

	commands := <-serverCommands
	want := []string{
		"STARTTLS",
		"AUTHINFO USER " + provider.Username,
		"AUTHINFO PASS " + provider.Password,
	}
	if fmt.Sprint(commands) != fmt.Sprint(want) {
		t.Fatalf("commands = %q, want %q", commands, want)
	}
}

func TestImplicitTLSHandshakeIsBounded(t *testing.T) {
	originalTimeouts := timeouts
	timeouts.HandshakeTimeout = 100 * time.Millisecond
	defer func() { timeouts = originalTimeouts }()

	listener, provider := localNNTPListener(t)
	provider.SSL = true
	serverDone := make(chan struct{})
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			defer connection.Close()
			_, _ = io.Copy(io.Discard, connection)
		}
		close(serverDone)
	}()

	started := time.Now()
	client := &Client{logger: zerolog.Nop()}
	connection, err := client.createConnection(context.Background(), provider)
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil {
		t.Fatal("stalled implicit TLS handshake succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stalled TLS handshake took %s, want under 1s", elapsed)
	}

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("stalled TLS server did not observe connection closure")
	}
}

func TestImplicitTLSHandshakeHonorsContextCancellation(t *testing.T) {
	originalTimeouts := timeouts
	timeouts.HandshakeTimeout = 5 * time.Second
	defer func() { timeouts = originalTimeouts }()

	listener, provider := localNNTPListener(t)
	provider.SSL = true
	serverAccepted := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			close(serverAccepted)
			defer connection.Close()
			_, _ = io.Copy(io.Discard, connection)
		} else {
			close(serverAccepted)
		}
		close(serverDone)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		connection, err := (&Client{logger: zerolog.Nop()}).createConnection(ctx, provider)
		if connection != nil {
			_ = connection.Close()
		}
		result <- err
	}()

	<-serverAccepted
	cancel()
	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("implicit TLS cancellation error = %v, want context.Canceled", err)
	}

	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("TLS server did not observe connection closure after cancellation")
	}
}
