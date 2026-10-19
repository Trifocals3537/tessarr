package nntp

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/rs/zerolog"
)

func newPipeConnection(t *testing.T, respond bool) *Connection {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	conn := &Connection{
		conn:   clientSide,
		reader: bufio.NewReader(clientSide),
		writer: bufio.NewWriter(clientSide),
	}
	t.Cleanup(func() { _ = conn.Close() })

	if !respond {
		_ = serverSide.Close()
		return conn
	}

	go func() {
		defer serverSide.Close()
		reader := bufio.NewReader(serverSide)
		for {
			if _, err := reader.ReadString('\n'); err != nil {
				return
			}
			if _, err := serverSide.Write([]byte("111 20260720000000\r\n")); err != nil {
				return
			}
		}
	}()
	return conn
}

func newReaperTestClient(pool *ProviderPool) *Client {
	return &Client{
		pools:          map[string]*ProviderPool{"test": pool},
		logger:         zerolog.Nop(),
		idleTimeout:    5 * time.Minute,
		staleThreshold: time.Minute,
		pingInterval:   30 * time.Second,
	}
}

func newReaperTestPool(maxConnections int) *ProviderPool {
	return &ProviderPool{
		conns:  make([]*connectionEntry, 0, maxConnections),
		slots:  make(chan struct{}, maxConnections),
		max:    maxConnections,
		config: config.UsenetProvider{Host: "test"},
	}
}

func addReaperTestEntry(pool *ProviderPool, conn *Connection, idleFor time.Duration) *connectionEntry {
	entry := acquireConnectionEntry(conn, pool.config, utils.Now().Add(-idleFor))
	pool.conns = append(pool.conns, entry)
	return entry
}

func TestReaperKeepsAndPingsIdleConnection(t *testing.T) {
	pool := newReaperTestPool(4)
	conn := newPipeConnection(t, true)
	addReaperTestEntry(pool, conn, 40*time.Second)
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 1 {
		t.Fatalf("expected connection kept in pool, got %d entries", len(pool.conns))
	}
	entry := pool.conns[0]
	if entry.lastPing.IsZero() {
		t.Error("expected keepalive to record the successful ping")
	}
	if entry.conn.IsClosed() {
		t.Error("expected connection to stay open after a successful ping")
	}
	if len(pool.slots) != 0 {
		t.Errorf("expected all slots released, %d still held", len(pool.slots))
	}
}

func TestReaperSkipsRecentlyActiveConnection(t *testing.T) {
	pool := newReaperTestPool(4)
	conn := newPipeConnection(t, true)
	entry := addReaperTestEntry(pool, conn, 5*time.Second)
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 1 || pool.conns[0] != entry {
		t.Fatal("expected recently active entry to remain untouched")
	}
	if !entry.lastPing.IsZero() {
		t.Error("expected no keepalive ping for a recently active connection")
	}
}

func TestReaperClosesExpiredConnection(t *testing.T) {
	pool := newReaperTestPool(4)
	conn := newPipeConnection(t, true)
	addReaperTestEntry(pool, conn, 6*time.Minute)
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 0 {
		t.Fatalf("expected expired connection removed, got %d entries", len(pool.conns))
	}
	if !conn.IsClosed() {
		t.Error("expected expired connection to be closed")
	}
}

func TestReaperExpiryUsesRealWorkNotKeepalive(t *testing.T) {
	pool := newReaperTestPool(4)
	conn := newPipeConnection(t, true)
	entry := addReaperTestEntry(pool, conn, 6*time.Minute)
	entry.lastPing = utils.Now()
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 0 {
		t.Fatal("expected keepalive activity not to postpone real idle expiry")
	}
	if !conn.IsClosed() {
		t.Error("expected truly idle connection to be closed")
	}
}

func TestReaperClosesConnectionOnFailedPing(t *testing.T) {
	pool := newReaperTestPool(4)
	conn := newPipeConnection(t, false)
	addReaperTestEntry(pool, conn, 40*time.Second)
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 0 {
		t.Fatalf("expected stale connection removed, got %d entries", len(pool.conns))
	}
	if !conn.IsClosed() {
		t.Error("expected stale connection to be closed")
	}
	if len(pool.slots) != 0 {
		t.Errorf("expected slot released after failed ping, %d still held", len(pool.slots))
	}
}

func TestReaperSkipsPingWhenPoolBusy(t *testing.T) {
	pool := newReaperTestPool(1)
	pool.slots <- struct{}{}
	conn := newPipeConnection(t, true)
	entry := addReaperTestEntry(pool, conn, 40*time.Second)
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.conns) != 1 || pool.conns[0] != entry {
		t.Fatal("expected entry kept without ping when no slot is free")
	}
	if !entry.lastPing.IsZero() {
		t.Error("expected no ping while the pool is busy")
	}
}

func TestReaperPingsOnlyBoundedBatch(t *testing.T) {
	pool := newReaperTestPool(10)
	for range 10 {
		addReaperTestEntry(pool, newPipeConnection(t, true), 40*time.Second)
	}
	client := newReaperTestClient(pool)

	client.reapIdleConnections()

	pool.mu.Lock()
	defer pool.mu.Unlock()
	pinged := 0
	for _, entry := range pool.conns {
		if !entry.lastPing.IsZero() {
			pinged++
		}
	}
	if pinged != 2 {
		t.Errorf("pinged %d connections, want bounded quarter-pool batch of 2", pinged)
	}
	if len(pool.conns) != 10 {
		t.Errorf("pool size = %d after keepalive, want 10", len(pool.conns))
	}
	if len(pool.slots) != 0 {
		t.Errorf("expected all slots released, %d still held", len(pool.slots))
	}
}

func TestNormalizeTimeoutsKeepsPingInsideIdleWindow(t *testing.T) {
	got := normalizeTimeouts(TimeoutConfig{})
	if got.PingInterval != 30*time.Second {
		t.Errorf("default PingInterval = %v, want 30s", got.PingInterval)
	}
	if got.IdleTimeout != 5*time.Minute {
		t.Errorf("default IdleTimeout = %v, want 5m", got.IdleTimeout)
	}

	got = normalizeTimeouts(TimeoutConfig{
		IdleTimeout:  20 * time.Second,
		PingInterval: time.Minute,
	})
	if got.PingInterval != 10*time.Second {
		t.Errorf("clamped PingInterval = %v, want 10s", got.PingInterval)
	}
}

func TestKeepAliveBatchSizePreservesPoolCapacity(t *testing.T) {
	tests := []struct {
		name             string
		pooled, capacity int
		want             int
	}{
		{name: "empty", pooled: 0, capacity: 10, want: 0},
		{name: "single connection", pooled: 1, capacity: 1, want: 1},
		{name: "small pool leaves one", pooled: 2, capacity: 10, want: 1},
		{name: "quarter of ordinary pool", pooled: 10, capacity: 10, want: 2},
		{name: "large pool is capped", pooled: 100, capacity: 100, want: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := keepAliveBatchSize(test.pooled, test.capacity); got != test.want {
				t.Errorf("keepAliveBatchSize(%d, %d) = %d, want %d",
					test.pooled, test.capacity, got, test.want)
			}
		})
	}
}

func TestSetIdleTimeoutAppliesValidOverride(t *testing.T) {
	client := &Client{
		idleTimeout:    5 * time.Minute,
		staleThreshold: time.Minute,
		pingInterval:   30 * time.Second,
	}
	if err := client.setIdleTimeout("2m"); err != nil {
		t.Fatalf("setIdleTimeout: %v", err)
	}

	if client.idleTimeout != 2*time.Minute {
		t.Errorf("idleTimeout = %v, want 2m", client.idleTimeout)
	}
	if client.pingInterval <= 0 || client.pingInterval >= client.idleTimeout {
		t.Errorf("pingInterval = %v, want inside idle window %v", client.pingInterval, client.idleTimeout)
	}
	if client.staleThreshold <= 0 || client.staleThreshold >= client.idleTimeout {
		t.Errorf("staleThreshold = %v, want inside idle window %v", client.staleThreshold, client.idleTimeout)
	}

	if err := client.setIdleTimeout("0"); err == nil {
		t.Error("expected non-positive timeout to be rejected")
	}
}
