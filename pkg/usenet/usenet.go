package usenet

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet/fs"
	"github.com/Trifocals3537/tessarr/pkg/usenet/fs/reader"
	"github.com/Trifocals3537/tessarr/pkg/usenet/parser"
	"github.com/Trifocals3537/tessarr/pkg/usenet/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

const (
	bufferSize = 256 * 1024 // 256KB buffer for streaming
)

var streamBufferPool = sync.Pool{
	New: func() any {
		return new([bufferSize]byte)
	},
}

func acquireStreamBuffer() []byte {
	return streamBufferPool.Get().(*[bufferSize]byte)[:]
}

func releaseStreamBuffer(buf []byte) {
	if buf == nil {
		return
	}
	if len(buf) < bufferSize {
		return
	}
	streamBufferPool.Put((*[bufferSize]byte)(buf[:bufferSize]))
}

type fsEntry struct {
	fs            *fs.FS
	volumes       []*types.Volume
	reader        fs.PrefetchableReaderAt // Shared reader with prefetch capability
	readerSize    int64                   // Size of the volume
	readerCleanup func()                  // Cleanup function for reader
	readerOnce    sync.Once               // Ensures reader is created exactly once
	readerErr     error                   // Error from reader creation (if any)
	refCount      atomic.Int32
	lastAccessed  atomic.Int64 // Unix timestamp
}

// fsEntryTombstone marks an entry claimed for teardown. Once refCount holds
// this value no new stream can acquire the entry (see acquire), which is what
// makes cleanup safe against a concurrent Stream that already Load()ed the
// entry from the map.
const fsEntryTombstone = int32(-1 << 30)

func (fe *fsEntry) cleanup() {
	if fe.readerCleanup != nil {
		fe.readerCleanup()
		fe.readerCleanup = nil
		fe.reader = nil
	}
}

// acquire takes a reference unless the entry has been claimed for teardown.
func (fe *fsEntry) acquire() bool {
	for {
		n := fe.refCount.Load()
		if n < 0 {
			return false
		}
		if fe.refCount.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// claimForCleanup atomically claims an idle (refCount == 0) entry for
// teardown, fencing out any future acquire.
func (fe *fsEntry) claimForCleanup() bool {
	return fe.refCount.CompareAndSwap(0, fsEntryTombstone)
}

// getOrCreateReader returns the shared reader, creating it lazily on first use.
// Uses sync.Once to ensure the reader is created exactly once even under concurrent access.
func (fe *fsEntry) getOrCreateReader() (fs.PrefetchableReaderAt, int64, error) {
	fe.readerOnce.Do(func() {
		var readerAt fs.PrefetchableReaderAt
		var size int64
		var cleanup func()
		var err error

		// Single volume optimization - skip multi-volume overhead
		if len(fe.volumes) == 1 {
			readerAt, size, cleanup, err = fe.fs.CreateReaderAtForVolume(fe.volumes[0])
		} else {
			// Multi-volume case - need to create reader differently
			// For now, fall back to io.ReaderAt (no prefetch for multi-volume)
			var plainReaderAt io.ReaderAt
			plainReaderAt, size, cleanup, err = fe.fs.CreateReaderAt()
			if err != nil {
				fe.readerErr = err
				return
			}
			// Wrap in a no-op prefetchable reader
			readerAt = &noPrefetchReader{ReaderAt: plainReaderAt}
		}

		if err != nil {
			fe.readerErr = err
			return
		}

		fe.reader = readerAt
		fe.readerSize = size
		fe.readerCleanup = cleanup
	})

	if fe.readerErr != nil {
		return nil, 0, fe.readerErr
	}
	// cleanup() nils the reader after the Once has fired; a caller racing a
	// shutdown-path cleanup must get an error, not a nil interface.
	if fe.reader == nil {
		return nil, 0, fmt.Errorf("reader has been closed")
	}
	return fe.reader, fe.readerSize, nil
}

// noPrefetchReader wraps io.ReaderAt for cases where prefetch isn't available
type noPrefetchReader struct {
	io.ReaderAt
}

func (n *noPrefetchReader) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	nr, err := n.ReaderAt.ReadAt(p, off)
	if ctxErr := ctx.Err(); ctxErr != nil && nr == 0 {
		return 0, ctxErr
	}
	return nr, err
}

func (n *noPrefetchReader) Prefetch(ctx context.Context, off, length int64) {
	// No-op for multi-volume readers
}

type contextSectionReader struct {
	ctx   context.Context
	r     fs.PrefetchableReaderAt
	base  int64
	limit int64
	off   int64
}

func newContextSectionReader(ctx context.Context, r fs.PrefetchableReaderAt, off, length int64) *contextSectionReader {
	ctx = reader.WithPrefetchSession(ctx)
	return &contextSectionReader{
		ctx:   ctx,
		r:     r,
		base:  off,
		limit: length,
	}
}

func (r *contextSectionReader) Read(p []byte) (int, error) {
	if r.off >= r.limit {
		return 0, io.EOF
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	remaining := r.limit - r.off
	if int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	n, err := r.r.ReadAtContext(r.ctx, p, r.base+r.off)
	r.off += int64(n)
	if err == io.EOF && r.off < r.limit {
		return n, io.ErrUnexpectedEOF
	}
	if err == nil && r.off >= r.limit {
		return n, io.EOF
	}
	return n, err
}

type Usenet struct {
	bufferPools              *reader.Pools
	nntp                     *nntp.Client
	logger                   zerolog.Logger
	metadataDir              string
	nzbStorage               *NZBStorage // File-based NZB metadata storage
	maxConnections           int         // Connections allocated per streaming file
	processingMaxConnections int         // Connections allocated per file for parsing and NZB downloads
	prefetchSize             int64       // Streaming prefetch size in bytes
	failedFiles              *xsync.Map[string, *failedFileState]
	failureNow               func() time.Time
	failureProbe             func(context.Context, string, string, int64) error
	contentVerifySlots       chan struct{} // Deep repair probes are serialized to protect playback.

	fs *xsync.Map[string, *fsEntry]

	watcherMu       sync.Mutex
	claimScanner    *NZBClaimScanner
	acceptedCleaner *AcceptedNZBCleaner

	lifecycleOnce sync.Once
	ctx           context.Context
	cancel        context.CancelFunc
	background    sync.WaitGroup
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
}

// fsKey builds a cache key for fs map entries efficiently.
// Uses direct byte slice manipulation to avoid strings.Builder overhead.
func fsKey(nzoID, filename string) string {
	// Single allocation: nzoID + "::" + filename
	buf := make([]byte, len(nzoID)+2+len(filename))
	n := copy(buf, nzoID)
	buf[n] = ':'
	buf[n+1] = ':'
	copy(buf[n+2:], filename)
	return string(buf)
}

// New creates a new usenet instance
func New() (*Usenet, error) {
	cfg := config.Get()
	usenetConfig := cfg.Usenet
	if len(usenetConfig.Providers) == 0 {
		return nil, fmt.Errorf("no usenet providers configured")
	}
	if err := initStreamsDir(usenetConfig.DiskBufferPath); err != nil {
		return nil, fmt.Errorf("initialize usenet stream cache: %w", err)
	}
	_logger := logger.New("usenet")

	metadataDir := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	if _, err := safepath.EnsureRoot(metadataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create metadata dir: %w", err)
	}

	// Create file-based NZB storage
	nzbStorage, err := NewNZBStorage()
	if err != nil {
		return nil, fmt.Errorf("failed to create NZB storage: %w", err)
	}

	// Create NNTP client with retry configuration
	client, err := nntp.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	maxConns := usenetConfig.MaxConnections
	if maxConns <= 0 {
		maxConns = 10
	}
	processingMaxConns := usenetConfig.ProcessingMaxConnections
	if processingMaxConns <= 0 {
		processingMaxConns = maxConns
	}

	prefetchSize, err := config.ParseSize(usenetConfig.ReadAhead)
	if err != nil {
		prefetchSize = 16 * 1024 * 1024 // Default to 16MB
	}

	u := &Usenet{
		bufferPools:              reader.NewPools(usenetConfig.BufferMemoryBytes()),
		nzbStorage:               nzbStorage,
		nntp:                     client,
		logger:                   _logger,
		metadataDir:              metadataDir,
		maxConnections:           maxConns,
		processingMaxConnections: processingMaxConns,
		prefetchSize:             prefetchSize,
		fs:                       xsync.NewMap[string, *fsEntry](),
		failedFiles:              xsync.NewMap[string, *failedFileState](),
		contentVerifySlots:       make(chan struct{}, 1),
	}
	u.initLifecycle()

	// Both workers belong to this instance. Close cancels and joins them before
	// a replacement instance can touch the same metadata or stream state.
	u.startBackground(func(ctx context.Context) {
		if _, err := nzbStorage.MigrateLegacyContext(ctx); err != nil && ctx.Err() == nil {
			nzbStorage.logger.Warn().Err(err).Msg("Legacy NZB meta migration failed")
		}
	})
	u.startBackground(func(ctx context.Context) {
		u.cleanupIdleFS(ctx, 30*time.Second)
	})

	return u, nil
}

func (u *Usenet) initLifecycle() {
	u.lifecycleOnce.Do(func() {
		u.ctx, u.cancel = context.WithCancel(context.Background())
		u.closeDone = make(chan struct{})
	})
}

func (u *Usenet) startBackground(work func(context.Context)) {
	u.initLifecycle()
	u.background.Add(1)
	go func() {
		defer u.background.Done()
		work(u.ctx)
	}()
}

func initStreamsDir(streamsDir string) error {
	_, err := reader.PrepareDiskCacheRoot(streamsDir)
	return err
}

func (u *Usenet) createEntry(file *storage.NZBFile) (*fsEntry, error) {
	return u.createEntryWithReadLimits(file, u.maxConnections, u.prefetchSize)
}

// createEntryWithReadLimits builds an isolated reader with explicit
// concurrency and read-ahead limits. Deep verification uses one foreground
// connection and no prefetch so a 512-byte probe cannot fan out into a full
// streaming window.
func (u *Usenet) createEntryWithReadLimits(file *storage.NZBFile, maxConcurrent int, prefetchSize int64) (*fsEntry, error) {
	volumes := GetFileVolumes(file)
	if len(volumes) == 0 {
		return nil, fmt.Errorf("no volumes available for file %s", file.Name)
	}

	u.initLifecycle()
	if err := u.ctx.Err(); err != nil {
		return nil, err
	}

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	usenetFS, err := fs.NewFS(
		u.ctx,
		u.nntp,
		maxConcurrent,
		prefetchSize,
		volumes,
		u.logger,
		fs.WithPools(u.bufferPools),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create usenet FS: %w", err)
	}

	return &fsEntry{
		fs:      usenetFS,
		volumes: volumes,
	}, nil
}

// getOrCreateEntry returns the fsEntry and its cache key to avoid redundant key computation.
func (u *Usenet) getOrCreateEntry(ctx context.Context, nzoID, filename string) (*fsEntry, string, error) {
	return u.getOrCreateEntryInternal(ctx, nzoID, filename, false)
}

func (u *Usenet) getOrCreateEntryInternal(ctx context.Context, nzoID, filename string, allowFailed bool) (*fsEntry, string, error) {
	key := fsKey(nzoID, filename)
	if !allowFailed {
		if err := u.CheckStreamReady(nzoID, filename); err != nil {
			return nil, key, err
		}
	}

	// Fast path: entry already exists and isn't being torn down. acquire() (a
	// CAS, not a blind Add) is what closes the race against cleanupIdleFS:
	// once the janitor claims an idle entry no new reference can be taken, so
	// a stream can never end up on an entry whose reader is being closed.
	if entry, ok := u.fs.Load(key); ok && entry.acquire() {
		entry.lastAccessed.Store(utils.NowUnix())
		return entry, key, nil
	}

	// Slow path: need to create entry
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return nil, key, err
	}

	// Pre-checks
	if err := u.preStreamChecksInternal(file, allowFailed); err != nil {
		return nil, key, err
	}

	newEntry, err := u.createEntry(file)
	if err != nil {
		return nil, key, err
	}

	// Atomically store only if key doesn't exist (prevents race condition)
	for {
		actual, loaded := u.fs.LoadOrStore(key, newEntry)
		if !loaded {
			// We won the race - use our new entry
			newEntry.refCount.Add(1)
			newEntry.lastAccessed.Store(utils.NowUnix())
			return newEntry, key, nil
		}
		// Another goroutine created the entry first - use theirs.
		// Our newEntry was never used (readers are lazy), GC reclaims it.
		if actual.acquire() {
			actual.lastAccessed.Store(utils.NowUnix())
			return actual, key, nil
		}
		// The mapped entry is claimed for teardown; the janitor removes it
		// from the map immediately after claiming, so retry until our entry
		// can be stored.
		if err := ctx.Err(); err != nil {
			return nil, key, err
		}
		runtime.Gosched()
	}
}

// releaseFS releases an fs entry using a pre-computed key (avoids redundant allocation).
func (u *Usenet) releaseFS(key string) {
	entry, ok := u.fs.Load(key)
	if !ok {
		return
	}

	entry.refCount.Add(-1)
	entry.lastAccessed.Store(utils.NowUnix())
}

// cleanupIdleFS removes sessions with refCount=0 that haven't been used recently.
// It exits with the owning Usenet instance instead of surviving config resets.
func (u *Usenet) cleanupIdleFS(ctx context.Context, interval time.Duration) {
	// Keep a warm reader through short pauses, then tear it down. Usenet segment
	// buffering is only for active latency hiding; stale buffers should disappear
	// quickly instead of behaving like a VFS cache.
	const idleThreshold = int64(120) // 2 minutes idle
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := utils.NowUnix()

			u.fs.Range(func(key string, entry *fsEntry) bool {
				if entry.refCount.Load() == 0 {
					lastUsed := entry.lastAccessed.Load()
					if now-lastUsed > idleThreshold {
						// Claim before touching anything: the CAS fences out a
						// concurrent Stream that already Load()ed this entry from
						// the map (it will fail acquire() and create a fresh
						// entry). Delete from the map before the (potentially
						// slow) cleanup so waiting creators aren't stalled.
						if entry.claimForCleanup() {
							u.fs.Delete(key)
							entry.cleanup()
						}
					}
				}
				return true
			})
		}
	}
}

// Parse processes an NZB for download/streaming (quick parse, defers archive extraction)
func (u *Usenet) Parse(ctx context.Context, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	return u.ParseWithID(ctx, "", name, content, category)
}

// ParseWithID parses an NZB using a caller-provided ID. Supplying the ID lets
// the manager expose a queued entry before the active-download worker starts.
func (u *Usenet) ParseWithID(ctx context.Context, id, name string, content []byte, category string) (*storage.NZB, map[string]*parser.FileGroup, error) {
	if len(content) == 0 {
		return nil, nil, fmt.Errorf("NZB content is empty")
	}
	var canonicalID string
	if id != "" {
		var err error
		canonicalID, err = canonicalNZBID(id)
		if err != nil {
			return nil, nil, err
		}
	}

	// Validate NZB content
	if err := validateNZB(content); err != nil {
		return nil, nil, fmt.Errorf("invalid NZB content: %w", err)
	}

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())

	// Quick parse: defer archive extraction for async processing
	nzb, groups, err := prs.Parse(ctx, name, content)
	if err != nil {
		return nil, nil, err
	}
	if err := safepath.ValidateIdentifier(nzb.Name); err != nil {
		return nil, nil, fmt.Errorf("unsafe NZB name: %w", err)
	}
	if category != "" {
		if err := safepath.ValidateIdentifier(category); err != nil {
			return nil, nil, fmt.Errorf("unsafe NZB category: %w", err)
		}
	}
	if id != "" {
		nzb.ID = canonicalID
	}
	if _, err := canonicalNZBID(nzb.ID); err != nil {
		return nil, nil, err
	}

	nzb.Category = category
	nzb.Status = NZBStatusParsing
	// Save NZB file to disk
	nzbPath, err := u.saveNZBFile(nzb.ID, content)
	if err != nil {
		return nil, nil, err
	}
	nzb.Path = nzbPath

	// Mark as processing
	if err := u.markAsProcessing(nzb); err != nil {
		// Don't leave the source file orphaned; an un-marked .nzb would be
		// re-claimed by the refresh watcher on every scan.
		_ = removeMetadataFileIfExists(u.metadataDir, nzbPath)
		return nil, nil, fmt.Errorf("failed to mark NZB as processing: %w", err)
	}

	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		processingPath, _ := metadataFilePath(u.metadataDir, nzb.ID, nzbProcessingSuffix)
		_ = removeMetadataFileIfExists(u.metadataDir, processingPath)
		_ = removeMetadataFileIfExists(u.metadataDir, nzbPath)
		return nil, nil, fmt.Errorf("failed to save NZB to storage: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Int("groups", len(groups)).
		Msg("Successfully parsed NZB file")
	return nzb, groups, nil
}

// Process processes archive files in an NZB (full parse)
func (u *Usenet) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*parser.FileGroup) (*storage.NZB, error) {
	u.logger.Info().
		Str("nzb_id", nzb.ID).
		Str("name", nzb.Name).
		Msg("Processing archive files in NZB")

	// Create parser with the manager
	prs := parser.NewParser(u.nntp, u.processingMaxConnections, u.logger.With().Str("component", "parser").Logger())
	// Process the groups (archives)
	updatedNZB, err := prs.Process(ctx, nzb, groups)
	if err != nil {
		// Mark as failed
		_ = u.markAsFailed(nzb, err)
		return nzb, fmt.Errorf("failed to process NZB archives: %w", err)
	}
	if err := normalizeLogicalNZBFileNames(updatedNZB.Files); err != nil {
		_ = u.markAsFailed(updatedNZB, err)
		return updatedNZB, err
	}

	// Post-parse availability gate: probe a sample of each content file's
	// segments before declaring the NZB complete. Segments can go missing
	// between the original parse and now; without this gate they slip through
	// to Sonarr/Radarr and only surface later as failed ffprobes. Connection
	// errors are non-fatal here (CheckFileAvailability returns nil for those),
	// so a provider hiccup won't wrongly fail an import — only a definitively
	// missing segment (gone on every provider) fails the NZB.
	if err := u.checkNZBAvailability(ctx, updatedNZB); err != nil {
		_ = u.markAsFailed(updatedNZB, err)
		return updatedNZB, fmt.Errorf("availability check failed: %w", err)
	}

	// Mark as completed
	if err := u.markAsCompleted(updatedNZB); err != nil {
		return updatedNZB, fmt.Errorf("failed to mark NZB as completed: %w", err)
	}

	u.logger.Info().
		Str("nzb_id", updatedNZB.ID).
		Str("name", updatedNZB.Name).
		Int("files", len(updatedNZB.Files)).
		Msg("Successfully processed NZB archives (full parse)")
	return updatedNZB, nil
}

// flattenLogicalNZBFileName retains its historical name, but logical NZB files
// must already be single path components. Separators and traversal are rejected
// before non-portable filename punctuation is sanitized.
func flattenLogicalNZBFileName(name string) (string, error) {
	if err := validateLogicalNZBFileNameSource(name); err != nil {
		return "", err
	}

	sanitized := strings.Map(func(r rune) rune {
		switch r {
		case '?', ':', '"', '<', '>', '|', '*':
			return '_'
		default:
			return r
		}
	}, name)
	if err := safepath.ValidateIdentifier(sanitized); err != nil {
		return "", err
	}
	return sanitized, nil
}

func validateLogicalNZBFileNameSource(name string) error {
	if name == "" {
		return fmt.Errorf("path identifier is empty")
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("path identifier contains a NUL byte")
	}
	if strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("path identifier %q contains a control character", name)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("path identifier %q is traversal", name)
	}
	if filepath.IsAbs(name) || filepath.VolumeName(name) != "" || looksLikeWindowsDrivePrefix(name) {
		return fmt.Errorf("path identifier %q is absolute", name)
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("path identifier %q contains a path separator", name)
	}
	return nil
}

func looksLikeWindowsDrivePrefix(name string) bool {
	return len(name) >= 2 &&
		((name[0] >= 'a' && name[0] <= 'z') || (name[0] >= 'A' && name[0] <= 'Z')) &&
		name[1] == ':'
}

func normalizeLogicalNZBFileNames(files []storage.NZBFile) error {
	normalizedNames := make([]string, len(files))
	seenNames := make(map[string]string, len(files))
	for i := range files {
		originalName := files[i].Name
		normalizedName, err := flattenLogicalNZBFileName(originalName)
		if err != nil {
			return fmt.Errorf("unsafe NZB file name %q: %w", originalName, err)
		}
		collisionKey, err := safepath.PortableNameKey(normalizedName)
		if err != nil {
			return fmt.Errorf("unsafe NZB file name %q: %w", originalName, err)
		}
		if conflictingName, exists := seenNames[collisionKey]; exists {
			return fmt.Errorf(
				"duplicate NZB file name after sanitization: %q and %q both resolve to %q",
				conflictingName,
				originalName,
				normalizedName,
			)
		}
		seenNames[collisionKey] = originalName
		normalizedNames[i] = normalizedName
	}
	for i := range files {
		files[i].Name = normalizedNames[i]
	}
	return nil
}

// checkAvailability samples each content file's segments (via the same
// repair-bank-gated BatchStat path as CheckFile) and returns an error if any
// file is definitively unavailable — i.e. a sampled segment is missing on
// every provider. Recovery/noise files (par2, ignore), deleted files, and
// segment-less entries are skipped so the gate fails only on genuinely missing
// playable content. Connection-only failures are treated as non-fatal by
// CheckFileAvailability, so they do not fail the NZB. It returns on the first
// definitively-missing file (fail fast).
func (u *Usenet) checkNZBAvailability(ctx context.Context, nzb *storage.NZB) error {
	samplePercent := config.Get().Usenet.ImportAvailabilitySamplePercent
	for i := range nzb.Files {
		file := &nzb.Files[i]
		if file.IsDeleted || len(file.Segments) == 0 {
			continue
		}
		switch file.FileType {
		case storage.NZBFileTypePar2, storage.NZBFileTypeIgnore:
			continue
		}
		if ctx.Err() != nil {
			// Cancelled/timed out: not a content failure — don't fail the NZB.
			return nil
		}
		if err := u.CheckFileAvailability(ctx, file, samplePercent); err != nil {
			u.logger.Warn().
				Err(err).
				Str("nzb_id", nzb.ID).
				Str("file", file.Name).
				Msg("Post-parse availability check failed; marking NZB unavailable")
			return fmt.Errorf("file %q unavailable: %w", file.Name, err)
		}
	}
	return nil
}

// CheckFile probes the availability of a single NZB file. Connection use is
// gated by the NNTP client's repair bank so concurrent probes don't starve
// streaming traffic.
func (u *Usenet) CheckFile(ctx context.Context, nzoID, filename string) error {
	// Repair/availability probes only need a sample of one file's message ids.
	// Decode just those (no numeric columns, no NZBSegment structs, no other
	// files) so a full sweep doesn't hold whole segment maps in memory.
	samplePercent := config.Get().Usenet.AvailabilitySamplePercent
	messageIDs, err := u.nzbStorage.SampleFileMessageIDs(nzoID, filename, samplePercent)
	if err != nil {
		return fmt.Errorf("failed to sample file segments: %w", err)
	}
	if len(messageIDs) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}
	return u.checkAvailability(ctx, filename, messageIDs)
}

func (u *Usenet) CheckFileAvailability(ctx context.Context, file *storage.NZBFile, samplePercent int) error {
	return u.checkAvailability(ctx, file.Name, u.sampleSegments(file.Segments, samplePercent))
}

// checkAvailability batch-STATs the given sampled message ids. The NNTP client
// gates each worker through its internal repair bank so concurrent availability
// checks don't starve streaming connections.
func (u *Usenet) checkAvailability(ctx context.Context, fileName string, messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}

	result, err := u.nntp.BatchStat(ctx, messageIDs)
	if err != nil {
		// Connection/system error - log and continue (don't fail availability check)
		u.logger.Warn().
			Err(err).
			Str("file", fileName).
			Msg("Non-fatal error during availability check, ignoring")
		return nil
	}

	// Check if all sampled segments are available.
	// Distinguish genuine article-not-found from connection errors:
	//   TotalCount = FoundCount + notFoundCount + ErrorCount
	// Only treat a file as unavailable when segments are definitively missing
	// (notFoundCount > 0). Connection errors mean we couldn't check — treat
	// those the same as the top-level error path above (non-fatal, skip check).
	if !result.AllAvailable() {
		notFoundCount := result.TotalCount - result.FoundCount - result.ErrorCount
		if result.ErrorCount > 0 && notFoundCount == 0 {
			// All failures were connection errors, not missing articles.
			return nil
		}
		// At least some segments are definitively missing.
		u.logger.Warn().
			Str("file", fileName).
			Int("sampled_segments", len(messageIDs)).
			Int("available_segments", result.FoundCount).
			Int("missing_segments", notFoundCount).
			Int("error_count", result.ErrorCount).
			Msg("File is unavailable - one or more segments are missing")
		return customerror.UsenetSegmentMissingError
	}

	return nil
}

// sampleSegments returns a sample of segment message IDs based on the given
// percentage. Always includes first and last segments, then uniformly samples
// from the middle (see sampleIndices).
func (u *Usenet) sampleSegments(segments []storage.NZBSegment, percent int) []string {
	idx := sampleIndices(len(segments), percent)
	if len(idx) == 0 {
		return nil
	}
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = segments[j].MessageID
	}
	return out
}

func (u *Usenet) Stop() {
	if u == nil {
		return
	}
	u.initLifecycle()
	u.cancel()
	u.logger.Info().Msg("Stopping Usenet")
}

// Close closes all usenet resources including NNTP connections
func (u *Usenet) Close() error {
	if u == nil {
		return nil
	}
	u.initLifecycle()
	u.closeOnce.Do(func() {
		u.closeErr = u.close()
		close(u.closeDone)
	})
	<-u.closeDone
	return u.closeErr
}

func (u *Usenet) close() error {
	u.logger.Info().Msg("Closing Usenet NNTP client")
	var closeErr error

	u.watcherMu.Lock()
	claimScanner := u.claimScanner
	acceptedCleaner := u.acceptedCleaner
	u.claimScanner = nil
	u.acceptedCleaner = nil
	u.watcherMu.Unlock()
	if claimScanner != nil {
		if err := claimScanner.Close(); err != nil {
			u.logger.Warn().Err(err).Msg("Failed to close watched NZB scanner")
		}
	}
	if acceptedCleaner != nil {
		if err := acceptedCleaner.Close(); err != nil {
			u.logger.Warn().Err(err).Msg("Failed to close accepted NZB cleaner")
		}
	}

	// Stop all instance-owned work before a replacement can be created. Closing
	// NNTP below unblocks any active network reads; the wait happens after that.
	u.cancel()

	// Close NNTP client FIRST to force-close all active connections.
	// This unblocks any in-flight StreamBody/TCP reads in prefetch workers,
	// allowing SegmentFetcher.Close() (prefetchWg.Wait()) to complete without hanging.
	if u.nntp != nil {
		if err := u.nntp.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
			u.logger.Warn().Err(err).Msg("Failed to close NNTP client")
		}
	}
	u.background.Wait()

	// Cleanup all active FS entries (fetcher.Close() now completes quickly
	// because connections were already force-closed above)
	if u.fs != nil {
		u.fs.Range(func(key string, entry *fsEntry) bool {
			entry.cleanup()
			return true
		})
		u.fs.Clear()
	}
	if u.bufferPools != nil {
		closeErr = errors.Join(closeErr, u.bufferPools.Close())
	}

	u.logger.Info().Msg("Usenet closed")
	return closeErr
}

func (u *Usenet) getFile(nzoID, filename string) (*storage.NZBFile, error) {
	files, err := u.getFiles(nzoID, []string{filename})
	if err != nil {
		return nil, err
	}
	file := files[filename]
	if file == nil {
		return nil, fmt.Errorf("file %s not found in NZB %s", filename, nzoID)
	}
	return file, nil
}

func (u *Usenet) getFiles(nzoID string, filenames []string) (map[string]*storage.NZBFile, error) {
	nzb, err := u.nzbStorage.GetNZB(nzoID)
	if err != nil {
		return nil, fmt.Errorf("metadata load failed: %w", err)
	}

	requested := make(map[string]struct{}, len(filenames))
	for _, filename := range filenames {
		requested[filename] = struct{}{}
	}

	files := make(map[string]*storage.NZBFile, len(requested))
	for i := range nzb.Files {
		source := nzb.Files[i]
		if source.IsDeleted {
			continue
		}
		if _, ok := requested[source.Name]; !ok {
			continue
		}
		file := source
		if file.NzbID == "" {
			file.NzbID = nzoID
		}
		files[file.Name] = &file
	}
	return files, nil
}

func (u *Usenet) preStreamChecks(file *storage.NZBFile) error {
	return u.preStreamChecksInternal(file, false)
}

func (u *Usenet) preStreamChecksInternal(file *storage.NZBFile, allowFailed bool) error {
	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", file.Name)
	}

	if allowFailed {
		return nil
	}
	return u.CheckStreamReady(file.NzbID, file.Name)
}

// CheckStreamReady returns a permanent error for a file whose serving path has
// already proven that an article is missing across all configured providers.
// Callers use this before writing response headers so later WebDAV requests
// receive a complete HTTP error instead of a successful response with a
// truncated body that clients may retry indefinitely.
func (u *Usenet) CheckStreamReady(nzoID, filename string) error {
	if u == nil || u.failedFiles == nil {
		return nil
	}
	if state, ok := u.failedFiles.Load(fsKey(nzoID, filename)); ok {
		return state.streamError()
	}
	return nil
}

// Stream streams a file using the new streaming system with caching and worker limiting
func (u *Usenet) Stream(ctx context.Context, nzoID, filename string, start, end int64, writer io.Writer) error {
	return u.streamInternal(ctx, nzoID, filename, start, end, writer, false, true)
}

func (u *Usenet) streamInternal(ctx context.Context, nzoID, filename string, start, end int64, writer io.Writer, allowFailed, recordFailure bool) error {
	if start < 0 {
		start = 0
	}
	if end < start {
		return fmt.Errorf("invalid byte range %d-%d", start, end)
	}

	// Use getOrCreateEntry to get both entry and key in one call,
	// avoiding redundant key computation in releaseFS.
	ufsEntry, key, err := u.getOrCreateEntryInternal(ctx, nzoID, filename, allowFailed)
	if err != nil {
		return fmt.Errorf("failed to get or create file system: %w", err)
	}
	defer u.releaseFS(key)

	// Use start/end directly - file segments are already positioned correctly
	rangeStart := start
	rangeEnd := end

	// Validate range against volume size
	if rangeEnd >= ufsEntry.volumes[0].Size {
		rangeEnd = ufsEntry.volumes[0].Size - 1
	}

	if rangeEnd < rangeStart {
		return fmt.Errorf("invalid resolved byte range %d-%d", rangeStart, rangeEnd)
	}

	// get shared reader from entry (created once, reused by all streams)
	readerAt, _, err := ufsEntry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	length := rangeEnd - rangeStart + 1

	// Check context before starting
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Prefetch only a bounded read-ahead window from the requested start,
	// NOT the entire range. Queuing a whole multi-GB file would flood the
	// fixed-depth prefetch channel with head segments and starve reads that
	// land elsewhere (e.g. ffprobe seeking to the moov atom at EOF). The
	// per-read sliding window in readAtPlain advances this as playback
	// progresses; PreCache separately warms the head and tail.
	prefetchLen := length
	if u.prefetchSize > 0 && prefetchLen > u.prefetchSize {
		prefetchLen = u.prefetchSize
	}
	section := newContextSectionReader(ctx, readerAt, rangeStart, length)
	readerAt.Prefetch(section.ctx, rangeStart, prefetchLen)
	buf := acquireStreamBuffer()
	defer releaseStreamBuffer(buf)

	// Use a safe copy loop that checks context and validates read counts
	written, err := safeCopyBuffer(ctx, writer, section, buf)

	// Handle context cancellation explicitly
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}

	// Mark file as failed if article not found (permanent error)
	if err != nil && nntp.IsArticleNotFoundError(err) {
		if recordFailure {
			u.recordFailedFile(key, failedReadOffset(rangeStart, rangeEnd, written), err)
		}
		// Wrap error to mark as permanent
		return customerror.NewArticleNotFoundError(err)
	}

	return err
}

// safeCopyBuffer copies from src to dst using buf, with context checking and
// validation of read counts to prevent panics from corrupted readers during shutdown.
func safeCopyBuffer(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	var release func()
	if len(buf) == 0 {
		buf = acquireStreamBuffer()
		release = func() { releaseStreamBuffer(buf) }
	}
	if release != nil {
		defer release()
	}
	bufLen := len(buf)

	for {
		// Check context before each read
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, er := src.Read(buf)

		// Validate read count - this catches corrupted readers during shutdown
		if nr < 0 {
			return written, fmt.Errorf("reader returned negative count: %d", nr)
		}
		if nr > bufLen {
			// Reader returned more bytes than buffer capacity - this would panic
			// Return error instead of panicking
			return written, fmt.Errorf("reader returned invalid count %d (buffer size %d)", nr, bufLen)
		}

		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nw > nr {
				nw = 0
				if ew == nil {
					ew = fmt.Errorf("invalid write count: %d", nw)
				}
			}
			written += int64(nw)
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

// Touch validates that the first segment of a file is available via NNTP STAT
func (u *Usenet) Touch(ctx context.Context, nzoID, filename string) error {
	file, err := u.getFile(nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get file: %w", err)
	}

	if err := u.preStreamChecks(file); err != nil {
		return err
	}

	// Check if we have Segments
	if len(file.Segments) == 0 {
		return fmt.Errorf("file has no Segments: %s", filename)
	}

	// get first segment
	firstSeg := file.Segments[0]
	// Run STAT command to check if article exists
	_, _, err = u.nntp.Stat(ctx, firstSeg.MessageID)
	if err != nil {
		return fmt.Errorf("segment not available: %w", err)
	}
	return nil
}

// PreCache creates a file system entry and pre-fetches head and tail segments.
// This warms up the cache to reduce latency for subsequent reads (e.g. ffprobe).
// Uses the shared entry/reader so the cache is available for Stream calls.
func (u *Usenet) PreCache(ctx context.Context, nzoID, filename string) error {
	// Use shared entry (same as Stream)
	entry, key, err := u.getOrCreateEntry(ctx, nzoID, filename)
	if err != nil {
		return fmt.Errorf("failed to get or create entry: %w", err)
	}
	defer u.releaseFS(key)

	if len(entry.volumes) == 0 {
		return fmt.Errorf("no volumes available for file %s", filename)
	}

	fileSize := entry.volumes[0].Size

	// Calculate how much to read for head and tail
	headSize := int64(2 * 1024 * 1024) // 2MB head (~3 segments)
	tailSize := int64(2 * 1024 * 1024) // 2MB tail (~3 segments)

	if headSize > fileSize {
		headSize = fileSize
	}

	// get shared reader from entry
	readerAt, _, err := entry.getOrCreateReader()
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	// Pre-fetch head segments using Prefetch (non-blocking segment download)
	readerAt.Prefetch(ctx, 0, headSize)

	// Pre-fetch tail segments (if file is large enough)
	if fileSize > headSize+tailSize {
		tailOffset := fileSize - tailSize
		readerAt.Prefetch(ctx, tailOffset, tailSize)
	}

	return nil
}

// Stats returns nntp statistics
func (u *Usenet) Stats() map[string]any {
	stats := u.nntp.Stats()
	stats["readers"] = u.fs.Size()
	stats["nzb_storage"] = u.nzbStorage.Stats()
	if u.bufferPools != nil {
		stats["buffers"] = u.bufferPools.Stats()
	}
	return stats
}

// GetNZB returns NZB metadata by ID
func (u *Usenet) GetNZB(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZB(id)
}

// GetNZBHeader returns NZB metadata without its segment map. Use this when only
// scalar fields or the file list are needed (status, path, sizes); it avoids
// decoding/allocating the multi-megabyte segment data.
func (u *Usenet) GetNZBHeader(id string) (*storage.NZB, error) {
	return u.nzbStorage.GetNZBHeader(id)
}

// ForEachNZB iterates over all NZBs
func (u *Usenet) ForEachNZB(fn func(*storage.NZB) error) error {
	return u.nzbStorage.ForEachNZB(fn)
}

// NZBStorage returns the underlying NZB storage
func (u *Usenet) NZBStorage() *NZBStorage {
	return u.nzbStorage
}

// SpeedTest runs a speed test for a specific NNTP provider
// It finds a segment from a processed NZB to download for real speed measurement
func (u *Usenet) SpeedTest(ctx context.Context, providerHost string) nntp.SpeedTestResult {
	// Try to find a segment from any processed NZB for the speed test
	messageID := u.findTestSegment()
	return u.nntp.SpeedTest(ctx, providerHost, messageID)
}

// findTestSegment looks for a segment from any processed NZB to use for speed testing
func (u *Usenet) findTestSegment() string {
	var messageID string

	// Iterate through NZBs to find a usable segment
	_ = u.nzbStorage.ForEachNZB(func(nzb *storage.NZB) error {
		for _, file := range nzb.Files {
			if file.IsDeleted || len(file.Segments) == 0 {
				continue
			}
			// Use the first segment we find
			messageID = file.Segments[0].MessageID
			// Return an error to stop iteration (not a real error)
			return fmt.Errorf("found")
		}
		return nil
	})

	return messageID
}

// GetSpeedTestResults returns all stored speed test results
func (u *Usenet) GetSpeedTestResults() map[string]nntp.SpeedTestResult {
	return u.nntp.GetSpeedTestResults()
}

func (u *Usenet) saveNZBFile(id string, content []byte) (string, error) {
	// Store the raw source keyed by the bounded NZB ID rather than the
	// (untrusted, arbitrarily long) display name. ext4 caps a path component at
	// 255 bytes; a long release name plus a ".processing"/".importing"/".queued"
	// marker suffix blew past that limit, which failed the rename, wedged the
	// refresh watcher, and left truncated fragment files behind. The UUID keeps
	// every derived name comfortably under the cap.
	path, err := metadataFilePath(u.metadataDir, id, nzbSourceSuffix)
	if err != nil {
		return "", err
	}
	if err := writeMetadataFile(u.metadataDir, path, content, 0644); err != nil {
		return "", fmt.Errorf("failed to save NZB file to disk: %w", err)
	}
	return path, nil
}

// StageNZB persists a queued NZB before an active-download worker starts.
func (u *Usenet) StageNZB(id string, content []byte) (string, error) {
	return stageNZBAt(u.metadataDir, id, content)
}

// ReadNZBSource reads the exact ID-bound .nzb source recorded in metadata.
func (u *Usenet) ReadNZBSource(id, persistedPath string) ([]byte, error) {
	path, err := validatePersistedMetadataPath(u.metadataDir, id, persistedPath, nzbSourceSuffix)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("NZB source path is empty")
	}
	return readMetadataFile(u.metadataDir, path)
}

// ReadStagedNZB reads the exact ID-bound .queued source held by the active queue.
func (u *Usenet) ReadStagedNZB(id, persistedPath string) ([]byte, error) {
	const maxInt64 = int64(^uint64(0) >> 1)
	return ReadStagedNZBAt(u.metadataDir, id, persistedPath, maxInt64-1)
}

// RemoveStagedNZB removes an exact ID-bound queued source after parsing.
func (u *Usenet) RemoveStagedNZB(id, persistedPath string) error {
	return RemoveStagedNZBAt(u.metadataDir, id, persistedPath)
}

// ValidateStagedNZBAt verifies that persistedPath is either empty or the exact
// canonical <id>.queued source directly beneath metadataRoot.
func ValidateStagedNZBAt(metadataRoot, id, persistedPath string) error {
	_, err := validatePersistedMetadataPath(metadataRoot, id, persistedPath, nzbStagedSuffix)
	return err
}

// RemoveStagedNZBAt removes only the canonical <id>.queued source directly
// beneath metadataRoot. It is exported for queue cleanup paths that do not own
// a Usenet instance but must enforce the same ID binding.
func RemoveStagedNZBAt(metadataRoot, id, persistedPath string) error {
	if persistedPath == "" {
		return nil
	}
	// Hold the leaf lock across the ID-bound persisted-path validation and the
	// removal itself. On Windows, a concurrent root-relative removal can make
	// even the validation Lstat fail with a sharing violation.
	unlock := lockMetadataRemoval(persistedPath)
	defer unlock()
	path, err := validatePersistedMetadataPath(metadataRoot, id, persistedPath, nzbStagedSuffix)
	if err != nil {
		return err
	}
	return removeMetadataFileLocked(metadataRoot, path)
}

func (u *Usenet) markAsProcessing(nzb *storage.NZB) error {
	if nzb == nil {
		return fmt.Errorf("NZB is nil")
	}
	if _, err := validatePersistedMetadataPath(u.metadataDir, nzb.ID, nzb.Path, nzbSourceSuffix); err != nil {
		return err
	}
	markerPath, err := metadataFilePath(u.metadataDir, nzb.ID, nzbProcessingSuffix)
	if err != nil {
		return err
	}
	if err := writeMetadataFile(u.metadataDir, markerPath, []byte(nzb.ID), 0644); err != nil {
		return fmt.Errorf("failed to create processing marker: %w", err)
	}
	return nil
}

func (u *Usenet) markAsCompleted(nzb *storage.NZB) error {
	if nzb == nil {
		return fmt.Errorf("NZB is nil")
	}
	sourcePath, err := validatePersistedMetadataPath(u.metadataDir, nzb.ID, nzb.Path, nzbSourceSuffix)
	if err != nil {
		return err
	}
	nzb.Status = NZBStatusCompleted

	// Persist completion before deleting its source. If cleanup fails or the
	// process crashes, restart logic sees a completed record with an exact path
	// that Delete can safely retry instead of a parsing record with no source.
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("failed to save completed NZB to storage: %w", err)
	}
	if sourcePath == "" {
		return nil
	}
	processingPath, err := metadataFilePath(u.metadataDir, nzb.ID, nzbProcessingSuffix)
	if err != nil {
		return err
	}
	var cleanupErr error
	if err := removeMetadataFileIfExists(u.metadataDir, sourcePath); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove completed NZB source: %w", err))
	}
	if err := removeMetadataFileIfExists(u.metadataDir, processingPath); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove completed NZB processing marker: %w", err))
	}
	if cleanupErr != nil {
		return cleanupErr
	}
	nzb.Path = ""
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("clear completed NZB source path: %w", err)
	}
	return nil
}

func (u *Usenet) markAsFailed(nzb *storage.NZB, cause error) error {
	if nzb == nil {
		return fmt.Errorf("NZB is nil")
	}
	if cause == nil {
		return fmt.Errorf("NZB failure cause is nil")
	}
	sourcePath, err := validatePersistedMetadataPath(u.metadataDir, nzb.ID, nzb.Path, nzbSourceSuffix)
	if err != nil {
		return err
	}
	// Mark as failed in storage
	nzb.Status = NZBStatusFailed
	nzb.FailMessage = cause.Error()
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		return fmt.Errorf("failed to mark NZB as failed in storage: %w", err)
	}
	if sourcePath == "" {
		return nil
	}
	processingPath, err := metadataFilePath(u.metadataDir, nzb.ID, nzbProcessingSuffix)
	if err != nil {
		return err
	}
	return errors.Join(
		removeMetadataFileIfExists(u.metadataDir, processingPath),
		removeMetadataFileIfExists(u.metadataDir, sourcePath),
	)
}

func (u *Usenet) Delete(nzoID string) error {
	nzb, err := u.nzbStorage.GetNZBHeader(nzoID)
	if err != nil {
		if IsNZBNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get NZB: %w", err)
	}

	sourcePath, err := validatePersistedMetadataPath(u.metadataDir, nzoID, nzb.Path, nzbSourceSuffix)
	if err != nil {
		return fmt.Errorf("refusing unsafe persisted NZB path: %w", err)
	}
	if sourcePath != "" {
		processingPath, pathErr := metadataFilePath(u.metadataDir, nzoID, nzbProcessingSuffix)
		if pathErr != nil {
			return pathErr
		}
		processedPath, pathErr := metadataFilePath(u.metadataDir, nzoID, nzbProcessedSuffix)
		if pathErr != nil {
			return pathErr
		}
		failedPath, pathErr := metadataFilePath(u.metadataDir, nzoID, nzbFailedSuffix)
		if pathErr != nil {
			return pathErr
		}
		if err := errors.Join(
			removeMetadataFileIfExists(u.metadataDir, sourcePath),
			removeMetadataFileIfExists(u.metadataDir, processingPath),
			removeMetadataFileIfExists(u.metadataDir, processedPath),
			removeMetadataFileIfExists(u.metadataDir, failedPath),
		); err != nil {
			return fmt.Errorf("remove NZB source artifacts: %w", err)
		}
	}

	// Delete from file-based storage
	if err := u.nzbStorage.DeleteNZB(nzoID); err != nil {
		return fmt.Errorf("failed to delete NZB from storage: %w", err)
	}
	return nil
}

// PendingNZB is an unmanaged NZB file claimed by the metadata-directory watcher.
type PendingNZB struct {
	Name          string
	Path          string
	Content       []byte
	ContentDigest [sha256.Size]byte
	Size          int64
	ModTime       time.Time
}

// ClaimNewNZBs moves unmanaged NZB files out of the watched extension and
// returns them for submission to the shared active-download queue.
func (u *Usenet) ClaimNewNZBs() ([]PendingNZB, error) {
	result, err := u.ClaimNewNZBsBounded(DefaultClaimNewNZBLimits())
	return result.Pending, err
}

func (u *Usenet) ClaimNewNZBsBounded(
	limits ClaimNewNZBLimits,
) (ClaimNewNZBResult, error) {
	var result ClaimNewNZBResult
	u.watcherMu.Lock()
	if u.claimScanner == nil {
		scanner, err := NewNZBClaimScanner(u.metadataDir)
		if err != nil {
			u.watcherMu.Unlock()
			return result, err
		}
		u.claimScanner = scanner
	}
	scanner := u.claimScanner
	u.watcherMu.Unlock()

	result, err := scanner.Scan(limits)
	if len(result.Pending) > 0 {
		u.logger.Info().
			Int("count", len(result.Pending)).
			Int("scanned", result.Scanned).
			Int64("bytes", result.BytesRead).
			Msg("Found new NZB files to queue")
	}
	return result, err
}
