package parser

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Tensai75/nzbparser"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/sourcegraph/conc/iter"
)

var ErrMoreRarDataNeeded = fmt.Errorf("rar: need more data")

var (
	// defaultMaxSnippetSize is used for content-type detection via magic bytes.
	// TS sync-byte check at offset 188 is the deepest we go, so 512 bytes is ample.
	defaultMaxSnippetSize = 512
	// metadataOnly requests the yEnc header (name/size/offsets) without any
	// decoded payload — the connection is drained and returned to the pool.
	metadataOnly = 0
)

// NZBParser provides a simplified, robust NZB parser
type NZBParser struct {
	logger        zerolog.Logger
	manager       *nntp.Client // Connection manager for parsing operations
	maxConcurrent int          // Max concurrent connections
}

type fileAnalysisResult struct {
	fileSize     int64 // Total decoded size of the NZB file entry.
	lastFileSize int64 // Total decoded size of the last NZB file entry in the group.
	segmentSize  int64 // Decoded size of a single yEnc part/segment.
}

type contentResult struct {
	file           nzbparser.NzbFile
	fileType       storage.NZBFileType
	actualFilename string
	err            error
	fileSize       int64 // decoded size of the part (from yEnc), if available
	segmentSize    int64 // decoded size of a segment (from yEnc), if available
	partNumber     int64 // yEnc part number, if available
	partBegin      int64 // yEnc begin offset, if available
}

type filePartMeta struct {
	fileSize    int64
	segmentSize int64
	partNumber  int64
	partBegin   int64
}

type FileGroup struct {
	BaseName       string
	ActualFilename string
	Type           storage.NZBFileType
	Files          []nzbparser.NzbFile
	metadata       *fileAnalysisResult
	fileMeta       map[string]filePartMeta
	Groups         map[string]struct{}
}

// NewParser creates a new simplified NZB parser with a connection manager
func NewParser(manager *nntp.Client, maxConcurrent int, logger zerolog.Logger) *NZBParser {
	return &NZBParser{
		logger:        logger,
		manager:       manager,
		maxConcurrent: maxConcurrent,
	}
}

var (
	// RAR file patterns - simplified and more accurate
	rarMainPattern       = regexp.MustCompile(`\.rar$`)
	rarPartPattern       = regexp.MustCompile(`\.r\d{2}$`) // .r00, .r01, etc.
	rarVolumePattern     = regexp.MustCompile(`\.part\d+\.rar$`)
	ignoreExtensions     = []string{".sfv", ".nfo", ".jpg", ".png", ".txt", ".srt", ".idx", ".sub"}
	sevenZMainPattern    = regexp.MustCompile(`\.7z$`)
	sevenZPartPattern    = regexp.MustCompile(`\.7z\.\d{3}$`)
	extWithNumberPattern = regexp.MustCompile(`\.[^ "\.]*\.\d+$`)
	volPar2Pattern       = regexp.MustCompile(`(?i)\.vol\d+\+\d+\.par2?$`)
	partPattern          = regexp.MustCompile(`(?i)\.part\d+\.[^ "\.]*$`)
	regularExtPattern    = regexp.MustCompile(`\.[^ "\.]*$`)
)

func (p *NZBParser) Parse(ctx context.Context, filename string, content []byte) (nzb *storage.NZB, groups map[string]*FileGroup, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("filename", filename).Msg("Panic recovered in Parse")
			err = fmt.Errorf("parse panic: %v", r)
		}
	}()

	// Parse raw XML
	raw, err := nzbparser.Parse(bytes.NewReader(content))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse NZB content: %w", err)
	}

	// Create base NZB structure
	nzb = &storage.NZB{
		Files:    []storage.NZBFile{},
		Status:   "parsed",
		Name:     determineNZBName(filename, raw.Meta),
		Title:    raw.Meta["title"],
		Password: raw.Meta["password"],
	}
	// Group files by base Name and type
	fileGroups, detectionErr := p.groupFiles(ctx, raw.Files)

	if len(fileGroups) == 0 {
		if detectionErr != nil {
			return nil, nil, detectionErr
		}
		return nil, nil, fmt.Errorf("no valid file groups found in NZB")
	}

	// Confirm connectivity using representative segments. Map iteration order is
	// deliberately irrelevant: a missing article in one group must not reject a
	// partially available release when another group can still be reached.
	err = probeFileGroupConnectivity(fileGroups, func(messageID string) error {
		return p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			_, _, statErr := conn.Stat(messageID)
			return statErr
		})
	})
	if err != nil {
		return nil, nil, err
	}

	nzb.ID = uuid.New().String()
	if nzb.Name == "" {
		// Keep every derived download path entry-scoped even when all filename
		// and metadata candidates collapse or are not portable path components.
		nzb.Name = nzb.ID
	}
	return nzb, fileGroups, nil
}

func probeFileGroupConnectivity(
	fileGroups map[string]*FileGroup,
	stat func(messageID string) error,
) error {
	failures := articleProbeFailures{}
	for _, group := range fileGroups {
		if len(group.Files) == 0 || len(group.Files[0].Segments) == 0 {
			continue
		}
		segment := group.Files[0].Segments[0]
		err := stat(segment.Id)
		if err == nil {
			return nil
		}
		if nntp.IsArticleNotFoundError(err) {
			failures.add(err)
			continue
		}
		return fmt.Errorf(
			"failed to stat segment %s <%s>: %w",
			group.ActualFilename,
			segment.Id,
			err,
		)
	}
	if unavailableErr := failures.unavailableError(); unavailableErr != nil {
		return unavailableErr
	}
	return fmt.Errorf("no segments available to stat in NZB")
}

func (p *NZBParser) Process(ctx context.Context, nzb *storage.NZB, groups map[string]*FileGroup) (result *storage.NZB, err error) {
	// Recover from panics to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error().Interface("panic", r).Str("nzb", nzb.Name).Msg("Panic recovered in Process")
			err = fmt.Errorf("process panic: %v", r)
		}
	}()

	// Parse each group (with deferred archive option)
	files, groupErr := p.processFileGroups(ctx, groups, nzb.Password)

	if len(files) == 0 {
		if groupErr != nil {
			return nil, fmt.Errorf("no valid files found in NZB: %w", groupErr)
		}
		return nil, fmt.Errorf("no valid files found in NZB")
	}

	cfg := config.Get()

	// Change file name if there's only one file
	hasOneFile := len(files) == 1
	skippedFiles := 0
	var skippedErr error
	// Calculate total Size
	for _, file := range files {
		if hasOneFile {
			// Only append extension if NZB name doesn't already have the same extension
			fileExt := filepath.Ext(file.Name)
			nzbExt := filepath.Ext(nzb.Name)
			if fileExt != "" && !strings.EqualFold(nzbExt, fileExt) {
				file.Name = nzb.Name + fileExt
			} else {
				file.Name = nzb.Name
			}
		}
		if err := cfg.IsFileAllowed(file.Name, file.Size); err != nil {
			skippedFiles++
			skippedErr = err
			continue
		}
		if err := p.verifyImportFile(ctx, &file, cfg.Usenet.ImportAvailabilitySamplePercent); err != nil {
			return nil, err
		}
		nzb.TotalSize += file.Size
		file.NzbID = nzb.ID
		nzb.Files = append(nzb.Files, file)
	}
	if skippedFiles > 0 {
		p.logger.Info().Err(skippedErr).Int("skipped_files", skippedFiles).Str("nzb", nzb.Name).Msg("Some files were skipped due to size or extension restrictions")
	}
	if len(nzb.Files) == 0 {
		if skippedFiles > 0 {
			return nil, fmt.Errorf("all files were skipped due to size or extension restrictions(error %v)", skippedErr)
		}
		return nil, fmt.Errorf("no valid files found in NZB after processing")
	}
	return nzb, nil
}

func (p *NZBParser) groupFiles(ctx context.Context, files nzbparser.NzbFiles) (map[string]*FileGroup, error) {
	// Assign XML document order as Number for files with uniform Number values.
	// This preserves upload order for obfuscated archives where the subject
	// line doesn't contain file number patterns like [X/Y].
	if len(files) > 1 {
		allSameNumber := true
		firstNum := files[0].Number
		for _, f := range files[1:] {
			if f.Number != firstNum {
				allSameNumber = false
				break
			}
		}
		if allSameNumber {
			for i := range files {
				files[i].Number = i + 1
			}
		}
	}

	var unknownFiles []nzbparser.NzbFile
	var allFiles []contentResult

	for _, file := range files {
		if len(file.Segments) == 0 {
			continue
		}

		fileType := p.detectFileType(file.Filename)
		if fileType == storage.NZBFileTypePar2 {
			// ignore PAR2 files for now
			continue
		}

		if fileType == storage.NZBFileTypeUnknown {
			unknownFiles = append(unknownFiles, file)
		} else {
			allFiles = append(allFiles, contentResult{
				file:           file,
				fileType:       fileType,
				actualFilename: file.Filename,
			})
		}
	}

	detectionFailures := articleProbeFailures{}
	// Obfuscated releases can contain hundreds of files whose subject names do
	// not reveal a type. A complete, connection-reusing STAT pass is much
	// cheaper than attempting a header fetch with provider failover for every
	// missing article. Only use it when no filename-classified content exists;
	// ordinary releases retain the existing fast path.
	if len(allFiles) == 0 && len(unknownFiles) > 0 {
		var availabilityFailures articleProbeFailures
		var availabilityErr error
		unknownFiles, availabilityFailures, availabilityErr =
			p.filterUnavailableUnknownFiles(ctx, unknownFiles)
		if availabilityErr != nil {
			return nil, availabilityErr
		}
		detectionFailures.merge(availabilityFailures)
	}

	unknownResults, contentFailures := p.batchDetectContentTypes(ctx, unknownFiles)
	detectionFailures.merge(contentFailures)

	// Add unknown results
	allFiles = append(allFiles, unknownResults...)

	groups := p.groupProcessedFiles(allFiles)

	// Merge obfuscated RAR groups - when subjects are random strings,
	// each RAR volume gets its own group. This merges them back together.
	groups = p.mergeObfuscatedRarGroups(groups)

	if len(groups) == 0 {
		return groups, detectionFailures.unavailableError()
	}
	return groups, nil
}

func (p *NZBParser) filterUnavailableUnknownFiles(
	ctx context.Context,
	files []nzbparser.NzbFile,
) ([]nzbparser.NzbFile, articleProbeFailures, error) {
	messageIDs := make([]string, 0, len(files))
	candidates := make([]nzbparser.NzbFile, 0, len(files))
	for _, file := range files {
		if len(file.Segments) == 0 {
			continue
		}
		messageIDs = append(messageIDs, file.Segments[0].Id)
		candidates = append(candidates, file)
	}
	if len(messageIDs) == 0 {
		return files, articleProbeFailures{}, nil
	}

	result, err := p.manager.BatchStatAll(ctx, messageIDs)
	if err != nil {
		return nil, articleProbeFailures{}, fmt.Errorf("preflight unknown NZB articles: %w", err)
	}
	if result == nil || len(result.Results) != len(candidates) {
		return nil, articleProbeFailures{}, fmt.Errorf("preflight unknown NZB articles returned incomplete results")
	}

	available := make([]nzbparser.NzbFile, 0, len(candidates))
	failures := articleProbeFailures{}
	for i, stat := range result.Results {
		switch {
		case stat.Available:
			available = append(available, candidates[i])
		case nntp.IsArticleNotFoundError(stat.Error):
			failures.add(stat.Error)
		default:
			// A transient or ambiguous STAT result must not reject the release.
			// Retain the file so the established header-fetch path gets a chance
			// to recover through its normal provider failover.
			available = append(available, candidates[i])
		}
	}
	return available, failures, nil
}

// mergeObfuscatedRarGroups detects and merges RAR FileGroups that likely belong
// to the same multi-volume archive but couldn't be grouped due to obfuscated
// subjects/filenames.
//
// Obfuscation detection: When an NZB has random subjects (e.g., "yXIBWWn7qKVUVpS6")
// instead of descriptive filenames (e.g., "movie.part01.rar"), each RAR volume
// ends up in its own single-file group. This function merges those back together.
func (p *NZBParser) mergeObfuscatedRarGroups(groups map[string]*FileGroup) map[string]*FileGroup {
	// Collect all single-file RAR groups (potential obfuscation victims)
	var singleFileRarGroups []*FileGroup
	var otherGroups []*FileGroup

	for _, group := range groups {
		if group.Type == storage.NZBFileTypeRar && len(group.Files) == 1 {
			singleFileRarGroups = append(singleFileRarGroups, group)
		} else {
			otherGroups = append(otherGroups, group)
		}
	}

	// If we have multiple single-file RAR groups, this is likely obfuscation
	// Merge them into a single group
	if len(singleFileRarGroups) > 1 {
		p.logger.Debug().
			Int("single_file_rar_groups", len(singleFileRarGroups)).
			Msg("Detected potential obfuscated RAR archive, merging groups")

		// Create a merged group using the first group as base
		mergedGroup := &FileGroup{
			BaseName:       singleFileRarGroups[0].BaseName,
			ActualFilename: singleFileRarGroups[0].ActualFilename,
			Type:           storage.NZBFileTypeRar,
			Files:          make([]nzbparser.NzbFile, 0, len(singleFileRarGroups)),
			Groups:         make(map[string]struct{}),
		}

		// Merge all files from single-file RAR groups
		for _, group := range singleFileRarGroups {
			mergedGroup.Files = append(mergedGroup.Files, group.Files...)
			for g := range group.Groups {
				mergedGroup.Groups[g] = struct{}{}
			}
		}

		// Sort merged files by their NZB file Number (index in original NZB)
		// This preserves upload order which typically matches volume order
		// for multi-volume RAR archives uploaded sequentially
		sort.Slice(mergedGroup.Files, func(i, j int) bool {
			// Use the NZB file Number field which represents order in NZB
			return mergedGroup.Files[i].Number < mergedGroup.Files[j].Number
		})

		// Rebuild the groups map with the merged group
		result := make(map[string]*FileGroup)
		result[mergedGroup.BaseName] = mergedGroup
		for _, group := range otherGroups {
			result[group.BaseName] = group
		}

		p.logger.Info().
			Int("merged_files", len(mergedGroup.Files)).
			Str("group_name", mergedGroup.BaseName).
			Msg("Merged obfuscated RAR groups into single group")

		return result
	}

	// No merging needed
	return groups
}

// Batch process unknown files in parallel
func (p *NZBParser) batchDetectContentTypes(
	ctx context.Context,
	unknownFiles []nzbparser.NzbFile,
) ([]contentResult, articleProbeFailures) {
	if len(unknownFiles) == 0 {
		return nil, articleProbeFailures{}
	}

	// Use up to maxConcurrent workers — same budget as the rest of the parser.
	workers := min(len(unknownFiles), p.maxConcurrent)

	mapper := iter.Mapper[nzbparser.NzbFile, contentResult]{
		MaxGoroutines: workers, // limit concurrency
	}

	mapped := mapper.Map(unknownFiles, func(f *nzbparser.NzbFile) contentResult {
		// You can still pass ctx through to your inner function.
		detectedType, actualFilename, err := p.detectFileTypeByContent(ctx, *f)
		if err != nil {
			p.logger.Trace().
				Err(err).
				Str("file", f.Filename).
				Msg("Failed to detect file type by content")
		}

		return contentResult{
			file:           *f,
			fileType:       detectedType,
			actualFilename: actualFilename,
			err:            err,
		}
	})

	processed := make([]contentResult, 0, len(mapped))
	failures := articleProbeFailures{}
	for _, r := range mapped {
		if r.fileType != storage.NZBFileTypeUnknown {
			r.err = nil
			processed = append(processed, r)
		} else {
			if r.err == nil {
				failures.add(errFileTypeUndetected)
			} else {
				failures.add(r.err)
			}
		}
	}
	return processed, failures
}

// Group already processed files (fast)
func (p *NZBParser) groupProcessedFiles(allFiles []contentResult) map[string]*FileGroup {
	groups := make(map[string]*FileGroup)

	for _, item := range allFiles {
		// Skip unwanted files
		if item.fileType == storage.NZBFileTypeIgnore {
			continue
		}

		// If we only got the name from yEnc, try to infer type from it.
		if item.fileType == storage.NZBFileTypeUnknown && item.actualFilename != "" {
			if detected := p.detectFileType(item.actualFilename); detected != storage.NZBFileTypeUnknown {
				item.fileType = detected
			}
		}

		var groupKey string
		if item.actualFilename != "" && item.actualFilename != item.file.Filename {
			groupKey = p.getBaseFilename(item.actualFilename)
		} else {
			groupKey = item.file.Basefilename
		}

		group, exists := groups[groupKey]
		if !exists {
			group = &FileGroup{
				ActualFilename: item.actualFilename,
				BaseName:       groupKey,
				Type:           item.fileType,
				Files:          []nzbparser.NzbFile{},
				fileMeta:       make(map[string]filePartMeta),
				Groups:         make(map[string]struct{}),
			}
			groups[groupKey] = group
		} else if group.Type == storage.NZBFileTypeUnknown && item.fileType != storage.NZBFileTypeUnknown {
			group.Type = item.fileType
		}
		if group.ActualFilename == "" && item.actualFilename != "" {
			group.ActualFilename = item.actualFilename
		}

		// Update the filename only when content detection produced one; a
		// content-detected file whose yEnc header carried no name must keep
		// its subject-derived filename (it may be the only extension source).
		if item.actualFilename != "" {
			item.file.Filename = item.actualFilename
		}

		group.Files = append(group.Files, item.file)
		for _, g := range item.file.Groups {
			group.Groups[g] = struct{}{}
		}

		if item.fileSize > 0 || item.segmentSize > 0 || item.partNumber > 0 || item.partBegin > 0 {
			if group.fileMeta == nil {
				group.fileMeta = make(map[string]filePartMeta)
			}
			metaKey := fileMetaKey(item.file)
			if metaKey != "" {
				meta := group.fileMeta[metaKey]
				if meta.fileSize == 0 && item.fileSize > 0 {
					meta.fileSize = item.fileSize
				}
				if meta.segmentSize == 0 && item.segmentSize > 0 {
					meta.segmentSize = item.segmentSize
				}
				if meta.partNumber == 0 && item.partNumber > 0 {
					meta.partNumber = item.partNumber
				}
				if meta.partBegin == 0 && item.partBegin > 0 {
					meta.partBegin = item.partBegin
				}
				group.fileMeta[metaKey] = meta
			}
		}
	}

	return groups
}

func (p *NZBParser) getBaseFilename(filename string) string {
	if filename == "" {
		return ""
	}

	// First remove any quotes and trim spaces
	cleaned := strings.Trim(filename, `" -`)

	// Check for vol\d+\+\d+\.par2? (PAR2 Volume files)
	if volPar2Pattern.MatchString(cleaned) {
		return volPar2Pattern.ReplaceAllString(cleaned, "")
	}

	// Check for part\d+\.[^ "\.]* (part files like .part01.rar)

	if partPattern.MatchString(cleaned) {
		return partPattern.ReplaceAllString(cleaned, "")
	}

	// Check for [^ "\.]*\.\d+ (extensions with numbers like .7z.001, .r01, etc.)
	if extWithNumberPattern.MatchString(cleaned) {
		return extWithNumberPattern.ReplaceAllString(cleaned, "")
	}

	// Check for regular extensions [^ "\.]*

	if regularExtPattern.MatchString(cleaned) {
		return regularExtPattern.ReplaceAllString(cleaned, "")
	}

	return cleaned
}

// Simplified file type detection
func (p *NZBParser) detectFileType(filename string) storage.NZBFileType {
	lower := strings.ToLower(filename)

	// Check for media first
	if utils.IsMediaFile(lower) {
		return storage.NZBFileTypeMedia
	}

	// Check rar next
	if p.isRarFile(lower) {
		return storage.NZBFileTypeRar
	}

	if strings.HasSuffix(lower, ".par2") {
		return storage.NZBFileTypePar2
	}

	// Check for 7z files
	if sevenZMainPattern.MatchString(lower) || sevenZPartPattern.MatchString(lower) {
		return storage.NZBFileTypeSevenZip
	}

	if strings.HasSuffix(lower, ".zip") || strings.HasSuffix(lower, ".tar") ||
		strings.HasSuffix(lower, ".gz") || strings.HasSuffix(lower, ".bz2") {
		if strings.HasSuffix(lower, ".zip") {
			return storage.NZBFileTypeZip
		}
		return storage.NZBFileTypeUnknown
	}

	// Check for ignored file types
	for _, ext := range ignoreExtensions {
		if strings.HasSuffix(lower, ext) {
			return storage.NZBFileTypeIgnore
		}
	}
	// Default to unknown type
	return storage.NZBFileTypeUnknown
}

// Simplified RAR detection
func (p *NZBParser) isRarFile(filename string) bool {
	return rarMainPattern.MatchString(filename) ||
		rarPartPattern.MatchString(filename) ||
		rarVolumePattern.MatchString(filename)
}

func (p *NZBParser) processFileGroups(ctx context.Context, groups map[string]*FileGroup, password string) ([]storage.NZBFile, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	rarCounts, sevenZCounts, zipCounts, mediaCounts, deferredCounts := 0, 0, 0, 0, 0

	// Convert map into slice of *values*, not pointers
	fileGroups := make([]FileGroup, 0, len(groups))
	for _, g := range groups {
		if len(g.Files) == 0 {
			continue
		}
		fileGroups = append(fileGroups, *g)
	}

	// Use a Mapper with limited concurrency to prevent goroutine explosion
	// when nested with RAR/archive parsers that also use parallel processing
	type groupResult struct {
		files []*storage.NZBFile
		err   error
	}
	mapper := iter.Mapper[FileGroup, groupResult]{
		MaxGoroutines: p.maxConcurrent,
	}

	results := mapper.Map(fileGroups, func(g *FileGroup) groupResult {
		files, err := p.processFileGroup(ctx, g, password)
		if err != nil {
			p.logger.Warn().Err(err).Str("group", g.BaseName).Msg("Failed to process file group")
			return groupResult{err: err}
		}
		return groupResult{files: files}
	})

	// Filter nils
	var files []storage.NZBFile
	var failures []error
	for _, result := range results {
		if result.err != nil {
			failures = append(failures, result.err)
		}
		for _, f := range result.files {
			if f != nil {
				files = append(files, *f)
				// Count types
				switch f.FileType {
				case storage.NZBFileTypeRar:
					rarCounts++
				case storage.NZBFileTypeSevenZip:
					sevenZCounts++
				case storage.NZBFileTypeZip:
					zipCounts++
				case storage.NZBFileTypeMedia:
					mediaCounts++
				}
			}
		}
	}

	// Count deferred archives
	for _, g := range fileGroups {
		switch g.Type {
		case storage.NZBFileTypeRar, storage.NZBFileTypeSevenZip, storage.NZBFileTypeZip:
			deferredCounts++
		}
	}

	return files, errors.Join(failures...)
}

// Simplified individual group processing
func (p *NZBParser) processFileGroup(ctx context.Context, group *FileGroup, password string) ([]*storage.NZBFile, error) {
	if err := p.enrichGroupWithFileInfo(ctx, group); err != nil {
		return nil, err
	}

	switch group.Type {
	case storage.NZBFileTypeMedia:
		return wrapNZBFile(p.processMediaFile(group, password))
	case storage.NZBFileTypeRar:
		rarParser := NewRARParser(p.manager, p.maxConcurrent, p.logger)
		return rarParser.Process(ctx, group, password)
	case storage.NZBFileTypeSevenZip:
		zipParser := NewSevenZParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	case storage.NZBFileTypeZip:
		zipParser := NewZIPParser(p.manager, p.maxConcurrent, p.logger)
		return zipParser.Process(ctx, group, password)
	default:
		return nil, fmt.Errorf("unsupported file type: %v", group.Type)
	}
}

func (p *NZBParser) enrichGroupWithFileInfo(ctx context.Context, group *FileGroup) error {
	return p.analyzeFileGeometry(ctx, group)
}

// Process regular media files
func (p *NZBParser) processMediaFile(group *FileGroup, password string) *storage.NZBFile {
	if len(group.Files) == 0 {
		return nil
	}

	// Sort files for consistent ordering
	sort.Slice(group.Files, func(i, j int) bool {
		return group.Files[i].Number < group.Files[j].Number
	})

	// Determine extension
	ext := determineExtension(group)
	if ext == "" {
		ext = filepath.Ext(group.ActualFilename)
	}
	if ext == "" {
		return nil
	}

	name := group.BaseName + ext

	file := &storage.NZBFile{
		Name:     name,
		Groups:   getGroupsList(group.Groups),
		Segments: []storage.NZBSegment{},
		Password: password,
		FileType: group.Type,
	}

	currentOffset := int64(0)
	for index, nzbFile := range group.Files {
		totalSize, segments := getNZBSegments(index, nzbFile, group)
		if len(segments) == 0 && len(nzbFile.Segments) > 0 {
			// getNZBSegments rejected the sub-file (missing/duplicate segment
			// numbers). Silently dropping it would splice a hole into the
			// middle of the merged stream; fail the whole file instead.
			p.logger.Warn().
				Str("group", group.BaseName).
				Str("file", nzbFile.Filename).
				Msg("Incomplete or inconsistent segment numbering; rejecting media file")
			return nil
		}
		for i := range segments {
			segments[i].StartOffset += currentOffset
			segments[i].EndOffset += currentOffset
		}
		file.Segments = append(file.Segments, segments...)
		currentOffset += totalSize
	}
	file.Size = currentOffset
	return file
}

func (p *NZBParser) detectFileTypeByContent(ctx context.Context, file nzbparser.NzbFile) (storage.NZBFileType, string, error) {
	if len(file.Segments) == 0 {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("no segments in file %s", file.Filename)
	}

	// Download first segment to check file signature
	firstSegment := file.Segments[0]
	var data *nntp.YencMetadata
	// GetHeaderPrefix returns the connection to the pool after draining;
	// only a small snippet is needed for magic-byte / filename detection.
	err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
		d, e := conn.GetHeaderPrefix(firstSegment.Id, defaultMaxSnippetSize)
		data = d
		return e
	})
	if err != nil {
		return storage.NZBFileTypeUnknown, "", fmt.Errorf("failed to fetch segment header for file %s: %w", file.Filename, err)
	}

	if data.Name != "" {
		fileType := p.detectFileType(data.Name)
		if fileType != storage.NZBFileTypeUnknown {
			return fileType, data.Name, nil
		}
	}

	fileType, ext := p.detectFileTypeAndExtensionFromContent(data.Snippet)
	name := data.Name
	if fileType == storage.NZBFileTypeMedia && ext != "" {
		if name == "" {
			name = file.Filename
		}
		// Keep the original obfuscated component; only append a known media
		// extension when neither the yEnc name nor the fallback identifies it.
		if p.detectFileType(name) == storage.NZBFileTypeUnknown {
			name += ext
		}
	}
	return fileType, name, nil
}

func (p *NZBParser) detectFileTypeAndExtensionFromContent(data []byte) (storage.NZBFileType, string) {
	if len(data) == 0 {
		return storage.NZBFileTypeUnknown, ""
	}

	// Check for RAR signatures (both RAR 4.x and 5.x)
	if len(data) >= 7 {
		// RAR 4.x signature
		if bytes.Equal(data[:7], []byte("Rar!\x1A\x07\x00")) {
			return storage.NZBFileTypeRar, ""
		}
	}
	if len(data) >= 8 {
		// RAR 5.x signature
		if bytes.Equal(data[:8], []byte("Rar!\x1A\x07\x01\x00")) {
			return storage.NZBFileTypeRar, ""
		}
	}

	// Check for ZIP signature
	if len(data) >= 4 && bytes.Equal(data[:4], []byte{0x50, 0x4B, 0x03, 0x04}) {
		return storage.NZBFileTypeZip, ""
	}

	// Check for 7z signature
	if len(data) >= 6 && bytes.Equal(data[:6], []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}) {
		return storage.NZBFileTypeSevenZip, ""
	}

	// Check for common media file signatures
	if len(data) >= 4 {
		// Matroska (MKV/WebM)
		if bytes.Equal(data[:4], []byte{0x1A, 0x45, 0xDF, 0xA3}) {
			return storage.NZBFileTypeMedia, ".mkv"
		}

		// MP4/MOV (check for 'ftyp' at offset 4)
		if len(data) >= 8 && bytes.Equal(data[4:8], []byte("ftyp")) {
			// ftyp also identifies image containers (HEIF/AVIF). Preserve the
			// existing type classification, but infer an extension only from a
			// recognized media major brand in a complete fixed-size header.
			if len(data) >= 16 && binary.BigEndian.Uint32(data[:4]) >= 16 {
				switch string(data[8:12]) {
				case "isom", "iso2", "mp41", "mp42", "avc1":
					return storage.NZBFileTypeMedia, ".mp4"
				case "qt  ":
					return storage.NZBFileTypeMedia, ".mov"
				case "M4A ":
					return storage.NZBFileTypeMedia, ".m4a"
				}
			}
			return storage.NZBFileTypeMedia, ""
		}

		// AVI
		if len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) &&
			bytes.Equal(data[8:12], []byte("AVI ")) {
			return storage.NZBFileTypeMedia, ".avi"
		}
	}

	// MPEG checks need more specific patterns
	if len(data) >= 4 {
		// MPEG-1/2 Program Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xBA}) {
			return storage.NZBFileTypeMedia, ".mpg"
		}

		// MPEG-1/2 Video Stream
		if bytes.Equal(data[:4], []byte{0x00, 0x00, 0x01, 0xB3}) {
			return storage.NZBFileTypeMedia, ".mpg"
		}
	}

	// Check for Transport Stream (TS files)
	if len(data) >= 1 && data[0] == 0x47 {
		// Additional validation: TS packets are 188 bytes, so the next
		// sync byte sits at index 188 (requires at least 189 bytes).
		if len(data) > 188 && data[188] == 0x47 {
			return storage.NZBFileTypeMedia, ".ts"
		}
	}

	return storage.NZBFileTypeUnknown, ""
}
