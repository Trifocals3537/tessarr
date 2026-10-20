package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet/parser"
	"github.com/rs/zerolog"
)

func main() {
	logger := diagnosticLogger(os.Stdout)
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// Check arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: test-parser <nzb-file>")
		fmt.Println("Example: test-parser test.nzb")
		fmt.Println("")
		fmt.Println("This tool parses an NZB file and displays:")
		fmt.Println("  - File structure and sizes")
		fmt.Println("  - RAR detection and compression method")
		fmt.Println("  - M0 (stored) validation")
		fmt.Println("  - Segment information")
		os.Exit(1)
	}

	nzbFile := os.Args[1]

	logger.Info().
		Str("file", nzbFile).
		Msg("Reading NZB file")

	// This operator-run diagnostic intentionally accepts any local input path.
	// It is not a service endpoint; see docs/codeql-developer-tools-review.md.
	nzbContent, err := os.ReadFile(nzbFile)
	if err != nil {
		logger.Fatal().
			Err(err).
			Str("file", nzbFile).
			Msg("Failed to read NZB file")
	}

	logger.Info().
		Int("size", len(nzbContent)).
		Msg("NZB file read successfully")

	// Create NNTP client (10 max connections for test)
	if err := config.SetConfigPath("data/"); err != nil {
		fmt.Fprintln(os.Stderr, "invalid parser data path")
		os.Exit(1)
	}
	cfg := config.Get()
	nntpClient, err := nntp.NewClient(cfg)
	if err != nil {
		logger.Fatal().
			Err(err).
			Msg("Failed to create NNTP client")
	}
	defer nntpClient.Close()

	logger.Info().Msg("NNTP client created successfully")

	// Create NZB parser with manager
	p := parser.NewParser(nntpClient, 10, logger)

	logger.Info().Msg("Parsing NZB file...")

	// Parse NZB
	ctx := context.Background()
	nzb, groups, err := p.Parse(ctx, nzbFile, nzbContent)
	if err != nil {
		logger.Fatal().
			Err(err).
			Msg("Failed to parse NZB")
	}
	updatedNZB, err := p.Process(ctx, nzb, groups)
	if err != nil {
		logger.Fatal().
			Err(err).
			Msg("Failed to process NZB")
	}
	nzb = updatedNZB

	logger.Info().
		Str("id", nzb.ID).
		Str("name", nzb.Name).
		Int64("total_size", nzb.TotalSize).
		Int("logical_files", len(nzb.Files)).
		Int("source_files", len(nzb.Files)).
		Msg("NZB parsed successfully")

	printSummary(os.Stdout, nzb)

	logger.Info().Msg("Test completed successfully")
}

func getPasswordStatus(password string) string {
	if password == "" {
		return "None"
	}
	return "Protected (***)"
}

func diagnosticLogger(out io.Writer) zerolog.Logger {
	// Keep untrusted parser/error fields JSON-escaped, including terminal controls.
	return zerolog.New(out).With().Timestamp().Logger()
}

// Quote every untrusted text field so an NZB cannot forge lines or terminal commands.
func printSummary(out io.Writer, nzb *storage.NZB) {
	// Print detailed file summary
	fmt.Fprintln(out, "\n"+strings.Repeat("=", 80))
	fmt.Fprintln(out, "FILE SUMMARY")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintf(out, "NZB ID:     %q\n", nzb.ID)
	fmt.Fprintf(out, "Name:       %q\n", nzb.Name)
	fmt.Fprintf(out, "Total Size: %.2f GB\n", float64(nzb.TotalSize)/(1024*1024*1024))
	fmt.Fprintf(out, "Logical Files: %d\n", len(nzb.Files))
	fmt.Fprintf(out, "Source Parts:  %d\n", len(nzb.Files))
	fmt.Fprintln(out, strings.Repeat("=", 80))

	for i, file := range nzb.Files {
		fmt.Fprintf(out, "\n[%d] %q\n", i+1, file.Name)
		fmt.Fprintf(out, "    Size:         %.2f MB (%d bytes)\n", float64(file.Size)/(1024*1024), file.Size)
		fmt.Fprintf(out, "    Segments:     %d\n", len(file.Segments))

		if file.SegmentSize > 0 {
			fmt.Fprintf(out, "    Segment Size: %.2f KB\n", float64(file.SegmentSize)/1024)
		}

		fmt.Fprintf(out, "      Password:     %s\n", getPasswordStatus(file.Password))
		fmt.Fprintf(out, "      Entry:        %q (%d bytes)\n", file.Name, file.Size)
		if file.InternalPath != "" {
			fmt.Fprintf(out, "      Internal:     %q\n", file.InternalPath)
		}
		if file.IsStored {
			fmt.Fprintf(out, "      Compression:  ✅ Stored (seekable)\n")
		} else {
			fmt.Fprintf(out, "      Compression:  ⚠️  Compressed\n")
		}

		// Groups
		if len(file.Groups) > 0 {
			fmt.Fprintf(out, "    Groups:       %q\n", file.Groups[:min(3, len(file.Groups))])
		}

		// Check for zero-byte segments
		zeroByteCount := 0
		for segIdx, seg := range file.Segments {
			if seg.Bytes <= 0 {
				zeroByteCount++
				if zeroByteCount <= 5 {
					fmt.Fprintf(out, "    ⚠️  ZERO BYTE SEG[%d]: Bytes=%d, StartOffset=%d, EndOffset=%d, DataStart=%d\n",
						segIdx, seg.Bytes, seg.StartOffset, seg.EndOffset, seg.SegmentDataStart)
				}
			}
		}
		if zeroByteCount > 5 {
			fmt.Fprintf(out, "    ⚠️  ... and %d more zero-byte segments\n", zeroByteCount-5)
		}
		if zeroByteCount > 0 {
			fmt.Fprintf(out, "    ⚠️  TOTAL ZERO-BYTE SEGMENTS: %d (this breaks seeking!)\n", zeroByteCount)
		}

		// Show first few and any problematic segment offsets
		if len(file.Segments) > 0 {
			fmt.Fprintf(out, "    First segment: Bytes=%d, StartOff=%d, EndOff=%d, DataStart=%d\n",
				file.Segments[0].Bytes, file.Segments[0].StartOffset, file.Segments[0].EndOffset, file.Segments[0].SegmentDataStart)
		}
		if len(file.Segments) > 3 {
			fmt.Fprintf(out, "    Seg[3]: Bytes=%d, StartOff=%d, EndOff=%d, DataStart=%d\n",
				file.Segments[3].Bytes, file.Segments[3].StartOffset, file.Segments[3].EndOffset, file.Segments[3].SegmentDataStart)
		}

		fmt.Fprintln(out, "    "+strings.Repeat("-", 76))
	}

	// Summary statistics
	fmt.Fprintln(out, "\n"+strings.Repeat("=", 80))
	fmt.Fprintln(out, "SUMMARY")
	fmt.Fprintln(out, strings.Repeat("=", 80))
	fmt.Fprintln(out, strings.Repeat("=", 80))
}
