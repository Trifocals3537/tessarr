package parser

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	streamreader "github.com/Trifocals3537/tessarr/pkg/usenet/fs/reader"
)

func TestImportRejectsConflictingSampleGeometry(t *testing.T) {
	for _, tc := range []struct {
		name                string
		offset, total, part int
		valid               bool
		zeroBased           bool
	}{
		{"valid control", 64, 192, 2, true, false},
		{"valid zero-based NZB", 64, 192, 2, true, true},
		{"wrong byte offset with correct part number", 0, 192, 2, false, false},
		{"wrong file size with correct part number", 64, 256, 2, false, false},
		{"next part accepted in one-based NZB", 128, 192, 3, false, false},
		{"previous part in zero-based NZB", 64, 192, 1, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := append(bytes.Repeat([]byte{'A'}, 64), bytes.Repeat([]byte{'B'}, 64)...)
			want = append(want, bytes.Repeat([]byte{'C'}, 64)...)
			middle := want[64:128]
			if tc.offset != 64 {
				middle = want[tc.offset : tc.offset+64]
			}
			articles := map[string][]byte{
				"<a@fixture.invalid>": integrityYEncPart(want[:64], 192, 0, 1, 3),
				"<b@fixture.invalid>": integrityYEncPart(middle, tc.total, tc.offset, tc.part, 3),
				"<c@fixture.invalid>": integrityYEncPart(want[128:], 192, 128, 3, 3),
			}
			p := newContentTestParser(t, articles)
			nzb := []byte(`<nzb><file poster="fixture" date="1" subject="&quot;video.mkv&quot; yEnc (1/3)"><groups><group>alt.test</group></groups><segments><segment number="1" bytes="100">a@fixture.invalid</segment><segment number="2" bytes="100">b@fixture.invalid</segment><segment number="3" bytes="100">c@fixture.invalid</segment></segments></file></nzb>`)
			if tc.zeroBased {
				nzb = []byte(strings.NewReplacer(`number="1"`, `number="0"`, `number="2"`, `number="1"`, `number="3"`, `number="2"`).Replace(string(nzb)))
			}
			entry, groups, err := p.Parse(t.Context(), "Fixture.nzb", nzb)
			if err != nil {
				t.Fatal(err)
			}
			result, err := p.Process(t.Context(), entry, groups)
			if tc.valid {
				if err != nil || result == nil {
					t.Fatalf("valid control rejected: %v", err)
				}
			} else if err != nil {
				return
			}
			sr, err := streamreader.NewStreamingReader(t.Context(), p.manager, streamreader.NewSegmentMetaSlice(result.Files[0].Segments), streamreader.WithDiskPath(t.TempDir()), streamreader.WithPrefetchAhead(0), streamreader.WithMaxConnections(2))
			if err != nil {
				t.Fatal(err)
			}
			defer sr.Close()
			got := make([]byte, len(want))
			n, readErr := sr.ReadAt(got, 0)
			if tc.valid {
				if readErr != nil || n != len(want) || !bytes.Equal(got, want) {
					t.Fatalf("valid source read failed: n=%d err=%v", n, readErr)
				}
				return
			}
			t.Fatalf("invalid sampled geometry accepted: logical_size=%d read_bytes=%d read_error=%v payload_matches=%v metadata=%s", result.Files[0].Size, n, readErr, bytes.Equal(want, got), fmt.Sprintf("offset=%d total=%d part=%d", tc.offset, tc.total, tc.part))
		})
	}
}

// Reproduce a conflict outside the import sample, and after metadata reload:
// every fetched body must still pass source checks before becoming readable.
func TestStreamingRejectsConflictingUnsampledGeometry(t *testing.T) {
	articles := map[string][]byte{
		"<a@fixture.invalid>": integrityYEncPart(bytes.Repeat([]byte{'A'}, 64), 192, 0, 1, 3),
		"<b@fixture.invalid>": integrityYEncPart(bytes.Repeat([]byte{'A'}, 64), 192, 0, 2, 3),
		"<c@fixture.invalid>": integrityYEncPart(bytes.Repeat([]byte{'C'}, 64), 192, 128, 3, 3),
	}
	p := newContentTestParser(t, articles)
	group := &FileGroup{BaseName: "video", ActualFilename: "video.mkv", Type: storage.NZBFileTypeMedia, Files: []nzbparser.NzbFile{{Filename: "video.mkv", Segments: nzbparser.NzbSegments{{Number: 1, Id: "a@fixture.invalid"}, {Number: 2, Id: "b@fixture.invalid"}, {Number: 3, Id: "c@fixture.invalid"}}}}}
	if err := p.enrichGroupWithFileInfo(t.Context(), group); err != nil {
		t.Fatal(err)
	}
	file := p.processMediaFile(group, "") // Deliberately do not sample the body.
	encoded, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	var reloaded storage.NZBFile
	if err := json.Unmarshal(encoded, &reloaded); err != nil {
		t.Fatal(err)
	}
	sr, err := streamreader.NewStreamingReader(t.Context(), p.manager, streamreader.NewSegmentMetaSlice(reloaded.Segments), streamreader.WithDiskPath(t.TempDir()), streamreader.WithPrefetchAhead(0), streamreader.WithMaxConnections(2))
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	for attempt := 0; attempt < 2; attempt++ {
		if n, err := sr.ReadAt(make([]byte, 64), 64); err == nil || n != 0 {
			t.Fatalf("conflicting article published to cache: n=%d err=%v", n, err)
		}
	}
}
