package parser

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const contentTestNZB = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="fixture" date="1" subject="&quot;subject-name&quot; [1/1] yEnc (1/1)">
  <groups><group>alt.test</group></groups>
  <segments><segment bytes="1100" number="1">media@fixture.invalid</segment></segments>
 </file>
</nzb>`

func contentTestMP4Header(brand string) []byte {
	header := make([]byte, 16)
	binary.BigEndian.PutUint32(header, uint32(len(header)))
	copy(header[4:], "ftyp")
	copy(header[8:], brand)
	return header
}

func TestObfuscatedMediaExtensionSurvivesParseAndProcess(t *testing.T) {
	ts := make([]byte, 189)
	ts[0], ts[188] = 0x47, 0x47
	for _, test := range []struct {
		format string
		header []byte
		ext    string
	}{
		{format: "matroska", header: []byte{0x1A, 0x45, 0xDF, 0xA3}, ext: ".mkv"},
		{format: "mp4", header: contentTestMP4Header("mp42"), ext: ".mp4"},
		{format: "quicktime", header: contentTestMP4Header("qt  "), ext: ".mov"},
		{format: "m4a", header: contentTestMP4Header("M4A "), ext: ".m4a"},
		{format: "avi", header: []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'A', 'V', 'I', ' '}, ext: ".avi"},
		{format: "mpeg", header: []byte{0, 0, 1, 0xBA}, ext: ".mpg"},
		{format: "transport stream", header: ts, ext: ".ts"},
	} {
		for _, name := range []string{"opaque", "opaque.obf", ""} {
			t.Run(fmt.Sprintf("%s/header-name=%s", test.format, name), func(t *testing.T) {
				payload := make([]byte, 1024)
				copy(payload, test.header)
				p := newContentTestParser(t, map[string][]byte{
					"<media@fixture.invalid>": encodeContentTestArticle(payload, name),
				})
				raw, err := nzbparser.Parse(bytes.NewReader([]byte(contentTestNZB)))
				if err != nil || len(raw.Files) != 1 || len(raw.Files[0].Segments) != 1 {
					t.Fatalf("invalid NZB fixture: %+v, %v", raw, err)
				}
				nzb, groups, err := p.Parse(t.Context(), "Release.nzb", []byte(contentTestNZB))
				if err != nil {
					t.Fatal(err)
				}
				if len(groups) != 1 {
					t.Fatalf("group count = %d, want 1", len(groups))
				}
				result, err := p.Process(t.Context(), nzb, groups)
				if err != nil {
					t.Fatalf("recognized obfuscated media was not importable: %v", err)
				}
				if len(result.Files) != 1 || result.Files[0].Name != "Release"+test.ext || result.TotalSize != 1024 {
					t.Fatalf("processed NZB = %+v, want one Release%s with 1024 bytes", result, test.ext)
				}
				file := result.Files[0]
				if file.FileType != storage.NZBFileTypeMedia || len(file.Segments) != 1 || file.NzbID != result.ID {
					t.Fatalf("logical file identity changed: %+v", file)
				}
				segment := file.Segments[0]
				if segment.MessageID != "media@fixture.invalid" || segment.StartOffset != 0 || segment.EndOffset != 1023 || segment.Bytes != 1024 {
					t.Fatalf("stream segment mapping changed: %+v", segment)
				}
			})
		}
	}
}

func TestContentExtensionInferencePreservesClassification(t *testing.T) {
	p := &NZBParser{}
	ts := make([]byte, 189)
	ts[0], ts[188] = 0x47, 0x47
	for _, test := range []struct {
		name string
		data []byte
		kind storage.NZBFileType
		ext  string
	}{
		{"empty", nil, storage.NZBFileTypeUnknown, ""},
		{"unknown", []byte("not media"), storage.NZBFileTypeUnknown, ""},
		{"rar4", []byte("Rar!\x1a\x07\x00"), storage.NZBFileTypeRar, ""},
		{"rar5", []byte("Rar!\x1a\x07\x01\x00"), storage.NZBFileTypeRar, ""},
		{"zip", []byte("PK\x03\x04"), storage.NZBFileTypeZip, ""},
		{"7z", []byte("7z\xbc\xaf\x27\x1c"), storage.NZBFileTypeSevenZip, ""},
		{"par2", []byte("PAR2\x00PKT"), storage.NZBFileTypeUnknown, ""},
		{"matroska", []byte("\x1a\x45\xdf\xa3"), storage.NZBFileTypeMedia, ".mkv"},
		{"short matroska", []byte("\x1a\x45\xdf"), storage.NZBFileTypeUnknown, ""},
		{"avi", []byte("RIFF\x00\x00\x00\x00AVI "), storage.NZBFileTypeMedia, ".avi"},
		{"wave", []byte("RIFF\x00\x00\x00\x00WAVE"), storage.NZBFileTypeUnknown, ""},
		{"mpeg program", []byte("\x00\x00\x01\xba"), storage.NZBFileTypeMedia, ".mpg"},
		{"mpeg video", []byte("\x00\x00\x01\xb3"), storage.NZBFileTypeMedia, ".mpg"},
		{"transport stream", ts, storage.NZBFileTypeMedia, ".ts"},
		{"short transport stream", ts[:188], storage.NZBFileTypeUnknown, ""},
		{"short ftyp", []byte("\x00\x00\x00\x10fty"), storage.NZBFileTypeUnknown, ""},
		{"missing brand", []byte("\x00\x00\x00\x10ftyp"), storage.NZBFileTypeMedia, ""},
		{"incomplete header", contentTestMP4Header("mp42")[:12], storage.NZBFileTypeMedia, ""},
		{"invalid box size", []byte("\x00\x00\x00\x08ftypmp42\x00\x00\x00\x00"), storage.NZBFileTypeMedia, ""},
		{"extended box size", []byte("\x00\x00\x00\x01ftypmp42\x00\x00\x00\x00"), storage.NZBFileTypeMedia, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind, ext := p.detectFileTypeAndExtensionFromContent(test.data)
			if kind != test.kind || ext != test.ext {
				t.Fatalf("got (%s, %q), want (%s, %q)", kind, ext, test.kind, test.ext)
			}
		})
	}
	for _, test := range []struct{ brand, ext string }{
		{"isom", ".mp4"}, {"iso2", ".mp4"}, {"mp41", ".mp4"}, {"mp42", ".mp4"}, {"avc1", ".mp4"},
		{"qt  ", ".mov"}, {"M4A ", ".m4a"},
		{"avif", ""}, {"avis", ""}, {"heic", ""}, {"mif1", ""}, {"xxxx", ""},
	} {
		t.Run("brand="+test.brand, func(t *testing.T) {
			data := append(contentTestMP4Header(test.brand), []byte("isom")...)
			binary.BigEndian.PutUint32(data, uint32(len(data)))
			kind, ext := p.detectFileTypeAndExtensionFromContent(data)
			if kind != storage.NZBFileTypeMedia || ext != test.ext {
				t.Fatalf("got (%s, %q), want media with %q", kind, ext, test.ext)
			}
		})
	}
}

func TestContentExtensionInferencePreservesKnownNames(t *testing.T) {
	for _, test := range []struct {
		name string
		kind storage.NZBFileType
	}{
		{"original.WEBM", storage.NZBFileTypeMedia},
		{"original.mov", storage.NZBFileTypeMedia},
		{"original.bin", storage.NZBFileTypeMedia},
		{"original.srt", storage.NZBFileTypeIgnore},
		{"original.rar", storage.NZBFileTypeRar},
		{"original.par2", storage.NZBFileTypePar2},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newContentTestParser(t, map[string][]byte{
				"<media@fixture.invalid>": encodeContentTestArticle([]byte("\x1a\x45\xdf\xa3"), test.name),
			})
			kind, name, err := p.detectFileTypeByContent(t.Context(), nzbparser.NzbFile{
				Filename: "opaque", Segments: nzbparser.NzbSegments{{Id: "media@fixture.invalid"}},
			})
			if err != nil || kind != test.kind || name != test.name {
				t.Fatalf("got (%s, %q, %v), want (%s, %q, nil)", kind, name, err, test.kind, test.name)
			}
		})
	}
}

func TestInferredMediaExtensionRespectsImportFilters(t *testing.T) {
	for _, test := range []struct {
		name, release string
		configure     func(*config.Config)
		allowed       bool
	}{
		{"default", "Release.nzb", func(*config.Config) {}, true},
		{"sample blocked", "Release-sample.nzb", func(c *config.Config) { c.AllowSamples = false }, false},
		{"sample allowed", "Release-sample.nzb", func(c *config.Config) { c.AllowSamples = true }, true},
		{"extension blocked", "Release.nzb", func(c *config.Config) { c.AllowedExt = []string{"mp4"} }, false},
		{"size blocked", "Release.nzb", func(c *config.Config) { c.MinFileSize = "1MB" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := make([]byte, 1024)
			copy(payload, "\x1a\x45\xdf\xa3")
			p := newContentTestParser(t, map[string][]byte{
				"<media@fixture.invalid>": encodeContentTestArticle(payload, "opaque"),
			})
			test.configure(config.Get())
			nzb, groups, err := p.Parse(t.Context(), test.release, []byte(contentTestNZB))
			if err != nil {
				t.Fatal(err)
			}
			result, err := p.Process(t.Context(), nzb, groups)
			if test.allowed {
				if err != nil || len(result.Files) != 1 {
					t.Fatalf("allowed file rejected: %+v, %v", result, err)
				}
			} else if err == nil || len(nzb.Files) != 0 {
				t.Fatalf("blocked file was admitted: %+v, %v", nzb, err)
			}
		})
	}
}

func TestInferredMediaExtensionDoesNotBypassSegmentValidation(t *testing.T) {
	for _, test := range []struct {
		name     string
		segments nzbparser.NzbSegments
	}{
		{"gap", nzbparser.NzbSegments{{Number: 1, Id: "first"}, {Number: 3, Id: "third"}}},
		{"duplicate", nzbparser.NzbSegments{{Number: 1, Id: "first"}, {Number: 1, Id: "other"}, {Number: 3, Id: "third"}}},
		{"empty ID", nzbparser.NzbSegments{{Number: 1, Id: ""}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newContentTestParser(t, map[string][]byte{
				"<media@fixture.invalid>": encodeContentTestArticle([]byte("\x1a\x45\xdf\xa3"), "opaque"),
			})
			_, groups, err := p.Parse(t.Context(), "Release.nzb", []byte(contentTestNZB))
			if err != nil {
				t.Fatal(err)
			}
			for _, group := range groups {
				if determineExtension(group) != ".mkv" {
					t.Fatal("fixture did not reach extension inference")
				}
				group.Files[0].Segments = test.segments
				group.Files[0].TotalSegments = 3
				group.metadata = &fileAnalysisResult{fileSize: 3072, lastFileSize: 3072, segmentSize: 1024}
				if file := p.processMediaFile(group, ""); file != nil {
					t.Fatalf("incomplete media file was admitted: %+v", file)
				}
			}
		})
	}
}

func TestContentExtensionInferenceDoesNotAdmitUnknownBrands(t *testing.T) {
	for _, brand := range []string{"avif", "heic", "mif1", "xxxx"} {
		t.Run(brand, func(t *testing.T) {
			p := newContentTestParser(t, map[string][]byte{
				"<media@fixture.invalid>": encodeContentTestArticle(contentTestMP4Header(brand), "opaque"),
			})
			nzb, groups, err := p.Parse(t.Context(), "Release.nzb", []byte(contentTestNZB))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Process(t.Context(), nzb, groups); err == nil {
				t.Fatal("inferred a usable media filename from an unrecognized brand")
			}
		})
	}
}

func TestContentExtensionInferencePreservesUnavailableErrors(t *testing.T) {
	p := newContentTestParser(t, nil)
	_, _, err := p.Parse(t.Context(), "Release.nzb", []byte(contentTestNZB))
	if !errors.Is(err, ErrNZBArticlesUnavailable) || !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("missing article lost its classification: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = p.detectFileTypeByContent(ctx, nzbparser.NzbFile{
		Filename: "opaque", Segments: nzbparser.NzbSegments{{Id: "media@fixture.invalid"}},
	})
	if !errors.Is(err, context.Canceled) || nntp.IsArticleNotFoundError(err) {
		t.Fatalf("cancellation lost its classification: %v", err)
	}
}
