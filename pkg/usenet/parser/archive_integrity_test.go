package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/Tensai75/nzbparser"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	streamreader "github.com/Trifocals3537/tessarr/pkg/usenet/fs/reader"
)

func integrityRAR4Block(kind byte, flags uint16, data []byte) []byte {
	block := make([]byte, 7+len(data))
	block[2] = kind
	binary.LittleEndian.PutUint16(block[3:], flags)
	binary.LittleEndian.PutUint16(block[5:], uint16(len(block)))
	copy(block[7:], data)
	binary.LittleEndian.PutUint16(block, uint16(crc32.ChecksumIEEE(block[2:])))
	return block
}

func integrityRAR4Volume(payload []byte, total uint32, splitFlags uint16) []byte {
	const name = "video.mkv"
	body := make([]byte, 25+len(name))
	binary.LittleEndian.PutUint32(body, uint32(len(payload)))
	binary.LittleEndian.PutUint32(body[4:], total)
	binary.LittleEndian.PutUint32(body[9:], crc32.ChecksumIEEE(payload))
	body[17], body[18] = 20, RAR4CompressionMethodStore
	binary.LittleEndian.PutUint16(body[19:], uint16(len(name)))
	binary.LittleEndian.PutUint32(body[21:], 32)
	copy(body[25:], name)
	result := []byte(RAR4Signature)
	result = append(result, integrityRAR4Block(RAR4HeaderTypeArchive, 1, make([]byte, 6))...)
	result = append(result, integrityRAR4Block(RAR4HeaderTypeFile, RAR4HeaderFlagLongBlock|splitFlags, body)...)
	result = append(result, payload...)
	return append(result, integrityRAR4Block(RAR4HeaderTypeEnd, 0, nil)...)
}

func TestRARRejectsIncompleteStoredMember(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    uint16
		truncate bool
	}{
		{"orphan continuation", 1, false},
		{"missing last volume", 2, false},
		{"declared size exceeds available member", 0, false},
		{"truncated volume payload", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			total := uint32(128)
			if tc.truncate {
				total = 64
			}
			volume := integrityRAR4Volume(bytes.Repeat([]byte{0x55}, 64), total, tc.flags)
			if tc.truncate {
				volume = volume[:len(volume)-39]
			}
			p := newContentTestParser(t, map[string][]byte{"<part@fixture.invalid>": encodeContentTestArticle(volume, "release.part02.rar")})
			group := &FileGroup{BaseName: "release", Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{{Filename: "release.part02.rar", Segments: nzbparser.NzbSegments{{Number: 1, Id: "part@fixture.invalid", Bytes: len(volume)}}}}, metadata: &fileAnalysisResult{fileSize: int64(len(volume)), lastFileSize: int64(len(volume)), segmentSize: int64(len(volume))}}
			files, err := NewRARParser(p.manager, 2, p.logger).Process(t.Context(), group, "")
			if err == nil || len(files) != 0 {
				t.Fatalf("incomplete archive accepted: err=%v files=%+v", err, files)
			}
		})
	}
}

func TestRejectNonInitialYEncSegmentMasqueradingAsFirst(t *testing.T) {
	payload := bytes.Repeat([]byte{0x55}, 512)
	article := encodeContentTestArticle(payload, "video.mkv")
	article = bytes.Replace(article, []byte("part=1 line=128 size=512"), []byte("part=318 line=128 size=10000"), 1)
	article = bytes.Replace(article, []byte("begin=1 end=512"), []byte("begin=5001 end=5512"), 1)
	article = bytes.Replace(article, []byte("size=512 part=1 "), []byte("size=512 part=318 "), 1)
	p := newContentTestParser(t, map[string][]byte{"<part@fixture.invalid>": article})
	nzb := fmt.Sprintf(`<nzb><file poster="fixture" date="1" subject="&quot;video.mkv&quot; yEnc (1/1)"><groups><group>alt.test</group></groups><segments><segment number="1" bytes="%d">part@fixture.invalid</segment></segments></file></nzb>`, len(article))
	entry, groups, err := p.Parse(t.Context(), "Release.nzb", []byte(nzb))
	if err == nil {
		_, err = p.Process(t.Context(), entry, groups)
	}
	if err == nil {
		t.Fatal("import accepted a mid-file yEnc fragment as a complete media file")
	}
	if !strings.Contains(err.Error(), "yEnc geometry") {
		t.Fatalf("missing actionable geometry error: %v", err)
	}
}

func integrityYEncPart(payload []byte, total, offset, part, count int) []byte {
	article := encodeContentTestArticle(payload, fmt.Sprintf("random-name-%d", part))
	article = bytes.Replace(article, []byte(fmt.Sprintf("part=1 line=128 size=%d", len(payload))), []byte(fmt.Sprintf("part=%d total=%d line=128 size=%d", part, count, total)), 1)
	article = bytes.Replace(article, []byte(fmt.Sprintf("begin=1 end=%d", len(payload))), []byte(fmt.Sprintf("begin=%d end=%d", offset+1, offset+len(payload))), 1)
	return bytes.Replace(article, []byte(fmt.Sprintf("size=%d part=1 ", len(payload))), []byte(fmt.Sprintf("size=%d part=%d ", len(payload), part)), 1)
}

func integrityRAR5Block(kind, flags uint64, dataSize int, data []byte) []byte {
	body := binary.AppendUvarint(nil, kind)
	body = binary.AppendUvarint(body, flags)
	if flags&RAR5HeaderFlagDataArea != 0 {
		body = binary.AppendUvarint(body, uint64(dataSize))
	}
	body = append(body, data...)
	checked := binary.AppendUvarint(nil, uint64(len(body)))
	checked = append(checked, body...)
	return append(binary.LittleEndian.AppendUint32(nil, crc32.ChecksumIEEE(checked)), checked...)
}

func integrityRAR5Volume(payload []byte, total int, splitFlags uint64) []byte {
	body := []byte{RAR5FileFlagHasCRC32}
	body = binary.AppendUvarint(body, uint64(total))
	body = append(body, 0) // attributes
	body = binary.LittleEndian.AppendUint32(body, crc32.ChecksumIEEE(payload))
	body = append(body, 0, 0, 9) // stored, host OS, name length
	body = append(body, "video.mkv"...)
	volume := append([]byte(RAR5Signature), integrityRAR5Block(RAR5HeaderTypeMain, 0, 0, []byte{1})...)
	volume = append(volume, integrityRAR5Block(RAR5HeaderTypeFile, RAR5HeaderFlagDataArea|splitFlags, len(payload), body)...)
	volume = append(volume, payload...)
	return append(volume, integrityRAR5Block(RAR5HeaderTypeEndOfArc, 0, 0, []byte{0})...)
}

// The NZB counters repeat and both article and volume order are scrambled.
// Volume sizes and article sizes differ. yEnc names are randomized per part.
func TestRARCompleteMultiVolumeRandomReads(t *testing.T) {
	for _, version := range []int{4, 5} {
		t.Run(fmt.Sprintf("RAR%d", version), func(t *testing.T) {
			payload := make([]byte, 10000)
			for i := range payload {
				payload[i] = byte(i*31 + i/256)
			}
			var volumes [][]byte
			if version == 4 {
				volumes = [][]byte{integrityRAR4Volume(payload[:6103], uint32(len(payload)), 2), integrityRAR4Volume(payload[6103:], uint32(len(payload)), 1)}
			} else {
				volumes = [][]byte{integrityRAR5Volume(payload[:6103], len(payload), 16), integrityRAR5Volume(payload[6103:], len(payload), 8)}
			}
			articles := make(map[string][]byte)
			group := &FileGroup{BaseName: "release", Type: storage.NZBFileTypeRar}
			for v, volume := range volumes {
				file := nzbparser.NzbFile{Number: 7, Filename: fmt.Sprintf("release.part%02d.rar", v+1)}
				chunk := 113 + v*38
				count := (len(volume) + chunk - 1) / chunk
				for part, offset := 1, 0; offset < len(volume); part, offset = part+1, offset+chunk {
					id := fmt.Sprintf("v%d-s%d@fixture.invalid", v, part)
					article := integrityYEncPart(volume[offset:min(offset+chunk, len(volume))], len(volume), offset, part, count)
					articles["<"+id+">"] = article
					file.Segments = append(nzbparser.NzbSegments{{Number: part, Id: id, Bytes: len(article)}}, file.Segments...)
				}
				group.Files = append([]nzbparser.NzbFile{file}, group.Files...)
			}
			p := newContentTestParser(t, articles)
			if err := p.enrichGroupWithFileInfo(t.Context(), group); err != nil {
				t.Fatal(err)
			}
			files, err := NewRARParser(p.manager, 2, p.logger).Process(t.Context(), group, "")
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0].Size != int64(len(payload)) {
				t.Fatalf("invalid logical result: %+v", files)
			}
			sr, err := streamreader.NewStreamingReader(t.Context(), p.manager, streamreader.NewSegmentMetaSlice(files[0].Segments), streamreader.WithDiskPath(t.TempDir()), streamreader.WithPrefetchAhead(0), streamreader.WithMaxConnections(2))
			if err != nil {
				t.Fatal(err)
			}
			defer sr.Close()
			for _, offset := range []int64{0, 4931, 6050, 9839} {
				buf := make([]byte, 161)
				n, err := sr.ReadAt(buf, offset)
				if err != nil || n != len(buf) || !bytes.Equal(buf, payload[offset:offset+int64(len(buf))]) {
					t.Fatalf("read at %d: n=%d err=%v mismatch=%v", offset, n, err, !bytes.Equal(buf, payload[offset:offset+int64(len(buf))]))
				}
			}
			if _, err := sr.ReadAt(make([]byte, 1), int64(len(payload))); err != io.EOF {
				t.Fatalf("past EOF: %v", err)
			}
		})
	}
}

func TestRAR5RejectsBrokenChainsAndHeaders(t *testing.T) {
	for _, test := range []struct {
		name    string
		flags   uint64
		corrupt bool
	}{{"orphan", 8, false}, {"missing end", 16, false}, {"size mismatch", 0, false}, {"header CRC", 0, true}} {
		t.Run(test.name, func(t *testing.T) {
			volume := integrityRAR5Volume(bytes.Repeat([]byte{0x55}, 64), 128, test.flags)
			if test.corrupt {
				volume[8] ^= 1
			}
			p := newContentTestParser(t, map[string][]byte{"<part@fixture.invalid>": encodeContentTestArticle(volume, "release.rar")})
			group := &FileGroup{BaseName: "release", Type: storage.NZBFileTypeRar, Files: []nzbparser.NzbFile{{Filename: "release.rar", Segments: nzbparser.NzbSegments{{Number: 1, Id: "part@fixture.invalid", Bytes: len(volume)}}}}}
			if err := p.enrichGroupWithFileInfo(t.Context(), group); err != nil {
				t.Fatal(err)
			}
			if files, err := NewRARParser(p.manager, 2, p.logger).Process(t.Context(), group, ""); err == nil || len(files) != 0 {
				t.Fatalf("invalid archive accepted: err=%v", err)
			}
		})
	}
}

func TestRARDoesNotDiscardVolumeWithMissingTail(t *testing.T) {
	articles := make(map[string][]byte)
	group := &FileGroup{BaseName: "release", Type: storage.NZBFileTypeRar}
	for i, flags := range []uint16{2, 1} {
		volume := integrityRAR4Volume(bytes.Repeat([]byte{0x55}, 128), 256, flags)
		chunk := len(volume) / 2
		file := nzbparser.NzbFile{Filename: fmt.Sprintf("release.part%02d.rar", i+1)}
		for part, offset := 1, 0; offset < len(volume); part, offset = part+1, offset+chunk {
			id := fmt.Sprintf("v%d-p%d@fixture.invalid", i, part)
			if i != 0 || offset+chunk < len(volume) {
				articles["<"+id+">"] = integrityYEncPart(volume[offset:min(offset+chunk, len(volume))], len(volume), offset, part, (len(volume)+chunk-1)/chunk)
			}
			file.Segments = append(file.Segments, nzbparser.NzbSegment{Number: part, Id: id, Bytes: chunk})
		}
		group.Files = append(group.Files, file)
		group.metadata = &fileAnalysisResult{fileSize: int64(len(volume)), lastFileSize: int64(len(volume)), segmentSize: int64(chunk)}
	}
	p := newContentTestParser(t, articles)
	files, err := NewRARParser(p.manager, 2, p.logger).Process(t.Context(), group, "")
	if err == nil || len(files) != 0 {
		t.Fatalf("missing volume was silently discarded: %v", err)
	}
}

func TestYEncGeometryRejectsMalformedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*FileGroup, map[string][]byte)
	}{
		{"duplicate numbering", func(g *FileGroup, _ map[string][]byte) { g.Files[0].Segments[1].Number = 1 }},
		{"numbering gap", func(g *FileGroup, _ map[string][]byte) { g.Files[0].Segments[1].Number = 3 }},
		{"duplicate article", func(g *FileGroup, _ map[string][]byte) { g.Files[0].Segments[1].Id = g.Files[0].Segments[0].Id }},
		{"wrong tail offset", func(_ *FileGroup, a map[string][]byte) {
			a["<b@fixture.invalid>"] = integrityYEncPart(make([]byte, 16), 32, 15, 2, 2)
		}},
		{"wrong tail size", func(_ *FileGroup, a map[string][]byte) {
			a["<b@fixture.invalid>"] = integrityYEncPart(make([]byte, 16), 33, 16, 2, 2)
		}},
		{"missing tail", func(_ *FileGroup, a map[string][]byte) { delete(a, "<b@fixture.invalid>") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := &FileGroup{BaseName: "video", Type: storage.NZBFileTypeMedia, Files: []nzbparser.NzbFile{{Filename: "video.mkv", Segments: nzbparser.NzbSegments{{Number: 1, Id: "a@fixture.invalid", Bytes: 30}, {Number: 2, Id: "b@fixture.invalid", Bytes: 30}}}}}
			articles := map[string][]byte{"<a@fixture.invalid>": integrityYEncPart(make([]byte, 16), 32, 0, 1, 2), "<b@fixture.invalid>": integrityYEncPart(make([]byte, 16), 32, 16, 2, 2)}
			tc.change(group, articles)
			p := newContentTestParser(t, articles)
			if err := p.enrichGroupWithFileInfo(t.Context(), group); err == nil {
				t.Fatal("malformed geometry accepted")
			}
		})
	}
}

func TestProcessRejectsUnavailableMiddleArticle(t *testing.T) {
	articles := map[string][]byte{"<a@fixture.invalid>": integrityYEncPart(make([]byte, 64), 192, 0, 1, 3), "<c@fixture.invalid>": integrityYEncPart(make([]byte, 64), 192, 128, 3, 3)}
	p := newContentTestParser(t, articles)
	nzb := []byte(`<nzb><file poster="fixture" date="1" subject="&quot;video.mkv&quot; yEnc (1/3)"><groups><group>alt.test</group></groups><segments><segment number="1" bytes="100">a@fixture.invalid</segment><segment number="2" bytes="100">b@fixture.invalid</segment><segment number="3" bytes="100">c@fixture.invalid</segment></segments></file></nzb>`)
	entry, groups, err := p.Parse(t.Context(), "Release.nzb", nzb)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := p.Process(t.Context(), entry, groups); err == nil || result != nil {
		t.Fatalf("unreadable sampled article accepted: err=%v", err)
	}
}

func TestImportPreservesOnlyRequiredEncryptionPadding(t *testing.T) {
	for _, tc := range []struct {
		name             string
		size             int64
		encrypted, valid bool
	}{{"plain exact", 64, false, true}, {"plain extra bytes", 63, false, false}, {"cipher padding", 63, true, true}, {"cipher extra block", 48, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p := newContentTestParser(t, map[string][]byte{"<part@fixture.invalid>": encodeContentTestArticle(make([]byte, 64), "video.mkv")})
			file := &storage.NZBFile{Name: "video.mkv", Size: tc.size, IsEncrypted: tc.encrypted, Segments: []storage.NZBSegment{{Number: 1, MessageID: "part@fixture.invalid", Bytes: 64, StartOffset: 0, EndOffset: 63}}}
			file.Segments[0].Source = &storage.NZBArticleGeometry{Size: 64, Bytes: 64, Part: 1, Total: 1}
			if err := p.verifyImportFile(t.Context(), file, 1); (err == nil) != tc.valid {
				t.Fatalf("err=%v valid=%v", err, tc.valid)
			}
		})
	}
}
