package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"

	streamreader "github.com/Trifocals3537/tessarr/pkg/usenet/fs/reader"
)

func emptyMemberFixture(version int, name string, payload []byte, unpacked int) []byte {
	if version == 4 {
		body := make([]byte, 25+len(name))
		binary.LittleEndian.PutUint32(body, uint32(len(payload)))
		binary.LittleEndian.PutUint32(body[4:], uint32(unpacked))
		binary.LittleEndian.PutUint32(body[9:], crc32.ChecksumIEEE(payload))
		body[17], body[18] = 20, RAR4CompressionMethodStore
		binary.LittleEndian.PutUint16(body[19:], uint16(len(name)))
		binary.LittleEndian.PutUint32(body[21:], 32)
		copy(body[25:], name)
		return append(integrityRAR4Block(RAR4HeaderTypeFile, RAR4HeaderFlagLongBlock, body), payload...)
	}
	body := []byte{RAR5FileFlagHasCRC32}
	body = binary.AppendUvarint(body, uint64(unpacked))
	body = append(body, 0) // attributes
	body = binary.LittleEndian.AppendUint32(body, crc32.ChecksumIEEE(payload))
	body = append(body, 0, 0) // stored, host OS
	body = binary.AppendUvarint(body, uint64(len(name)))
	body = append(body, name...)
	return append(integrityRAR5Block(RAR5HeaderTypeFile, RAR5HeaderFlagDataArea, len(payload), body), payload...)
}

func TestRARPreservesMediaWithEmptySidecar(t *testing.T) {
	for _, version := range []int{4, 5} {
		for _, mode := range []string{"before media", "after media", "empty only", "missing nonempty payload"} {
			t.Run(fmt.Sprintf("RAR%d/%s", version, mode), func(t *testing.T) {
				payload := bytes.Repeat([]byte{'A'}, 128)
				empty := emptyMemberFixture(version, "empty.txt", nil, 0)
				media := emptyMemberFixture(version, "video.mkv", payload, len(payload))
				var volume, end []byte
				if version == 4 {
					volume = append([]byte(RAR4Signature), integrityRAR4Block(RAR4HeaderTypeArchive, 0, make([]byte, 6))...)
					end = integrityRAR4Block(RAR4HeaderTypeEnd, 0, nil)
				} else {
					volume = append([]byte(RAR5Signature), integrityRAR5Block(RAR5HeaderTypeMain, 0, 0, []byte{0})...)
					end = integrityRAR5Block(RAR5HeaderTypeEndOfArc, 0, 0, []byte{0})
				}
				switch mode {
				case "before media":
					volume = append(append(volume, empty...), media...)
				case "after media":
					volume = append(append(volume, media...), empty...)
				case "empty only":
					volume = append(volume, empty...)
				case "missing nonempty payload":
					volume = append(append(volume, empty...), emptyMemberFixture(version, "video.mkv", nil, 128)...)
				}
				volume = append(volume, end...)
				p := newContentTestParser(t, map[string][]byte{"<rar@fixture.invalid>": encodeContentTestArticle(volume, "release.rar")})
				nzb := []byte(`<nzb><file poster="fixture" date="1" subject="&quot;release.rar&quot; yEnc (1/1)"><groups><group>alt.test</group></groups><segments><segment number="1" bytes="100">rar@fixture.invalid</segment></segments></file></nzb>`)
				entry, groups, err := p.Parse(t.Context(), "Fixture.nzb", nzb)
				if err != nil {
					t.Fatal(err)
				}
				result, err := p.Process(t.Context(), entry, groups)
				if mode == "empty only" || mode == "missing nonempty payload" {
					if err == nil || result != nil {
						t.Fatal("invalid or empty-only archive reported successful import")
					}
					return
				}
				if err != nil {
					t.Fatalf("valid media rejected because of empty sidecar: %v", err)
				}
				if len(result.Files) != 1 || result.Files[0].InternalPath != "video.mkv" || result.Files[0].Name != "Fixture.mkv" || result.Files[0].Size != int64(len(payload)) {
					t.Fatalf("wrong logical media: %+v", result.Files)
				}
				sr, err := streamreader.NewStreamingReader(t.Context(), p.manager, streamreader.NewSegmentMetaSlice(result.Files[0].Segments), streamreader.WithDiskPath(t.TempDir()), streamreader.WithPrefetchAhead(0), streamreader.WithMaxConnections(2))
				if err != nil {
					t.Fatal(err)
				}
				defer sr.Close()
				got := make([]byte, len(payload))
				if n, err := sr.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
					t.Fatalf("media bytes changed: n=%d err=%v", n, err)
				}
			})
		}
	}
}
