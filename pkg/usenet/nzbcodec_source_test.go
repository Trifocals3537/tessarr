package usenet

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func sourceCodecFixture() *storage.NZB {
	return &storage.NZB{ID: "fixture", Name: "fixture", Files: []storage.NZBFile{{Name: "video.mkv", Size: 8, Segments: []storage.NZBSegment{
		{Number: 0, MessageID: "a@fixture.invalid", Bytes: 4, EndOffset: 3, Group: "alt.test", SegmentDataStart: 2, Source: &storage.NZBArticleGeometry{Size: 12, Bytes: 6, Part: 1, Total: 2}},
		{Number: 1, MessageID: "b@fixture.invalid", Bytes: 4, StartOffset: 4, EndOffset: 7, Group: "alt.test", Source: &storage.NZBArticleGeometry{Size: 12, Offset: 6, Bytes: 6, Part: 2, Total: 2}},
	}}}}
}

func TestNZBSourceGeometryRoundTripAndLegacyCompatibility(t *testing.T) {
	for _, mode := range []string{"complete", "mixed", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			nzb := sourceCodecFixture()
			if mode != "complete" {
				nzb.Files[0].Segments[1].Source = nil
			}
			if mode == "legacy" {
				nzb.Files[0].Segments[0].Source = nil
			}
			data, err := encodeNZBV2(nzb)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeNZBV2(data)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.Files[0].Segments, nzb.Files[0].Segments) {
				t.Fatalf("geometry changed across reload: %+v", got.Files[0].Segments)
			}
			header, err := decodeNZBV2Header(data)
			if err != nil || len(header.Files[0].Segments) != 0 {
				t.Fatalf("header-only read: %v", err)
			}
			ids, count, err := decodeFileMessageIDsSampled(data, "video.mkv", 100)
			if err != nil || count != 2 || !reflect.DeepEqual(ids, []string{"a@fixture.invalid", "b@fixture.invalid"}) {
				t.Fatalf("ID scan changed: %v %d %v", ids, count, err)
			}
			// Pre-extension readers consume exactly this numeric prefix. New
			// provenance does not move legacy columns or the message-ID region.
			withSource, idsBefore := encodeSegments(nzb)
			for i := range nzb.Files[0].Segments {
				nzb.Files[0].Segments[i].Source = nil
			}
			legacy, idsAfter := encodeSegments(nzb)
			if !bytes.HasPrefix(withSource, legacy) || !bytes.Equal(idsBefore, idsAfter) {
				t.Fatal("legacy layout changed")
			}
		})
	}
}

func TestNZBSourceGeometryRejectsDamagedExtension(t *testing.T) {
	nzb := sourceCodecFixture()
	w := &byteWriter{}
	encodeSourceGeometry(w, nzb)
	for end := 1; end < len(w.buf); end++ {
		if err := decodeSourceGeometry(&byteReader{buf: w.buf[:end]}, make([]storage.NZBSegment, 2)); err == nil {
			t.Fatalf("truncated extension accepted at %d", end)
		}
	}
	for _, bad := range [][]byte{{0}, append(append([]byte(nil), w.buf...), 0)} {
		if err := decodeSourceGeometry(&byteReader{buf: bad}, make([]storage.NZBSegment, 2)); err == nil {
			t.Fatal("invalid extension accepted")
		}
	}
	nzb.Files[0].Segments[0].Source.Offset = -1
	if _, err := encodeNZBV2(nzb); err == nil {
		t.Fatal("invalid geometry encoded")
	}
}

func TestNZBSourceGeometryRejectsMalformedRecords(t *testing.T) {
	for _, tc := range []struct {
		name     string
		count    uint64
		indices  []uint64
		geometry storage.NZBArticleGeometry
	}{
		{"zero count", 0, nil, storage.NZBArticleGeometry{}},
		{"excessive count", 3, nil, storage.NZBArticleGeometry{}},
		{"out of range index", 1, []uint64{2}, storage.NZBArticleGeometry{Size: 12, Bytes: 6, Part: 1, Total: 2}},
		{"duplicate index", 2, []uint64{0, 0}, storage.NZBArticleGeometry{Size: 12, Bytes: 6, Part: 1, Total: 2}},
		{"descending index", 2, []uint64{1, 0}, storage.NZBArticleGeometry{Size: 12, Bytes: 6, Part: 1, Total: 2}},
		{"negative offset", 1, []uint64{0}, storage.NZBArticleGeometry{Size: 12, Offset: -1, Bytes: 6, Part: 1, Total: 2}},
		{"range beyond file", 1, []uint64{0}, storage.NZBArticleGeometry{Size: 12, Offset: 7, Bytes: 6, Part: 1, Total: 2}},
		{"part beyond count", 1, []uint64{0}, storage.NZBArticleGeometry{Size: 12, Bytes: 6, Part: 3, Total: 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &byteWriter{}
			w.uvarint(sourceGeometryV1)
			w.uvarint(tc.count)
			for _, index := range tc.indices {
				w.uvarint(index)
				g := tc.geometry
				for _, value := range []int64{g.Size, g.Offset, g.Bytes, g.Part, g.Total} {
					w.varint(value)
				}
			}
			if err := decodeSourceGeometry(&byteReader{buf: w.buf}, make([]storage.NZBSegment, 2)); err == nil {
				t.Fatal("malformed geometry accepted")
			}
		})
	}
}
