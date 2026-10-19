package usenet

import (
	"fmt"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// Optional v2 numeric-region suffix. Existing v2 records end before this tag;
// old readers ignore the suffix, while new readers reject damaged extensions.
// Header-only and sampled-ID reads never decompress this region. No automatic
// rewrite is needed. An older writer will drop provenance if it rewrites a map.
const sourceGeometryV1 = 0x534731

func encodeSourceGeometry(w *byteWriter, nzb *storage.NZB) {
	count := 0
	for _, file := range nzb.Files {
		for _, seg := range file.Segments {
			if seg.Source != nil {
				count++
			}
		}
	}
	if count == 0 {
		return
	}
	w.uvarint(sourceGeometryV1)
	w.uvarint(uint64(count))
	index := 0
	for _, file := range nzb.Files {
		for _, seg := range file.Segments {
			if g := seg.Source; g != nil {
				w.uvarint(uint64(index))
				w.varint(g.Size)
				w.varint(g.Offset)
				w.varint(g.Bytes)
				w.varint(g.Part)
				w.varint(g.Total)
			}
			index++
		}
	}
}

func decodeSourceGeometry(r *byteReader, segments []storage.NZBSegment) error {
	if r.pos == len(r.buf) {
		return nil
	} // Legacy v2: provenance unknown.
	tag, err := r.uvarint()
	if err != nil || tag != sourceGeometryV1 {
		return fmt.Errorf("nzbcodec: invalid source geometry extension")
	}
	count, err := r.uvarint()
	if err != nil || count == 0 || count > uint64(len(segments)) {
		return fmt.Errorf("nzbcodec: invalid source geometry count")
	}
	// One contiguous backing array, not an allocation for every article.
	sources := make([]storage.NZBArticleGeometry, int(count))
	previous := -1
	for i := range sources {
		index, err := r.uvarint()
		if err != nil || index >= uint64(len(segments)) || int(index) <= previous {
			return fmt.Errorf("nzbcodec: invalid source geometry index")
		}
		g := &sources[i]
		for _, field := range []*int64{&g.Size, &g.Offset, &g.Bytes, &g.Part, &g.Total} {
			*field, err = r.varint()
			if err != nil {
				return fmt.Errorf("nzbcodec: truncated source geometry: %w", err)
			}
		}
		if !g.Valid() {
			return fmt.Errorf("nzbcodec: invalid source article geometry")
		}
		segments[index].Source = g
		previous = int(index)
	}
	if r.pos != len(r.buf) {
		return fmt.Errorf("nzbcodec: trailing source geometry data")
	}
	return nil
}
