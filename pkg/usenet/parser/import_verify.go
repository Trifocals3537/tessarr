package parser

import (
	"context"
	"fmt"
	"math"

	"github.com/Trifocals3537/tessarr/internal/crypto"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// verifyImportFile validates the logical map and actual sampled article bodies.
// STAT can succeed when BODY and ARTICLE both fail. Sampling is deliberately
// not a complete-availability guarantee; 100 percent opts into reading all
// articles. Each file is checked sequentially so probes do not fan out beyond
// the parser's existing processing connection budget.
func (p *NZBParser) verifyImportFile(ctx context.Context, file *storage.NZBFile, percent int) error {
	mappedSize, err := storedStreamSize(file.Size, file.IsEncrypted)
	if err != nil {
		return err
	}
	var offset int64
	for _, seg := range file.Segments {
		if seg.MessageID == "" || seg.Bytes <= 0 || seg.StartOffset != offset || offset > mappedSize || seg.Bytes > mappedSize-offset || seg.EndOffset != offset+seg.Bytes-1 || seg.SegmentDataStart < 0 {
			return fmt.Errorf("invalid logical segment map for %q", file.Name)
		}
		if !seg.Source.Valid() || seg.SegmentDataStart > seg.Source.Bytes || seg.Bytes > seg.Source.Bytes-seg.SegmentDataStart {
			return fmt.Errorf("missing or invalid source article geometry for %q", file.Name)
		}
		offset += seg.Bytes
	}
	if file.Size <= 0 || offset != mappedSize {
		return fmt.Errorf("incomplete logical segment map for %q", file.Name)
	}
	count := len(file.Segments)
	samples := min(count, max(3, count*min(100, max(1, percent))/100))
	for i := 0; i < samples; i++ {
		index := 0
		if samples > 1 {
			index = (count - 1) * i / (samples - 1)
		}
		seg := file.Segments[index]
		if err := ctx.Err(); err != nil {
			return err
		}
		err := p.manager.ExecuteWithFailover(ctx, func(conn *nntp.Connection) error {
			meta, err := conn.GetHeaderPrefix(seg.MessageID, 0)
			if err != nil {
				return err
			}
			if meta == nil || meta.PartSize != meta.DecodedSize || !seg.Source.Matches(meta.Size, meta.Begin, meta.End, meta.Part, meta.Total, meta.DecodedSize) {
				return &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "invalid yEnc geometry in import body sample"}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("import body sample %d of %q: %w", index+1, file.Name, err)
		}
	}
	return nil
}

// AES-CBC readers need the final complete cipher block even though the
// advertised logical size excludes padding. No other short/extra data is valid.
func storedStreamSize(size int64, encrypted bool) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative stored file size")
	}
	if encrypted && size%crypto.BlockSize != 0 {
		padding := int64(crypto.BlockSize) - size%crypto.BlockSize
		if size > math.MaxInt64-padding {
			return 0, fmt.Errorf("stored file size overflow")
		}
		size += padding
	}
	return size, nil
}
