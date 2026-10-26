package usenet

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

type metadataFingerprint struct {
	info   os.FileInfo
	digest string
}

func repairAccountsFingerprint(providers []config.UsenetProvider) string {
	h := sha256.New()
	for _, provider := range providers {
		mac := hmac.New(sha256.New, []byte(provider.Password))
		_, _ = fmt.Fprintf(mac, "tessarr/health-nntp-account/v1\x00%s\x00%d\x00%s\x00%t", provider.Host, provider.Port, provider.Username, provider.SSL)
		_, _ = h.Write(mac.Sum(nil))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Hash the compressed metadata once per immutable file generation. Do not
// decode entire segment maps just to resume an availability-only pack probe.
// The bounded cache is checked against inode, size and modification time on
// every use; normal metadata writes atomically replace the file.
func (u *Usenet) FileRepairFingerprint(id, name string) (string, error) {
	accountKey := repairAccountsFingerprint(config.Get().Usenet.Providers)
	u.nzbStorage.mu.RLock()
	defer u.nzbStorage.mu.RUnlock()
	path, err := u.nzbStorage.metaFilePath(id)
	if err != nil {
		return "", err
	}
	file, err := openMetadataFile(u.nzbStorage.metaDir, path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() <= 0 || info.Size() > maxNZBMetadataFileBytes {
		return "", errors.New("invalid metadata size")
	}
	u.repairFingerprintMu.Lock()
	defer u.repairFingerprintMu.Unlock()
	if previous, ok := u.repairFingerprints[id]; ok && os.SameFile(previous.info, info) && previous.info.Size() == info.Size() && previous.info.ModTime().Equal(info.ModTime()) {
		return previous.digest + ":" + name + ":" + accountKey, nil
	}
	h := sha256.New()
	if n, err := io.Copy(h, io.LimitReader(file, maxNZBMetadataFileBytes+1)); err != nil {
		return "", err
	} else if n != info.Size() {
		return "", errors.New("metadata changed while hashing")
	}
	after, err := file.Stat()
	if err != nil {
		return "", err
	}
	if after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return "", errors.New("metadata changed while hashing")
	}
	if u.repairFingerprints == nil || len(u.repairFingerprints) >= 256 {
		u.repairFingerprints = make(map[string]metadataFingerprint)
	}
	digest := hex.EncodeToString(h.Sum(nil))
	u.repairFingerprints[id] = metadataFingerprint{info: info, digest: digest}
	return digest + ":" + name + ":" + accountKey, nil
}

type contextRangeReader interface {
	ReadAtContext(context.Context, []byte, int64) (int, error)
}

// Read in bounded chunks without buffering a failed range in its entirety.
// All reads go through the production reader and its complete-segment, CRC,
// and source-geometry publication guards; STAT cannot substitute for this.
func verifyExactRange(ctx context.Context, reader contextRangeReader, size, offset, length int64) error {
	if offset < 0 || length <= 0 || offset > size || length > size-offset {
		return errors.New("failed byte range does not fit current file")
	}
	buf := make([]byte, min(length, int64(64*1024)))
	for remaining := length; remaining > 0; {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := min(remaining, int64(len(buf)))
		n, err := reader.ReadAtContext(ctx, buf[:want], offset)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if n < 0 || int64(n) > want {
			return errors.New("invalid byte range read count")
		}
		if nntp.IsArticleNotFoundError(err) {
			return customerror.UsenetSegmentMissingError
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if int64(n) != want {
			return io.ErrUnexpectedEOF
		}
		offset += want
		remaining -= want
	}
	return nil
}

// VerifyFileRanges includes a supported media head and each recorded failed
// logical range. Unknown formats still require exact byte recovery.
func (u *Usenet) VerifyFileRanges(ctx context.Context, id, name string, ranges []storage.FailedReadRange) error {
	release, err := u.acquireContentVerifySlot(ctx)
	if err != nil {
		return err
	}
	defer release()
	file, err := u.getFile(id, name)
	if err != nil {
		return err
	}
	if file == nil || file.Size <= 0 {
		return errors.New("invalid current file size")
	}
	entry, err := u.createEntryWithReadLimits(file, 1, 0)
	if err != nil {
		return err
	}
	defer entry.cleanup()
	reader, _, err := entry.getOrCreateReader()
	if err != nil {
		return err
	}
	if _, supported := mediaHeadRuleFor(file.Name); supported {
		head := make([]byte, min(int64(verifyHeadBytes), file.Size))
		n, readErr := reader.ReadAtContext(ctx, head, 0)
		if n < 0 || n > len(head) {
			return errors.New("invalid head read count")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return verificationError(file.Name, head[:n], readErr)
		}
		if n != len(head) {
			return io.ErrUnexpectedEOF
		}
		if err := verificationError(file.Name, head[:n], readErr); err != nil {
			return err
		}
	}
	for _, failed := range ranges {
		if failed.InfoHash != id || failed.FileName != name {
			return errors.New("failed range ownership changed")
		}
		if err := verifyExactRange(ctx, reader, file.Size, failed.Offset, failed.Length); err != nil {
			return fmt.Errorf("failed byte range validation: %w", err)
		}
	}
	return nil
}
