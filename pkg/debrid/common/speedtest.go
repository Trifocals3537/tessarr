package common

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Trifocals3537/tessarr/internal/request"
)

const speedTestDownloadSize int64 = 1 << 20

// ProbeDownload measures a bounded range from a provider-generated download
// URL. Generated URLs authorize themselves (for example with a signature or a
// query token), so the provider API client's default Authorization header must
// not accompany the request or any redirect it follows.
func ProbeDownload(ctx context.Context, client *request.Client, downloadURL string) (int64, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("creating download speed probe: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", speedTestDownloadSize-1))

	start := time.Now()
	resp, err := client.DoWithoutDefaultHeaders(req, "Authorization")
	if err != nil {
		return 0, time.Since(start), fmt.Errorf("requesting download speed probe: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return 0, time.Since(start), fmt.Errorf("download speed probe returned HTTP %d", resp.StatusCode)
	}

	bytesRead, err := io.Copy(io.Discard, io.LimitReader(resp.Body, speedTestDownloadSize))
	duration := time.Since(start)
	if err != nil {
		return bytesRead, duration, fmt.Errorf("reading download speed probe: %w", err)
	}
	if bytesRead == 0 {
		return 0, duration, fmt.Errorf("download speed probe returned an empty body")
	}

	return bytesRead, duration, nil
}
