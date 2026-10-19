package config

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

const (
	StrmDeliveryProxy    = "proxy"
	StrmDeliveryRedirect = "redirect"
)

// Strm configures an optional mountless media library. The export contains
// signed .strm files whose URLs resolve stable entry/file identities through
// Tessarr, so provider changes and refreshed CDN links do not invalidate the
// library.
type Strm struct {
	Enabled            bool   `json:"enabled,omitempty"`
	Path               string `json:"path,omitempty"`
	Secret             string `json:"secret,omitempty"`
	DeliveryMode       string `json:"delivery_mode,omitempty"`
	KeepMediaExtension bool   `json:"keep_media_extension,omitempty"`
}

func (s Strm) IsZero() bool {
	return !s.Enabled && s.Path == "" && s.Secret == "" &&
		s.DeliveryMode == "" && !s.KeepMediaExtension
}

func (s Strm) Active() bool {
	return s.Enabled && strings.TrimSpace(s.Path) != ""
}

func (s Strm) Validate(appURL string) error {
	if !s.Enabled {
		return nil
	}
	if strings.TrimSpace(s.Path) == "" {
		return fmt.Errorf("STRM export path is required when STRM is enabled")
	}
	if _, err := safepath.ValidateRoot(s.Path); err != nil {
		return fmt.Errorf("invalid STRM export path: %w", err)
	}
	if s.DeliveryMode != "" &&
		s.DeliveryMode != StrmDeliveryProxy &&
		s.DeliveryMode != StrmDeliveryRedirect {
		return fmt.Errorf("STRM delivery mode must be %q or %q", StrmDeliveryProxy, StrmDeliveryRedirect)
	}
	if s.Secret != "" {
		key, err := hex.DecodeString(s.Secret)
		if err != nil || len(key) != 32 {
			return fmt.Errorf("STRM signing secret must be 64 hexadecimal characters")
		}
	}
	if strings.TrimSpace(appURL) != "" {
		u, err := url.Parse(appURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("application URL must be an absolute HTTP or HTTPS URL for STRM playback")
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("application URL for STRM playback cannot contain credentials, a query, or a fragment")
		}
	}
	return nil
}

func (c *Config) setStrmDefaults(generateSecret bool) error {
	if c.Strm.IsZero() {
		return nil
	}
	if c.Strm.DeliveryMode == "" {
		c.Strm.DeliveryMode = StrmDeliveryProxy
	}
	if c.Strm.Secret == "" && generateSecret {
		secret, err := generateAPIToken()
		if err != nil {
			return fmt.Errorf("generate STRM signing secret: %w", err)
		}
		c.Strm.Secret = secret
	}
	return nil
}
