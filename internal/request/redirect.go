package request

import (
	"fmt"
	"net/http"
)

const maxRedirects = 10

// NoRefererRedirectPolicy preserves Go's default redirect limit while removing
// the automatically generated Referer header. Provider-generated URLs can put
// API tokens or short-lived signatures in their query strings; forwarding the
// source URL as a referrer would disclose those credentials to the redirect
// destination even when Authorization is absent.
func NoRefererRedirectPolicy(req *http.Request, via []*http.Request) error {
	req.Header.Del("Referer")
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	return nil
}
