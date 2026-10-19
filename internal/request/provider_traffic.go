package request

import (
	"net/http"

	"github.com/Trifocals3537/tessarr/internal/providertraffic"
)

// providerTrafficTransport sits below retryablehttp so each physical attempt
// consumes the correct provider budget and can extend shared 429 backoff.
type providerTrafficTransport struct {
	base       http.RoundTripper
	controller *providertraffic.Controller
	identity   providertraffic.Identity
}

func (t *providerTrafficTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	operation := providertraffic.ClassifyRequest(t.identity.ProviderType, req)
	endpoint := providertraffic.EndpointKey(t.identity.ProviderType, req.Method, req.URL)
	if err := t.controller.WaitEndpoint(req.Context(), t.identity, operation, endpoint); err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(req)
	if response != nil {
		t.controller.Observe(
			t.identity,
			operation,
			response.StatusCode,
			response.Header,
		)
	}
	return response, err
}
