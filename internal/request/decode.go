package request

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// DecodeJSON decodes exactly one JSON value without taking ownership of the
// response body. An empty body returns io.EOF and leaves out untouched.
//
// The standard-library decoder owns decoded strings, so retaining a path does
// not retain the full response buffer. Buffer growth is amortized, not driven
// by the size of each network read. The full value is still materialized; this
// is not a response-size limit or an item-at-a-time array decoder.
func DecodeJSON(resp *http.Response, out any) error {
	if resp == nil || resp.Body == nil || out == nil {
		return nil
	}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(out); err != nil {
		return err
	}
	// A token is enough to reject another value without materializing an
	// unwanted trailing array or object.
	_, err := decoder.Token()
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return errors.New("response body contains more than one JSON value")
}
