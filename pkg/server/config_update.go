package server

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
)

const maxConfigRequestBytes = 4 << 20

var legacyPostRequiredFields = []string{"debrids", "mount", "usenet"}

// decodeConfigUpdate gives each HTTP method an unambiguous update contract:
// PUT replaces the editable configuration, PATCH applies RFC 7396 merge-patch
// semantics, and legacy POST is accepted only when it is visibly a complete
// settings-form document. Auth fields remain protected by the caller.
func decodeConfigUpdate(method string, body io.Reader, current *config.Config) (*config.Config, error) {
	data, err := readConfigRequest(body)
	if err != nil {
		return nil, err
	}

	requestObject, err := decodeJSONObject(data)
	if err != nil {
		return nil, err
	}

	var candidate []byte
	switch method {
	case http.MethodPut:
		candidate = data
	case http.MethodPatch:
		currentData, err := stdjson.Marshal(current)
		if err != nil {
			return nil, fmt.Errorf("encode current config: %w", err)
		}
		candidate, err = applyJSONMergePatch(currentData, requestObject)
		if err != nil {
			return nil, err
		}
	case http.MethodPost:
		missing := missingObjectFields(requestObject, legacyPostRequiredFields)
		if len(missing) != 0 {
			return nil, fmt.Errorf(
				"legacy POST must contain a complete configuration; missing %s; use PATCH for partial updates",
				strings.Join(missing, ", "),
			)
		}
		currentData, err := stdjson.Marshal(current)
		if err != nil {
			return nil, fmt.Errorf("encode current config: %w", err)
		}
		candidate, err = applyJSONMergePatch(currentData, requestObject)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported config update method %s", method)
	}

	var updated config.Config
	if err := stdjson.Unmarshal(candidate, &updated); err != nil {
		return nil, fmt.Errorf("decode configuration: %w", err)
	}
	return &updated, nil
}

func readConfigRequest(body io.Reader) ([]byte, error) {
	if body == nil {
		return nil, fmt.Errorf("request body is required")
	}
	data, err := io.ReadAll(io.LimitReader(body, maxConfigRequestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	if len(data) > maxConfigRequestBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxConfigRequestBytes)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("request body is required")
	}
	return data, nil
}

func decodeJSONObject(data []byte) (map[string]stdjson.RawMessage, error) {
	var object map[string]stdjson.RawMessage
	if err := stdjson.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("decode JSON object: %w", err)
	}
	if object == nil {
		return nil, fmt.Errorf("configuration update must be a JSON object")
	}
	return object, nil
}

func missingObjectFields(object map[string]stdjson.RawMessage, required []string) []string {
	missing := make([]string, 0, len(required))
	for _, field := range required {
		if _, ok := object[field]; !ok {
			missing = append(missing, field)
		}
	}
	return missing
}

func applyJSONMergePatch(current []byte, patch map[string]stdjson.RawMessage) ([]byte, error) {
	target, err := decodeJSONObject(current)
	if err != nil {
		return nil, fmt.Errorf("decode current config: %w", err)
	}

	if err := mergeJSONObject(target, patch); err != nil {
		return nil, err
	}
	merged, err := stdjson.Marshal(target)
	if err != nil {
		return nil, fmt.Errorf("encode merged config: %w", err)
	}
	return merged, nil
}

func mergeJSONObject(target, patch map[string]stdjson.RawMessage) error {
	for key, patchValue := range patch {
		if bytes.Equal(bytes.TrimSpace(patchValue), []byte("null")) {
			delete(target, key)
			continue
		}

		var patchObject map[string]stdjson.RawMessage
		if err := stdjson.Unmarshal(patchValue, &patchObject); err == nil && patchObject != nil {
			targetObject := make(map[string]stdjson.RawMessage)
			if currentValue, ok := target[key]; ok {
				_ = stdjson.Unmarshal(currentValue, &targetObject)
				if targetObject == nil {
					targetObject = make(map[string]stdjson.RawMessage)
				}
			}
			if err := mergeJSONObject(targetObject, patchObject); err != nil {
				return err
			}
			mergedValue, err := stdjson.Marshal(targetObject)
			if err != nil {
				return fmt.Errorf("encode merged field %q: %w", key, err)
			}
			target[key] = mergedValue
			continue
		}

		target[key] = bytes.Clone(patchValue)
	}
	return nil
}
