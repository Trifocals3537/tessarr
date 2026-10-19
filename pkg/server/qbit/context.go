package qbit

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/go-chi/chi/v5"
)

type contextKey string

const (
	categoryKey contextKey = "category"
	hashesKey   contextKey = "hashes"
	arrKey      contextKey = "arr"
)

func getCategory(ctx context.Context) string {
	if category, ok := ctx.Value(categoryKey).(string); ok {
		return category
	}
	return ""
}

func getHashes(ctx context.Context) []string {
	if hashes, ok := ctx.Value(hashesKey).([]string); ok {
		return hashes
	}
	return nil
}

func getArrFromContext(ctx context.Context) *arr.Arr {
	if a, ok := ctx.Value(arrKey).(*arr.Arr); ok {
		return a
	}
	return nil
}

func decodeAuthHeader(header string) (string, string, error) {
	encodedTokens := strings.Split(header, " ")
	if len(encodedTokens) != 2 {
		return "", "", nil
	}
	encodedToken := encodedTokens[1]

	bytes, err := base64.StdEncoding.DecodeString(encodedToken)
	if err != nil {
		return "", "", err
	}

	bearer := string(bytes)

	colonIndex := strings.LastIndex(bearer, ":")
	if colonIndex < 0 {
		// strings.LastIndex returns -1 when the substring is absent; without
		// this guard `bearer[:colonIndex]` would panic with
		// "slice bounds out of range [:-1]". Triggers on any Authorization
		// header whose decoded base64 payload contains no ':' separator
		// (e.g. an empty payload, or garbage bytes that decode but lack a
		// 'user:pass' shape).
		return "", "", fmt.Errorf("malformed credentials: missing colon separator")
	}
	username := bearer[:colonIndex]
	password := bearer[colonIndex+1:]

	if username == "" || password == "" {
		return username, password, fmt.Errorf("empty username or password")
	}

	return strings.TrimSpace(username), strings.TrimSpace(password), nil
}

func (q *QBit) categoryContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType := strings.ToLower(r.Header.Get("Content-Type"))
		var err error
		if strings.Contains(contentType, "multipart/form-data") {
			err = utils.ParseMultipartFormBounded(
				w,
				r,
				utils.MaxImportRequestBytes,
				utils.MaxMultipartMemoryBytes,
			)
			if r.MultipartForm != nil {
				defer r.MultipartForm.RemoveAll()
			}
			if err == nil &&
				utils.MultipartFormPartCount(r.MultipartForm) > utils.MaxMultipartFormParts {
				http.Error(w, "Request has too many multipart fields", http.StatusRequestEntityTooLarge)
				return
			}
		} else {
			err = utils.ParseFormBounded(w, r, utils.MaxImportRequestBytes)
		}
		if err != nil {
			if utils.IsRequestTooLarge(err) {
				http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "Invalid form request", http.StatusBadRequest)
			return
		}

		category := strings.TrimSpace(r.URL.Query().Get("category"))
		if category == "" {
			category = strings.TrimSpace(r.FormValue("category"))
		}
		ctx := context.WithValue(r.Context(), categoryKey, category)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authContext creates a middleware that extracts the Arr host and token from the Authorization header
// and adds it to the request context.
// This is used to identify the Arr instance for the utils.
// Only a valid host and token will be added to the context/config. The rest are manual
func (q *QBit) authContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		username, password, err := q.getUsernameAndPassword(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		category := getCategory(r.Context())
		a, err := q.authenticate(category, username, password)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (q *QBit) getUsernameAndPassword(r *http.Request) (string, string, error) {
	// Try to get from authorization header
	username, password, err := decodeAuthHeader(r.Header.Get("Authorization"))
	if err == nil && username != "" {
		return username, password, err
	}
	// Try to get from cookie
	sid, err := r.Cookie("sid")
	if err != nil {
		// try SID
		sid, err = r.Cookie("SID")
	}
	if err == nil {
		if q.sessions == nil {
			return "", "", fmt.Errorf("invalid or expired SID")
		}
		username, password, ok := q.sessions.credentials(sid.Value)
		if !ok {
			return "", "", fmt.Errorf("invalid or expired SID")
		}
		return username, password, nil
	}
	return "", "", nil
}

func (q *QBit) authenticate(category, username, password string) (*arr.Arr, error) {
	cfg := config.Get()
	arrStorage := q.manager.Arr()
	if matched := arrStorage.MatchCredentials(category, username, password); matched != nil {
		return matched, nil
	}
	if cfg.UseAuth && !config.VerifyAuth(username, password) {
		return nil, fmt.Errorf("unauthorized: invalid credentials")
	}
	return arrStorage.GetOrCreate(category), nil
}

func hashesContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rawHashes []string
		if pathHashes := chi.URLParam(r, "hashes"); pathHashes != "" {
			rawHashes = append(rawHashes, pathHashes)
		}
		if len(rawHashes) == 0 {
			_ = r.ParseForm()
			rawHashes = append(rawHashes, r.Form["hashes"]...)
		}
		hashes := normalizeHashes(rawHashes)
		ctx := context.WithValue(r.Context(), hashesKey, hashes)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func normalizeHashes(values []string) []string {
	hashes := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		for hash := range strings.SplitSeq(value, "|") {
			hash = strings.TrimSpace(hash)
			if hash == "" {
				continue
			}
			key := strings.ToLower(hash)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			hashes = append(hashes, hash)
		}
	}
	return hashes
}
