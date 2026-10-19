package sabnzbd

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"

	"github.com/Trifocals3537/tessarr/pkg/arr"
)

type contextKey string

const (
	apiKeyKey   contextKey = "apikey"
	modeKey     contextKey = "mode"
	arrKey      contextKey = "arr"
	categoryKey contextKey = "category"
)

func getMode(ctx context.Context) string {
	if mode, ok := ctx.Value(modeKey).(string); ok {
		return mode
	}
	return ""
}

func (s *SABnzbd) categoryContext(next http.Handler) http.Handler {
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
				s.writeError(w, "Request has too many multipart fields", http.StatusRequestEntityTooLarge)
				return
			}
		} else {
			err = utils.ParseFormBounded(w, r, utils.MaxImportRequestBytes)
		}
		if err != nil {
			if utils.IsRequestTooLarge(err) {
				s.writeError(w, "Request is too large", http.StatusRequestEntityTooLarge)
				return
			}
			s.writeError(w, "Invalid form request", http.StatusBadRequest)
			return
		}

		category := r.FormValue("category")
		if category == "" {
			category = r.FormValue("cat")
		}

		ctx := context.WithValue(r.Context(), categoryKey, strings.TrimSpace(category))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getArrFromContext(ctx context.Context) *arr.Arr {
	if a, ok := ctx.Value(arrKey).(*arr.Arr); ok {
		return a
	}
	return nil
}

func getCategory(ctx context.Context) string {
	if category, ok := ctx.Value(categoryKey).(string); ok {
		return category
	}
	return ""
}

// modeContext extracts the mode parameter from the request
func (s *SABnzbd) modeContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// categoryContext has already parsed the request through the bounded
		// form helpers, so FormValue can safely support both query parameters
		// and the POST form fields used by SAB-compatible clients.
		mode := r.FormValue("mode")
		category := getCategory(r.Context())

		// Keep the Arr admitted by authContext. Replacing it here used to
		// discard configured provider selection and download policy after
		// authentication had already succeeded.
		a := getArrFromContext(r.Context())
		if a == nil {
			downloadUncached := false
			a = arr.New(category, "", "", false, &downloadUncached, "", "auto")
		}

		ctx := context.WithValue(r.Context(), modeKey, strings.TrimSpace(mode))
		ctx = context.WithValue(ctx, arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authContext creates a middleware that extracts the Arr host and token from the Authorization header
// and adds it to the request context.
// This is used to identify the Arr instance for the request.
// Only a valid host and token will be added to the context/config. The rest are manual
func (s *SABnzbd) authContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.FormValue("ma_username")
		token := r.FormValue("ma_password")
		category := getCategory(r.Context())
		a, err := s.authenticate(category, host, token)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), arrKey, a)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *SABnzbd) authenticate(category, username, password string) (*arr.Arr, error) {
	cfg := config.Get()
	arrStorage := s.manager.Arr()
	if matched := arrStorage.MatchCredentials(category, username, password); matched != nil {
		return matched, nil
	}
	if cfg.UseAuth && !config.VerifyAuth(username, password) {
		return nil, fmt.Errorf("unauthorized: invalid credentials")
	}
	return arrStorage.GetOrCreate(category), nil
}
