package arr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestImportIncludesOnlyTheApplicableArrMediaID(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	tests := []struct {
		name         string
		manualImport string
		wantMovieID  int
		wantSeriesID int
	}{
		{
			name:         "radarr",
			manualImport: `[{"path":"/downloads/movie.mkv","folderName":"movie","movie":{"id":42}}]`,
			wantMovieID:  42,
		},
		{
			name:         "sonarr",
			manualImport: `[{"path":"/downloads/show.mkv","folderName":"show","series":{"id":7},"seasonNumber":1,"episodes":[{"id":11}]}]`,
			wantSeriesID: 7,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var command struct {
				Name  string `json:"name"`
				Files []struct {
					MovieID  *int `json:"movieId"`
					SeriesID *int `json:"seriesId"`
				} `json:"files"`
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/api/v3/manualimport":
					if got := r.URL.Query().Get("downloadId"); got != "download-id" {
						t.Errorf("downloadId = %q", got)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(test.manualImport))
				case r.Method == http.MethodPost && r.URL.Path == "/api/v3/command":
					if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
						t.Errorf("decode command: %v", err)
						http.Error(w, "bad request", http.StatusBadRequest)
						return
					}
					w.WriteHeader(http.StatusOK)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			client := New(test.name, server.URL, "token", false, nil, "", "manual")
			body, err := client.Import("download-id")
			if err != nil {
				t.Fatal(err)
			}
			if body != nil {
				_ = body.Close()
			}

			if command.Name != "ManualImport" || len(command.Files) != 1 {
				t.Fatalf("command = %#v", command)
			}
			file := command.Files[0]
			if test.wantMovieID != 0 {
				if file.MovieID == nil || *file.MovieID != test.wantMovieID {
					t.Fatalf("movieId = %v, want %d", file.MovieID, test.wantMovieID)
				}
				if file.SeriesID != nil {
					t.Fatalf("seriesId = %v, want omitted", *file.SeriesID)
				}
			}
			if test.wantSeriesID != 0 {
				if file.SeriesID == nil || *file.SeriesID != test.wantSeriesID {
					t.Fatalf("seriesId = %v, want %d", file.SeriesID, test.wantSeriesID)
				}
				if file.MovieID != nil {
					t.Fatalf("movieId = %v, want omitted", *file.MovieID)
				}
			}
		})
	}
}
