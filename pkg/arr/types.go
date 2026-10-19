package arr

import (
	"fmt"
	"os"
)

type Movie struct {
	Title         string `json:"title"`
	OriginalTitle string `json:"originalTitle"`
	Path          string `json:"path"`
	MovieFile     struct {
		MovieId      int    `json:"movieId"`
		RelativePath string `json:"relativePath"`
		Path         string `json:"path"`
		Id           int    `json:"id"`
		Size         int64  `json:"size"`
	} `json:"movieFile"`
	Id int `json:"id"`
}

type ContentFile struct {
	Name         string `json:"name"`
	Path         string `json:"path"`
	Id           int    `json:"id"`
	EpisodeId    int    `json:"showId"`
	FileId       int    `json:"fileId"`
	TargetPath   string `json:"targetPath"`
	EntryName    string `json:"entryName,omitempty"`
	IsSymlink    bool   `json:"isSymlink"`
	IsBroken     bool   `json:"isBroken"`
	SeasonNumber int    `json:"seasonNumber"`
	Processed    bool   `json:"processed"`
	Size         int64  `json:"size"`
}

func (file ContentFile) Delete() error {
	if file.Path == "" {
		return nil
	}
	if err := os.Remove(file.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete local content file %q: %w", file.Path, err)
	}
	return nil
}

type Content struct {
	Title string        `json:"title"`
	Id    int           `json:"id"`
	Files []ContentFile `json:"files"`
}

type seriesFile struct {
	SeriesId     int    `json:"seriesId"`
	SeasonNumber int    `json:"seasonNumber"`
	Path         string `json:"path"`
	Id           int    `json:"id"`
	Size         int64  `json:"size"`
}
