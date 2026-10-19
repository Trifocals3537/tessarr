package usenet

import (
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestFlattenLogicalNZBFileName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "normal release",
			input: "[Group] Show Name - S01E02 [1080p].mkv",
			want:  "[Group] Show Name - S01E02 [1080p].mkv",
		},
		{
			name:  "pokemon whosawhatsit",
			input: "Pokemon.1997.S20E15.Someone.We.Cant.See!.Whosawhatsit?!.English.Dub.1080p.HDTV.x265.10bit.AAC.2.0.Sonarr.TVDB-Vengeance.mkv",
			want:  "Pokemon.1997.S20E15.Someone.We.Cant.See!.Whosawhatsit_!.English.Dub.1080p.HDTV.x265.10bit.AAC.2.0.Sonarr.TVDB-Vengeance.mkv",
		},
		{
			name:  "pokemon prickly floragato",
			input: "Pokemon.1997.S20E51.A.Prickly.Floragato?!.The.Mysterious.Flower.Pillar.English.Dub.1080p.HDTV.x265.10bit.AAC.2.0.Sonarr.TVDB-Vengeance.mkv",
			want:  "Pokemon.1997.S20E51.A.Prickly.Floragato_!.The.Mysterious.Flower.Pillar.English.Dub.1080p.HDTV.x265.10bit.AAC.2.0.Sonarr.TVDB-Vengeance.mkv",
		},
		{
			name:  "colon",
			input: "Episode: The Return.mkv",
			want:  "Episode_ The Return.mkv",
		},
		{
			name:  "all sanitizable punctuation",
			input: `Bad"<>|*?.mp4`,
			want:  "Bad______.mp4",
		},
		{
			name:  "unicode title",
			input: "Pokémon – Déjà Vu?.srt",
			want:  "Pokémon – Déjà Vu_.srt",
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "archive slash path",
			input:   "Season 01/Episode 01.mkv",
			wantErr: true,
		},
		{
			name:    "archive backslash path",
			input:   `Season 01\Episode 01.mkv`,
			wantErr: true,
		},
		{
			name:    "traversal",
			input:   "../../Episode 01.mkv",
			wantErr: true,
		},
		{
			name:    "absolute path",
			input:   "/tmp/Episode 01.mkv",
			wantErr: true,
		},
		{
			name:    "windows drive path",
			input:   `C:\Episode 01.mkv`,
			wantErr: true,
		},
		{
			name:    "windows drive relative path",
			input:   `C:Episode 01.mkv`,
			wantErr: true,
		},
		{
			name:    "dot",
			input:   ".",
			wantErr: true,
		},
		{
			name:    "dot dot",
			input:   "..",
			wantErr: true,
		},
		{
			name:    "control character leaf",
			input:   "bad\nname.mkv",
			wantErr: true,
		},
		{
			name:    "nul byte",
			input:   "bad\x00name.mkv",
			wantErr: true,
		},
		{
			name:    "reserved device",
			input:   "CON.mkv",
			wantErr: true,
		},
		{
			name:    "trailing dot",
			input:   "Episode.mkv.",
			wantErr: true,
		},
		{
			name:    "trailing space",
			input:   "Episode.mkv ",
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := flattenLogicalNZBFileName(test.input)
			if test.wantErr {
				if err == nil {
					t.Fatalf("flattenLogicalNZBFileName(%q) error = nil", test.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("flattenLogicalNZBFileName(%q) error = %v", test.input, err)
			}
			if got != test.want {
				t.Fatalf("flattenLogicalNZBFileName(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestNormalizeLogicalNZBFileNamesRejectsSanitizationCollision(t *testing.T) {
	files := []storage.NZBFile{
		{Name: "Episode?.mkv"},
		{Name: "Episode*.mkv"},
	}
	if err := normalizeLogicalNZBFileNames(files); err == nil {
		t.Fatal("normalizeLogicalNZBFileNames() accepted colliding sanitized names")
	}
	if files[0].Name != "Episode?.mkv" || files[1].Name != "Episode*.mkv" {
		t.Fatalf("collision partially mutated names: %#v", files)
	}
}

func TestNormalizeLogicalNZBFileNamesRejectsPortableCaseCollision(t *testing.T) {
	files := []storage.NZBFile{
		{Name: "Episode.MKV"},
		{Name: "episode.mkv"},
	}
	if err := normalizeLogicalNZBFileNames(files); err == nil {
		t.Fatal("normalizeLogicalNZBFileNames() accepted a case-insensitive collision")
	}
}

func TestNormalizeLogicalNZBFileNamesLeavesValidNamesUnchanged(t *testing.T) {
	files := []storage.NZBFile{
		{Name: "Pokémon.S20E01.The.Pendant.of.Beginning.mkv"},
		{Name: "Pokemon.S20E01.English.srt"},
	}
	if err := normalizeLogicalNZBFileNames(files); err != nil {
		t.Fatal(err)
	}
	if files[0].Name != "Pokémon.S20E01.The.Pendant.of.Beginning.mkv" ||
		files[1].Name != "Pokemon.S20E01.English.srt" {
		t.Fatalf("valid names changed: %#v", files)
	}
}
