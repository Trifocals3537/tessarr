package parser

import "testing"

func TestDetermineNZBNameUsesFirstPortableCandidate(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		meta     map[string]string
		want     string
	}{
		{name: "filename", filename: "release.nzb", meta: map[string]string{"Name": "meta"}, want: "release"},
		{name: "invalid filename falls back", filename: "???.nzb", meta: map[string]string{"Name": "meta-release"}, want: "meta-release"},
		{name: "reserved filename falls back", filename: "CON.nzb", meta: map[string]string{"title": "safe-title"}, want: "safe-title"},
		{name: "all invalid", filename: ".nzb", meta: map[string]string{"Name": "..", "title": "***"}, want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := determineNZBName(test.filename, test.meta); got != test.want {
				t.Fatalf("determineNZBName() = %q, want %q", got, test.want)
			}
		})
	}
}
