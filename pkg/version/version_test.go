package version

import "testing"

func TestInfoString(t *testing.T) {
	tests := []struct {
		name string
		info Info
		want string
	}{
		{
			name: "stable release",
			info: Info{Version: "2.4.1", Channel: "stable"},
			want: "2.4.1",
		},
		{
			name: "prerelease",
			info: Info{Version: "2.4.1-beta.2", Channel: "beta"},
			want: "2.4.1-beta.2",
		},
		{
			name: "continuous integration",
			info: Info{Version: "ci-0123456789ab", Channel: "ci"},
			want: "ci-0123456789ab",
		},
		{
			name: "channel-only fallback",
			info: Info{Channel: "nightly"},
			want: "nightly",
		},
		{
			name: "development fallback",
			info: Info{},
			want: "development",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.info.String(); got != test.want {
				t.Fatalf("Info.String() = %q, want %q", got, test.want)
			}
		})
	}
}
