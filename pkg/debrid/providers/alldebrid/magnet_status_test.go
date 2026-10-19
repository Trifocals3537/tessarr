package alldebrid

import (
	"testing"

	json "github.com/bytedance/sonic"
)

func TestMagnetsAcceptsSingleObjectAndArrayResponses(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "single object", body: `{"id":2,"filename":"wanted"}`, want: 1},
		{name: "array", body: `[{"id":1},{"id":2,"filename":"wanted"}]`, want: 2},
		{name: "keyed map", body: `{"2":{"id":2,"filename":"wanted"}}`, want: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var magnets Magnets
			if err := json.Unmarshal([]byte(test.body), &magnets); err != nil {
				t.Fatal(err)
			}
			if len(magnets) != test.want {
				t.Fatalf("magnets = %#v, want %d items", magnets, test.want)
			}
		})
	}
}

func TestFindMagnetSelectsExactID(t *testing.T) {
	magnets := Magnets{{Id: 1, Filename: "other"}, {Id: 2, Filename: "wanted"}}

	got, err := findMagnet(magnets, "2")
	if err != nil {
		t.Fatal(err)
	}
	if got.Filename != "wanted" {
		t.Fatalf("magnet = %#v", got)
	}
	if _, err := findMagnet(magnets, "3"); err == nil {
		t.Fatal("missing ID should return torrent-not-found error")
	}
}
