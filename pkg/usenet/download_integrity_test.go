package usenet

import (
	"strings"
	"testing"
)

func TestPrepareSegmentPayloadRequiresCompleteExpectedSlice(t *testing.T) {
	for _, test := range []struct {
		name          string
		data          string
		dataStart     int64
		expectedBytes int64
		want          string
		wantErr       string
	}{
		{name: "exact", data: "abc", expectedBytes: 3, want: "abc"},
		{name: "trims padding", data: "abc-padding", expectedBytes: 3, want: "abc"},
		{name: "sliced", data: "xxabc-padding", dataStart: 2, expectedBytes: 3, want: "abc"},
		{name: "short", data: "ab", expectedBytes: 3, wantErr: "incomplete decoded data"},
		{name: "short after slice", data: "xxab", dataStart: 2, expectedBytes: 3, wantErr: "incomplete decoded data"},
		{name: "offset exceeds data", data: "abc", dataStart: 4, expectedBytes: 1, wantErr: "exceeds decoded size"},
		{name: "negative offset", data: "abc", dataStart: -1, expectedBytes: 1, wantErr: "negative data offset"},
		{name: "invalid expected size", data: "abc", expectedBytes: 0, wantErr: "invalid expected size"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := prepareSegmentPayload([]byte(test.data), test.dataStart, test.expectedBytes)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || string(got) != test.want {
				t.Fatalf("payload = %q, error = %v; want %q", got, err, test.want)
			}
		})
	}
}
