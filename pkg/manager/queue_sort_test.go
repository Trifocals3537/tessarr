package manager

import (
	"testing"
	"time"
)

func TestQueueCompletedBeforeHandlesIncompleteEntries(t *testing.T) {
	earlier := time.Unix(10, 0)
	later := time.Unix(20, 0)

	tests := []struct {
		name        string
		left, right *time.Time
		want        bool
	}{
		{name: "both incomplete", want: false},
		{name: "incomplete before completed", right: &later, want: true},
		{name: "completed after incomplete", left: &earlier, want: false},
		{name: "chronological", left: &earlier, right: &later, want: true},
		{name: "reverse chronological", left: &later, right: &earlier, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := queueCompletedBefore(tt.left, tt.right); got != tt.want {
				t.Fatalf("queueCompletedBefore() = %v, want %v", got, tt.want)
			}
		})
	}
}
