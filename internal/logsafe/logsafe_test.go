package logsafe

import "testing"

func TestTextRemovesEveryLineBreak(t *testing.T) {
	got := Text("first\r\nforged\nthird\rfourth")
	if got != "firstforgedthirdfourth" {
		t.Fatalf("Text() = %q", got)
	}
}
