package yenc

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"
)

func TestPureGoRejectsCorruptArticleIntegrity(t *testing.T) {
	const begin = "=ybegin part=1 line=128 size=4 name=video.mkv\r\n=ypart begin=1 end=4\r\nkkkk\r\n"
	goodEnd := fmt.Sprintf("=yend size=4 part=1 pcrc32=%08x\r\n", crc32.ChecksumIEEE([]byte("AAAA")))
	for _, tc := range []struct {
		name, article string
		valid         bool
	}{
		{"valid", begin + goodEnd, true},
		{"wrong CRC", begin + "=yend size=4 part=1 pcrc32=00000000\r\n", false},
		{"malformed CRC", begin + "=yend size=4 part=1 pcrc32=wrong\r\n", false},
		{"short decoded data", strings.Replace(begin, "kkkk", "kkk", 1) + goodEnd, false},
		{"inconsistent footer size", begin + strings.Replace(goodEnd, "size=4", "size=5", 1), false},
		{"missing footer size", begin + strings.Replace(goodEnd, "size=4 ", "", 1), false},
		{"nonnumeric footer size", begin + strings.Replace(goodEnd, "size=4", "size=invalid", 1), false},
		{"overflowing footer size", begin + strings.Replace(goodEnd, "size=4", "size=9223372036854775808", 1), false},
		{"negative footer size", begin + strings.Replace(goodEnd, "size=4", "size=-4", 1), false},
		{"duplicate footer size", begin + strings.Replace(goodEnd, "size=4", "size=5 size=4", 1), false},
		{"inconsistent part size", strings.Replace(begin, "end=4", "end=5", 1) + goodEnd, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var meta DecoderMeta
			dec := newPureGoYencDecoder(bufio.NewReader(strings.NewReader(tc.article+".\r\n")), &meta)
			dec.nntp = true
			_, err := io.ReadAll(dec)
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v want valid=%v", err, tc.valid)
			}
		})
	}
}
