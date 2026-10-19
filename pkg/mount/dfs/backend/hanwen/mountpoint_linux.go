//go:build linux

package hanwen

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var mountInfoPathReplacer = strings.NewReplacer(
	`\040`, " ",
	`\011`, "\t",
	`\012`, "\n",
	`\134`, `\`,
)

// mountPointActive checks the process mount namespace without touching the
// mount itself. That avoids blocking on a stale FUSE connection and avoids
// launching four unmount helpers on every clean startup.
func mountPointActive(path string) (bool, error) {
	target, err := filepath.Abs(path)
	if err != nil {
		return false, fmt.Errorf("resolve mount path: %w", err)
	}
	target = filepath.Clean(target)

	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("open mountinfo: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mountPath := filepath.Clean(mountInfoPathReplacer.Replace(fields[4]))
		if mountPath == target {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("read mountinfo: %w", err)
	}
	return false, nil
}
