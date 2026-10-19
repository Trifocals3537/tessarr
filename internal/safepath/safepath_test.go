package safepath

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidateIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "release name", value: "[Group] Show Name - S01E02 [1080p].mkv"},
		{name: "dots within name", value: "Show..Name.2026.mkv"},
		{name: "empty", value: "", wantErr: true},
		{name: "dot", value: ".", wantErr: true},
		{name: "dot dot", value: "..", wantErr: true},
		{name: "parent traversal", value: "../outside", wantErr: true},
		{name: "nested slash", value: "season/episode.mkv", wantErr: true},
		{name: "nested backslash", value: `season\episode.mkv`, wantErr: true},
		{name: "absolute", value: string(filepath.Separator) + "outside", wantErr: true},
		{name: "windows drive", value: `C:outside.mkv`, wantErr: true},
		{name: "alternate data stream", value: "episode.mkv:payload", wantErr: true},
		{name: "trailing dot", value: "episode.mkv.", wantErr: true},
		{name: "trailing space", value: "episode.mkv ", wantErr: true},
		{name: "reserved device", value: "NUL", wantErr: true},
		{name: "reserved device extension", value: "con.mkv", wantErr: true},
		{name: "reserved numbered device", value: "COM9.txt", wantErr: true},
		{name: "reserved superscript device", value: "LPT\u00b2.log", wantErr: true},
		{name: "angle bracket", value: "bad<name", wantErr: true},
		{name: "quote", value: `bad"name`, wantErr: true},
		{name: "pipe", value: "bad|name", wantErr: true},
		{name: "question", value: "bad?name", wantErr: true},
		{name: "asterisk", value: "bad*name", wantErr: true},
		{name: "newline", value: "release\nname", wantErr: true},
		{name: "tab", value: "release\tname", wantErr: true},
		{name: "nul", value: "release\x00name", wantErr: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateIdentifier(test.value)
			if test.wantErr && err == nil {
				t.Fatalf("ValidateIdentifier(%q) error = nil", test.value)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("ValidateIdentifier(%q) error = %v", test.value, err)
			}
		})
	}
}

func TestPortableNameKeyIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	first, err := PortableNameKey("Episode.MKV")
	if err != nil {
		t.Fatal(err)
	}
	second, err := PortableNameKey("episode.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("PortableNameKey values differ: %q != %q", first, second)
	}
}

func TestCompactIdentifierLeavesValidNameUnchanged(t *testing.T) {
	t.Parallel()
	name := "Pokémon.S20E01.Whosawhatsit.mkv"
	got, err := CompactIdentifier(name, PortableIdentifierMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got != name {
		t.Fatalf("CompactIdentifier() = %q, want unchanged %q", got, name)
	}
}

func TestCompactIdentifierPreservesExtensionAndIsDeterministic(t *testing.T) {
	t.Parallel()
	name := strings.Repeat("Very.Long.Release.Name.", 20) + "Pokémon.nzb"
	first, err := CompactIdentifier(name, PortableIdentifierMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompactIdentifier(name, PortableIdentifierMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("compaction is not deterministic: %q != %q", first, second)
	}
	if len(first) > PortableIdentifierMaxBytes {
		t.Fatalf("compacted length = %d, want <= %d", len(first), PortableIdentifierMaxBytes)
	}
	if !strings.HasSuffix(first, ".nzb") {
		t.Fatalf("compacted name %q did not preserve .nzb", first)
	}
	if !strings.HasPrefix(first, "Very.Long.Release.Name.") {
		t.Fatalf("compacted name %q did not preserve readable prefix", first)
	}
	if !utf8.ValidString(first) {
		t.Fatalf("compacted name is not valid UTF-8: %q", first)
	}
	if err := ValidateIdentifier(first); err != nil {
		t.Fatalf("compacted identifier is not portable: %v", err)
	}
}

func TestCompactIdentifierSeparatesSamePrefixNames(t *testing.T) {
	t.Parallel()
	prefix := strings.Repeat("same-readable-prefix-", 20)
	first, err := CompactIdentifier(prefix+"first.nzb", PortableIdentifierMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CompactIdentifier(prefix+"second.nzb", PortableIdentifierMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("different oversized names compacted to the same identifier %q", first)
	}
}

func TestCompactIdentifierRejectsUnsafeSuffixBeforeShortening(t *testing.T) {
	t.Parallel()
	prefix := strings.Repeat("safe", PortableIdentifierMaxBytes)
	for _, value := range []string{
		prefix + "/outside.nzb",
		prefix + `\outside.nzb`,
		prefix + "\x00outside.nzb",
		prefix + "\noutside.nzb",
	} {
		if _, err := CompactIdentifier(value, PortableIdentifierMaxBytes); err == nil {
			t.Fatalf("CompactIdentifier(%q) accepted unsafe input", value)
		}
	}
}

func TestValidateRootRejectsDangerousTargets(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	volumeRoot := filepath.VolumeName(home) + string(filepath.Separator)

	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "filesystem root", path: volumeRoot},
		{name: "user home", path: home},
		{name: "home container", path: filepath.Dir(home)},
		{name: "tilde directory", path: filepath.Join(t.TempDir(), "~")},
		{name: "control character", path: filepath.Join(t.TempDir(), "bad\nroot")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateRoot(test.path); err == nil {
				t.Fatalf("ValidateRoot(%q) error = nil", test.path)
			}
		})
	}
}

func TestJoinIdentifiersContainsNormalPath(t *testing.T) {
	root := t.TempDir()
	got, err := JoinIdentifiers(root, "sonarr", "[Group] Show S01E01 [1080p]")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "sonarr", "[Group] Show S01E01 [1080p]")
	if got != want {
		t.Fatalf("JoinIdentifiers() = %q, want %q", got, want)
	}
}

func TestValidateUnderRootRejectsEscape(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(filepath.Dir(root), "outside")
	if _, err := ValidateUnderRoot(root, outside); err == nil {
		t.Fatal("ValidateUnderRoot() accepted an escaping target")
	}
	if _, err := ValidateUnderRoot(root, root); err == nil {
		t.Fatal("ValidateUnderRoot() accepted the root itself")
	}
}

func TestValidateUnderRootRejectsSymlinkParentAndTarget(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(outsideFile, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	parentLink := filepath.Join(root, "category")
	if err := os.Symlink(outside, parentLink); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ValidateUnderRoot(root, filepath.Join(parentLink, "release")); err == nil {
		t.Fatal("ValidateUnderRoot() accepted a symlink parent")
	}

	targetLink := filepath.Join(root, "target")
	if err := os.Symlink(outsideFile, targetLink); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateUnderRoot(root, targetLink); err == nil {
		t.Fatal("ValidateUnderRoot() accepted a symlink target")
	}

	if err := RemoveAll(root, parentLink); err == nil {
		t.Fatal("RemoveAll() accepted a symlink target")
	}
	if contents, err := os.ReadFile(outsideFile); err != nil || string(contents) != "keep" {
		t.Fatalf("outside sentinel changed: contents=%q err=%v", contents, err)
	}
}

func TestValidateRootRejectsSymlinkRoot(t *testing.T) {
	parent := t.TempDir()
	actual := t.TempDir()
	link := filepath.Join(parent, "cache-link")
	if err := os.Symlink(actual, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ValidateRoot(link); err == nil {
		t.Fatal("ValidateRoot() accepted a symlink root")
	}
}

func TestEnsureRootCreatesNestedDirectory(t *testing.T) {
	parent := t.TempDir()
	want := filepath.Join(parent, "cache", "segments")
	got, err := EnsureRoot(want, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("EnsureRoot() = %q, want %q", got, want)
	}
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("EnsureRoot() created non-directory mode %v", info.Mode())
	}
	rooted, absolute, err := OpenRoot(got)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()
	if absolute != want {
		t.Fatalf("OpenRoot() path = %q, want %q", absolute, want)
	}
}

func TestEnsureRootRejectsSymlinkParent(t *testing.T) {
	parent := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(parent, "cache")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := EnsureRoot(filepath.Join(link, "segments"), 0o700); err == nil {
		t.Fatal("EnsureRoot() accepted a symlink parent")
	}
	if _, err := os.Lstat(filepath.Join(outside, "segments")); !os.IsNotExist(err) {
		t.Fatalf("EnsureRoot() created content outside its boundary: %v", err)
	}
}

func TestAnchoredRootTraversalRejectsSymlinkAncestor(t *testing.T) {
	parent := t.TempDir()
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(outside, "segments"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "cache")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	absolute := filepath.Join(link, "segments")
	if _, err := openRootFromFilesystemAnchor(absolute, false, 0); err == nil {
		t.Fatal("anchored traversal accepted a symlink ancestor")
	}
}

func TestVerifyOpenRootDetectsVisibleParentReplacement(t *testing.T) {
	base := t.TempDir()
	visibleParent := filepath.Join(base, "visible")
	visibleRoot := filepath.Join(visibleParent, "root")
	movedParent := filepath.Join(base, "moved")
	if err := os.MkdirAll(visibleRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	rooted, _, err := OpenRoot(visibleRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer rooted.Close()

	if err := os.Rename(visibleParent, movedParent); err != nil {
		t.Skipf("cannot rename a directory containing an open root on this platform: %v", err)
	}
	if err := os.MkdirAll(visibleRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := VerifyOpenRoot(rooted, visibleRoot); err == nil {
		t.Fatal("VerifyOpenRoot() accepted a visible parent replacement")
	}
}

func TestOpenFileDoesNotTruncateOutsideHardLink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	insideFile := filepath.Join(root, "media.mkv")
	if err := os.Link(outsideFile, insideFile); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}

	file, err := OpenFile(root, insideFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("inside"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	assertSafePathContents(t, outsideFile, "outside")
	assertSafePathContents(t, insideFile, "inside")
}

func TestOpenFileDoesNotFollowOutsideSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "sentinel")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	insideFile := filepath.Join(root, "media.mkv")
	if err := os.Symlink(outsideFile, insideFile); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if _, err := OpenFile(root, insideFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600); err == nil {
		t.Fatal("OpenFile() accepted an outside symlink")
	}
	assertSafePathContents(t, outsideFile, "outside")
}

func TestRenameReplacesOnlyWithinRoot(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "old.tmp")
	newPath := filepath.Join(root, "new.txt")
	if err := os.WriteFile(oldPath, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Rename(root, oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(newPath)
	if err != nil || string(data) != "new" {
		t.Fatalf("renamed content = %q, %v", data, err)
	}
	outside := filepath.Join(filepath.Dir(root), "outside.txt")
	if err := Rename(root, newPath, outside); err == nil {
		t.Fatal("Rename accepted an outside destination")
	}
}

func TestSymlinkAllowsOutsideTargetButKeepsLinkUnderRoot(t *testing.T) {
	root := t.TempDir()
	linksDir := filepath.Join(root, "category", "release")
	if err := os.MkdirAll(linksDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outsideTarget := filepath.Join(t.TempDir(), "mounted.mkv")
	linkPath := filepath.Join(linksDir, "episode.mkv")

	if err := Symlink(root, outsideTarget, linkPath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlinks unavailable: %v", err)
		}
		t.Fatal(err)
	}
	got, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != outsideTarget {
		t.Fatalf("symlink target = %q, want %q", got, outsideTarget)
	}

	if err := Symlink(root, outsideTarget, linkPath); err != nil {
		t.Fatalf("repeat rooted symlink: %v", err)
	}

	replacementTarget := filepath.Join(t.TempDir(), "replacement.mkv")
	if err := Symlink(root, replacementTarget, linkPath); err == nil {
		t.Fatal("Symlink() replaced a link to a different target")
	}
	got, err = os.Readlink(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != outsideTarget {
		t.Fatalf("existing symlink target = %q, want preserved %q", got, outsideTarget)
	}

	regularPath := filepath.Join(linksDir, "regular.mkv")
	if err := os.WriteFile(regularPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Symlink(root, outsideTarget, regularPath); err == nil {
		t.Fatal("Symlink() replaced an existing regular file")
	}
	assertSafePathContents(t, regularPath, "keep")
}

func TestSymlinkAcceptsEquivalentAbsoluteAndRelativeTargets(t *testing.T) {
	root := t.TempDir()
	linksDir := filepath.Join(root, "category", "release")
	if err := os.MkdirAll(linksDir, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "mounted.mkv")
	if err := os.WriteFile(target, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(linksDir, "episode.mkv")
	if err := os.Symlink(target, linkPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	relativeTarget, err := filepath.Rel(linksDir, target)
	if err != nil {
		t.Skipf("relative symlinks unavailable for this layout: %v", err)
	}

	if err := Symlink(root, relativeTarget, linkPath); err != nil {
		t.Fatalf("equivalent relative target rejected: %v", err)
	}
	stored, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if stored != target {
		t.Fatalf("existing absolute link was rewritten to %q", stored)
	}
}

func TestSymlinkRejectsOutsideParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	parentLink := filepath.Join(root, "category")
	if err := os.Symlink(outside, parentLink); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	linkPath := filepath.Join(parentLink, "episode.mkv")
	if err := Symlink(root, filepath.Join(t.TempDir(), "mounted.mkv"), linkPath); err == nil {
		t.Fatal("Symlink() accepted an outside parent")
	}
	if _, err := os.Lstat(filepath.Join(outside, "episode.mkv")); !os.IsNotExist(err) {
		t.Fatalf("outside link was created: %v", err)
	}
}

func TestRegularFileOperationsStayPinnedToParent(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "config.json")
	if err := AtomicWriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("first atomic write: %v", err)
	}
	if err := AtomicWriteFile(path, []byte("second"), 0o600); err != nil {
		t.Fatalf("replacement atomic write: %v", err)
	}
	data, err := ReadRegularFile(path, 16)
	if err != nil {
		t.Fatalf("read regular file: %v", err)
	}
	if string(data) != "second" {
		t.Fatalf("contents = %q, want second", data)
	}
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatalf("make regular file read-only: %v", err)
	}
	if err := ChmodRegularFile(path, 0o600); err != nil {
		t.Fatalf("restore regular file permissions: %v", err)
	}
	writable, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open restored regular file for writing: %v", err)
	}
	if err := writable.Close(); err != nil {
		t.Fatalf("close restored regular file: %v", err)
	}
	if _, err := ReadRegularFile(path, 3); err == nil {
		t.Fatal("bounded read accepted an oversized file")
	}

	renamed := filepath.Join(root, "auth.json")
	if err := RenameRegularFile(path, renamed); err != nil {
		t.Fatalf("rename regular file: %v", err)
	}
	if err := RemoveRegularFile(renamed); err != nil {
		t.Fatalf("remove regular file: %v", err)
	}
}

func TestRegularFileOperationsRejectSymlinksAndCrossParentRename(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "config.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ReadRegularFile(link, 32); err == nil {
		t.Fatal("regular file read followed a symlink")
	}
	if err := RemoveRegularFile(link); err == nil {
		t.Fatal("regular file removal accepted a symlink")
	}
	if err := AtomicWriteFile(link, []byte("replacement"), 0o600); err == nil {
		t.Fatal("atomic write replaced a symlink")
	}
	assertSafePathContents(t, outside, "secret")

	directory := filepath.Join(root, "directory.json")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := AtomicWriteFile(directory, []byte("replacement"), 0o600); err == nil {
		t.Fatal("atomic write replaced a directory")
	}

	source := filepath.Join(root, "source.json")
	if err := os.WriteFile(source, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameRegularFile(source, filepath.Join(t.TempDir(), "destination.json")); err == nil {
		t.Fatal("regular file rename crossed parent directories")
	}
}

func TestReadRegularFilePreservesNotExistAcrossMissingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "removed", "config.json")
	_, err := ReadRegularFile(path, 32)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file error = %v, want os.ErrNotExist", err)
	}
}

func assertSafePathContents(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %q: %v", path, err)
	}
	if string(contents) != want {
		t.Fatalf("%q contents = %q, want %q", path, contents, want)
	}
}
