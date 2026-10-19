package types

import (
	"path"
	"reflect"
	"strings"
	"testing"
)

func TestFilesByLogicalNamePreservesUniqueBasenameCompatibility(t *testing.T) {
	files, err := FilesByLogicalName([]File{{
		Id:   "1",
		Name: "ignored-name",
		Path: "/Release/Season 01/Episode 01.mkv",
	}})
	if err != nil {
		t.Fatal(err)
	}
	file, exists := files["Episode 01.mkv"]
	if !exists {
		t.Fatalf("files = %#v, want historical basename key", files)
	}
	if file.Name != "Episode 01.mkv" {
		t.Fatalf("logical name = %q", file.Name)
	}
	if file.Path != "Release/Season 01/Episode 01.mkv" {
		t.Fatalf("provider path = %q", file.Path)
	}
	if file.OutputPath != "Release/Season 01/Episode 01.mkv" {
		t.Fatalf("output path = %q", file.OutputPath)
	}
}

func TestFilesByLogicalNameRejectsUnicodeEquivalentProviderPath(t *testing.T) {
	_, err := FilesByLogicalName([]File{
		{Id: "nfc", Path: "Release/Caf\u00e9.mkv"},
		{Id: "nfd", Path: "Release/Cafe\u0301.mkv"},
	})
	if err == nil || !strings.Contains(err.Error(), "same portable path") {
		t.Fatalf("Unicode-equivalent provider paths error = %v", err)
	}
}

func TestFilesByLogicalNameDisambiguatesUnicodeEquivalentDirectories(t *testing.T) {
	files, err := FilesByLogicalName([]File{
		{Id: "nfc", Path: "Caf\u00e9/first.mkv"},
		{Id: "nfd", Path: "Cafe\u0301/second.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := strings.Split(fileByProviderID(t, files, "nfc").OutputPath, "/")[0]
	second := strings.Split(fileByProviderID(t, files, "nfd").OutputPath, "/")[0]
	if portableProviderPathKey(first) == portableProviderPathKey(second) {
		t.Fatalf("Unicode-equivalent directories share output: %q and %q", first, second)
	}
}

func TestFilesByLogicalNameDisambiguatesUnicodeEquivalentBasenames(t *testing.T) {
	files, err := FilesByLogicalName([]File{
		{Id: "nfc", Path: "Season 01/Caf\u00e9.mkv"},
		{Id: "nfd", Path: "Season 02/Cafe\u0301.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := fileByProviderID(t, files, "nfc")
	second := fileByProviderID(t, files, "nfd")
	if first.Name == second.Name || portableProviderPathKey(first.Name) == portableProviderPathKey(second.Name) {
		t.Fatalf("Unicode-equivalent basenames share logical identity: %q and %q", first.Name, second.Name)
	}
}

func TestFilesByLogicalNamePreservesProviderTrailingWhitespace(t *testing.T) {
	files, err := FilesByLogicalName([]File{{Id: "space", Path: "Release/Movie.mkv "}})
	if err != nil {
		t.Fatal(err)
	}
	file := fileByProviderID(t, files, "space")
	if file.Path != "Release/Movie.mkv " {
		t.Fatalf("provider source path lost trailing whitespace: %q", file.Path)
	}
	if strings.HasSuffix(file.OutputPath, " ") {
		t.Fatalf("portable output retained trailing whitespace: %q", file.OutputPath)
	}
}

func TestFilesByLogicalNamePreservesNestedDuplicateBasenames(t *testing.T) {
	files, err := FilesByLogicalName([]File{
		{Id: "1", Path: `Release\Season 01\Episode.mkv`},
		{Id: "2", Path: "Release/Season 02/Episode.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := map[string]bool{
		"Release/Season 01/Episode.mkv": true,
		"Release/Season 02/Episode.mkv": true,
	}
	for name, file := range files {
		if file.Name != name || strings.ContainsAny(name, `/\`) || path.Ext(name) != ".mkv" {
			t.Fatalf("logical file %q = %#v", name, file)
		}
		if path.Base(file.OutputPath) != name || strings.Contains(file.OutputPath, `\`) {
			t.Fatalf("output path for %q = %q", name, file.OutputPath)
		}
		if !wantPaths[file.Path] {
			t.Fatalf("unexpected provider path %q", file.Path)
		}
		delete(wantPaths, file.Path)
	}
	if len(wantPaths) != 0 {
		t.Fatalf("missing provider paths: %#v", wantPaths)
	}
}

func TestFilesByLogicalNameSanitizesPortablePunctuationAndDetectsCollisions(t *testing.T) {
	files, err := FilesByLogicalName([]File{{Id: "1", Path: "Release/Why?.mkv"}})
	if err != nil {
		t.Fatal(err)
	}
	first := fileByProviderID(t, files, "1")
	if strings.ContainsAny(first.Name, `/\?*`) || path.Ext(first.Name) != ".mkv" {
		t.Fatalf("sanitized file = %#v", first)
	}
	if first.Path != "Release/Why?.mkv" || path.Base(first.OutputPath) != first.Name {
		t.Fatalf("provider/output paths were not separated: %#v", first)
	}
	disambiguated, err := FilesByLogicalName([]File{
		{Id: "1", Path: "Release/Why?.mkv"},
		{Id: "2", Path: "Release/Why*.mkv"},
	})
	if err != nil {
		t.Fatalf("distinct unsafe basenames should be deterministically disambiguated: %v", err)
	}
	if len(disambiguated) != 2 {
		t.Fatalf("disambiguated file count = %d, want 2", len(disambiguated))
	}
	if paired := fileByProviderID(t, disambiguated, "1"); paired.Name != first.Name {
		t.Fatalf("lossy logical identity changed with siblings: alone=%q paired=%q", first.Name, paired.Name)
	}
	for name, file := range disambiguated {
		if name != file.Name || strings.ContainsAny(name, `/\?*`) || path.Ext(name) != ".mkv" {
			t.Fatalf("unsafe disambiguated file %q = %#v", name, file)
		}
		if path.Base(file.OutputPath) != name || strings.ContainsAny(file.OutputPath, `\?*`) {
			t.Fatalf("unsafe output path for %q = %q", name, file.OutputPath)
		}
	}
}

func TestFilesByLogicalNameResolvesGeneratedAndLiteralNameConflict(t *testing.T) {
	duplicates := []File{
		{Id: "1", Path: "Release/Season 01/Episode.mkv"},
		{Id: "2", Path: "Release/Season 02/Episode.mkv"},
	}
	initial, err := FilesByLogicalName(duplicates)
	if err != nil {
		t.Fatal(err)
	}
	generated := fileByProviderID(t, initial, "1").Name
	files, err := FilesByLogicalName(append(duplicates, File{
		Id:   "3",
		Path: "Release/Extras/" + generated,
	}))
	if err != nil {
		t.Fatalf("literal generated-looking basename rejected: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("file count = %d, want 3", len(files))
	}
	if literal := fileByProviderID(t, files, "3"); literal.Name == generated {
		t.Fatalf("literal conflict was not deterministically disambiguated: %#v", literal)
	}
}

func TestFilesByLogicalNameSanitizesOutputDirectoriesOnly(t *testing.T) {
	files, err := FilesByLogicalName([]File{{
		Id:   "1",
		Path: "Release?/Season: 01/Episode.mkv",
	}})
	if err != nil {
		t.Fatal(err)
	}
	file := fileByProviderID(t, files, "1")
	if file.Path != "Release?/Season: 01/Episode.mkv" {
		t.Fatalf("provider path changed to %q", file.Path)
	}
	if !strings.HasPrefix(file.OutputPath, "Release_~") ||
		!strings.Contains(file.OutputPath, "/Season_ 01~") ||
		!strings.HasSuffix(file.OutputPath, "/Episode.mkv") {
		t.Fatalf("output path = %q", file.OutputPath)
	}
}

func TestFilesByLogicalNameKeepsLossyDirectoriesDistinct(t *testing.T) {
	files, err := FilesByLogicalName([]File{
		{Id: "1", Path: "A?/movie.mkv"},
		{Id: "2", Path: "A*/movie.mkv/extra.srt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := fileByProviderID(t, files, "1")
	second := fileByProviderID(t, files, "2")
	firstRoot := strings.Split(first.OutputPath, "/")[0]
	secondRoot := strings.Split(second.OutputPath, "/")[0]
	if firstRoot == secondRoot || !strings.HasPrefix(firstRoot, "A_~") || !strings.HasPrefix(secondRoot, "A_~") {
		t.Fatalf("lossy directory outputs were merged: first=%q second=%q", first.OutputPath, second.OutputPath)
	}
	if first.Path != "A?/movie.mkv" || second.Path != "A*/movie.mkv/extra.srt" {
		t.Fatalf("provider paths changed: first=%q second=%q", first.Path, second.Path)
	}
}

func TestFilesByLogicalNameKeepsPortableFoldedDirectoriesDistinct(t *testing.T) {
	input := []File{
		{Id: "trailing-dot", Path: "A./one.mkv"},
		{Id: "literal", Path: "A/two.mkv"},
	}
	files, err := FilesByLogicalName(input)
	if err != nil {
		t.Fatal(err)
	}
	firstRoot := strings.Split(fileByProviderID(t, files, "trailing-dot").OutputPath, "/")[0]
	secondRoot := strings.Split(fileByProviderID(t, files, "literal").OutputPath, "/")[0]
	if portableProviderPathKey(firstRoot) == portableProviderPathKey(secondRoot) {
		t.Fatalf("portable-folded provider directories were merged: %q and %q", firstRoot, secondRoot)
	}
	reversed, err := FilesByLogicalName([]File{input[1], input[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(files, reversed) {
		t.Fatalf("folded directory allocation changed with provider order:\nfirst: %#v\nsecond: %#v", files, reversed)
	}
}

func TestFilesByLogicalNameHashesEveryFoldedDirectoryCollisionMember(t *testing.T) {
	initial, err := FilesByLogicalName([]File{{
		Id: "lower", Path: "a/movie.mkv/extra.srt",
	}})
	if err != nil {
		t.Fatal(err)
	}
	initialRoot := strings.Split(fileByProviderID(t, initial, "lower").OutputPath, "/")[0]
	expanded, err := FilesByLogicalName([]File{
		{Id: "lower", Path: "a/movie.mkv/extra.srt"},
		{Id: "upper", Path: "A/movie.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	lowerRoot := strings.Split(fileByProviderID(t, expanded, "lower").OutputPath, "/")[0]
	upperRoot := strings.Split(fileByProviderID(t, expanded, "upper").OutputPath, "/")[0]
	if lowerRoot == initialRoot || upperRoot == initialRoot {
		t.Fatalf("folded collision retained a literal winner: initial=%q lower=%q upper=%q", initialRoot, lowerRoot, upperRoot)
	}
	if portableProviderPathKey(lowerRoot) == portableProviderPathKey(upperRoot) {
		t.Fatalf("folded collision members share an output root: lower=%q upper=%q", lowerRoot, upperRoot)
	}
}

func TestFilesByLogicalNameDisambiguatesOwnershipArtifactNames(t *testing.T) {
	files, err := FilesByLogicalName([]File{
		{Id: "marker", Path: ".tessarr-torrent-owner-v1"},
		{Id: "legacy-part", Path: ".decypharr-torrent-part-provider/Episode.mkv"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		for _, component := range strings.Split(file.OutputPath, "/") {
			if isReservedProviderOutputName(component) {
				t.Fatalf("reserved ownership name survived output planning: %#v", file)
			}
		}
	}
}

func TestFilesByLogicalNameReservesLiteralDirectoryAgainstGeneratedName(t *testing.T) {
	generated, err := disambiguateProviderDirectoryName("A_", portableProviderPathKey("A?"), 0)
	if err != nil {
		t.Fatal(err)
	}
	input := []File{
		{Id: "lossy", Path: "A?/movie.mkv"},
		{Id: "literal", Path: generated + "/extra.srt"},
	}
	files, err := FilesByLogicalName(input)
	if err != nil {
		t.Fatalf("generated-looking literal directory rejected: %v", err)
	}
	lossy := fileByProviderID(t, files, "lossy")
	literal := fileByProviderID(t, files, "literal")
	if strings.Split(lossy.OutputPath, "/")[0] == strings.Split(literal.OutputPath, "/")[0] {
		t.Fatalf("generated directory stole literal identity: lossy=%q literal=%q", lossy.OutputPath, literal.OutputPath)
	}
	if !strings.HasPrefix(literal.OutputPath, generated+"/") {
		t.Fatalf("literal directory moved to %q", literal.OutputPath)
	}
	reversed, err := FilesByLogicalName([]File{input[1], input[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(files, reversed) {
		t.Fatalf("directory allocation changed with provider order:\nfirst: %#v\nsecond: %#v", files, reversed)
	}
}

func TestFilesByLogicalNameSeparatesFileAndDirectoryOutputPaths(t *testing.T) {
	input := []File{
		{Id: "1", Path: "Release/Movie_.mkv"},
		{Id: "2", Path: "Release/Movie?.mkv/Episode.mkv"},
	}
	files, err := FilesByLogicalName(input)
	if err != nil {
		t.Fatal(err)
	}
	file := fileByProviderID(t, files, "1")
	nested := fileByProviderID(t, files, "2")
	if file.Path != input[0].Path || nested.Path != input[1].Path {
		t.Fatalf("provider paths changed: file=%q nested=%q", file.Path, nested.Path)
	}
	if file.OutputPath != "Release/Movie_.mkv" {
		t.Fatalf("non-conflicting file output changed to %q", file.OutputPath)
	}
	if !strings.HasPrefix(nested.OutputPath, "Release/Movie_~") || !strings.HasSuffix(nested.OutputPath, ".mkv/Episode.mkv") {
		t.Fatalf("nested output was not safely disambiguated: %q", nested.OutputPath)
	}
	if strings.HasPrefix(
		portableProviderPathKey(nested.OutputPath),
		portableProviderPathKey(file.OutputPath)+"/",
	) {
		t.Fatalf("file output %q remains a parent of %q", file.OutputPath, nested.OutputPath)
	}

	reversed, err := FilesByLogicalName([]File{input[1], input[0]})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(files, reversed) {
		t.Fatalf("file/directory disambiguation changed with provider order:\nfirst: %#v\nsecond: %#v", files, reversed)
	}
}

func TestFilesByLogicalNameIsDeterministicAcrossProviderOrder(t *testing.T) {
	firstInput := []File{
		{Id: "1", Path: "Release/Season 01/Episode.mkv"},
		{Id: "2", Path: "Release/Season 02/Episode.mkv"},
	}
	secondInput := []File{firstInput[1], firstInput[0]}
	first, err := FilesByLogicalName(firstInput)
	if err != nil {
		t.Fatal(err)
	}
	second, err := FilesByLogicalName(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("logical names changed with provider order:\nfirst: %#v\nsecond: %#v", first, second)
	}
}

func TestFilesByLogicalNameFailsClosedOnPortablePathCollision(t *testing.T) {
	_, err := FilesByLogicalName([]File{
		{Id: "1", Path: "Release/Season/Episode.mkv"},
		{Id: "2", Path: "release/season/episode.MKV"},
	})
	if err == nil {
		t.Fatal("expected portable path collision to be rejected")
	}
}

func TestFilesByLogicalNameRejectsTraversal(t *testing.T) {
	if _, err := FilesByLogicalName([]File{{Path: "../escape.mkv"}}); err == nil {
		t.Fatal("expected traversal to be rejected")
	}
}

func TestFilesByLogicalNameRejectsUnboundedFileSets(t *testing.T) {
	if _, err := FilesByLogicalName(make([]File, maxProviderFileRecords+1)); err == nil {
		t.Fatal("expected oversized provider file set to be rejected")
	}
}

func TestFilesByLogicalNameRejectsUnboundedPathDepth(t *testing.T) {
	providerPath := strings.Repeat("nested/", maxProviderOutputDepth+1) + "movie.mkv"
	if _, err := FilesByLogicalName([]File{{Path: providerPath}}); err == nil || !strings.Contains(err.Error(), "maximum output depth") {
		t.Fatalf("deep provider path error = %v", err)
	}
}

func fileByProviderID(t *testing.T, files map[string]File, id string) File {
	t.Helper()
	for _, file := range files {
		if file.Id == id {
			return file
		}
	}
	t.Fatalf("provider file %q not found in %#v", id, files)
	return File{}
}
