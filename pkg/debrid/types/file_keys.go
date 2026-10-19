package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"golang.org/x/text/unicode/norm"
)

const (
	maxProviderFileRecords   = 100_000
	maxProviderOutputDepth   = 64
	maxDirectoryNameAttempts = 16
)

type providerFileCandidate struct {
	file                   File
	baseName               string
	baseKey                string
	fullPath               string
	providerParts          []string
	outputParts            []string
	requiresDisambiguation bool
	logicalName            string
}

type providerDirectoryGroup struct {
	componentIndex int
	providerKey    string
	members        []int
	lossy          bool
}

// FilesByLogicalName converts provider file records into Tessarr's
// name-keyed representation without losing nested files that share a
// basename. The historical basename key remains unchanged when it is safe and
// unambiguous. Unsafe or colliding basenames receive a stable,
// collision-resistant single-component name. Provider paths remain untouched
// for API lookups while OutputPath carries a separately validated local layout.
func FilesByLogicalName(files []File) (map[string]File, error) {
	if len(files) > maxProviderFileRecords {
		return nil, fmt.Errorf(
			"provider returned %d file records, maximum is %d",
			len(files),
			maxProviderFileRecords,
		)
	}

	candidates := make([]providerFileCandidate, 0, len(files))
	basenameCounts := make(map[string]int, len(files))
	providerPaths := make(map[string]string, len(files))
	for _, file := range files {
		fullPath, err := normalizeProviderFilePath(file.Path, file.Name)
		if err != nil {
			return nil, fmt.Errorf("provider file %q: %w", file.Name, err)
		}
		pathKey := portableProviderPathKey(fullPath)
		if previous, exists := providerPaths[pathKey]; exists {
			return nil, fmt.Errorf(
				"provider files %q and %q have the same portable path",
				previous,
				fullPath,
			)
		}
		providerPaths[pathKey] = fullPath

		originalBaseName := path.Base(fullPath)
		baseName, err := portableProviderFileName(originalBaseName)
		if err != nil {
			return nil, fmt.Errorf("provider file %q: %w", file.Name, err)
		}
		baseKey, _ := safepath.PortableNameKey(baseName)
		file.Path = fullPath
		candidates = append(candidates, providerFileCandidate{
			file:                   file,
			baseName:               baseName,
			baseKey:                baseKey,
			fullPath:               fullPath,
			requiresDisambiguation: baseName != originalBaseName,
		})
		basenameCounts[baseKey]++
	}

	baseOwners := make(map[string][]int, len(candidates))
	queue := make([]int, 0, len(candidates))
	for index := range candidates {
		baseOwners[candidates[index].baseKey] = append(baseOwners[candidates[index].baseKey], index)
		if basenameCounts[candidates[index].baseKey] > 1 {
			candidates[index].requiresDisambiguation = true
		}
		if candidates[index].requiresDisambiguation {
			if err := assignProviderLogicalName(&candidates[index]); err != nil {
				return nil, err
			}
			queue = append(queue, index)
		}
	}

	// Generated names take precedence over a provider-supplied literal that
	// happens to look generated. Promote that literal into the generated
	// namespace too. Each candidate is queued at most once, so even a malicious
	// chain of generated-looking basenames is resolved in linear time.
	for head := 0; head < len(queue); head++ {
		generated := &candidates[queue[head]]
		key, err := safepath.PortableNameKey(generated.logicalName)
		if err != nil {
			return nil, fmt.Errorf("provider file %q: %w", generated.fullPath, err)
		}
		for _, owner := range baseOwners[key] {
			if candidates[owner].requiresDisambiguation {
				continue
			}
			candidates[owner].requiresDisambiguation = true
			if err := assignProviderLogicalName(&candidates[owner]); err != nil {
				return nil, err
			}
			queue = append(queue, owner)
		}
	}
	for index := range candidates {
		if candidates[index].logicalName == "" {
			candidates[index].logicalName = candidates[index].baseName
		}
	}
	if err := assignProviderOutputPaths(candidates); err != nil {
		return nil, err
	}

	result := make(map[string]File, len(candidates))
	portableNames := make(map[string]string, len(candidates))
	portableOutputs := make(map[string]string, len(candidates))
	for index := range candidates {
		candidate := &candidates[index]
		logicalKey, err := safepath.PortableNameKey(candidate.logicalName)
		if err != nil {
			return nil, fmt.Errorf("provider file %q: %w", candidate.fullPath, err)
		}
		if previous, exists := portableNames[logicalKey]; exists {
			return nil, fmt.Errorf("provider files %q and %q have the same generated logical name", previous, candidate.logicalName)
		}
		portableNames[logicalKey] = candidate.logicalName
		outputPath := candidate.file.OutputPath
		outputKey := portableProviderPathKey(outputPath)
		if previous, exists := portableOutputs[outputKey]; exists {
			return nil, fmt.Errorf("provider files %q and %q have the same portable output path", previous, outputPath)
		}
		portableOutputs[outputKey] = outputPath
		candidate.file.Name = candidate.logicalName
		candidate.file.OutputPath = outputPath
		result[candidate.logicalName] = candidate.file
	}
	return result, nil
}

func assignProviderLogicalName(candidate *providerFileCandidate) error {
	candidate.logicalName = candidate.baseName
	if !candidate.requiresDisambiguation {
		return nil
	}
	logicalName, err := disambiguateProviderFileName(candidate.baseName, candidate.fullPath)
	if err != nil {
		return fmt.Errorf("provider file %q: %w", candidate.fullPath, err)
	}
	candidate.logicalName = logicalName
	return nil
}

func portableProviderFileName(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("filename is empty")
	}
	if strings.IndexByte(name, 0) >= 0 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("filename contains a NUL byte or control character")
	}
	if strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("filename contains a path separator")
	}

	name = strings.Map(func(r rune) rune {
		switch r {
		case '?', ':', '"', '<', '>', '|', '*':
			return '_'
		default:
			return r
		}
	}, name)
	name = strings.TrimRight(name, " .")
	if name == "" || name == "." || name == ".." {
		return "", fmt.Errorf("filename is empty or traversal after normalization")
	}
	if err := safepath.ValidateIdentifier(name); err != nil {
		prefixed := "file-" + name
		if prefixErr := safepath.ValidateIdentifier(prefixed); prefixErr != nil {
			return "", err
		}
		name = prefixed
	}
	if isReservedProviderOutputName(name) {
		name = "provider-" + name
	}
	return safepath.CompactIdentifier(name, safepath.PortableIdentifierMaxBytes)
}

func isReservedProviderOutputName(name string) bool {
	lower := strings.ToLower(name)
	return lower == ".tessarr-torrent-owner-v1" ||
		lower == ".decypharr-torrent-owner-v1" ||
		lower == ".tessarr-torrent-ownership.lock" ||
		lower == ".decypharr-torrent-ownership.lock" ||
		strings.HasPrefix(lower, ".tessarr-torrent-quarantine-") ||
		strings.HasPrefix(lower, ".decypharr-torrent-quarantine-") ||
		strings.HasPrefix(lower, ".tessarr-torrent-part-") ||
		strings.HasPrefix(lower, ".decypharr-torrent-part-")
}

func disambiguateProviderFileName(baseName, fullPath string) (string, error) {
	extension := path.Ext(baseName)
	stem := strings.TrimSuffix(baseName, extension)
	digest := sha256.Sum256([]byte(portableProviderPathKey(fullPath)))
	name := stem + "~" + hex.EncodeToString(digest[:]) + extension
	return safepath.CompactIdentifier(name, safepath.PortableIdentifierMaxBytes)
}

func assignProviderOutputPaths(candidates []providerFileCandidate) error {
	filesByDepth := make(map[int][]int)
	directoryGroupsByDepth := make(map[int]map[string]*providerDirectoryGroup)
	maxDirectoryDepth := -1
	for index := range candidates {
		candidate := &candidates[index]
		outputParts, err := portableProviderOutputParts(candidate.fullPath, candidate.logicalName)
		if err != nil {
			return fmt.Errorf("provider file %q: %w", candidate.fullPath, err)
		}
		candidate.providerParts = strings.Split(candidate.fullPath, "/")
		candidate.outputParts = outputParts
		leafDepth := len(outputParts) - 1
		if leafDepth > maxProviderOutputDepth {
			return fmt.Errorf(
				"provider file %q exceeds maximum output depth %d",
				candidate.fullPath,
				maxProviderOutputDepth,
			)
		}
		filesByDepth[leafDepth] = append(filesByDepth[leafDepth], index)
		for depth := 0; depth < leafDepth; depth++ {
			if depth > maxDirectoryDepth {
				maxDirectoryDepth = depth
			}
			groups := directoryGroupsByDepth[depth]
			if groups == nil {
				groups = make(map[string]*providerDirectoryGroup)
				directoryGroupsByDepth[depth] = groups
			}
			providerKey := providerDirectoryIdentityKey(strings.Join(candidate.providerParts[:depth+1], "/"))
			group := groups[providerKey]
			if group == nil {
				group = &providerDirectoryGroup{
					componentIndex: depth,
					providerKey:    providerKey,
				}
				groups[providerKey] = group
			}
			if candidate.outputParts[depth] != candidate.providerParts[depth] {
				group.lossy = true
			}
			group.members = append(group.members, index)
		}
	}

	// Resolve a file/directory collision at each depth only after every parent
	// component is final. Files at this depth cannot move later, so a stable
	// hash of the provider directory identity is sufficient and the pass stays
	// bounded by the number of path components.
	for depth := 0; depth <= maxDirectoryDepth; depth++ {
		fileKeys := make(map[string]struct{}, len(filesByDepth[depth]))
		for _, index := range filesByDepth[depth] {
			fileKeys[portableProviderPathKey(strings.Join(candidates[index].outputParts, "/"))] = struct{}{}
		}
		groups := directoryGroupsByDepth[depth]
		groupKeys := make([]string, 0, len(groups))
		for key := range groups {
			groupKeys = append(groupKeys, key)
		}
		sort.Strings(groupKeys)
		reservedPrefixes := make(map[string]string, len(groups))
		needsGeneratedName := make(map[string]bool, len(groups))
		groupsByPortablePrefix := make(map[string][]string, len(groups))
		// Reserve every literal sibling before allocating generated names. This
		// prevents a lossy directory's hash-derived candidate from taking a
		// provider-supplied directory name that merely looks generated.
		for _, groupKey := range groupKeys {
			group := groups[groupKey]
			representative := &candidates[group.members[0]]
			prefix := strings.Join(representative.outputParts[:depth+1], "/")
			prefixKey := portableProviderPathKey(prefix)
			groupsByPortablePrefix[prefixKey] = append(groupsByPortablePrefix[prefixKey], groupKey)
			_, fileConflict := fileKeys[prefixKey]
			if group.lossy || fileConflict {
				needsGeneratedName[groupKey] = true
			}
		}
		// If multiple provider directories fold to the same portable prefix,
		// generate a stable identity-derived name for every participant. Leaving
		// one literal would make the winner depend on which sibling is present.
		for _, groupKeys := range groupsByPortablePrefix {
			if len(groupKeys) < 2 {
				continue
			}
			for _, groupKey := range groupKeys {
				needsGeneratedName[groupKey] = true
			}
		}
		for _, groupKey := range groupKeys {
			if needsGeneratedName[groupKey] {
				continue
			}
			group := groups[groupKey]
			representative := &candidates[group.members[0]]
			prefix := strings.Join(representative.outputParts[:depth+1], "/")
			reservedPrefixes[portableProviderPathKey(prefix)] = group.providerKey
		}
		for _, groupKey := range groupKeys {
			if !needsGeneratedName[groupKey] {
				continue
			}
			group := groups[groupKey]
			representative := &candidates[group.members[0]]
			original := representative.outputParts[depth]
			resolved := ""
			for attempt := 0; attempt < maxDirectoryNameAttempts; attempt++ {
				name, err := disambiguateProviderDirectoryName(original, group.providerKey, attempt)
				if err != nil {
					return fmt.Errorf("provider directory %q: %w", group.providerKey, err)
				}
				parts := append([]string(nil), representative.outputParts[:depth]...)
				parts = append(parts, name)
				prefixKey := portableProviderPathKey(strings.Join(parts, "/"))
				if _, conflict := fileKeys[prefixKey]; conflict {
					continue
				}
				if owner, conflict := reservedPrefixes[prefixKey]; conflict && owner != group.providerKey {
					continue
				}
				resolved = name
				reservedPrefixes[prefixKey] = group.providerKey
				break
			}
			if resolved == "" {
				return fmt.Errorf("provider directory %q cannot be assigned a unique portable name", group.providerKey)
			}
			for _, index := range group.members {
				candidates[index].outputParts[group.componentIndex] = resolved
			}
		}
	}

	fileKeys := make(map[string]string, len(candidates))
	directoryOwners := make(map[string]string)
	for index := range candidates {
		outputPath := strings.Join(candidates[index].outputParts, "/")
		key := portableProviderPathKey(outputPath)
		if previous, exists := fileKeys[key]; exists {
			return fmt.Errorf("provider files %q and %q have the same portable output path", previous, outputPath)
		}
		fileKeys[key] = outputPath
		candidates[index].file.OutputPath = outputPath
	}
	for index := range candidates {
		for depth := 0; depth < len(candidates[index].outputParts)-1; depth++ {
			prefix := strings.Join(candidates[index].outputParts[:depth+1], "/")
			prefixKey := portableProviderPathKey(prefix)
			if file, conflict := fileKeys[prefixKey]; conflict {
				return fmt.Errorf("provider output directory %q conflicts with file %q", prefix, file)
			}
			providerKey := providerDirectoryIdentityKey(strings.Join(candidates[index].providerParts[:depth+1], "/"))
			if owner, exists := directoryOwners[prefixKey]; exists && owner != providerKey {
				return fmt.Errorf("provider directories %q and %q have the same portable output path %q", owner, providerKey, prefix)
			}
			directoryOwners[prefixKey] = providerKey
		}
	}
	return nil
}

func portableProviderOutputParts(fullPath, logicalName string) ([]string, error) {
	parts := strings.Split(fullPath, "/")
	output := make([]string, 0, len(parts))
	for _, component := range parts[:len(parts)-1] {
		portable, err := portableProviderFileName(component)
		if err != nil {
			return nil, fmt.Errorf("invalid output directory %q: %w", component, err)
		}
		output = append(output, portable)
	}
	output = append(output, logicalName)
	return output, nil
}

func disambiguateProviderDirectoryName(name, providerKey string, attempt int) (string, error) {
	extension := path.Ext(name)
	stem := strings.TrimSuffix(name, extension)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00directory\x00%d", providerKey, attempt)))
	disambiguated := stem + "~" + hex.EncodeToString(digest[:]) + extension
	return safepath.CompactIdentifier(disambiguated, safepath.PortableIdentifierMaxBytes)
}

func normalizeProviderFilePath(providerPath, fallbackName string) (string, error) {
	value := providerPath
	if value == "" {
		value = fallbackName
	}
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("path contains a NUL byte")
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("path contains a control character")
	}

	// Provider APIs commonly prefix torrent-internal paths with a slash. It is
	// a provider-root marker, not a host filesystem absolute path.
	value = strings.ReplaceAll(value, `\`, "/")
	value = strings.TrimLeft(value, "/")
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path traverses outside the provider release")
	}
	return clean, nil
}

func portableProviderPathKey(value string) string {
	parts := strings.Split(strings.ReplaceAll(value, `\`, "/"), "/")
	for i := range parts {
		parts[i] = strings.ToLower(norm.NFC.String(strings.TrimRight(parts[i], " .")))
	}
	return strings.Join(parts, "/")
}

func providerDirectoryIdentityKey(value string) string {
	return path.Clean(strings.ReplaceAll(value, `\`, "/"))
}
