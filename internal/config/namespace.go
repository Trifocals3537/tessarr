package config

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// Built-in names in the root virtual filesystem. Provider instance names and
// custom folders share this namespace, while UsenetProviderName is also the
// persisted provider key for NZB entries.
const (
	MountAllFolderName     = "__all__"
	MountBadFolderName     = "__bad__"
	MountTorrentFolderName = "torrents"
	MountNZBFolderName     = "nzbs"
	MountVersionFileName   = "version.txt"
	UsenetProviderName     = "usenet"
)

func validateMountNamespace(
	debrids []Debrid,
	customFolders map[string]CustomFolders,
) error {
	claimed := map[string]string{}
	for _, reserved := range []string{
		MountAllFolderName,
		MountBadFolderName,
		MountTorrentFolderName,
		MountNZBFolderName,
		MountVersionFileName,
		UsenetProviderName,
	} {
		key, err := safepath.PortableNameKey(reserved)
		if err != nil {
			return fmt.Errorf("validate built-in namespace %q: %w", reserved, err)
		}
		claimed[key] = fmt.Sprintf("built-in name %q", reserved)
	}

	claim := func(kind, name string) error {
		if name != strings.TrimSpace(name) {
			return fmt.Errorf("%s name %q has leading or trailing whitespace", kind, name)
		}
		key, err := safepath.PortableNameKey(name)
		if err != nil {
			return fmt.Errorf("%s name %q is not portable: %w", kind, name, err)
		}
		if previous, exists := claimed[key]; exists {
			return fmt.Errorf("%s name %q collides with %s", kind, name, previous)
		}
		claimed[key] = fmt.Sprintf("%s %q", kind, name)
		return nil
	}

	for _, debrid := range debrids {
		name := strings.TrimSpace(debrid.Name)
		if name == "" {
			name = strings.ToLower(strings.TrimSpace(debrid.Provider))
		}
		if err := claim("debrid provider", name); err != nil {
			return err
		}
	}

	customNames := make([]string, 0, len(customFolders))
	for name := range customFolders {
		customNames = append(customNames, name)
	}
	sort.Strings(customNames)
	for _, name := range customNames {
		if err := claim("custom folder", name); err != nil {
			return err
		}
	}
	return nil
}

func normalizeArrDebridSelections(arrs []Arr, debrids []Debrid) {
	for i := range arrs {
		selected := strings.TrimSpace(arrs[i].SelectedDebrid)
		if selected == "" {
			arrs[i].SelectedDebrid = ""
			continue
		}
		arrs[i].SelectedDebrid = selected
		for _, debrid := range debrids {
			if strings.EqualFold(selected, debrid.Name) {
				arrs[i].SelectedDebrid = debrid.Name
				break
			}
		}
	}
}

func validateArrDebridSelections(arrs []Arr, debrids []Debrid) error {
	configured := make(map[string]struct{}, len(debrids))
	for _, debrid := range debrids {
		configured[debrid.Name] = struct{}{}
	}
	for _, arr := range arrs {
		if arr.SelectedDebrid == "" {
			continue
		}
		if _, ok := configured[arr.SelectedDebrid]; !ok {
			return fmt.Errorf(
				"arr %q selects unknown debrid provider %q",
				arr.Name,
				arr.SelectedDebrid,
			)
		}
	}
	return nil
}
