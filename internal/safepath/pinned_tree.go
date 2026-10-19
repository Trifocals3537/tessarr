package safepath

import (
	"fmt"
	"io"
	"os"
)

// PinnedTreeRemovalOptions bounds a removal walk performed through an
// already-open os.Root. PreserveTopLevel leaves named direct children in
// place so callers can retain an ownership marker until the parent identity
// has been rechecked.
type PinnedTreeRemovalOptions struct {
	MaxEntries       int
	MaxDepth         int
	ReadBatch        int
	PreserveTopLevel []string
}

// RemovePinnedTreeContents removes children only through the supplied,
// already-open root. Directories are opened and pinned before their contents
// are traversed, and each directory name is identity-checked before the final
// non-recursive unlink. It never removes the root itself.
func RemovePinnedTreeContents(root *os.Root, options PinnedTreeRemovalOptions) error {
	if root == nil {
		return fmt.Errorf("pinned tree root is nil")
	}
	if options.MaxEntries <= 0 {
		return fmt.Errorf("pinned tree maximum entries must be positive")
	}
	if options.MaxDepth < 0 {
		return fmt.Errorf("pinned tree maximum depth must be non-negative")
	}
	if options.ReadBatch <= 0 {
		return fmt.Errorf("pinned tree read batch must be positive")
	}

	preserved := make(map[string]struct{}, len(options.PreserveTopLevel))
	for _, name := range options.PreserveTopLevel {
		if err := ValidateIdentifier(name); err != nil {
			return fmt.Errorf("invalid preserved top-level name %q: %w", name, err)
		}
		preserved[name] = struct{}{}
	}
	state := pinnedTreeRemovalState{
		options:   options,
		preserved: preserved,
	}
	return state.removeContents(root, 0)
}

type pinnedTreeRemovalState struct {
	options   PinnedTreeRemovalOptions
	preserved map[string]struct{}
	seen      int
}

func (state *pinnedTreeRemovalState) removeContents(root *os.Root, depth int) error {
	if depth > state.options.MaxDepth {
		return fmt.Errorf("pinned tree exceeds maximum depth %d", state.options.MaxDepth)
	}

	for {
		directory, err := root.Open(".")
		if err != nil {
			return fmt.Errorf("open pinned tree directory: %w", err)
		}
		removedAny := false
		for {
			entries, readErr := directory.ReadDir(state.options.ReadBatch)
			for _, entry := range entries {
				state.seen++
				if state.seen > state.options.MaxEntries {
					_ = directory.Close()
					return fmt.Errorf("pinned tree exceeds maximum entry observations %d", state.options.MaxEntries)
				}
				if depth == 0 {
					if _, keep := state.preserved[entry.Name()]; keep {
						continue
					}
				}
				if err := state.removeEntry(root, entry.Name(), depth); err != nil {
					_ = directory.Close()
					return err
				}
				removedAny = true
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = directory.Close()
				return fmt.Errorf("read pinned tree directory: %w", readErr)
			}
		}
		if err := directory.Close(); err != nil {
			return fmt.Errorf("close pinned tree directory: %w", err)
		}
		if !removedAny {
			return nil
		}
	}
}

func (state *pinnedTreeRemovalState) removeEntry(root *os.Root, name string, depth int) error {
	before, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect pinned tree entry %q: %w", name, err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove pinned tree entry %q: %w", name, err)
		}
		return nil
	}

	child, err := root.OpenRoot(name)
	if err != nil {
		return fmt.Errorf("open pinned tree child %q: %w", name, err)
	}
	pinned, statErr := child.Stat(".")
	if statErr != nil || !os.SameFile(before, pinned) {
		_ = child.Close()
		if statErr != nil {
			return fmt.Errorf("stat pinned tree child %q: %w", name, statErr)
		}
		return fmt.Errorf("pinned tree child %q changed while opening", name)
	}
	if err := state.removeContents(child, depth+1); err != nil {
		_ = child.Close()
		return err
	}
	if err := child.Close(); err != nil {
		return fmt.Errorf("close pinned tree child %q: %w", name, err)
	}

	after, err := root.Lstat(name)
	if err != nil || !os.SameFile(pinned, after) {
		if err != nil {
			return fmt.Errorf("reinspect emptied pinned tree child %q: %w", name, err)
		}
		return fmt.Errorf("pinned tree child %q changed before unlink", name)
	}
	if err := root.Remove(name); err != nil {
		return fmt.Errorf("remove emptied pinned tree child %q: %w", name, err)
	}
	return nil
}
