package storage

import (
	"errors"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

var ErrIncompleteProviderFileScope = errors.New("provider snapshot does not contain the complete restricted file scope")

// ScopeTorrentSnapshot uses the same stable ID/path reconciliation as placement
// publication, on an isolated copy. Readiness must ignore other seasons while
// still requiring every scoped file. A partial snapshot cannot change the entry.
func (e *Entry) ScopeTorrentSnapshot(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	if e == nil || remote == nil || !e.FileScopeRestricted {
		return remote, nil
	}
	probe := ProtoToEntry(EntryToProto(e))
	names, err := probe.reconcileTorrentFiles(remote)
	if err != nil {
		return nil, err
	}
	scoped := remote.Copy()
	scoped.Files = make(map[string]debridTypes.File, len(names))
	scoped.Size, scoped.Bytes = 0, 0
	for key, file := range remote.Files {
		if names[file.Name] == "" {
			continue
		}
		scoped.Files[key] = file
		scoped.Size += file.Size
		scoped.Bytes += file.Size
	}
	scoped.Name, scoped.OriginalFilename = e.Name, e.OriginalFilename
	return scoped, nil
}
