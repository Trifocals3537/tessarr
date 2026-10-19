package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

// NewTorrentOutputName is used only when admitting new torrent work. The
// readable prefix is bounded in bytes, and the 128-bit identity suffix prevents
// sanitization, truncation and case-folding from merging different torrents.
// Persist the result: later provider title changes must not move local output.
func NewTorrentOutputName(title, infoHash string) string {
	const maxLabelBytes = 96
	label := utils.RemoveExtension(strings.TrimSpace(title))
	var b strings.Builder
	for _, r := range label {
		if !(unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsMark(r) || strings.ContainsRune(" ._-", r)) {
			r = '-'
		}
		if b.Len()+utf8.RuneLen(r) > maxLabelBytes {
			break
		}
		b.WriteRune(r)
	}
	label = strings.Trim(b.String(), " .")
	if label == "" {
		label = "release"
	}
	id := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(infoHash))))
	// A fixed public prefix avoids device names and private ownership names,
	// including when the entire display title is punctuation or reserved text.
	return "torrent-" + label + "-" + hex.EncodeToString(id[:16])
}

// OutputComponent is the single source of truth for download/symlink/STRM
// output, not for provider or mount display names. New torrent and oversized
// NZB records pin a safe component in OutputName. Missing output_name means a
// legacy record: keep its exact old component, including leading whitespace.
// Callers performing filesystem operations must still validate this component
// and prove ownership; this method does not grant cleanup authority.
func (e *Entry) OutputComponent() string {
	if e.OutputName != "" {
		return e.OutputName
	}
	return utils.RemoveExtension(e.Name)
}

// TorrentPathFirstComponentIsEntryRoot identifies only the entry's known
// provider release root. Output planning and materialization must use the same
// test or a later provider refresh can reserve a different file namespace.
func (e *Entry) TorrentPathFirstComponentIsEntryRoot(component string) bool {
	if e == nil {
		return false
	}
	component = strings.TrimSpace(component)
	for _, candidate := range []string{
		e.Name, e.OriginalFilename,
		utils.RemoveExtension(e.Name), utils.RemoveExtension(e.OriginalFilename),
	} {
		candidate = strings.TrimSpace(strings.ReplaceAll(candidate, `\`, "/"))
		if candidate != "" && !strings.Contains(candidate, "/") && strings.EqualFold(component, candidate) {
			return true
		}
	}
	return false
}

// PreserveTorrentOutputPath retains an existing local output identity while
// merging a fresh queue/provider representation. No directory is moved and no
// new identity is inferred for provider-only entries without a SavePath.
func PreserveTorrentOutputPath(existing, incoming *Entry) {
	if existing == nil || incoming == nil || !existing.IsTorrent() || !incoming.IsTorrent() || existing.SavePath == "" {
		return
	}
	if existing.OutputName != "" || incoming.OutputName != "" || existing.Name != incoming.Name {
		incoming.OutputName = existing.OutputComponent()
	}
	incoming.SavePath = existing.SavePath
	incoming.ContentPath = existing.ContentPath
}
