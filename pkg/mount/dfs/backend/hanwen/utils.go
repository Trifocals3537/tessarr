//go:build linux || (darwin && amd64)

package hanwen

import (
	"hash"
	"hash/fnv"
	"sync"
)

var hasherPool = sync.Pool{
	New: func() interface{} {
		return fnv.New64a()
	},
}

func hashPath(path string) uint64 {
	h := hasherPool.Get().(hash.Hash64)
	defer hasherPool.Put(h)

	h.Reset()
	_, _ = h.Write([]byte(path))
	hs := h.Sum64()
	if hs <= 1 {
		hs = 2
	}
	return hs
}

// hashIdentity hashes a tuple without making concatenation ambiguous. It is
// used for inode generations, where the same path can later refer to a new
// durable file identity.
func hashIdentity(parts ...string) uint64 {
	h := hasherPool.Get().(hash.Hash64)
	defer hasherPool.Put(h)

	h.Reset()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	hs := h.Sum64()
	if hs <= 1 {
		hs = 2
	}
	return hs
}
