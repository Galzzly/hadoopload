package magic

import "bytes"

type (
	Detector func(raw []byte, limit uint32) bool
)

var (
	// Gz matches the gzip file format
	Gz = prefix([]byte{0x1f, 0x8b})
)

func prefix(sigs ...[]byte) Detector {
	return func(raw []byte, limit uint32) bool {
		for _, sig := range sigs {
			if bytes.HasPrefix(raw, sig) {
				return true
			}
		}
		return false
	}
}
