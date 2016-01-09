package bitcask

import "os"

const (
	// StaleOk ...
	StaleOk = iota
	// StaleNot ..
	StaleNot
)

// operation command
const (
	// Write ..
	Write = iota
	// Merge ..
	Merge
)

const (
	lockFileName = "bitcask.lock"
)

// Lock ...
type Lock struct {
	Stale       int
	Type        int
	IsWriteLock bool
	Fp          *os.File
}
