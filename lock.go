package bitcask

import (
	"io/ioutil"
	"os"
)

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

// Lock is the file lock
type Lock struct {
	Stale       int
	Type        int
	IsWriteLock bool
	Fp          *os.File
}

func (l *Lock) readLockData() string {
	l.Fp.Seek(0, 0)
	b, err := ioutil.ReadAll(l.Fp)
	if err != nil {
		panic(err)
	}
	return string(b)
}
