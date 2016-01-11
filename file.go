package bitcask

import "os"

// Wead ...
const (
	Wead = iota
	Fresh
	Ok
)

const (
	// 4 + 4 + 2 + 4
	HeaderSize = 14
	// 4 + 2 + 4 + 8 = 18 byte
	// {timeStamp:keySize:valueOffset:key}
	HintHeaderSize = 18
)

// BFile ...
type BFile struct {
	fp          *os.File
	fileID      int
	fileName    string
	writeOffset int
}

func newBFile() *BFile {
	return &BFile{}
}
