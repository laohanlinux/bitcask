package bitcask

import (
	"os"
)

// Open ...
func Open(dirName string, opts *Options) (*BitCask, error) {
	if opts == nil {
		opts1 := NewOptions(0, 0, -1, true)
		opts = &opts1
	}

	//make sure the fileName is exits
	err := os.Mkdir(dirName, os.ModeDir)
	if err != nil {
		return nil, err
	}

	b := &BitCask{}
	b.DirName = dirName

	// Get The Active File
	b.lockFile, err = lockFile(dirName + "/" + lockFileName)
	if err != nil {
		return nil, err
	}

	// Scan KeyDir
	return nil, nil
}

// BitCask ...
type BitCask struct {
	DirName     string
	MaxFileSize int
	Opts        *Options
	ActiveFile  *os.File
	lockFile    *os.File
}
