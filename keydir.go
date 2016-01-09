package bitcask

import (
	"os"
	"sync"
)

// KeyDirs for HashMap
var KeyDirs map[*os.File]KeyDir
var keyDirsLock *sync.Mutex

func init() {
	KeyDirs = make(map[*os.File]KeyDir)
	keyDirsLock = &sync.Mutex{}
}

// KeyDir ...
type KeyDir struct {
	//keyDirs map[*BFile]*KeyDir
}

// NewKeyDir return a KeyDir Obj
func NewKeyDir(fp *os.File, timeoutSecs int) {
	//filepath.Abs(fp.Name())
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

}
