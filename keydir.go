package bitcask

import (
	"sync"

	"github.com/laohanlinux/go-logger/logger"
)

// KeyDirs for HashMap
var keyDirsLock *sync.RWMutex

var keyDirs *KeyDirs
var keyDirsOnce sync.Once

func init() {
	keyDirsLock = &sync.RWMutex{}
}

// KeyDirs ...
type KeyDirs struct {
	entrys map[string]*entry
}

// NewKeyDir return a KeyDir Obj
func NewKeyDir(dirName string) *KeyDirs {
	//filepath.Abs(fp.Name())
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	keyDirsOnce.Do(func() {
		if keyDirs == nil {
			keyDirs = &KeyDirs{
				entrys: make(map[string]*entry),
			}
		}
	})
	return keyDirs
}
func (keyDirs *KeyDirs) get(key string) *entry {
	keyDirsLock.RLock()
	defer keyDirsLock.RUnlock()
	e, _ := keyDirs.entrys[key]
	return e
}

func (keyDirs *KeyDirs) del(key string) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	delete(keyDirs.entrys, key)
}

// put a key with value into bitcask
func (keyDirs *KeyDirs) put(key string, e *entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	if key == "53" {
		logger.Info("53准备加入key/value:", e.fileID)
	}
	keyDirs.entrys[key] = e
	/*
		old, ok := keyDirs.entrys[key]
		if !ok || e.isNewerThan(old) {
			keyDirs.entrys[key] = e
			return
		}
		if key != "53" {
			return
		}
		logger.Error(key, "被拒绝加入，因为它too old")
		fmt.Printf("e:%d, %d\n", e.fileID, e.timeStamp)
		fmt.Printf("old:%d, %d\n", old.fileID, old.timeStamp)
		//keyDirs.entrys[key] = old
	*/
}

// put a key for merging operation
func (keyDirs *KeyDirs) putMerge(key string, e *entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	old, ok := keyDirs.entrys[key]
	// if not exists, mybey in merging, someone delete it as time
	if !ok {
		return
	}
	if e.isNewerThan1(old) {
		keyDirs.entrys[key] = e
	}
}

func (keyDirs *KeyDirs) setCompare(key string, e *entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	old, ok := keyDirs.entrys[key]
	if !ok || e.isNewerThan(old) {
		keyDirs.entrys[key] = e
	}
}

/***
用于合并时候进行的比较使用
1.如果old在合并的文件中，那么直接直接更替掉
2.如果old不再合并的文件中，说明old是新增的内容，合并的内容都比它久

其中mergelist 为本次合并的文件列表
**/
func (keyDirs *KeyDirs) setMerge(key string, e *entry, mergeList []uint32) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	in := false
	old, ok := keyDirs.entrys[key]
	if !ok {
		keyDirs.entrys[key] = e
		return
	}

	for _, v := range mergeList {
		if v == old.fileID {
			in = true
			break
		}
	}
	// if in merging process
	if in {
		keyDirs.entrys[key] = e
		return
	}

	keyDirs.entrys[key] = e
}

func (keyDirs *KeyDirs) updateFileID(oldID, newID uint32) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	for _, e := range keyDirs.entrys {
		if e.fileID == oldID {
			e.fileID = newID
		}
	}
}
