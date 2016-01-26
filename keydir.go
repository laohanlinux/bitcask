package bitcask

import "sync"

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
	keyDirs.entrys[key] = e
}

func (keyDirs *KeyDirs) setCompare(key string, e *entry) bool {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()
	old, ok := keyDirs.entrys[key]
	//println("old:", old.toString())
	//println("new:", e.toString())
	if !ok || e.isNewerThan1(old) {
		// logger.Info("update data:", key, e.fileID, e.timeStamp)
		keyDirs.entrys[key] = e
		return true
	}
	// logger.Error("update fail:", key, e.fileID, e.timeStamp)
	return false
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
