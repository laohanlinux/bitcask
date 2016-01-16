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

// put a key with value into bitcask
func (keyDirs *KeyDirs) put(key string, e *entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	old, ok := keyDirs.entrys[key]
	if !ok || e.isNewerThan(old) {
		keyDirs.entrys[key] = e
		return
	}

	keyDirs.entrys[key] = old
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
