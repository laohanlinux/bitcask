package bitcask

import (
	"io/ioutil"
	"os"
	"strconv"
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

// return readable file data director list 
func readActiveFile(Stale int, fileName string) *os.File {
	fp, err := lockFile(fileName)
	if err != nil {
		panic(err)
	}

	//获取文件锁，如果该文件已被锁，则失败
	lock := lockAcquire(fileName, false)
	// 获取锁住的内容
	lock.writeData([]byte(strconv.Itoa(os.Getgid())))
}

// 返回文件的内容
func (l *Lock) readLockData() string {
	l.Fp.Seek(0, 0)
	b, err := ioutil.ReadAll(l.Fp)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// 覆盖文件
func (l *Lock) writeData(b []byte) {
	_, err := l.Fp.WriteAt(b, 0)
	if err != nil {
		panic(err)
	}
}

func scanKeyFiles([]*os.File, keyDir *KeyDir){

}
