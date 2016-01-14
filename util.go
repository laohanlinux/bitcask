package bitcask

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
}

func writePID(pidFp *os.File, fileID uint32) {
	pidFp.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+".data"), 0)
}

func lastFileID(files []*os.File) uint32 {
	if files == nil {
		return uint32(0)
	}
	lastFp := files[0]
	idxs := strings.Split(lastFp.Name(), ".")
	idx, _ := strconv.Atoi(idxs[0])
	lastID := idx
	for i := 0; i < len(files); i++ {
		lastFp := files[i]
		idxs = strings.Split(lastFp.Name(), ".")
		idx, _ = strconv.Atoi(idxs[0])
		if lastID < idx {
			lastID = idx
		}
	}
	return uint32(lastID)
}

func lockAcquire(fileName string, isWriteLock bool) *Lock {

	var fp *os.File
	var err error
	if isWriteLock {
		if fp, err = lockFile(fileName); err != nil {
			panic(err)
		}
	}

	return &Lock{
		Fp:          fp,
		IsWriteLock: isWriteLock,
	}
}

func setWriteableFile(fileID uint32, dirName string) (*os.File, uint32) {
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fp, err := os.Create(dirName + "/" + strconv.Itoa(int(fileID)) + ".data")
	if err != nil {
		panic(err)
	}
	return fp, fileID
}

func setHintFile(fileID uint32, dirName string) *os.File {
	fp, err := os.Create(dirName + "/" + strconv.Itoa(int(fileID)) + ".hint")
	if err != nil {
		panic(err)
	}
	return fp
}
