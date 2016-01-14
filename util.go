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

func lastFileInfo(files []*os.File) (uint32, *os.File) {
	if files == nil {
		return uint32(0), nil
	}
	lastFp := files[0]

	fileName := lastFp.Name()
	s := strings.LastIndex(fileName, "/") + 1
	e := strings.LastIndex(fileName, ".hint")
	idx, _ := strconv.Atoi(fileName[s:e])
	lastID := idx
	for i := 0; i < len(files); i++ {
		idxFp := files[i]
		fileName = lastFp.Name()
		s = strings.LastIndex(fileName, "/") + 1
		e = strings.LastIndex(fileName, ".hint")
		idx, _ = strconv.Atoi(fileName[s:e])
		if lastID < idx {
			lastID = idx
			lastFp = idxFp
		}
	}
	return uint32(lastID), lastFp
}

func closeReadHintFp(files []*os.File, fileID uint32) {
	for _, fp := range files {
		if !strings.Contains(fp.Name(), strconv.Itoa(int(fileID))) {
			fp.Close()
		}
	}
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
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + ".data"
	fp, err = os.OpenFile(fileName, os.O_APPEND|os.O_EXCL, 0755)
	if err != nil {
		panic(err)
	}
	return fp, fileID
}

func setHintFile(fileID uint32, dirName string) *os.File {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + ".hint"
	fp, err = os.OpenFile(fileName, os.O_APPEND|os.O_EXCL, 0755)
	if err != nil {
		panic(err)
	}
	return fp
}
