package bitcask

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// Wead ...
const (
	Wrap = iota
	Fresh
	Ok
)

const (
	// 4 + 4 + 2 + 4
	// {}
	HeaderSize = 14
	// 4 + 2 + 4 + 8 = 18 byte
	// {timeStamp:keySize:valueOffset:key}
	HintHeaderSize = 18
)

// BFiles ...
type BFiles struct {
	bfs    map[int32]*BFile
	rwLock *sync.RWMutex
}

func (bfs *BFiles) get(fileID int32) *BFile {
	bfs.rwLock.RLock()
	defer bfs.rwLock.RUnlock()
	bf, _ := bfs.bfs[fileID]
	return bf
}

func (bfs *BFiles) put(bf *BFile, fileID int32) {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()
	bfs.bfs[fileID] = bf
}

// BFile 可写文件信息 1: datafile and hint file
type BFile struct {
	// fp is the writeable file
	fp          *os.File
	fileID      int32
	writeOffset int64
	// hintFp is the hint file
	hintFp *os.File
}

//
func newBFile() *BFile {
	return &BFile{}
}

func openBFile(dirName string, tStamp int) *BFile {
	fp, err := os.OpenFile(dirName+"/"+strconv.Itoa(tStamp)+".bitcask.data", os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}

	return &BFile{
		fileID:      int32(tStamp),
		fp:          fp,
		hintFp:      nil,
		writeOffset: -1,
	}
}

// 检测可写文件
func (bf *BFile) checkWrite(key []byte, value []byte, maxFileSize int64) int {
	if bf.fileID == -1 {
		return Fresh
	}

	size := HeaderSize + len(key) + len(value)

	if bf.writeOffset+int64(size) > maxFileSize {
		return Wrap
	}
	return Ok
}

func (bf *BFile) writeDatat(key []byte, value []byte) (entry, error) {
	// 1. write into datafile
	timeStamp := int32(time.Now().Unix())
	keySize := int32(len(key))
	valueSize := int32(len(value))

	vec := bf.fileEntry(key, value, timeStamp, keySize, valueSize)
	entrySize := HeaderSize + keySize + valueSize
	// TODO
	// race data
	entryPos := bf.writeOffset + int64(entrySize)

	// write data file into disk
	// TODO
	// assert WriteAt function
	bf.fp.WriteAt(vec, bf.writeOffset)
	bf.writeOffset += int64(entrySize)

	// write hint file disk
	hintData := bf.hintFileEntry(key, timeStamp, entryPos, entrySize)
	// TODO
	// assert write function
	bf.hintFp.Write(hintData)

	return entry{
		fileID:    bf.fileID,
		entryLen:  entrySize,
		offset:    entryPos,
		timeStamp: timeStamp,
	}, nil
}

// data File
func (bf *BFile) fileEntry(key []byte, value []byte, timeStamp int32,
	keySize int32, valueSize int32) []byte {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	crc32 := []byte("crc3")
	bString := fmt.Sprintf("%s%4d%4d%4d%s%s", crc32, timeStamp, keySize, valueSize, key, value)
	return []byte(bString)
}

func (bf *BFile) hintFileEntry(key []byte, tStamp int32,
	entryOffset int64, entrySize int32) []byte {
	/**
		tStamp	:	ksz	:	valueSz	:	valuePos	:	key
	**/
	return []byte(fmt.Sprintf("%4d%4d%4d%8d%s", tStamp, len(key), entrySize, entryOffset, key))
}
