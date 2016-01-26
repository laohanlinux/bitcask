package bitcask

import (
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	// HeaderSize : 4 + 4 + 4 + 4
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	HeaderSize = 16
	// HintHeaderSize : 4 + 4 + 4 + 8 = 20 byte
	/**
	tstamp	:	ksz	:	valuesz	：	valuePos	:	key
		4	:	4	:	4		:		8		:	xxxx
	*/
	HintHeaderSize = 20
)

// BFiles ...
type BFiles struct {
	bfs    map[uint32]*BFile
	rwLock *sync.RWMutex
}

func newBFiles() *BFiles {
	return &BFiles{
		bfs:    make(map[uint32]*BFile),
		rwLock: &sync.RWMutex{},
	}
}

func (bfs *BFiles) get(fileID uint32) *BFile {
	bfs.rwLock.RLock()
	defer bfs.rwLock.RUnlock()
	bf, _ := bfs.bfs[fileID]
	return bf
}

func (bfs *BFiles) put(bf *BFile, fileID uint32) {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()
	bfs.bfs[fileID] = bf
}

func (bfs *BFiles) close() {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()
	for _, bf := range bfs.bfs {
		bf.fp.Close()
		bf.hintFp.Close()
	}
}

// BFile 可写文件信息 1: datafile and hint file
type BFile struct {
	// fp is the writeable file
	fp          *os.File
	fileID      uint32
	writeOffset uint64
	// hintFp is the hint file
	hintFp *os.File
}

//
func newBFile() *BFile {
	return &BFile{}
}

func openBFile(dirName string, tStamp int) (*BFile, error) {
	fp, err := os.OpenFile(dirName+"/"+strconv.Itoa(tStamp)+".data", os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &BFile{
		fileID:      uint32(tStamp),
		fp:          fp,
		hintFp:      nil,
		writeOffset: 0,
	}, nil
}

func (bf *BFile) read(offset uint64, length uint32) ([]byte, error) {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4	:		 4	:		4:		4	:	xxxx	: xxxx
	**/
	value := make([]byte, length)
	//TODO
	// assert read function and crc32
	bf.fp.Seek(int64(offset), 0)
	_, err := bf.fp.Read(value)
	if err != nil {
		return nil, err
	}
	return value, err
}

// including writing data file and hint file
func (bf *BFile) writeDatat(key []byte, value []byte) (entry, error) {
	// 1. write into datafile
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	vec := encodeEntry(timeStamp, keySize, valueSize, key, value)
	entrySize := HeaderSize + keySize + valueSize

	valueOffset := bf.writeOffset + uint64(HeaderSize+keySize)
	// write data file into disk
	// TODO
	// assert WriteAt function
	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into data file:", n)

	// 2. write hint file disk
	hintData := encodeHint(timeStamp, keySize, valueSize, valueOffset, key)
	// TODO
	// assert write function
	_, err = appendWriteFile(bf.hintFp, hintData)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into hint file:", n)
	bf.writeOffset += uint64(entrySize)

	return entry{
		fileID:      bf.fileID,
		valueSz:     valueSize,
		valueOffset: valueOffset,
		timeStamp:   timeStamp,
	}, nil
}

func (bf *BFile) del(key []byte) error {
	// 1. write into datafile
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(0)
	valueSize := uint32(0)
	vec := encodeEntry(timeStamp, keySize, valueSize, key, nil)
	//logger.Info(len(vec), keySize, valueSize)
	entrySize := HeaderSize + keySize + valueSize
	// TODO
	// race data
	valueOffset := bf.writeOffset + uint64(HeaderSize+keySize)
	// write data file into disk
	// TODO
	// assert WriteAt function
	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
	}

	//logger.Debug("has write into data file:", n)
	// 2. write hint file disk
	hintData := encodeHint(timeStamp, keySize, valueSize, valueOffset, key)

	// TODO
	// assert write function
	_, err = appendWriteFile(bf.hintFp, hintData)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into hint file:", n)
	bf.writeOffset += uint64(entrySize)

	return nil
}
