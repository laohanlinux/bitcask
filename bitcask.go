package bitcask

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/laohanlinux/go-logger/logger"
)

// ErrNotFound ...
var (
	ErrNotFound = fmt.Errorf("Not Found.")
	ErrIsNotDir = fmt.Errorf("the file is not dir")
)

// Open ...
func Open(dirName string, opts *Options) (*BitCask, error) {
	if opts == nil {
		opts1 := NewOptions(0, 0, -1, true)
		opts = &opts1
	}

	//make sure the fileName is exits
	_, err := os.Stat(dirName)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if os.IsNotExist(err) {
		err = os.Mkdir(dirName, 0777)
		if err != nil {
			return nil, err
		}
	}

	b := &BitCask{}
	b.dirFile = dirName
	b.ActiveFile = newBFiles()

	// 锁住进程ID到锁文件中
	b.lockFile, err = lockFile(dirName + "/" + lockFileName)
	if err != nil {
		return nil, err
	}
	// 暂时不知道有什么鬼用
	b.keyDirs = NewKeyDir(dirName)
	//　获取可读文件
	files, _ := b.readableFiles()
	// 检索可读文件
	b.scanKeyFiles(files)
	// 获取最新fileID
	fileID := lastFileID(files)
	var writeFp *os.File
	var hintFp *os.File
	if fileID == uint32(0) {
		// new create data file
		writeFp, fileID = setWriteableFile(fileID, dirName)
		// new Hint data file
		hintFp = setHintFile(fileID, dirName)
	}
	// 设置writeable文件
	bf := &BFile{
		fp:          writeFp,
		fileID:      fileID,
		writeOffset: 0,
		hintFp:      hintFp,
	}
	b.writeFile = bf
	fmt.Println(b.writeFile)
	// 把进程ID写入锁文件
	writePID(b.lockFile, fileID)
	return b, nil
}

// BitCask ...
type BitCask struct {
	MaxFileSize uint64 // single file maxsize
	Opts        *Options
	ActiveFile  *BFiles // data file
	lockFile    *os.File
	keyDirs     *KeyDirs
	dirFile     string // bitcask storage  root dir
	writeFile   *BFile // writeable file
}

// Close ...
func (bc *BitCask) Close() {
}

// Put ...
func (bc *BitCask) Put(key []byte, value []byte) {
	if bc.writeFile == nil {
		panic("read only")
	}
	switch bc.writeFile.checkWrite(key, value, bc.MaxFileSize) {
	case Wrap:
		e, err := bc.writeFile.writeDatat(key, value)
		if err != nil {
			panic(err)
		}
		// must to put key/value into keyDirs(HashMap)
		keyDirs.put(string(key), &e)
	case Fresh:
		// time to start our first write file
	case Ok:
	}
}

// Get ...
func (bc *BitCask) Get(key []byte) ([]byte, error) {
	e := keyDirs.get(string(key))
	if e == nil {
		return nil, ErrNotFound
	}

	// get the value from data file
	fileID := e.fileID
	bf := bc.getFileState(fileID)
	if bf == nil {
		panic(bf)
	}

	logger.Info("entry offset:", e.offset, "\t entryLen:", e.entryLen)
	return bf.read(e.offset, e.entryLen)
}

// return readable file: xxxx.data, yyyyy.data
func (bc *BitCask) readableFiles() ([]*os.File, error) {
	ldfs, err := bc.listDataFiles()
	if err != nil {
		return nil, err
	}

	fps := make([]*os.File, len(ldfs))
	for idx, filePath := range ldfs {
		fp, err := os.Open(filePath)
		if err != nil {
			return nil, err
		}
		fps[idx] = fp
	}
	return fps, nil
}

func (bc *BitCask) listDataFiles() ([]string, error) {
	dirFp, err := os.OpenFile(bc.dirFile, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()
	return dirFp.Readdirnames(-1)
}

//
func (bc *BitCask) scanKeyFiles(files []*os.File) {
	for _, file := range files {
		fileName := file.Name()
		hintName := fileName[0:strings.Index(fileName, "data")] + ".hint"
		// 检索ｈｉｎｔ文件
		bc.parseHint(hintName)
	}
}

func (bc *BitCask) getFileState(fileID uint32) *BFile {
	bf := bc.ActiveFile.get(fileID)
	if bf != nil {
		return bf
	}

	bf = openBFile(bc.dirFile, int(fileID))
	bc.ActiveFile.put(bf, fileID)
	return bf
}

func (bc *BitCask) parseHint(hintName string) {
	fp, err := os.Open(hintName)
	if err != nil {
		panic(err)
	}
	defer fp.Close()

	b := make([]byte, HintHeaderSize, HintHeaderSize+8)
	offset := int64(0)
	fileID, _ := strconv.ParseInt(hintName[:strings.Index(hintName, ".hint")], 10, 32)

	for {
		n, err := fp.ReadAt(b, offset)
		offset += int64(n)
		if err != nil && err != io.EOF {
			panic(err)
		}

		if err == io.EOF {
			break
		}

		if n != HeaderSize {
			panic(n)
		}

		tStamp, ksz, valueSz, valuePos := decodeHint(b)
		keyByte := make([]byte, ksz)
		fp.ReadAt(keyByte, offset)
		key := string(keyByte)
		e := &entry{
			fileID:    uint32(fileID),
			entryLen:  valueSz,
			offset:    valuePos,
			timeStamp: tStamp,
		}
		// put entry into keyDirs
		keyDirs.put(key, e)
	}
}
