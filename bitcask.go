package bitcask

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

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
	b.rwLock = &sync.RWMutex{}

	// 锁住进程ID到锁文件中
	b.lockFile, err = lockFile(dirName + "/" + lockFileName)
	if err != nil {
		return nil, err
	}

	b.keyDirs = NewKeyDir(dirName)
	//　获取可读文件
	files, _ := b.readableFiles()
	logger.Info(files, len(files), cap(files))
	// 检索可读文件
	b.parseHint(files)
	// 获取最新fileID
	fileID, hintFp := lastFileInfo(files)
	logger.Info("最新的fileid:", fileID)
	var writeFp *os.File
	writeFp, fileID = setWriteableFile(fileID, dirName)
	hintFp = setHintFile(fileID, dirName)
	// close other hint
	closeReadHintFp(files, fileID)
	// 设置writeable文件
	dataStat, _ := writeFp.Stat()
	logger.Info("writeoffset:", dataStat.Size())
	bf := &BFile{
		fp:          writeFp,
		fileID:      fileID,
		writeOffset: uint64(dataStat.Size()),
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
	ActiveFile  *BFiles // hint file, data file
	lockFile    *os.File
	keyDirs     *KeyDirs
	dirFile     string // bitcask storage  root dir
	writeFile   *BFile // writeable file
	rwLock      *sync.RWMutex
}

// Close opening fp
func (bc *BitCask) Close() {
	// close ActiveFiles
	bc.ActiveFile.close()
	// close writeable file
	bc.writeFile.fp.Close()
	bc.writeFile.hintFp.Close()
	// close lockFile
	bc.lockFile.Close()
	// delete lockFile
	os.Remove(bc.dirFile + "/" + lockFileName)
}

// Put key/value
func (bc *BitCask) Put(key []byte, value []byte) {
	bc.rwLock.Lock()
	defer bc.rwLock.Unlock()
	if bc.writeFile == nil {
		panic("read only")
	}
	bc.writeFile.writeDatat(key, value)
	if bc.writeFile.writeOffset > bc.MaxFileSize {
		//close data/hint fp
		bc.writeFile.hintFp.Close()
		bc.writeFile.fp.Close()

		writeFp, fileID := setWriteableFile(0, bc.dirFile)
		hintFp := setHintFile(fileID, bc.dirFile)
		bf := &BFile{
			fp:          writeFp,
			fileID:      fileID,
			writeOffset: 0,
			hintFp:      hintFp,
		}
		bc.writeFile = bf
		// 把进程ID写入锁文件
		writePID(bc.lockFile, fileID)
	}
}

// Get ...
func (bc *BitCask) Get(key []byte) ([]byte, error) {
	bc.rwLock.RLock()
	defer bc.rwLock.RUnlock()
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

// return readable file: xxxx.hint
func (bc *BitCask) readableFiles() ([]*os.File, error) {
	ldfs, err := bc.listHintFiles()
	//logger.Info(ldfs)
	if err != nil {
		return nil, err
	}

	fps := make([]*os.File, 0, len(ldfs))
	for _, filePath := range ldfs {
		if filePath == lockFileName {
			continue
		}
		logger.Info(filePath)
		fp, err := os.OpenFile(bc.dirFile+"/"+filePath, os.O_RDONLY, 0755)
		if err != nil {
			return nil, err
		}
		fps = append(fps, fp)
	}
	if len(fps) == 0 {
		return nil, nil
	}
	return fps, nil
}

func (bc *BitCask) listHintFiles() ([]string, error) {
	dirFp, err := os.OpenFile(bc.dirFile, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()
	//
	lists, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	var hintLists []string
	for _, v := range lists {
		if strings.Contains(v, "hint") {
			hintLists = append(hintLists, v)
		}
	}
	return hintLists, nil
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

func (bc *BitCask) parseHint(hintFps []*os.File) {

	b := make([]byte, HintHeaderSize, HintHeaderSize)
	for _, fp := range hintFps {
		offset := int64(0)
		hintName := fp.Name()
		s := strings.LastIndex(hintName, "/") + 1
		e := strings.LastIndex(hintName, ".hint")
		fileID, _ := strconv.ParseInt(hintName[s:e], 10, 32)

		for {
			n, err := fp.ReadAt(b, offset)
			offset += int64(n)
			if err != nil && err != io.EOF {
				panic(err)
			}
			//time.Sleep(time.Second * 3)
			logger.Info("n:", n)
			if err == io.EOF {
				break
			}

			if n != HintHeaderSize {
				panic(n)
			}

			tStamp, ksz, valueSz, valuePos := decodeHint(b)
			logger.Info("ksz:", ksz, "offset:", offset)
			keyByte := make([]byte, ksz)
			fp.ReadAt(keyByte, offset)
			key := string(keyByte)
			e := &entry{
				fileID:    uint32(fileID),
				entryLen:  valueSz,
				offset:    valuePos,
				timeStamp: tStamp,
			}
			offset += int64(ksz)
			// put entry into keyDirs
			keyDirs.put(key, e)
		}
	}
}
