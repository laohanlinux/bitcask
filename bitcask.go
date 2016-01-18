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
		opts1 := NewOptions(0, 0, -1, 60, true)
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

	b := &BitCask{
		Opts:       opts,
		dirFile:    dirName,
		ActiveFile: newBFiles(),
		rwLock:     &sync.RWMutex{},
	}
	// lock file
	b.lockFile, err = lockFile(dirName + "/" + lockFileName)
	if err != nil {
		return nil, err
	}

	b.keyDirs = NewKeyDir(dirName)
	// scan readAble file
	files, _ := b.readableFiles()
	b.parseHint(files)
	// get the last fileid
	fileID, hintFp := lastFileInfo(files)

	var writeFp *os.File
	writeFp, fileID = setWriteableFile(fileID, dirName)

	hintFp = setHintFile(fileID, dirName)
	// close other hint
	closeReadHintFp(files, fileID)
	// setting writeable file, only one
	dataStat, _ := writeFp.Stat()
	bf := &BFile{
		fp:          writeFp,
		fileID:      fileID,
		writeOffset: uint64(dataStat.Size()),
		hintFp:      hintFp,
	}
	b.writeFile = bf
	// save pid into bitcask.lock file
	writePID(b.lockFile, fileID)
	return b, nil
}

// BitCask ...
type BitCask struct {
	Opts       *Options      // opts for bitcask
	ActiveFile *BFiles       // hint file, data file
	lockFile   *os.File      // lock file with process
	keyDirs    *KeyDirs      // key/value hashMap, building with hint file
	dirFile    string        // bitcask storage  root dir
	writeFile  *BFile        // writeable file
	rwLock     *sync.RWMutex // rwlocker for bitcask Get and put Operation
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
func (bc *BitCask) Put(key []byte, value []byte) error {
	bc.rwLock.Lock()
	defer bc.rwLock.Unlock()
	if bc.writeFile == nil {
		return fmt.Errorf("Can Not Read The Bitcask Root Director")
	}

	checkWriteableFile(bc)
	// write data into writeable file
	e, err := bc.writeFile.writeDatat(key, value)
	if err != nil {
		return err
	}
	// add key/value into keydirs
	keyDirs.put(string(key), &e)
	return nil
}

// Get ...
func (bc *BitCask) Get(key []byte) ([]byte, error) {
	bc.rwLock.RLock()
	defer bc.rwLock.RUnlock()
	e := keyDirs.get(string(key))
	if e == nil {
		return nil, ErrNotFound
	}

	fileID := e.fileID
	logger.Debug("Get FileID:", fileID)
	bf := bc.getFileState(fileID)
	if bf == nil {
		panic(bf)
	}

	//logger.Info("fileID", fileID, "entry offset:", e.offset, "\t entryLen:", e.entryLen)
	return bf.read(e.offset, e.entryLen)
}

// Del value by key
func (bc *BitCask) Del(key []byte) error {
	bc.rwLock.Lock()
	defer bc.rwLock.Unlock()
	if bc.writeFile == nil {
		return fmt.Errorf("Can Not Read The Bitcask Root Director")
	}

	e := keyDirs.get(string(key))
	if e == nil {
		return ErrNotFound
	}

	checkWriteableFile(bc)
	// write data into writeable file
	err := bc.writeFile.del(key)
	if err != nil {
		return err
	}
	// delete key/value from keydirs
	keyDirs.del(string(key))
	return nil
}

// return readable hint file: xxxx.hint
func (bc *BitCask) readableFiles() ([]*os.File, error) {
	filterFiles := []string{mergeDataSuffix, mergeHintSuffix, lockFileName}
	ldfs, err := listHintFiles(bc)
	if err != nil {
		return nil, err
	}

	fps := make([]*os.File, 0, len(ldfs))
	for _, filePath := range ldfs {
		if existsSuffixs(filterFiles, filePath) {
			continue
		}
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

func (bc *BitCask) getFileState(fileID uint32) *BFile {
	// lock up it from write able file
	if fileID == bc.writeFile.fileID {
		return bc.writeFile
	}
	// if not exits in write able file, look up it from ActiveFile
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
			//logger.Info("n:", n, err)
			if err == io.EOF {
				break
			}

			if n != HintHeaderSize {
				panic(n)
			}

			tStamp, ksz, valueSz, valuePos := decodeHint(b)
			//logger.Info("ksz:", ksz, "offset:", offset)
			if ksz+valueSz == 0 { // the record is deleted
				continue
			}
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
