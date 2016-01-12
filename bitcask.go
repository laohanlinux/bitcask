package bitcask

import (
	"io"
	"os"
	"strconv"
	"strings"
)

// Open ...
func Open(dirName string, opts *Options) (*BitCask, error) {
	if opts == nil {
		opts1 := NewOptions(0, 0, -1, true)
		opts = &opts1
	}

	//make sure the fileName is exits
	err := os.Mkdir(dirName, os.ModeDir)
	if err != nil {
		return nil, err
	}
	b := &BitCask{}
	// Open dirName
	if fp, err := os.Open(dirName); err != nil {
		panic(err)
	} else {
		b.dirFile = fp
	}
	// Get The Active File
	b.lockFile, err = lockFile(dirName + "/" + lockFileName)
	if err != nil {
		return nil, err
	}

	// 暂时不知道有什么鬼用
	b.keyDirs = NewKeyDir(dirName, 10)
	//　获取可读文件
	files, _ := b.readableFiles()
	// 检索可读文件

	return nil, nil
}

// BitCask ...
type BitCask struct {
	//	DirName     string
	MaxFileSize int
	Opts        *Options
	ActiveFile  *os.File
	lockFile    *os.File
	keyDirs     *KeyDirs
	dirFile     *os.File
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
	if stat, err := bc.dirFile.Stat(); err != nil {
		panic(err)
	} else if !stat.IsDir() {
		panic(bc.dirFile.Name() + " is not a director")
	}

	return bc.dirFile.Readdirnames(-1)
}

func (bc *BitCask) scanKeyFiles(files []*os.File, keyDir *KeyDir) {
	for _, file := range files {
		fileName := file.Name()
		hintName := fileName[0:strings.Index(fileName, "data")] + ".hint"
		// 检索ｈｉｎｔ文件

	}
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
		// 4 + 2 + 4 + 8
		timeStamp, _ := strconv.Atoi(string(b[0:4]))
		keyLen, _ := strconv.Atoi(string(b[4:6]))
		entryLen, _ := strconv.Atoi(string(b[6:10]))
		entryOffset, _ := strconv.ParseInt(string(b[10:18]), 10, 8)

		keyByte := make([]byte, keyLen)
		fp.ReadAt(keyByte, offset)
		key := string(keyByte)
		e := &entry{
			fileID:    int32(fileID),
			entryLen:  int32(entryLen),
			offset:    entryOffset,
			timeStamp: int32(timeStamp),
		}
		// put entry into keyDirs
		keyDirs.put(key, e)
	}

}
