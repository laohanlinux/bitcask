package bitcask

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/laohanlinux/go-logger/logger"
)

const (
	lockFileName      = "bitcask.lock"
	mergeBasePath     = "mergebase"
	mergeDataSuffix   = "merge.data"
	mergeHintSuffix   = "merge.hint"
	mergingDataSuffix = mergeDataSuffix + ".tmp"
	mergingHintSuffix = mergeHintSuffix + ".tmp"
)

func getMergingHintFile(bc *BitCask, baseTime int) string {
	mergingHintFile := bc.dirFile + "/" + strconv.Itoa(baseTime) + "." + mergingHintSuffix

	_, err := os.Stat(mergingHintFile)
	if os.IsNotExist(err) {
		return mergingHintFile
	}
	return bc.dirFile + "/" + strconv.Itoa(baseTime) + "." + mergingHintSuffix
}

func getMergingDataFile(bc *BitCask, baseTime int) string {
	mergingDataFile := bc.dirFile + "/" + strconv.Itoa(baseTime) + "." + mergingDataSuffix

	_, err := os.Stat(mergingDataFile)
	if os.IsNotExist(err) {
		return mergingDataFile
	}
	return bc.dirFile + "/" + strconv.Itoa(baseTime) + "." + mergingDataSuffix
}

// return the merge hint file, if no merge hint file, it will create a new uniquem merge hint file by Unix time stamp
func getMergeHintFile(bc *BitCask) string {
	dirFp, err := os.OpenFile(bc.dirFile, os.O_RDONLY, 0755)
	if err != nil {
		panic(err)
	}
	defer dirFp.Close()
	fileLists, err := dirFp.Readdirnames(-1)
	if err != nil {
		panic(err)
	}

	for _, file := range fileLists {
		if strings.HasSuffix(file, mergeHintSuffix) {
			return file
		}
	}
	return bc.dirFile + "/" + uniqueFileName(bc.dirFile, mergeHintSuffix)
}

// return the merge hint file, if no merge data file, it will create a new unique merge data file by Unix time stamp
func getMergeDataFile(bc *BitCask) string {
	dirFp, err := os.OpenFile(bc.dirFile, os.O_RDONLY, 0755)
	if err != nil {
		panic(err)
	}
	defer dirFp.Close()
	fileLists, err := dirFp.Readdirnames(-1)
	if err != nil {
		panic(err)
	}

	for _, file := range fileLists {
		if strings.HasSuffix(file, mergeDataSuffix) {
			return file
		}
	}

	return bc.dirFile + "/" + uniqueFileName(bc.dirFile, mergeDataSuffix)
}

// if writeableFile size large than Opts.MaxFileSize and the fileID not equal to local time stamp;
// if will create a new writeable file
func checkWriteableFile(bc *BitCask) {
	if bc.writeFile.writeOffset > bc.Opts.MaxFileSize && bc.writeFile.fileID != uint32(time.Now().Unix()) {
		logger.Info("open a new data/hint file:", bc.writeFile.writeOffset, bc.Opts.MaxFileSize)
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
		// update pid
		writePID(bc.lockFile, fileID)
	}
}

// return the hint file lists
func listHintFiles(bc *BitCask) ([]string, error) {
	filterFiles := []string{mergeDataSuffix, mergeHintSuffix, lockFileName}
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
		if strings.Contains(v, "hint") && !existsSuffixs(filterFiles, v) {
			hintLists = append(hintLists, v)
		}
	}
	return hintLists, nil
}

// return the data file lists
func listDataFiles(bc *BitCask) ([]string, error) {
	filterFiles := []string{mergeDataSuffix, mergeHintSuffix, mergingDataSuffix, mergingHintSuffix, lockFileName}
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

	var dataFileLists []string
	for _, v := range lists {
		if strings.Contains(v, ".data") && !existsSuffixs(filterFiles, v) {
			dataFileLists = append(dataFileLists, v)
		}
	}
	sort.Strings(dataFileLists)
	return dataFileLists, nil
}

// lock a file by fp locker; the file must exits
func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func existsSuffixs(suffixs []string, src string) (b bool) {
	for _, suffix := range suffixs {
		if b = strings.HasSuffix(src, suffix); b {
			return
		}
	}
	return
}

func writePID(pidFp *os.File, fileID uint32) {
	pidFp.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+".data"), 0)
}

// get file last hint file info
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
		fileName = idxFp.Name()
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

func setWriteableFile(fileID uint32, dirName string) (*os.File, uint32) {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + ".data"
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
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
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return fp
}

func appendWriteFile(fp *os.File, buf []byte) (int, error) {
	stat, err := fp.Stat()
	if err != nil {
		return -1, err
	}

	return fp.WriteAt(buf, stat.Size())
}

// return a unique not exists file name by timeStamp
func uniqueFileName(root, suffix string) string {
	for {
		tStamp := strconv.Itoa(int(time.Now().Unix()))
		_, err := os.Stat(root + "/" + tStamp + "." + suffix)
		if err != nil && os.IsNotExist(err) {
			return tStamp + "." + suffix
		}
		time.Sleep(time.Second)
	}
}
