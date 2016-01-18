package bitcask

import (
	"container/list"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/laohanlinux/go-logger/logger"
)

var mergeOnce *sync.Once
var bitcaskMerge *Merge

// StopComm ...
const (
	StopComm  = "stop merge"
	StartComm = "start merge"
)

func init() {
	mergeOnce = &sync.Once{}
}

// Merge for bitcask file merge
type Merge struct {
	bc           *BitCask
	Rate         int64
	mergeOffset  uint64
	command      string
	mdFp         *os.File
	mhFp         *os.File
	mergedLists  *list.List
	oldMergeSize int
}

// NewMerge return a merge single obj
func NewMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if bitcaskMerge == nil {
			bitcaskMerge = &Merge{
				bc:           bc,
				Rate:         rate,
				mergedLists:  list.New(),
				oldMergeSize: 2,
			}
			mergeDataFile := getMergeDataFile(bitcaskMerge.bc)
			mergeHintFile := getMergeHintFile(bitcaskMerge.bc)
			mdFp, err := os.OpenFile(mergeDataFile, os.O_RDONLY|os.O_CREATE, 0755)
			if err != nil {
				logger.Error(err)
				return
			}
			mhFp, err := os.OpenFile(mergeHintFile, os.O_RDONLY|os.O_CREATE, 0755)
			if err != nil {
				logger.Error(err)
				return
			}
			bitcaskMerge.mdFp, bitcaskMerge.mhFp = mdFp, mhFp
		}
	})
	return bitcaskMerge
}

// Start a merge worker
func (m *Merge) Start() {
	go m.work()
}

func (m *Merge) work() {
	t := time.NewTimer(time.Second * time.Duration(m.Rate))
	for {
		select {
		case <-t.C:
			logger.Info("Start to merge file.")
			t.Reset(time.Second * time.Duration(m.Rate))
			// scan need merged files
			dataFileLists, err := listDataFiles(m.bc)
			if err != nil {
				logger.Error(err)
				continue
			}
			if len(dataFileLists) <= m.oldMergeSize {
				logger.Debug("No files need to merge, dataList:", dataFileLists)
				continue
			}
			logger.Info("Need to merge file lists:", dataFileLists)
			// no inclued the writeable data/hint file
			for i := 0; i < len(dataFileLists)-1; i++ {
				logger.Info("Merging File is:", dataFileLists[i])
				err := m.mergeDataFile(dataFileLists[i])
				if err != nil {
					logger.Error(err)
				}
				idx := strings.LastIndex(dataFileLists[i], ".data")
				if err != nil {
					m.mergedLists.PushBack(struct {
						df string
						hf string
					}{
						df: dataFileLists[i],
						hf: dataFileLists[i][:idx] + ".hint",
					})
				}
			}
			// TODO
			// rollback merge
			_, _, err = m.clearLastFile()
			if err != nil {
				panic(err)
			}
			// update merge size
			m.oldMergeSize = len(dataFileLists)
		}
	}
}

func (m *Merge) mergeDataFile(dFile string) error {

	dFp, err := os.OpenFile(m.bc.dirFile+"/"+dFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer func() {
		if dFp != nil {
			logger.Info("+=================")
			dFp.Close()
		}
	}()
	/**
	    crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	    4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	buf := make([]byte, HeaderSize)
	for {

		_, err := dFp.Read(buf)
		logger.Info("+=================")
		if err != nil && err != io.EOF {
			logger.Info("+=================")
			return err
		}
		if err == io.EOF {
			logger.Info("+=================")
			break
		}

		// parse data file
		_, tStamp, ksz, valuesz, key, value, err := decodeEntryDetail(buf)
		logger.Info("+=================", string(buf))
		if err != nil {
			logger.Info("+=================", string(buf))
			return err
		}
		if ksz+valuesz == 0 {
			continue
		}
		logger.Info("+=================", string(key))
		if e := keyDirs.get(string(key)); e == nil {
			logger.Info("+=================")
			continue
		} else {
			if e.timeStamp > tStamp {
				continue
			}
		}
		// write date file
		buf := encodeEntry(tStamp, ksz, valuesz, key, value)
		// TODO
		// assert n
		n, err := appendWriteFile(m.mdFp, buf)
		if err != nil {
			panic(err)
		}
		valuePos := uint64(n) + m.mergeOffset
		logger.Info("+=================")
		// write hint file
		// TODO
		// assert n
		buf = encodeHint(tStamp, ksz, valuesz, valuePos, key)
		n, err = appendWriteFile(m.mhFp, buf)
		if err != nil {
			panic(err)
		}
		m.mergeOffset = valuePos

		logger.Info(key, m.mhFp.Name())
		// check merge data file size
		if m.mergeOffset > m.bc.Opts.MaxFileSize {
			m.mdFp.Close()
			m.mhFp.Close()
			// rename merge data/hint file
			mergeDataFile := m.mdFp.Name()
			mergeHintFile := m.mhFp.Name()
			uniqueDataFile := uniqueFileName(m.bc.dirFile, "data")
			uniqueHintFile := uniqueFileName(m.bc.dirFile, "hint")
			os.Rename(mergeDataFile, uniqueDataFile)
			os.Rename(mergeHintFile, uniqueHintFile)
			// create new merge data/hint file
			mergeDataFile = getMergeDataFile(m.bc)
			mergeHintFile = getMergeHintFile(m.bc)
			mdFp, err := os.OpenFile(mergeDataFile, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}
			mhFp, err := os.OpenFile(mergeHintFile, os.O_RDONLY, 0755)
			if err != nil {
				return err
			}
			m.mdFp, m.mhFp = mdFp, mhFp
			m.mergeOffset = 0

			// clear old data/hint file
			m.clearData(uniqueDataFile, uniqueHintFile)
		}
	}

	err = dFp.Close()
	if err != nil {
		return err
	}
	idx := strings.LastIndex(dFile, ".data")
	fileID, _ := strconv.Atoi(dFile[0:idx])
	// remove old data/hint file from activefiles
	if err = m.bc.ActiveFile.delWithFileID(uint32(fileID)); err != nil {
		return err
	}
	dFp = nil
	// delete old/hint data/hint file from disk
	if err = os.Remove(m.bc.dirFile + "/" + dFile); err != nil {
		return err
	}
	if err = os.Remove(m.bc.dirFile + "/" + dFile[0:idx] + ".hint"); err != nil {
		return err
	}
	return nil
}

func (m *Merge) clearData(mdataFile, mhintFile string) error {
	// update keyDirs
	hFp, err := os.OpenFile(m.bc.dirFile+"/"+mhintFile, os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	defer hFp.Close()

	idx := strings.LastIndex(mdataFile, ".data")
	fileID, _ := strconv.Atoi(mdataFile[:idx])

	buf := make([]byte, HintHeaderSize)
	off := int64(0)
	for {
		// TODO
		// asset ReadAt function
		_, err := hFp.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		tStamp, ksz, valueSz, valuePos := decodeHint(buf)
		if ksz+valueSz == 0 { // the record is deleted
			continue
		}
		keyByte := make([]byte, ksz)
		hFp.ReadAt(keyByte, off)
		key := string(keyByte)

		e := &entry{
			fileID:    uint32(fileID),
			entryLen:  valueSz,
			offset:    valuePos,
			timeStamp: tStamp,
		}
		off += int64(ksz)
		// logger.Info("更新key/value", key, e.fileID)
		// put entry into keyDirs
		keyDirs.put(key, e)
		// Update ActiveFile
		m.bc.ActiveFile.delWithFileID(uint32(fileID))
	}

	for {
		iterm := m.mergedLists.Front()
		if iterm == nil {
			break
		}
		value, _ := iterm.Value.(struct {
			df string
			hf string
		})

		nIterm := iterm.Next()
		m.mergedLists.Remove(iterm)
		iterm.Value = nil
		iterm = nIterm
		// TODO
		// asset remove function
		os.Remove(m.bc.dirFile + "/" + value.df)
		os.Remove(m.bc.dirFile + "/" + value.hf)
	}
	return nil
}

func (m *Merge) clearLastFile() (string, string, error) {
	m.mdFp.Close()
	m.mhFp.Close()
	// rename merge data/hint file
	mergeDataFile := m.mdFp.Name()
	mergeHintFile := m.mhFp.Name()
	uniqueDataFile := uniqueFileName(m.bc.dirFile, "data")
	uniqueHintFile := uniqueFileName(m.bc.dirFile, "hint")
	logger.Info("u:", uniqueDataFile, uniqueHintFile, mergeDataFile)
	if err := os.Rename(mergeDataFile, m.bc.dirFile+"/"+uniqueDataFile); err != nil {
		panic(err)
	}
	if err := os.Rename(mergeHintFile, m.bc.dirFile+"/"+uniqueHintFile); err != nil {
		panic(err)
	}

	// create new merge data/hint file
	mergeDataFile = getMergeDataFile(m.bc)
	mergeHintFile = getMergeHintFile(m.bc)
	mdFp, err := os.OpenFile(mergeDataFile, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		return uniqueDataFile, uniqueHintFile, err
	}
	mhFp, err := os.OpenFile(mergeHintFile, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		return uniqueDataFile, uniqueHintFile, err
	}
	m.mdFp, m.mhFp = mdFp, mhFp
	m.mergeOffset = 0

	offset := int64(0)
	buf := make([]byte, HintHeaderSize)
	uhFp, err := os.OpenFile(m.bc.dirFile+"/"+uniqueDataFile, os.O_RDONLY, 0755)
	if err != nil {
		return uniqueDataFile, uniqueHintFile, err
	}

	idx := strings.LastIndex(uniqueDataFile, ".data")
	fileID, _ := strconv.Atoi(uniqueDataFile[:idx])

	for {
		//TODO
		_, err := uhFp.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return uniqueDataFile, uniqueHintFile, err
		}
		if err == io.EOF {
			break
		}
		tStamp, ksz, valueSz, valuePos := decodeHint(buf)
		if ksz+valueSz == 0 { // the record is deleted
			continue
		}
		keyByte := make([]byte, ksz)
		uhFp.ReadAt(keyByte, offset)
		key := string(keyByte)

		e := &entry{
			fileID:    uint32(fileID),
			entryLen:  valueSz,
			offset:    valuePos,
			timeStamp: tStamp,
		}
		offset += int64(ksz)
		// put entry into keyDirs
		// logger.Info("更新key/value", key, e.fileID)
		keyDirs.put(key, e)
		// Update ActiveFile
		m.bc.ActiveFile.delWithFileID(uint32(fileID))
	}

	for {
		iterm := m.mergedLists.Front()
		if iterm == nil {
			break
		}
		value, _ := iterm.Value.(struct {
			df string
			hf string
		})

		nIterm := iterm.Next()
		m.mergedLists.Remove(iterm)
		iterm.Value = nil
		iterm = nIterm
		// TODO
		// asset remove function
		os.Remove(m.bc.dirFile + "/" + value.df)
		os.Remove(m.bc.dirFile + "/" + value.hf)
	}
	return "", "", nil
}
