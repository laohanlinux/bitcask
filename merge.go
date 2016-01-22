/*=============================================================================

 Copyright (C) 2016 All rights reserved.

 Author: Rg

 Email: daimaldd@gmail.com

 Last modified: 2016-01-22 18:10

 Filename: merge.go

 Description:

=============================================================================*/

//
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
	bc               *BitCask
	Rate             int64      // worker rate time for merging
	mergeOffset      uint64     // the being merged data file fp offset
	command          string     // merge command, not used now
	mdFp             *os.File   // being merged data file fp
	mhFp             *os.File   // being merged hint file fp
	mergedLists      *list.List // has been merged data/hint fileName list
	newDataHintLists *list.List //
	oldMergeSize     int        // previus merged list size
}

// NewMerge return a merge single obj
func NewMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if bitcaskMerge == nil {
			bitcaskMerge = &Merge{
				bc:               bc,
				Rate:             rate,
				mergedLists:      list.New(),
				newDataHintLists: list.New(),
				oldMergeSize:     2, // if just one atctiveable and one writeable data/hint file, need not to merge
			}
			mergingDataFile := getMergingDataFile(bitcaskMerge.bc)
			mergingHintFile := getMergingHintFile(bitcaskMerge.bc)
			mdFp, err := os.OpenFile(mergingDataFile, os.O_RDWR|os.O_CREATE, 0755)
			if err != nil {
				logger.Error(err)
				return
			}
			mhFp, err := os.OpenFile(mergingHintFile, os.O_RDWR|os.O_CREATE, 0755)
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
					goto tryAgain
				}
				idx := strings.LastIndex(dataFileLists[i], ".data")
				m.mergedLists.PushBack(struct {
					df string
					hf string
				}{
					df: dataFileLists[i],
					hf: dataFileLists[i][:idx] + ".hint",
				})
			}
			// rename the mergingDataFile/mergingHintFile,
			// if their size > 0
			if err := m.updateMergingFiles(); err != nil {
				logger.Error(err)
				goto tryAgain
			}

			for {
				iterm := m.newDataHintLists.Front()
				if iterm == nil {
					break
				}
				nIterm := iterm.Next()
				value, _ := iterm.Value.(struct {
					mergeDataFile string
					mergeHintFile string
				})
				logger.Info(value)
				if err := m.updateHintFile(value.mergeDataFile, value.mergeHintFile); err != nil {
					logger.Error(err)
					goto tryAgain
				}
				m.newDataHintLists.Remove(iterm)
				iterm.Value = nil
				iterm = nIterm
			}
			m.newDataHintLists = nil

			m.removeOldFiles()
			// update merge size
			m.oldMergeSize = len(dataFileLists)

		tryAgain:
		}
	}
}

func (m *Merge) mergeDataFile(dFile string) error {
	// maybe check dFile is need to clear
	//TODO
	dFp, err := os.OpenFile(m.bc.dirFile+"/"+dFile, os.O_RDONLY, 0755)

	if err != nil {
		return err
	}
	defer dFp.Close()

	//TODO
	mergeDataFileName := m.mdFp.Name()

	idx := strings.LastIndex(mergeDataFileName, ".data")
	mergeFileID, _ := strconv.Atoi(mergeDataFileName[:idx])

	buf := make([]byte, HeaderSize)
	offset := int64(0)
	logger.Debug("解析的文件是:", dFile)
	for {
		// parse data header
		n, err := dFp.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		offset += int64(n)

		if n != HeaderSize {
			logger.Fatal(n, "not equal ", HintHeaderSize)
		}
		// parse data file
		_, tStamp, ksz, valuesz := DecodeEntryHeader(buf)

		if err != nil {
			logger.Fatal(err)
			return err
		}
		if ksz+valuesz == 0 {
			continue
		}
		// parse key, value
		keyValue := make([]byte, ksz+valuesz)
		n, err = dFp.ReadAt(keyValue, offset)
		if err != nil && err != io.EOF {
			logger.Error(err)
			return err
		}
		if err == io.EOF {
			break
		}
		//logger.Info("t:", tStamp, "ksz:", ksz, "valuesz:", valuesz, "key:", string(keyValue[:ksz]), "value:", string(keyValue[ksz:]))
		offset += int64(n)

		// the record is deleted
		if e := keyDirs.get(string(keyValue[:ksz])); e == nil {
			continue
		} else {
			if e.timeStamp > tStamp {
				logger.Debug("过滤:", e.timeStamp)
				logger.Info("t:", tStamp, "ksz:", ksz, "valuesz:", valuesz, "key:", string(keyValue[:ksz]), "value:", string(keyValue[ksz:]))
				continue
			}
		}

		// TODO
		// checkSumCrc32
		// write date file
		dataBuf := encodeEntry(tStamp, ksz, valuesz, keyValue[:ksz], keyValue[ksz:])
		// TODO
		// assert n
		n, err = appendWriteFile(m.mdFp, dataBuf)
		if err != nil {
			panic(err)
		}
		valueOffset := m.mergeOffset + uint64(HeaderSize+ksz)
		m.mergeOffset += uint64(n)
		// write hint file
		// TODO
		// assert n
		hintBuf := encodeHint(tStamp, ksz, valuesz, valueOffset, keyValue[:ksz])
		n, err = appendWriteFile(m.mhFp, hintBuf)
		if err != nil {
			panic(err)
		}
		// check merge data file size
		if m.mergeOffset > m.bc.Opts.MaxFileSize*100 && mergeFileID != int(time.Now().Unix()) {
			if err = m.updateMergingFiles(); err != nil {
				return err
			}

			idx = strings.LastIndex(m.mdFp.Name(), ".data")
			mergeFileID, _ = strconv.Atoi(m.mdFp.Name()[:idx])
		}
	}
	return nil
}

// update merging data/hint file
func (m *Merge) updateMergingFiles() error {
	m.mdFp.Close()
	m.mhFp.Close()
	mergeDataFile := m.mdFp.Name()
	mergeHintFile := m.mhFp.Name()
	uniqueDataFile := uniqueFileName(m.bc.dirFile, mergeDataSuffix)
	uniqueHintFile := uniqueFileName(m.bc.dirFile, mergeHintSuffix)
	if err := os.Rename(mergeDataFile, m.bc.dirFile+"/"+uniqueDataFile); err != nil {
		return err
	}
	if err := os.Rename(mergeHintFile, m.bc.dirFile+"/"+uniqueHintFile); err != nil {
		return err
	}

	m.newDataHintLists.PushFront(struct {
		mergeDataFile string
		mergeHintFile string
	}{
		mergeDataFile: uniqueDataFile,
		mergeHintFile: uniqueHintFile,
	})

	// create new merge data/hint file
	mdFp, err := os.OpenFile(getMergingDataFile(m.bc), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	mhFp, err := os.OpenFile(getMergingHintFile(m.bc), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	m.mdFp, m.mhFp = mdFp, mhFp
	m.mergeOffset = 0
	return nil
}

func (m *Merge) updateHintFile(dFile, hFile string) error {
	idx := strings.LastIndex(hFile, "."+mergeHintSuffix)
	logger.Info("The Hint File:", hFile, hFile[:idx])

	//rename mdfile/mhfile to normal fort data/hint file
	//rg:123413123.merge.hint => 123412123.hint
	if err := os.Rename(m.bc.dirFile+"/"+dFile, m.bc.dirFile+"/"+hFile[:idx]+".data"); err != nil {
		return err
	}
	if err := os.Rename(m.bc.dirFile+"/"+hFile, m.bc.dirFile+"/"+hFile[:idx]+".hint"); err != nil {
		return err
	}

	hFile = hFile[:idx] + ".hint"
	logger.Info("The Hint File:", hFile)
	idx = strings.LastIndex(hFile, ".hint")
	fileID, _ := strconv.Atoi(hFile[:idx])

	hFp, err := os.OpenFile(m.bc.dirFile+"/"+hFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer hFp.Close()

	// update data/hint record for keyDirs and activeable files
	offset := int64(0)
	buf := make([]byte, HintHeaderSize)
	for {
		//TODO
		// assert readat function
		n, err := hFp.ReadAt(buf, offset)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		tStamp, ksz, valueSz, valueOffset := DecodeHint(buf)
		if ksz+valueSz == 0 { // the record is deleted, but in here it not happend the condition
			continue
		}

		keyByte := make([]byte, ksz)
		// TODO
		// assert ReadAt function
		offset += int64(n)
		hFp.ReadAt(keyByte, offset)
		key := string(keyByte)
		//logger.Warn("hFile:", hFile, "fid:", fileID, "t:", tStamp, "ksz:", ksz, "valuesz:", valueSz, "valueOffset:", valueOffset, "key:", key)

		e := &entry{
			fileID:      uint32(fileID),
			valueSz:     valueSz,
			valueOffset: valueOffset,
			timeStamp:   tStamp,
		}
		offset += int64(ksz)

		// put entry into keyDirs
		logger.Info("更新key/value in merge function", key, e.fileID)
		keyDirs.put(key, e)
	}
	return nil
}

func (m *Merge) removeOldFiles() {
	for {
		iterm := m.mergedLists.Front()
		if iterm == nil {
			break
		}
		nIterm := iterm.Next()

		value, _ := iterm.Value.(struct {
			df string
			hf string
		})
		idx := strings.LastIndex(value.hf, ".hint")
		fileID, _ := strconv.Atoi(value.hf[:idx])
		//clear activeable file
		m.bc.ActiveFile.delWithFileID(uint32(fileID))
		m.mergedLists.Remove(iterm)
		iterm.Value = nil
		iterm = nIterm
		// TODO
		// asset remove function
		if err := os.Remove(m.bc.dirFile + "/" + value.df); err != nil {
			logger.Error(err)
		}
		logger.Info("remove old datafile:", fileID, value.df)
		if err := os.Remove(m.bc.dirFile + "/" + value.hf); err != nil {
			logger.Error(err)
		}
		logger.Info("remove old hintfile:", fileID, value.hf)
	}
	m.mergedLists = nil
}
