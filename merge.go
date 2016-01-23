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
	"fmt"
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
	Rate             int64    // worker rate time for merging
	mergeOffset      uint64   // the being merged data file fp offset
	command          string   // merge command, not used now
	mdFp             *os.File // being merged data file fp
	mhFp             *os.File // being merged hint file fp
	firstFileID      uint32
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
		}
		// clear all old merged file

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
			// rename the mergingDataFile/mergingHintFile
			if err := m.updateMergingFiles(0); err != nil {
				logger.Error(err)
				goto tryAgain
			}

			m.removeOldFiles()
			// update merge size

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
	idx := strings.LastIndex(dFile, ".data")
	baseTime, _ := strconv.Atoi(dFile[:idx])
	if m.mdFp == nil {
		mergingDataFile := getMergingDataFile(m.bc, baseTime)
		mergingHintFile := getMergingHintFile(m.bc, baseTime)
		mdFp, err := os.OpenFile(mergingDataFile, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		mhFp, err := os.OpenFile(mergingHintFile, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return err
		}
		m.mdFp, m.mhFp = mdFp, mhFp
		m.mergeOffset = 0
		m.firstFileID = uint32(baseTime)
	}

	//TODO
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
		valueOff := uint64(offset + int64(ksz))
		offset += int64(n)
		//logger.Info("t:", tStamp, "ksz:", ksz, "valuesz:", valuesz, "key:", string(keyValue[:ksz]), "value:", string(keyValue[ksz:]))

		// the record is deleted
		if e := keyDirs.get(string(keyValue[:ksz])); e == nil {
			continue
		} else {
			// 因为相同的时间戳会在同一个文件里面，所以时间戳一样，只要比较偏移值即可
			eTmp := entry{
				fileID:      uint32(baseTime),
				timeStamp:   tStamp,
				valueOffset: valueOff,
			}
			if e.isNewerThan(&eTmp) && string(keyValue[:ksz]) == "53" {
				logger.Debug("过滤:", e.timeStamp)
				logger.Info("t:", tStamp, "ksz:", ksz, "valuesz:", valuesz, "key:", string(keyValue[:ksz]), "value:", string(keyValue[ksz:]))
				continue
			}
		}

		// check merge data file size
		if m.mergeOffset > m.bc.Opts.MaxFileSize*100 && tStamp > m.firstFileID {
			if err = m.updateMergingFiles(tStamp); err != nil {
				return err
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
	}
	return nil
}

// update merging data/hint file, return a new merging data/hint file with newFileID name
func (m *Merge) updateMergingFiles(newFileID uint32) error {
	mergingDataFile := m.mdFp.Name()
	mergingHintFile := m.mhFp.Name()
	logger.Debug("====>", mergingDataFile, mergingHintFile)
	m.mdFp.Close()
	m.mhFp.Close()
	idx := strings.LastIndex(mergingDataFile, "."+mergingDataSuffix)
	mergeDataFile := mergingDataFile[:idx] + "." + mergeDataSuffix
	mergeHintFile := mergingDataFile[:idx] + "." + mergeHintSuffix
	logger.Debug(mergeHintFile, mergeDataFile)
	if err := os.Rename(mergingDataFile, mergeDataFile); err != nil {
		return err
	}
	if err := os.Rename(mergingHintFile, mergeHintFile); err != nil {
		return err
	}
	m.newDataHintLists.PushFront(struct {
		mergeDataFile string
		mergeHintFile string
	}{
		mergeDataFile: mergeDataFile,
		mergeHintFile: mergeHintFile,
	})

	if newFileID == 0 {
		m.mdFp, m.mhFp = nil, nil
		m.mergeOffset = 0
		m.firstFileID = 0
		return nil
	}
	mdName := fmt.Sprintf("%s/%d.%s", m.bc.dirFile, newFileID, mergingDataSuffix)
	mhName := fmt.Sprintf("%s/%d.%s", m.bc.dirFile, newFileID, mergingHintSuffix)
	mdFp, err := os.OpenFile(mdName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	mhFp, err := os.OpenFile(mhName, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	m.mdFp, m.mhFp = mdFp, mhFp
	m.mergeOffset = 0
	m.firstFileID = newFileID
	return nil
}

func (m *Merge) updateHintFile(dFile, hFile string) error {
	idx := strings.LastIndex(hFile, "."+mergeHintSuffix)
	logger.Info(dFile, hFile)
	logger.Info("The Hint File:", hFile, hFile[:idx])

	//rename mdfile/mhfile to normal fort data/hint file
	//rg:123413123.merge.hint => 123412123.hint
	if err := os.Rename(dFile, hFile[:idx]+".data"); err != nil {
		return err
	}
	if err := os.Rename(hFile, hFile[:idx]+".hint"); err != nil {
		return err
	}

	hFile = hFile[:idx] + ".hint"
	logger.Info("The Hint File:", hFile)
	idx1 := strings.LastIndex(hFile, "/")
	idx2 := strings.LastIndex(hFile, ".hint")
	fileID, err := strconv.Atoi(hFile[idx1+1 : idx2])
	if err != nil {
		panic(err)
	}

	hFp, err := os.OpenFile(hFile, os.O_RDONLY, 0755)
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
		if key == "53" {
			logger.Error(string(key), "被更新到文件:", hFile, dFile, "中.")
		}
		//logger.Warn("hFile:", hFile, "fid:", fileID, "t:", tStamp, "ksz:", ksz, "valuesz:", valueSz, "valueOffset:", valueOffset, "key:", key)

		e := &entry{
			fileID:      uint32(fileID),
			valueSz:     valueSz,
			valueOffset: valueOffset,
			timeStamp:   tStamp,
		}
		offset += int64(ksz)

		// put entry into keyDirs
		//logger.Info("更新key/value in merge function", key, e.fileID)
		//keyDirs.setCompare(key, e)
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

}
