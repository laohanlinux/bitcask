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

const (
	//MergeHeaderSize ...
	MergeHeaderSize = 20

	mergeModerm = 0755
)

const (
	cmmandStart = "start merging process" // not used
	commandStop = "stop merging process"
)

var mergeOnce *sync.Once
var bcm *Merge

// Merge ...
type Merge struct {
	bc           *BitCask
	command      chan string
	rate         int64
	oldMergeSize int
	mergedLists  *list.List // has been merged data/hint fileName list
}

func init() {
	mergeOnce = &sync.Once{}
}

// NewMerge ...
func NewMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if bcm == nil {
			bcm = &Merge{
				bc:           bc,
				command:      make(chan string),
				rate:         rate,
				oldMergeSize: 2,
				mergedLists:  list.New(),
			}
		}
	})

	return bcm
}

// Start ...
func (m *Merge) Start() {
	go m.work()
}

// Stop ...
func (m *Merge) Stop() {
	m.command <- commandStop
}

func (m *Merge) work() {
	t := time.NewTimer(time.Second * time.Duration(m.rate))
	for {
		select {
		case cmd := <-m.command:
			logger.Debug("received command:", cmd)
		case <-t.C:
			logger.Info("start to merge file")
			t.Reset(time.Second * time.Duration(m.rate))
			//scan needed merged file
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
				m.mergedLists.PushBack(struct {
					df string
					hf string
				}{
					df: dataFileLists[i],
					hf: dataFileLists[i][:idx] + ".hint",
				})
			}
			if err := m.removeOldFiles(); err != nil {
				logger.Error(err)
			}
		}
	}
}

func (m *Merge) mergeDataFile(dFile string) error {
	dFp, err := os.OpenFile(m.bc.dirFile+"/"+dFile, os.O_RDONLY, mergeModerm)
	if err != nil {
		return err
	}
	defer dFp.Close()

	idx := strings.LastIndex(dFile, ".data")
	fileID, err := strconv.Atoi(dFile[:idx])
	if err != nil {
		return err
	}

	buf := make([]byte, HeaderSize)
	offset := 0
	valueOffset := uint64(0)
	for {
		//TODO
		n, err := dFp.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		offset += n
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
		n, err = dFp.Read(keyValue)
		valueOffset = uint64(offset) + uint64(ksz)

		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}

		e := entry{
			fileID:      uint32(fileID),
			timeStamp:   tStamp,
			valueOffset: valueOffset,
		}
		// call put operation
		keyDirs.putMerge(string(keyValue[:ksz]), &e)
	}
	return nil
}

func (m *Merge) removeOldFiles() error {
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

		// TODO
		// asset remove function
		if err := os.Remove(m.bc.dirFile + "/" + value.df); err != nil {
			return err
		}
		logger.Info("remove old datafile:", fileID, value.df)
		if err := os.Remove(m.bc.dirFile + "/" + value.hf); err != nil {
			return err
		}
		logger.Info("remove old hintfile:", fileID, value.hf)
		m.mergedLists.Remove(iterm)
		iterm.Value = nil
		iterm = nIterm
	}

	return nil
}
