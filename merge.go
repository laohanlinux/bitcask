package bitcask

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/laohanlinux/go-logger/logger"
)

var mergeOnce *sync.Once
var bitcaskMerge *Merge

func init() {
	mergeOnce = &sync.Once{}
}

// Merge for bitcask file merge
type Merge struct {
	bc   *BitCask
	Rate int64
}

// NewMerge return a merge single obj
func NewMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if bitcaskMerge == nil {
			bitcaskMerge = &Merge{
				bc:   bc,
				Rate: rate,
			}
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
			t.Reset(time.Second * time.Duration(m.Rate))
			// scan need merged files
			dataFileLists, err := listDataFiles(m.bc)
			if err != nil {
				logger.Error(err)
			}
			if len(dataFileLists) <= 1 {
				logger.Debug("No files need to merge.")
			}
			//

			//	mergeOffset := 0
			for i := 0; i < len(dataFileLists)-1; i++ {
				m.mergeDataFile(dataFileLists[i])
			}
		}
	}
}

func (m *Merge) mergeDataFile(dFile string) error {
	mergeDataFile := getMergeDataFile(m.bc)
	mergeHintFile := getMergeHintFile(m.bc)
	mdFp, err := os.OpenFile(mergeDataFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer mdFp.Close()

	mhFp, err := os.OpenFile(mergeHintFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer mhFp.Close()

	dFp, err := os.OpenFile(m.bc.dirFile+"/"+dFile, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	defer dFp.Close()

	/**
	    crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	    4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	**/
	buf := make([]byte, HeaderSize)
	for {
		_, err := dFp.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		// parse data file
		c32, tStamp, ksz, valuesz, key, value, err := decodeEntryDetail(buf)
		if err != nil {
			return err
		}
		if ksz+valuesz == 0 {
			continue
		}
		// write hint file
	}
}
