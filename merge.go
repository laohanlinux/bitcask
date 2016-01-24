package bitcask

import (
	"sync"
	"time"

	"github.com/laohanlinux/go-logger/logger"
)

const (
	//MergeHeaderSize ...
	MergeHeaderSize = 20
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
}

func init() {
	mergeOnce = &sync.Once{}
}

func newMerge(bc *BitCask, rate int64) *Merge {
	mergeOnce.Do(func() {
		if bcm == nil {
			bcm = &Merge{
				bc:           bc,
				command:      make(chan string),
				rate:         rate,
				oldMergeSize: 2,
			}
		}
	})

	return bcm
}

func (m *Merge) Start() {

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
			dataFileLists, err := dataFileLists(m.bc)
			if err != nil {
				logger.Error(err)
				continue
			}

		}
	}
}
