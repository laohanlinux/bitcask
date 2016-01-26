package bitcask

import "fmt"

// {fileID:value_sz:value_pos:tstamp}
// 4 * 4 + 64 = 80Bit
type entry struct {
	fileID      uint32 // file id
	valueSz     uint32 // value size in data block
	valueOffset uint64 // value offset in data block
	timeStamp   uint32 // file access time spot
}

func (e *entry) toString() string {
	return fmt.Sprintf("timeStamp:%d, fileID:%d, valuesz:%d, offset:%d", e.timeStamp,
		e.fileID, e.valueSz, e.valueOffset)
}

// if all attr equal to old entry, return false
func (e *entry) isNewerThan(old *entry) bool {
	if old.timeStamp < e.timeStamp {
		return true
	} else if old.timeStamp > e.timeStamp {
		return false
	}

	if old.fileID < e.fileID {
		return true
	} else if old.fileID > e.fileID {
		return false
	}

	if old.valueOffset < e.valueOffset {
		return true
	} else if old.valueOffset > e.valueOffset {
		return false
	}

	return false
}

// if all attr equal to old entry, return true
func (e *entry) isNewerThan1(old *entry) bool {
	if old.timeStamp < e.timeStamp {
		return true
	}
	if old.timeStamp > e.timeStamp {
		return false
	}
	return true
}
