package bitcask

// {fileID:value_sz:value_pos:tstamp}
// 4 * 4 + 64 = 80Bit
type entry struct {
	fileID      uint32 // file id
	valueSz     uint32 // value size in data block
	valueOffset uint64 // value offset in data block
	timeStamp   uint32 // file access time spot
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
