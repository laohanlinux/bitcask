package bitcask

// {fileID:value_sz:value_pos:tstamp}
// 4 * 4 + 64 = 80Bit
type entry struct {
	fileID      uint32 // file id
	valueSz     uint32 // value size in data block
	valueOffset uint64 // value offset in data block
	timeStamp   uint32 // file access time spot
}

func (e *entry) isNewerThan(old *entry) bool {
	if old.timeStamp < e.timeStamp {
		return true
	}
	if old.timeStamp > e.timeStamp {
		return false
	}

	if old.fileID < e.fileID {
		return true
	}
	if old.fileID > e.fileID {
		return false
	}
	return old.valueOffset < e.valueOffset
}
