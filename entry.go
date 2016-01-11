package bitcask

// {fileID:value_sz:value_pos:tstamp}
type entry struct {
	fileID    int32 // file id
	entryLen  int32
	offset    int64 // file offset in data block
	timeStamp int32 // file access time spot
}

func (e *entry) isNewerThan(old *entry) bool {
	if old.timeStamp < e.timeStamp {
		return true
	}
	if old.timeStamp > e.timeStamp {
		return false
	}
	//
	if old.fileID < e.fileID {
		return true
	}
	if old.fileID > e.fileID {
		return false
	}
	return old.offset < e.offset
}
