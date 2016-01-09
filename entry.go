package bitcask

type entry struct {
	fileID    int32 // file id
	timeStamp int32
	offset    int64
	totalSize int32
}

func (e *entry) isNewerThan(old entry) bool {
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
