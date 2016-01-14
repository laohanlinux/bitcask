package bitcask

// TODO
// add iter for scan bitcask item
type keyValueIter interface {
	each(string, string, uint64)
}
