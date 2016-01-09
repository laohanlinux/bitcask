package bitcask

type keyValueIter interface {
	each(string, string, uint64)
}
