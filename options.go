package bitcask

const (
	defaultExpirySecs    = 0
	defaultMaxFileSize   = 1 << 31 // 2G
	defaultTimeoutSecs   = 10
	defaultValueMaxSize  = 1 << 20 // 1m
	defaultCheckSumCrc32 = false
)

// Options .
// now, just MaxFileSize is used
type Options struct {
	ExpirySecs      int
	MaxFileSize     uint64
	OpenTimeoutSecs int
	ReadWrite       bool
	MergeSecs       int
	CheckSumCrc32   bool
	ValueMaxSize    uint64
}

// NewOptions ...
func NewOptions(expirySecs int, maxFileSize uint64, openTimeoutSecs, mergeSecs int, readWrite bool) Options {
	if expirySecs < 0 {
		expirySecs = defaultExpirySecs
	}

	if maxFileSize <= 0 {
		maxFileSize = defaultMaxFileSize
	}

	if openTimeoutSecs < 0 {
		openTimeoutSecs = defaultTimeoutSecs
	}

	return Options{
		ExpirySecs:      expirySecs,
		OpenTimeoutSecs: openTimeoutSecs,
		MaxFileSize:     maxFileSize,
		ReadWrite:       readWrite,
		CheckSumCrc32:   defaultCheckSumCrc32,
		ValueMaxSize:    defaultValueMaxSize,
	}
}
