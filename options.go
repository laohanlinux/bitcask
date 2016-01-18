package bitcask

const (
	defaultExpirySecs  = 0
	defaultMaxFileSize = 1 << 30 // 1G
	defaultTimeoutSecs = 10
	defaultMergeSecs   = 180
)

// Options .
// now, just MaxFileSize is used
type Options struct {
	ExpirySecs      int
	MaxFileSize     uint64
	OpenTimeoutSecs int
	ReadWrite       bool
	MergeSecs       int
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

	if mergeSecs <= 0 {
		mergeSecs = defaultMergeSecs
	}

	return Options{
		ExpirySecs:      expirySecs,
		OpenTimeoutSecs: openTimeoutSecs,
		MaxFileSize:     maxFileSize,
		ReadWrite:       readWrite,
		MergeSecs:       mergeSecs,
	}
}

// ExpiryTime ...
func (opt *Options) ExpiryTime() int {
	if opt.ExpirySecs > 0 {

	}
	return 0
}
