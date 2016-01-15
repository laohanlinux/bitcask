package bitcask

const (
	defaultExpirySecs  = 0
	defaultMaxFileSize = 1024 * 1024 * 4
	defaultTimeoutSecs = 10
)

// Options .
// now, just MaxFileSize is used
type Options struct {
	ExpirySecs      int
	MaxFileSize     uint64
	OpenTimeoutSecs int
	ReadWrite       bool
}

// NewOptions ...
func NewOptions(expirySecs int, maxFileSize uint64, openTimeoutSecs int, readWrite bool) Options {
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
	}
}

// ExpiryTime ...
func (opt *Options) ExpiryTime() int {
	if opt.ExpirySecs > 0 {

	}
	return 0
}
