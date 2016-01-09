package bitcask

const (
	defaultExpirySecs  = 0
	defaultMaxFileSize = 1024 * 1024 * 4
	defaultTimeoutSecs = 10
)

// Options ...
type Options struct {
	expirySecs      int
	maxFileSize     uint64
	openTimeoutSecs int
	readWrite       bool
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
		expirySecs:      expirySecs,
		openTimeoutSecs: openTimeoutSecs,
		maxFileSize:     maxFileSize,
		readWrite:       readWrite,
	}
}

// ExpiryTime ...
func (opt *Options) ExpiryTime() int {
	if opt.expirySecs > 0 {

	}
	return 0
}

// IsReadWrite ...
func (opt *Options) IsReadWrite() bool { return opt.readWrite }
