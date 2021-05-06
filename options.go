package bitcask

import "errors"

// Error
var (
	ErrNotFound    = errors.New("Not Found")
	ErrIsNotDir    = errors.New("This File is not dir")
	ErrNotReadRoot = errors.New("Can Not Read The Bitcask Root Director")
)

const (
	defaultExpirySecs    = 0
	defaultMaxFileSize   = 1 << 31 // 2G
	defaultTimeoutSecs   = 10
	defaultValueMaxSize  = 1 << 20 // 1M
	defaultCheckSumCrc32 = true
)

// Options .
type Options struct {
	ExpirySecs      int    // 过期时间
	MaxFileSize     uint64 // 最大文件大小
	OpenTimeoutSecs int    // 最大打开文件
	ReadWrite       bool   // 读写
	MergeSecs       int    // merger 时间
	CheckSumCrc32   bool   // crc32校验
	ValueMaxSize    uint64 // 最大Value
}

// NewOptions ...
func NewOptions(expirySecs int, maxFileSize uint64,
	openTimeoutSecs, mergeSecs int, readWrite bool) Options {
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
		mergeSecs = 30
	}

	return Options{
		ExpirySecs:      expirySecs,
		OpenTimeoutSecs: openTimeoutSecs,
		MaxFileSize:     maxFileSize,
		ReadWrite:       readWrite,
		CheckSumCrc32:   defaultCheckSumCrc32,
		ValueMaxSize:    defaultMaxFileSize,
		MergeSecs:       mergeSecs,
	}
}
