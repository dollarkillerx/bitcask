package bitcask

import (
	"sync"
)

// key value endurance
const (
	// HeaderSize: 4 + 4 + 4 + 4 磁盘源数据域 data
	/**
	crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	*/
	HeaderSize = 16

	// HintHeaderSize: 4 + 4 + 4 + 8 = 20 byte 内存源数据域 hint
	/**
	tstamp	:	ksz	:	valuesz	：	valuePos	:	key
		4	:	4	:	4		:		8		:	xxxx
	*/
	HintHeaderSize = 20
)

type BFiles struct {
	bfs    map[uint32]*BFile
	rwLock *sync.RWMutex
}

func newBFiles() *BFiles {
	return &BFiles{
		bfs:    make(map[uint32]*BFile),
		rwLock: &sync.RWMutex{},
	}
}

func (bfs *BFiles) get(fileID uint32) (*BFile, error) {
	bfs.rwLock.RLock()
	defer bfs.rwLock.Unlock()

	file, ex := bfs.bfs[fileID]
	if !ex {
		return nil, ErrNotFound
	}

	return file, nil
}

func (bfs *BFiles) put(bf *BFile, fileID uint32) {
	bfs.rwLock.RLock()
	defer bfs.rwLock.Unlock()
	bfs.bfs[fileID] = bf
}

func (bfs *BFiles) close() {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()

	for _, bf := range bfs.bfs {
		bf.fp.Close()
		bf.hintFp.Close()
	}
}
