package bitcask

import (
	"os"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// BFile hint file
type BFile struct {
	// fp write file handler, val
	fp          *os.File
	fileID      uint32
	writeOffset uint64
	// hintFp hint file
	hintFp *os.File
	crc    bool
}

// openBFile ...
func openBFile(dirName string, tStamp int, crc bool) (*BFile, error) {
	fp, err := os.OpenFile(path.Join(dirName, strconv.Itoa(tStamp)+".data"), os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &BFile{
		fileID:      uint32(tStamp),
		fp:          fp,
		hintFp:      nil,
		writeOffset: 0,
		crc:         crc,
	}, nil
}

// read 根据offset读
func (bf *BFile) read(offset uint64, length uint32) ([]byte, error) {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
		4	:		 4	:		4:		4	:	xxxx	: xxxx
	**/
	value := make([]byte, length)
	bf.fp.Seek(int64(offset), 0)
	_, err := bf.fp.Read(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return value, errors.WithStack(err)
}

// writeData 写
func (bf *BFile) writeData(key []byte, value []byte) (entry, error) {
	// val data
	timestamp := uint32(time.Now().Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))

	vec := encodeEntry(timestamp, keySize, valueSize, key, value, bf.crc)
	entrySize := HeaderSize + keySize + valueSize
	valueOffset := bf.writeOffset + uint64(HeaderSize+keySize)

	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
		return entry{}, errors.WithStack(err)
	}

	// write hint
	hintData := encodeHint(timestamp, keySize, valueSize, valueOffset, key)
	_, err = appendWriteFile(bf.hintFp, hintData)
	if err != nil {
		panic(err)
		return entry{}, errors.WithStack(err)
	}

	bf.writeOffset += uint64(entrySize)

	return entry{
		fileID:      bf.fileID,
		valueSz:     valueSize,
		valueOffset: valueOffset,
		timeStamp:   timestamp,
	}, nil
}

func (bf *BFile) del(key []byte) error {
	// write into datafile
	timeStamp := uint32(time.Now().Unix())
	kSz := uint32(0)
	valueSz := uint32(0)

	vec := encodeEntry(timeStamp, kSz, valueSz, key, nil, bf.crc)

	entrySize := HeaderSize + kSz + valueSz

	valueOffset := bf.writeOffset + uint64(HeaderSize+kSz)

	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
	}

	// hint
	hintData := encodeHint(timeStamp, kSz, valueSz, valueOffset, key)

	_, err = appendWriteFile(bf.hintFp, hintData)
	if err != nil {
		panic(err)
	}

	bf.writeOffset += uint64(entrySize)

	return nil
}
