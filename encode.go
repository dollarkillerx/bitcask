package bitcask

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/pkg/errors"
)

var ErrCrc32 = errors.New("checksumIEEE error")

func encodeEntry(tStamp, keySize, valueSize uint32,
	key, value []byte, crc bool) []byte {
	/**
		crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	    4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	*/
	bufSize := HeaderSize + keySize + valueSize
	buf := make([]byte, bufSize)

	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[HeaderSize:(HeaderSize+keySize)], key)
	copy(buf[(HeaderSize+keySize):(HeaderSize+keySize+valueSize)], key)

	if crc {
		u := crc32.ChecksumIEEE(buf[4:])
		binary.LittleEndian.PutUint32(buf[:4], u)
	}
	return buf
}

func DecodeEntry(buf []byte, crc bool) ([]byte, error) {
	/**
	  crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	  4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	*/
	kSz := binary.LittleEndian.Uint32(buf[8:12])
	valueSz := binary.LittleEndian.Uint32(buf[12:HeaderSize])

	value := make([]byte, valueSz)
	copy(value, buf[(HeaderSize+kSz):(HeaderSize+kSz+valueSz)])

	if crc {
		c32 := binary.LittleEndian.Uint32(buf[:4])
		if crc32.ChecksumIEEE(buf[4:]) != c32 {
			return nil, ErrCrc32
		}
	}

	return value, nil
}

// DecodeEntryHeader 获取Header基础信息
func DecodeEntryHeader(buf []byte) (uint32, uint32, uint32, uint32) {
	/**
	  crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	  4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	*/
	c32 := binary.LittleEndian.Uint32(buf[:4])
	tStamp := binary.LittleEndian.Uint32(buf[4:8])
	kSz := binary.LittleEndian.Uint32(buf[8:12])
	valueSz := binary.LittleEndian.Uint32(buf[12:HeaderSize])
	return c32, tStamp, kSz, valueSz
}

func DecodeEntryDetail(buf []byte, crc bool) (uint32, uint32, uint32, uint32, []byte, []byte, error) {
	/**
	  crc32	:	tStamp	:	ksz	:	valueSz	:	key	:	value
	  4 		:	4 		: 	4 	: 		4	:	xxxx	: xxxx
	*/

	c32, tStamp, kSz, valueSz := DecodeEntryHeader(buf)
	if crc {
		if crc32.ChecksumIEEE(buf[4:]) != c32 {
			return 0, 0, 0, 0, nil, nil, ErrCrc32
		}
	}

	if kSz+valueSz == 0 {
		return c32, tStamp, kSz, valueSz, nil, nil, nil
	}

	key := make([]byte, kSz)
	value := make([]byte, valueSz)
	copy(key, buf[HeaderSize:(HeaderSize+kSz)])
	copy(value, buf[(HeaderSize+kSz):(HeaderSize+kSz+valueSz)])
	return c32, tStamp, kSz, valueSz, key, value, nil
}

func encodeHint(tStamp, kSz, valueSz uint32, valuePos uint64, key []byte) []byte {
	/**
	tStamp	:	ksz	:	valueSz	:	valuePos	:	key
	4       :   4   :   4       :       8       :   xxxxx
	*/

	buf := make([]byte, HintHeaderSize+len(key))
	binary.LittleEndian.PutUint32(buf[:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], kSz)
	binary.LittleEndian.PutUint32(buf[8:12], valueSz)
	binary.LittleEndian.PutUint64(buf[12:HintHeaderSize], valuePos)
	copy(buf[HintHeaderSize:], key)
	return buf
}

func DecodeHint(buf []byte) (uint32, uint32, uint32, uint64) {
	/**
	tStamp	:	ksz	:	valueSz	:	valuePos	:	key
	4       :   4   :   4       :       8       :   xxxxx
	*/
	tStamp := binary.LittleEndian.Uint32(buf[:4])
	kSz := binary.LittleEndian.Uint32(buf[4:8])
	valueSz := binary.LittleEndian.Uint32(buf[8:12])
	valueOffset := binary.LittleEndian.Uint64(buf[12:HintHeaderSize])
	return tStamp, kSz, valueSz, valueOffset
}
