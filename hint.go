package bitcask

import (
	"io"
	"os"
	"path"
	"strconv"
	"strings"
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

// readableHintFile 扫描可读Hint文件
func (bc *BitCask) readableHintFile() ([]*os.File, error) {
	filterFiles := []string{lockFileName}
	ldfs, err := listHintFiles(bc)
	if err != nil {
		return nil, err
	}

	fps := make([]*os.File, 0, len(ldfs))
	for _, filePath := range ldfs {
		// TODO: Del
		if existsSuffixs(filterFiles, filePath) {
			continue
		}
		fp, err := os.OpenFile(path.Join(bc.dirFileRoot, filePath), os.O_RDONLY, 07555)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		fps = append(fps, fp)
	}

	if len(fps) == 0 {
		return nil, nil
	}
	return fps, nil
}

// listHintFiles 获取hint文件list
func listHintFiles(bc *BitCask) ([]string, error) {
	filterFiles := []string{lockFileName}
	dirFp, err := os.OpenFile(bc.dirFileRoot, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer dirFp.Close()

	// 获取下面所有文件
	lists, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	var hintLists []string
	for _, v := range lists {
		if strings.Contains(v, "hint") && !existsSuffixs(filterFiles, v) {
			hintLists = append(hintLists, v)
		}
	}

	return hintLists, nil
}

// parseHint 解析parseHind 并构造索引
func (bc *BitCask) parseHint(hintFps []*os.File) {
	b := make([]byte, HintHeaderSize)
	for _, fp := range hintFps {
		offset := int64(0)
		hintName := fp.Name()

		// TODO: Del
		//start := strings.LastIndex(hintName, getFileSeparator()) + 1
		//end := strings.LastIndex(hintName,".hint")
		//fileID, err := strconv.ParseInt(hintName[start:end], 10, 32)
		fileID, err := getFileIDByHintFile(hintName)
		if err != nil {
			panic(err)
		}

		for {
			ln, err := fp.ReadAt(b, offset)
			offset += int64(ln)
			if err != nil && err != io.EOF {
				panic(err)
			}

			if err == io.EOF {
				break
			}

			if ln != HintHeaderSize {
				panic(ln)
			}

			tStamp, ksz, valueSz, valuePos := DecodeHint(b)
			if ksz+valueSz == 0 {
				continue // del val: ksz + valueSz == 0
			}

			// get key
			keyByte := make([]byte, ksz)
			ln, err = fp.ReadAt(keyByte, offset)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err == io.EOF {
				break
			}
			// 校验长度
			if ln != int(ksz) {
				panic(ln)
			}
			key := string(keyByte)

			e := &entry{
				fileID:      uint32(fileID),
				valueSz:     valueSz,
				valueOffset: valuePos,
				timeStamp:   tStamp,
			}

			offset += int64(ksz)
			keyDirs.set(key, e) // 构造索引
		}
	}
}
