package bitcask

import (
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

// BitCask ...
type BitCask struct {
	Opts      *Options // base config
	oldFile   *BFiles  // hint file
	writeFile *BFile   // value file
	keyDirs   *KeyDirs // index 索引

	dirFileRoot string
	lockFile    *os.File // lock file, 文件锁,同时只能单写,但可以多读
	rwLock      *sync.RWMutex
}

// New ...
func New(dirName string, opts *Options) (*BitCask, error) {
	if opts == nil {
		// default opts
		ops := NewOptions(0, 0, -1, 60, true)
		opts = &ops
	}

	// 如果打开目录失败painc
	_, err := os.Stat(dirName)
	if err != nil && !os.IsNotExist(err) { // 处理其他问题
		return nil, errors.WithStack(err)
	}

	// 初始化目录
	if os.IsNotExist(err) {
		err := os.MkdirAll(dirName, 0755)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	bc := &BitCask{
		Opts:        opts,
		dirFileRoot: dirName,
		oldFile:     newBFiles(),
		rwLock:      &sync.RWMutex{},
	}

	// lock file
	bc.lockFile, err = lockFile(path.Join(dirName, lockFileName))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 初始化索引
	bc.keyDirs = NewKeyDir(dirName)

	// 获取旧数据并重建索引

	// 1.扫描可读Hint文件
	hintFiles, err := bc.readableHintFile()
	if err != nil {
		return nil, err
	}
	// 2. 解析hint文件 并构造索引
	bc.parseHint(hintFiles)
	// 3. last hint
	fileID, hintFp, err := lastHintFileInfo(hintFiles)
	if err != nil {
		return nil, err
	}

	// 设置value data file
	var writeFp *os.File
	writeFp, fileID = setWriteableFile(fileID, dirName)

	// 设置hint data file
	hintFp = setHintFile(fileID, dirName)

	closeReadHintFp(hintFiles, fileID)

	// 设置可写文件，只有一个
	dataStat, err := writeFp.Stat()
	if err != nil {
		return nil, err
	}
	bf := &BFile{
		fp:          writeFp,
		fileID:      fileID,
		writeOffset: uint64(dataStat.Size()),
		hintFp:      hintFp,
		crc:         opts.CheckSumCrc32,
	}
	bc.writeFile = bf

	// 将pid保存到bitcask.lock文件中
	writePID(bc.lockFile, fileID)
	return bc, nil
}

func (bc *BitCask) Close() {
	bc.oldFile.close()
	bc.writeFile.fp.Close()
	bc.writeFile.hintFp.Close()
	bc.lockFile.Close()
	os.Remove(path.Join(bc.dirFileRoot, lockFileName))
}

// Set key/val
func (bc *BitCask) Set(key []byte, value []byte) error {
	bc.rwLock.Lock()
	defer bc.rwLock.Unlock()
	checkWriteableFile(bc) // 检测是否满载

	// 写文件
	e, err := bc.writeFile.writeData(key, value)
	if err != nil {
		return err
	}

	// 改索引
	keyDirs.set(string(key), &e)
	return nil
}

// Get ...
func (bc *BitCask) Get(key []byte) ([]byte, error) {
	// 索引获取
	e, ex := keyDirs.get(string(key))
	if !ex || e == nil {
		return nil, errors.WithStack(ErrNotFound)
	}

	fileID := e.fileID
	bf, err := bc.getFileState(fileID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// 磁盘获取
	return bf.read(e.valueOffset, e.valueSz)
}

// Del ...
func (bc *BitCask) Del(key []byte) error {
	bc.rwLock.Lock()
	defer bc.rwLock.Unlock()

	if bc.writeFile == nil {
		return ErrNotReadRoot
	}

	e, ex := keyDirs.get(string(key))
	if !ex || e == nil {
		return ErrNotFound
	}

	checkWriteableFile(bc)
	// del value disk
	err := bc.writeFile.del(key)
	if err != nil {
		return err
	}

	// del mem index
	keyDirs.del(string(key))
	return nil
}

// getFileState 获取文件状态
func (bc *BitCask) getFileState(fileID uint32) (*BFile, error) {
	// 从可写文件中锁定它
	if fileID == bc.writeFile.fileID {
		return bc.writeFile, nil
	}

	// 如果可写文件中不存在，请从OldFile查找它
	bf, err := bc.oldFile.get(fileID)
	if err != nil && err != ErrNotFound {
		return nil, err
	}

	if err == nil {
		return bf, nil
	}

	// open file
	bf, err = openBFile(bc.dirFileRoot, int(fileID), bc.Opts.CheckSumCrc32)
	if err != nil {
		return nil, err
	}

	bc.oldFile.put(bf, fileID)
	return bf, nil
}
