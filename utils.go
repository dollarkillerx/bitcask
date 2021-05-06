package bitcask

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	lockFileName = "bitcask.lock"
)

func appendWriteFile(fp *os.File, buf []byte) (int, error) {
	stat, err := fp.Stat()
	if err != nil {
		return -1, err
	}

	return fp.WriteAt(buf, stat.Size())
}

func lockFile(fileName string) (*os.File, error) {
	// os.O_EXCL 排它
	return os.OpenFile(fileName, os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)
}

// 是否存在这些后缀
func existsSuffixs(suffixs []string, src string) bool {
	for _, sf := range suffixs {
		if strings.HasSuffix(src, sf) {
			return true
		}
	}
	return false
}

func getFileSeparator() string {
	if runtime.GOOS == "windows" {
		return "\\"
	}

	return "/"
}

// getFileIDByHintFile 获得fileid
func getFileIDByHintFile(fileName string) (int, error) {
	s := strings.LastIndex(fileName, getFileSeparator()) + 1
	e := strings.LastIndex(fileName, ".hint")
	return strconv.Atoi(fileName[s:e])
}

// lastHintFileInfo 获取文件最后提示文件信息
func lastHintFileInfo(files []*os.File) (uint32, *os.File, error) {
	if files == nil {
		return uint32(0), nil, nil
	}
	lastFp := files[0]

	fileName := lastFp.Name()
	fileID, err := getFileIDByHintFile(fileName)
	if err != nil {
		return 0, nil, errors.WithStack(err)
	}

	lastID := fileID
	for i := 0; i < len(files); i++ {
		idxFp := files[i]
		fileName = idxFp.Name()
		idx, err := getFileIDByHintFile(fileName)
		if err != nil {
			return 0, nil, errors.WithStack(err)
		}
		// last就是最大的
		if lastID < idx {
			lastID = idx
			lastFp = idxFp
		}
	}

	return uint32(lastID), lastFp, nil
}

// setWriteableFile 设置可写文件value data
func setWriteableFile(fileID uint32, dirName string) (*os.File, uint32) {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := path.Join(dirName, fmt.Sprintf("%d.data", fileID))
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 00755)
	if err != nil {
		panic(err)
	}

	return fp, fileID
}

// setHintFile 设置hint file data
func setHintFile(fileID uint32, dirName string) *os.File {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}

	fileName := path.Join(dirName, fmt.Sprintf("%d.hint", fileID))
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 00755)
	if err != nil {
		panic(err)
	}

	return fp
}

// closeReadHintFp 关闭以打开的hint
func closeReadHintFp(files []*os.File, fileID uint32) {
	for _, fp := range files {
		if !strings.Contains(fp.Name(), strconv.Itoa(int(fileID))) {
			fp.Close()
		}
	}
}

// writePID 将pid保存到bitcask.lock文件中
func writePID(pidFp *os.File, fileID uint32) {
	pidFp.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+".data"), 0)
}

//如果writeableFile大小大于 Opts.MaxFileSize 并且 fileID 不等于本地时间戳；
//如果将创建一个新的可写文件
func checkWriteableFile(bc *BitCask) {
	if bc.writeFile.writeOffset > bc.Opts.MaxFileSize &&
		bc.writeFile.fileID != uint32(time.Now().Unix()) {
		bc.writeFile.hintFp.Close()
		bc.writeFile.fp.Close()

		writeFp, fileID := setWriteableFile(0, bc.dirFileRoot)
		hintFp := setHintFile(fileID, bc.dirFileRoot)
		bf := &BFile{
			fp:          writeFp,
			fileID:      fileID,
			writeOffset: 0,
			hintFp:      hintFp,
			crc:         bc.Opts.CheckSumCrc32,
		}

		bc.writeFile = bf
		writePID(bc.lockFile, fileID)
	}
}
