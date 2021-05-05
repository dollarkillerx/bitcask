package bitcask

import "fmt"

// kv structs
type entry struct {
	fileID      uint32 // file id
	valueSz     uint32 // 数据块中的值大小
	valueOffset uint64 // 数据块中的值偏移
	timeStamp   uint32 // 文件访问时间点
}

func (e *entry) toString() string {
	return fmt.Sprintf("timeStamp:%d, fileID:%d, valuesz:%d, offset:%d", e.timeStamp,
		e.fileID, e.valueSz, e.valueOffset)
}

// isNewerThan 如果所有attr都等于旧条目，则返回false
func (e *entry) isNewerThan(old *entry) bool {
	switch {
	case old.timeStamp < e.timeStamp:
		return true
	case old.timeStamp > e.timeStamp:
		return false
	case old.fileID < e.fileID:
		return true
	case old.fileID > e.fileID:
		return false
	case old.valueOffset < e.valueOffset:
		return true
	case old.valueOffset > e.valueOffset:
		return false
	}

	return false
}

// isNewerThan1 如果所有attr都等于旧条目，则返回true
func (e *entry) isNewerThan1(old *entry) bool {
	switch {
	case old.timeStamp < e.timeStamp:
		return false
	case old.timeStamp > e.timeStamp:
		return true
	}

	return true
}
