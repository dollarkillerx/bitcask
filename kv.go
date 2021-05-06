package bitcask

import "sync"

// keyDirsLock for HashMap
var keyDirsLock *sync.RWMutex

var keyDirs *KeyDirs
var keyDirsOnce sync.Once

func init() {
	keyDirsLock = &sync.RWMutex{}
}

type KeyDirs struct {
	entrys  map[string]*entry
	dirName string
}

func NewKeyDir(dirName string) *KeyDirs {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	keyDirsOnce.Do(func() {
		if keyDirs == nil {
			keyDirs = &KeyDirs{
				entrys:  map[string]*entry{},
				dirName: dirName,
			}
		}
	})

	return keyDirs
}

func (k *KeyDirs) get(key string) (*entry, bool) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	e, ex := k.entrys[key]
	return e, ex
}

func (k *KeyDirs) del(key string) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	delete(k.entrys, key)
}

func (k *KeyDirs) set(key string, e *entry) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	k.entrys[key] = e
}

func (k *KeyDirs) setCompare(key string, e *entry) bool {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	old, ex := k.entrys[key]
	if !ex || e.isNewerThan1(old) { // 如果是旧的 或者不存在 就更新
		k.entrys[key] = e
		return true
	}

	return false
}

func (k *KeyDirs) updateFileID(oldID, newID uint32) {
	keyDirsLock.Lock()
	defer keyDirsLock.Unlock()

	for _, e := range k.entrys {
		if e.fileID == oldID {
			e.fileID = newID
		}
	}
}
