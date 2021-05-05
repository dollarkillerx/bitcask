详细资料
===

### 基础文件
- `data` 文件格式

```
crc32(4byte)|tStamp(4byte)|ksz(4byte)|valueSz(4byte)|key|value 
```

这样通过`key`的大小和`value`的大小就可以找到`key`的位置和`value`的文件，
但是如果`bitcask`重启后，直接扫描`data`文件来建立索引是一件非常耗时的工作，
这时候`hint`文件就派上场了，`hint`文件格式如下：

``` 
tstamp(4byte)|ksz(4byte)|valuesz(4byte)|valuePos(8byte)|key
```

这样在可以跳过`value`的扫描，扫描速度自然就起来了，通过`valuePos`就可以直接找到文件的内容。

### 数据结构的设计
- 文件映射的结构体 `BFile`

```go
// BFile 可写文件信息 1: datafile and hint file
type BFile struct {
	// fp is the writeable file
	fp          *os.File
	fileID      uint32
	writeOffset uint64
	// hintFp is the hint file
	hintFp *os.File
}
```

`fp`指向`Active data file`， `fileID`表示`Active data file`的文件名，`hintFp`表示`Active hint file`

- `BFiles `

```go
// BFiles ...
type BFiles struct {
	bfs    map[uint32]*BFile
	rwLock *sync.RWMutex
}
```

`bfs`每一项表示一个文件索引项，直接使用`map`来存储不是一个高效的方法，以后再优化吧…

- `key/value`结构体 `entry`

```go 
type entry struct {
	fileID      uint32 // file id
	valueSz     uint32 // value size in data block
	valueOffset uint64 // value offset in data block
	timeStamp   uint32 // file access time spot
}
```

该结是`hint`文件的映射，`fileID`为`data`的文件名，`valueSz`表示值的大小，`valueOffset`表示`value`在`data`文件的索引位置，
`timeStamp`表示`value`的存储时间（这个存储时间是会变的，因为在`merge`的时候，旧的数据会重新追加到`Active`文件中，
这样这些旧的数据会重新洗牌，变成新的数据）.

- `KeyDirs `

```go 
// KeyDirs ...
type KeyDirs struct {
	entrys map[string]*entry
}
```

这个结构是主要的占内存的地方，因为所有`key`都存储于此，这个结构体由`hint`文件构建的.

这个结构体也是后续需要优化的地方，比如：`fileID`很多是相同的，可以将他们存储在一个数组中，`entry`只要存储数组的`fileID`索引即可。

- `BitCask `

```go 
// BitCask ...
type BitCask struct {
	Opts      *Options      // opts for bitcask
	oldFile   *BFiles       // hint file, data file
	lockFile  *os.File      // lock file with process
	keyDirs   *KeyDirs      // key/value hashMap, building with hint file
	dirFile   string        // bitcask storage  root dir
	writeFile *BFile        // writeable file
	rwLock    *sync.RWMutex // rwlocker for bitcask Get and put Operation
}
```

`bitcask`是最重要的结构体，是程序的入口，`oldFile`是只读文件的索引；`writeFile`是`Active file`的索引；`keyDirs`是`key`的索引。

### 关于Merge
为了节省空间，`bitcask`采用`merge`的方式剔除脏数据，`merge`期间会影响到服务的访问，`merge`是一件消耗`disk io`时间，
用户应该错开`merge`的`io`高峰期.其中`merge`的触发也有很多种（触发不一定就会执行），如：

- 定时策略
- 容量策略 

