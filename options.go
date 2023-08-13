package JDawDB

import "os"

// Options 定义打开文件的配置项
type Options struct {
	DirPath      string
	DataFileSize int64     //数据文件大小
	SyncWrites   bool      //每次写完数据是否都需要安全的持久化
	IndexType    IndexType //索引类型
}

type IndexType = int8

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	Reverse bool   // 是否反向遍历，默认 false 是正向
}

const (
	// Btree BTree索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// DefaultOptions 默认配置
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

// DefaultIteratorOptions 默认迭代器配置
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
