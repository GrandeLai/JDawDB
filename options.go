package JDawDB

import (
	"os"
)

// Options 定义打开文件的配置项
type Options struct {
	DirPath            string
	DataFileSize       int64     //数据文件大小
	SyncWrites         bool      //每次写完数据是否都需要安全的持久化
	BytesPerSync       uint      //累计写到多少字节进行持久化
	IndexType          IndexType //索引类型
	MMapAtStart        bool      //是否在启动时使用 MMap 加载数据
	DataFileMergeRatio float32   //需要merge的数据文件占总数据文件的比例阈值
}

type IndexType = int8

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	Reverse bool   // 是否反向遍历，默认 false 是正向
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchNum uint
	//每一次事务提交时是否持久化
	SyncWrites bool
}

const (
	// Btree BTree索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+ 树索引
	BPTree
)

// DefaultOptions 默认配置
var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          Btree,
	MMapAtStart:        true,
	DataFileMergeRatio: 0.5,
}

// DefaultIteratorOptions 默认迭代器配置
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}
