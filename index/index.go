package index

import (
	"bytes"
	"github.com/GrandeLai/JDawDB/data"
	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续加入的数据结构可以直接实现该接口
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	// Size 返回索引的数据量
	Size() int
	// Iterator 返回索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据类型索引
func NewIndexer(tp IndexType) Indexer {
	switch tp {
	case Btree:
		return NewBtree()
	case ART:
		//todo： return NewART()
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item 实现btree库内的Item接口
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (it Item) Less(bi btree.Item) bool {
	return bytes.Compare(it.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器接口
type Iterator interface {
	Rewind()                   //重新回到迭代器的起点，即第一个数据
	Seek(kf []byte)            //根据传入的 key 查找到第一个大于 (或小于)等于的目标 key，根据从这个 key 开始遍历
	Next()                     //跳转到下一个 key
	Valid() bool               //是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Key() []byte               //当前遍历位置的 Key 数据
	Value() *data.LogRecordPos //当前遍历位置的 Value 数据
	Close()                    //关闭迭代器，释放相应资源
}
