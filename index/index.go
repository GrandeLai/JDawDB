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
}

type IndexType = int8

const (
	// BTree 索引
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
