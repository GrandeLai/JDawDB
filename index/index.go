package index

import (
	"JDawDB/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口，后续加入的数据结构可以直接实现该接口
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

// Item 实现btree库内的Item接口
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (it Item) Less(bi btree.Item) bool {
	return bytes.Compare(it.key, bi.(*Item).key) == -1
}
