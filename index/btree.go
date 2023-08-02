package index

import (
	"JDawDB/data"
	"github.com/google/btree"
	"sync"
)

// BTree 封装google/btree这个库
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	//因为btree对写操作不是并发安全的，所以需要加锁
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key, nil}
	btItem := bt.tree.Get(it)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key, nil}
	//返回原先的item
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}
