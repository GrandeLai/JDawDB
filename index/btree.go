package index

import (
	"JDawDB/data"
	"github.com/google/btree"
	"sync"
)

// Btree 封装google/btree这个库
type Btree struct {
	btree *btree.BTree
	lock  *sync.RWMutex
}

func NewBtree() *Btree {
	return &Btree{
		btree: btree.New(32),
		lock:  new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	//因为btree对写操作不是并发安全的，所以需要加锁
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.btree.ReplaceOrInsert(it)
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key, nil}
	btItem := bt.btree.Get(it)
	if btItem == nil {
		return nil
	}
	return btItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key, nil}
	//返回原先的item
	oldItem := bt.btree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}
