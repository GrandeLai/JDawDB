package index

import (
	"bytes"
	"github.com/GrandeLai/JDawDB/data"
	"github.com/google/btree"
	"sort"
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
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return NewBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BtreeIterator Btree索引迭代器
type BtreeIterator struct {
	currIndex int     //当前遍历到的索引
	reverse   bool    //是否反向遍历
	values    []*Item //存储key+位置索引信息的数组
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	//自定义一个方法
	saveValues := func(i btree.Item) bool {
		values[idx] = i.(*Item)
		idx++
		return true //返回false表示停止遍历
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &BtreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (bit *BtreeIterator) Rewind() {
	bit.currIndex = 0
}

func (bit *BtreeIterator) Seek(key []byte) {
	//二分查找加速
	if bit.reverse {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) <= 0
		})
	} else {
		bit.currIndex = sort.Search(len(bit.values), func(i int) bool {
			return bytes.Compare(bit.values[i].key, key) >= 0
		})
	}
}

func (bit *BtreeIterator) Next() {
	bit.currIndex++
}

func (bit *BtreeIterator) Valid() bool {
	return bit.currIndex >= 0 && bit.currIndex < len(bit.values)
}

func (bit *BtreeIterator) Key() []byte {
	return bit.values[bit.currIndex].key
}

func (bit *BtreeIterator) Value() *data.LogRecordPos {
	return bit.values[bit.currIndex].pos
}

func (bit *BtreeIterator) Close() {
	//清理临时数组
	bit.values = nil
}
