package index

import (
	"bytes"
	"github.com/GrandeLai/JDawDB/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ARTIterator ART索引迭代器
type ARTIterator struct {
	currIndex int     //当前遍历到的索引
	reverse   bool    //是否反向遍历
	values    []*Item //存储key+位置索引信息的数组
}

func NewARTIterator(tree goart.Tree, reverse bool) *ARTIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{node.Key(), node.Value().(*data.LogRecordPos)}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &ARTIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (ai *ARTIterator) Rewind() {
	ai.currIndex = 0
}

func (ai *ARTIterator) Seek(key []byte) {
	//二分查找加速
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *ARTIterator) Next() {
	ai.currIndex++
}

func (ai *ARTIterator) Valid() bool {
	return ai.currIndex >= 0 && ai.currIndex < len(ai.values)
}

func (ai *ARTIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

func (ai *ARTIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

func (ai *ARTIterator) Close() {
	//清理临时数组
	ai.values = nil
}
