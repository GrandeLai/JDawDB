package JDawDB

import (
	"bytes"
	"github.com/GrandeLai/JDawDB/index"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	indexIt index.Iterator
	db      *DB
	Options IteratorOptions
}

// NewIterator 初始化用户迭代器
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	it := db.indexer.Iterator(options.Reverse)
	return &Iterator{
		indexIt: it,
		db:      db,
		Options: options,
	}
}

func (it *Iterator) Rewind() {
	it.indexIt.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIt.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.indexIt.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIt.Valid()
}

func (it *Iterator) Key() []byte {
	return it.indexIt.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIt.Value()
	it.db.mu.Lock()
	defer it.db.mu.Unlock()
	return it.db.GetValueByPosition(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIt.Close()
}

// 筛选过滤器
func (it *Iterator) skipToNext() {
	prefixLen := len(it.Options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIt.Valid(); it.indexIt.Next() {
		key := it.indexIt.Key()
		if prefixLen <= len(key) && bytes.Compare(it.Options.Prefix, key[:prefixLen]) == 0 { //如果前缀部分相等
			break
		}
	}
}
