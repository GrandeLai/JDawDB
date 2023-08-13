package index

import (
	"github.com/GrandeLai/JDawDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get([]byte("hello"))
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put(nil, &data.LogRecordPos{Fid: 2, Offset: 2})
	assert.True(t, res2)

	pos2 := bt.Get(nil)
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)
	t.Log(pos2)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("hello"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	//1.BTree 为空
	it1 := bt1.Iterator(false)
	assert.Equal(t, false, it1.Valid())

	//2.BTree 不为空
	bt1.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 10})
	it2 := bt1.Iterator(false)
	assert.Equal(t, true, it2.Valid())
	t.Log(it2.Key(), it2.Value())
	it2.Next()
	assert.Equal(t, false, it2.Valid())

	//3.多个元素
	bt1.Put([]byte("hello1"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("hello2"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("hello3"), &data.LogRecordPos{Fid: 1, Offset: 10})
	it3 := bt1.Iterator(false)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		t.Log("key =", string(it3.Key()))
	}

	//4.反向遍历
	it4 := bt1.Iterator(true)
	for it4.Rewind(); it4.Valid(); it4.Next() {
		t.Log("key =", string(it4.Key()))
	}

	//5.seek
	it5 := bt1.Iterator(false)
	it5.Seek([]byte("hello2"))
	assert.Equal(t, true, it5.Valid())
	assert.Equal(t, "hello2", string(it5.Key()))
}
