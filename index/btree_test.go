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
